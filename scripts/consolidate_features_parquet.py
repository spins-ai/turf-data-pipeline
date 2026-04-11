#!/usr/bin/env python3
"""Consolidate all builder outputs into one Parquet file.

Strategy (memory-safe):
1. Read the features_to_drop.csv to know which features to exclude
2. For each builder output, read in streaming and build a {uid: {features}} dict
3. Process builders in chunks to stay under RAM limit
4. Join all features by partant_uid and write to Parquet in batches

This is the FINAL step before ML model training.

Output: D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet

WARNING: This script reads ~80 GB of JSONL and needs ~30 GB RAM.
Run ONLY when no other heavy process is running.
"""
import csv
import gc
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pip install pyarrow")
    sys.exit(1)

BASE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
DROP_CSV = Path("D:/turf-data-pipeline/04_FEATURES/features_to_drop.csv")
OUTPUT = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
MASTER = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

# Process builders in groups of this many to limit RAM
BUILDER_CHUNK_SIZE = 20
BATCH_WRITE_SIZE = 100_000


def _load_drop_set() -> set[str]:
    """Load features to drop from the dedup/correlation audit."""
    drops = set()
    if DROP_CSV.exists():
        with open(DROP_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                # feature_to_drop format: "builder/feature"
                drops.add(row["feature_to_drop"])
    return drops


def _read_builder_features(bdir: Path, drop_set: set[str]) -> dict[str, dict[str, float | None]]:
    """Read a builder's JSONL and return {uid: {prefixed_features}}."""
    jsonls = [f for f in bdir.iterdir() if f.suffix == ".jsonl" and ".tmp" not in f.name]
    if not jsonls:
        return {}

    builder_name = bdir.name
    result = {}

    try:
        with open(jsonls[0], "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue

                uid = rec.get("partant_uid", "")
                if not uid:
                    continue

                features = {}
                for k, v in rec.items():
                    if k == "partant_uid":
                        continue
                    # Skip dropped features
                    full_key = f"{builder_name}/{k}"
                    if full_key in drop_set:
                        continue
                    # Skip non-feature columns
                    if k in ("course_uid", "date_reunion_iso", "date_reunion"):
                        continue
                    # Convert to float if possible
                    if v is None or (isinstance(v, str) and v.strip() == ""):
                        features[k] = None
                    else:
                        try:
                            fv = float(v)
                            features[k] = fv if math.isfinite(fv) else None
                        except (ValueError, TypeError):
                            # Skip non-numeric features for now
                            pass

                if features:
                    result[uid] = features
    except Exception as e:
        print(f"  ERROR reading {builder_name}: {e}", file=sys.stderr)

    return result


def main():
    t0 = time.perf_counter()

    # Load drop set
    drop_set = _load_drop_set()
    print(f"Features to drop: {len(drop_set)}", file=sys.stderr)

    # Get all builder directories
    builders = sorted(d for d in BASE.iterdir() if d.is_dir())
    # Filter to only those with valid JSONL
    builders = [b for b in builders if any(
        f.suffix == ".jsonl" and ".tmp" not in f.name for f in b.iterdir()
    )]
    print(f"Builders to process: {len(builders)}", file=sys.stderr)

    # Get master UIDs (ordered)
    print("Reading master UIDs...", file=sys.stderr)
    master_uids = []
    with open(MASTER, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            master_uids.append(rec.get("partant_uid", ""))
    print(f"  Master UIDs: {len(master_uids):,}", file=sys.stderr)

    # Process builders in chunks
    # For each chunk, read all builder features into memory, then merge
    all_columns = set()
    uid_features = defaultdict(dict)  # uid -> {col: val}

    for chunk_start in range(0, len(builders), BUILDER_CHUNK_SIZE):
        chunk = builders[chunk_start:chunk_start + BUILDER_CHUNK_SIZE]
        chunk_names = [b.name for b in chunk]
        print(f"\nProcessing chunk {chunk_start // BUILDER_CHUNK_SIZE + 1}: "
              f"{chunk_names[0]}...{chunk_names[-1]} ({len(chunk)} builders)", file=sys.stderr)

        for bdir in chunk:
            features = _read_builder_features(bdir, drop_set)
            if not features:
                continue

            # Prefix columns with builder name to avoid collisions
            sample_uid = next(iter(features))
            feat_cols = list(features[sample_uid].keys())

            # Check for column name collisions
            for col in feat_cols:
                if col in all_columns:
                    # Prefix with builder name
                    for uid, feats in features.items():
                        if col in feats:
                            feats[f"{bdir.name}__{col}"] = feats.pop(col)
                    feat_cols = [f"{bdir.name}__{c}" if c == col else c for c in feat_cols]

            all_columns.update(feat_cols)

            # Merge into uid_features
            for uid, feats in features.items():
                uid_features[uid].update(feats)

            del features
            print(f"  {bdir.name}: {len(feat_cols)} features", file=sys.stderr)

        gc.collect()

    # Write Parquet
    print(f"\nWriting Parquet with {len(all_columns)} features...", file=sys.stderr)
    columns = sorted(all_columns)
    tmp = OUTPUT.with_suffix(".tmp.parquet")

    writer = None
    batch_data = {col: [] for col in ["partant_uid"] + columns}
    batch_count = 0

    for uid in master_uids:
        feats = uid_features.get(uid, {})
        batch_data["partant_uid"].append(uid)
        for col in columns:
            batch_data[col].append(feats.get(col))
        batch_count += 1

        if batch_count >= BATCH_WRITE_SIZE:
            table = pa.table(batch_data)
            if writer is None:
                writer = pq.ParquetWriter(str(tmp), table.schema, compression="snappy")
            writer.write_table(table)
            batch_data = {col: [] for col in ["partant_uid"] + columns}
            batch_count = 0
            del table
            gc.collect()

            if sum(1 for _ in batch_data["partant_uid"]) == 0:
                pass  # Just reset

    # Write remaining
    if batch_count > 0:
        table = pa.table(batch_data)
        if writer is None:
            writer = pq.ParquetWriter(str(tmp), table.schema, compression="snappy")
        writer.write_table(table)

    if writer:
        writer.close()

    if OUTPUT.exists():
        OUTPUT.unlink()
    tmp.rename(OUTPUT)

    elapsed = time.perf_counter() - t0
    out_size = OUTPUT.stat().st_size / 1e9

    print(f"\n{'='*60}")
    print(f"CONSOLIDATION COMPLETE")
    print(f"{'='*60}")
    print(f"Records: {len(master_uids):,}")
    print(f"Features: {len(columns)}")
    print(f"Output size: {out_size:.1f} GB")
    print(f"Time: {elapsed:.0f}s")
    print(f"Output: {OUTPUT}")


if __name__ == "__main__":
    main()
