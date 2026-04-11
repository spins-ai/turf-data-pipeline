#!/usr/bin/env python3
"""
integrate_new_features.py - Intègre de nouveaux builders dans features_consolidated
==================================================================================
Lit les JSONL de builder_outputs, crée un index par partant_uid,
puis fusionne avec features_consolidated.parquet row-group par row-group.
Gère les schémas incohérents entre row groups avec un schéma unifié.
"""

import sys
import gc
import time
import json
import math
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

CONSOLIDATED = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")

BUILDERS = {
    "rapphist_x": OUTPUT_DIR / "rapphist_x" / "rapphist_x_features.jsonl",
    "pagerank_x": OUTPUT_DIR / "pagerank_x" / "pagerank_x_features.jsonl",
}


def load_builder_index(name, jsonl_path):
    """Load JSONL into {partant_uid: {feature: value}} index."""
    print(f"  Loading {name} from {jsonl_path.name}...")
    index = {}
    feature_names = set()
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            uid = rec.pop("partant_uid", None)
            if uid:
                index[uid] = rec
                feature_names.update(rec.keys())
    print(f"    {len(index):,} records, {len(feature_names)} features")
    return index, sorted(feature_names)


def main():
    start = time.time()
    print("=" * 70)
    print("  INTEGRATION NOUVELLES FEATURES (schema unifié)")
    print("=" * 70)

    # Load all builder indexes
    all_indexes = {}
    all_new_features = []
    for name, path in BUILDERS.items():
        if not path.exists():
            print(f"  SKIP {name}: fichier non trouvé")
            continue
        idx, feats = load_builder_index(name, path)
        all_indexes[name] = idx
        all_new_features.extend(feats)

    if not all_indexes:
        print("  Rien à intégrer!")
        return

    print(f"\n  Total: {len(all_new_features)} nouvelles features à intégrer")

    # Build unified schema: read all row groups to find the "best" type for each column
    print(f"\nConstruction schema unifié...")
    pf = pq.ParquetFile(str(CONSOLIDATED))
    n_rg = pf.metadata.num_row_groups
    existing_cols = set(pf.schema_arrow.names)

    new_features_only = [f for f in all_new_features if f not in existing_cols]
    print(f"  {len(new_features_only)} features vraiment nouvelles")

    # Build best-type map from existing schema
    # For null-typed columns, try to find the real type from another row group
    col_types = {}
    for i, field in enumerate(pf.schema_arrow):
        col_types[field.name] = field.type

    # Scan other row groups for any null-typed columns
    null_cols = [name for name, typ in col_types.items() if typ == pa.null()]
    if null_cols:
        print(f"  {len(null_cols)} colonnes null-type, scanning pour types réels...")
        for rg_idx in range(min(n_rg, 10)):
            if not null_cols:
                break
            table = pf.read_row_group(rg_idx)
            for col_name in list(null_cols):
                if col_name in table.column_names:
                    col = table.column(col_name)
                    if col.type != pa.null():
                        col_types[col_name] = col.type
                        null_cols.remove(col_name)
            del table
            gc.collect()

    # Default remaining null columns to float64
    for col_name in null_cols:
        col_types[col_name] = pa.float64()

    # Add new feature columns as float64
    for feat in new_features_only:
        col_types[feat] = pa.float64()

    # Build target schema
    all_col_names = list(pf.schema_arrow.names) + new_features_only
    target_schema = pa.schema([(name, col_types.get(name, pa.float64())) for name in all_col_names])
    print(f"  Schema cible: {len(all_col_names)} colonnes")

    # Process row groups
    print(f"\nFusion row-group par row-group...")
    tmp_path = CONSOLIDATED.parent / "features_consolidated_new.parquet"
    writer = None
    total = 0

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx)
        df = table.to_pandas()
        del table

        uids = df["partant_uid"].values

        # Add new features
        for feat_name in new_features_only:
            vals = []
            for uid in uids:
                val = None
                for name, idx in all_indexes.items():
                    rec = idx.get(uid)
                    if rec and feat_name in rec:
                        val = rec[feat_name]
                        break
                vals.append(val)
            df[feat_name] = vals

        total += len(df)

        # Convert to table with unified schema
        new_table = pa.Table.from_pandas(df, preserve_index=False)

        # Cast to target schema
        cast_cols = []
        for field in target_schema:
            if field.name in new_table.column_names:
                col = new_table.column(field.name)
                if col.type != field.type and col.type != pa.null():
                    try:
                        col = col.cast(field.type)
                    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                        pass  # keep original type
                elif col.type == pa.null():
                    col = pa.array([None] * len(new_table), type=field.type)
                cast_cols.append(col)
            else:
                cast_cols.append(pa.array([None] * len(new_table), type=field.type))

        final_table = pa.table(
            {field.name: col for field, col in zip(target_schema, cast_cols)},
        )

        if writer is None:
            writer = pq.ParquetWriter(str(tmp_path), final_table.schema, compression="snappy")
        writer.write_table(final_table)

        del df, new_table, final_table, cast_cols
        gc.collect()

        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            print(f"  RG {rg_idx+1}/{n_rg} | {total:,} rows | {time.time()-start:.0f}s")

    if writer:
        writer.close()

    # Rename
    print("\nRenommage...")
    del pf
    gc.collect()
    time.sleep(3)

    import os
    backup = CONSOLIDATED.parent / "features_consolidated_backup.parquet"
    if backup.exists():
        os.remove(str(backup))
    os.rename(str(CONSOLIDATED), str(backup))
    os.rename(str(tmp_path), str(CONSOLIDATED))

    # Verify
    pf2 = pq.ParquetFile(str(CONSOLIDATED))
    print(f"\nVérification: {pf2.metadata.num_rows:,} rows, {pf2.metadata.num_columns} cols")
    new_cols = [c for c in pf2.schema_arrow.names if c.startswith(("rapphist_x", "pagerank_x"))]
    print(f"  Nouvelles colonnes: {new_cols}")

    # Check a sample
    t = pf2.read_row_group(15, columns=new_cols)
    for c in new_cols:
        col = t.column(c)
        non_null = col.drop_null()
        print(f"  {c}: {len(non_null)}/{len(col)} non-null")
    del t, pf2

    # Cleanup backup
    gc.collect()
    time.sleep(2)
    if backup.exists():
        os.remove(str(backup))
        print("  Backup supprimé")

    elapsed = time.time() - start
    print(f"\n{'='*70}")
    print(f"  TERMINE en {elapsed:.0f}s")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
