#!/usr/bin/env python3
"""Encode remaining categorical (string) columns for ML.

Strategy:
  1. Keep partant_uid as row identifier
  2. Many string cols are duplicated across builders (comblage__sexe,
     nettoyage__sexe, etc.) -- pick ONE canonical version per base feature
  3. Low cardinality (<20)   -> label encoding (int)
  4. Mid cardinality (20-200) -> frequency encoding (int, train-set counts)
  5. High cardinality (>200) or text/ID columns -> drop

Uses PyArrow for memory-safe streaming.

Input:  D:/turf-data-pipeline/04_FEATURES/features_clean.parquet
Output: D:/turf-data-pipeline/04_FEATURES/features_encoded.parquet
"""

import sys
import time
import csv
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from collections import Counter

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
INPUT_PARQUET  = Path("D:/turf-data-pipeline/04_FEATURES/features_clean.parquet")
TRAIN_UIDS     = Path("D:/turf-data-pipeline/04_FEATURES/splits/train_uids.txt")
OUTPUT_PARQUET = Path("D:/turf-data-pipeline/04_FEATURES/features_encoded.parquet")
TMP_PARQUET    = Path("D:/turf-data-pipeline/04_FEATURES/features_encoded_tmp.parquet")
ENCODING_LOG   = Path("D:/turf-data-pipeline/04_FEATURES/encoding_log.csv")

# Cardinality thresholds
LOW_CARD_MAX  = 20    # label encode
HIGH_CARD_MAX = 200   # freq encode up to this; drop above

# Columns that are identifiers/metadata -- always drop (except partant_uid)
ID_PATTERNS = {
    "reunion_uid", "cle_partant", "source", "horse_id", "nom_cheval",
    "musique", "commentaire_apres_course", "avis_entraineur",
    "ecart_precedent", "rap_course_key", "rap_rapport_uid",
    "mch_record_key", "pgr_nom", "rap_combinaison",
    "proprietaire", "eleveur", "pere_mere",
}

# Patterns that indicate a combinaison/pari column (always drop)
DROP_SUBSTRINGS = ["_combinaison", "_rapport_uid", "_record_key", "_course_key"]


def is_id_or_text(base_name: str) -> bool:
    """Check if a base feature name is an ID/text column to drop."""
    if base_name in ID_PATTERNS:
        return True
    for sub in DROP_SUBSTRINGS:
        if sub in base_name:
            return True
    return False


def get_base_name(col: str) -> str:
    """Extract base feature name: 'comblage__sexe' -> 'sexe'."""
    if "__" in col:
        return col.split("__", 1)[1]
    return col


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    t0 = time.time()

    if not INPUT_PARQUET.exists():
        print(f"ERROR: {INPUT_PARQUET} not found")
        sys.exit(1)

    print("=" * 60)
    print("CATEGORICAL ENCODING")
    print("=" * 60)
    print(f"Input: {INPUT_PARQUET}")

    pf = pq.ParquetFile(str(INPUT_PARQUET))
    schema = pf.schema_arrow
    n_rg = pf.metadata.num_row_groups
    total_rows = pf.metadata.num_rows
    print(f"Cols: {len(schema):,}, Rows: {total_rows:,}, RGs: {n_rg}")

    # Identify string columns
    str_cols = []
    for field in schema:
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            str_cols.append(field.name)
    print(f"\nString columns: {len(str_cols):,}")

    # Load train UIDs
    train_uids = set()
    if TRAIN_UIDS.exists():
        with open(TRAIN_UIDS, "r", encoding="utf-8") as f:
            train_uids = {line.strip() for line in f if line.strip()}
        print(f"Train UIDs loaded: {len(train_uids):,}")

    # ---------------------------------------------------------------------------
    # 1. Classify string columns
    # ---------------------------------------------------------------------------
    print("\n[1/4] Classifying string columns ...")

    # Group by base_name, pick canonical (prefer comblage__ prefix)
    base_to_cols = {}
    for col in str_cols:
        if col == "partant_uid":
            continue
        base = get_base_name(col)
        if base not in base_to_cols:
            base_to_cols[base] = []
        base_to_cols[base].append(col)

    # Pick canonical column for each base feature
    canonical = {}  # base_name -> col_name
    for base, cols in base_to_cols.items():
        # Prefer comblage__ prefix, then first available
        preferred = [c for c in cols if c.startswith("comblage__")]
        canonical[base] = preferred[0] if preferred else cols[0]

    # Columns to drop entirely (all non-canonical duplicates + IDs)
    cols_to_drop = set()
    canonical_set = set(canonical.values())

    for col in str_cols:
        if col == "partant_uid":
            continue
        if col not in canonical_set:
            cols_to_drop.add(col)  # duplicate

    # Among canonical cols, check if ID/text -> drop those too
    canonical_ids = set()
    for base, col in canonical.items():
        if is_id_or_text(base):
            cols_to_drop.add(col)
            canonical_ids.add(base)

    # Remaining canonical cols need cardinality check
    to_check = {b: c for b, c in canonical.items() if b not in canonical_ids}
    print(f"  Duplicate string cols to drop: {len(str_cols) - 1 - len(canonical):,}")
    print(f"  ID/text cols to drop: {len(canonical_ids):,}")
    print(f"  Canonical cols to check cardinality: {len(to_check):,}")

    # ---------------------------------------------------------------------------
    # 2. Compute cardinality for canonical columns (sample first RG)
    # ---------------------------------------------------------------------------
    print("\n[2/4] Computing cardinality ...")
    cardinality = {}
    check_cols = list(to_check.values())

    if check_cols:
        # Read first few row groups for cardinality estimate
        sample_table = pf.read_row_group(0, columns=check_cols)
        for rg_idx in range(1, min(5, n_rg)):
            sample_table = pa.concat_tables([sample_table, pf.read_row_group(rg_idx, columns=check_cols)])

        for col in check_cols:
            arr = sample_table.column(col)
            unique_vals = arr.drop_null().unique()
            cardinality[col] = len(unique_vals)
        del sample_table

    # Classify
    label_cols = []    # low cardinality -> label encode
    freq_cols = []     # mid cardinality -> frequency encode
    high_card_drop = []  # too many categories -> drop

    for base, col in to_check.items():
        card = cardinality.get(col, 0)
        if card < LOW_CARD_MAX:
            label_cols.append(col)
        elif card <= HIGH_CARD_MAX:
            freq_cols.append(col)
        else:
            high_card_drop.append(col)
            cols_to_drop.add(col)

    print(f"  Label encode  (<{LOW_CARD_MAX}): {len(label_cols)}")
    print(f"  Freq encode   ({LOW_CARD_MAX}-{HIGH_CARD_MAX}): {len(freq_cols)}")
    print(f"  Drop (>{HIGH_CARD_MAX}): {len(high_card_drop)}")

    for col in label_cols:
        print(f"    LABEL: {col} ({cardinality.get(col, '?')} unique)")
    for col in freq_cols:
        print(f"    FREQ:  {col} ({cardinality.get(col, '?')} unique)")
    for col in high_card_drop:
        print(f"    DROP:  {col} ({cardinality.get(col, '?')} unique)")

    # ---------------------------------------------------------------------------
    # 3. Build encoding maps from train set
    # ---------------------------------------------------------------------------
    print("\n[3/4] Building encoding maps (train set) ...")

    encode_cols = label_cols + freq_cols
    label_maps = {}   # col -> {value: int_label}
    freq_maps = {}    # col -> {value: count}

    if encode_cols:
        # Read all data for encode cols + partant_uid for train filtering
        read_cols = ["partant_uid"] + encode_cols if train_uids else encode_cols
        read_cols = [c for c in read_cols if c in schema.names]

        chunks = []
        for rg_idx in range(n_rg):
            chunks.append(pf.read_row_group(rg_idx, columns=read_cols))
        full = pa.concat_tables(chunks).to_pandas()
        del chunks

        # Filter to train set
        if train_uids and "partant_uid" in full.columns:
            train_df = full[full["partant_uid"].isin(train_uids)]
        else:
            train_df = full

        # Build label maps (sorted by frequency desc, 0 reserved for unknown)
        for col in label_cols:
            vc = train_df[col].dropna().astype(str).value_counts()
            label_maps[col] = {val: idx + 1 for idx, (val, _) in enumerate(vc.items())}

        # Build frequency maps
        for col in freq_cols:
            vc = train_df[col].dropna().astype(str).value_counts()
            freq_maps[col] = {val: int(cnt) for val, cnt in vc.items()}

        del full, train_df

    # ---------------------------------------------------------------------------
    # 4. Write encoded Parquet (streaming row-group by row-group)
    # ---------------------------------------------------------------------------
    print(f"\n[4/4] Writing encoded Parquet ...")

    # Determine output columns: all non-dropped columns + encoded replacements
    output_cols = []
    for field in schema:
        if field.name in cols_to_drop:
            continue
        output_cols.append(field.name)

    print(f"  Input cols: {len(schema):,}")
    print(f"  Dropping: {len(cols_to_drop):,} string cols")
    print(f"  Encoding: {len(label_cols)} label + {len(freq_cols)} freq")
    print(f"  Output cols: ~{len(output_cols):,}")

    if TMP_PARQUET.exists():
        TMP_PARQUET.unlink()

    writer = None

    for rg_idx in range(n_rg):
        # Read all retained columns
        table = pf.read_row_group(rg_idx, columns=output_cols)

        # Apply label encoding
        for col in label_cols:
            if col not in table.column_names:
                continue
            col_idx = table.column_names.index(col)
            arr = table.column(col)
            mapping = label_maps[col]
            # Convert: map string values to integers
            encoded = np.array([
                mapping.get(str(v.as_py()), 0) if v.is_valid else 0
                for v in arr
            ], dtype=np.int32)
            new_col = pa.array(encoded, type=pa.int32())
            table = table.set_column(col_idx, col, new_col)

        # Apply frequency encoding
        for col in freq_cols:
            if col not in table.column_names:
                continue
            col_idx = table.column_names.index(col)
            arr = table.column(col)
            mapping = freq_maps[col]
            encoded = np.array([
                mapping.get(str(v.as_py()), 0) if v.is_valid else 0
                for v in arr
            ], dtype=np.int32)
            new_col = pa.array(encoded, type=pa.int32())
            table = table.set_column(col_idx, col, new_col)

        if writer is None:
            writer = pq.ParquetWriter(str(TMP_PARQUET), table.schema, compression='snappy')
        writer.write_table(table)

        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            print(f"  Row group {rg_idx + 1}/{n_rg} done")
        del table

    writer.close()

    # Atomic rename
    OUTPUT_PARQUET.unlink(missing_ok=True)
    TMP_PARQUET.rename(OUTPUT_PARQUET)
    size_mb = OUTPUT_PARQUET.stat().st_size / 1024 / 1024

    # Write encoding log
    with open(ENCODING_LOG, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["column", "encoding", "n_categories", "cardinality"])
        for col in sorted(label_cols):
            w.writerow([col, "label", len(label_maps.get(col, {})), cardinality.get(col, "")])
        for col in sorted(freq_cols):
            w.writerow([col, "frequency", len(freq_maps.get(col, {})), cardinality.get(col, "")])
        for col in sorted(cols_to_drop):
            w.writerow([col, "dropped", "", ""])

    elapsed = time.time() - t0

    print("\n" + "=" * 60)
    print("ENCODING SUMMARY")
    print("=" * 60)
    print(f"  Input columns       : {len(schema):,}")
    print(f"  String cols dropped : {len(cols_to_drop):,}")
    print(f"  Label encoded       : {len(label_cols)}")
    print(f"  Freq encoded        : {len(freq_cols)}")
    print(f"  Output columns      : {len(output_cols):,}")
    print(f"  Output size         : {size_mb:.1f} MB")
    print(f"  Encoding log        : {ENCODING_LOG}")
    print(f"  Time                : {elapsed:.0f}s")
    print(f"  Output              : {OUTPUT_PARQUET}")
    print("=" * 60)


if __name__ == "__main__":
    main()
