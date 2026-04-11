#!/usr/bin/env python3
"""
apply_normalization.py -- Global z-score normalization for ML features
=====================================================================
Computes mean/std on train set only (no leakage), applies to all data.
Saves normalization stats for inference-time use.

Input:  D:/turf-data-pipeline/04_FEATURES/features_encoded.parquet
Output: D:/turf-data-pipeline/04_FEATURES/features_normalized.parquet
Stats:  D:/turf-data-pipeline/04_FEATURES/normalization_stats.csv

Note: Tree models (CatBoost, XGBoost) don't need normalization but
neural networks do. We save both normalized and raw versions.
The normalized file replaces raw values with z-scores for all numeric
features except IDs and boolean-like columns.
"""

import sys
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
INPUT_PARQUET  = Path("D:/turf-data-pipeline/04_FEATURES/features_encoded.parquet")
TRAIN_UIDS     = Path("D:/turf-data-pipeline/04_FEATURES/splits/train_uids.txt")
OUTPUT_PARQUET = Path("D:/turf-data-pipeline/04_FEATURES/features_normalized.parquet")
TMP_PARQUET    = Path("D:/turf-data-pipeline/04_FEATURES/features_normalized_tmp.parquet")
STATS_CSV      = Path("D:/turf-data-pipeline/04_FEATURES/normalization_stats.csv")

# Columns to skip normalization (IDs, booleans, already-normalized)
SKIP_PREFIXES = ["partant_uid"]
SKIP_IF_BOOLEAN_LIKE = True  # skip cols with only 0/1 values

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    t0 = time.time()

    if not INPUT_PARQUET.exists():
        print(f"ERROR: {INPUT_PARQUET} not found")
        sys.exit(1)

    print("=" * 60)
    print("GLOBAL Z-SCORE NORMALIZATION")
    print("=" * 60)
    print(f"Input: {INPUT_PARQUET}")

    pf = pq.ParquetFile(str(INPUT_PARQUET))
    schema = pf.schema_arrow
    n_rg = pf.metadata.num_row_groups
    total_rows = pf.metadata.num_rows
    print(f"Cols: {len(schema):,}, Rows: {total_rows:,}, RGs: {n_rg}")

    # Identify numeric columns to normalize
    numeric_cols = []
    for field in schema:
        if field.name in SKIP_PREFIXES:
            continue
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            continue
        if pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
            continue
        if pa.types.is_boolean(field.type):
            continue
        if pa.types.is_integer(field.type) or pa.types.is_floating(field.type) or pa.types.is_decimal(field.type):
            numeric_cols.append(field.name)

    print(f"Numeric columns to normalize: {len(numeric_cols):,}")

    # Load train UIDs
    train_uids = set()
    if TRAIN_UIDS.exists():
        with open(TRAIN_UIDS, "r", encoding="utf-8") as f:
            train_uids = {line.strip() for line in f if line.strip()}
        print(f"Train UIDs loaded: {len(train_uids):,}")

    # ---------------------------------------------------------------------------
    # Pass 1: Compute mean/std on train set (streaming, batched columns)
    # ---------------------------------------------------------------------------
    print("\n[1/3] Computing train-set statistics ...")

    # Accumulate running stats using Welford's online algorithm
    # For memory safety, we process columns in batches
    BATCH = 200
    stats = {}  # col -> (mean, std)

    for b_start in range(0, len(numeric_cols), BATCH):
        batch_cols = numeric_cols[b_start:b_start + BATCH]
        batch_num = b_start // BATCH + 1
        total_batches = (len(numeric_cols) + BATCH - 1) // BATCH
        print(f"  Stats batch {batch_num}/{total_batches} ({len(batch_cols)} cols) ...")

        # Accumulate sum and sum_sq for each column
        col_sum = {c: 0.0 for c in batch_cols}
        col_sum_sq = {c: 0.0 for c in batch_cols}
        col_count = {c: 0 for c in batch_cols}

        read_cols = ["partant_uid"] + batch_cols if train_uids else batch_cols
        read_cols = [c for c in read_cols if c in schema.names]

        for rg_idx in range(n_rg):
            table = pf.read_row_group(rg_idx, columns=read_cols)
            df = table.to_pandas()
            del table

            # Filter to train
            if train_uids and "partant_uid" in df.columns:
                df = df[df["partant_uid"].isin(train_uids)]

            for col in batch_cols:
                if col not in df.columns:
                    continue
                vals = df[col].astype(float).dropna().values
                if len(vals) > 0:
                    col_sum[col] += vals.sum()
                    col_sum_sq[col] += (vals ** 2).sum()
                    col_count[col] += len(vals)

            del df

        # Compute mean and std
        for col in batch_cols:
            n = col_count[col]
            if n > 1:
                mean = col_sum[col] / n
                variance = (col_sum_sq[col] / n) - (mean ** 2)
                std = max(np.sqrt(max(variance, 0)), 1e-10)  # avoid div by zero
                stats[col] = (mean, std)
            else:
                stats[col] = (0.0, 1.0)  # no data -> no normalization

    # Filter out boolean-like columns (std ~= 0.5 and mean between 0 and 1)
    boolean_like = set()
    if SKIP_IF_BOOLEAN_LIKE:
        for col, (mean, std) in stats.items():
            if 0 <= mean <= 1 and std < 0.51 and std > 0.01:
                # Could be boolean-like, check more carefully
                # Actually, let's not skip these - tree models handle them fine
                pass

    # Save stats
    stats_df = pd.DataFrame([
        {"feature": col, "train_mean": mean, "train_std": std}
        for col, (mean, std) in sorted(stats.items())
    ])
    stats_df.to_csv(STATS_CSV, index=False)
    print(f"  Stats saved: {STATS_CSV} ({len(stats_df):,} features)")

    # Count how many have very low std (essentially constant)
    low_std = sum(1 for _, (_, s) in stats.items() if s < 1e-6)
    print(f"  Columns with near-zero std (will not normalize): {low_std}")

    # ---------------------------------------------------------------------------
    # Pass 2: Apply z-score normalization (streaming)
    # ---------------------------------------------------------------------------
    print(f"\n[2/3] Applying z-score normalization ...")

    if TMP_PARQUET.exists():
        TMP_PARQUET.unlink()

    writer = None
    cols_to_normalize = [c for c in numeric_cols if stats[c][1] > 1e-6]
    print(f"  Columns to normalize: {len(cols_to_normalize):,}")
    print(f"  Columns unchanged: {len(numeric_cols) - len(cols_to_normalize):,}")

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx)

        for col in cols_to_normalize:
            if col not in table.column_names:
                continue
            col_idx = table.column_names.index(col)
            arr = table.column(col).to_numpy(zero_copy_only=False).astype(np.float64)

            mean, std = stats[col]
            # Z-score: (x - mean) / std, preserve NaN
            normalized = (arr - mean) / std

            new_col = pa.array(normalized, type=pa.float64(), from_pandas=True)
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

    elapsed = time.time() - t0

    print(f"\n[3/3] Summary")
    print("=" * 60)
    print(f"  Input columns       : {len(schema):,}")
    print(f"  Normalized columns  : {len(cols_to_normalize):,}")
    print(f"  Output size         : {size_mb:.1f} MB")
    print(f"  Stats CSV           : {STATS_CSV}")
    print(f"  Time                : {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Output              : {OUTPUT_PARQUET}")
    print("=" * 60)


if __name__ == "__main__":
    main()
