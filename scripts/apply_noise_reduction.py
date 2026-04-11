#!/usr/bin/env python3
"""
apply_noise_reduction.py -- Noise Reduction for ML Features
============================================================
Cleans noisy features from the capped Parquet. Uses PyArrow for streaming.

Checks:
  1. Near-zero variance  -- drop cols where >99% of non-null values are identical
  2. High NaN rate       -- drop cols with >90% NaN (from fill_rate_audit.csv)
  3. Target leakage      -- flag features with |corr(feat, is_gagnant)| > 0.5

Outputs:
  D:/turf-data-pipeline/04_FEATURES/noise_audit.csv
  D:/turf-data-pipeline/04_FEATURES/features_clean.parquet
"""

import sys
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from collections import Counter

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
INPUT_PARQUET   = Path("D:/turf-data-pipeline/04_FEATURES/features_capped.parquet")
TRAIN_UIDS_FILE = Path("D:/turf-data-pipeline/04_FEATURES/splits/train_uids.txt")
FILL_RATE_CSV   = Path("D:/turf-data-pipeline/04_FEATURES/fill_rate_audit.csv")
NOISE_AUDIT_CSV = Path("D:/turf-data-pipeline/04_FEATURES/noise_audit.csv")
CLEAN_PARQUET   = Path("D:/turf-data-pipeline/04_FEATURES/features_clean.parquet")
TMP_PARQUET     = Path("D:/turf-data-pipeline/04_FEATURES/features_clean_tmp.parquet")

TARGET_COL      = "comblage__is_gagnant"
UID_COL         = "partant_uid"

NZV_THRESHOLD   = 0.99   # near-zero variance ratio
NAN_THRESHOLD   = 0.90   # NaN rate threshold
CORR_THRESHOLD  = 0.5    # |correlation| threshold for leakage

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_train_uids() -> set:
    with open(TRAIN_UIDS_FILE, "r", encoding="utf-8") as f:
        uids = {line.strip() for line in f if line.strip()}
    print(f"  Loaded {len(uids):,} train UIDs")
    return uids


def get_numeric_cols(schema: pa.Schema) -> list[str]:
    """Return names of numeric (non-boolean) columns."""
    numeric = []
    for field in schema:
        t = field.type
        if (pa.types.is_integer(t) or pa.types.is_floating(t) or
            pa.types.is_decimal(t)) and not pa.types.is_boolean(t):
            numeric.append(field.name)
    return numeric


# ---------------------------------------------------------------------------
# Step 1: Near-zero variance (streaming)
# ---------------------------------------------------------------------------

def check_near_zero_variance(pf: pq.ParquetFile, numeric_cols: list[str],
                              threshold: float) -> list[dict]:
    """
    For each numeric col, compute ratio = count(most_common_value) / count(non_null).
    Use ALL data (not train-only) since we're measuring variance, not modeling.
    Process row-group by row-group to limit memory.
    """
    print(f"\n[Step 1] Near-zero variance check on {len(numeric_cols):,} cols (threshold={threshold})")

    # Accumulate per-value counts across row groups using approximate method:
    # Track total_non_null and top_value_count per column
    # For each row group, find the mode and its count
    # This is approximate but very fast

    # Actually, let's use a simpler approach: for each column, count non-null
    # and count the single most common value. We do this by reading columns
    # in batches to keep memory manageable.

    BATCH = 200  # columns per batch
    n_rg = pf.metadata.num_row_groups
    flagged = []

    for b_start in range(0, len(numeric_cols), BATCH):
        batch_cols = numeric_cols[b_start:b_start + BATCH]
        batch_num = b_start // BATCH + 1
        total_batches = (len(numeric_cols) + BATCH - 1) // BATCH
        print(f"  NZV batch {batch_num}/{total_batches} ({len(batch_cols)} cols) ...")

        # For each column in batch, accumulate value counts
        # Use Counter limited to top values for memory safety
        col_stats = {}  # col -> {total_non_null, top_count}

        for col in batch_cols:
            col_stats[col] = {"total": 0, "counter": Counter()}

        for rg_idx in range(n_rg):
            # Read only the batch columns for this row group
            cols_to_read = [c for c in batch_cols if c in pf.schema_arrow.names]
            if not cols_to_read:
                continue

            table = pf.read_row_group(rg_idx, columns=cols_to_read)

            for col in cols_to_read:
                arr = table.column(col).to_numpy(zero_copy_only=False)
                mask = ~pd.isna(arr)
                valid = arr[mask]
                col_stats[col]["total"] += len(valid)

                # Update counter (keep only top 10 to save memory)
                if len(valid) > 0:
                    vals, counts = np.unique(valid, return_counts=True)
                    for v, c in zip(vals, counts):
                        col_stats[col]["counter"][v] += int(c)

                    # Prune to top 10 to save memory
                    if len(col_stats[col]["counter"]) > 100:
                        col_stats[col]["counter"] = Counter(
                            dict(col_stats[col]["counter"].most_common(10))
                        )

            del table

        # Evaluate NZV for each column
        for col in batch_cols:
            total = col_stats[col]["total"]
            if total == 0:
                ratio = 1.0
            else:
                top_count = col_stats[col]["counter"].most_common(1)[0][1] if col_stats[col]["counter"] else 0
                ratio = top_count / total

            if ratio > threshold:
                flagged.append({
                    "feature": col,
                    "issue_type": "near_zero_variance",
                    "detail": f"top_value_ratio={ratio:.4f}, total_non_null={total}",
                    "action": "DROP",
                })

    print(f"  -> Flagged {len(flagged):,} near-zero variance features")
    return flagged


# ---------------------------------------------------------------------------
# Step 2: High NaN rate
# ---------------------------------------------------------------------------

def check_high_nan_rate(threshold: float) -> list[dict]:
    print(f"\n[Step 2] High NaN rate check (threshold={threshold})")

    if not FILL_RATE_CSV.exists():
        print(f"  WARNING: {FILL_RATE_CSV} not found. Skipping.")
        return []

    df = pd.read_csv(FILL_RATE_CSV)
    # Format: builder, feature, fill_rate, fill_pct, sampled
    # Build prefixed col name: builder__feature
    if "builder" in df.columns and "feature" in df.columns:
        df["col_name"] = df["builder"] + "__" + df["feature"]
    elif "col_name" in df.columns:
        pass
    else:
        print(f"  WARNING: Cannot determine column names from CSV. Skipping.")
        return []

    # fill_rate is 0-1
    fill_col = "fill_rate"
    if fill_col not in df.columns:
        for c in ["fill_pct", "pct_non_null"]:
            if c in df.columns:
                fill_col = c
                break

    # Normalize to 0-1
    if df[fill_col].max() > 1.5:
        df[fill_col] = df[fill_col] / 100.0

    # NaN rate = 1 - fill_rate
    high_nan = df[df[fill_col] < (1.0 - threshold)]

    flagged = []
    for _, row in high_nan.iterrows():
        nan_rate = 1.0 - row[fill_col]
        flagged.append({
            "feature": row["col_name"],
            "issue_type": "high_nan_rate",
            "detail": f"nan_rate={nan_rate:.4f}, fill_rate={row[fill_col]:.4f}",
            "action": "DROP",
        })

    print(f"  -> Flagged {len(flagged):,} high-NaN features")
    return flagged


# ---------------------------------------------------------------------------
# Step 3: Target leakage (correlation with is_gagnant)
# ---------------------------------------------------------------------------

def check_target_leakage(pf: pq.ParquetFile, numeric_cols: list[str],
                          train_uids: set, threshold: float) -> list[dict]:
    print(f"\n[Step 3] Target leakage check (|corr| > {threshold} with {TARGET_COL})")

    if TARGET_COL not in pf.schema_arrow.names:
        print(f"  WARNING: {TARGET_COL} not found. Skipping.")
        return []

    if UID_COL not in pf.schema_arrow.names:
        print(f"  WARNING: {UID_COL} not found. Cannot filter train set. Using all data.")
        train_uids = None

    # Read target column + UID for train filtering, then compute correlations
    # in batches of columns to avoid OOM
    BATCH = 100
    n_rg = pf.metadata.num_row_groups
    flagged = []

    # Exclude the target itself
    cols_to_check = [c for c in numeric_cols if c != TARGET_COL]

    total_batches = (len(cols_to_check) + BATCH - 1) // BATCH
    print(f"  Checking {len(cols_to_check):,} columns in {total_batches} batches ...")

    for b_start in range(0, len(cols_to_check), BATCH):
        batch_cols = cols_to_check[b_start:b_start + BATCH]
        batch_num = b_start // BATCH + 1
        print(f"  Leakage batch {batch_num}/{total_batches} ({len(batch_cols)} cols) ...")

        # Read target + batch cols across all row groups
        read_cols = [UID_COL, TARGET_COL] + batch_cols if train_uids else [TARGET_COL] + batch_cols
        read_cols = [c for c in read_cols if c in pf.schema_arrow.names]

        chunks = []
        for rg_idx in range(n_rg):
            table = pf.read_row_group(rg_idx, columns=read_cols)
            chunks.append(table)

        full_table = pa.concat_tables(chunks)
        del chunks

        df = full_table.to_pandas()
        del full_table

        # Filter to train set
        if train_uids and UID_COL in df.columns:
            df = df[df[UID_COL].isin(train_uids)]

        target = df[TARGET_COL].astype(float)

        for col in batch_cols:
            if col not in df.columns:
                continue
            try:
                vals = df[col].astype(float)
                mask = target.notna() & vals.notna()
                if mask.sum() < 100:
                    continue
                corr = np.corrcoef(target[mask].values, vals[mask].values)[0, 1]
                if np.isnan(corr):
                    continue
                if abs(corr) > threshold:
                    flagged.append({
                        "feature": col,
                        "issue_type": "target_leakage",
                        "detail": f"corr_with_{TARGET_COL}={corr:.4f}",
                        "action": "FLAG_ONLY",
                    })
            except (ValueError, TypeError):
                pass

        del df

    print(f"  -> Flagged {len(flagged):,} potential target-leakage features")
    return flagged


# ---------------------------------------------------------------------------
# Write clean parquet (drop flagged-DROP columns)
# ---------------------------------------------------------------------------

def write_clean_parquet(pf: pq.ParquetFile, cols_to_drop: set[str]) -> None:
    all_cols = pf.schema_arrow.names
    retained = [c for c in all_cols if c not in cols_to_drop]
    print(f"\nWriting clean Parquet: {len(retained):,}/{len(all_cols):,} cols retained "
          f"({len(cols_to_drop):,} dropped)")

    if TMP_PARQUET.exists():
        TMP_PARQUET.unlink()

    writer = None
    n_rg = pf.metadata.num_row_groups

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=retained)
        if writer is None:
            writer = pq.ParquetWriter(str(TMP_PARQUET), table.schema, compression='snappy')
        writer.write_table(table)
        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            print(f"  Row group {rg_idx + 1}/{n_rg} done")
        del table

    writer.close()

    # Atomic rename
    CLEAN_PARQUET.unlink(missing_ok=True)
    TMP_PARQUET.rename(CLEAN_PARQUET)
    size_mb = CLEAN_PARQUET.stat().st_size / 1024 / 1024
    print(f"  -> {CLEAN_PARQUET} ({size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    t0 = time.time()

    if not INPUT_PARQUET.exists():
        print(f"ERROR: Input not found: {INPUT_PARQUET}")
        sys.exit(1)

    print("=" * 60)
    print("NOISE REDUCTION PIPELINE")
    print("=" * 60)
    print(f"Input: {INPUT_PARQUET}")

    pf = pq.ParquetFile(str(INPUT_PARQUET))
    schema = pf.schema_arrow
    print(f"Cols: {len(schema):,}, Rows: {pf.metadata.num_rows:,}, RGs: {pf.metadata.num_row_groups}")

    numeric_cols = get_numeric_cols(schema)
    print(f"Numeric columns: {len(numeric_cols):,}")

    all_flags = []

    # Step 1: Near-zero variance
    nzv_flags = check_near_zero_variance(pf, numeric_cols, NZV_THRESHOLD)
    all_flags.extend(nzv_flags)

    # Step 2: High NaN rate
    nan_flags = check_high_nan_rate(NAN_THRESHOLD)
    all_flags.extend(nan_flags)

    # Step 3: Target leakage
    if TRAIN_UIDS_FILE.exists():
        train_uids = load_train_uids()
    else:
        print(f"  WARNING: {TRAIN_UIDS_FILE} not found. Using all data for leakage check.")
        train_uids = set()
    leakage_flags = check_target_leakage(pf, numeric_cols, train_uids, CORR_THRESHOLD)
    all_flags.extend(leakage_flags)

    # Write audit CSV
    audit_df = pd.DataFrame(all_flags, columns=["feature", "issue_type", "detail", "action"])
    audit_df = audit_df.drop_duplicates(subset=["feature", "issue_type"])
    audit_df.to_csv(NOISE_AUDIT_CSV, index=False, encoding="utf-8")
    print(f"\nAudit CSV: {NOISE_AUDIT_CSV} ({len(audit_df):,} rows)")

    # Determine columns to drop
    cols_to_drop = {f["feature"] for f in all_flags if f["action"] == "DROP"}

    # Summary
    issue_counts = Counter(f["issue_type"] for f in all_flags)
    print("\n" + "=" * 60)
    print("NOISE REDUCTION SUMMARY")
    print("=" * 60)
    print(f"  Total features in input  : {len(schema):,}")
    print(f"  Total features flagged   : {len(set(f['feature'] for f in all_flags)):,}")
    print(f"  By issue type:")
    for issue, cnt in sorted(issue_counts.items()):
        print(f"    {issue:<30} {cnt:>6,}")
    print(f"  Features to DROP         : {len(cols_to_drop):,}")
    print(f"  Features retained        : {len(schema) - len(cols_to_drop):,}")
    print("=" * 60)

    # Write clean Parquet
    write_clean_parquet(pf, cols_to_drop)

    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print("Done.")


if __name__ == "__main__":
    main()
