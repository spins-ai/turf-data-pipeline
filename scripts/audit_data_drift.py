#!/usr/bin/env python3
"""
audit_data_drift.py -- Detect distribution drift between train and test splits
===============================================================================
Compares feature distributions between train and test to identify potential
data drift that could hurt model generalization.

Uses Population Stability Index (PSI) as the drift metric:
  PSI < 0.1  : no significant drift
  0.1 - 0.25 : moderate drift (monitor)
  PSI > 0.25 : significant drift (investigate)

Input:  D:/turf-data-pipeline/04_FEATURES/features_selected.parquet
Splits: D:/turf-data-pipeline/04_FEATURES/splits/train_uids.txt
        D:/turf-data-pipeline/04_FEATURES/splits/test_uids.txt
Output: D:/turf-data-pipeline/04_FEATURES/data_drift_audit.csv
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
INPUT_PARQUET  = Path("D:/turf-data-pipeline/04_FEATURES/features_selected.parquet")
TRAIN_UIDS     = Path("D:/turf-data-pipeline/04_FEATURES/splits/train_uids.txt")
TEST_UIDS      = Path("D:/turf-data-pipeline/04_FEATURES/splits/test_uids.txt")
OUTPUT_CSV     = Path("D:/turf-data-pipeline/04_FEATURES/data_drift_audit.csv")
UID_COL        = "partant_uid"

N_BINS = 10  # bins for PSI calculation


def compute_psi(expected: np.ndarray, actual: np.ndarray, n_bins: int = N_BINS) -> float:
    """Compute Population Stability Index between two distributions."""
    expected = expected[~np.isnan(expected)]
    actual = actual[~np.isnan(actual)]

    if len(expected) < 10 or len(actual) < 10:
        return 0.0

    breakpoints = np.percentile(expected, np.linspace(0, 100, n_bins + 1))
    breakpoints = np.unique(breakpoints)
    if len(breakpoints) < 3:
        return 0.0

    expected_counts = np.histogram(expected, bins=breakpoints)[0]
    actual_counts = np.histogram(actual, bins=breakpoints)[0]

    eps = 1e-6
    expected_pct = expected_counts / len(expected) + eps
    actual_pct = actual_counts / len(actual) + eps

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return float(psi)


def main() -> None:
    t0 = time.time()

    if not INPUT_PARQUET.exists():
        print(f"ERROR: {INPUT_PARQUET} not found")
        sys.exit(1)

    print("=" * 60)
    print("DATA DRIFT AUDIT (PSI)")
    print("=" * 60)

    train_uids = set()
    test_uids = set()
    if TRAIN_UIDS.exists():
        with open(TRAIN_UIDS, "r") as f:
            train_uids = {l.strip() for l in f if l.strip()}
    if TEST_UIDS.exists():
        with open(TEST_UIDS, "r") as f:
            test_uids = {l.strip() for l in f if l.strip()}

    print(f"Train UIDs: {len(train_uids):,}")
    print(f"Test UIDs:  {len(test_uids):,}")

    if not train_uids or not test_uids:
        print("ERROR: Need both train and test UIDs")
        sys.exit(1)

    pf = pq.ParquetFile(str(INPUT_PARQUET))
    schema = pf.schema_arrow
    n_rg = pf.metadata.num_row_groups

    numeric_cols = []
    for field in schema:
        if field.name == UID_COL:
            continue
        if pa.types.is_integer(field.type) or pa.types.is_floating(field.type) or pa.types.is_decimal(field.type):
            numeric_cols.append(field.name)

    print(f"Numeric columns: {len(numeric_cols):,}")

    # Read ALL data for all numeric cols in one pass (streaming by RG)
    # but only keep UIDs + assign train/test in one pass, then compute PSI
    print(f"\nReading data (1 pass, all {len(numeric_cols)} cols) ...")

    # Collect train and test arrays per column
    train_vals = {c: [] for c in numeric_cols}
    test_vals = {c: [] for c in numeric_cols}

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx)
        df = table.to_pandas()
        del table

        mask_train = df[UID_COL].isin(train_uids)
        mask_test = df[UID_COL].isin(test_uids)

        for col in numeric_cols:
            if col not in df.columns:
                continue
            arr = df[col].astype(np.float32).values
            train_vals[col].append(arr[mask_train.values])
            test_vals[col].append(arr[mask_test.values])

        del df
        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            print(f"  Row group {rg_idx + 1}/{n_rg} done")

    print(f"Computing PSI for {len(numeric_cols)} features ...")
    results = []

    for col in numeric_cols:
        train_arr = np.concatenate(train_vals[col]).astype(np.float64) if train_vals[col] else np.array([])
        test_arr = np.concatenate(test_vals[col]).astype(np.float64) if test_vals[col] else np.array([])

        psi = compute_psi(train_arr, test_arr)
        train_mean = float(np.nanmean(train_arr)) if len(train_arr) > 0 and not np.all(np.isnan(train_arr)) else 0.0
        test_mean = float(np.nanmean(test_arr)) if len(test_arr) > 0 and not np.all(np.isnan(test_arr)) else 0.0
        train_std = float(np.nanstd(train_arr)) if len(train_arr) > 0 and not np.all(np.isnan(train_arr)) else 0.0
        test_std = float(np.nanstd(test_arr)) if len(test_arr) > 0 and not np.all(np.isnan(test_arr)) else 0.0

        if psi < 0.1:
            status = "OK"
        elif psi < 0.25:
            status = "MODERATE"
        else:
            status = "HIGH_DRIFT"

        results.append({
            "feature": col,
            "psi": round(psi, 6),
            "status": status,
            "train_mean": round(train_mean, 6),
            "test_mean": round(test_mean, 6),
            "train_std": round(train_std, 6),
            "test_std": round(test_std, 6),
            "train_n": int(np.sum(~np.isnan(train_arr))) if len(train_arr) > 0 else 0,
            "test_n": int(np.sum(~np.isnan(test_arr))) if len(test_arr) > 0 else 0,
        })

    del train_vals, test_vals

    drift_df = pd.DataFrame(results)
    drift_df = drift_df.sort_values("psi", ascending=False)
    drift_df.to_csv(OUTPUT_CSV, index=False)

    n_ok = len(drift_df[drift_df["status"] == "OK"])
    n_moderate = len(drift_df[drift_df["status"] == "MODERATE"])
    n_high = len(drift_df[drift_df["status"] == "HIGH_DRIFT"])

    elapsed = time.time() - t0

    print("\n" + "=" * 60)
    print("DRIFT AUDIT SUMMARY")
    print("=" * 60)
    print(f"  Features analyzed  : {len(results):,}")
    print(f"  OK (PSI < 0.1)     : {n_ok:,}")
    print(f"  MODERATE (0.1-0.25): {n_moderate:,}")
    print(f"  HIGH DRIFT (>0.25) : {n_high:,}")
    if n_high > 0:
        print(f"\n  Top drifted features:")
        for _, row in drift_df[drift_df["status"] == "HIGH_DRIFT"].head(10).iterrows():
            print(f"    {row['feature']}: PSI={row['psi']:.4f}")
    print(f"\n  Output: {OUTPUT_CSV}")
    print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print("=" * 60)


if __name__ == "__main__":
    main()
