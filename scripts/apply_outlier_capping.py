"""
apply_outlier_capping.py
------------------------
Clips extreme feature values using pre-computed thresholds.
Uses PyArrow for memory-safe streaming (reads/writes row-group by row-group).

Input  : D:/turf-data-pipeline/04_FEATURES/features_imputed.parquet
Thresholds: D:/turf-data-pipeline/04_FEATURES/outlier_capping_thresholds.csv
Output : D:/turf-data-pipeline/04_FEATURES/features_capped.parquet
"""

import sys
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
THRESHOLDS_CSV = "D:/turf-data-pipeline/04_FEATURES/outlier_capping_thresholds.csv"
INPUT_PARQUET  = "D:/turf-data-pipeline/04_FEATURES/features_imputed.parquet"
OUTPUT_PARQUET = "D:/turf-data-pipeline/04_FEATURES/features_capped.parquet"
TMP_OUTPUT     = "D:/turf-data-pipeline/04_FEATURES/features_capped_tmp.parquet"

OUTLIER_PCT_MIN = 1.0  # only cap columns where >1% of values are outliers

# ---------------------------------------------------------------------------
# 1. Read thresholds
# ---------------------------------------------------------------------------
print("Reading thresholds CSV ...")
thresholds = pd.read_csv(THRESHOLDS_CSV)

required_cols = {"builder", "feature", "outlier_pct", "cap_low", "cap_high"}
missing = required_cols - set(thresholds.columns)
if missing:
    print(f"ERROR: thresholds CSV is missing columns: {missing}")
    sys.exit(1)

thresholds["col_name"] = thresholds["builder"] + "__" + thresholds["feature"]

to_cap = thresholds[thresholds["outlier_pct"] > OUTLIER_PCT_MIN].copy()
print(f"  Total threshold rows : {len(thresholds):,}")
print(f"  Features to cap (outlier_pct > {OUTLIER_PCT_MIN}%) : {len(to_cap):,}")

# ---------------------------------------------------------------------------
# 2. Read Parquet schema, build cap_map
# ---------------------------------------------------------------------------
print("\nReading Parquet schema ...")
pf = pq.ParquetFile(INPUT_PARQUET)
schema = pf.schema_arrow
parquet_cols = set(schema.names)
n_row_groups = pf.metadata.num_row_groups
total_rows = pf.metadata.num_rows
print(f"  Columns: {len(parquet_cols):,}, Row groups: {n_row_groups}, Rows: {total_rows:,}")

# Exclude BOOLEAN columns
bool_cols = set()
for i, field in enumerate(schema):
    if pa.types.is_boolean(field.type):
        bool_cols.add(field.name)

eligible = to_cap[to_cap["col_name"].isin(parquet_cols) & ~to_cap["col_name"].isin(bool_cols)].copy()
skipped = to_cap[~to_cap["col_name"].isin(parquet_cols)]

if not skipped.empty:
    print(f"  WARNING: {len(skipped)} threshold rows reference columns not in Parquet")

cap_map = {}
for _, row in eligible.iterrows():
    low = row["cap_low"] if pd.notna(row["cap_low"]) else None
    high = row["cap_high"] if pd.notna(row["cap_high"]) else None
    cap_map[row["col_name"]] = (low, high)

print(f"  Eligible columns to cap : {len(cap_map):,}")

# ---------------------------------------------------------------------------
# 3. Stream row-group by row-group, apply capping, write output
# ---------------------------------------------------------------------------
print(f"\nProcessing {n_row_groups} row groups ...")
Path(TMP_OUTPUT).parent.mkdir(parents=True, exist_ok=True)
if Path(TMP_OUTPUT).exists():
    Path(TMP_OUTPUT).unlink()

writer = None
total_clipped = 0

for rg_idx in range(n_row_groups):
    table = pf.read_row_group(rg_idx)

    # Convert capped columns to numpy, clip, replace in table
    for col_name, (low, high) in cap_map.items():
        if col_name not in table.column_names:
            continue

        col_idx = table.column_names.index(col_name)
        arr = table.column(col_idx)

        # Convert to numpy (float64) for clipping
        np_arr = arr.to_numpy(zero_copy_only=False).astype(np.float64)

        # Count values that will be clipped (before clipping)
        mask_valid = ~np.isnan(np_arr)
        clipped_count = 0
        if low is not None:
            clipped_count += np.sum(mask_valid & (np_arr < low))
        if high is not None:
            clipped_count += np.sum(mask_valid & (np_arr > high))
        total_clipped += int(clipped_count)

        # Apply clipping
        if low is not None:
            np_arr = np.where(mask_valid & (np_arr < low), low, np_arr)
        if high is not None:
            np_arr = np.where(mask_valid & (np_arr > high), high, np_arr)

        # Replace column in table
        new_col = pa.array(np_arr, type=pa.float64(), from_pandas=True)
        table = table.set_column(col_idx, col_name, new_col)

    # Write row group
    if writer is None:
        writer = pq.ParquetWriter(TMP_OUTPUT, table.schema, compression='snappy')
    writer.write_table(table)

    if (rg_idx + 1) % 5 == 0 or rg_idx == n_row_groups - 1:
        print(f"  Row group {rg_idx + 1}/{n_row_groups} done")

writer.close()
print("  Write complete.")

# Move temp to final output
print(f"\nMoving to {OUTPUT_PARQUET} ...")
Path(OUTPUT_PARQUET).unlink(missing_ok=True)
Path(TMP_OUTPUT).rename(OUTPUT_PARQUET)

# ---------------------------------------------------------------------------
# 4. Summary
# ---------------------------------------------------------------------------
size_mb = Path(OUTPUT_PARQUET).stat().st_size / 1024 / 1024

print("\n" + "=" * 60)
print("OUTLIER CAPPING SUMMARY")
print("=" * 60)
print(f"  Input rows              : {total_rows:,}")
print(f"  Total columns in file   : {len(parquet_cols):,}")
print(f"  Threshold rows loaded   : {len(thresholds):,}")
print(f"  Features eligible (>1%) : {len(eligible):,}")
print(f"  Features actually capped: {len(cap_map):,}")
print(f"  Individual values clipped: {total_clipped:,}")
print(f"  Output size             : {size_mb:.1f} MB")
print(f"  Output Parquet          : {OUTPUT_PARQUET}")
print("=" * 60)
