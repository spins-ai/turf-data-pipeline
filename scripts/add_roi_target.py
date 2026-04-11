#!/usr/bin/env python3
"""
add_roi_target.py -- Add ROI target column to features_selected.parquet
=======================================================================
Creates target_roi column:
  - Winner:  exp(log_rapport_simple_gagnant) / 100 - 1
  - Loser:   -1.0
  - Missing: NaN

Reads from consolidated parquet for source data, adds to selected parquet.
"""

import sys
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

SELECTED_PARQUET = Path("D:/turf-data-pipeline/04_FEATURES/features_selected.parquet")
CONSOLIDATED     = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
OUTPUT_PARQUET   = Path("D:/turf-data-pipeline/04_FEATURES/features_selected.parquet")
TMP_PARQUET      = Path("D:/turf-data-pipeline/04_FEATURES/features_selected_roi_tmp.parquet")

UID_COL          = "partant_uid"
LOG_RAPPORT_COL  = "log_transforms__log_rapport_simple_gagnant"
IS_GAGNANT_COL   = "comblage__is_gagnant"


def main() -> None:
    t0 = time.time()

    print("=" * 60)
    print("ADD ROI TARGET")
    print("=" * 60)

    # Step 1: Build UID -> ROI lookup from consolidated parquet
    print("Loading ROI source data ...")
    pf_consol = pq.ParquetFile(str(CONSOLIDATED))
    src_cols = [UID_COL, IS_GAGNANT_COL, LOG_RAPPORT_COL]
    src_cols = [c for c in src_cols if c in pf_consol.schema_arrow.names]

    chunks = []
    for rg_idx in range(pf_consol.metadata.num_row_groups):
        table = pf_consol.read_row_group(rg_idx, columns=src_cols)
        chunks.append(table)
        if (rg_idx + 1) % 10 == 0:
            print(f"  Read RG {rg_idx + 1}/{pf_consol.metadata.num_row_groups}")

    roi_df = pa.concat_tables(chunks).to_pandas()
    del chunks, pf_consol

    # Compute ROI: winner = exp(log_rapport)/100 - 1, loser = -1
    roi_df["target_roi"] = np.where(
        roi_df[IS_GAGNANT_COL].astype(bool),
        np.exp(roi_df[LOG_RAPPORT_COL].astype(float)) / 100.0 - 1.0,
        -1.0
    )
    # Where is_gagnant is True but log_rapport is NaN, set ROI to NaN
    mask_winner_no_rap = roi_df[IS_GAGNANT_COL].astype(bool) & roi_df[LOG_RAPPORT_COL].isna()
    roi_df.loc[mask_winner_no_rap, "target_roi"] = np.nan

    roi_lookup = roi_df.set_index(UID_COL)["target_roi"]
    n_winners = roi_df[IS_GAGNANT_COL].astype(bool).sum()
    n_valid_roi = roi_df["target_roi"].notna().sum()
    print(f"  Loaded {len(roi_df):,} runners, {n_winners:,} winners, {n_valid_roi:,} with ROI")

    del roi_df

    # Step 2: Add target_roi to features_selected, streaming
    print("\nAdding target_roi column ...")
    pf_sel = pq.ParquetFile(str(SELECTED_PARQUET))
    n_rg = pf_sel.metadata.num_row_groups

    if TMP_PARQUET.exists():
        TMP_PARQUET.unlink()

    writer = None
    for rg_idx in range(n_rg):
        table = pf_sel.read_row_group(rg_idx)
        uids = table.column(UID_COL).to_pandas()

        # Lookup ROI values
        roi_values = uids.map(roi_lookup).values.astype(np.float64)
        roi_col = pa.array(roi_values, type=pa.float64())
        table = table.append_column("target_roi", roi_col)

        if writer is None:
            writer = pq.ParquetWriter(str(TMP_PARQUET), table.schema, compression='snappy')
        writer.write_table(table)

        if (rg_idx + 1) % 10 == 0 or rg_idx == n_rg - 1:
            print(f"  RG {rg_idx + 1}/{n_rg}")
        del table

    writer.close()
    del pf_sel

    # Atomic rename
    import gc; gc.collect()
    OUTPUT_PARQUET.unlink(missing_ok=True)
    TMP_PARQUET.rename(OUTPUT_PARQUET)
    size_mb = OUTPUT_PARQUET.stat().st_size / 1024 / 1024

    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print("ROI TARGET SUMMARY")
    print("=" * 60)
    print(f"  Winners         : {n_winners:,}")
    print(f"  With ROI        : {n_valid_roi:,}")
    print(f"  Output size     : {size_mb:.0f} MB")
    print(f"  Time            : {elapsed:.0f}s")
    print(f"  Output          : {OUTPUT_PARQUET}")
    print("=" * 60)


if __name__ == "__main__":
    main()
