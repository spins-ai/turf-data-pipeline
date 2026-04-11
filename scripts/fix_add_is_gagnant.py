#!/usr/bin/env python3
"""
fix_add_is_gagnant.py -- Add missing comblage__is_gagnant target to features_selected.parquet
================================================================================================
Bug: apply_feature_selection.py excluded the binary target from the output file.
Fix: Read it from features_consolidated.parquet and inject it into features_selected.parquet.
"""

import time
import gc
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

SELECTED_PARQUET = Path("D:/turf-data-pipeline/04_FEATURES/features_selected.parquet")
CONSOLIDATED     = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
TMP_PARQUET      = Path("D:/turf-data-pipeline/04_FEATURES/features_selected_fix_tmp.parquet")

UID_COL          = "partant_uid"
TARGET_COL       = "comblage__is_gagnant"


def main() -> None:
    t0 = time.time()

    print("=" * 60)
    print("FIX: ADD comblage__is_gagnant TO features_selected.parquet")
    print("=" * 60)

    # Step 1: Build UID -> is_gagnant lookup from consolidated
    print("Loading is_gagnant from consolidated ...")
    pf_consol = pq.ParquetFile(str(CONSOLIDATED))
    src_cols = [UID_COL, TARGET_COL]
    src_cols = [c for c in src_cols if c in pf_consol.schema_arrow.names]

    if TARGET_COL not in pf_consol.schema_arrow.names:
        print(f"ERROR: {TARGET_COL} not found in consolidated parquet!")
        return

    chunks = []
    for rg_idx in range(pf_consol.metadata.num_row_groups):
        table = pf_consol.read_row_group(rg_idx, columns=src_cols)
        chunks.append(table)
        if (rg_idx + 1) % 10 == 0:
            print(f"  Read RG {rg_idx + 1}/{pf_consol.metadata.num_row_groups}")

    lookup_df = pa.concat_tables(chunks).to_pandas()
    del chunks, pf_consol

    # Build lookup: UID -> is_gagnant (as int8: 0/1)
    lookup = lookup_df.set_index(UID_COL)[TARGET_COL]
    n_winners = (lookup == 1).sum()
    print(f"  Loaded {len(lookup):,} runners, {n_winners:,} winners")
    del lookup_df

    # Step 2: Add is_gagnant to features_selected, streaming
    print("\nAdding comblage__is_gagnant column ...")
    pf_sel = pq.ParquetFile(str(SELECTED_PARQUET))
    n_rg = pf_sel.metadata.num_row_groups

    # Check it's not already there
    if TARGET_COL in pf_sel.schema_arrow.names:
        print(f"  {TARGET_COL} already exists in features_selected.parquet! Nothing to do.")
        return

    if TMP_PARQUET.exists():
        TMP_PARQUET.unlink()

    writer = None
    total_matched = 0
    for rg_idx in range(n_rg):
        table = pf_sel.read_row_group(rg_idx)
        uids = table.column(UID_COL).to_pandas()

        # Lookup is_gagnant values
        values = uids.map(lookup).values.astype(np.float64)
        matched = np.sum(~np.isnan(values))
        total_matched += int(matched)

        # Store as int8 (0/1) with NaN as null
        is_gagnant_col = pa.array(values, type=pa.float64())
        table = table.append_column(TARGET_COL, is_gagnant_col)

        if writer is None:
            writer = pq.ParquetWriter(str(TMP_PARQUET), table.schema, compression='snappy')
        writer.write_table(table)

        if (rg_idx + 1) % 10 == 0 or rg_idx == n_rg - 1:
            print(f"  RG {rg_idx + 1}/{n_rg} ({matched:,} matched)")
        del table

    writer.close()
    del pf_sel
    gc.collect()

    # Atomic rename
    SELECTED_PARQUET.unlink(missing_ok=True)
    TMP_PARQUET.rename(SELECTED_PARQUET)
    size_mb = SELECTED_PARQUET.stat().st_size / 1024 / 1024

    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print("FIX COMPLETE")
    print("=" * 60)
    print(f"  Column added    : {TARGET_COL}")
    print(f"  Rows matched    : {total_matched:,}")
    print(f"  Winners         : {n_winners:,}")
    print(f"  Output size     : {size_mb:.0f} MB")
    print(f"  Time            : {elapsed:.0f}s")
    print(f"  Output          : {SELECTED_PARQUET}")
    print("=" * 60)


if __name__ == "__main__":
    main()
