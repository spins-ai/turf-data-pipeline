#!/usr/bin/env python3
"""Remove high-drift features from the selected Parquet.

Reads data_drift_audit.csv, drops all HIGH_DRIFT features,
writes a new features_selected.parquet.
"""
import sys
import time
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

INPUT_PARQUET  = Path("D:/turf-data-pipeline/04_FEATURES/features_selected.parquet")
DRIFT_CSV      = Path("D:/turf-data-pipeline/04_FEATURES/data_drift_audit.csv")
OUTPUT_PARQUET = Path("D:/turf-data-pipeline/04_FEATURES/features_selected.parquet")
TMP_PARQUET    = Path("D:/turf-data-pipeline/04_FEATURES/features_selected_nodrift_tmp.parquet")

def main():
    t0 = time.time()

    drift_df = pd.read_csv(DRIFT_CSV)
    high_drift = set(drift_df[drift_df["status"] == "HIGH_DRIFT"]["feature"].tolist())
    print(f"High-drift features to remove: {len(high_drift)}")

    pf = pq.ParquetFile(str(INPUT_PARQUET))
    all_cols = pf.schema_arrow.names
    retained = [c for c in all_cols if c not in high_drift]
    n_rg = pf.metadata.num_row_groups

    print(f"Input cols: {len(all_cols)}, Retained: {len(retained)}, Dropped: {len(all_cols) - len(retained)}")

    if TMP_PARQUET.exists():
        TMP_PARQUET.unlink()

    writer = None
    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=retained)
        if writer is None:
            writer = pq.ParquetWriter(str(TMP_PARQUET), table.schema, compression='snappy')
        writer.write_table(table)
        if (rg_idx + 1) % 10 == 0 or rg_idx == n_rg - 1:
            print(f"  RG {rg_idx + 1}/{n_rg}")
        del table

    writer.close()
    del pf  # release file handle on Windows

    # Replace original
    import gc; gc.collect()
    OUTPUT_PARQUET.unlink(missing_ok=True)
    TMP_PARQUET.rename(OUTPUT_PARQUET)
    size_mb = OUTPUT_PARQUET.stat().st_size / 1024 / 1024

    elapsed = time.time() - t0
    print(f"\nDone: {OUTPUT_PARQUET} ({size_mb:.0f} MB, {len(retained)} cols, {elapsed:.0f}s)")

if __name__ == "__main__":
    main()
