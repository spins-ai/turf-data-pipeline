#!/usr/bin/env python3
"""Patch chunk_010 into the consolidated Parquet.

The consolidated Parquet currently has 2659 columns (chunks 000-009 + 011).
chunk_010 (~200 columns) failed due to a power outage. This script adds it.
"""
import shutil
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: pip install duckdb")
    sys.exit(1)

CONSOLIDATED = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
CHUNK_010 = Path("D:/turf-data-pipeline/tmp/consolidation/chunks/chunk_010.parquet")
TMP_DIR = Path("D:/turf-data-pipeline/tmp/consolidation/duckdb_tmp")
DB_PATH = TMP_DIR / "patch010.duckdb"


def main():
    t0 = time.perf_counter()
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    if not CONSOLIDATED.exists():
        print(f"ERROR: {CONSOLIDATED} not found", file=sys.stderr)
        sys.exit(1)
    if not CHUNK_010.exists():
        print(f"ERROR: {CHUNK_010} not found", file=sys.stderr)
        sys.exit(1)

    # Clean up old files
    if DB_PATH.exists():
        DB_PATH.unlink()
    for f in TMP_DIR.glob("duckdb_temp_storage_*"):
        f.unlink()

    con = duckdb.connect(str(DB_PATH))
    con.execute("SET memory_limit='24GB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")

    # Load consolidated into persistent DB
    print("Loading consolidated Parquet into DB...", file=sys.stderr)
    con.execute(f"""
        CREATE TABLE features AS
        SELECT * FROM read_parquet('{CONSOLIDATED.as_posix()}')
    """)
    n_before = len(con.execute("DESCRIBE features").fetchall())
    print(f"  Loaded: {n_before} columns", file=sys.stderr)

    # Add chunk_010
    print(f"Adding chunk_010...", file=sys.stderr)
    con.execute(f"""
        CREATE OR REPLACE TABLE features AS
        SELECT f.*, c.* EXCLUDE (partant_uid)
        FROM features f
        LEFT JOIN read_parquet('{CHUNK_010.as_posix()}') c
        ON f.partant_uid = c.partant_uid
    """)
    n_after = len(con.execute("DESCRIBE features").fetchall())
    print(f"  {n_before} -> {n_after} columns (+{n_after - n_before})", file=sys.stderr)

    # Export
    n_rows = con.execute("SELECT COUNT(*) FROM features").fetchone()[0]
    print(f"Exporting {n_rows:,} rows x {n_after} cols...", file=sys.stderr)

    tmp_out = CONSOLIDATED.with_suffix(".tmp.parquet")
    con.execute(f"""
        COPY features TO '{tmp_out.as_posix()}'
        (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
    """)
    con.close()

    # Swap
    backup = CONSOLIDATED.with_name("features_consolidated_2659cols.parquet")
    shutil.copy2(str(CONSOLIDATED), str(backup))
    CONSOLIDATED.unlink()
    tmp_out.rename(CONSOLIDATED)

    # Cleanup
    if DB_PATH.exists():
        DB_PATH.unlink()

    elapsed = time.perf_counter() - t0
    out_size = CONSOLIDATED.stat().st_size / 1e9
    print(f"\n{'='*60}")
    print(f"PATCH COMPLETE")
    print(f"{'='*60}")
    print(f"Records: {n_rows:,}")
    print(f"Columns: {n_after}")
    print(f"Output size: {out_size:.1f} GB")
    print(f"Time: {elapsed:.0f}s")
    print(f"Backup: {backup}")


if __name__ == "__main__":
    main()
