#!/usr/bin/env python3
"""Convert partants_master.jsonl (25 GB) to Parquet using DuckDB.

DuckDB handles heterogeneous JSON types natively and streams efficiently.
Much faster and more robust than manual PyArrow conversion.

Requires: pip install duckdb
"""
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: pip install duckdb")
    sys.exit(1)

INPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")


def main():
    t0 = time.perf_counter()
    tmp = OUTPUT.with_suffix(".tmp.parquet")

    con = duckdb.connect()
    # Set memory limit to stay safe
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=2")

    print("Reading JSONL and writing Parquet via DuckDB...", file=sys.stderr)
    print(f"  Input: {INPUT}", file=sys.stderr)
    print(f"  Output: {tmp}", file=sys.stderr)

    # DuckDB can read JSONL directly and write Parquet
    # Use ignore_errors and all_varchar to avoid type inference issues
    # Then cast types in a second step
    con.execute(f"""
        COPY (
            SELECT * FROM read_json_auto(
                '{INPUT.as_posix()}',
                format='newline_delimited',
                maximum_object_size=1048576,
                sample_size=50000,
                ignore_errors=true,
                union_by_name=true
            )
        ) TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
    """)

    con.close()

    # Rename
    if OUTPUT.exists():
        OUTPUT.unlink()
    tmp.rename(OUTPUT)

    elapsed = time.perf_counter() - t0
    in_size = INPUT.stat().st_size / 1e9
    out_size = OUTPUT.stat().st_size / 1e9

    print(f"\n{'='*60}")
    print(f"PARQUET CONVERSION COMPLETE")
    print(f"{'='*60}")
    print(f"Input: {in_size:.1f} GB (JSONL)")
    print(f"Output: {out_size:.1f} GB (Parquet)")
    print(f"Compression ratio: {in_size/out_size:.1f}x")
    print(f"Time: {elapsed:.0f}s")
    print(f"Output: {OUTPUT}")


if __name__ == "__main__":
    main()
