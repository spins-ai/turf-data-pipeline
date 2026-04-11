#!/usr/bin/env python3
"""Create a permanent DuckDB database index for fast querying of consolidated features.

Reads the consolidated Parquet, imports it into a DuckDB database,
and creates indexes on key columns for fast lookups.

Usage: python scripts/create_duckdb_index.py
"""
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

# Input: prefer consolidated, fall back to base
PARQUET_PRIMARY = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
PARQUET_FALLBACK = Path("D:/turf-data-pipeline/04_FEATURES/features_base_2509.parquet")

# Output DuckDB database
DB_PATH = Path("D:/turf-data-pipeline/04_FEATURES/features.duckdb")


def main() -> None:
    # Resolve input file
    if PARQUET_PRIMARY.exists():
        parquet_path = PARQUET_PRIMARY
    elif PARQUET_FALLBACK.exists():
        parquet_path = PARQUET_FALLBACK
    else:
        print(f"ERROR: No Parquet file found.")
        print(f"  Tried: {PARQUET_PRIMARY}")
        print(f"  Tried: {PARQUET_FALLBACK}")
        sys.exit(1)

    print(f"Source Parquet: {parquet_path}")
    print(f"Target DuckDB:  {DB_PATH}")

    # Remove existing DB to start fresh
    if DB_PATH.exists():
        print("Removing existing database...")
        DB_PATH.unlink()

    t0 = time.perf_counter()

    # Connect with resource limits
    con = duckdb.connect(str(DB_PATH))
    con.execute("SET memory_limit='16GB'")
    con.execute("SET threads=2")
    con.execute("SET temp_directory='D:/turf-data-pipeline/tmp/duckdb_tmp'")

    # Import Parquet into features table
    print("Importing Parquet into features table...")
    con.execute(f"""
        CREATE TABLE features AS
        SELECT * FROM read_parquet('{parquet_path.as_posix()}')
    """)

    # Get table stats
    row_count: int = con.execute("SELECT COUNT(*) FROM features").fetchone()[0]
    col_count: int = len(con.execute("DESCRIBE features").fetchall())

    # Get available column names
    columns: list[str] = [
        row[0] for row in con.execute("DESCRIBE features").fetchall()
    ]

    # Create indexes on key columns
    print("Creating indexes...")

    if "partant_uid" in columns:
        con.execute("CREATE INDEX idx_partant_uid ON features(partant_uid)")
        print("  Index created: partant_uid")
    else:
        print("  SKIP: partant_uid column not found")

    if "course_uid" in columns:
        con.execute("CREATE INDEX idx_course_uid ON features(course_uid)")
        print("  Index created: course_uid")
    else:
        print("  SKIP: course_uid column not found")

    con.close()

    elapsed = time.perf_counter() - t0
    db_size_mb = DB_PATH.stat().st_size / 1e6

    # Print final stats
    print()
    print("=" * 50)
    print("DuckDB index created successfully")
    print("=" * 50)
    print(f"Rows:    {row_count:,}")
    print(f"Columns: {col_count:,}")
    print(f"DB size: {db_size_mb:.1f} MB")
    print(f"Time:    {elapsed:.1f}s")
    print(f"Path:    {DB_PATH}")


if __name__ == "__main__":
    main()
