#!/usr/bin/env python3
"""Phase 3: Merge 12 chunk Parquets into final consolidated Parquet.

Strategy: Use a persistent DuckDB database on disk.
- Create table from first chunk
- For each subsequent chunk, add columns via ALTER TABLE + UPDATE
- This avoids wide JOINs that explode memory/time
- Final COPY to Parquet

Requires phases 1-2 to have completed (chunk Parquets in WORK_DIR/chunks/).
"""
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: pip install duckdb")
    sys.exit(1)

CHUNK_DIR = Path("D:/turf-data-pipeline/tmp/consolidation/chunks")
OUTPUT = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
DB_PATH = Path("D:/turf-data-pipeline/tmp/consolidation/consolidation.duckdb")
TMP_DIR = Path("D:/turf-data-pipeline/tmp/consolidation/duckdb_tmp")


def main():
    t0 = time.perf_counter()
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Find chunk Parquets
    chunks = sorted(CHUNK_DIR.glob("chunk_*.parquet"))
    chunks = [c for c in chunks if c.stat().st_size > 0 and ".tmp" not in c.name]
    print(f"Found {len(chunks)} chunk Parquets", file=sys.stderr)
    if not chunks:
        print("ERROR: No chunks found!", file=sys.stderr)
        sys.exit(1)

    # Remove old DB if exists (fresh start)
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))
    con.execute("SET memory_limit='16GB'")
    con.execute("SET threads=4")
    con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")

    # Step 1: Create base table from first chunk
    print(f"Loading chunk 1/{len(chunks)}: {chunks[0].name}...", file=sys.stderr)
    con.execute(f"""
        CREATE TABLE features AS
        SELECT * FROM read_parquet('{chunks[0].as_posix()}')
    """)
    n_cols = len(con.execute("DESCRIBE features").fetchall())
    print(f"  Base: {n_cols} columns", file=sys.stderr)

    # Step 2: For each remaining chunk, add columns via JOIN into new table
    # Using CREATE OR REPLACE TABLE is more efficient than ALTER TABLE + UPDATE
    # because DuckDB can optimize the whole operation
    for i, chunk_path in enumerate(chunks[1:], 2):
        print(f"Adding chunk {i}/{len(chunks)}: {chunk_path.name}...", file=sys.stderr)
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE features AS
                SELECT f.*, c.* EXCLUDE (partant_uid)
                FROM features f
                LEFT JOIN read_parquet('{chunk_path.as_posix()}') c
                ON f.partant_uid = c.partant_uid
            """)
            n_cols = len(con.execute("DESCRIBE features").fetchall())
            print(f"  Now: {n_cols} columns", file=sys.stderr)
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            # Try to continue
            continue

    # Step 3: Export to Parquet
    n_rows = con.execute("SELECT COUNT(*) FROM features").fetchone()[0]
    n_cols = len(con.execute("DESCRIBE features").fetchall())
    print(f"\nWriting final Parquet ({n_rows:,} rows, {n_cols} columns)...", file=sys.stderr)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUTPUT.with_suffix(".tmp.parquet")
    con.execute(f"""
        COPY features TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
    """)

    con.close()

    if OUTPUT.exists():
        OUTPUT.unlink()
    tmp.rename(OUTPUT)

    # Clean up DB
    if DB_PATH.exists():
        DB_PATH.unlink()

    elapsed = time.perf_counter() - t0
    out_size = OUTPUT.stat().st_size / 1e9

    print(f"\n{'='*60}")
    print(f"CONSOLIDATION COMPLETE")
    print(f"{'='*60}")
    print(f"Records: {n_rows:,}")
    print(f"Columns: {n_cols}")
    print(f"Output size: {out_size:.1f} GB")
    print(f"Time: {elapsed:.0f}s")
    print(f"Output: {OUTPUT}")


if __name__ == "__main__":
    main()
