#!/usr/bin/env python3
"""Export existing DuckDB consolidation (2509 cols) to Parquet,
then patch in the 2 missing chunks (chunk_010 + chunk_011) separately.

Strategy:
  Step 1: Open existing persistent DB, export 2509-col table to Parquet
  Step 2: Add missing chunks one at a time using higher memory (24GB)
  Step 3: Final export
"""
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: pip install duckdb")
    sys.exit(1)

DB_PATH = Path("D:/turf-data-pipeline/tmp/consolidation/consolidation.duckdb")
CHUNK_DIR = Path("D:/turf-data-pipeline/tmp/consolidation/chunks")
OUTPUT = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
TMP_DIR = Path("D:/turf-data-pipeline/tmp/consolidation/duckdb_tmp")

# Chunks that failed OOM (chunk_010 = chunk 11, chunk_011 = chunk 12)
MISSING_CHUNKS = ["chunk_010.parquet", "chunk_011.parquet"]


def step1_export_base():
    """Export the existing 2509-column table from the persistent DB."""
    print("=== STEP 1: Export existing DB to Parquet ===", file=sys.stderr)

    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}", file=sys.stderr)
        return False

    # Clean up tmp files from crashed session
    for f in TMP_DIR.glob("duckdb_temp_storage_*"):
        print(f"  Cleaning up temp file: {f.name}", file=sys.stderr)
        f.unlink()

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        n_rows = con.execute("SELECT COUNT(*) FROM features").fetchone()[0]
        n_cols = len(con.execute("DESCRIBE features").fetchall())
        print(f"  Table: {n_rows:,} rows, {n_cols} columns", file=sys.stderr)

        base_pq = OUTPUT.with_name("features_base_2509.parquet")
        base_pq.parent.mkdir(parents=True, exist_ok=True)
        tmp = base_pq.with_suffix(".tmp.parquet")

        print(f"  Exporting to {base_pq.name}...", file=sys.stderr)
        con.execute(f"""
            COPY features TO '{tmp.as_posix()}'
            (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
        """)
        con.close()

        if base_pq.exists():
            base_pq.unlink()
        tmp.rename(base_pq)

        sz = base_pq.stat().st_size / 1e9
        print(f"  Exported: {sz:.1f} GB", file=sys.stderr)
        return True
    except Exception as e:
        print(f"  ERROR: {e}", file=sys.stderr)
        con.close()
        return False


def step2_patch_missing_chunks():
    """Add missing chunk columns one at a time with 24GB memory."""
    print("\n=== STEP 2: Patch missing chunks ===", file=sys.stderr)

    base_pq = OUTPUT.with_name("features_base_2509.parquet")
    if not base_pq.exists():
        print(f"ERROR: Base parquet not found: {base_pq}", file=sys.stderr)
        return False

    current = base_pq

    for chunk_name in MISSING_CHUNKS:
        chunk_path = CHUNK_DIR / chunk_name
        if not chunk_path.exists():
            print(f"  SKIP {chunk_name}: not found", file=sys.stderr)
            continue

        print(f"  Adding {chunk_name}...", file=sys.stderr)

        # Use a fresh persistent DB for each patch to manage memory
        patch_db = TMP_DIR / "patch.duckdb"
        if patch_db.exists():
            patch_db.unlink()

        con = duckdb.connect(str(patch_db))
        con.execute("SET memory_limit='24GB'")
        con.execute("SET threads=2")  # fewer threads = less memory
        con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")
        con.execute("SET preserve_insertion_order=false")

        try:
            # Load base into persistent DB
            con.execute(f"""
                CREATE TABLE features AS
                SELECT * FROM read_parquet('{current.as_posix()}')
            """)

            n_cols_before = len(con.execute("DESCRIBE features").fetchall())

            # Add chunk columns
            con.execute(f"""
                CREATE OR REPLACE TABLE features AS
                SELECT f.*, c.* EXCLUDE (partant_uid)
                FROM features f
                LEFT JOIN read_parquet('{chunk_path.as_posix()}') c
                ON f.partant_uid = c.partant_uid
            """)

            n_cols_after = len(con.execute("DESCRIBE features").fetchall())
            print(f"  {n_cols_before} -> {n_cols_after} columns", file=sys.stderr)

            # Export
            out_name = f"features_patched_{chunk_name.replace('.parquet', '')}.parquet"
            out_pq = OUTPUT.parent / out_name
            tmp = out_pq.with_suffix(".tmp.parquet")
            con.execute(f"""
                COPY features TO '{tmp.as_posix()}'
                (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
            """)
            con.close()

            if out_pq.exists():
                out_pq.unlink()
            tmp.rename(out_pq)

            current = out_pq
            sz = out_pq.stat().st_size / 1e9
            print(f"  Output: {out_pq.name} ({sz:.1f} GB)", file=sys.stderr)

        except Exception as e:
            print(f"  ERROR patching {chunk_name}: {e}", file=sys.stderr)
            try:
                con.close()
            except Exception:
                pass
            # Continue with what we have
            continue
        finally:
            if patch_db.exists():
                try:
                    patch_db.unlink()
                except Exception:
                    pass

    # Rename final result to the official output
    if current != base_pq:
        if OUTPUT.exists():
            OUTPUT.unlink()
        import shutil
        shutil.copy2(str(current), str(OUTPUT))
        print(f"\n  Final output: {OUTPUT}", file=sys.stderr)
    else:
        # No patches applied, just rename base
        if OUTPUT.exists():
            OUTPUT.unlink()
        import shutil
        shutil.copy2(str(current), str(OUTPUT))

    return True


def main():
    t0 = time.perf_counter()
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    if not step1_export_base():
        print("FAILED at step 1", file=sys.stderr)
        sys.exit(1)

    if not step2_patch_missing_chunks():
        print("FAILED at step 2", file=sys.stderr)
        sys.exit(1)

    # Final stats
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    n_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUTPUT.as_posix()}')").fetchone()[0]
    n_cols = len(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{OUTPUT.as_posix()}')").fetchall())
    con.close()

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

    # Cleanup intermediate files
    for f in OUTPUT.parent.glob("features_base_*.parquet"):
        f.unlink()
    for f in OUTPUT.parent.glob("features_patched_*.parquet"):
        if f != OUTPUT:
            f.unlink()


if __name__ == "__main__":
    main()
