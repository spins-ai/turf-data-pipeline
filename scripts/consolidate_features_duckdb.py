#!/usr/bin/env python3
"""Consolidate all builder outputs into one Parquet using DuckDB.

Strategy (memory-safe, chunked):
  Phase 1: Convert each builder JSONL → individual Parquet (with prefixed columns)
  Phase 2: Join builders in chunks of CHUNK_SIZE → intermediate Parquets
  Phase 3: Join all intermediates → final consolidated Parquet

This avoids the RAM explosion from iterative LEFT JOINs on one huge table.
"""
import csv
import shutil
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: pip install duckdb")
    sys.exit(1)

BASE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
DROP_CSV = Path("D:/turf-data-pipeline/04_FEATURES/features_to_drop.csv")
OUTPUT = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
MASTER_PARQUET = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
WORK_DIR = Path("D:/turf-data-pipeline/tmp/consolidation")
CHUNK_SIZE = 25  # builders per intermediate Parquet


def _load_drop_features() -> set[str]:
    drops = set()
    if DROP_CSV.exists():
        with open(DROP_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                drops.add(row["feature_to_drop"])
    return drops


def _fresh_connection():
    """Create a fresh DuckDB connection with conservative memory settings."""
    con = duckdb.connect()
    con.execute("SET memory_limit='16GB'")
    con.execute("SET threads=4")
    con.execute(f"SET temp_directory='{(WORK_DIR / 'duckdb_tmp').as_posix()}'")
    return con


def phase1_convert_builders(drop_set: set[str]) -> list[tuple[str, Path]]:
    """Convert each builder JSONL to individual Parquet with prefixed columns.

    Returns list of (builder_name, parquet_path) for successfully converted builders.
    """
    print("\n=== PHASE 1: Convert builders to individual Parquets ===", file=sys.stderr)
    parquet_dir = WORK_DIR / "individual"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    builders = sorted(d for d in BASE.iterdir() if d.is_dir())
    results = []
    skipped = 0

    for i, bdir in enumerate(builders):
        jsonls = [f for f in bdir.iterdir() if f.suffix == ".jsonl" and ".tmp" not in f.name]
        if not jsonls:
            continue
        fpath = jsonls[0]
        if fpath.stat().st_size < 10_000:
            continue

        builder_name = bdir.name
        out_pq = parquet_dir / f"{builder_name}.parquet"

        # Skip if already converted (resume support)
        if out_pq.exists() and out_pq.stat().st_size > 0:
            results.append((builder_name, out_pq))
            continue

        con = _fresh_connection()
        try:
            # Read JSONL
            con.execute(f"""
                CREATE TEMP TABLE raw AS
                SELECT * FROM read_json_auto(
                    '{fpath.as_posix()}',
                    format='newline_delimited',
                    sample_size=5000,
                    ignore_errors=true
                )
            """)

            # Get feature columns (exclude join keys)
            cols = con.execute("DESCRIBE raw").fetchall()
            feature_cols = []
            for col_name, col_type, *_ in cols:
                if col_name in ("partant_uid", "course_uid", "date_reunion_iso", "date_reunion"):
                    continue
                full_key = f"{builder_name}/{col_name}"
                if full_key in drop_set:
                    continue
                feature_cols.append(col_name)

            if not feature_cols:
                skipped += 1
                con.close()
                continue

            # Write Parquet with prefixed column names
            renamed = ', '.join(
                f'"{c}" AS "{builder_name}__{c}"' for c in feature_cols
            )
            tmp_pq = out_pq.with_suffix(".tmp.parquet")
            con.execute(f"""
                COPY (
                    SELECT partant_uid, {renamed} FROM raw
                ) TO '{tmp_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)
            con.close()

            tmp_pq.rename(out_pq)
            results.append((builder_name, out_pq))

        except Exception as e:
            print(f"  SKIP {builder_name}: {e}", file=sys.stderr)
            skipped += 1
            try:
                con.close()
            except Exception:
                pass
            continue

        if (len(results) + skipped) % 50 == 0:
            print(f"  {len(results) + skipped}/{len(builders)} done ({len(results)} ok, {skipped} skipped)", file=sys.stderr)

    print(f"  Phase 1 done: {len(results)} builders converted, {skipped} skipped", file=sys.stderr)
    return results


def phase2_chunk_joins(builder_parquets: list[tuple[str, Path]]) -> list[Path]:
    """Join builders in chunks → intermediate Parquets.

    Each chunk: load CHUNK_SIZE builder Parquets, LEFT JOIN on partant_uid, write result.
    """
    print(f"\n=== PHASE 2: Join in chunks of {CHUNK_SIZE} ===", file=sys.stderr)
    chunk_dir = WORK_DIR / "chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)

    chunks = []
    for i in range(0, len(builder_parquets), CHUNK_SIZE):
        chunks.append(builder_parquets[i:i + CHUNK_SIZE])

    intermediates = []

    for ci, chunk in enumerate(chunks):
        out_pq = chunk_dir / f"chunk_{ci:03d}.parquet"

        # Skip if already done (resume support)
        if out_pq.exists() and out_pq.stat().st_size > 0:
            intermediates.append(out_pq)
            print(f"  Chunk {ci+1}/{len(chunks)}: already done", file=sys.stderr)
            continue

        con = _fresh_connection()
        try:
            # Start with master UIDs
            con.execute(f"""
                CREATE TEMP TABLE result AS
                SELECT partant_uid FROM read_parquet('{MASTER_PARQUET.as_posix()}')
            """)

            # Join each builder in this chunk
            for builder_name, pq_path in chunk:
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE result AS
                    SELECT r.*, b.* EXCLUDE (partant_uid)
                    FROM result r
                    LEFT JOIN read_parquet('{pq_path.as_posix()}') b
                    ON r.partant_uid = b.partant_uid
                """)

            # Write chunk result
            tmp_pq = out_pq.with_suffix(".tmp.parquet")
            con.execute(f"""
                COPY result TO '{tmp_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
            """)
            con.close()
            tmp_pq.rename(out_pq)
            intermediates.append(out_pq)

            n_cols = len(chunk) * 5  # rough estimate
            print(f"  Chunk {ci+1}/{len(chunks)}: {len(chunk)} builders joined", file=sys.stderr)

        except Exception as e:
            print(f"  ERROR chunk {ci}: {e}", file=sys.stderr)
            try:
                con.close()
            except Exception:
                pass
            continue

    print(f"  Phase 2 done: {len(intermediates)} chunk Parquets", file=sys.stderr)
    return intermediates


def _merge_pair(left: Path, right: Path, out: Path):
    """Merge two Parquet files by partant_uid into a new Parquet."""
    con = _fresh_connection()
    con.execute("SET preserve_insertion_order=false")
    try:
        tmp = out.with_suffix(".tmp.parquet")
        con.execute(f"""
            COPY (
                SELECT l.*, r.* EXCLUDE (partant_uid)
                FROM read_parquet('{left.as_posix()}') l
                LEFT JOIN read_parquet('{right.as_posix()}') r
                ON l.partant_uid = r.partant_uid
            ) TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
        """)
        con.close()
        tmp.rename(out)
    except Exception as e:
        try:
            con.close()
        except Exception:
            pass
        raise e


def phase3_final_merge(intermediates: list[Path]):
    """Merge all intermediate chunk Parquets via binary tree (pair-wise).

    Each merge reads 2 Parquets, joins, writes result — never builds huge in-memory table.
    """
    print(f"\n=== PHASE 3: Binary tree merge ({len(intermediates)} chunks) ===", file=sys.stderr)

    if not intermediates:
        print("ERROR: No intermediates to merge!", file=sys.stderr)
        return

    merge_dir = WORK_DIR / "merges"
    merge_dir.mkdir(parents=True, exist_ok=True)

    current_level = list(intermediates)
    level = 0

    while len(current_level) > 1:
        next_level = []
        level += 1
        print(f"  Level {level}: merging {len(current_level)} files...", file=sys.stderr)

        for i in range(0, len(current_level), 2):
            if i + 1 < len(current_level):
                out_pq = merge_dir / f"level{level}_pair{i//2}.parquet"
                # Resume support
                if out_pq.exists() and out_pq.stat().st_size > 0:
                    print(f"    Pair {i//2}: already done", file=sys.stderr)
                    next_level.append(out_pq)
                    continue
                print(f"    Merging pair {i//2}: {current_level[i].name} + {current_level[i+1].name}", file=sys.stderr)
                _merge_pair(current_level[i], current_level[i+1], out_pq)
                next_level.append(out_pq)
            else:
                # Odd one out, carry forward
                next_level.append(current_level[i])

        current_level = next_level

    # The last file is our result — get stats and move to OUTPUT
    final_pq = current_level[0]
    con = _fresh_connection()
    n_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{final_pq.as_posix()}')").fetchone()[0]
    n_cols = len(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{final_pq.as_posix()}')").fetchall())
    con.close()

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT.exists():
        OUTPUT.unlink()
    shutil.copy2(str(final_pq), str(OUTPUT))

    return n_rows, n_cols


def main():
    t0 = time.perf_counter()

    # Setup
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    (WORK_DIR / "duckdb_tmp").mkdir(parents=True, exist_ok=True)

    drop_set = _load_drop_features()
    print(f"Features to drop: {len(drop_set)}", file=sys.stderr)

    # Phase 1: Individual Parquets
    builder_parquets = phase1_convert_builders(drop_set)

    # Phase 2: Chunk joins
    intermediates = phase2_chunk_joins(builder_parquets)

    # Phase 3: Final merge
    result = phase3_final_merge(intermediates)
    if result is None:
        print("FAILED: Could not produce final Parquet", file=sys.stderr)
        sys.exit(1)

    n_rows, n_cols = result
    elapsed = time.perf_counter() - t0
    out_size = OUTPUT.stat().st_size / 1e9

    print(f"\n{'='*60}")
    print(f"CONSOLIDATION COMPLETE")
    print(f"{'='*60}")
    print(f"Records: {n_rows:,}")
    print(f"Columns: {n_cols}")
    print(f"Builders: {len(builder_parquets)}")
    print(f"Output size: {out_size:.1f} GB")
    print(f"Time: {elapsed:.0f}s")
    print(f"Output: {OUTPUT}")

    # Cleanup work dir
    print(f"\nCleanup work dir? (keeping for now)", file=sys.stderr)


if __name__ == "__main__":
    main()
