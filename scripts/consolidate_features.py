#!/usr/bin/env python3
"""Consolidate all 297 builder JSONL outputs into a single Parquet file.

Strategy (memory-safe, fast):
  Phase 1: Convert each JSONL to individual Parquet (chunked reads)
  Phase 2: Use DuckDB to join all Parquet files on partant_uid
           DuckDB handles disk-spilling automatically for large joins.

Output: D:/turf-data-pipeline/04_FEATURES/features_all_builders.parquet
"""
from __future__ import annotations
import gc, json, os, sys, time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import duckdb

BUILDER_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
PARQUET_STAGING = Path("D:/turf-data-pipeline/04_FEATURES/staging_parquet")
OUTPUT_FINAL = Path("D:/turf-data-pipeline/04_FEATURES/features_all_builders.parquet")
CHUNK_SIZE = 200_000


def find_all_jsonl():
    files = []
    for root, dirs, fnames in os.walk(BUILDER_DIR):
        for f in fnames:
            if f.endswith('.jsonl') and not f.endswith('.tmp'):
                files.append(Path(root) / f)
    return sorted(files)


def jsonl_to_parquet(jsonl_path: Path, parquet_path: Path):
    """Convert a single JSONL file to Parquet, chunked."""
    chunks = []
    current_chunk = []

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            rec = json.loads(line)
            current_chunk.append(rec)
            if len(current_chunk) >= CHUNK_SIZE:
                df = pd.DataFrame(current_chunk)
                chunks.append(pa.Table.from_pandas(df, preserve_index=False))
                current_chunk = []
                del df

    if current_chunk:
        df = pd.DataFrame(current_chunk)
        chunks.append(pa.Table.from_pandas(df, preserve_index=False))
        del df

    if not chunks:
        return False

    # Unify schemas across chunks (some chunks may have different columns)
    if len(chunks) > 1:
        all_fields = {}
        for tbl in chunks:
            for field in tbl.schema:
                if field.name not in all_fields:
                    all_fields[field.name] = field.type
        unified_chunks = []
        for tbl in chunks:
            for fname, ftype in all_fields.items():
                if fname not in tbl.schema.names:
                    tbl = tbl.append_column(fname, pa.nulls(len(tbl), type=ftype))
            tbl = tbl.select(list(all_fields.keys()))
            unified_chunks.append(tbl)
        table = pa.concat_tables(unified_chunks)
    else:
        table = chunks[0]

    pq.write_table(table, parquet_path, compression='zstd')
    del table, chunks
    gc.collect()
    return True


def phase1_convert_all():
    """Phase 1: Convert each JSONL to individual Parquet."""
    PARQUET_STAGING.mkdir(parents=True, exist_ok=True)
    jsonl_files = find_all_jsonl()
    print(f"Found {len(jsonl_files)} JSONL files to convert")

    converted = 0
    skipped = 0
    errors = []

    for i, jp in enumerate(jsonl_files):
        builder_name = jp.parent.name
        parquet_name = f"{builder_name}__{jp.stem}.parquet"
        parquet_path = PARQUET_STAGING / parquet_name

        # Skip if already converted and newer than source
        if parquet_path.exists() and parquet_path.stat().st_size > 0:
            if parquet_path.stat().st_mtime > jp.stat().st_mtime:
                skipped += 1
                continue

        t0 = time.perf_counter()
        try:
            jsonl_to_parquet(jp, parquet_path)
            elapsed = time.perf_counter() - t0
            size_mb = parquet_path.stat().st_size / 1024 / 1024
            converted += 1
            print(f"  [{i+1}/{len(jsonl_files)}] {builder_name}: {size_mb:.0f} MB ({elapsed:.0f}s)")
        except Exception as e:
            errors.append((builder_name, str(e)))
            print(f"  [{i+1}/{len(jsonl_files)}] ERROR {builder_name}: {e}")

        if (i + 1) % 20 == 0:
            gc.collect()

    print(f"\nPhase 1: {converted} converted, {skipped} skipped, {len(errors)} errors")
    if errors:
        for name, err in errors:
            print(f"  FAILED: {name}: {err}")


def phase2_merge_duckdb():
    """Phase 2: Merge all Parquets into one using DuckDB.

    DuckDB handles:
    - Memory management (disk spilling when needed)
    - Efficient hash joins on partant_uid
    - Parallel execution
    """
    parquet_files = sorted(PARQUET_STAGING.glob("*.parquet"))
    # Filter out intermediate merge files
    parquet_files = [f for f in parquet_files if not f.name.startswith("_merged")]
    print(f"\nPhase 2: Merging {len(parquet_files)} Parquet files via DuckDB")

    # Configure DuckDB for heavy workload
    con = duckdb.connect()
    con.execute("SET memory_limit='40GB'")
    con.execute("SET threads=6")
    con.execute("SET temp_directory='D:/turf-data-pipeline/04_FEATURES/duckdb_tmp'")
    os.makedirs("D:/turf-data-pipeline/04_FEATURES/duckdb_tmp", exist_ok=True)

    # Step 1: Register all parquet files as views
    print("  Registering files...")
    view_names = []
    col_registry = {}  # track column -> first view that has it

    for i, pf in enumerate(parquet_files):
        vname = f"v{i:03d}"
        con.execute(f"CREATE VIEW {vname} AS SELECT * FROM read_parquet('{pf.as_posix()}')")
        view_names.append(vname)

        # Get columns
        cols = con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{vname}'").fetchall()
        col_names = [c[0] for c in cols]
        for c in col_names:
            if c != 'partant_uid' and c not in col_registry:
                col_registry[c] = vname

    print(f"  {len(view_names)} views, {len(col_registry)} unique feature columns")

    # Step 2: Iterative merge in batches of 10 views
    # DuckDB can handle this but we batch to avoid enormous SQL queries
    BATCH = 10
    current_base = view_names[0]

    merge_round = 0
    i = 1  # start from second view
    while i < len(view_names):
        batch_views = view_names[i:i + BATCH]
        merge_round += 1

        # Build JOIN query
        # Get columns for each view (excluding partant_uid)
        select_parts = [f"{current_base}.*"]
        join_parts = []

        for vn in batch_views:
            cols = con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{vn}' AND column_name != 'partant_uid'"
            ).fetchall()
            for (c,) in cols:
                select_parts.append(f"{vn}.{c}")
            join_parts.append(
                f"LEFT JOIN {vn} ON {current_base}.partant_uid = {vn}.partant_uid"
            )

        merged_name = f"merged_{merge_round:03d}"
        sql = f"""
            CREATE TABLE {merged_name} AS
            SELECT {', '.join(select_parts)}
            FROM {current_base}
            {' '.join(join_parts)}
        """

        t0 = time.perf_counter()
        con.execute(sql)
        elapsed = time.perf_counter() - t0

        row_count = con.execute(f"SELECT COUNT(*) FROM {merged_name}").fetchone()[0]
        col_count = len(con.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{merged_name}'"
        ).fetchall())

        print(f"  Round {merge_round}: joined {len(batch_views)} views -> "
              f"{row_count:,} rows x {col_count} cols ({elapsed:.0f}s)")

        # Drop previous base if it was a merge table
        if current_base.startswith("merged_"):
            con.execute(f"DROP TABLE {current_base}")

        current_base = merged_name
        i += BATCH
        gc.collect()

    # Step 3: Export final table to Parquet
    print(f"\n  Exporting final table to Parquet...")
    t0 = time.perf_counter()
    con.execute(f"""
        COPY {current_base} TO '{OUTPUT_FINAL.as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)
    elapsed = time.perf_counter() - t0
    final_size = OUTPUT_FINAL.stat().st_size / 1024 / 1024

    # Get final stats
    final_rows = con.execute(f"SELECT COUNT(*) FROM {current_base}").fetchone()[0]
    final_cols = len(con.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{current_base}'"
    ).fetchall())

    print(f"  DONE: {final_rows:,} rows x {final_cols} columns")
    print(f"  File: {OUTPUT_FINAL} ({final_size:.0f} MB)")
    print(f"  Export time: {elapsed:.0f}s")

    con.close()


def main():
    t0 = time.perf_counter()

    print("=" * 60)
    print("PHASE 1: Convert JSONL -> Individual Parquet")
    print("=" * 60)
    phase1_convert_all()

    print("\n" + "=" * 60)
    print("PHASE 2: Merge all Parquet -> Single file (DuckDB)")
    print("=" * 60)
    phase2_merge_duckdb()

    elapsed = time.perf_counter() - t0
    print(f"\n{'='*60}")
    print(f"TOTAL TIME: {elapsed/60:.0f} minutes")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
