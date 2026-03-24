#!/usr/bin/env python3
"""
Convert main JSONL/CSV data files to a DuckDB database.

Creates data_master/turf_pipeline.duckdb with tables:
  - partants   (from data_master/partants_master.jsonl)
  - labels     (from output/labels/training_labels.jsonl)
  - elo_ratings (from output/elo_ratings/elo_ratings.jsonl)

Streams data in 100K-row chunks to keep RAM under 4 GB.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CHUNK_SIZE = 100_000  # rows per batch

ROOT = Path(__file__).resolve().parent.parent

SOURCES = [
    {
        "name": "partants",
        "path": ROOT / "data_master" / "partants_master.jsonl",
        "format": "jsonl",
    },
    {
        "name": "labels",
        "path": ROOT / "output" / "labels" / "training_labels.jsonl",
        # Despite the .jsonl extension this file is actually CSV
        "format": "csv",
    },
    {
        "name": "elo_ratings",
        "path": ROOT / "output" / "elo_ratings" / "elo_ratings.jsonl",
        "format": "jsonl",
    },
]

DB_PATH = ROOT / "data_master" / "turf_pipeline.duckdb"

INDEX_COLUMNS = ["partant_uid", "course_uid", "date_reunion_iso"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def iter_jsonl_chunks(path: Path, chunk_size: int):
    """Yield DataFrames of *chunk_size* rows read from a JSONL file."""
    buf: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            buf.append(json.loads(line))
            if len(buf) >= chunk_size:
                yield pd.DataFrame(buf)
                buf = []
    if buf:
        yield pd.DataFrame(buf)


def iter_csv_chunks(path: Path, chunk_size: int):
    """Yield DataFrames of *chunk_size* rows read from a CSV file."""
    reader = pd.read_csv(path, chunksize=chunk_size, low_memory=False)
    yield from reader


def import_table(
    con: duckdb.DuckDBPyConnection,
    name: str,
    path: Path,
    fmt: str,
) -> int:
    """Stream *path* into DuckDB table *name* and return total row count."""

    if not path.exists():
        print(f"  [SKIP] {path} not found")
        return 0

    iterator = (
        iter_jsonl_chunks(path, CHUNK_SIZE)
        if fmt == "jsonl"
        else iter_csv_chunks(path, CHUNK_SIZE)
    )

    total_rows = 0
    for chunk_idx, df in enumerate(iterator):
        if chunk_idx == 0:
            # Create or replace the table from the first chunk
            con.execute(f"DROP TABLE IF EXISTS {name}")
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM df")
        else:
            con.execute(f"INSERT INTO {name} SELECT * FROM df")
        total_rows += len(df)
        print(f"    chunk {chunk_idx}: +{len(df):>8,} rows  (total {total_rows:>10,})")

    return total_rows


def create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    """Create indexes on common join / filter columns where they exist."""
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    for table in tables:
        cols = {
            row[0]
            for row in con.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
            ).fetchall()
        }
        for col in INDEX_COLUMNS:
            if col in cols:
                idx_name = f"idx_{table}_{col}"
                con.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({col})")
                print(f"  index {idx_name}")


def print_summary(con: duckdb.DuckDBPyConnection, db_path: Path) -> None:
    """Print table sizes and total DB file size."""
    print("\n=== Database summary ===")
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    for table in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:20s}  {count:>12,} rows")
    size_mb = db_path.stat().st_size / (1024 * 1024)
    print(f"\n  DB file size: {size_mb:,.1f} MB  ({db_path})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print(f"DuckDB version {duckdb.__version__}")
    print(f"Output: {DB_PATH}\n")

    # Ensure parent directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Remove stale DB to start fresh
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))

    try:
        for src in SOURCES:
            t0 = time.perf_counter()
            print(f"[{src['name']}] importing {src['path']}")
            n = import_table(con, src["name"], src["path"], src["format"])
            elapsed = time.perf_counter() - t0
            print(f"  -> {n:,} rows in {elapsed:.1f}s\n")

        print("Creating indexes ...")
        create_indexes(con)

        # Checkpoint to flush WAL to the main DB file
        con.execute("CHECKPOINT")

        print_summary(con, DB_PATH)
    finally:
        con.close()


if __name__ == "__main__":
    main()
