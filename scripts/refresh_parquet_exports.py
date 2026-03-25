#!/usr/bin/env python3
"""Refresh Parquet exports from stale JSONL masters.

Converts JSONL -> Parquet only when the JSONL is newer than the existing .parquet.
Uses chunked reading (50K rows) to stay under 4GB RAM.
Handles schema evolution across chunks by collecting all keys in pass 1,
then writing with a unified schema in pass 2.
"""

import os
import sys
import json
import time
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    sys.exit("pyarrow is required: pip install pyarrow")

ROOT = Path(__file__).resolve().parent.parent
CHUNK_SIZE = 50_000  # rows per chunk


def needs_refresh(jsonl_path: Path, parquet_path: Path) -> bool:
    """Return True if parquet is missing or older than jsonl."""
    if not parquet_path.exists():
        return True
    return jsonl_path.stat().st_mtime > parquet_path.stat().st_mtime


def discover_schema(jsonl_path: Path) -> list[str]:
    """Pass 1: Discover all unique keys across the entire JSONL file."""
    all_keys = set()
    count = 0
    print("  Pass 1: Discovering schema...")
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                all_keys.update(rec.keys())
            except json.JSONDecodeError:
                continue
            count += 1
            if count % 500_000 == 0:
                print(f"    Scanned {count:,} rows, {len(all_keys)} unique keys")
    print(f"    Total: {count:,} rows, {len(all_keys)} unique keys")
    return sorted(all_keys), count


def jsonl_to_parquet_chunked(jsonl_path: Path, parquet_path: Path):
    """Convert JSONL to Parquet using chunked reading with unified schema."""
    print(f"  Converting {jsonl_path.name} -> {parquet_path.name}")
    file_size_gb = jsonl_path.stat().st_size / (1024**3)
    print(f"  Source size: {file_size_gb:.2f} GB")

    start = time.time()

    # Pass 1: Discover all keys
    all_keys, expected_rows = discover_schema(jsonl_path)

    # Pass 2: Write with unified schema (all columns as string for safety,
    # then let downstream consumers cast as needed)
    print(f"  Pass 2: Writing Parquet with {len(all_keys)} columns...")
    writer = None
    total_rows = 0
    batch_lines = []

    tmp_path = parquet_path.with_suffix(".parquet.tmp")

    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                batch_lines.append(line)

                if len(batch_lines) >= CHUNK_SIZE:
                    table = _lines_to_table(batch_lines, all_keys)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            str(tmp_path),
                            table.schema,
                            compression="snappy",
                        )
                    writer.write_table(table)
                    total_rows += len(batch_lines)
                    elapsed = time.time() - start
                    pct = total_rows / expected_rows * 100 if expected_rows else 0
                    print(
                        f"    {total_rows:,} / {expected_rows:,} rows ({pct:.0f}%, {elapsed:.0f}s)",
                        end="\r",
                    )
                    batch_lines = []

        # Write remaining
        if batch_lines:
            table = _lines_to_table(batch_lines, all_keys)
            if writer is None:
                writer = pq.ParquetWriter(
                    str(tmp_path),
                    table.schema,
                    compression="snappy",
                )
            writer.write_table(table)
            total_rows += len(batch_lines)

    finally:
        if writer:
            writer.close()

    # Atomic rename
    if tmp_path.exists():
        if parquet_path.exists():
            parquet_path.unlink()
        tmp_path.rename(parquet_path)

    elapsed = time.time() - start
    out_size = parquet_path.stat().st_size / (1024**2)
    print(f"\n  Done: {total_rows:,} rows, {out_size:.1f} MB, {elapsed:.1f}s")
    return total_rows


def _lines_to_table(lines: list[str], all_keys: list[str]) -> pa.Table:
    """Convert a list of JSON lines to a PyArrow Table with a fixed set of keys."""
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if not records:
        return pa.table({k: pa.array([], type=pa.string()) for k in all_keys})

    # Build columns using all_keys for consistent schema
    columns = {}
    for key in all_keys:
        values = [r.get(key) for r in records]

        # Try native type inference first
        try:
            arr = pa.array(values)
            columns[key] = arr
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            # Fall back to string for mixed types
            columns[key] = pa.array(
                [json.dumps(v, ensure_ascii=False) if v is not None and not isinstance(v, str) else v for v in values]
            )

    return pa.table(columns)


def main():
    files_to_convert = [
        (
            ROOT / "data_master" / "partants_master.jsonl",
            ROOT / "data_master" / "partants_master.parquet",
        ),
        (
            ROOT / "output" / "labels" / "training_labels.jsonl",
            ROOT / "output" / "labels" / "training_labels.parquet",
        ),
        (
            ROOT / "output" / "elo_ratings" / "elo_ratings.jsonl",
            ROOT / "output" / "elo_ratings" / "elo_ratings.parquet",
        ),
    ]

    print("=" * 60)
    print("Parquet Export Refresh")
    print("=" * 60)

    converted = 0
    skipped = 0

    for jsonl_path, parquet_path in files_to_convert:
        print(f"\nChecking: {jsonl_path.name}")

        if not jsonl_path.exists():
            print(f"  SKIP: {jsonl_path} does not exist")
            skipped += 1
            continue

        if not needs_refresh(jsonl_path, parquet_path):
            jt = jsonl_path.stat().st_mtime
            pt = parquet_path.stat().st_mtime
            print(f"  SKIP: parquet is up to date (jsonl={jt:.0f}, parquet={pt:.0f})")
            skipped += 1
            continue

        try:
            jsonl_to_parquet_chunked(jsonl_path, parquet_path)
            converted += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'=' * 60}")
    print(f"Summary: {converted} converted, {skipped} skipped")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
