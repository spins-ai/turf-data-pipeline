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

    # For large files with many keys (high schema variance), force all-string
    # to avoid cross-chunk type mismatches.
    force_string = len(all_keys) > 500
    if force_string:
        print(f"  NOTE: {len(all_keys)} columns detected — using all-string mode for schema safety")

    # Build a fixed schema upfront so all chunks match
    schema = pa.schema([(k, pa.string()) for k in all_keys]) if force_string else None

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
                    table = _lines_to_table(batch_lines, all_keys, force_string=force_string)
                    if writer is None:
                        write_schema = schema if schema else table.schema
                        writer = pq.ParquetWriter(
                            str(tmp_path),
                            write_schema,
                            compression="snappy",
                        )
                    if not force_string:
                        table = _reconcile_schema(table, writer.schema)
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
            table = _lines_to_table(batch_lines, all_keys, force_string=force_string)
            if writer is None:
                write_schema = schema if schema else table.schema
                writer = pq.ParquetWriter(
                    str(tmp_path),
                    write_schema,
                    compression="snappy",
                )
            if not force_string:
                table = _reconcile_schema(table, writer.schema)
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


def _reconcile_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Cast table columns to match target_schema.  When a column type differs
    (e.g. list<int> vs list<string>), serialize the column to JSON strings."""
    if table.schema.equals(target_schema):
        return table

    new_columns = {}
    for field in target_schema:
        col = table.column(field.name)
        if col.type != field.type:
            # Try cast first, then fall back to JSON string serialization
            try:
                col = col.cast(field.type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
                # Serialize both to string — but target type wins
                str_values = [
                    json.dumps(v.as_py(), ensure_ascii=False) if v.as_py() is not None else None
                    for v in col
                ]
                col = pa.array(str_values, type=pa.string())
                if field.type != pa.string():
                    # Target schema had a complex type on chunk 1; can't fix.
                    # This is a fundamental mismatch — skip gracefully.
                    pass
        new_columns[field.name] = col

    return pa.table(new_columns)


def _lines_to_table(lines: list[str], all_keys: list[str], force_string: bool = False) -> pa.Table:
    """Convert a list of JSON lines to a PyArrow Table with a fixed set of keys.
    Lists and dicts are serialized to JSON strings for schema stability across chunks."""
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if not records:
        return pa.table({k: pa.array([], type=pa.string()) for k in all_keys})

    def _to_str(v):
        if v is None:
            return None
        if isinstance(v, str):
            return v
        return json.dumps(v, ensure_ascii=False)

    # Build columns using all_keys for consistent schema
    columns = {}
    for key in all_keys:
        values = [r.get(key) for r in records]

        if force_string:
            columns[key] = pa.array([_to_str(v) for v in values], type=pa.string())
            continue

        # Check if any value is a list or dict — serialize to JSON string for stability
        has_complex = any(isinstance(v, (list, dict)) for v in values if v is not None)
        if has_complex:
            columns[key] = pa.array([_to_str(v) for v in values])
            continue

        # Try native type inference for scalar columns
        try:
            arr = pa.array(values)
            columns[key] = arr
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            columns[key] = pa.array([_to_str(v) for v in values])

    return pa.table(columns)


def auto_discover_stale(output_dir: Path) -> list[tuple[Path, Path]]:
    """Walk output/ and find every .jsonl that has a corresponding .parquet
    where the jsonl is newer (or parquet is missing)."""
    stale = []
    for jsonl_path in output_dir.rglob("*.jsonl"):
        parquet_path = jsonl_path.with_suffix(".parquet")
        if parquet_path.exists() and needs_refresh(jsonl_path, parquet_path):
            stale.append((jsonl_path, parquet_path))
    stale.sort(key=lambda pair: pair[0].stat().st_size)  # smallest first
    return stale


def main():
    # --- Explicit high-priority files ---
    files_to_convert = [
        (
            ROOT / "output" / "labels" / "training_labels.jsonl",
            ROOT / "output" / "labels" / "training_labels.parquet",
        ),
        (
            ROOT / "output" / "elo_ratings" / "elo_ratings.jsonl",
            ROOT / "output" / "elo_ratings" / "elo_ratings.parquet",
        ),
    ]

    # --- Auto-discover any other stale parquets ---
    explicit_jsonls = {p[0] for p in files_to_convert}
    auto_stale = auto_discover_stale(ROOT / "output")
    for jsonl_path, parquet_path in auto_stale:
        if jsonl_path not in explicit_jsonls:
            files_to_convert.append((jsonl_path, parquet_path))

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
