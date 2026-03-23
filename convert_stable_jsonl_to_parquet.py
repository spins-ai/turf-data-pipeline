#!/usr/bin/env python3
"""
convert_stable_jsonl_to_parquet.py
===================================
Converts stable JSONL files (NOT features_matrix.jsonl) to Parquet with snappy compression.

Targets:
  - output/elo_ratings/elo_ratings.jsonl
  - output/recovery/recovery_features.jsonl  (if exists)
  - output/fatigue/fatigue_features.jsonl     (if exists)
  - output/labels/training_labels.jsonl       (SKIP if .parquet exists)
  - data_master/partants_master.jsonl

Streaming: 50,000 records per chunk to keep RAM < 4GB.
"""

import json
import os
import sys
import time
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERREUR: pyarrow non installe. Executer: pip install pyarrow")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERREUR: pandas non installe. Executer: pip install pandas")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent
CHUNK_SIZE = 50_000

# Files to convert: (jsonl_path, parquet_path)
TARGETS = [
    (BASE_DIR / "output" / "elo_ratings" / "elo_ratings.jsonl",
     BASE_DIR / "output" / "elo_ratings" / "elo_ratings.parquet"),
    (BASE_DIR / "output" / "recovery" / "recovery_features.jsonl",
     BASE_DIR / "output" / "recovery" / "recovery_features.parquet"),
    (BASE_DIR / "output" / "fatigue" / "fatigue_features.jsonl",
     BASE_DIR / "output" / "fatigue" / "fatigue_features.parquet"),
    (BASE_DIR / "output" / "labels" / "training_labels.jsonl",
     BASE_DIR / "output" / "labels" / "training_labels.parquet"),
    (BASE_DIR / "data_master" / "partants_master.jsonl",
     BASE_DIR / "data_master" / "partants_master.parquet"),
]


def sanitize_df_for_arrow(df):
    """Convert columns with mixed types (dict/list mixed with scalars) to JSON strings."""
    for col in df.columns:
        if df[col].dtype == object:
            mask = df[col].apply(lambda x: isinstance(x, (dict, list)))
            if mask.any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
                )
    return df


def align_table_to_schema(table, target_schema):
    """Align a table to match target schema: reorder columns, add missing as null, drop extra."""
    columns = []
    for field in target_schema:
        if field.name in table.schema.names:
            col = table.column(field.name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type)
                except (pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError):
                    col = pa.nulls(len(table), type=field.type)
            columns.append(col)
        else:
            columns.append(pa.nulls(len(table), type=field.type))
    return pa.table(columns, schema=target_schema)


def df_to_table_safe(df):
    """Convert DataFrame to Arrow table with fallback for mixed types."""
    sanitize_df_for_arrow(df)
    try:
        return pa.Table.from_pandas(df, preserve_index=False)
    except (pa.lib.ArrowTypeError, pa.lib.ArrowInvalid):
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].apply(
                    lambda x: json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (dict, list)) else
                    (str(x) if x is not None else None)
                )
        return pa.Table.from_pandas(df, preserve_index=False)


def convert_one_file(jsonl_path: Path, parquet_path: Path):
    """Convert a single JSONL file to Parquet with snappy compression, streaming."""
    fname = jsonl_path.name
    file_size = jsonl_path.stat().st_size
    file_size_mb = file_size / (1024**2)
    file_size_gb = file_size / (1024**3)

    size_str = f"{file_size_gb:.2f} GB" if file_size_gb >= 1.0 else f"{file_size_mb:.0f} MB"
    print(f"\n{'='*60}")
    print(f"  {jsonl_path.relative_to(BASE_DIR)}  ({size_str})")
    print(f"{'='*60}")

    t0 = time.time()

    parquet_tmp = parquet_path.with_suffix(".parquet.tmp")
    writer = None
    writer_schema = None
    total_rows = 0
    chunk_num = 0
    chunk_buffer = []
    bad_lines = 0

    with open(jsonl_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                chunk_buffer.append(rec)
            except json.JSONDecodeError:
                bad_lines += 1
                continue

            if len(chunk_buffer) >= CHUNK_SIZE:
                chunk_num += 1
                df = pd.DataFrame(chunk_buffer)
                table = df_to_table_safe(df)

                if writer is None:
                    writer_schema = table.schema
                    writer = pq.ParquetWriter(
                        str(parquet_tmp),
                        writer_schema,
                        compression="snappy",
                    )

                try:
                    aligned = align_table_to_schema(table, writer_schema)
                    writer.write_table(aligned)
                except (ValueError, pa.lib.ArrowInvalid) as e:
                    print(f"    [WARN] Schema align failed chunk {chunk_num}: {e}")

                total_rows += len(chunk_buffer)
                chunk_buffer = []
                del df, table

                elapsed = time.time() - t0
                rate = total_rows / elapsed if elapsed > 0 else 0
                print(f"    Chunk {chunk_num}: {total_rows:>12,} rows  [{rate:,.0f} rec/s]", flush=True)

    # Last chunk
    if chunk_buffer:
        chunk_num += 1
        df = pd.DataFrame(chunk_buffer)
        table = df_to_table_safe(df)

        if writer is None:
            writer_schema = table.schema
            writer = pq.ParquetWriter(
                str(parquet_tmp),
                writer_schema,
                compression="snappy",
            )

        try:
            aligned = align_table_to_schema(table, writer_schema)
            writer.write_table(aligned)
        except (ValueError, pa.lib.ArrowInvalid) as e:
            print(f"    [WARN] Schema align failed last chunk: {e}")

        total_rows += len(chunk_buffer)
        del df, table, chunk_buffer

    if writer is not None:
        writer.close()

    # Atomic replace
    if parquet_tmp.exists():
        os.replace(str(parquet_tmp), str(parquet_path))

    elapsed = time.time() - t0
    parquet_size = parquet_path.stat().st_size if parquet_path.exists() else 0
    parquet_size_mb = parquet_size / (1024**2)
    parquet_size_gb = parquet_size / (1024**3)
    ratio = file_size / parquet_size if parquet_size > 0 else 0

    out_str = f"{parquet_size_gb:.2f} GB" if parquet_size_gb >= 1.0 else f"{parquet_size_mb:.0f} MB"
    print(f"\n  Result: {jsonl_path.relative_to(BASE_DIR)}")
    print(f"    Input:    {size_str}")
    print(f"    Output:   {out_str}")
    print(f"    Ratio:    {ratio:.1f}x compression")
    print(f"    Rows:     {total_rows:,}")
    print(f"    Bad lines:{bad_lines:,}")
    print(f"    Time:     {elapsed:.1f}s ({elapsed/60:.1f} min)")

    return {
        "file": str(jsonl_path.relative_to(BASE_DIR)),
        "rows": total_rows,
        "input_size": file_size,
        "output_size": parquet_size,
        "ratio": round(ratio, 1),
        "elapsed_s": round(elapsed, 1),
    }


def main():
    t0 = time.time()
    print("=" * 60)
    print("CONVERT STABLE JSONL -> PARQUET (snappy)")
    print("=" * 60)

    results = []
    skipped = []

    for jsonl_path, parquet_path in TARGETS:
        if not jsonl_path.exists():
            print(f"\n  [SKIP] {jsonl_path.relative_to(BASE_DIR)} -- not found")
            skipped.append(str(jsonl_path.relative_to(BASE_DIR)))
            continue

        if parquet_path.exists():
            pq_size = parquet_path.stat().st_size
            if pq_size > 0:
                print(f"\n  [SKIP] {jsonl_path.relative_to(BASE_DIR)} -- .parquet already exists "
                      f"({pq_size / (1024**2):.0f} MB)")
                skipped.append(str(jsonl_path.relative_to(BASE_DIR)))
                continue

        result = convert_one_file(jsonl_path, parquet_path)
        results.append(result)

    # Final report
    elapsed_total = time.time() - t0
    print("\n" + "=" * 60)
    print("FINAL REPORT")
    print("=" * 60)

    if skipped:
        print(f"\nSkipped ({len(skipped)}):")
        for s in skipped:
            print(f"  - {s}")

    if results:
        print(f"\n{'File':<50s} {'Rows':>12s} {'Input':>10s} {'Output':>10s} {'Ratio':>7s}")
        print("-" * 92)

        total_in = 0
        total_out = 0
        total_rows = 0
        for r in results:
            in_str = f"{r['input_size']/(1024**3):.2f} GB" if r['input_size'] >= 1024**3 else f"{r['input_size']/(1024**2):.0f} MB"
            out_str = f"{r['output_size']/(1024**3):.2f} GB" if r['output_size'] >= 1024**3 else f"{r['output_size']/(1024**2):.0f} MB"
            print(f"  {r['file']:<48s} {r['rows']:>12,} {in_str:>10s} {out_str:>10s} {r['ratio']:>6.1f}x")
            total_in += r["input_size"]
            total_out += r["output_size"]
            total_rows += r["rows"]

        print("-" * 92)
        total_ratio = total_in / total_out if total_out > 0 else 0
        in_str = f"{total_in/(1024**3):.2f} GB"
        out_str = f"{total_out/(1024**3):.2f} GB"
        print(f"  {'TOTAL':<48s} {total_rows:>12,} {in_str:>10s} {out_str:>10s} {total_ratio:>6.1f}x")

    print(f"\nTotal time: {elapsed_total:.1f}s ({elapsed_total/60:.1f} min)")
    print("Done.")


if __name__ == "__main__":
    main()
