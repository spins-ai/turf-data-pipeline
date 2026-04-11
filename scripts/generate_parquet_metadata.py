"""
Generate column-level metadata for features_consolidated.parquet.
Writes column_metadata.csv with stats per column, plus a summary.
"""

import duckdb
import csv
import os
import math
from pathlib import Path

INPUT_FILE = "D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet"
OUTPUT_CSV = "D:/turf-data-pipeline/04_FEATURES/column_metadata.csv"
BATCH_SIZE = 50  # columns per batch to avoid memory issues with 2800+ cols


def get_file_size_mb(path: str) -> float:
    size_bytes = os.path.getsize(path)
    return round(size_bytes / (1024 * 1024), 2)


def main():
    print(f"Connecting to DuckDB...")
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=2")

    print(f"Reading schema from: {INPUT_FILE}")
    schema_rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{INPUT_FILE}') LIMIT 0"
    ).fetchall()

    columns = [(row[0], row[1]) for row in schema_rows]
    total_cols = len(columns)
    print(f"Found {total_cols} columns")

    # Get total row count once
    total_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{INPUT_FILE}')"
    ).fetchone()[0]
    print(f"Total rows: {total_rows:,}")

    file_size_mb = get_file_size_mb(INPUT_FILE)

    # Categorise columns by type
    numeric_types = {"INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT",
                     "FLOAT", "DOUBLE", "DECIMAL", "REAL"}
    string_types = {"VARCHAR", "TEXT", "CHAR", "STRING"}

    def is_numeric(dtype: str) -> bool:
        return any(dtype.upper().startswith(t) for t in numeric_types)

    def is_string(dtype: str) -> bool:
        return any(dtype.upper().startswith(t) for t in string_types)

    numeric_col_count = sum(1 for _, dt in columns if is_numeric(dt))
    string_col_count = sum(1 for _, dt in columns if is_string(dt))

    print(f"Numeric columns: {numeric_col_count}, String columns: {string_col_count}")
    print(f"File size: {file_size_mb} MB")

    # Process columns in batches
    results = []
    total_batches = math.ceil(total_cols / BATCH_SIZE)

    for batch_idx in range(total_batches):
        batch = columns[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
        print(f"Processing batch {batch_idx + 1}/{total_batches} "
              f"({len(batch)} columns)...")

        for col_name, col_type in batch:
            safe_col = f'"{col_name}"'
            row = {
                "name": col_name,
                "type": col_type,
                "null_count": None,
                "null_pct": None,
                "min": None,
                "max": None,
                "mean": None,
                "std": None,
                "distinct_count": None,
            }

            try:
                # Null count (all types)
                null_count = con.execute(
                    f"SELECT COUNT(*) - COUNT({safe_col}) "
                    f"FROM read_parquet('{INPUT_FILE}')"
                ).fetchone()[0]
                row["null_count"] = null_count
                row["null_pct"] = round(null_count / total_rows * 100, 4) if total_rows > 0 else 0.0

                if is_numeric(col_type):
                    stats = con.execute(
                        f"SELECT MIN({safe_col}), MAX({safe_col}), "
                        f"AVG({safe_col}), STDDEV({safe_col}) "
                        f"FROM read_parquet('{INPUT_FILE}')"
                    ).fetchone()
                    row["min"] = stats[0]
                    row["max"] = stats[1]
                    row["mean"] = round(float(stats[2]), 6) if stats[2] is not None else None
                    row["std"] = round(float(stats[3]), 6) if stats[3] is not None else None

                elif is_string(col_type):
                    stats = con.execute(
                        f"SELECT MIN({safe_col}), MAX({safe_col}), "
                        f"COUNT(DISTINCT {safe_col}) "
                        f"FROM read_parquet('{INPUT_FILE}')"
                    ).fetchone()
                    row["min"] = stats[0]
                    row["max"] = stats[1]
                    row["distinct_count"] = stats[2]

                else:
                    # Boolean, date, etc. — just min/max
                    stats = con.execute(
                        f"SELECT MIN({safe_col}), MAX({safe_col}) "
                        f"FROM read_parquet('{INPUT_FILE}')"
                    ).fetchone()
                    row["min"] = stats[0]
                    row["max"] = stats[1]

            except Exception as e:
                print(f"  WARNING: could not compute stats for column '{col_name}': {e}")

            results.append(row)

    # Write column metadata CSV
    fieldnames = ["name", "type", "null_count", "null_pct",
                  "min", "max", "mean", "std", "distinct_count"]

    print(f"\nWriting column metadata to: {OUTPUT_CSV}")
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # Write summary alongside the CSV
    summary_path = OUTPUT_CSV.replace("column_metadata.csv", "column_metadata_summary.csv")
    print(f"Writing summary to: {summary_path}")
    summary = {
        "total_rows": total_rows,
        "total_cols": total_cols,
        "file_size_mb": file_size_mb,
        "numeric_cols": numeric_col_count,
        "string_cols": string_col_count,
        "other_cols": total_cols - numeric_col_count - string_col_count,
    }
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)

    con.close()

    print("\nDone.")
    print(f"  Columns processed : {len(results)}")
    print(f"  Output CSV        : {OUTPUT_CSV}")
    print(f"  Summary CSV       : {summary_path}")


if __name__ == "__main__":
    main()
