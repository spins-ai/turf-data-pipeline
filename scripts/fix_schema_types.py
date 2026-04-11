#!/usr/bin/env python3
"""Fix schema type issues in the consolidated Parquet.

Issues addressed (from schema_consistency_audit.csv):
  - mixed_types with empty_string: columns that mix '' and real strings
      -> replace '' with NULL
  - mixed_types with numeric_string: columns that mix strings and numeric strings
      -> if dominant type is numeric, cast to DOUBLE; otherwise keep as VARCHAR
  - empty_string (standalone): columns where '' should be NULL
      -> replace '' with NULL

The audit CSV has columns: builder, issue_type, key, detail
We work on the *consolidated* Parquet (one unified table), so we deduplicate
column names across builders and apply each fix once.

Strategy:
  1. Parse audit CSV -> collect unique columns to fix and their fix type
  2. Open consolidated Parquet in DuckDB (streaming, memory-safe)
  3. Build a SELECT that applies the appropriate CASE/CAST for each flagged column
  4. Write to a temp file, then atomic rename over the original
"""

import csv
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: pip install duckdb")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PARQUET_PRIMARY = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
PARQUET_FALLBACK = Path("D:/turf-data-pipeline/04_FEATURES/features_base_2509.parquet")
AUDIT_CSV = Path("D:/turf-data-pipeline/04_FEATURES/schema_consistency_audit.csv")
TMP_DIR = Path("D:/turf-data-pipeline/tmp")

TMP_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Parse audit CSV
# ---------------------------------------------------------------------------

def _parse_detail(detail: str) -> dict[str, int]:
    """Parse 'Mixed types: {'string': 184, 'numeric_string': 27}' -> dict."""
    counts: dict[str, int] = {}
    for m in re.finditer(r"'(\w+)':\s*(\d+)", detail):
        counts[m.group(1)] = int(m.group(2))
    return counts


def load_audit(audit_csv: Path) -> tuple[dict[str, str], set[str]]:
    """Return:
      mixed_numeric  : {col: 'cast_double' | 'keep_string'}  — mixed_types with numeric_string
      empty_cols     : set of column names where '' -> NULL
    """
    mixed_numeric: dict[str, str] = {}
    empty_cols: set[str] = set()

    with open(audit_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            issue = row["issue_type"].strip()
            col = row["key"].strip()
            detail = row.get("detail", "").strip()

            if issue == "empty_string":
                empty_cols.add(col)

            elif issue == "mixed_types":
                counts = _parse_detail(detail)
                has_numeric_str = "numeric_string" in counts
                has_empty_str = "empty_string" in counts

                # Always mark empty_string presence for NULL replacement
                if has_empty_str:
                    empty_cols.add(col)

                # Decide whether to cast to DOUBLE
                if has_numeric_str:
                    numeric_count = counts.get("numeric_string", 0)
                    string_count = counts.get("string", 0)
                    # Cast to DOUBLE only when numeric values dominate
                    # and there are no real (non-numeric) strings
                    if string_count == 0:
                        # Pure numeric strings -> safe to cast
                        mixed_numeric[col] = "cast_double"
                    elif numeric_count > string_count:
                        # Numeric majority but some real strings -> keep string,
                        # just note ambiguity (column is already VARCHAR in Parquet)
                        mixed_numeric[col] = "keep_string"
                    else:
                        mixed_numeric[col] = "keep_string"

    return mixed_numeric, empty_cols


# ---------------------------------------------------------------------------
# Build the fixing SELECT
# ---------------------------------------------------------------------------

def _quote(name: str) -> str:
    """Double-quote a column name for DuckDB."""
    return f'"{name}"'


def build_select(
    all_columns: list[str],
    col_types: dict[str, str],  # col -> duckdb type string
    mixed_numeric: dict[str, str],
    empty_cols: set[str],
) -> str:
    """Build SELECT clause that applies fixes for flagged columns only."""
    parts: list[str] = []

    for col in all_columns:
        qcol = _quote(col)
        dtype = col_types.get(col, "").upper()

        fix_empty = col in empty_cols
        fix_numeric = col in mixed_numeric and mixed_numeric[col] == "cast_double"

        if fix_numeric and fix_empty:
            # Replace '' with NULL then try to cast to DOUBLE
            expr = (
                f"TRY_CAST(NULLIF(TRIM({qcol}), '') AS DOUBLE) AS {qcol}"
            )
        elif fix_numeric:
            expr = f"TRY_CAST({qcol} AS DOUBLE) AS {qcol}"
        elif fix_empty:
            # Only replace empty strings; preserve existing NULLs
            if "VARCHAR" in dtype or "TEXT" in dtype or "CHAR" in dtype or dtype == "":
                expr = f"NULLIF(TRIM({qcol}), '') AS {qcol}"
            else:
                # Non-string column with empty_string issue: leave as-is
                # (shouldn't happen, but be safe)
                expr = qcol
        else:
            expr = qcol

        parts.append(f"    {expr}")

    return ",\n".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # Resolve Parquet path with fallback
    if PARQUET_PRIMARY.exists():
        PARQUET = PARQUET_PRIMARY
    elif PARQUET_FALLBACK.exists():
        print(f"WARNING: Primary Parquet not found, using fallback: {PARQUET_FALLBACK}")
        PARQUET = PARQUET_FALLBACK
    else:
        print(f"ERROR: Neither Parquet found:\n  {PARQUET_PRIMARY}\n  {PARQUET_FALLBACK}")
        sys.exit(1)

    if not AUDIT_CSV.exists():
        print(f"ERROR: Audit CSV not found: {AUDIT_CSV}")
        sys.exit(1)

    print("=== fix_schema_types.py ===")
    print(f"Input : {PARQUET}")
    print(f"Audit : {AUDIT_CSV}")

    # 1. Parse audit
    print("\n[1/4] Parsing schema audit CSV ...")
    mixed_numeric, empty_cols = load_audit(AUDIT_CSV)
    print(f"  Columns to cast -> DOUBLE      : {len([v for v in mixed_numeric.values() if v == 'cast_double'])}")
    print(f"  Columns to keep as string     : {len([v for v in mixed_numeric.values() if v == 'keep_string'])}")
    print(f"  Columns with empty->NULL fix   : {len(empty_cols)}")

    # 2. Connect DuckDB
    print("\n[2/4] Connecting DuckDB ...")
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='16GB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")

    # 3. Inspect actual columns in the Parquet
    print("\n[3/4] Inspecting Parquet schema ...")
    schema_rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{PARQUET.as_posix()}')"
    ).fetchall()
    all_columns = [r[0] for r in schema_rows]
    col_types = {r[0]: r[1] for r in schema_rows}
    print(f"  Total columns in Parquet: {len(all_columns)}")

    # Filter fixes to only columns that actually exist in the Parquet
    mixed_numeric_present = {c: v for c, v in mixed_numeric.items() if c in col_types}
    empty_cols_present = {c for c in empty_cols if c in col_types}

    n_double = len([v for v in mixed_numeric_present.values() if v == "cast_double"])
    n_empty = len(empty_cols_present)
    print(f"  Applying cast->DOUBLE on {n_double} columns")
    print(f"  Applying empty->NULL  on {n_empty} columns")

    if n_double == 0 and n_empty == 0:
        print("\nNo fixes needed. Parquet is already clean.")
        con.close()
        return

    # 4. Build & execute fixing query
    print("\n[4/4] Writing fixed Parquet ...")
    select_clause = build_select(all_columns, col_types, mixed_numeric_present, empty_cols_present)

    tmp_path = PARQUET.parent / f"_fix_tmp_{os.getpid()}.parquet"
    sql = (
        f"COPY (\n"
        f"  SELECT\n{select_clause}\n"
        f"  FROM read_parquet('{PARQUET.as_posix()}')\n"
        f") TO '{tmp_path.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)"
    )

    print(f"  Writing to temp: {tmp_path.name}")
    con.execute(sql)
    con.close()

    # Atomic rename
    print(f"  Renaming -> {PARQUET.name}")
    tmp_path.replace(PARQUET)

    size_mb = PARQUET.stat().st_size / 1024 / 1024
    print(f"\nDone. Fixed Parquet: {PARQUET} ({size_mb:.1f} MB)")

    # Summary
    print("\n--- Fix Summary ---")
    if mixed_numeric_present:
        print("Cast to DOUBLE:")
        for col, action in sorted(mixed_numeric_present.items()):
            if action == "cast_double":
                print(f"  + {col}")
        print("Kept as VARCHAR (had mixed real strings):")
        for col, action in sorted(mixed_numeric_present.items()):
            if action == "keep_string":
                print(f"  ~ {col}")
    if empty_cols_present:
        print("Empty string -> NULL:")
        for col in sorted(empty_cols_present):
            print(f"  ~ {col}")


if __name__ == "__main__":
    main()
