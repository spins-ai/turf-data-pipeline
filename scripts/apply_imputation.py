#!/usr/bin/env python3
"""Apply NaN imputation rules to the consolidated Parquet file.

Strategy (memory-safe, DuckDB-based):
  1. Inspect column names + types from the consolidated Parquet
  2. Classify each column into an imputation bucket:
       - _count / _nb_ / nb_courses  -> fill with 0
       - _ratio / _pct / _rate       -> fill with 0
       - _elo                        -> fill with 1500
       - _streak                     -> fill with 0
       - boolean/flag (0/1 only)     -> fill with 0
       - other numeric               -> fill with train-set median
       - non-numeric / excluded      -> leave as-is
  3. Compute train-set medians for "other numeric" columns using only
     records whose uid appears in train_uids.txt  (no data leakage)
  4. Write the imputed Parquet to the output path

Imputation respects the IMPUTATION_STRATEGY.md principles:
  - Categorical columns are NOT imputed (left as NaN for tree models)
  - Fill rate < 20% columns are NOT imputed with median (left as NaN)
  - Targets are never imputed
"""

import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed.  Run: pip install duckdb")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
INPUT = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
OUTPUT = Path("D:/turf-data-pipeline/04_FEATURES/features_imputed.parquet")
TRAIN_UIDS = Path("D:/turf-data-pipeline/04_FEATURES/splits/train_uids.txt")
TMP_DIR = Path("D:/turf-data-pipeline/tmp")
TMP_TRAIN_UIDS_PARQUET = TMP_DIR / "imputation_train_uids.parquet"

# ---------------------------------------------------------------------------
# Columns that should never be imputed
# ---------------------------------------------------------------------------
NEVER_IMPUTE = {
    "is_gagnant",
    "position_arrivee",
    "rapport_simple_gagnant",
    "rapport_place",
    "gains",
}

# Prefixes / substrings that mark a column as a target or result
RESULT_PATTERNS = ("rapport_", "gains_", "position_", "is_gagnant")

# Fill-rate threshold: below this, skip median imputation (leave NaN)
MIN_FILL_RATE_FOR_MEDIAN = 0.20

# DuckDB memory settings
DUCKDB_MEMORY = "16GB"
DUCKDB_THREADS = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect() -> duckdb.DuckDBPyConnection:
    """Return a fresh DuckDB connection with conservative settings."""
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY}'")
    con.execute(f"SET threads={DUCKDB_THREADS}")
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")
    return con


def _classify_column(col: str, dtype: str, is_bool_01: bool) -> str:
    """Return the imputation bucket for a column.

    Buckets:
        'zero'      -> fill with 0
        'elo'       -> fill with 1500
        'median'    -> fill with train-set median
        'skip'      -> do not impute
    """
    col_lower = col.lower()

    # Never impute targets / results
    if col in NEVER_IMPUTE:
        return "skip"
    for pat in RESULT_PATTERNS:
        if col_lower.startswith(pat):
            return "skip"

    # Non-numeric -> skip (CatBoost/LightGBM handle NaN in categoricals natively)
    if dtype not in ("FLOAT", "DOUBLE", "INTEGER", "BIGINT", "SMALLINT",
                     "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER",
                     "USMALLINT", "UTINYINT", "DECIMAL", "REAL"):
        return "skip"

    # Rule-based on column name
    if any(p in col_lower for p in ("_count", "_nb_", "nb_courses")):
        return "zero"
    if any(p in col_lower for p in ("_ratio", "_pct", "_rate")):
        return "zero"
    if "_elo" in col_lower:
        return "elo"
    if "_streak" in col_lower:
        return "zero"

    # Boolean / flag columns (only values 0 and 1)
    if is_bool_01:
        return "zero"

    # All other numeric -> median (subject to fill-rate check later)
    return "median"


def _load_train_uids(con: duckdb.DuckDBPyConnection) -> None:
    """Load train UIDs from text file into a DuckDB table for join."""
    if not TRAIN_UIDS.exists():
        print(f"WARNING: train_uids.txt not found at {TRAIN_UIDS}", file=sys.stderr)
        print("         Medians will be computed on the FULL dataset (potential leakage).",
              file=sys.stderr)
        return

    print(f"Loading train UIDs from {TRAIN_UIDS} ...", file=sys.stderr)
    # Read UIDs into a DuckDB table directly
    con.execute(f"""
        CREATE OR REPLACE TABLE train_uids AS
        SELECT CAST(column0 AS VARCHAR) AS uid
        FROM read_csv_auto('{TRAIN_UIDS.as_posix()}', header=false)
    """)
    count = con.execute("SELECT COUNT(*) FROM train_uids").fetchone()[0]
    print(f"  Train UIDs loaded: {count:,}", file=sys.stderr)


def _train_uids_available(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute("SELECT 1 FROM train_uids LIMIT 1")
        return True
    except Exception:
        return False


def _get_columns_info(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Return list of {name, dtype} dicts for all columns in INPUT."""
    rows = con.execute(f"""
        SELECT column_name, column_type
        FROM (DESCRIBE SELECT * FROM read_parquet('{INPUT.as_posix()}'))
    """).fetchall()
    return [{"name": r[0], "dtype": r[1]} for r in rows]


def _check_bool_01(con: duckdb.DuckDBPyConnection, col: str, dtype: str) -> bool:
    """Return True if an integer column contains only values 0 and 1 (or NaN)."""
    if dtype not in ("INTEGER", "BIGINT", "SMALLINT", "TINYINT",
                     "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT"):
        return False
    try:
        result = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT "{col}"
                FROM read_parquet('{INPUT.as_posix()}')
                WHERE "{col}" IS NOT NULL
                  AND "{col}" NOT IN (0, 1)
                LIMIT 1
            )
        """).fetchone()[0]
        return result == 0
    except Exception:
        return False


def _compute_fill_rate(con: duckdb.DuckDBPyConnection, col: str, total_rows: int) -> float:
    """Compute fill rate (non-NaN fraction) for one column."""
    try:
        non_null = con.execute(f"""
            SELECT COUNT("{col}")
            FROM read_parquet('{INPUT.as_posix()}')
            WHERE "{col}" IS NOT NULL
        """).fetchone()[0]
        return non_null / total_rows if total_rows > 0 else 0.0
    except Exception:
        return 0.0


def _compute_train_medians(
    con: duckdb.DuckDBPyConnection,
    median_cols: list[str],
    use_train_filter: bool,
) -> dict[str, float]:
    """Compute median for each column, restricted to train split if available.

    Returns {col_name: median_value}. Columns that are entirely NULL get 0.0.
    """
    if not median_cols:
        return {}

    print(f"\nComputing train-set medians for {len(median_cols)} columns ...", file=sys.stderr)
    t0 = time.time()

    # Build the filter clause
    if use_train_filter:
        filter_clause = "WHERE partant_uid IN (SELECT uid FROM train_uids)"
    else:
        filter_clause = ""

    # Process in batches of 200 columns to avoid overly long SQL
    BATCH = 200
    medians: dict[str, float] = {}

    for i in range(0, len(median_cols), BATCH):
        batch = median_cols[i: i + BATCH]
        select_exprs = ", ".join(
            f'MEDIAN("{c}") AS "{c}"' for c in batch
        )
        sql = f"""
            SELECT {select_exprs}
            FROM read_parquet('{INPUT.as_posix()}')
            {filter_clause}
        """
        try:
            row = con.execute(sql).fetchone()
            for j, col in enumerate(batch):
                val = row[j]
                medians[col] = float(val) if val is not None else 0.0
        except Exception as e:
            print(f"  WARNING: batch median failed ({e}), computing individually for batch",
                  file=sys.stderr)
            for col in batch:
                try:
                    single_sql = f"""
                        SELECT MEDIAN("{col}")
                        FROM read_parquet('{INPUT.as_posix()}')
                        {filter_clause}
                    """
                    val = con.execute(single_sql).fetchone()[0]
                    medians[col] = float(val) if val is not None else 0.0
                except Exception as e2:
                    print(f"    SKIP median for {col}: {e2}", file=sys.stderr)
                    medians[col] = 0.0

        done = min(i + BATCH, len(median_cols))
        elapsed = time.time() - t0
        print(f"  {done}/{len(median_cols)} medians computed  ({elapsed:.1f}s)",
              file=sys.stderr)

    return medians


def _build_imputation_sql(
    columns_info: list[dict],
    bucket_map: dict[str, str],  # col -> bucket
    medians: dict[str, float],   # col -> median value
    skipped_low_fill: set[str],
) -> str:
    """Build the SELECT clause that applies imputation for each column."""
    parts = []
    for col_info in columns_info:
        col = col_info["name"]
        bucket = bucket_map.get(col, "skip")

        if bucket == "skip" or col in skipped_low_fill:
            # Pass through unchanged
            parts.append(f'"{col}"')
        elif bucket == "zero":
            parts.append(f'COALESCE("{col}", 0) AS "{col}"')
        elif bucket == "elo":
            parts.append(f'COALESCE("{col}", 1500) AS "{col}"')
        elif bucket == "median":
            median_val = medians.get(col, 0.0)
            # Format as integer if the value is a whole number, otherwise float
            if median_val == int(median_val):
                fill_literal = str(int(median_val))
            else:
                fill_literal = repr(median_val)
            parts.append(f'COALESCE("{col}", {fill_literal}) AS "{col}"')
        else:
            parts.append(f'"{col}"')

    return ",\n    ".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    t_start = time.time()

    print("=" * 70, file=sys.stderr)
    print("  apply_imputation.py", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # Validate input
    if not INPUT.exists():
        print(f"ERROR: Input file not found: {INPUT}", file=sys.stderr)
        sys.exit(1)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    con = _connect()

    # -----------------------------------------------------------------------
    # Step 1: Load train UIDs
    # -----------------------------------------------------------------------
    _load_train_uids(con)
    use_train_filter = _train_uids_available(con)

    # -----------------------------------------------------------------------
    # Step 2: Inspect columns
    # -----------------------------------------------------------------------
    print("\nInspecting column schema ...", file=sys.stderr)
    columns_info = _get_columns_info(con)
    print(f"  Total columns: {len(columns_info):,}", file=sys.stderr)

    # Total row count for fill-rate calculation
    total_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{INPUT.as_posix()}')"
    ).fetchone()[0]
    print(f"  Total rows: {total_rows:,}", file=sys.stderr)

    # -----------------------------------------------------------------------
    # Step 3: Classify columns and identify 0/1 booleans
    # -----------------------------------------------------------------------
    print("\nClassifying columns ...", file=sys.stderr)

    bucket_map: dict[str, str] = {}
    bool_check_count = 0

    for col_info in columns_info:
        col = col_info["name"]
        dtype = col_info["dtype"]

        # Skip slow per-column bool check (would query Parquet 3000+ times)
        # Instead, just use name matching for boolean detection
        is_b01 = False
        col_lower = col.lower()
        if any(p in col_lower for p in ("_flag", "_is_", "is_", "_bool", "_has_")):
            is_b01 = True
            bool_check_count += 1

        bucket_map[col] = _classify_column(col, dtype, is_b01)

    # Count by bucket
    from collections import Counter
    bucket_counts = Counter(bucket_map.values())
    print(f"  Bucket summary:", file=sys.stderr)
    for bkt, cnt in sorted(bucket_counts.items()):
        print(f"    {bkt:10s}: {cnt:,}", file=sys.stderr)
    print(f"  Boolean/flag columns detected: {bool_check_count}", file=sys.stderr)

    # -----------------------------------------------------------------------
    # Step 4: Check fill rates for "median" bucket columns
    #         Skip median imputation if fill rate < MIN_FILL_RATE_FOR_MEDIAN
    # -----------------------------------------------------------------------
    median_candidates = [
        col_info["name"]
        for col_info in columns_info
        if bucket_map.get(col_info["name"]) == "median"
    ]

    print(f"\nChecking fill rates for {len(median_candidates)} median-bucket columns ...",
          file=sys.stderr)

    skipped_low_fill: set[str] = set()
    median_cols_to_compute: list[str] = []

    # Batch fill-rate check (200 cols at a time instead of 1-by-1)
    FILL_BATCH = 200
    for i in range(0, len(median_candidates), FILL_BATCH):
        batch = median_candidates[i:i + FILL_BATCH]
        select_parts = ", ".join(f'COUNT("{c}") AS "n_{j}"' for j, c in enumerate(batch))
        try:
            row = con.execute(f"""
                SELECT {select_parts}
                FROM read_parquet('{INPUT.as_posix()}')
            """).fetchone()
            for j, col in enumerate(batch):
                fill_rate = row[j] / total_rows if total_rows > 0 else 0.0
                if fill_rate < MIN_FILL_RATE_FOR_MEDIAN:
                    skipped_low_fill.add(col)
                else:
                    median_cols_to_compute.append(col)
        except Exception as e:
            print(f"  WARNING: batch fill-rate check failed: {e}", file=sys.stderr)
            # Fallback: accept all
            for col in batch:
                median_cols_to_compute.append(col)
        done = min(i + FILL_BATCH, len(median_candidates))
        print(f"  Fill rates: {done}/{len(median_candidates)} checked", file=sys.stderr)

    print(f"  Median imputation: {len(median_cols_to_compute)} columns", file=sys.stderr)
    print(f"  Skipped (fill rate < {MIN_FILL_RATE_FOR_MEDIAN:.0%}): "
          f"{len(skipped_low_fill)} columns", file=sys.stderr)

    # -----------------------------------------------------------------------
    # Step 5: Compute train-set medians
    # -----------------------------------------------------------------------
    medians = _compute_train_medians(con, median_cols_to_compute, use_train_filter)

    # -----------------------------------------------------------------------
    # Step 6: Build and execute imputation SQL -> output Parquet
    # -----------------------------------------------------------------------
    print("\nBuilding imputation SQL ...", file=sys.stderr)
    select_sql = _build_imputation_sql(columns_info, bucket_map, medians, skipped_low_fill)

    impute_sql = f"""
COPY (
    SELECT
    {select_sql}
    FROM read_parquet('{INPUT.as_posix()}')
)
TO '{OUTPUT.as_posix()}'
(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
"""

    print(f"\nWriting imputed Parquet to:\n  {OUTPUT}", file=sys.stderr)
    print("  This may take several minutes for 2.9M rows x 3000+ columns ...",
          file=sys.stderr)
    t_write = time.time()
    con.execute(impute_sql)
    elapsed_write = time.time() - t_write
    print(f"  Write completed in {elapsed_write:.1f}s", file=sys.stderr)

    # -----------------------------------------------------------------------
    # Step 7: Summary report
    # -----------------------------------------------------------------------
    elapsed_total = time.time() - t_start
    out_size_mb = OUTPUT.stat().st_size / 1024 / 1024

    print("\n" + "=" * 70, file=sys.stderr)
    print("  IMPUTATION COMPLETE", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(f"  Input : {INPUT}", file=sys.stderr)
    print(f"  Output: {OUTPUT}  ({out_size_mb:.0f} MB)", file=sys.stderr)
    print(f"  Total columns    : {len(columns_info):,}", file=sys.stderr)
    print(f"  -> zero-filled    : {bucket_counts.get('zero', 0):,}", file=sys.stderr)
    print(f"  -> elo-filled     : {bucket_counts.get('elo', 0):,}", file=sys.stderr)
    print(f"  -> median-filled  : {len(median_cols_to_compute):,}  "
          f"(train-set medians, leakage-safe)", file=sys.stderr)
    print(f"  -> skipped (low fill rate): {len(skipped_low_fill):,}", file=sys.stderr)
    print(f"  -> skipped (non-numeric / targets): "
          f"{bucket_counts.get('skip', 0) - len(skipped_low_fill):,}", file=sys.stderr)
    print(f"  Train-set filter : {'YES' if use_train_filter else 'NO (fallback: full dataset)'}",
          file=sys.stderr)
    print(f"  Elapsed          : {elapsed_total:.1f}s", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # Print machine-readable summary to stdout for orchestrators
    print(f"OUTPUT={OUTPUT}")
    print(f"ROWS={total_rows}")
    print(f"COLS_TOTAL={len(columns_info)}")
    print(f"COLS_ZERO={bucket_counts.get('zero', 0)}")
    print(f"COLS_ELO={bucket_counts.get('elo', 0)}")
    print(f"COLS_MEDIAN={len(median_cols_to_compute)}")
    print(f"COLS_SKIPPED_LOWFILL={len(skipped_low_fill)}")
    print(f"COLS_SKIPPED_OTHER={bucket_counts.get('skip', 0) - len(skipped_low_fill)}")
    print(f"TRAIN_FILTER={'yes' if use_train_filter else 'no'}")
    print(f"OUTPUT_MB={out_size_mb:.1f}")

    con.close()


if __name__ == "__main__":
    main()
