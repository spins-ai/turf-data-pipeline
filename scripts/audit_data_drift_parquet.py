#!/usr/bin/env python3
"""Audit data drift between 2023 and 2024 cohorts using Parquet files via DuckDB.

Fast replacement for the JSONL-based audit_data_drift.py.
Reads individual builder Parquets, joins with master to get dates, then
computes per-feature distribution statistics for both year cohorts.

Flags features where:
  - |mean_shift| > 0.3  (in sigma units of the 2023 distribution)
  - |var_ratio - 1| > 0.3
  - |fill_rate_change| > 10 percentage points
"""
import sys
import csv
import math
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
MASTER_PARQUET = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
BUILDERS_DIR   = Path("D:/turf-data-pipeline/tmp/consolidation/individual")
OUTPUT_CSV     = Path("D:/turf-data-pipeline/04_FEATURES/data_drift_audit.csv")

# Drift thresholds
MEAN_SHIFT_THRESHOLD = 0.3   # sigma units
VAR_RATIO_THRESHOLD  = 0.3   # |ratio - 1|
FILL_CHANGE_THRESHOLD = 10.0 # percentage points

# ---------------------------------------------------------------------------
# DuckDB connection (shared for the whole run)
# ---------------------------------------------------------------------------
def make_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=2")
    return con


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe(v):
    """Return float or None, filtering out non-finite values."""
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def get_date_map(con: duckdb.DuckDBPyConnection) -> None:
    """Register the master Parquet as a DuckDB view 'master_dates'."""
    con.execute(f"""
        CREATE OR REPLACE VIEW master_dates AS
        SELECT partant_uid,
               date_reunion_iso
        FROM read_parquet('{MASTER_PARQUET.as_posix()}')
        WHERE partant_uid IS NOT NULL
          AND date_reunion_iso IS NOT NULL
    """)


def list_builder_parquets() -> list[Path]:
    if not BUILDERS_DIR.exists():
        print(f"ERROR: builders dir not found: {BUILDERS_DIR}", file=sys.stderr)
        sys.exit(1)
    return sorted(p for p in BUILDERS_DIR.glob("*.parquet"))


def get_numeric_feature_cols(con: duckdb.DuckDBPyConnection,
                             parquet_path: Path,
                             builder_name: str) -> list[str]:
    """Return numeric columns (excluding partant_uid) from the builder Parquet."""
    schema = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path.as_posix()}') LIMIT 0"
    ).fetchall()
    numeric_types = {
        "INTEGER", "BIGINT", "DOUBLE", "FLOAT", "REAL",
        "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT",
        "UINTEGER", "USMALLINT", "UTINYINT", "DECIMAL", "NUMERIC",
    }
    cols = []
    for row in schema:
        col_name = row[0]
        col_type = row[1].upper().split("(")[0]  # strip precision, e.g. DECIMAL(18,3)
        if col_name == "partant_uid":
            continue
        if col_type in numeric_types:
            cols.append(col_name)
    return cols


def compute_drift_for_builder(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    builder_name: str,
) -> list[dict]:
    """Join builder Parquet with master dates, split by year, compute stats."""

    feature_cols = get_numeric_feature_cols(con, parquet_path, builder_name)
    if not feature_cols:
        return []

    pq_posix = parquet_path.as_posix()

    # Build a single SQL query that returns per-feature stats for 2023 and 2024
    # We use a UNION ALL of per-column aggregations to avoid pivoting.
    # Each sub-select computes (count_non_null, mean, stddev_pop) for one feature
    # in both cohorts.

    union_parts = []
    for feat in feature_cols:
        # Escape column name with double-quotes in case it contains special chars
        qfeat = f'"{feat}"'
        union_parts.append(f"""
            SELECT
                '{feat}' AS feature,
                COUNT_IF(year_cohort = '2023' AND {qfeat} IS NOT NULL)       AS n23,
                COUNT_IF(year_cohort = '2024' AND {qfeat} IS NOT NULL)       AS n24,
                COUNT_IF(year_cohort = '2023')                               AS total23,
                COUNT_IF(year_cohort = '2024')                               AS total24,
                AVG(CASE WHEN year_cohort = '2023' THEN {qfeat} END)         AS mean23,
                AVG(CASE WHEN year_cohort = '2024' THEN {qfeat} END)         AS mean24,
                STDDEV_POP(CASE WHEN year_cohort = '2023' THEN {qfeat} END)  AS std23,
                STDDEV_POP(CASE WHEN year_cohort = '2024' THEN {qfeat} END)  AS std24
            FROM joined
            WHERE year_cohort IN ('2023', '2024')
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    full_sql = f"""
        WITH builder AS (
            SELECT *
            FROM read_parquet('{pq_posix}')
        ),
        joined AS (
            SELECT
                b.*,
                CASE
                    WHEN m.date_reunion_iso < '2024-01-01' THEN '2023'
                    WHEN m.date_reunion_iso >= '2024-01-01' THEN '2024'
                    ELSE NULL
                END AS year_cohort
            FROM builder b
            JOIN master_dates m USING (partant_uid)
        )
        {union_sql}
    """

    try:
        rows = con.execute(full_sql).fetchall()
    except Exception as exc:
        print(f"  WARN [{builder_name}]: query failed – {exc}", file=sys.stderr)
        return []

    results = []
    for row in rows:
        (feat, n23, n24, total23, total24,
         mean23, mean24, std23, std24) = row

        n23    = n23    or 0
        n24    = n24    or 0
        total23 = total23 or 0
        total24 = total24 or 0
        mean23 = _safe(mean23) or 0.0
        mean24 = _safe(mean24) or 0.0
        std23  = _safe(std23)  or 0.0
        std24  = _safe(std24)  or 0.0

        if n23 < 10 and n24 < 10:
            continue

        # Mean shift in sigma units of the 2023 distribution
        if std23 > 1e-10:
            mean_shift = (mean24 - mean23) / std23
        else:
            mean_shift = 0.0

        # Variance ratio (std24 / std23)
        if std23 > 1e-10:
            var_ratio = (std24 / std23) if std24 > 0 else 0.0
        else:
            var_ratio = 1.0

        # Fill rates (% of rows in that cohort that have a non-null value)
        fill_23 = (n23 / total23 * 100) if total23 > 0 else 0.0
        fill_24 = (n24 / total24 * 100) if total24 > 0 else 0.0
        fill_change = fill_24 - fill_23

        # Only emit rows that exceed at least one threshold
        if (abs(mean_shift) > MEAN_SHIFT_THRESHOLD
                or abs(var_ratio - 1) > VAR_RATIO_THRESHOLD
                or abs(fill_change) > FILL_CHANGE_THRESHOLD):
            results.append({
                "builder":          builder_name,
                "feature":          feat,
                "n_2023":           n23,
                "n_2024":           n24,
                "mean_2023":        round(mean23, 4),
                "mean_2024":        round(mean24, 4),
                "std_2023":         round(std23, 4),
                "std_2024":         round(std24, 4),
                "mean_shift_sigma": round(mean_shift, 3),
                "var_ratio":        round(var_ratio, 3),
                "fill_2023":        round(fill_23, 1),
                "fill_2024":        round(fill_24, 1),
                "fill_change":      round(fill_change, 1),
                "flagged_shift":    1 if abs(mean_shift) > MEAN_SHIFT_THRESHOLD else 0,
                "flagged_var":      1 if abs(var_ratio - 1) > VAR_RATIO_THRESHOLD else 0,
                "flagged_fill":     1 if abs(fill_change) > FILL_CHANGE_THRESHOLD else 0,
            })

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=== DATA DRIFT AUDIT (Parquet / DuckDB) ===", file=sys.stderr)
    print(f"Master : {MASTER_PARQUET}", file=sys.stderr)
    print(f"Builders: {BUILDERS_DIR}", file=sys.stderr)
    print(f"Output  : {OUTPUT_CSV}", file=sys.stderr)
    print("", file=sys.stderr)

    if not MASTER_PARQUET.exists():
        print(f"ERROR: master Parquet not found: {MASTER_PARQUET}", file=sys.stderr)
        sys.exit(1)

    con = make_conn()
    get_date_map(con)

    # Sanity-check: how many rows in each cohort?
    cohort_counts = con.execute("""
        SELECT
            CASE
                WHEN date_reunion_iso < '2024-01-01' THEN '2023'
                WHEN date_reunion_iso >= '2024-01-01' THEN '2024'
                ELSE 'other'
            END AS cohort,
            COUNT(*) AS n
        FROM master_dates
        GROUP BY cohort
        ORDER BY cohort
    """).fetchall()
    for cohort, n in cohort_counts:
        print(f"  Cohort {cohort}: {n:,} rows", file=sys.stderr)
    print("", file=sys.stderr)

    builder_parquets = list_builder_parquets()
    print(f"Found {len(builder_parquets)} builder Parquets", file=sys.stderr)

    all_results: list[dict] = []
    for i, pq in enumerate(builder_parquets):
        builder_name = pq.stem
        print(f"  [{i+1}/{len(builder_parquets)}] {builder_name} ...", file=sys.stderr)
        try:
            rows = compute_drift_for_builder(con, pq, builder_name)
            all_results.extend(rows)
            if rows:
                print(f"    -> {len(rows)} features flagged", file=sys.stderr)
        except Exception as exc:
            print(f"    ERROR: {exc}", file=sys.stderr)
            continue

    con.close()

    # Sort by absolute mean shift (most drifted first)
    all_results.sort(key=lambda r: -abs(r["mean_shift_sigma"]))

    # Write output
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "builder", "feature",
        "n_2023", "n_2024",
        "mean_2023", "mean_2024",
        "std_2023", "std_2024",
        "mean_shift_sigma", "var_ratio",
        "fill_2023", "fill_2024", "fill_change",
        "flagged_shift", "flagged_var", "flagged_fill",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    # Summary
    severe_shift  = [r for r in all_results if abs(r["mean_shift_sigma"]) > 1.0]
    severe_var    = [r for r in all_results if abs(r["var_ratio"] - 1) > 0.5]
    fill_drops    = [r for r in all_results if r["fill_change"] < -20]
    fill_gains    = [r for r in all_results if r["fill_change"] > 20]

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"DATA DRIFT AUDIT COMPLETE (2023 vs 2024)", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"Total features flagged  : {len(all_results)}", file=sys.stderr)
    print(f"Severe shift (>1 sigma) : {len(severe_shift)}", file=sys.stderr)
    print(f"Severe var ratio (>0.5) : {len(severe_var)}", file=sys.stderr)
    print(f"Fill rate drops (>20pp) : {len(fill_drops)}", file=sys.stderr)
    print(f"Fill rate gains (>20pp) : {len(fill_gains)}", file=sys.stderr)
    print(f"\nOutput written to: {OUTPUT_CSV}", file=sys.stderr)

    if severe_shift:
        print(f"\n--- TOP 20 SEVERE MEAN SHIFTS ---", file=sys.stderr)
        for r in severe_shift[:20]:
            print(
                f"  {r['builder']}/{r['feature']}: "
                f"shift={r['mean_shift_sigma']:+.3f}s  "
                f"(2023: {r['mean_2023']} ± {r['std_2023']}, "
                f"2024: {r['mean_2024']} ± {r['std_2024']})",
                file=sys.stderr,
            )

    if fill_drops:
        print(f"\n--- TOP 10 FILL RATE DROPS ---", file=sys.stderr)
        fill_drops.sort(key=lambda r: r["fill_change"])
        for r in fill_drops[:10]:
            print(
                f"  {r['builder']}/{r['feature']}: "
                f"fill 2023={r['fill_2023']}% -> 2024={r['fill_2024']}%  "
                f"(Δ={r['fill_change']:+.1f}pp)",
                file=sys.stderr,
            )


if __name__ == "__main__":
    main()
