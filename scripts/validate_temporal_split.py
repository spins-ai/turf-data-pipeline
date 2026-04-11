"""
validate_temporal_split.py
==========================
Validates that the temporal train/val/test split boundaries do not cut
in the middle of a race day, i.e. no course_uid has runners that appear
on both sides of a split boundary.

Split boundaries checked
------------------------
  - 2024-01-01  (train / validation)
  - 2024-07-01  (validation / test)

A boundary is "clean" if no course_uid spans the boundary.
If any course_uid does span the boundary, it is printed with its dates.

Input
-----
  D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet

Uses DuckDB with memory_limit='8GB'.
"""

import sys

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MASTER_PARQUET = "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet"

SPLIT_BOUNDARIES = [
    ("2024-01-01", "train", "validation"),
    ("2024-07-01", "validation", "test"),
]

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
print("Connecting to DuckDB…")
con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=4")

# ---------------------------------------------------------------------------
# Helper: how many distinct race days exist per split
# ---------------------------------------------------------------------------
print(f"\nSource: {MASTER_PARQUET}\n")
total_rows, total_courses, date_min, date_max = con.execute(f"""
    SELECT
        COUNT(*)                          AS n_rows,
        COUNT(DISTINCT course_uid)        AS n_courses,
        MIN(date_reunion_iso)             AS date_min,
        MAX(date_reunion_iso)             AS date_max
    FROM read_parquet('{MASTER_PARQUET}')
""").fetchone()

print(f"Dataset overview")
print(f"  Rows            : {total_rows:,}")
print(f"  Unique courses  : {total_courses:,}")
print(f"  Date range      : {date_min}  ->  {date_max}")

# ---------------------------------------------------------------------------
# Validate each boundary
# ---------------------------------------------------------------------------
all_clean = True

for boundary_date, split_before, split_after in SPLIT_BOUNDARIES:
    print(f"\n{'='*60}")
    print(f"Boundary: {boundary_date}  ({split_before} | {split_after})")
    print(f"{'='*60}")

    # For each course_uid, compute min and max date_reunion_iso.
    # A crossing course has min_date < boundary AND max_date >= boundary.
    problematic = con.execute(f"""
        SELECT
            course_uid,
            MIN(date_reunion_iso) AS date_min,
            MAX(date_reunion_iso) AS date_max,
            COUNT(*)              AS n_runners
        FROM read_parquet('{MASTER_PARQUET}')
        GROUP BY course_uid
        HAVING MIN(date_reunion_iso) < '{boundary_date}'
           AND MAX(date_reunion_iso) >= '{boundary_date}'
        ORDER BY date_min DESC
        LIMIT 100
    """).fetchall()

    if not problematic:
        print(f"  CLEAN – no course_uid crosses the {boundary_date} boundary.")
    else:
        all_clean = False
        print(f"  WARNING: {len(problematic)} course(s) span the boundary!")
        print(f"  {'course_uid':<20}  {'date_min':<12}  {'date_max':<12}  {'runners':>8}")
        print(f"  {'-'*20}  {'-'*12}  {'-'*12}  {'-'*8}")
        for uid, dmin, dmax, n in problematic:
            print(f"  {uid:<20}  {dmin!s:<12}  {dmax!s:<12}  {n:>8}")

    # Also print split sizes for context
    sizes = con.execute(f"""
        SELECT
            CASE WHEN date_reunion_iso < '2024-01-01' THEN 'train'
                 WHEN date_reunion_iso < '2024-07-01' THEN 'validation'
                 ELSE 'test'
            END AS split,
            COUNT(DISTINCT course_uid) AS n_courses,
            COUNT(*)                   AS n_runners,
            MIN(date_reunion_iso)      AS date_from,
            MAX(date_reunion_iso)      AS date_to
        FROM read_parquet('{MASTER_PARQUET}')
        GROUP BY 1
        ORDER BY 2 DESC
    """).fetchall()

    print(f"\n  Split sizes (all boundaries combined):")
    print(f"  {'split':<12}  {'courses':>8}  {'runners':>10}  {'from':<12}  {'to':<12}")
    print(f"  {'-'*12}  {'-'*8}  {'-'*10}  {'-'*12}  {'-'*12}")
    for split, nc, nr, dfrom, dto in sizes:
        print(f"  {split:<12}  {nc:>8,}  {nr:>10,}  {dfrom!s:<12}  {dto!s:<12}")
    break  # sizes block is the same for every boundary; print once

# ---------------------------------------------------------------------------
# Additional check: any single course_uid with dates on BOTH sides of ANY boundary
# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
print("Cross-boundary course check (all boundaries at once)")
print(f"{'='*60}")

cross_all = con.execute(f"""
    WITH course_dates AS (
        SELECT
            course_uid,
            MIN(date_reunion_iso) AS date_min,
            MAX(date_reunion_iso) AS date_max
        FROM read_parquet('{MASTER_PARQUET}')
        GROUP BY course_uid
    )
    SELECT
        course_uid,
        date_min,
        date_max,
        CASE
            WHEN date_min < '2024-01-01' AND date_max >= '2024-01-01' THEN 'train|validation'
            WHEN date_min < '2024-07-01' AND date_max >= '2024-07-01' THEN 'validation|test'
            ELSE 'unknown'
        END AS crosses
    FROM course_dates
    WHERE
        (date_min < '2024-01-01' AND date_max >= '2024-01-01')
        OR
        (date_min < '2024-07-01' AND date_max >= '2024-07-01')
    ORDER BY crosses, date_min DESC
    LIMIT 200
""").fetchall()

if not cross_all:
    print("  CLEAN – no course_uid crosses any split boundary. Temporal split is valid.")
else:
    all_clean = False
    print(f"  {len(cross_all)} problematic course(s) found across all boundaries:")
    print(f"  {'course_uid':<20}  {'date_min':<12}  {'date_max':<12}  {'crosses'}")
    print(f"  {'-'*20}  {'-'*12}  {'-'*12}  {'-'*22}")
    for uid, dmin, dmax, crosses in cross_all:
        print(f"  {uid:<20}  {dmin!s:<12}  {dmax!s:<12}  {crosses}")

# ---------------------------------------------------------------------------
# Final verdict
# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
if all_clean:
    print("RESULT: PASS – Temporal split is clean for all boundaries.")
else:
    print("RESULT: FAIL – Some courses cross split boundaries (see above).")
    print("        Consider shifting boundaries or filtering problematic courses.")
print(f"{'='*60}")

con.close()
print("Done.")
