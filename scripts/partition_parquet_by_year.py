"""
Partition features_consolidated.parquet by year.

Joins features with partants_master to extract year from date_reunion_iso,
then writes one Parquet file per year to 04_FEATURES/partitioned/year=YYYY/data.parquet.
"""

import os
import duckdb

FEATURES_PATH = "D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet"
MASTER_PATH = "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet"
OUTPUT_DIR = "D:/turf-data-pipeline/04_FEATURES/partitioned"


def main():
    con = duckdb.connect(config={"memory_limit": "8GB", "threads": 2})

    print("Fetching distinct years from master...")
    years_result = con.execute(f"""
        SELECT DISTINCT
            YEAR(CAST(date_reunion_iso AS DATE)) AS year
        FROM read_parquet('{MASTER_PATH}')
        WHERE date_reunion_iso IS NOT NULL
        ORDER BY year
    """).fetchall()

    years = [row[0] for row in years_result]
    print(f"Found {len(years)} years: {years}")

    for year in years:
        out_dir = os.path.join(OUTPUT_DIR, f"year={year}")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "data.parquet")

        print(f"\nWriting year={year}...")
        con.execute(f"""
            COPY (
                SELECT f.*
                FROM read_parquet('{FEATURES_PATH}') f
                JOIN read_parquet('{MASTER_PATH}') m
                    ON f.partant_uid = m.partant_uid
                WHERE YEAR(CAST(m.date_reunion_iso AS DATE)) = {year}
            )
            TO '{out_path}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
        file_size_mb = os.path.getsize(out_path) / (1024 * 1024)
        print(f"  year={year}: {row_count:,} rows, {file_size_mb:.1f} MB -> {out_path}")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
