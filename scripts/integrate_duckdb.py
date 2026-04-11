#!/usr/bin/env python3
"""
integrate_duckdb.py - Intègre builders JSONL dans features_consolidated via DuckDB
==================================================================================
100% DuckDB natif : read_json_auto + LEFT JOIN + COPY TO PARQUET.
Pas de pyarrow, pas de pandas, pas de deadlock.
"""

import os
import time
from pathlib import Path

import duckdb

CONSOLIDATED = Path("D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")

# Builders to integrate: name -> jsonl path
BUILDERS = {
    "renr_x": OUTPUT_DIR / "renr_x" / "renr_x_features.jsonl",
    "meteofine_x": OUTPUT_DIR / "meteofine_x" / "meteofine_x_features.jsonl",
}


def main():
    start = time.time()
    print("=" * 70, flush=True)
    print("  INTEGRATION DuckDB (renr_x + meteofine_x)", flush=True)
    print("=" * 70, flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='40GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")

    # Phase 1: Create views for each JSONL using read_json_auto
    all_features = {}
    for name, path in BUILDERS.items():
        if not path.exists():
            print(f"  SKIP {name}: {path} not found", flush=True)
            continue

        print(f"\n  Loading {name}...", flush=True)
        # Create view with full schema detection
        con.execute(f"""
            CREATE VIEW {name} AS
            SELECT * FROM read_json_auto('{path}', sample_size=-1)
        """)
        # Get feature columns (everything except partant_uid)
        desc = con.execute(f"DESCRIBE {name}").fetchall()
        feats = [d[0] for d in desc if d[0] != "partant_uid"]
        cnt = con.execute(f"SELECT COUNT(*) FROM {name} WHERE {feats[0]} IS NOT NULL").fetchone()[0]
        print(f"    {len(feats)} features, {cnt:,} rows with data", flush=True)
        all_features[name] = feats

    if not all_features:
        print("  Nothing to integrate!", flush=True)
        return

    total_new = sum(len(f) for f in all_features.values())
    print(f"\n  Total: {total_new} new features to integrate", flush=True)

    # Phase 2: Get consolidated info
    print(f"\nPhase 2: Consolidated info...", flush=True)
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{CONSOLIDATED}') LIMIT 0").fetchall()
    n_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{CONSOLIDATED}')").fetchone()[0]
    print(f"  {n_rows:,} rows, {len(desc)} columns", flush=True)

    # Phase 3: Build and execute JOIN query
    print(f"\nPhase 3: JOIN + COPY TO PARQUET...", flush=True)

    select_parts = ["c.*"]
    join_parts = []
    for name, feats in all_features.items():
        for feat in feats:
            select_parts.append(f'{name}."{feat}"')
        join_parts.append(f'LEFT JOIN {name} ON c.partant_uid = {name}.partant_uid')

    selects = ",\n       ".join(select_parts)
    joins = "\n        ".join(join_parts)

    tmp_path = CONSOLIDATED.parent / "features_consolidated_new.parquet"

    query = f"""
    COPY (
        SELECT {selects}
        FROM read_parquet('{CONSOLIDATED}') c
        {joins}
    ) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
    """

    t0 = time.time()
    con.execute(query)
    print(f"  Written in {time.time() - t0:.0f}s", flush=True)

    # Phase 4: Verify
    print(f"\nPhase 4: Verification...", flush=True)
    new_desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{tmp_path}') LIMIT 0").fetchall()
    new_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_path}')").fetchone()[0]
    print(f"  New file: {new_count:,} rows, {len(new_desc)} columns", flush=True)

    # Check fill rates for new features
    for name, feats in all_features.items():
        for feat in feats[:3]:
            filled = con.execute(f"""
                SELECT COUNT("{feat}") FROM read_parquet('{tmp_path}')
                WHERE "{feat}" IS NOT NULL
            """).fetchone()[0]
            print(f"  {feat}: {filled:,} / {new_count:,} ({filled*100/new_count:.1f}%)", flush=True)

    con.close()

    # Phase 5: Rename
    print(f"\nPhase 5: Remplacement...", flush=True)
    backup = CONSOLIDATED.parent / "features_consolidated_backup.parquet"
    if backup.exists():
        os.remove(str(backup))
    os.rename(str(CONSOLIDATED), str(backup))
    os.rename(str(tmp_path), str(CONSOLIDATED))

    # Final verify
    con2 = duckdb.connect()
    final = con2.execute(f"DESCRIBE SELECT * FROM read_parquet('{CONSOLIDATED}') LIMIT 0").fetchall()
    print(f"  Final: {len(final)} columns", flush=True)
    con2.close()

    # Remove backup
    if backup.exists():
        os.remove(str(backup))
        print(f"  Backup supprime", flush=True)

    elapsed = time.time() - start
    print(f"\n{'='*70}", flush=True)
    print(f"  TERMINE en {elapsed:.0f}s", flush=True)
    print(f"  {len(desc)} -> {len(final)} colonnes (+{len(final)-len(desc)})", flush=True)
    if CONSOLIDATED.exists():
        sz = CONSOLIDATED.stat().st_size / 1024 / 1024 / 1024
        print(f"  Taille: {sz:.1f} Go", flush=True)
    print(f"{'='*70}", flush=True)


if __name__ == "__main__":
    main()
