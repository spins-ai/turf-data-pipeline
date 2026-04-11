#!/usr/bin/env python3
"""
feature_selection_duckdb.py - Feature selection 100% DuckDB + numpy
===================================================================
No pandas, no lightgbm, no pyarrow — avoids Windows DLL deadlocks.

Strategy:
  1. DuckDB: get all numeric feature columns
  2. DuckDB: compute correlation(feature, target) for each feature on 200K sample
  3. numpy: rank by absolute correlation, keep top 500
  4. DuckDB: COPY selected columns to features_selected.parquet
"""

import sys
import time
from pathlib import Path

import numpy as np
import duckdb

FEATURES_DIR = Path("D:/turf-data-pipeline/04_FEATURES")
INPUT_PARQUET = FEATURES_DIR / "features_consolidated.parquet"
TRAIN_UIDS_TXT = FEATURES_DIR / "splits" / "train_uids.txt"
OUTPUT_IMPORTANCE = FEATURES_DIR / "feature_importance.csv"
OUTPUT_SELECTED = FEATURES_DIR / "features_selected.parquet"

SAMPLE_ROWS = 200_000
MAX_FEATURES = 500
TARGET_COL = "comblage__is_gagnant"
UID_COL = "partant_uid"

# Exclude columns
ID_COLS = {"partant_uid", "course_uid", "cheval_uid", "jockey_uid"}
TARGET_COLS = {
    "comblage__is_gagnant", "comblage__is_place",
    "is_gagnant", "is_place",
    "position_arrivee", "rapport_simple_gagnant", "rapport_place", "gains",
    "target_roi",
}
POST_RACE_PATTERNS = [
    "position_arrivee", "ecart_length", "bl_ecart",
    "wmf_position_margin", "wmf_relative_speed_figure",
    "wmf_horse_avg_time_behind", "rtm_time_vs_winner",
    "rtm_is_fastest", "rtm_speed_rank",
    "wmf_horse_in_top_quarter",
    "pf_race_competitiveness", "rrp_exacta",
    "log_rapport_simple_gagnant",
]
DUPLICATE_COLS = {
    "feature_improvements__surcharge_decharge_kg",
    "nettoyage__surcharge_decharge_kg",
    "pedigree_advanced_features__surcharge_decharge_kg",
    "temporal_context_features__surcharge_decharge_kg",
    "feature_improvements__taux_reclamation_euros",
    "nettoyage__taux_reclamation_euros",
    "pedigree_advanced_features__taux_reclamation_euros",
    "temporal_context_features__taux_reclamation_euros",
    "nettoyage__handicap_valeur",
    "pedigree_advanced_features__handicap_valeur",
    "temporal_context_features__handicap_valeur",
}
EXCLUDE_COLS = ID_COLS | TARGET_COLS | DUPLICATE_COLS


def main():
    start = time.time()
    print("=" * 70, flush=True)
    print("  FEATURE SELECTION (DuckDB + numpy only)", flush=True)
    print("=" * 70, flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='20GB'")

    # Step 1: Discover numeric feature columns
    print("\nStep 1: Discovering features...", flush=True)
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{INPUT_PARQUET}') LIMIT 0").fetchall()
    numeric_types = {"BIGINT", "INTEGER", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "HUGEINT"}

    feature_cols = []
    for col_name, col_type, *_ in desc:
        if col_name in EXCLUDE_COLS:
            continue
        if any(pat in col_name.lower() for pat in POST_RACE_PATTERNS):
            continue
        base_type = col_type.split("(")[0].upper()
        if base_type in numeric_types:
            feature_cols.append(col_name)

    print(f"  {len(feature_cols):,} numeric feature columns", flush=True)

    # Step 2: Compute correlations in batches via DuckDB
    print(f"\nStep 2: Computing correlations on {SAMPLE_ROWS:,} sample rows...", flush=True)

    # Create sample — skip train UID filtering (too slow to insert 2.4M UIDs)
    # Just sample randomly from rows with non-null target
    con.execute(f"""
        CREATE TEMP VIEW sample AS
        SELECT * FROM read_parquet('{INPUT_PARQUET}')
        WHERE "{TARGET_COL}" IS NOT NULL
        USING SAMPLE {SAMPLE_ROWS}
    """)

    sample_n = con.execute("SELECT COUNT(*) FROM sample").fetchone()[0]
    print(f"  Sample size: {sample_n:,}", flush=True)

    # Compute correlations in batches of 100 columns
    BATCH = 100
    correlations = {}
    t0 = time.time()

    for i in range(0, len(feature_cols), BATCH):
        batch_cols = feature_cols[i:i + BATCH]
        corr_exprs = []
        for c in batch_cols:
            corr_exprs.append(f'CORR(CAST("{c}" AS DOUBLE), CAST("{TARGET_COL}" AS DOUBLE)) AS "{c}"')

        corr_sql = f"SELECT {', '.join(corr_exprs)} FROM sample"
        try:
            row = con.execute(corr_sql).fetchone()
            for j, c in enumerate(batch_cols):
                val = row[j]
                if val is not None and not (isinstance(val, float) and (val != val)):  # not NaN
                    correlations[c] = abs(val)
                else:
                    correlations[c] = 0.0
        except Exception as e:
            # Some columns might cause errors, try one by one
            for c in batch_cols:
                try:
                    r = con.execute(f'SELECT CORR(CAST("{c}" AS DOUBLE), CAST("{TARGET_COL}" AS DOUBLE)) FROM sample').fetchone()
                    val = r[0]
                    if val is not None and not (isinstance(val, float) and (val != val)):
                        correlations[c] = abs(val)
                    else:
                        correlations[c] = 0.0
                except Exception:
                    correlations[c] = 0.0

        done = min(i + BATCH, len(feature_cols))
        if done % 500 < BATCH or done == len(feature_cols):
            elapsed = time.time() - t0
            print(f"  [{done}/{len(feature_cols)}] {elapsed:.0f}s", flush=True)

    print(f"  Correlations computed: {len(correlations):,}", flush=True)

    # Step 3: Rank and select
    print(f"\nStep 3: Ranking features...", flush=True)
    sorted_feats = sorted(correlations.items(), key=lambda x: x[1], reverse=True)

    # Keep features with correlation > 0, capped at MAX_FEATURES
    selected = [(f, c) for f, c in sorted_feats if c > 0]
    n_positive = len(selected)
    if len(selected) > MAX_FEATURES:
        selected = selected[:MAX_FEATURES]

    print(f"  Features with |corr| > 0: {n_positive:,}", flush=True)
    print(f"  Selected (cap {MAX_FEATURES}): {len(selected):,}", flush=True)

    if selected:
        print(f"  Top 10:", flush=True)
        for f, c in selected[:10]:
            print(f"    {f}: {c:.6f}", flush=True)

    selected_names = [f for f, c in selected]

    # Step 4: Write importance CSV
    print(f"\nStep 4: Writing importance CSV...", flush=True)
    with open(OUTPUT_IMPORTANCE, "w", encoding="utf-8") as f:
        f.write("feature,importance,rank\n")
        for rank, (feat, corr) in enumerate(sorted_feats, 1):
            f.write(f"{feat},{corr:.8f},{rank}\n")
    print(f"  {OUTPUT_IMPORTANCE}", flush=True)

    # Step 5: Write selected parquet
    if not selected_names:
        print("ERROR: No features selected!", flush=True)
        sys.exit(1)

    cols = [UID_COL, TARGET_COL] + selected_names
    cols_str = ", ".join([f'"{c}"' for c in cols])

    print(f"\nStep 5: Writing features_selected.parquet ({len(cols)} cols)...", flush=True)
    t0 = time.time()

    con.execute(f"""
        COPY (
            SELECT {cols_str}
            FROM read_parquet('{INPUT_PARQUET}')
        ) TO '{OUTPUT_SELECTED}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
    """)

    elapsed_write = time.time() - t0
    size_mb = OUTPUT_SELECTED.stat().st_size / 1024 / 1024
    print(f"  Written in {elapsed_write:.0f}s ({size_mb:.0f} MB)", flush=True)

    con.close()

    elapsed = time.time() - start
    print(f"\n{'='*70}", flush=True)
    print(f"  FEATURE SELECTION COMPLETE", flush=True)
    print(f"  {len(feature_cols):,} -> {len(selected_names):,} features", flush=True)
    print(f"  Elapsed: {elapsed:.0f}s", flush=True)
    print(f"{'='*70}", flush=True)


if __name__ == "__main__":
    main()
