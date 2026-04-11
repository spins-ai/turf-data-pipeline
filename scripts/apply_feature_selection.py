#!/usr/bin/env python3
"""Feature importance analysis and selection using LightGBM + DuckDB.

Strategy:
  1. Use DuckDB to discover feature columns and sample train rows
  2. Target column: comblage__is_gagnant
  3. Fit a quick LightGBM (100 trees, gain-based importance)
  4. Keep features with importance > 0 (capped at top 500)
  5. Write feature_importance.csv
  6. Write features_selected.parquet via DuckDB COPY

Input:  D:/turf-data-pipeline/04_FEATURES/features_consolidated.parquet
Output: D:/turf-data-pipeline/04_FEATURES/features_selected.parquet
        D:/turf-data-pipeline/04_FEATURES/feature_importance.csv

Uses DuckDB for all I/O (avoids pyarrow deadlock on Windows).
"""

import sys
import time
import gc
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import lightgbm as lgb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FEATURES_DIR = Path("D:/turf-data-pipeline/04_FEATURES")
INPUT_PARQUET = FEATURES_DIR / "features_consolidated.parquet"
TRAIN_UIDS_TXT = FEATURES_DIR / "splits" / "train_uids.txt"
OUTPUT_IMPORTANCE = FEATURES_DIR / "feature_importance.csv"
OUTPUT_SELECTED = FEATURES_DIR / "features_selected.parquet"

# ---------------------------------------------------------------------------
# Tuning knobs
# ---------------------------------------------------------------------------
SAMPLE_ROWS = 200_000
MAX_FEATURES = 500
N_TREES = 100
TARGET_COL = "comblage__is_gagnant"
UID_COL = "partant_uid"

# Columns that are IDs / targets -- never used as features
ID_COLS = {"partant_uid", "course_uid", "cheval_uid", "jockey_uid"}
TARGET_COLS = {
    "comblage__is_gagnant", "comblage__is_place",
    "is_gagnant", "is_place",
    "position_arrivee", "rapport_simple_gagnant", "rapport_place", "gains",
    "target_roi",
}
# Post-race result columns (leak the outcome)
POST_RACE_COLS = {
    "comblage__position_arrivee", "nettoyage__position_arrivee",
    "feature_improvements__position_arrivee",
    "comblage__statut", "nettoyage__statut",
    "beaten_lengths__bl_ecart_lengths",
    "beaten_lengths__bl_ecart_vs_avg",
    "beaten_lengths__bl_ecart_per_km",
    "beaten_lengths__bl_race_tightness",
    "log_transforms__log_rapport_simple_gagnant",
    "race_timing__rtm_time_vs_winner",
    "race_timing__rtm_is_fastest",
    "race_timing__rtm_speed_rank",
    "win_margin_features__wmf_horse_in_top_quarter",
    "race_result_prediction__rrp_exacta_proxy",
    "photo_finish__pf_race_competitiveness",
}
POST_RACE_PATTERNS = [
    "position_arrivee", "ecart_length", "bl_ecart",
    "wmf_position_margin", "wmf_relative_speed_figure",
    "wmf_horse_avg_time_behind", "rtm_time_vs_winner",
    "rtm_is_fastest", "rtm_speed_rank",
    "wmf_horse_in_top_quarter",
    "pf_race_competitiveness", "rrp_exacta",
]
# Duplicate columns to exclude (keep comblage__ version, drop copies)
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
EXCLUDE_COLS = ID_COLS | TARGET_COLS | POST_RACE_COLS | DUPLICATE_COLS


def discover_feature_columns(con) -> list[str]:
    """Return list of numeric feature column names."""
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{INPUT_PARQUET}') LIMIT 0").fetchall()

    feature_cols = []
    numeric_types = {"BIGINT", "INTEGER", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "HUGEINT", "DECIMAL"}

    for col_name, col_type, *_ in desc:
        if col_name in EXCLUDE_COLS:
            continue
        if any(pat in col_name.lower() for pat in POST_RACE_PATTERNS):
            continue
        # Check if numeric type
        base_type = col_type.split("(")[0].upper()
        if base_type in numeric_types:
            feature_cols.append(col_name)

    return feature_cols


def load_train_uids() -> set[str]:
    if not TRAIN_UIDS_TXT.exists():
        print(f"WARNING: {TRAIN_UIDS_TXT} not found. Sampling from full dataset.", flush=True)
        return set()
    with open(TRAIN_UIDS_TXT, "r", encoding="utf-8") as fh:
        uids = {line.strip() for line in fh if line.strip()}
    print(f"  Train UIDs loaded: {len(uids):,}", flush=True)
    return uids


def sample_train_data(con, feature_cols: list[str], train_uids: set[str]) -> pd.DataFrame:
    """Sample train rows using DuckDB, reading features in column batches."""
    print(f"\n  Sampling {SAMPLE_ROWS:,} train rows...", flush=True)
    t0 = time.time()

    # First get UIDs to sample
    if train_uids:
        # Create temp table with train UIDs
        con.execute("CREATE TEMP TABLE train_uids (uid VARCHAR)")
        # Insert in batches
        uid_list = list(train_uids)
        for i in range(0, len(uid_list), 50000):
            batch = uid_list[i:i+50000]
            con.executemany("INSERT INTO train_uids VALUES (?)", [(u,) for u in batch])

        sample_uids = con.execute(f"""
            SELECT p.{UID_COL}
            FROM read_parquet('{INPUT_PARQUET}') p
            JOIN train_uids t ON p.{UID_COL} = t.uid
            WHERE p."{TARGET_COL}" IS NOT NULL
            ORDER BY RANDOM()
            LIMIT {SAMPLE_ROWS}
        """).fetchall()
    else:
        sample_uids = con.execute(f"""
            SELECT {UID_COL}
            FROM read_parquet('{INPUT_PARQUET}')
            WHERE "{TARGET_COL}" IS NOT NULL
            ORDER BY RANDOM()
            LIMIT {SAMPLE_ROWS}
        """).fetchall()

    uids = [r[0] for r in sample_uids]
    print(f"  {len(uids):,} UIDs sampled ({time.time()-t0:.1f}s)", flush=True)

    # Store sample UIDs
    con.execute("CREATE TEMP TABLE sample_uids (uid VARCHAR)")
    for i in range(0, len(uids), 50000):
        batch = uids[i:i+50000]
        con.executemany("INSERT INTO sample_uids VALUES (?)", [(u,) for u in batch])

    # Read features in column batches of 200
    COL_BATCH = 200
    all_dfs = []

    for b_start in range(0, len(feature_cols), COL_BATCH):
        batch_cols = feature_cols[b_start:b_start + COL_BATCH]
        batch_num = b_start // COL_BATCH + 1
        total_batches = (len(feature_cols) + COL_BATCH - 1) // COL_BATCH

        # Build column list
        cols_str = ", ".join([f'p."{c}"' for c in batch_cols])
        if b_start == 0:
            cols_str = f'p.{UID_COL}, p."{TARGET_COL}", ' + cols_str

        df = con.execute(f"""
            SELECT {cols_str}
            FROM read_parquet('{INPUT_PARQUET}') p
            JOIN sample_uids s ON p.{UID_COL} = s.uid
        """).fetchdf()

        if b_start == 0:
            all_dfs.append(df)
        else:
            all_dfs.append(df)

        print(f"  Column batch {batch_num}/{total_batches}: {len(batch_cols)} cols ({len(df):,} rows)", flush=True)
        gc.collect()

    # Concat horizontally
    if len(all_dfs) == 1:
        result = all_dfs[0]
    else:
        # First df has uid+target+features, others just features
        result = pd.concat(all_dfs, axis=1)
        # Remove duplicate columns if any
        result = result.loc[:, ~result.columns.duplicated()]

    del all_dfs
    gc.collect()

    print(f"  Sample: {len(result):,} rows x {len(result.columns):,} cols ({time.time()-t0:.1f}s)", flush=True)
    return result


def compute_importance(sample_df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Fit LightGBM on sample, return importance DataFrame."""
    available = [c for c in feature_cols if c in sample_df.columns]
    X = sample_df[available].values.astype(np.float32)
    X[~np.isfinite(X)] = np.nan
    y = sample_df[TARGET_COL].astype(int).values

    print(f"  Training: {X.shape} | Target positive rate: {y.mean():.4f}", flush=True)

    t0 = time.time()
    model = lgb.LGBMClassifier(
        n_estimators=N_TREES, num_leaves=63, max_depth=-1,
        learning_rate=0.1, subsample=0.8, colsample_bytree=0.8,
        importance_type="gain", n_jobs=2, random_state=42,
        verbose=-1, force_col_wise=True,
    )
    model.fit(X, y)
    print(f"  LightGBM fit: {time.time()-t0:.1f}s", flush=True)

    importance_df = pd.DataFrame({
        "feature": available,
        "importance": model.feature_importances_,
    })
    importance_df = importance_df.sort_values("importance", ascending=False).reset_index(drop=True)
    importance_df["rank"] = range(1, len(importance_df) + 1)

    del model, X, y
    gc.collect()
    return importance_df


def select_features(importance_df: pd.DataFrame) -> list[str]:
    positive = importance_df[importance_df["importance"] > 0]
    selected = positive["feature"].tolist()
    if len(selected) > MAX_FEATURES:
        selected = selected[:MAX_FEATURES]
    print(f"  Features > 0 importance: {len(positive):,}", flush=True)
    print(f"  Features selected (cap {MAX_FEATURES}): {len(selected):,}", flush=True)
    return selected


def write_selected_parquet(con, selected_features: list[str]) -> None:
    """Write features_selected.parquet via DuckDB COPY."""
    cols = [UID_COL, TARGET_COL] + selected_features
    cols_str = ", ".join([f'"{c}"' for c in cols])

    print(f"\n  Writing features_selected.parquet ({len(cols)} cols)...", flush=True)
    t0 = time.time()

    con.execute(f"""
        COPY (
            SELECT {cols_str}
            FROM read_parquet('{INPUT_PARQUET}')
        ) TO '{OUTPUT_SELECTED}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
    """)

    elapsed = time.time() - t0
    size_mb = OUTPUT_SELECTED.stat().st_size / 1024 / 1024
    print(f"  Written in {elapsed:.0f}s ({size_mb:.0f} MB)", flush=True)


def main():
    t_start = time.time()
    print("=" * 70, flush=True)
    print("  FEATURE SELECTION (DuckDB + LightGBM)", flush=True)
    print("=" * 70, flush=True)

    if not INPUT_PARQUET.exists():
        print(f"ERROR: {INPUT_PARQUET} not found", flush=True)
        sys.exit(1)

    con = duckdb.connect()
    con.execute("SET memory_limit='20GB'")
    con.execute("SET preserve_insertion_order=false")

    # Info
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{INPUT_PARQUET}') LIMIT 0").fetchall()
    n_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{INPUT_PARQUET}')").fetchone()[0]
    size_gb = INPUT_PARQUET.stat().st_size / 1024**3
    print(f"  Input: {len(desc):,} cols, {n_rows:,} rows, {size_gb:.1f} GB", flush=True)

    # Step 1: Feature columns
    feature_cols = discover_feature_columns(con)
    print(f"  Numeric feature columns: {len(feature_cols):,}", flush=True)

    # Step 2: Train UIDs
    train_uids = load_train_uids()

    # Step 3: Sample
    sample_df = sample_train_data(con, feature_cols, train_uids)
    del train_uids
    gc.collect()

    # Step 4: Importance
    print("\nComputing feature importance...", flush=True)
    importance_df = compute_importance(sample_df, feature_cols)
    del sample_df
    gc.collect()

    # Step 5: Select
    selected_features = select_features(importance_df)
    if not selected_features:
        print("ERROR: No features selected!", flush=True)
        sys.exit(1)

    # Step 6: Write importance CSV
    importance_df[["feature", "importance", "rank"]].to_csv(
        OUTPUT_IMPORTANCE.as_posix(), index=False
    )
    print(f"  Importance CSV: {OUTPUT_IMPORTANCE}", flush=True)

    # Step 7: Write selected parquet
    write_selected_parquet(con, selected_features)

    con.close()

    elapsed = time.time() - t_start
    print(f"\n{'='*70}", flush=True)
    print(f"  FEATURE SELECTION COMPLETE", flush=True)
    print(f"  {len(feature_cols):,} -> {len(selected_features):,} features", flush=True)
    print(f"  Elapsed: {elapsed:.0f}s", flush=True)
    print(f"{'='*70}", flush=True)


if __name__ == "__main__":
    main()
