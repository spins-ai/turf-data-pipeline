#!/usr/bin/env python3
"""Feature importance analysis and selection using LightGBM.

Strategy:
  1. Load train UIDs from splits/train_uids.txt
  2. Use PyArrow streaming to sample ~200K train rows (memory-safe)
  3. Target column: comblage__is_gagnant (already in the Parquet)
  4. Fit a quick LightGBM (100 trees, gain-based importance)
  5. Keep features with importance > 0 (capped at top 500)
  6. Write feature_importance.csv (feature, importance, rank)
  7. Write features_selected.parquet (selected features + partant_uid, all rows)
     using streaming row-group-by-row-group copy with SNAPPY compression

Input:  D:/turf-data-pipeline/04_FEATURES/features_normalized.parquet
        (2577 cols, ~2.93M rows, 30 row groups)
Output: D:/turf-data-pipeline/04_FEATURES/features_selected.parquet
        D:/turf-data-pipeline/04_FEATURES/feature_importance.csv

IMPORTANT: Uses PyArrow for all I/O (NOT DuckDB -- crashes on wide tables).
           Max ~4 GB RAM usage.

Python: /c/Users/celia/AppData/Local/Programs/Python/Python312/python.exe
"""

import sys
import time
import gc
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FEATURES_DIR = Path("D:/turf-data-pipeline/04_FEATURES")

INPUT_PARQUET = FEATURES_DIR / "features_consolidated.parquet"
TRAIN_UIDS_TXT = FEATURES_DIR / "splits" / "train_uids.txt"

OUTPUT_IMPORTANCE = FEATURES_DIR / "feature_importance.csv"
OUTPUT_SELECTED = FEATURES_DIR / "features_selected.parquet"
TMP_SELECTED = FEATURES_DIR / "features_selected_tmp.parquet"

# ---------------------------------------------------------------------------
# Tuning knobs
# ---------------------------------------------------------------------------
SAMPLE_ROWS = 200_000      # rows sampled from train split for importance
MAX_FEATURES = 500          # keep at most this many features
N_TREES = 100               # quick model -- not tuned, just for ranking

# Target column (binary 0/1, already in the Parquet file)
TARGET_COL = "comblage__is_gagnant"

# UID column (string row ID, always kept in output)
UID_COL = "partant_uid"

# Columns that are IDs / targets -- never used as features
ID_COLS = {"partant_uid", "course_uid", "cheval_uid", "jockey_uid"}
TARGET_COLS = {
    "comblage__is_gagnant", "comblage__is_place",
    "is_gagnant", "is_place",
    "position_arrivee", "rapport_simple_gagnant", "rapport_place", "gains",
    "target_roi",
}
# Post-race result columns that leak the outcome (must NEVER be features)
# These are known ONLY after the race finishes.
# NOTE: career/historical gains (gains_carriere, gains_annee, perf_gains_moy_5)
#       are NOT leakage — they are cumulative stats known before the race.
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
# Pattern-based exclusion: any column containing these substrings is post-race
POST_RACE_PATTERNS = [
    "position_arrivee", "ecart_length", "bl_ecart",
    "wmf_position_margin", "wmf_relative_speed_figure",
    "wmf_horse_avg_time_behind", "rtm_time_vs_winner",
    "rtm_is_fastest", "rtm_speed_rank",
    "wmf_horse_in_top_quarter",
    "pf_race_competitiveness", "rrp_exacta",
]
EXCLUDE_COLS = ID_COLS | TARGET_COLS | POST_RACE_COLS


# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow not installed.  Run: pip install pyarrow", file=sys.stderr)
    sys.exit(1)

try:
    import numpy as np
except ImportError:
    print("ERROR: numpy not installed.  Run: pip install numpy", file=sys.stderr)
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed.  Run: pip install pandas", file=sys.stderr)
    sys.exit(1)

try:
    import lightgbm as lgb
except ImportError:
    print(
        "ERROR: lightgbm not installed.\n"
        "  Install with:  pip install lightgbm\n"
        "  Or on Windows: pip install lightgbm --prefer-binary",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Step 1: Discover schema and identify numeric feature columns
# ---------------------------------------------------------------------------

def discover_feature_columns(pf: pq.ParquetFile) -> list[str]:
    """Return list of numeric feature column names (excluding IDs/targets/post-race)."""
    schema = pf.schema_arrow
    feature_cols = []
    for field in schema:
        if field.name in EXCLUDE_COLS:
            continue
        # Pattern-based exclusion for post-race result columns
        if any(pat in field.name.lower() for pat in POST_RACE_PATTERNS):
            continue
        # Keep only numeric types (int, float, decimal)
        if (
            pa.types.is_integer(field.type)
            or pa.types.is_floating(field.type)
            or pa.types.is_decimal(field.type)
        ):
            feature_cols.append(field.name)
    return feature_cols


# ---------------------------------------------------------------------------
# Step 2: Load train UIDs
# ---------------------------------------------------------------------------

def load_train_uids() -> set[str]:
    """Load train UIDs from text file. Returns empty set if not found."""
    if not TRAIN_UIDS_TXT.exists():
        print(
            f"WARNING: {TRAIN_UIDS_TXT} not found. "
            "Will sample from the full dataset.",
            file=sys.stderr,
        )
        return set()
    with open(TRAIN_UIDS_TXT, "r", encoding="utf-8") as fh:
        uids = {line.strip() for line in fh if line.strip()}
    print(f"Train UIDs loaded: {len(uids):,}", file=sys.stderr)
    return uids


# ---------------------------------------------------------------------------
# Step 3: Sample train rows via PyArrow streaming
# ---------------------------------------------------------------------------

def sample_train_data(
    pf: pq.ParquetFile,
    feature_cols: list[str],
    train_uids: set[str],
) -> pd.DataFrame:
    """Stream row groups, filter to train UIDs, collect up to SAMPLE_ROWS rows.

    Reads columns in batches to stay under ~4 GB RAM.
    Returns a DataFrame with UID_COL + TARGET_COL + all feature_cols.
    """
    n_rg = pf.metadata.num_row_groups
    total_rows = pf.metadata.num_rows

    # Calculate how many rows to sample per row group (proportional)
    if train_uids:
        # We don't know how many train rows are in each RG, so we collect
        # greedily until we hit SAMPLE_ROWS
        pass

    read_cols = [UID_COL, TARGET_COL] + feature_cols
    # Filter to columns that actually exist in schema
    schema_names = set(pf.schema_arrow.names)
    read_cols = [c for c in read_cols if c in schema_names]

    # For memory safety on wide tables (2500+ cols), read in column batches
    COL_BATCH = 300
    meta_cols = [UID_COL, TARGET_COL]
    meta_cols = [c for c in meta_cols if c in schema_names]

    # First pass: collect row indices from each row group (train + sample)
    # We read just UID + target to decide which rows to keep
    print(f"Pass 1: Identifying train rows to sample ...", file=sys.stderr)
    t0 = time.time()

    sampled_uids = set()
    rg_keep_counts = {}  # rg_idx -> number of rows to keep

    rng = np.random.RandomState(42)
    collected = 0

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=meta_cols)
        df_meta = table.to_pandas()
        del table

        # Filter to train UIDs
        if train_uids:
            mask = df_meta[UID_COL].isin(train_uids)
            df_train = df_meta[mask]
        else:
            df_train = df_meta

        # Drop rows with missing target
        df_train = df_train.dropna(subset=[TARGET_COL])

        n_available = len(df_train)
        still_need = SAMPLE_ROWS - collected

        if still_need <= 0:
            rg_keep_counts[rg_idx] = 0
            del df_meta, df_train
            continue

        if n_available <= still_need:
            # Keep all train rows from this RG
            keep_uids = set(df_train[UID_COL].values)
        else:
            # Subsample
            idx = rng.choice(n_available, size=still_need, replace=False)
            keep_uids = set(df_train.iloc[idx][UID_COL].values)

        sampled_uids.update(keep_uids)
        rg_keep_counts[rg_idx] = len(keep_uids)
        collected += len(keep_uids)

        del df_meta, df_train

        if collected >= SAMPLE_ROWS:
            # Mark remaining RGs as 0
            for ri in range(rg_idx + 1, n_rg):
                rg_keep_counts[ri] = 0
            break

    elapsed = time.time() - t0
    print(
        f"  Identified {collected:,} train rows to sample ({elapsed:.1f}s)",
        file=sys.stderr,
    )

    # Second pass: read feature columns in batches, filtering to sampled UIDs
    print(f"Pass 2: Reading features for sampled rows ...", file=sys.stderr)
    t0 = time.time()

    # We'll collect partial DataFrames per column batch, then concat horizontally
    feature_batches = []
    uid_series = None  # will hold the UID column (consistent across batches)
    target_series = None

    for b_start in range(0, len(feature_cols), COL_BATCH):
        batch_cols = feature_cols[b_start : b_start + COL_BATCH]
        batch_num = b_start // COL_BATCH + 1
        total_batches = (len(feature_cols) + COL_BATCH - 1) // COL_BATCH
        print(
            f"  Column batch {batch_num}/{total_batches} "
            f"({len(batch_cols)} cols) ...",
            file=sys.stderr,
        )

        cols_to_read = [UID_COL] + batch_cols
        if b_start == 0:
            cols_to_read = [UID_COL, TARGET_COL] + batch_cols
        cols_to_read = [c for c in cols_to_read if c in schema_names]

        batch_dfs = []

        for rg_idx in range(n_rg):
            if rg_keep_counts.get(rg_idx, 0) == 0:
                continue

            table = pf.read_row_group(rg_idx, columns=cols_to_read)
            df = table.to_pandas()
            del table

            # Filter to sampled UIDs
            mask = df[UID_COL].isin(sampled_uids)
            df = df[mask].copy()

            batch_dfs.append(df)
            del mask

        if not batch_dfs:
            continue

        combined = pd.concat(batch_dfs, ignore_index=True)
        del batch_dfs

        if b_start == 0:
            uid_series = combined[UID_COL]
            target_series = combined[TARGET_COL]
            feature_batches.append(combined[batch_cols])
        else:
            # Align by UID order (should match since we use same sampled_uids)
            feature_batches.append(combined[batch_cols])

        del combined
        gc.collect()

    # Assemble final DataFrame
    result = pd.concat([uid_series, target_series] + feature_batches, axis=1)
    del feature_batches, uid_series, target_series
    gc.collect()

    elapsed = time.time() - t0
    print(
        f"  Sample loaded: {len(result):,} rows x {len(result.columns):,} cols "
        f"({elapsed:.1f}s)",
        file=sys.stderr,
    )
    return result


# ---------------------------------------------------------------------------
# Step 4: Train LightGBM and extract feature importance
# ---------------------------------------------------------------------------

def compute_importance(
    sample_df: pd.DataFrame,
    feature_cols: list[str],
) -> pd.DataFrame:
    """Fit LightGBM on sample, return importance DataFrame."""

    # Prepare arrays
    available_features = [c for c in feature_cols if c in sample_df.columns]
    X_df = sample_df[available_features]

    y = sample_df[TARGET_COL].astype(int).values

    print(f"Training shape: {X_df.shape} (rows x features)", file=sys.stderr)
    print(f"Target positive rate: {y.mean():.4f}", file=sys.stderr)

    # Replace inf with NaN (LightGBM handles NaN natively)
    X = X_df.values.astype(np.float32)
    X[~np.isfinite(X)] = np.nan

    del X_df
    gc.collect()

    # Fit LightGBM
    print(f"Fitting LightGBM ({N_TREES} trees) ...", file=sys.stderr)
    t0 = time.time()

    model = lgb.LGBMClassifier(
        n_estimators=N_TREES,
        num_leaves=63,
        max_depth=-1,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        importance_type="gain",
        n_jobs=2,
        random_state=42,
        verbose=-1,
        force_col_wise=True,
    )
    model.fit(X, y)
    elapsed = time.time() - t0
    print(f"  LightGBM fit done in {elapsed:.1f}s", file=sys.stderr)

    importances = model.feature_importances_
    del model, X, y
    gc.collect()

    # Build importance DataFrame
    importance_df = pd.DataFrame({
        "feature": available_features,
        "importance": importances,
    })
    importance_df = importance_df.sort_values("importance", ascending=False)
    importance_df["rank"] = range(1, len(importance_df) + 1)
    importance_df = importance_df.reset_index(drop=True)

    return importance_df


# ---------------------------------------------------------------------------
# Step 5: Select features
# ---------------------------------------------------------------------------

def select_features(importance_df: pd.DataFrame) -> list[str]:
    """Return list of selected feature names (importance > 0, capped)."""
    positive = importance_df[importance_df["importance"] > 0]
    n_positive = len(positive)
    selected = positive["feature"].tolist()

    if len(selected) > MAX_FEATURES:
        selected = selected[:MAX_FEATURES]

    print(f"Features with importance > 0: {n_positive:,}", file=sys.stderr)
    print(f"Features selected (cap {MAX_FEATURES}): {len(selected):,}", file=sys.stderr)
    return selected


# ---------------------------------------------------------------------------
# Step 6: Write outputs
# ---------------------------------------------------------------------------

def write_importance_csv(importance_df: pd.DataFrame) -> None:
    """Save feature importance to CSV."""
    OUTPUT_IMPORTANCE.parent.mkdir(parents=True, exist_ok=True)
    importance_df[["feature", "importance", "rank"]].to_csv(
        OUTPUT_IMPORTANCE.as_posix(), index=False
    )
    print(f"Importance CSV: {OUTPUT_IMPORTANCE}", file=sys.stderr)


def write_selected_parquet(
    input_path: Path,
    selected_features: list[str],
) -> None:
    """Write features_selected.parquet streaming row-group by row-group.

    Only keeps UID_COL + selected features. Uses SNAPPY compression.
    """
    OUTPUT_SELECTED.parent.mkdir(parents=True, exist_ok=True)

    pf = pq.ParquetFile(str(input_path))
    n_rg = pf.metadata.num_row_groups
    total_rows = pf.metadata.num_rows

    # Columns to write (always include UID + target)
    cols_to_write = [UID_COL, TARGET_COL] + selected_features
    # Filter to columns that exist in schema
    schema_names = set(pf.schema_arrow.names)
    cols_to_write = [c for c in cols_to_write if c in schema_names]

    print(
        f"\nWriting features_selected.parquet "
        f"({len(cols_to_write)} cols, {total_rows:,} rows, {n_rg} row groups) ...",
        file=sys.stderr,
    )
    t0 = time.time()

    writer = None

    for rg_idx in range(n_rg):
        rg_t0 = time.time()
        table = pf.read_row_group(rg_idx, columns=cols_to_write)

        if writer is None:
            writer = pq.ParquetWriter(
                TMP_SELECTED.as_posix(),
                schema=table.schema,
                compression="snappy",
            )

        writer.write_table(table)
        rg_elapsed = time.time() - rg_t0
        rg_rows = table.num_rows
        del table
        gc.collect()

        print(
            f"  RG {rg_idx + 1}/{n_rg}: {rg_rows:,} rows ({rg_elapsed:.1f}s)",
            file=sys.stderr,
        )

    if writer is not None:
        writer.close()

    # Atomic rename
    if OUTPUT_SELECTED.exists():
        OUTPUT_SELECTED.unlink()
    TMP_SELECTED.rename(OUTPUT_SELECTED)

    elapsed = time.time() - t0
    size_mb = OUTPUT_SELECTED.stat().st_size / 1024 / 1024
    print(
        f"  Written in {elapsed:.1f}s ({size_mb:.0f} MB)",
        file=sys.stderr,
    )
    print(f"  Output: {OUTPUT_SELECTED}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    t_start = time.time()

    print("=" * 70, file=sys.stderr)
    print("  apply_feature_selection.py  (PyArrow + LightGBM)", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # ------------------------------------------------------------------
    # Validate input
    # ------------------------------------------------------------------
    if not INPUT_PARQUET.exists():
        print(f"ERROR: Input not found: {INPUT_PARQUET}", file=sys.stderr)
        sys.exit(1)

    pf = pq.ParquetFile(str(INPUT_PARQUET))
    schema = pf.schema_arrow
    n_rg = pf.metadata.num_row_groups
    total_rows = pf.metadata.num_rows
    size_gb = INPUT_PARQUET.stat().st_size / 1024 ** 3

    print(f"Input: {INPUT_PARQUET}", file=sys.stderr)
    print(
        f"  {len(schema):,} cols, {total_rows:,} rows, "
        f"{n_rg} row groups, {size_gb:.2f} GB",
        file=sys.stderr,
    )

    # Verify target column exists
    if TARGET_COL not in schema.names:
        print(
            f"ERROR: Target column '{TARGET_COL}' not found in schema.\n"
            f"  Available columns containing 'gagnant': "
            + str([c for c in schema.names if "gagnant" in c.lower()]),
            file=sys.stderr,
        )
        sys.exit(1)

    # Verify UID column exists
    if UID_COL not in schema.names:
        print(f"ERROR: UID column '{UID_COL}' not found in schema.", file=sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 1: Discover feature columns
    # ------------------------------------------------------------------
    feature_cols = discover_feature_columns(pf)
    print(f"Numeric feature columns: {len(feature_cols):,}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Step 2: Load train UIDs
    # ------------------------------------------------------------------
    train_uids = load_train_uids()

    # ------------------------------------------------------------------
    # Step 3: Sample train rows
    # ------------------------------------------------------------------
    print(f"\nSampling {SAMPLE_ROWS:,} train rows ...", file=sys.stderr)
    sample_df = sample_train_data(pf, feature_cols, train_uids)

    # Free train UIDs (can be large)
    del train_uids
    gc.collect()

    # ------------------------------------------------------------------
    # Step 4: Compute feature importance
    # ------------------------------------------------------------------
    print("\nComputing feature importance ...", file=sys.stderr)
    importance_df = compute_importance(sample_df, feature_cols)

    del sample_df
    gc.collect()

    # ------------------------------------------------------------------
    # Step 5: Select features
    # ------------------------------------------------------------------
    selected_features = select_features(importance_df)

    if not selected_features:
        print(
            "ERROR: No features selected (all importances are 0). "
            "Check your data and target column.",
            file=sys.stderr,
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 6: Write importance CSV
    # ------------------------------------------------------------------
    write_importance_csv(importance_df)

    # ------------------------------------------------------------------
    # Step 7: Write selected Parquet (all rows, selected columns only)
    # ------------------------------------------------------------------
    write_selected_parquet(INPUT_PARQUET, selected_features)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    elapsed_total = time.time() - t_start
    print("\n" + "=" * 70, file=sys.stderr)
    print("  FEATURE SELECTION COMPLETE", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(f"  Input            : {INPUT_PARQUET}", file=sys.stderr)
    print(f"  Total features   : {len(feature_cols):,}", file=sys.stderr)
    print(f"  Features selected: {len(selected_features):,}", file=sys.stderr)
    print(f"  Importance CSV   : {OUTPUT_IMPORTANCE}", file=sys.stderr)
    print(f"  Selected Parquet : {OUTPUT_SELECTED}", file=sys.stderr)
    print(f"  Elapsed          : {elapsed_total:.1f}s", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # Machine-readable stdout for orchestrators
    print(f"INPUT={INPUT_PARQUET.as_posix()}")
    print(f"FEATURES_TOTAL={len(feature_cols)}")
    print(f"FEATURES_SELECTED={len(selected_features)}")
    print(f"OUTPUT_IMPORTANCE={OUTPUT_IMPORTANCE.as_posix()}")
    print(f"OUTPUT_SELECTED={OUTPUT_SELECTED.as_posix()}")


if __name__ == "__main__":
    main()
