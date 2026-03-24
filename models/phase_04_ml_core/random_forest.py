#!/usr/bin/env python3
"""
models/phase_04_ml_core/random_forest.py
=========================================
Script 22 -- Random Forest pour la prediction hippique.

Fonctionnalites :
  - Hyperparameter grid search
  - Feature importance ranking
  - OOB (Out-of-Bag) score
  - Split temporel train/val/test
  - Sauvegarde du modele + metriques

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold, GridSearchCV
from sklearn.metrics import (
    accuracy_score,
    log_loss,
    roc_auc_score,
)

# ===========================================================================
# CONFIG
# ===========================================================================

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA_DIR = ROOT / "models" / "data"
SAVED_DIR = ROOT / "models" / "saved"

DEFAULT_PARQUET = DATA_DIR / "features_master.parquet"
DEFAULT_TARGET = "is_winner"
DEFAULT_DATE_COL = "date"

PARAM_GRID = {
    "n_estimators": [300, 500, 800],
    "max_depth": [8, 12, 16, None],
    "min_samples_leaf": [10, 20, 50],
    "min_samples_split": [5, 10],
    "max_features": ["sqrt", "log2"],
}

N_FOLDS = 5
RANDOM_STATE = 42


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


logger = setup_logging("random_forest")


# ===========================================================================
# DATA LOADING + SPLIT
# ===========================================================================

def load_and_split(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_features_path: Optional[Path] = None,
    target: str = DEFAULT_TARGET,
    date_col: str = DEFAULT_DATE_COL,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, list[str]]:
    """Charge les donnees et split temporel.

    Returns
    -------
    tuple
        (X_train, y_train, X_val, y_val, X_test, y_test, feature_names)
    """
    logger.info("Chargement : %s", parquet_path)
    df = pd.read_parquet(parquet_path)

    # Filtrer features si fichier disponible
    if selected_features_path and selected_features_path.exists():
        with open(selected_features_path, "r", encoding="utf-8") as f:
            sel = json.load(f)
        feat_list = sel.get("selected_features") or sel.get("optimal", {}).get("features", [])
        if feat_list:
            logger.info("  Features selectionnees : %d", len(feat_list))

    if date_col in df.columns:
        df = df.sort_values(date_col).reset_index(drop=True)

    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_df = df.iloc[:train_end]
    val_df = df.iloc[train_end:val_end]
    test_df = df.iloc[val_end:]

    exclude = {target, date_col, "race_id", "horse_id", "horse_name", "jockey", "trainer"}

    # Utiliser les features selectionnees si disponibles
    if selected_features_path and selected_features_path.exists() and feat_list:
        feature_cols = [c for c in feat_list if c in df.columns and c not in exclude]
    else:
        feature_cols = [
            c for c in df.columns
            if c not in exclude and pd.api.types.is_numeric_dtype(df[c])
        ]

    medians = train_df[feature_cols].median()

    X_train = train_df[feature_cols].fillna(medians)
    y_train = train_df[target].astype(int)
    X_val = val_df[feature_cols].fillna(medians)
    y_val = val_df[target].astype(int)
    X_test = test_df[feature_cols].fillna(medians)
    y_test = test_df[target].astype(int)

    logger.info("  Split : train=%d, val=%d, test=%d", len(train_df), len(val_df), len(test_df))
    logger.info("  Features : %d", len(feature_cols))

    return X_train, y_train, X_val, y_val, X_test, y_test, feature_cols


# ===========================================================================
# METRIQUES
# ===========================================================================

def compute_metrics(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    prefix: str = "",
) -> dict[str, float]:
    """Calcule accuracy, log_loss, ROC-AUC et ROI simule."""
    y_pred = (y_proba >= 0.5).astype(int)
    p = prefix + "_" if prefix else ""

    metrics = {
        f"{p}accuracy": round(float(accuracy_score(y_true, y_pred)), 6),
        f"{p}log_loss": round(float(log_loss(y_true, y_proba, labels=[0, 1])), 6),
    }

    try:
        metrics[f"{p}roc_auc"] = round(float(roc_auc_score(y_true, y_proba)), 6)
    except ValueError:
        metrics[f"{p}roc_auc"] = None

    # ROI simule
    if y_pred.sum() > 0:
        stakes = float(y_pred.sum())
        wins_mask = (y_pred == 1) & (y_true == 1)
        gains = 0.0
        for i in np.where(wins_mask)[0]:
            cote = min(1.0 / max(y_proba[i], 0.01), 50.0)
            gains += cote
        metrics[f"{p}roi_simulated"] = round((gains - stakes) / stakes, 6)
    else:
        metrics[f"{p}roi_simulated"] = 0.0

    return metrics


# ===========================================================================
# TRAINING
# ===========================================================================

def train_random_forest(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    param_grid: Optional[dict] = None,
) -> dict:
    """Entraine et evalue le Random Forest avec grid search.

    Parameters
    ----------
    X_train, y_train, X_val, y_val, X_test, y_test
    param_grid : dict, optional

    Returns
    -------
    dict
    """
    if param_grid is None:
        param_grid = PARAM_GRID

    logger.info("=" * 60)
    logger.info("RANDOM FOREST")
    logger.info("=" * 60)

    # Grid search
    base_rf = RandomForestClassifier(
        class_weight="balanced",
        oob_score=True,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )

    cv = StratifiedKFold(n_splits=N_FOLDS, shuffle=True, random_state=RANDOM_STATE)

    logger.info("Grid search (%d folds)...", N_FOLDS)
    grid = GridSearchCV(
        base_rf, param_grid,
        cv=cv,
        scoring="neg_log_loss",
        n_jobs=-1,
        refit=True,
        verbose=0,
    )
    grid.fit(X_train, y_train)

    best_params = grid.best_params_
    logger.info("  Meilleurs params : %s", best_params)
    logger.info("  CV score : %.6f", grid.best_score_)

    best_model = grid.best_estimator_

    # OOB score
    oob = best_model.oob_score_
    logger.info("  OOB score : %.6f", oob)

    # Predictions
    val_proba = best_model.predict_proba(X_val)[:, 1]
    test_proba = best_model.predict_proba(X_test)[:, 1]

    val_metrics = compute_metrics(np.asarray(y_val), val_proba, prefix="val")
    test_metrics = compute_metrics(np.asarray(y_test), test_proba, prefix="test")

    logger.info("Validation : %s", val_metrics)
    logger.info("Test       : %s", test_metrics)

    # Feature importance
    imp_vals = best_model.feature_importances_
    feature_names = list(X_train.columns)
    importance = dict(zip(feature_names, imp_vals.tolist()))
    importance = dict(sorted(importance.items(), key=lambda x: -x[1]))

    top_10 = list(importance.items())[:10]
    logger.info("Top 10 features :")
    for feat, imp in top_10:
        logger.info("  %-30s : %.6f", feat, imp)

    return {
        "model": best_model,
        "best_params": best_params,
        "oob_score": round(oob, 6),
        "cv_score": round(grid.best_score_, 6),
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
        "feature_importance": importance,
        "feature_names": feature_names,
    }


# ===========================================================================
# SAVE
# ===========================================================================

def save_results(results: dict, model_name: str = "random_forest") -> Path:
    """Sauvegarde le modele et les metriques."""
    save_dir = SAVED_DIR / model_name
    save_dir.mkdir(parents=True, exist_ok=True)

    model_path = save_dir / f"{model_name}.joblib"
    joblib.dump(results["model"], model_path)
    logger.info("Modele sauvegarde : %s", model_path)

    metrics = {
        "model": model_name,
        "best_params": results["best_params"],
        "oob_score": results["oob_score"],
        "cv_score": results["cv_score"],
        **results["val_metrics"],
        **results["test_metrics"],
    }
    metrics_path = save_dir / "metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=True, default=str)
    logger.info("Metriques sauvegardees : %s", metrics_path)

    imp_path = save_dir / "feature_importance.json"
    with open(imp_path, "w", encoding="utf-8") as f:
        json.dump(results["feature_importance"], f, indent=2, ensure_ascii=True)
    logger.info("Importances sauvegardees : %s", imp_path)

    return save_dir


# ===========================================================================
# PIPELINE
# ===========================================================================

def run(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_features_path: Optional[Path] = None,
    target: str = DEFAULT_TARGET,
) -> dict:
    """Pipeline complet Random Forest."""
    X_train, y_train, X_val, y_val, X_test, y_test, _ = load_and_split(
        parquet_path, selected_features_path, target,
    )
    results = train_random_forest(X_train, y_train, X_val, y_val, X_test, y_test)
    save_dir = save_results(results)
    logger.info("Pipeline termine. Resultats dans : %s", save_dir)
    return results


# ===========================================================================
# CLI
# ===========================================================================

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Random Forest (script 22)",
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--selected-features", type=Path, default=None)
    parser.add_argument("--target", type=str, default=DEFAULT_TARGET)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    results = run(
        parquet_path=args.input,
        selected_features_path=args.selected_features,
        target=args.target,
    )
    test_ll = results["test_metrics"].get("test_log_loss", "N/A")
    oob = results["oob_score"]
    print(f"\n[OK] RandomForest — test log_loss={test_ll}, OOB={oob}")


if __name__ == "__main__":
    main()
