#!/usr/bin/env python3
"""
models/phase_04_ml_core/lightgbm_model.py
==========================================
Script 24 -- LightGBM pour la prediction hippique.

Fonctionnalites :
  - Gestion native des features categoriques
  - Mode DART (Dropouts meet Multiple Additive Regression Trees)
  - Objectif personnalise pour les courses hippiques
  - Early stopping
  - Split temporel train/val/test
  - Sauvegarde du modele + metriques

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import warnings
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    log_loss,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold

try:
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    HAS_LGB = False

try:
    import optuna
    HAS_OPTUNA = True
except ImportError:
    HAS_OPTUNA = False

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

CATEGORICAL_COLS = ["surface", "type_course", "hippodrome", "terrain"]

N_TRIALS = 50
EARLY_STOPPING_ROUNDS = 50
RANDOM_STATE = 42


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


logger = setup_logging("lightgbm_model")


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
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, list[str], list[str]]:
    """Charge les donnees et split temporel.

    Returns
    -------
    tuple
        (X_train, y_train, X_val, y_val, X_test, y_test, feature_names, cat_features)
    """
    logger.info("Chargement : %s", parquet_path)
    df = pd.read_parquet(parquet_path)

    feat_list: list[str] = []
    if selected_features_path and selected_features_path.exists():
        with open(selected_features_path, "r", encoding="utf-8") as f:
            sel = json.load(f)
        feat_list = sel.get("selected_features") or sel.get("optimal", {}).get("features", [])

    if date_col in df.columns:
        df = df.sort_values(date_col).reset_index(drop=True)

    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_df = df.iloc[:train_end]
    val_df = df.iloc[train_end:val_end]
    test_df = df.iloc[val_end:]

    exclude = {target, date_col, "race_id", "horse_id", "horse_name", "jockey", "trainer"}

    if feat_list:
        feature_cols = [c for c in feat_list if c in df.columns and c not in exclude]
    else:
        feature_cols = [
            c for c in df.columns
            if c not in exclude and (
                pd.api.types.is_numeric_dtype(df[c]) or c in CATEGORICAL_COLS
            )
        ]

    # Detecter les colonnes categoriques presentes
    cat_features = [c for c in feature_cols if c in CATEGORICAL_COLS and c in df.columns]
    for col in cat_features:
        for part in [train_df, val_df, test_df]:
            part[col] = part[col].astype("category")

    medians = train_df[[c for c in feature_cols if c not in cat_features]].median()

    def prepare(part: pd.DataFrame) -> pd.DataFrame:
        X = part[feature_cols].copy()
        num_cols = [c for c in feature_cols if c not in cat_features]
        X[num_cols] = X[num_cols].fillna(medians)
        return X

    X_train = prepare(train_df)
    y_train = train_df[target].astype(int)
    X_val = prepare(val_df)
    y_val = val_df[target].astype(int)
    X_test = prepare(test_df)
    y_test = test_df[target].astype(int)

    logger.info("  Split : train=%d, val=%d, test=%d", len(train_df), len(val_df), len(test_df))
    logger.info("  Features : %d (dont %d categoriques)", len(feature_cols), len(cat_features))

    return X_train, y_train, X_val, y_val, X_test, y_test, feature_cols, cat_features


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

    if y_pred.sum() > 0:
        stakes = float(y_pred.sum())
        wins_mask = (y_pred == 1) & (y_true == 1)
        gains = sum(
            min(1.0 / max(y_proba[i], 0.01), 50.0)
            for i in np.where(wins_mask)[0]
        )
        metrics[f"{p}roi_simulated"] = round((gains - stakes) / stakes, 6)
    else:
        metrics[f"{p}roi_simulated"] = 0.0

    return metrics


# ===========================================================================
# CUSTOM OBJECTIVE
# ===========================================================================

def racing_logloss_objective(y_pred: np.ndarray, dtrain: "lgb.Dataset"):
    """Objectif personnalise : logloss ponderee pour les courses.

    Penalise davantage les faux negatifs (rater un gagnant)
    que les faux positifs, adapte au contexte de paris hippiques.

    Parameters
    ----------
    y_pred : np.ndarray
        Predictions brutes (log-odds).
    dtrain : lgb.Dataset
        Dataset LightGBM.

    Returns
    -------
    tuple
        (gradient, hessian)
    """
    y_true = dtrain.get_label()
    # Sigmoid
    p = 1.0 / (1.0 + np.exp(-y_pred))

    # Poids : 2x plus pour les gagnants (faux negatifs couteux)
    weight = np.where(y_true == 1, 2.0, 1.0)

    grad = weight * (p - y_true)
    hess = weight * p * (1.0 - p)

    return grad, hess


# ===========================================================================
# TRAINING
# ===========================================================================

def train_lightgbm(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    cat_features: list[str],
    use_dart: bool = False,
    use_custom_obj: bool = False,
    n_trials: int = N_TRIALS,
) -> dict:
    """Entraine et evalue LightGBM.

    Parameters
    ----------
    X_train, y_train, X_val, y_val, X_test, y_test
    cat_features : list[str]
        Colonnes categoriques.
    use_dart : bool
        Utiliser le boosting DART.
    use_custom_obj : bool
        Utiliser l'objectif personnalise.
    n_trials : int
        Nombre de trials pour l'optimisation.

    Returns
    -------
    dict
    """
    if not HAS_LGB:
        raise ImportError("lightgbm requis : pip install lightgbm")

    logger.info("=" * 60)
    logger.info("LIGHTGBM MODEL")
    logger.info("  DART mode : %s", use_dart)
    logger.info("  Custom objective : %s", use_custom_obj)
    logger.info("  Categorical features : %s", cat_features)
    logger.info("=" * 60)

    # Datasets LightGBM
    dtrain = lgb.Dataset(X_train, label=y_train, categorical_feature=cat_features or "auto")
    dval = lgb.Dataset(X_val, label=y_val, reference=dtrain, categorical_feature=cat_features or "auto")

    # Parametres de base
    boosting = "dart" if use_dart else "gbdt"

    params = {
        "objective": "binary" if not use_custom_obj else None,
        "boosting_type": boosting,
        "metric": "binary_logloss",
        "is_unbalance": True,
        "learning_rate": 0.05,
        "num_leaves": 63,
        "max_depth": 8,
        "min_child_samples": 20,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "random_state": RANDOM_STATE,
        "n_jobs": -1,
        "verbose": -1,
    }

    if use_dart:
        params["drop_rate"] = 0.1
        params["max_drop"] = 50
        params["skip_drop"] = 0.5

    # Optimisation bayesienne si optuna disponible
    if HAS_OPTUNA and n_trials > 0:
        logger.info("Optimisation Optuna (%d trials)...", n_trials)
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        def objective(trial):
            trial_params = {
                **params,
                "num_leaves": trial.suggest_int("num_leaves", 20, 127),
                "max_depth": trial.suggest_int("max_depth", 4, 12),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
                "subsample": trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
                "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
                "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
            }

            callbacks = [lgb.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False)]
            if use_custom_obj:
                trial_params.pop("objective", None)
                bst = lgb.train(
                    trial_params, dtrain,
                    num_boost_round=1000,
                    valid_sets=[dval],
                    fobj=racing_logloss_objective,
                    callbacks=callbacks,
                )
            else:
                bst = lgb.train(
                    trial_params, dtrain,
                    num_boost_round=1000,
                    valid_sets=[dval],
                    callbacks=callbacks,
                )

            val_pred = bst.predict(X_val)
            return float(log_loss(y_val, val_pred))

        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE),
        )
        study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

        best_trial_params = study.best_trial.params
        params.update(best_trial_params)
        logger.info("  Meilleurs params Optuna : %s", best_trial_params)

    # Entrainement final
    callbacks = [
        lgb.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False),
        lgb.log_evaluation(period=0),
    ]

    if use_custom_obj:
        params.pop("objective", None)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = lgb.train(
                params, dtrain,
                num_boost_round=2000,
                valid_sets=[dval],
                fobj=racing_logloss_objective,
                callbacks=callbacks,
            )
    else:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = lgb.train(
                params, dtrain,
                num_boost_round=2000,
                valid_sets=[dval],
                callbacks=callbacks,
            )

    best_iteration = model.best_iteration
    logger.info("  Best iteration : %d", best_iteration)

    # Predictions
    val_proba = model.predict(X_val, num_iteration=best_iteration)
    test_proba = model.predict(X_test, num_iteration=best_iteration)

    val_metrics = compute_metrics(np.asarray(y_val), val_proba, prefix="val")
    test_metrics = compute_metrics(np.asarray(y_test), test_proba, prefix="test")

    logger.info("Validation : %s", val_metrics)
    logger.info("Test       : %s", test_metrics)

    # Feature importance
    feature_names = list(X_train.columns)
    imp_split = model.feature_importance(importance_type="split")
    imp_gain = model.feature_importance(importance_type="gain")

    importance_split = dict(zip(feature_names, imp_split.tolist()))
    importance_split = dict(sorted(importance_split.items(), key=lambda x: -x[1]))

    importance_gain = dict(zip(feature_names, imp_gain.tolist()))
    importance_gain = dict(sorted(importance_gain.items(), key=lambda x: -x[1]))

    logger.info("Top 10 features (gain) :")
    for feat, imp in list(importance_gain.items())[:10]:
        logger.info("  %-30s : %.4f", feat, imp)

    return {
        "model": model,
        "params": params,
        "best_iteration": best_iteration,
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
        "importance_split": importance_split,
        "importance_gain": importance_gain,
        "feature_names": feature_names,
        "cat_features": cat_features,
    }


# ===========================================================================
# SAVE
# ===========================================================================

def save_results(results: dict, model_name: str = "lightgbm") -> Path:
    """Sauvegarde le modele et les metriques."""
    save_dir = SAVED_DIR / model_name
    save_dir.mkdir(parents=True, exist_ok=True)

    # Modele natif LightGBM
    lgb_path = save_dir / f"{model_name}.lgb"
    results["model"].save_model(str(lgb_path))
    logger.info("Modele sauvegarde : %s", lgb_path)

    # Aussi en joblib pour compatibilite
    joblib_path = save_dir / f"{model_name}.joblib"
    joblib.dump(results["model"], joblib_path)

    # Metriques
    metrics = {
        "model": model_name,
        "best_iteration": results["best_iteration"],
        "cat_features": results["cat_features"],
        **results["val_metrics"],
        **results["test_metrics"],
    }
    metrics_path = save_dir / "metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=True, default=str)

    # Feature importance
    imp_path = save_dir / "feature_importance_gain.json"
    with open(imp_path, "w", encoding="utf-8") as f:
        json.dump(results["importance_gain"], f, indent=2, ensure_ascii=True)

    imp_split_path = save_dir / "feature_importance_split.json"
    with open(imp_split_path, "w", encoding="utf-8") as f:
        json.dump(results["importance_split"], f, indent=2, ensure_ascii=True)

    logger.info("Metriques sauvegardees : %s", metrics_path)
    return save_dir


# ===========================================================================
# PIPELINE
# ===========================================================================

def run(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_features_path: Optional[Path] = None,
    target: str = DEFAULT_TARGET,
    use_dart: bool = False,
    use_custom_obj: bool = False,
    n_trials: int = N_TRIALS,
) -> dict:
    """Pipeline complet LightGBM."""
    X_train, y_train, X_val, y_val, X_test, y_test, _, cat_features = load_and_split(
        parquet_path, selected_features_path, target,
    )
    results = train_lightgbm(
        X_train, y_train, X_val, y_val, X_test, y_test,
        cat_features=cat_features,
        use_dart=use_dart,
        use_custom_obj=use_custom_obj,
        n_trials=n_trials,
    )
    save_dir = save_results(results)
    logger.info("Pipeline termine. Resultats dans : %s", save_dir)
    return results


# ===========================================================================
# CLI
# ===========================================================================

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="LightGBM model (script 24)",
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--selected-features", type=Path, default=None)
    parser.add_argument("--target", type=str, default=DEFAULT_TARGET)
    parser.add_argument("--dart", action="store_true",
                        help="Utiliser le boosting DART")
    parser.add_argument("--custom-objective", action="store_true",
                        help="Utiliser l'objectif personnalise racing")
    parser.add_argument("--n-trials", type=int, default=N_TRIALS,
                        help="Nombre de trials Optuna")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    results = run(
        parquet_path=args.input,
        selected_features_path=args.selected_features,
        target=args.target,
        use_dart=args.dart,
        use_custom_obj=args.custom_objective,
        n_trials=args.n_trials,
    )
    test_ll = results["test_metrics"].get("test_log_loss", "N/A")
    test_auc = results["test_metrics"].get("test_roc_auc", "N/A")
    print(f"\n[OK] LightGBM — test log_loss={test_ll}, AUC={test_auc}")


if __name__ == "__main__":
    main()
