#!/usr/bin/env python3
"""
models/phase_04_ml_core/xgboost_model.py
==========================================
Script 23 -- XGBoost pour la prediction hippique.

Fonctionnalites :
  - Optimisation bayesienne des hyperparametres (optuna)
  - Early stopping
  - Feature importance (gain, cover, weight)
  - Valeurs SHAP
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
from sklearn.metrics import (
    accuracy_score,
    log_loss,
    roc_auc_score,
)

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

try:
    import optuna
    HAS_OPTUNA = True
except ImportError:
    HAS_OPTUNA = False

try:
    import shap
    HAS_SHAP = True
except ImportError:
    HAS_SHAP = False

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

N_TRIALS = 50
EARLY_STOPPING_ROUNDS = 50
RANDOM_STATE = 42


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


logger = setup_logging("xgboost_model")


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
    """Charge les donnees et split temporel."""
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
# BAYESIAN OPTIMIZATION (Optuna)
# ===========================================================================

def _optuna_objective(
    trial: "optuna.Trial",
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
) -> float:
    """Objectif Optuna pour XGBoost."""
    params = {
        "n_estimators": 1000,
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 30),
        "gamma": trial.suggest_float("gamma", 0.0, 5.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "scale_pos_weight": max(1, int((y_train == 0).sum() / max((y_train == 1).sum(), 1))),
        "eval_metric": "logloss",
        "random_state": RANDOM_STATE,
        "n_jobs": -1,
        "verbosity": 0,
    }

    model = xgb.XGBClassifier(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    val_proba = model.predict_proba(X_val)[:, 1]
    return float(log_loss(y_val, val_proba))


def bayesian_optimize(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    n_trials: int = N_TRIALS,
) -> dict:
    """Optimisation bayesienne des hyperparametres via Optuna.

    Parameters
    ----------
    X_train, y_train, X_val, y_val
    n_trials : int

    Returns
    -------
    dict
        Meilleurs hyperparametres.
    """
    if not HAS_OPTUNA:
        logger.warning("Optuna non disponible, utilisation des parametres par defaut")
        return {
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "min_child_weight": 10,
            "gamma": 0.1,
            "reg_alpha": 0.1,
            "reg_lambda": 1.0,
        }

    logger.info("Optimisation bayesienne Optuna (%d trials)...", n_trials)

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE))

    study.optimize(
        lambda trial: _optuna_objective(trial, X_train, y_train, X_val, y_val),
        n_trials=n_trials,
        show_progress_bar=False,
    )

    logger.info("  Meilleur trial : %.6f", study.best_trial.value)
    logger.info("  Meilleurs params : %s", study.best_trial.params)

    return study.best_trial.params


# ===========================================================================
# TRAINING
# ===========================================================================

def train_xgboost(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    n_trials: int = N_TRIALS,
    compute_shap: bool = True,
) -> dict:
    """Entraine et evalue XGBoost.

    Parameters
    ----------
    X_train, y_train, X_val, y_val, X_test, y_test
    n_trials : int
        Nombre de trials Optuna.
    compute_shap : bool
        Calculer les valeurs SHAP.

    Returns
    -------
    dict
    """
    if not HAS_XGB:
        raise ImportError("xgboost requis : pip install xgboost")

    logger.info("=" * 60)
    logger.info("XGBOOST MODEL")
    logger.info("=" * 60)

    # Optimisation bayesienne
    best_params = bayesian_optimize(X_train, y_train, X_val, y_val, n_trials=n_trials)

    # Entrainement final avec early stopping
    scale_pos = max(1, int((y_train == 0).sum() / max((y_train == 1).sum(), 1)))

    final_params = {
        "n_estimators": 2000,
        "scale_pos_weight": scale_pos,
        "eval_metric": "logloss",
        "random_state": RANDOM_STATE,
        "n_jobs": -1,
        "verbosity": 0,
        **best_params,
    }

    logger.info("Entrainement final avec early stopping (%d rounds)...",
                EARLY_STOPPING_ROUNDS)

    model = xgb.XGBClassifier(**final_params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    best_iteration = model.best_iteration if hasattr(model, "best_iteration") else final_params["n_estimators"]
    logger.info("  Best iteration : %d", best_iteration)

    # Predictions
    val_proba = model.predict_proba(X_val)[:, 1]
    test_proba = model.predict_proba(X_test)[:, 1]

    val_metrics = compute_metrics(np.asarray(y_val), val_proba, prefix="val")
    test_metrics = compute_metrics(np.asarray(y_test), test_proba, prefix="test")

    logger.info("Validation : %s", val_metrics)
    logger.info("Test       : %s", test_metrics)

    # Feature importance (gain, cover, weight)
    feature_names = list(X_train.columns)
    importance_types = {}
    for imp_type in ["weight", "gain", "cover"]:
        booster = model.get_booster()
        raw_imp = booster.get_score(importance_type=imp_type)
        imp_dict = {}
        for feat in feature_names:
            imp_dict[feat] = raw_imp.get(feat, 0.0)
        importance_types[imp_type] = dict(sorted(imp_dict.items(), key=lambda x: -x[1]))

    logger.info("Top 10 features (gain) :")
    for feat, imp in list(importance_types["gain"].items())[:10]:
        logger.info("  %-30s : %.4f", feat, imp)

    # SHAP values
    shap_values_dict: Optional[dict] = None
    if compute_shap and HAS_SHAP:
        logger.info("Calcul des valeurs SHAP...")
        try:
            explainer = shap.TreeExplainer(model)
            shap_vals = explainer.shap_values(X_test.iloc[:500])
            mean_abs_shap = np.abs(shap_vals).mean(axis=0)
            shap_values_dict = dict(zip(feature_names, mean_abs_shap.tolist()))
            shap_values_dict = dict(sorted(shap_values_dict.items(), key=lambda x: -x[1]))
            logger.info("  SHAP calcule sur %d echantillons", min(500, len(X_test)))
        except Exception as e:
            logger.warning("  SHAP echoue : %s", e)
    elif compute_shap and not HAS_SHAP:
        logger.warning("  shap non installe, valeurs SHAP ignorees")

    return {
        "model": model,
        "best_params": best_params,
        "best_iteration": best_iteration,
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
        "importance_gain": importance_types["gain"],
        "importance_cover": importance_types["cover"],
        "importance_weight": importance_types["weight"],
        "shap_values": shap_values_dict,
        "feature_names": feature_names,
    }


# ===========================================================================
# SAVE
# ===========================================================================

def save_results(results: dict, model_name: str = "xgboost") -> Path:
    """Sauvegarde le modele et les metriques."""
    save_dir = SAVED_DIR / model_name
    save_dir.mkdir(parents=True, exist_ok=True)

    # Modele (format natif XGBoost + joblib)
    model_path = save_dir / f"{model_name}.joblib"
    joblib.dump(results["model"], model_path)

    xgb_path = save_dir / f"{model_name}.xgb"
    results["model"].save_model(str(xgb_path))
    logger.info("Modele sauvegarde : %s + %s", model_path, xgb_path)

    # Metriques
    metrics = {
        "model": model_name,
        "best_params": results["best_params"],
        "best_iteration": results["best_iteration"],
        **results["val_metrics"],
        **results["test_metrics"],
    }
    metrics_path = save_dir / "metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=True, default=str)

    # Feature importance (gain)
    imp_path = save_dir / "feature_importance_gain.json"
    with open(imp_path, "w", encoding="utf-8") as f:
        json.dump(results["importance_gain"], f, indent=2, ensure_ascii=True)

    imp_cover_path = save_dir / "feature_importance_cover.json"
    with open(imp_cover_path, "w", encoding="utf-8") as f:
        json.dump(results["importance_cover"], f, indent=2, ensure_ascii=True)

    imp_weight_path = save_dir / "feature_importance_weight.json"
    with open(imp_weight_path, "w", encoding="utf-8") as f:
        json.dump(results["importance_weight"], f, indent=2, ensure_ascii=True)

    # SHAP
    if results.get("shap_values"):
        shap_path = save_dir / "shap_values.json"
        with open(shap_path, "w", encoding="utf-8") as f:
            json.dump(results["shap_values"], f, indent=2, ensure_ascii=True)
        logger.info("SHAP sauvegarde : %s", shap_path)

    logger.info("Metriques sauvegardees : %s", metrics_path)
    return save_dir


# ===========================================================================
# PIPELINE
# ===========================================================================

def run(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_features_path: Optional[Path] = None,
    target: str = DEFAULT_TARGET,
    n_trials: int = N_TRIALS,
    compute_shap: bool = True,
) -> dict:
    """Pipeline complet XGBoost."""
    X_train, y_train, X_val, y_val, X_test, y_test, _ = load_and_split(
        parquet_path, selected_features_path, target,
    )
    results = train_xgboost(
        X_train, y_train, X_val, y_val, X_test, y_test,
        n_trials=n_trials,
        compute_shap=compute_shap,
    )
    save_dir = save_results(results)
    logger.info("Pipeline termine. Resultats dans : %s", save_dir)
    return results


# ===========================================================================
# CLI
# ===========================================================================

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="XGBoost model (script 23)",
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--selected-features", type=Path, default=None)
    parser.add_argument("--target", type=str, default=DEFAULT_TARGET)
    parser.add_argument("--n-trials", type=int, default=N_TRIALS,
                        help="Nombre de trials Optuna")
    parser.add_argument("--no-shap", action="store_true",
                        help="Desactiver le calcul SHAP")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    results = run(
        parquet_path=args.input,
        selected_features_path=args.selected_features,
        target=args.target,
        n_trials=args.n_trials,
        compute_shap=not args.no_shap,
    )
    test_ll = results["test_metrics"].get("test_log_loss", "N/A")
    test_auc = results["test_metrics"].get("test_roc_auc", "N/A")
    print(f"\n[OK] XGBoost — test log_loss={test_ll}, AUC={test_auc}")


if __name__ == "__main__":
    main()
