#!/usr/bin/env python3
"""
models/phase_04_ml_core/catboost_model.py
==========================================
Script 25 -- CatBoost pour la prediction hippique.

Fonctionnalites :
  - Gestion native des features categoriques
  - Support GPU optionnel
  - Creation de Pool depuis Parquet/JSONL
  - Optimisation des hyperparametres
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
    import catboost as cb
    HAS_CB = True
except ImportError:
    HAS_CB = False

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


logger = setup_logging("catboost_model")


# ===========================================================================
# DATA LOADING + POOL CREATION
# ===========================================================================

def load_and_split(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_features_path: Optional[Path] = None,
    target: str = DEFAULT_TARGET,
    date_col: str = DEFAULT_DATE_COL,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, list[str], list[int]]:
    """Charge les donnees et split temporel.

    Returns
    -------
    tuple
        (X_train, y_train, X_val, y_val, X_test, y_test, feature_names, cat_indices)
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

    # Indices des colonnes categoriques (pour CatBoost)
    cat_indices = [i for i, c in enumerate(feature_cols) if c in CATEGORICAL_COLS and c in df.columns]
    cat_names = [feature_cols[i] for i in cat_indices]

    # Preparer les categoriques (string pour CatBoost)
    for col in cat_names:
        for part in [train_df, val_df, test_df]:
            part[col] = part[col].fillna("MISSING").astype(str)

    # Remplir les numeriques
    num_cols = [c for c in feature_cols if c not in cat_names]
    medians = train_df[num_cols].median()

    def prepare(part: pd.DataFrame) -> pd.DataFrame:
        X = part[feature_cols].copy()
        X[num_cols] = X[num_cols].fillna(medians)
        return X

    X_train = prepare(train_df)
    y_train = train_df[target].astype(int)
    X_val = prepare(val_df)
    y_val = val_df[target].astype(int)
    X_test = prepare(test_df)
    y_test = test_df[target].astype(int)

    logger.info("  Split : train=%d, val=%d, test=%d", len(train_df), len(val_df), len(test_df))
    logger.info("  Features : %d (dont %d categoriques : %s)",
                len(feature_cols), len(cat_indices), cat_names)

    return X_train, y_train, X_val, y_val, X_test, y_test, feature_cols, cat_indices


def create_pool(
    X: pd.DataFrame,
    y: pd.Series,
    cat_indices: list[int],
) -> "cb.Pool":
    """Cree un Pool CatBoost.

    Parameters
    ----------
    X : pd.DataFrame
    y : pd.Series
    cat_indices : list[int]
        Indices des colonnes categoriques.

    Returns
    -------
    cb.Pool
    """
    if not HAS_CB:
        raise ImportError("catboost requis : pip install catboost")

    return cb.Pool(
        data=X,
        label=y,
        cat_features=cat_indices if cat_indices else None,
        feature_names=list(X.columns),
    )


def load_jsonl_to_pool(
    jsonl_path: Path,
    target: str = DEFAULT_TARGET,
    cat_indices: Optional[list[int]] = None,
) -> "cb.Pool":
    """Charge un fichier JSONL et cree un Pool CatBoost.

    Parameters
    ----------
    jsonl_path : Path
        Chemin vers le fichier JSONL.
    target : str
        Colonne cible.
    cat_indices : list[int], optional
        Indices des colonnes categoriques.

    Returns
    -------
    cb.Pool
    """
    logger.info("Chargement JSONL : %s", jsonl_path)
    df = pd.read_json(jsonl_path, lines=True)
    y = df[target].astype(int)

    exclude = {target, "date", "race_id", "horse_id", "horse_name"}
    feature_cols = [c for c in df.columns if c not in exclude]
    X = df[feature_cols]

    return create_pool(X, y, cat_indices or [])


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
# TRAINING
# ===========================================================================

def train_catboost(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    cat_indices: list[int],
    use_gpu: bool = False,
    n_trials: int = N_TRIALS,
) -> dict:
    """Entraine et evalue CatBoost.

    Parameters
    ----------
    X_train, y_train, X_val, y_val, X_test, y_test
    cat_indices : list[int]
        Indices des colonnes categoriques.
    use_gpu : bool
        Utiliser le GPU pour l'entrainement.
    n_trials : int
        Nombre de trials Optuna.

    Returns
    -------
    dict
    """
    if not HAS_CB:
        raise ImportError("catboost requis : pip install catboost")

    logger.info("=" * 60)
    logger.info("CATBOOST MODEL")
    logger.info("  GPU : %s", use_gpu)
    logger.info("  Cat indices : %s", cat_indices)
    logger.info("=" * 60)

    # Pools
    train_pool = create_pool(X_train, y_train, cat_indices)
    val_pool = create_pool(X_val, y_val, cat_indices)
    test_pool = create_pool(X_test, y_test, cat_indices)

    # Parametres de base
    task_type = "GPU" if use_gpu else "CPU"

    base_params = {
        "iterations": 2000,
        "depth": 6,
        "learning_rate": 0.05,
        "l2_leaf_reg": 3.0,
        "auto_class_weights": "Balanced",
        "loss_function": "Logloss",
        "eval_metric": "Logloss",
        "random_seed": RANDOM_STATE,
        "task_type": task_type,
        "verbose": 0,
        "od_type": "Iter",
        "od_wait": EARLY_STOPPING_ROUNDS,
    }

    # Optimisation bayesienne si optuna disponible
    best_optuna_params: dict = {}
    if HAS_OPTUNA and n_trials > 0:
        logger.info("Optimisation Optuna (%d trials)...", n_trials)
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        def objective(trial):
            trial_params = {
                **base_params,
                "depth": trial.suggest_int("depth", 4, 10),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1e-2, 10.0, log=True),
                "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 5.0),
                "random_strength": trial.suggest_float("random_strength", 0.0, 5.0),
                "border_count": trial.suggest_int("border_count", 32, 255),
            }

            if not use_gpu:
                trial_params["subsample"] = trial.suggest_float("subsample", 0.5, 1.0)

            model = cb.CatBoostClassifier(**trial_params)
            model.fit(train_pool, eval_set=val_pool, verbose=0)

            val_pred = model.predict_proba(val_pool)[:, 1]
            return float(log_loss(y_val, val_pred))

        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE),
        )
        study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

        best_optuna_params = study.best_trial.params
        base_params.update(best_optuna_params)
        logger.info("  Meilleurs params Optuna : %s", best_optuna_params)

    # Entrainement final
    logger.info("Entrainement final...")
    model = cb.CatBoostClassifier(**base_params)
    model.fit(train_pool, eval_set=val_pool, verbose=0)

    best_iteration = model.get_best_iteration()
    logger.info("  Best iteration : %d", best_iteration if best_iteration else base_params["iterations"])

    # Predictions
    val_proba = model.predict_proba(val_pool)[:, 1]
    test_proba = model.predict_proba(test_pool)[:, 1]

    val_metrics = compute_metrics(np.asarray(y_val), val_proba, prefix="val")
    test_metrics = compute_metrics(np.asarray(y_test), test_proba, prefix="test")

    logger.info("Validation : %s", val_metrics)
    logger.info("Test       : %s", test_metrics)

    # Feature importance
    feature_names = list(X_train.columns)
    imp_vals = model.get_feature_importance(train_pool)
    importance = dict(zip(feature_names, imp_vals.tolist()))
    importance = dict(sorted(importance.items(), key=lambda x: -x[1]))

    logger.info("Top 10 features :")
    for feat, imp in list(importance.items())[:10]:
        logger.info("  %-30s : %.4f", feat, imp)

    # Feature importance par type
    imp_prediction = {}
    try:
        imp_pred_vals = model.get_feature_importance(train_pool, type="PredictionValuesChange")
        imp_prediction = dict(zip(feature_names, imp_pred_vals.tolist()))
        imp_prediction = dict(sorted(imp_prediction.items(), key=lambda x: -x[1]))
    except Exception as e:
        logger.debug("PredictionValuesChange importance not available: %s", e)

    return {
        "model": model,
        "params": base_params,
        "best_optuna_params": best_optuna_params,
        "best_iteration": best_iteration,
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
        "feature_importance": importance,
        "feature_importance_prediction": imp_prediction,
        "feature_names": feature_names,
        "cat_indices": cat_indices,
    }


# ===========================================================================
# SAVE
# ===========================================================================

def save_results(results: dict, model_name: str = "catboost") -> Path:
    """Sauvegarde le modele et les metriques."""
    save_dir = SAVED_DIR / model_name
    save_dir.mkdir(parents=True, exist_ok=True)

    # Modele natif CatBoost
    cb_path = save_dir / f"{model_name}.cbm"
    results["model"].save_model(str(cb_path))
    logger.info("Modele sauvegarde : %s", cb_path)

    # Aussi en joblib
    joblib_path = save_dir / f"{model_name}.joblib"
    joblib.dump(results["model"], joblib_path)

    # Metriques
    serializable_params = {}
    for k, v in results["params"].items():
        try:
            json.dumps(v)
            serializable_params[k] = v
        except (TypeError, ValueError):
            serializable_params[k] = str(v)

    metrics = {
        "model": model_name,
        "best_iteration": results["best_iteration"],
        "cat_indices": results["cat_indices"],
        "params": serializable_params,
        **results["val_metrics"],
        **results["test_metrics"],
    }
    metrics_path = save_dir / "metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=True, default=str)

    # Feature importance
    imp_path = save_dir / "feature_importance.json"
    with open(imp_path, "w", encoding="utf-8") as f:
        json.dump(results["feature_importance"], f, indent=2, ensure_ascii=True)

    if results.get("feature_importance_prediction"):
        imp_pred_path = save_dir / "feature_importance_prediction.json"
        with open(imp_pred_path, "w", encoding="utf-8") as f:
            json.dump(results["feature_importance_prediction"], f, indent=2, ensure_ascii=True)

    logger.info("Metriques sauvegardees : %s", metrics_path)
    return save_dir


# ===========================================================================
# PIPELINE
# ===========================================================================

def run(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_features_path: Optional[Path] = None,
    target: str = DEFAULT_TARGET,
    use_gpu: bool = False,
    n_trials: int = N_TRIALS,
) -> dict:
    """Pipeline complet CatBoost."""
    X_train, y_train, X_val, y_val, X_test, y_test, _, cat_indices = load_and_split(
        parquet_path, selected_features_path, target,
    )
    results = train_catboost(
        X_train, y_train, X_val, y_val, X_test, y_test,
        cat_indices=cat_indices,
        use_gpu=use_gpu,
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
        description="CatBoost model (script 25)",
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--selected-features", type=Path, default=None)
    parser.add_argument("--target", type=str, default=DEFAULT_TARGET)
    parser.add_argument("--gpu", action="store_true",
                        help="Utiliser le GPU")
    parser.add_argument("--n-trials", type=int, default=N_TRIALS,
                        help="Nombre de trials Optuna")
    parser.add_argument("--jsonl", type=Path, default=None,
                        help="Charger depuis un fichier JSONL (au lieu de Parquet)")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    results = run(
        parquet_path=args.input,
        selected_features_path=args.selected_features,
        target=args.target,
        use_gpu=args.gpu,
        n_trials=args.n_trials,
    )
    test_ll = results["test_metrics"].get("test_log_loss", "N/A")
    test_auc = results["test_metrics"].get("test_roc_auc", "N/A")
    print(f"\n[OK] CatBoost — test log_loss={test_ll}, AUC={test_auc}")


if __name__ == "__main__":
    main()
