#!/usr/bin/env python3
"""
models/phase_04_ml_core/logistic_regression_baseline.py
========================================================
Script 21 -- Logistic Regression baseline pour la prediction hippique.

Fonctionnalites :
  - Multi-classe (win / place / show) ou binaire (is_winner)
  - Tuning de la regularisation (C) via cross-validation
  - Extraction de l'importance des features (coefficients)
  - Validation croisee stratifiee
  - Sauvegarde du modele + metriques

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline

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

# Grille de regularisation
C_GRID = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
N_FOLDS = 5
RANDOM_STATE = 42


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


logger = setup_logging("logistic_regression_baseline")


# ===========================================================================
# DATA LOADING + SPLIT
# ===========================================================================

def load_features(
    path: Path,
    selected_features_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Charge le Parquet et retourne le DataFrame complet.

    Parameters
    ----------
    path : Path
        Chemin vers le fichier Parquet.
    selected_features_path : Path, optional
        JSON contenant la liste des features selectionnees.

    Returns
    -------
    pd.DataFrame
    """
    logger.info("Chargement : %s", path)
    df = pd.read_parquet(path)
    logger.info("  Shape brute : %s", df.shape)

    if selected_features_path and selected_features_path.exists():
        with open(selected_features_path, "r", encoding="utf-8") as f:
            sel = json.load(f)
        feat_list = sel.get("selected_features") or sel.get("optimal", {}).get("features", [])
        if feat_list:
            keep_cols = [c for c in feat_list if c in df.columns]
            meta_cols = [c for c in df.columns if c not in feat_list and not pd.api.types.is_numeric_dtype(df[c])]
            meta_cols += [DEFAULT_TARGET, DEFAULT_DATE_COL, "race_id"]
            keep_cols = list(set(keep_cols + [c for c in meta_cols if c in df.columns]))
            df = df[keep_cols]
            logger.info("  Features filtrees : %d colonnes", len(keep_cols))

    return df


def split_by_date(
    df: pd.DataFrame,
    target: str = DEFAULT_TARGET,
    date_col: str = DEFAULT_DATE_COL,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    """Split temporel train / val / test.

    Parameters
    ----------
    df : pd.DataFrame
    target : str
    date_col : str
    train_ratio : float
    val_ratio : float

    Returns
    -------
    tuple
        (X_train, y_train, X_val, y_val, X_test, y_test)
    """
    if date_col in df.columns:
        df = df.sort_values(date_col).reset_index(drop=True)

    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train = df.iloc[:train_end]
    val = df.iloc[train_end:val_end]
    test = df.iloc[val_end:]

    exclude = {target, date_col, "race_id", "horse_id", "horse_name", "jockey", "trainer"}
    feature_cols = [
        c for c in df.columns
        if c not in exclude and pd.api.types.is_numeric_dtype(df[c])
    ]

    X_train = train[feature_cols].fillna(train[feature_cols].median())
    y_train = train[target].astype(int)
    X_val = val[feature_cols].fillna(train[feature_cols].median())
    y_val = val[target].astype(int)
    X_test = test[feature_cols].fillna(train[feature_cols].median())
    y_test = test[target].astype(int)

    logger.info("  Split : train=%d, val=%d, test=%d", len(train), len(val), len(test))
    logger.info("  Win rate : train=%.3f, val=%.3f, test=%.3f",
                y_train.mean(), y_val.mean(), y_test.mean())

    return X_train, y_train, X_val, y_val, X_test, y_test


# ===========================================================================
# METRIQUES
# ===========================================================================

def compute_metrics(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    prefix: str = "",
) -> dict[str, float]:
    """Calcule accuracy, log_loss, ROC-AUC et ROI simule.

    Parameters
    ----------
    y_true : np.ndarray
    y_proba : np.ndarray
    prefix : str
        Prefixe pour les cles du dict.

    Returns
    -------
    dict[str, float]
    """
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

    # ROI simule : parier 1 euro sur chaque cheval predit gagnant
    # Cote estimee ~= 1/proba pour les favoris
    if y_pred.sum() > 0:
        stakes = float(y_pred.sum())
        # Gains : cote estimee * mise pour chaque victoire correcte
        wins_mask = (y_pred == 1) & (y_true == 1)
        gains = 0.0
        for i in np.where(wins_mask)[0]:
            cote = min(1.0 / max(y_proba[i], 0.01), 50.0)
            gains += cote
        roi = (gains - stakes) / stakes
        metrics[f"{p}roi_simulated"] = round(roi, 6)
    else:
        metrics[f"{p}roi_simulated"] = 0.0

    return metrics


# ===========================================================================
# TRAINING
# ===========================================================================

def train_logistic_regression(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    c_grid: Optional[list[float]] = None,
    multiclass: bool = False,
) -> dict:
    """Entraine et evalue le modele Logistic Regression.

    Parameters
    ----------
    X_train, y_train : donnees d'entrainement
    X_val, y_val : donnees de validation
    X_test, y_test : donnees de test
    c_grid : list[float], optional
        Valeurs de C a tester.
    multiclass : bool
        Si True, multi-classe (win/place/show). Sinon, binaire.

    Returns
    -------
    dict
        Resultats avec modele, metriques et importances.
    """
    if c_grid is None:
        c_grid = C_GRID

    logger.info("=" * 60)
    logger.info("LOGISTIC REGRESSION BASELINE")
    logger.info("  Multiclass : %s", multiclass)
    logger.info("  C grid : %s", c_grid)
    logger.info("=" * 60)

    # Pipeline : StandardScaler + LogisticRegression
    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LogisticRegression(
            solver="saga",
            max_iter=3000,
            class_weight="balanced",
            random_state=RANDOM_STATE,
            multi_class="multinomial" if multiclass else "auto",
        )),
    ])

    # Grid search sur C
    param_grid = {"lr__C": c_grid}
    cv = StratifiedKFold(n_splits=N_FOLDS, shuffle=True, random_state=RANDOM_STATE)

    logger.info("Grid search sur C (%d folds)...", N_FOLDS)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        grid = GridSearchCV(
            pipe, param_grid,
            cv=cv,
            scoring="neg_log_loss",
            n_jobs=-1,
            refit=True,
            verbose=0,
        )
        grid.fit(X_train, y_train)

    best_c = grid.best_params_["lr__C"]
    logger.info("  Meilleur C : %.4f (CV score : %.6f)", best_c, grid.best_score_)

    best_model = grid.best_estimator_

    # Predictions
    val_proba = best_model.predict_proba(X_val)[:, 1]
    test_proba = best_model.predict_proba(X_test)[:, 1]

    # Metriques
    val_metrics = compute_metrics(np.asarray(y_val), val_proba, prefix="val")
    test_metrics = compute_metrics(np.asarray(y_test), test_proba, prefix="test")

    logger.info("Validation : %s", {k: v for k, v in val_metrics.items()})
    logger.info("Test       : %s", {k: v for k, v in test_metrics.items()})

    # Feature importance (coefficients absolus)
    lr_model = best_model.named_steps["lr"]
    coefs = np.abs(lr_model.coef_[0])
    feature_names = list(X_train.columns)
    importance = dict(zip(feature_names, coefs.tolist()))
    importance = dict(sorted(importance.items(), key=lambda x: -x[1]))

    top_10 = list(importance.items())[:10]
    logger.info("Top 10 features :")
    for feat, imp in top_10:
        logger.info("  %-30s : %.6f", feat, imp)

    # Cross-validation sur tout le train
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        from sklearn.model_selection import cross_val_score
        cv_scores = cross_val_score(
            best_model, X_train, y_train,
            cv=cv, scoring="neg_log_loss", n_jobs=-1,
        )
    logger.info("CV log_loss : %.6f (+/- %.6f)", -cv_scores.mean(), cv_scores.std())

    return {
        "model": best_model,
        "best_C": best_c,
        "cv_log_loss_mean": round(-float(cv_scores.mean()), 6),
        "cv_log_loss_std": round(float(cv_scores.std()), 6),
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
        "feature_importance": importance,
        "feature_names": feature_names,
    }


# ===========================================================================
# SAVE
# ===========================================================================

def save_results(results: dict, model_name: str = "logistic_regression") -> Path:
    """Sauvegarde le modele et les metriques.

    Parameters
    ----------
    results : dict
        Resultats du training.
    model_name : str
        Nom du modele.

    Returns
    -------
    Path
        Repertoire de sauvegarde.
    """
    save_dir = SAVED_DIR / model_name
    save_dir.mkdir(parents=True, exist_ok=True)

    # Modele
    model_path = save_dir / f"{model_name}.joblib"
    joblib.dump(results["model"], model_path)
    logger.info("Modele sauvegarde : %s", model_path)

    # Metriques
    metrics = {
        "model": model_name,
        "best_C": results["best_C"],
        "cv_log_loss_mean": results["cv_log_loss_mean"],
        "cv_log_loss_std": results["cv_log_loss_std"],
        **results["val_metrics"],
        **results["test_metrics"],
    }
    metrics_path = save_dir / "metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=True)
    logger.info("Metriques sauvegardees : %s", metrics_path)

    # Feature importance
    imp_path = save_dir / "feature_importance.json"
    with open(imp_path, "w", encoding="utf-8") as f:
        json.dump(results["feature_importance"], f, indent=2, ensure_ascii=True)
    logger.info("Importances sauvegardees : %s", imp_path)

    return save_dir


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def run(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_features_path: Optional[Path] = None,
    target: str = DEFAULT_TARGET,
    multiclass: bool = False,
) -> dict:
    """Pipeline complet : chargement, split, training, sauvegarde.

    Parameters
    ----------
    parquet_path : Path
    selected_features_path : Path, optional
    target : str
    multiclass : bool

    Returns
    -------
    dict
    """
    df = load_features(parquet_path, selected_features_path)
    X_train, y_train, X_val, y_val, X_test, y_test = split_by_date(df, target=target)

    results = train_logistic_regression(
        X_train, y_train, X_val, y_val, X_test, y_test,
        multiclass=multiclass,
    )

    save_dir = save_results(results)
    logger.info("Pipeline termine. Resultats dans : %s", save_dir)
    return results


# ===========================================================================
# CLI
# ===========================================================================

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Logistic Regression baseline (script 21)",
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_PARQUET,
        help="Fichier Parquet d'entree",
    )
    parser.add_argument(
        "--selected-features", type=Path, default=None,
        help="JSON des features selectionnees",
    )
    parser.add_argument(
        "--target", type=str, default=DEFAULT_TARGET,
        help="Colonne cible",
    )
    parser.add_argument(
        "--multiclass", action="store_true",
        help="Mode multi-classe (win/place/show)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    results = run(
        parquet_path=args.input,
        selected_features_path=args.selected_features,
        target=args.target,
        multiclass=args.multiclass,
    )
    test_ll = results["test_metrics"].get("test_log_loss", "N/A")
    test_acc = results["test_metrics"].get("test_accuracy", "N/A")
    print(f"\n[OK] LogisticRegression — test log_loss={test_ll}, accuracy={test_acc}")


if __name__ == "__main__":
    main()
