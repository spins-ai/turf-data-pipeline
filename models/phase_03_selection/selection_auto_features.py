#!/usr/bin/env python3
"""
models/phase_03_selection/selection_auto_features.py
=====================================================
Script 19 -- Selection automatique de features pour la prediction hippique.

Methodes appliquees sequentiellement :
  1. Filtrage de correlation (seuil > 0.95)
  2. Seuil de variance (VarianceThreshold)
  3. Information mutuelle (mutual_info_classif)
  4. Selection L1 (Lasso / LogisticRegression)
  5. Importance arborescente (XGBoost feature_importances_)

Produit un fichier JSON avec la liste finale des features selectionnees.

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.feature_selection import (
    VarianceThreshold,
    SelectFromModel,
    mutual_info_classif,
)
from sklearn.linear_model import LogisticRegression

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

# ===========================================================================
# CONFIG
# ===========================================================================

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA_DIR = ROOT / "models" / "data"
OUTPUT_DIR = ROOT / "models" / "phase_03_selection"

DEFAULT_PARQUET = DATA_DIR / "features_master.parquet"
DEFAULT_TARGET = "is_winner"
DEFAULT_DATE_COL = "date"

CORRELATION_THRESHOLD = 0.95
VARIANCE_THRESHOLD = 0.01
MI_TOP_K = 80
L1_C = 0.05
XGB_TOP_K = 60


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


logger = setup_logging("selection_auto_features")


# ===========================================================================
# DATA LOADING
# ===========================================================================

def load_data(
    path: Path,
    target: str = DEFAULT_TARGET,
    date_col: str = DEFAULT_DATE_COL,
) -> tuple[pd.DataFrame, pd.Series]:
    """Charge le Parquet et separe features / cible.

    Parameters
    ----------
    path : Path
        Chemin vers le fichier Parquet.
    target : str
        Colonne cible.
    date_col : str
        Colonne de date (exclue des features).

    Returns
    -------
    tuple[pd.DataFrame, pd.Series]
        (X, y) avec X uniquement numerique.
    """
    logger.info("Chargement des donnees : %s", path)
    df = pd.read_parquet(path)
    logger.info("  Shape brute : %s", df.shape)

    if target not in df.columns:
        raise ValueError(f"Colonne cible '{target}' absente du DataFrame")

    y = df[target].astype(int)

    # Exclure colonnes non-features
    exclude = {target, date_col, "race_id", "horse_id", "horse_name", "jockey", "trainer"}
    feature_cols = [
        c for c in df.columns
        if c not in exclude and pd.api.types.is_numeric_dtype(df[c])
    ]
    X = df[feature_cols].copy()

    # Remplir NaN restants par la mediane
    X = X.fillna(X.median())

    logger.info("  Features numeriques : %d | Echantillons : %d", X.shape[1], X.shape[0])
    return X, y


# ===========================================================================
# METHODES DE SELECTION
# ===========================================================================

def filter_high_correlation(
    X: pd.DataFrame,
    threshold: float = CORRELATION_THRESHOLD,
) -> list[str]:
    """Supprime les features trop correlees (> threshold).

    Parameters
    ----------
    X : pd.DataFrame
        Matrice de features.
    threshold : float
        Seuil de correlation absolue.

    Returns
    -------
    list[str]
        Features a conserver.
    """
    logger.info("--- Filtrage correlation (seuil=%.2f) ---", threshold)
    corr = X.corr().abs()
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))

    to_drop = set()
    for col in upper.columns:
        high = upper.index[upper[col] > threshold].tolist()
        if high:
            to_drop.add(col)

    kept = [c for c in X.columns if c not in to_drop]
    logger.info("  Supprimees : %d | Conservees : %d", len(to_drop), len(kept))
    return kept


def filter_low_variance(
    X: pd.DataFrame,
    threshold: float = VARIANCE_THRESHOLD,
) -> list[str]:
    """Supprime les features a faible variance.

    Parameters
    ----------
    X : pd.DataFrame
        Matrice de features.
    threshold : float
        Seuil de variance minimale.

    Returns
    -------
    list[str]
        Features a conserver.
    """
    logger.info("--- Filtrage variance (seuil=%.4f) ---", threshold)
    selector = VarianceThreshold(threshold=threshold)
    selector.fit(X)
    mask = selector.get_support()
    kept = X.columns[mask].tolist()
    dropped = X.shape[1] - len(kept)
    logger.info("  Supprimees : %d | Conservees : %d", dropped, len(kept))
    return kept


def select_mutual_information(
    X: pd.DataFrame,
    y: pd.Series,
    top_k: int = MI_TOP_K,
) -> list[str]:
    """Selectionne les top-K features par information mutuelle.

    Parameters
    ----------
    X : pd.DataFrame
        Matrice de features.
    y : pd.Series
        Cible binaire.
    top_k : int
        Nombre de features a garder.

    Returns
    -------
    list[str]
        Features selectionnees.
    """
    logger.info("--- Information mutuelle (top_k=%d) ---", top_k)
    mi_scores = mutual_info_classif(X, y, random_state=42, n_neighbors=5)
    mi_series = pd.Series(mi_scores, index=X.columns).sort_values(ascending=False)

    top_k = min(top_k, len(mi_series))
    kept = mi_series.head(top_k).index.tolist()
    logger.info("  Top MI score : %.4f | Min retenu : %.4f",
                mi_series.iloc[0], mi_series.iloc[top_k - 1])
    return kept


def select_l1_lasso(
    X: pd.DataFrame,
    y: pd.Series,
    C: float = L1_C,
) -> list[str]:
    """Selectionne les features via regularisation L1 (Lasso).

    Parameters
    ----------
    X : pd.DataFrame
        Matrice de features (standardisee en interne).
    y : pd.Series
        Cible binaire.
    C : float
        Inverse de la force de regularisation.

    Returns
    -------
    list[str]
        Features avec coefficients non-nuls.
    """
    logger.info("--- Selection L1 / Lasso (C=%.4f) ---", C)

    # Standardisation rapide pour L1
    X_std = (X - X.mean()) / (X.std() + 1e-8)

    lr = LogisticRegression(
        penalty="l1",
        C=C,
        solver="saga",
        max_iter=2000,
        random_state=42,
        class_weight="balanced",
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        lr.fit(X_std, y)

    selector = SelectFromModel(lr, prefit=True)
    mask = selector.get_support()
    kept = X.columns[mask].tolist()
    logger.info("  Features non-nulles : %d / %d", len(kept), X.shape[1])
    return kept


def select_tree_importance(
    X: pd.DataFrame,
    y: pd.Series,
    top_k: int = XGB_TOP_K,
) -> list[str]:
    """Selectionne les top-K features par importance XGBoost.

    Parameters
    ----------
    X : pd.DataFrame
        Matrice de features.
    y : pd.Series
        Cible binaire.
    top_k : int
        Nombre de features a garder.

    Returns
    -------
    list[str]
        Features selectionnees.
    """
    if not HAS_XGB:
        logger.warning("XGBoost non disponible, etape ignoree")
        return list(X.columns)

    logger.info("--- Importance XGBoost (top_k=%d) ---", top_k)

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=max(1, int((y == 0).sum() / max((y == 1).sum(), 1))),
        eval_metric="logloss",
        random_state=42,
        n_jobs=-1,
        verbosity=0,
    )
    model.fit(X, y)

    imp = pd.Series(model.feature_importances_, index=X.columns)
    imp = imp.sort_values(ascending=False)

    top_k = min(top_k, len(imp))
    kept = imp.head(top_k).index.tolist()
    logger.info("  Top importance : %.4f | Min retenu : %.4f",
                imp.iloc[0], imp.iloc[top_k - 1])
    return kept


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def run_auto_selection(
    parquet_path: Path = DEFAULT_PARQUET,
    target: str = DEFAULT_TARGET,
    corr_threshold: float = CORRELATION_THRESHOLD,
    var_threshold: float = VARIANCE_THRESHOLD,
    mi_top_k: int = MI_TOP_K,
    l1_c: float = L1_C,
    xgb_top_k: int = XGB_TOP_K,
    output_path: Optional[Path] = None,
) -> list[str]:
    """Execute le pipeline complet de selection automatique.

    Applique les 5 methodes et conserve l'intersection des features
    retenues par au moins 3 methodes sur 5 (vote majoritaire).

    Parameters
    ----------
    parquet_path : Path
        Fichier de donnees.
    target : str
        Colonne cible.
    corr_threshold : float
        Seuil de correlation.
    var_threshold : float
        Seuil de variance.
    mi_top_k : int
        Top-K information mutuelle.
    l1_c : float
        Regularisation L1.
    xgb_top_k : int
        Top-K importance XGBoost.
    output_path : Path, optional
        Fichier de sortie JSON.

    Returns
    -------
    list[str]
        Liste finale des features selectionnees.
    """
    X, y = load_data(parquet_path, target=target)
    all_features = list(X.columns)

    logger.info("=" * 60)
    logger.info("SELECTION AUTOMATIQUE DE FEATURES")
    logger.info("  Features initiales : %d", len(all_features))
    logger.info("=" * 60)

    # 1. Correlation
    kept_corr = set(filter_high_correlation(X, threshold=corr_threshold))

    # 2. Variance (sur features post-correlation)
    X_corr = X[list(kept_corr)]
    kept_var = set(filter_low_variance(X_corr, threshold=var_threshold))

    # 3. Information mutuelle (sur toutes les features)
    kept_mi = set(select_mutual_information(X, y, top_k=mi_top_k))

    # 4. L1 / Lasso (sur toutes les features)
    kept_l1 = set(select_l1_lasso(X, y, C=l1_c))

    # 5. Importance XGBoost (sur toutes les features)
    kept_xgb = set(select_tree_importance(X, y, top_k=xgb_top_k))

    # Vote majoritaire : garder si retenu par >= 3 methodes sur 5
    votes: dict[str, int] = {}
    for feat in all_features:
        count = sum([
            feat in kept_corr,
            feat in kept_var,
            feat in kept_mi,
            feat in kept_l1,
            feat in kept_xgb,
        ])
        votes[feat] = count

    min_votes = 3
    selected = sorted([f for f, v in votes.items() if v >= min_votes])

    # Garantir au moins 10 features
    if len(selected) < 10:
        logger.warning("Moins de 10 features retenues, abaissement du seuil de vote")
        min_votes = 2
        selected = sorted([f for f, v in votes.items() if v >= min_votes])

    logger.info("=" * 60)
    logger.info("RESULTAT FINAL")
    logger.info("  Features selectionnees : %d / %d", len(selected), len(all_features))
    logger.info("  Seuil de vote : >= %d/5 methodes", min_votes)
    logger.info("=" * 60)

    # Sauvegarder le resultat
    if output_path is None:
        output_path = OUTPUT_DIR / "selected_features.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    result = {
        "n_initial": len(all_features),
        "n_selected": len(selected),
        "min_votes": min_votes,
        "methods": {
            "correlation": {"threshold": corr_threshold, "kept": len(kept_corr)},
            "variance": {"threshold": var_threshold, "kept": len(kept_var)},
            "mutual_info": {"top_k": mi_top_k, "kept": len(kept_mi)},
            "l1_lasso": {"C": l1_c, "kept": len(kept_l1)},
            "xgboost": {"top_k": xgb_top_k, "kept": len(kept_xgb)},
        },
        "votes": {f: votes[f] for f in selected},
        "selected_features": selected,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=True)

    logger.info("Sauvegarde : %s", output_path)

    return selected


# ===========================================================================
# CLI
# ===========================================================================

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Selection automatique de features (script 19)",
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_PARQUET,
        help="Fichier Parquet d'entree",
    )
    parser.add_argument(
        "--target", type=str, default=DEFAULT_TARGET,
        help="Colonne cible",
    )
    parser.add_argument(
        "--corr-threshold", type=float, default=CORRELATION_THRESHOLD,
        help="Seuil de correlation",
    )
    parser.add_argument(
        "--var-threshold", type=float, default=VARIANCE_THRESHOLD,
        help="Seuil de variance",
    )
    parser.add_argument(
        "--mi-top-k", type=int, default=MI_TOP_K,
        help="Top-K information mutuelle",
    )
    parser.add_argument(
        "--l1-c", type=float, default=L1_C,
        help="Regularisation L1 (C)",
    )
    parser.add_argument(
        "--xgb-top-k", type=int, default=XGB_TOP_K,
        help="Top-K importance XGBoost",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Fichier JSON de sortie",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    selected = run_auto_selection(
        parquet_path=args.input,
        target=args.target,
        corr_threshold=args.corr_threshold,
        var_threshold=args.var_threshold,
        mi_top_k=args.mi_top_k,
        l1_c=args.l1_c,
        xgb_top_k=args.xgb_top_k,
        output_path=args.output,
    )
    print(f"\n[OK] {len(selected)} features selectionnees.")


if __name__ == "__main__":
    main()
