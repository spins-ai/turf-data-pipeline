#!/usr/bin/env python3
"""
models/phase_03_selection/feature_subset_optimizer.py
=====================================================
Script 20 -- Optimisation de sous-ensembles de features.

Methodes :
  1. Selection forward (ajout sequentiel)
  2. Selection backward (suppression sequentielle)
  3. Algorithme genetique (evolution de sous-ensembles)
  4. Scoring par validation croisee (log_loss / accuracy)

Produit le sous-ensemble optimal et sauvegarde en JSON.

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.ensemble import GradientBoostingClassifier

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
SELECTION_DIR = ROOT / "models" / "phase_03_selection"
OUTPUT_DIR = SELECTION_DIR

DEFAULT_PARQUET = DATA_DIR / "features_master.parquet"
DEFAULT_SELECTED = SELECTION_DIR / "selected_features.json"
DEFAULT_TARGET = "is_winner"
DEFAULT_DATE_COL = "date"

N_FOLDS = 5
RANDOM_STATE = 42

# Genetique
GA_POPULATION = 30
GA_GENERATIONS = 20
GA_MUTATION_RATE = 0.1
GA_ELITE_RATIO = 0.2


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


logger = setup_logging("feature_subset_optimizer")


# ===========================================================================
# DATA LOADING
# ===========================================================================

def load_data(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_path: Optional[Path] = DEFAULT_SELECTED,
    target: str = DEFAULT_TARGET,
) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    """Charge les donnees et filtre sur les features pre-selectionnees.

    Parameters
    ----------
    parquet_path : Path
        Fichier Parquet.
    selected_path : Path, optional
        Fichier JSON des features selectionnees (script 19).
        Si None ou inexistant, utilise toutes les features numeriques.
    target : str
        Colonne cible.

    Returns
    -------
    tuple[pd.DataFrame, pd.Series, list[str]]
        (X, y, feature_names)
    """
    logger.info("Chargement : %s", parquet_path)
    df = pd.read_parquet(parquet_path)

    y = df[target].astype(int)

    # Charger la liste pre-selectionnee si disponible
    feature_cols: list[str] = []
    if selected_path and selected_path.exists():
        with open(selected_path, "r", encoding="utf-8") as f:
            sel_data = json.load(f)
        feature_cols = sel_data.get("selected_features", [])
        feature_cols = [c for c in feature_cols if c in df.columns]
        logger.info("  Features pre-selectionnees chargees : %d", len(feature_cols))

    if not feature_cols:
        exclude = {target, "date", "race_id", "horse_id", "horse_name", "jockey", "trainer"}
        feature_cols = [
            c for c in df.columns
            if c not in exclude and pd.api.types.is_numeric_dtype(df[c])
        ]

    X = df[feature_cols].fillna(df[feature_cols].median())
    logger.info("  Shape : %s", X.shape)
    return X, y, feature_cols


# ===========================================================================
# SCORING
# ===========================================================================

def _get_estimator():
    """Retourne un estimateur rapide pour le scoring."""
    if HAS_XGB:
        return xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            eval_metric="logloss",
            random_state=RANDOM_STATE,
            n_jobs=-1,
            verbosity=0,
        )
    return GradientBoostingClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        random_state=RANDOM_STATE,
    )


def cv_score(
    X: pd.DataFrame,
    y: pd.Series,
    feature_subset: list[str],
    n_folds: int = N_FOLDS,
) -> float:
    """Score de validation croisee (neg_log_loss) pour un sous-ensemble.

    Parameters
    ----------
    X : pd.DataFrame
        Donnees completes.
    y : pd.Series
        Cible.
    feature_subset : list[str]
        Sous-ensemble de features.
    n_folds : int
        Nombre de folds.

    Returns
    -------
    float
        Score moyen (neg_log_loss, plus haut = meilleur).
    """
    if not feature_subset:
        return -np.inf

    X_sub = X[feature_subset]
    cv = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    estimator = _get_estimator()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        scores = cross_val_score(
            estimator, X_sub, y,
            cv=cv, scoring="neg_log_loss", n_jobs=-1,
        )
    return float(scores.mean())


# ===========================================================================
# FORWARD SELECTION
# ===========================================================================

def forward_selection(
    X: pd.DataFrame,
    y: pd.Series,
    max_features: int = 30,
) -> tuple[list[str], float]:
    """Selection forward : ajoute les features une par une.

    Parameters
    ----------
    X : pd.DataFrame
        Donnees completes.
    y : pd.Series
        Cible.
    max_features : int
        Nombre maximum de features.

    Returns
    -------
    tuple[list[str], float]
        (meilleur sous-ensemble, meilleur score)
    """
    logger.info("--- Forward selection (max=%d) ---", max_features)
    remaining = list(X.columns)
    selected: list[str] = []
    best_score = -np.inf

    for step in range(min(max_features, len(remaining))):
        scores: dict[str, float] = {}
        for feat in remaining:
            candidate = selected + [feat]
            scores[feat] = cv_score(X, y, candidate)

        best_feat = max(scores, key=scores.get)  # type: ignore[arg-type]
        new_score = scores[best_feat]

        # Arreter si pas d'amelioration
        if new_score <= best_score and step > 5:
            logger.info("  Arret a l'etape %d (pas d'amelioration)", step)
            break

        selected.append(best_feat)
        remaining.remove(best_feat)
        best_score = new_score
        logger.info("  +%s | score=%.6f (%d features)", best_feat, best_score, len(selected))

    return selected, best_score


# ===========================================================================
# BACKWARD SELECTION
# ===========================================================================

def backward_selection(
    X: pd.DataFrame,
    y: pd.Series,
    min_features: int = 10,
) -> tuple[list[str], float]:
    """Selection backward : supprime les features une par une.

    Parameters
    ----------
    X : pd.DataFrame
        Donnees completes.
    y : pd.Series
        Cible.
    min_features : int
        Nombre minimum de features.

    Returns
    -------
    tuple[list[str], float]
        (meilleur sous-ensemble, meilleur score)
    """
    logger.info("--- Backward selection (min=%d) ---", min_features)
    selected = list(X.columns)
    best_score = cv_score(X, y, selected)
    logger.info("  Score initial (%d features) : %.6f", len(selected), best_score)

    while len(selected) > min_features:
        scores: dict[str, float] = {}
        for feat in selected:
            candidate = [f for f in selected if f != feat]
            scores[feat] = cv_score(X, y, candidate)

        # Feature dont la suppression ameliore le plus (ou degrade le moins)
        worst_feat = max(scores, key=scores.get)  # type: ignore[arg-type]
        new_score = scores[worst_feat]

        if new_score < best_score and len(selected) <= 20:
            logger.info("  Arret a %d features (suppression degraderait)", len(selected))
            break

        selected.remove(worst_feat)
        best_score = max(new_score, best_score)
        logger.info("  -%s | score=%.6f (%d features)", worst_feat, best_score, len(selected))

    return selected, best_score


# ===========================================================================
# ALGORITHME GENETIQUE
# ===========================================================================

def genetic_algorithm(
    X: pd.DataFrame,
    y: pd.Series,
    population_size: int = GA_POPULATION,
    generations: int = GA_GENERATIONS,
    mutation_rate: float = GA_MUTATION_RATE,
    elite_ratio: float = GA_ELITE_RATIO,
) -> tuple[list[str], float]:
    """Optimisation genetique de sous-ensembles de features.

    Parameters
    ----------
    X : pd.DataFrame
        Donnees completes.
    y : pd.Series
        Cible.
    population_size : int
        Taille de la population.
    generations : int
        Nombre de generations.
    mutation_rate : float
        Taux de mutation par gene.
    elite_ratio : float
        Proportion d'elite conservee.

    Returns
    -------
    tuple[list[str], float]
        (meilleur sous-ensemble, meilleur score)
    """
    logger.info("--- Algorithme genetique (pop=%d, gen=%d) ---",
                population_size, generations)

    features = list(X.columns)
    n_features = len(features)
    rng = random.Random(RANDOM_STATE)
    n_elite = max(2, int(population_size * elite_ratio))

    # Initialiser la population (vecteurs binaires)
    population: list[list[int]] = []
    for _ in range(population_size):
        # Chaque individu active ~50% des features
        chrom = [1 if rng.random() < 0.5 else 0 for _ in range(n_features)]
        # Au moins 5 features
        if sum(chrom) < 5:
            indices = rng.sample(range(n_features), 5)
            for idx in indices:
                chrom[idx] = 1
        population.append(chrom)

    def decode(chrom: list[int]) -> list[str]:
        return [features[i] for i, v in enumerate(chrom) if v == 1]

    def evaluate(chrom: list[int]) -> float:
        subset = decode(chrom)
        if len(subset) < 3:
            return -np.inf
        return cv_score(X, y, subset)

    best_overall_score = -np.inf
    best_overall_chrom: list[int] = population[0]

    for gen in range(generations):
        # Evaluer
        fitness = [evaluate(chrom) for chrom in population]

        # Trier par fitness
        ranked = sorted(zip(fitness, population), key=lambda x: -x[0])

        gen_best_score = ranked[0][0]
        gen_best_n = sum(ranked[0][1])

        if gen_best_score > best_overall_score:
            best_overall_score = gen_best_score
            best_overall_chrom = ranked[0][1][:]

        logger.info("  Gen %02d | best=%.6f (%d feat) | pop_mean=%.6f",
                     gen, gen_best_score, gen_best_n,
                     np.mean([s for s, _ in ranked]))

        # Nouvelle generation
        new_pop: list[list[int]] = []

        # Elite
        for _, chrom in ranked[:n_elite]:
            new_pop.append(chrom[:])

        # Croisement + mutation
        while len(new_pop) < population_size:
            # Selection par tournoi (top 50%)
            p1 = ranked[rng.randint(0, len(ranked) // 2)][1]
            p2 = ranked[rng.randint(0, len(ranked) // 2)][1]

            # Croisement uniforme
            child = [p1[i] if rng.random() < 0.5 else p2[i] for i in range(n_features)]

            # Mutation
            for i in range(n_features):
                if rng.random() < mutation_rate:
                    child[i] = 1 - child[i]

            # Au moins 5 features
            if sum(child) < 5:
                indices = rng.sample(range(n_features), 5)
                for idx in indices:
                    child[idx] = 1

            new_pop.append(child)

        population = new_pop

    best_features = decode(best_overall_chrom)
    logger.info("  GA termine : %d features, score=%.6f",
                len(best_features), best_overall_score)

    return best_features, best_overall_score


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def run_optimizer(
    parquet_path: Path = DEFAULT_PARQUET,
    selected_path: Optional[Path] = DEFAULT_SELECTED,
    target: str = DEFAULT_TARGET,
    method: str = "all",
    output_path: Optional[Path] = None,
) -> dict:
    """Execute l'optimisation de sous-ensembles.

    Parameters
    ----------
    parquet_path : Path
        Fichier de donnees.
    selected_path : Path, optional
        Features pre-selectionnees (script 19).
    target : str
        Colonne cible.
    method : str
        Methode : "forward", "backward", "genetic", "all".
    output_path : Path, optional
        Fichier JSON de sortie.

    Returns
    -------
    dict
        Resultats avec sous-ensemble optimal.
    """
    X, y, all_features = load_data(parquet_path, selected_path, target)

    logger.info("=" * 60)
    logger.info("OPTIMISATION DE SOUS-ENSEMBLES DE FEATURES")
    logger.info("  Features disponibles : %d", len(all_features))
    logger.info("  Methode : %s", method)
    logger.info("=" * 60)

    results: dict = {"methods": {}}

    if method in ("forward", "all"):
        fwd_features, fwd_score = forward_selection(X, y)
        results["methods"]["forward"] = {
            "features": fwd_features,
            "n_features": len(fwd_features),
            "cv_score": round(fwd_score, 6),
        }

    if method in ("backward", "all"):
        bwd_features, bwd_score = backward_selection(X, y)
        results["methods"]["backward"] = {
            "features": bwd_features,
            "n_features": len(bwd_features),
            "cv_score": round(bwd_score, 6),
        }

    if method in ("genetic", "all"):
        ga_features, ga_score = genetic_algorithm(X, y)
        results["methods"]["genetic"] = {
            "features": ga_features,
            "n_features": len(ga_features),
            "cv_score": round(ga_score, 6),
        }

    # Determiner le meilleur sous-ensemble
    best_method = max(
        results["methods"],
        key=lambda m: results["methods"][m]["cv_score"],
    )
    best_data = results["methods"][best_method]

    results["optimal"] = {
        "method": best_method,
        "features": best_data["features"],
        "n_features": best_data["n_features"],
        "cv_score": best_data["cv_score"],
    }

    logger.info("=" * 60)
    logger.info("RESULTAT OPTIMAL")
    logger.info("  Methode : %s", best_method)
    logger.info("  Features : %d", best_data["n_features"])
    logger.info("  Score CV : %.6f", best_data["cv_score"])
    logger.info("=" * 60)

    # Sauvegarder
    if output_path is None:
        output_path = OUTPUT_DIR / "optimal_features.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=True)

    logger.info("Sauvegarde : %s", output_path)

    return results


# ===========================================================================
# CLI
# ===========================================================================

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Optimisation de sous-ensembles de features (script 20)",
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_PARQUET,
        help="Fichier Parquet d'entree",
    )
    parser.add_argument(
        "--selected", type=Path, default=DEFAULT_SELECTED,
        help="Fichier JSON des features pre-selectionnees",
    )
    parser.add_argument(
        "--target", type=str, default=DEFAULT_TARGET,
        help="Colonne cible",
    )
    parser.add_argument(
        "--method", type=str, default="all",
        choices=["forward", "backward", "genetic", "all"],
        help="Methode d'optimisation",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Fichier JSON de sortie",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    results = run_optimizer(
        parquet_path=args.input,
        selected_path=args.selected,
        target=args.target,
        method=args.method,
        output_path=args.output,
    )
    opt = results["optimal"]
    print(f"\n[OK] Optimal : {opt['n_features']} features via {opt['method']} "
          f"(score={opt['cv_score']:.6f})")


if __name__ == "__main__":
    main()
