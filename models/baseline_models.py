#!/usr/bin/env python3
"""
models/baseline_models.py
==========================
Modeles de base pour la prediction de courses hippiques.

Fournit des wrappers avec interface commune pour :
  - LogisticRegression
  - RandomForest
  - XGBoost
  - LightGBM
  - CatBoost

Interface commune : fit(X, y), predict_proba(X), evaluate(X, y)
Metriques : accuracy, log_loss, ROC-AUC, top-N accuracy, calibration error
Extraction d'importance des features

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import logging
import sys
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    log_loss,
    roc_auc_score,
)

# Imports optionnels
try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

try:
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    HAS_LGB = False

try:
    import catboost as cb
    HAS_CB = True
except ImportError:
    HAS_CB = False

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.logging_setup import setup_logging


# ===========================================================================
# METRIQUES
# ===========================================================================

def top_n_accuracy(y_true: np.ndarray, y_proba: np.ndarray, n: int = 3) -> float:
    """Calcule la top-N accuracy : proportion de courses ou le gagnant est
    dans les N meilleures predictions.

    Note : cette metrique suppose que y_proba contient les probabilites
    de victoire pour chaque coureur d'une meme course. Pour un usage
    correct, regrouper par course avant d'appeler.

    Pour une utilisation simplifiee (sans regroupement course), on
    considere chaque ligne independamment : la prediction est correcte
    si y_true=1 et le runner est dans les top-N probas les plus elevees.

    Parameters
    ----------
    y_true : np.ndarray
        Labels binaires (1=gagnant).
    y_proba : np.ndarray
        Probabilites predites.
    n : int
        Nombre de positions top.

    Returns
    -------
    float
        Top-N accuracy.
    """
    if len(y_true) == 0:
        return 0.0
    # Indices des N plus grandes probas
    top_indices = np.argsort(y_proba)[-n:]
    # Le gagnant est-il dans les top N ?
    winners = np.where(y_true == 1)[0]
    if len(winners) == 0:
        return 0.0
    hit = any(w in top_indices for w in winners)
    return 1.0 if hit else 0.0


def top_n_accuracy_by_race(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    race_ids: np.ndarray,
    n: int = 3,
) -> float:
    """Top-N accuracy calculee par course.

    Parameters
    ----------
    y_true : np.ndarray
        Labels binaires (1=gagnant).
    y_proba : np.ndarray
        Probabilites predites.
    race_ids : np.ndarray
        Identifiants de course pour regrouper.
    n : int
        Nombre de positions top.

    Returns
    -------
    float
        Top-N accuracy moyenne sur toutes les courses.
    """
    unique_races = np.unique(race_ids)
    hits = 0
    total = 0

    for race_id in unique_races:
        mask = race_ids == race_id
        race_true = y_true[mask]
        race_proba = y_proba[mask]

        if race_true.sum() == 0:
            continue

        total += 1
        top_idx = np.argsort(race_proba)[-n:]
        winner_idx = np.where(race_true == 1)[0]

        if any(w in top_idx for w in winner_idx):
            hits += 1

    return hits / total if total > 0 else 0.0


def expected_calibration_error(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    n_bins: int = 10,
) -> float:
    """Expected Calibration Error (ECE).

    Parameters
    ----------
    y_true : np.ndarray
        Labels binaires.
    y_proba : np.ndarray
        Probabilites predites.
    n_bins : int
        Nombre de bins.

    Returns
    -------
    float
        ECE.
    """
    bin_edges = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    n = len(y_true)

    for i in range(n_bins):
        mask = (y_proba >= bin_edges[i]) & (y_proba < bin_edges[i + 1])
        if i == n_bins - 1:
            mask = (y_proba >= bin_edges[i]) & (y_proba <= bin_edges[i + 1])
        n_bin = mask.sum()
        if n_bin == 0:
            continue
        avg_confidence = y_proba[mask].mean()
        avg_accuracy = y_true[mask].mean()
        ece += (n_bin / n) * abs(avg_accuracy - avg_confidence)

    return float(ece)


def compute_metrics(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    race_ids: Optional[np.ndarray] = None,
) -> dict[str, float]:
    """Calcule toutes les metriques d'evaluation.

    Parameters
    ----------
    y_true : np.ndarray
        Labels binaires.
    y_proba : np.ndarray
        Probabilites predites (classe positive).
    race_ids : np.ndarray, optional
        Identifiants de course pour top-N par course.

    Returns
    -------
    dict[str, float]
        Dictionnaire de metriques.
    """
    y_pred = (y_proba >= 0.5).astype(int)

    metrics = {
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 6),
        "log_loss": round(float(log_loss(y_true, y_proba, labels=[0, 1])), 6),
        "ece": round(expected_calibration_error(y_true, y_proba), 6),
    }

    # ROC-AUC (peut echouer si une seule classe)
    try:
        metrics["roc_auc"] = round(float(roc_auc_score(y_true, y_proba)), 6)
    except ValueError:
        metrics["roc_auc"] = None

    # Top-N accuracy par course
    if race_ids is not None:
        for n in [1, 3, 5]:
            metrics[f"top_{n}_accuracy"] = round(
                float(top_n_accuracy_by_race(y_true, y_proba, race_ids, n=n)), 6
            )

    return metrics


# ===========================================================================
# BASE CLASS
# ===========================================================================

class BaseModel(ABC):
    """Classe abstraite pour les modeles de baseline.

    Interface commune pour tous les modeles de prediction hippique.
    """

    def __init__(self, name: str, **params):
        self.name = name
        self.params = params
        self.model = None
        self.is_fitted = False
        self.logger = setup_logging("baseline_models")

    @abstractmethod
    def fit(self, X: np.ndarray | pd.DataFrame, y: np.ndarray | pd.Series) -> None:
        """Entraine le modele."""
        pass

    @abstractmethod
    def predict_proba(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        """Retourne les probabilites de la classe positive."""
        pass

    def predict(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        """Prediction binaire (seuil 0.5)."""
        return (self.predict_proba(X) >= 0.5).astype(int)

    def evaluate(
        self,
        X: np.ndarray | pd.DataFrame,
        y: np.ndarray | pd.Series,
        race_ids: Optional[np.ndarray] = None,
    ) -> dict[str, float]:
        """Evalue le modele sur un ensemble de test.

        Parameters
        ----------
        X : array-like
            Features.
        y : array-like
            Labels binaires.
        race_ids : np.ndarray, optional
            Identifiants de course pour top-N par course.

        Returns
        -------
        dict[str, float]
            Dictionnaire de metriques.
        """
        y_proba = self.predict_proba(X)
        y_arr = np.asarray(y)
        metrics = compute_metrics(y_arr, y_proba, race_ids)
        metrics["model"] = self.name

        self.logger.info(
            "  %s — acc=%.4f, logloss=%.4f, auc=%s, ece=%.4f",
            self.name,
            metrics["accuracy"],
            metrics["log_loss"],
            f"{metrics['roc_auc']:.4f}" if metrics.get("roc_auc") is not None else "N/A",
            metrics["ece"],
        )

        return metrics

    @abstractmethod
    def feature_importances(self) -> Optional[dict[str, float]]:
        """Retourne l'importance des features."""
        pass


# ===========================================================================
# MODELES
# ===========================================================================

class LogisticRegressionModel(BaseModel):
    """Wrapper LogisticRegression pour courses hippiques.

    Parametres par defaut adaptes au desequilibre de classes (~7-15% win rate).
    """

    def __init__(self, **params):
        defaults = {
            "C": 0.1,
            "class_weight": "balanced",
            "max_iter": 1000,
            "solver": "lbfgs",
            "random_state": 42,
        }
        defaults.update(params)
        super().__init__(name="LogisticRegression", **defaults)
        self.model = LogisticRegression(**defaults)
        self._feature_names: list[str] = []

    def fit(self, X: np.ndarray | pd.DataFrame, y: np.ndarray | pd.Series) -> None:
        if isinstance(X, pd.DataFrame):
            self._feature_names = list(X.columns)
        self.model.fit(X, y)
        self.is_fitted = True
        self.logger.info("LogisticRegression entraine (%d samples)", len(y))

    def predict_proba(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        proba = self.model.predict_proba(X)
        return proba[:, 1] if proba.ndim > 1 else proba

    def feature_importances(self) -> Optional[dict[str, float]]:
        if not self.is_fitted:
            return None
        coefs = np.abs(self.model.coef_[0])
        names = self._feature_names or [f"f{i}" for i in range(len(coefs))]
        imp = dict(zip(names, coefs.tolist()))
        return dict(sorted(imp.items(), key=lambda x: -x[1]))


class RandomForestModel(BaseModel):
    """Wrapper RandomForest pour courses hippiques."""

    def __init__(self, **params):
        defaults = {
            "n_estimators": 500,
            "max_depth": 12,
            "min_samples_leaf": 20,
            "class_weight": "balanced",
            "n_jobs": -1,
            "random_state": 42,
        }
        defaults.update(params)
        super().__init__(name="RandomForest", **defaults)
        self.model = RandomForestClassifier(**defaults)
        self._feature_names: list[str] = []

    def fit(self, X: np.ndarray | pd.DataFrame, y: np.ndarray | pd.Series) -> None:
        if isinstance(X, pd.DataFrame):
            self._feature_names = list(X.columns)
        self.model.fit(X, y)
        self.is_fitted = True
        self.logger.info("RandomForest entraine (%d samples, %d trees)",
                         len(y), self.model.n_estimators)

    def predict_proba(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        proba = self.model.predict_proba(X)
        return proba[:, 1] if proba.ndim > 1 else proba

    def feature_importances(self) -> Optional[dict[str, float]]:
        if not self.is_fitted:
            return None
        imp_vals = self.model.feature_importances_
        names = self._feature_names or [f"f{i}" for i in range(len(imp_vals))]
        imp = dict(zip(names, imp_vals.tolist()))
        return dict(sorted(imp.items(), key=lambda x: -x[1]))


class XGBoostModel(BaseModel):
    """Wrapper XGBoost pour courses hippiques."""

    def __init__(self, **params):
        if not HAS_XGB:
            raise ImportError("xgboost requis : pip install xgboost")
        defaults = {
            "n_estimators": 500,
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "min_child_weight": 10,
            "scale_pos_weight": 8,  # ~1/(win_rate) pour desequilibre
            "eval_metric": "logloss",
            "random_state": 42,
            "n_jobs": -1,
            "verbosity": 0,
        }
        defaults.update(params)
        super().__init__(name="XGBoost", **defaults)
        self.model = xgb.XGBClassifier(**defaults)
        self._feature_names: list[str] = []

    def fit(self, X: np.ndarray | pd.DataFrame, y: np.ndarray | pd.Series) -> None:
        if isinstance(X, pd.DataFrame):
            self._feature_names = list(X.columns)
        self.model.fit(X, y)
        self.is_fitted = True
        self.logger.info("XGBoost entraine (%d samples)", len(y))

    def predict_proba(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        proba = self.model.predict_proba(X)
        return proba[:, 1] if proba.ndim > 1 else proba

    def feature_importances(self) -> Optional[dict[str, float]]:
        if not self.is_fitted:
            return None
        imp_vals = self.model.feature_importances_
        names = self._feature_names or [f"f{i}" for i in range(len(imp_vals))]
        imp = dict(zip(names, imp_vals.tolist()))
        return dict(sorted(imp.items(), key=lambda x: -x[1]))


class LightGBMModel(BaseModel):
    """Wrapper LightGBM pour courses hippiques."""

    def __init__(self, **params):
        if not HAS_LGB:
            raise ImportError("lightgbm requis : pip install lightgbm")
        defaults = {
            "n_estimators": 500,
            "max_depth": 8,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "min_child_samples": 20,
            "is_unbalance": True,
            "metric": "binary_logloss",
            "random_state": 42,
            "n_jobs": -1,
            "verbose": -1,
        }
        defaults.update(params)
        super().__init__(name="LightGBM", **defaults)
        self.model = lgb.LGBMClassifier(**defaults)
        self._feature_names: list[str] = []

    def fit(self, X: np.ndarray | pd.DataFrame, y: np.ndarray | pd.Series) -> None:
        if isinstance(X, pd.DataFrame):
            self._feature_names = list(X.columns)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.model.fit(X, y)
        self.is_fitted = True
        self.logger.info("LightGBM entraine (%d samples)", len(y))

    def predict_proba(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        proba = self.model.predict_proba(X)
        return proba[:, 1] if proba.ndim > 1 else proba

    def feature_importances(self) -> Optional[dict[str, float]]:
        if not self.is_fitted:
            return None
        imp_vals = self.model.feature_importances_
        names = self._feature_names or [f"f{i}" for i in range(len(imp_vals))]
        imp = dict(zip(names, imp_vals.tolist()))
        return dict(sorted(imp.items(), key=lambda x: -x[1]))


class CatBoostModel(BaseModel):
    """Wrapper CatBoost pour courses hippiques."""

    def __init__(self, **params):
        if not HAS_CB:
            raise ImportError("catboost requis : pip install catboost")
        defaults = {
            "iterations": 500,
            "depth": 6,
            "learning_rate": 0.05,
            "auto_class_weights": "Balanced",
            "loss_function": "Logloss",
            "eval_metric": "Logloss",
            "random_seed": 42,
            "verbose": 0,
        }
        defaults.update(params)
        super().__init__(name="CatBoost", **defaults)
        self.model = cb.CatBoostClassifier(**defaults)
        self._feature_names: list[str] = []

    def fit(self, X: np.ndarray | pd.DataFrame, y: np.ndarray | pd.Series) -> None:
        if isinstance(X, pd.DataFrame):
            self._feature_names = list(X.columns)
        self.model.fit(X, y, verbose=0)
        self.is_fitted = True
        self.logger.info("CatBoost entraine (%d samples)", len(y))

    def predict_proba(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        proba = self.model.predict_proba(X)
        return proba[:, 1] if proba.ndim > 1 else proba

    def feature_importances(self) -> Optional[dict[str, float]]:
        if not self.is_fitted:
            return None
        imp_vals = self.model.feature_importances_
        names = self._feature_names or [f"f{i}" for i in range(len(imp_vals))]
        imp = dict(zip(names, imp_vals.tolist()))
        return dict(sorted(imp.items(), key=lambda x: -x[1]))


# ===========================================================================
# FACTORY
# ===========================================================================

MODEL_REGISTRY: dict[str, type[BaseModel]] = {
    "logistic": LogisticRegressionModel,
    "random_forest": RandomForestModel,
}

if HAS_XGB:
    MODEL_REGISTRY["xgboost"] = XGBoostModel
if HAS_LGB:
    MODEL_REGISTRY["lightgbm"] = LightGBMModel
if HAS_CB:
    MODEL_REGISTRY["catboost"] = CatBoostModel


def create_model(name: str, **params) -> BaseModel:
    """Cree un modele par nom.

    Parameters
    ----------
    name : str
        Nom du modele (logistic, random_forest, xgboost, lightgbm, catboost).
    **params
        Parametres specifiques du modele.

    Returns
    -------
    BaseModel
        Instance du modele.
    """
    if name not in MODEL_REGISTRY:
        available = ", ".join(MODEL_REGISTRY.keys())
        raise ValueError(f"Modele inconnu: {name}. Disponibles: {available}")
    return MODEL_REGISTRY[name](**params)


def create_all_models(**common_params) -> list[BaseModel]:
    """Cree une instance de chaque modele disponible.

    Parameters
    ----------
    **common_params
        Parametres communs (ex: random_state).

    Returns
    -------
    list[BaseModel]
        Liste de modeles.
    """
    models = []
    for name in MODEL_REGISTRY:
        try:
            models.append(create_model(name, **common_params))
        except ImportError:
            pass
    return models
