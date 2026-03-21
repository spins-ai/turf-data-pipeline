#!/usr/bin/env python3
"""
models/stacking_ensemble.py
=============================
Ensemble par stacking pour la prediction de courses hippiques.

Architecture :
  - Level-0 : modeles de base (baseline_models.py)
  - Level-1 : meta-learner (LogisticRegression par defaut)
  - Predictions out-of-fold pour entrainer le meta-learner (CV temporelle)
  - predict_proba retourne des probabilites calibrees

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from models.baseline_models import (
    BaseModel,
    create_model,
    compute_metrics,
)
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"

DEFAULT_BASE_MODELS = ["logistic", "random_forest", "xgboost", "lightgbm"]


# ===========================================================================
# CORE
# ===========================================================================

class StackingEnsemble:
    """Ensemble par stacking a 2 niveaux.

    Parameters
    ----------
    base_model_names : list[str]
        Noms des modeles de base (cf. MODEL_REGISTRY).
    meta_learner : sklearn estimator or None
        Meta-learner de niveau 1. Par defaut LogisticRegression.
    base_model_params : dict[str, dict], optional
        Parametres specifiques par modele de base.
        Cle = nom du modele, valeur = dict de parametres.
    """

    def __init__(
        self,
        base_model_names: Optional[list[str]] = None,
        meta_learner=None,
        base_model_params: Optional[dict[str, dict]] = None,
    ):
        self.logger = setup_logging("stacking_ensemble")
        self.base_model_names = base_model_names or DEFAULT_BASE_MODELS
        self.base_model_params = base_model_params or {}

        self.meta_learner = meta_learner or LogisticRegression(
            C=1.0, max_iter=1000, random_state=42,
        )

        self.base_models: list[BaseModel] = []
        self.is_fitted = False
        self._feature_names: list[str] = []

    def _create_base_models(self) -> list[BaseModel]:
        """Instancie les modeles de base."""
        models = []
        for name in self.base_model_names:
            params = self.base_model_params.get(name, {})
            try:
                model = create_model(name, **params)
                models.append(model)
            except (ImportError, ValueError) as e:
                self.logger.warning("Modele %s ignore: %s", name, e)
        return models

    def _generate_oof_predictions(
        self,
        X: np.ndarray,
        y: np.ndarray,
        temporal_splits: list[tuple[list[int], list[int]]],
    ) -> np.ndarray:
        """Genere les predictions out-of-fold pour chaque modele de base.

        Parameters
        ----------
        X : np.ndarray
            Features (shape: n_samples x n_features).
        y : np.ndarray
            Labels.
        temporal_splits : list[tuple[list[int], list[int]]]
            Splits temporels (train_indices, val_indices).

        Returns
        -------
        np.ndarray
            Matrice OOF (shape: n_samples x n_base_models).
        """
        n_models = len(self.base_models)
        oof = np.full((len(X), n_models), np.nan)

        for fold_idx, (train_idx, val_idx) in enumerate(temporal_splits):
            self.logger.info("  Fold %d: train=%d, val=%d", fold_idx, len(train_idx), len(val_idx))

            X_train_fold = X[train_idx]
            y_train_fold = y[train_idx]
            X_val_fold = X[val_idx]

            for model_idx, model_name in enumerate(self.base_model_names):
                params = self.base_model_params.get(model_name, {})
                try:
                    model = create_model(model_name, **params)
                    model.fit(X_train_fold, y_train_fold)
                    preds = model.predict_proba(X_val_fold)
                    oof[val_idx, model_idx] = preds
                except Exception as e:
                    self.logger.warning("  Fold %d, %s erreur: %s", fold_idx, model_name, e)

        return oof

    def fit(
        self,
        X: np.ndarray | pd.DataFrame,
        y: np.ndarray | pd.Series,
        temporal_splits: list[tuple[list[int], list[int]]],
    ) -> None:
        """Entraine le stacking ensemble.

        1. Genere les predictions OOF des modeles de base
        2. Entraine le meta-learner sur les OOF
        3. Re-entraine les modeles de base sur l'ensemble complet

        Parameters
        ----------
        X : array-like
            Features d'entrainement.
        y : array-like
            Labels d'entrainement.
        temporal_splits : list[tuple[list[int], list[int]]]
            Splits temporels pour les predictions OOF.
            Utiliser DatasetSplitManager.walk_forward_splits() pour les generer.
        """
        self.logger.info("=" * 70)
        self.logger.info("Entrainement StackingEnsemble")
        self.logger.info("=" * 70)

        if isinstance(X, pd.DataFrame):
            self._feature_names = list(X.columns)
            X_arr = X.values
        else:
            X_arr = np.asarray(X)
        y_arr = np.asarray(y)

        # 1. Creer les modeles de base
        self.base_models = self._create_base_models()
        self.logger.info("Modeles de base: %s", [m.name for m in self.base_models])

        # 2. Predictions OOF
        self.logger.info("Generation des predictions out-of-fold...")
        oof = self._generate_oof_predictions(X_arr, y_arr, temporal_splits)

        # Filtrer les lignes avec NaN (pas couvertes par les folds)
        valid_mask = ~np.any(np.isnan(oof), axis=1)
        oof_valid = oof[valid_mask]
        y_valid = y_arr[valid_mask]

        self.logger.info("OOF: %d/%d lignes valides", valid_mask.sum(), len(oof))

        # 3. Entrainer le meta-learner
        self.logger.info("Entrainement du meta-learner...")
        self.meta_learner.fit(oof_valid, y_valid)

        # 4. Re-entrainer les modeles de base sur l'ensemble complet
        self.logger.info("Re-entrainement des modeles de base sur l'ensemble complet...")
        for model in self.base_models:
            model.fit(X_arr, y_arr)

        self.is_fitted = True
        self.logger.info("StackingEnsemble entraine.")

    def predict_proba(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        """Retourne les probabilites calibrees du stacking.

        Parameters
        ----------
        X : array-like
            Features.

        Returns
        -------
        np.ndarray
            Probabilites de la classe positive.
        """
        if not self.is_fitted:
            raise RuntimeError("Le modele n'est pas entraine. Appeler fit() d'abord.")

        if isinstance(X, pd.DataFrame):
            X_arr = X.values
        else:
            X_arr = np.asarray(X)

        # Predictions de chaque modele de base
        base_preds = np.column_stack([
            model.predict_proba(X_arr) for model in self.base_models
        ])

        # Meta-learner
        meta_proba = self.meta_learner.predict_proba(base_preds)
        return meta_proba[:, 1] if meta_proba.ndim > 1 else meta_proba

    def predict(self, X: np.ndarray | pd.DataFrame) -> np.ndarray:
        """Prediction binaire (seuil 0.5)."""
        return (self.predict_proba(X) >= 0.5).astype(int)

    def evaluate(
        self,
        X: np.ndarray | pd.DataFrame,
        y: np.ndarray | pd.Series,
        race_ids: Optional[np.ndarray] = None,
    ) -> dict:
        """Evalue l'ensemble et chaque modele de base.

        Parameters
        ----------
        X : array-like
            Features de test.
        y : array-like
            Labels de test.
        race_ids : np.ndarray, optional
            Identifiants de course pour top-N par course.

        Returns
        -------
        dict
            Metriques de l'ensemble et de chaque modele de base.
        """
        y_arr = np.asarray(y)

        # Metriques de l'ensemble
        ensemble_proba = self.predict_proba(X)
        ensemble_metrics = compute_metrics(y_arr, ensemble_proba, race_ids)
        ensemble_metrics["model"] = "StackingEnsemble"

        # Metriques par modele de base
        base_metrics = []
        for model in self.base_models:
            m = model.evaluate(X, y, race_ids)
            base_metrics.append(m)

        self.logger.info(
            "  StackingEnsemble — acc=%.4f, logloss=%.4f, auc=%s",
            ensemble_metrics["accuracy"],
            ensemble_metrics["log_loss"],
            f"{ensemble_metrics['roc_auc']:.4f}" if ensemble_metrics.get("roc_auc") else "N/A",
        )

        return {
            "ensemble": ensemble_metrics,
            "base_models": base_metrics,
        }

    def get_base_predictions(self, X: np.ndarray | pd.DataFrame) -> dict[str, np.ndarray]:
        """Retourne les predictions individuelles de chaque modele de base.

        Parameters
        ----------
        X : array-like
            Features.

        Returns
        -------
        dict[str, np.ndarray]
            Cle = nom du modele, valeur = probabilites.
        """
        if not self.is_fitted:
            raise RuntimeError("Le modele n'est pas entraine.")

        return {
            model.name: model.predict_proba(X)
            for model in self.base_models
        }

    def feature_importances(self) -> dict[str, dict[str, float]]:
        """Importance des features pour chaque modele de base.

        Returns
        -------
        dict[str, dict[str, float]]
            Cle = nom du modele, valeur = dict feature->importance.
        """
        result = {}
        for model in self.base_models:
            imp = model.feature_importances()
            if imp is not None:
                result[model.name] = imp
        return result
