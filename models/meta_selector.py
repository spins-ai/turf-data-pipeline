#!/usr/bin/env python3
"""
models/meta_selector.py
========================
Selecteur dynamique de modele pour les courses hippiques.

Route vers le meilleur modele en fonction du contexte de la course
(discipline, terrain, taille du champ, etc.).
Suit les performances par contexte et constitue un ensemble pondere.

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from models.baseline_models import BaseModel, compute_metrics, setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output" / "models"

DEFAULT_MIN_CONTEXT_SAMPLES = 50  # Minimum de courses pour faire confiance au contexte


# ===========================================================================
# CORE
# ===========================================================================

class MetaSelector:
    """Selecteur dynamique de modele par contexte de course.

    Route les predictions vers le modele le plus performant pour un
    contexte donne (discipline, terrain, taille du champ, etc.).

    Parameters
    ----------
    models : dict[str, BaseModel]
        Modeles candidats (cle = nom, valeur = modele entraine).
    context_keys : list[str]
        Colonnes definissant le contexte (ex: ["discipline", "terrain"]).
    min_context_samples : int
        Nombre minimum de courses par contexte pour utiliser la
        performance contextuelle. En dessous, utilise la performance globale.
    """

    def __init__(
        self,
        models: dict[str, BaseModel],
        context_keys: Optional[list[str]] = None,
        min_context_samples: int = DEFAULT_MIN_CONTEXT_SAMPLES,
    ):
        self.models = models
        self.context_keys = context_keys or ["discipline", "terrain"]
        self.min_context_samples = min_context_samples
        self.logger = setup_logging()

        # Performance tracking
        # context_key -> model_name -> list[float] (log_loss par course)
        self._context_performance: dict[str, dict[str, list[float]]] = defaultdict(
            lambda: defaultdict(list)
        )
        # Performance globale
        self._global_performance: dict[str, list[float]] = defaultdict(list)

    def _build_context_key(self, row: dict | pd.Series) -> str:
        """Construit une cle de contexte a partir des colonnes definies.

        Parameters
        ----------
        row : dict or pd.Series
            Ligne avec les colonnes de contexte.

        Returns
        -------
        str
            Cle de contexte (ex: "plat|bon").
        """
        parts = []
        for key in self.context_keys:
            val = row.get(key, "unknown") if isinstance(row, dict) else row.get(key, "unknown")
            parts.append(str(val) if val is not None else "unknown")
        return "|".join(parts)

    def update_performance(
        self,
        model_name: str,
        y_true: np.ndarray,
        y_proba: np.ndarray,
        contexts: list[str],
    ) -> None:
        """Met a jour le suivi de performance pour un modele.

        Parameters
        ----------
        model_name : str
            Nom du modele.
        y_true : np.ndarray
            Labels reels.
        y_proba : np.ndarray
            Probabilites predites.
        contexts : list[str]
            Cle de contexte pour chaque echantillon.
        """
        y_true = np.asarray(y_true)
        y_proba = np.asarray(y_proba).clip(1e-10, 1 - 1e-10)

        # Log-loss par echantillon
        ll = -(y_true * np.log(y_proba) + (1 - y_true) * np.log(1 - y_proba))

        for i, ctx in enumerate(contexts):
            self._context_performance[ctx][model_name].append(float(ll[i]))
            self._global_performance[model_name].append(float(ll[i]))

    def track_all_models(
        self,
        X: pd.DataFrame,
        y: np.ndarray | pd.Series,
        context_df: pd.DataFrame,
    ) -> dict[str, dict]:
        """Evalue et suit tous les modeles sur un ensemble de donnees.

        Parameters
        ----------
        X : pd.DataFrame
            Features.
        y : array-like
            Labels.
        context_df : pd.DataFrame
            DataFrame avec les colonnes de contexte.

        Returns
        -------
        dict[str, dict]
            Metriques par modele.
        """
        y_arr = np.asarray(y)
        contexts = [self._build_context_key(row) for _, row in context_df.iterrows()]

        all_metrics = {}
        for name, model in self.models.items():
            try:
                proba = model.predict_proba(X)
                self.update_performance(name, y_arr, proba, contexts)
                metrics = compute_metrics(y_arr, proba)
                metrics["model"] = name
                all_metrics[name] = metrics
                self.logger.info("  %s — logloss=%.4f, auc=%s",
                                 name, metrics["log_loss"],
                                 f"{metrics['roc_auc']:.4f}" if metrics.get("roc_auc") else "N/A")
            except Exception as e:
                self.logger.warning("  %s erreur: %s", name, e)

        return all_metrics

    def get_best_model(self, context_key: str) -> str:
        """Retourne le meilleur modele pour un contexte donne.

        Parameters
        ----------
        context_key : str
            Cle de contexte.

        Returns
        -------
        str
            Nom du meilleur modele.
        """
        ctx_perf = self._context_performance.get(context_key, {})

        # Verifier si assez de donnees pour ce contexte
        has_enough = any(
            len(scores) >= self.min_context_samples
            for scores in ctx_perf.values()
        )

        if has_enough:
            # Meilleur modele par contexte (log-loss moyen le plus bas)
            best = min(
                ((name, np.mean(scores))
                 for name, scores in ctx_perf.items()
                 if len(scores) >= self.min_context_samples),
                key=lambda x: x[1],
            )
            return best[0]

        # Fallback: meilleur modele global
        if self._global_performance:
            best = min(
                ((name, np.mean(scores))
                 for name, scores in self._global_performance.items()
                 if len(scores) > 0),
                key=lambda x: x[1],
            )
            return best[0]

        # Default: premier modele
        return next(iter(self.models))

    def get_context_weights(self, context_key: str) -> dict[str, float]:
        """Calcule les poids de chaque modele pour un contexte (softmax inverse sur log-loss).

        Parameters
        ----------
        context_key : str
            Cle de contexte.

        Returns
        -------
        dict[str, float]
            Poids normalises par modele.
        """
        ctx_perf = self._context_performance.get(context_key, {})
        has_enough = any(
            len(scores) >= self.min_context_samples
            for scores in ctx_perf.values()
        )

        if has_enough:
            perf = {
                name: np.mean(scores)
                for name, scores in ctx_perf.items()
                if len(scores) >= self.min_context_samples
            }
        elif self._global_performance:
            perf = {
                name: np.mean(scores)
                for name, scores in self._global_performance.items()
                if len(scores) > 0
            }
        else:
            # Poids egaux
            n = len(self.models)
            return {name: 1.0 / n for name in self.models}

        if not perf:
            n = len(self.models)
            return {name: 1.0 / n for name in self.models}

        # Softmax inverse : meilleur log-loss = poids le plus eleve
        losses = np.array(list(perf.values()))
        # Inverser : plus la loss est basse, plus le score est eleve
        scores = -losses
        # Softmax
        exp_scores = np.exp(scores - scores.max())
        weights = exp_scores / exp_scores.sum()

        return dict(zip(perf.keys(), weights.tolist()))

    def predict_proba(
        self,
        X: pd.DataFrame,
        context_df: pd.DataFrame,
        method: str = "best",
    ) -> np.ndarray:
        """Prediction avec selection dynamique du modele.

        Parameters
        ----------
        X : pd.DataFrame
            Features.
        context_df : pd.DataFrame
            DataFrame avec les colonnes de contexte.
        method : str
            "best" = utilise le meilleur modele par contexte
            "weighted" = ensemble pondere par contexte

        Returns
        -------
        np.ndarray
            Probabilites predites.
        """
        n = len(X)
        predictions = np.zeros(n)

        if method == "best":
            # Regrouper par contexte et router
            groups: dict[str, list[int]] = defaultdict(list)
            for i, (_, row) in enumerate(context_df.iterrows()):
                ctx = self._build_context_key(row)
                groups[ctx].append(i)

            for ctx, indices in groups.items():
                best_name = self.get_best_model(ctx)
                model = self.models[best_name]
                X_ctx = X.iloc[indices]
                proba = model.predict_proba(X_ctx)
                predictions[indices] = proba

                self.logger.info("  Contexte %s (%d samples) -> %s",
                                 ctx, len(indices), best_name)

        elif method == "weighted":
            # Ensemble pondere par contexte
            groups: dict[str, list[int]] = defaultdict(list)
            for i, (_, row) in enumerate(context_df.iterrows()):
                ctx = self._build_context_key(row)
                groups[ctx].append(i)

            for ctx, indices in groups.items():
                weights = self.get_context_weights(ctx)
                X_ctx = X.iloc[indices]

                weighted_proba = np.zeros(len(indices))
                for model_name, weight in weights.items():
                    if model_name in self.models:
                        proba = self.models[model_name].predict_proba(X_ctx)
                        weighted_proba += weight * proba

                predictions[indices] = weighted_proba

        else:
            raise ValueError(f"Methode inconnue: {method}. Utiliser 'best' ou 'weighted'.")

        return predictions

    def get_performance_report(self) -> dict:
        """Rapport de performance par contexte et par modele.

        Returns
        -------
        dict
            Rapport JSON-serialisable.
        """
        report = {
            "global": {},
            "by_context": {},
        }

        # Performance globale
        for name, scores in self._global_performance.items():
            if scores:
                report["global"][name] = {
                    "mean_logloss": round(float(np.mean(scores)), 6),
                    "n_samples": len(scores),
                }

        # Performance par contexte
        for ctx, models_perf in self._context_performance.items():
            report["by_context"][ctx] = {}
            for name, scores in models_perf.items():
                if scores:
                    report["by_context"][ctx][name] = {
                        "mean_logloss": round(float(np.mean(scores)), 6),
                        "n_samples": len(scores),
                    }

            # Meilleur modele pour ce contexte
            best = self.get_best_model(ctx)
            report["by_context"][ctx]["_best_model"] = best

        return report

    def save_performance(self, path: Optional[Path] = None) -> None:
        """Sauvegarde le rapport de performance.

        Parameters
        ----------
        path : Path, optional
            Chemin de sortie. Par defaut output/models/meta_selector_performance.json
        """
        if path is None:
            path = OUTPUT_DIR / "meta_selector_performance.json"
        path.parent.mkdir(parents=True, exist_ok=True)

        report = self.get_performance_report()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        self.logger.info("Performance sauvee: %s", path)

    def load_performance(self, path: Optional[Path] = None) -> None:
        """Charge un rapport de performance precedent.

        Parameters
        ----------
        path : Path, optional
            Chemin du fichier. Par defaut output/models/meta_selector_performance.json
        """
        if path is None:
            path = OUTPUT_DIR / "meta_selector_performance.json"

        if not path.exists():
            self.logger.warning("Fichier de performance introuvable: %s", path)
            return

        with open(path, "r", encoding="utf-8") as f:
            report = json.load(f)

        # Reconstruire les structures internes (approximation)
        # Note: on perd le detail par echantillon, on recree des listes
        # synthetiques de la bonne longueur et moyenne.
        for name, stats in report.get("global", {}).items():
            n = stats.get("n_samples", 0)
            mean_ll = stats.get("mean_logloss", 0.5)
            self._global_performance[name] = [mean_ll] * n

        for ctx, models_perf in report.get("by_context", {}).items():
            for name, stats in models_perf.items():
                if name.startswith("_"):
                    continue
                n = stats.get("n_samples", 0)
                mean_ll = stats.get("mean_logloss", 0.5)
                self._context_performance[ctx][name] = [mean_ll] * n

        self.logger.info("Performance chargee depuis: %s", path)
