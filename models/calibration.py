#!/usr/bin/env python3
"""
models/calibration.py
======================
Calibration des probabilites pour les modeles de prediction hippique.

Methodes :
  - Platt scaling (regression logistique)
  - Regression isotonique
  - Temperature scaling

Metriques de calibration :
  - ECE (Expected Calibration Error)
  - MCE (Maximum Calibration Error)
  - Donnees pour diagramme de fiabilite (reliability diagram)

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path("logs")
DEFAULT_N_BINS = 10


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("calibration")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(LOG_DIR / "calibration.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


# ===========================================================================
# METRIQUES DE CALIBRATION
# ===========================================================================

def reliability_diagram_data(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    n_bins: int = DEFAULT_N_BINS,
) -> list[dict]:
    """Calcule les donnees pour un diagramme de fiabilite.

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
    list[dict]
        Liste de dicts avec bin_center, avg_predicted, avg_observed, count.
    """
    bin_edges = np.linspace(0, 1, n_bins + 1)
    bins_data = []

    for i in range(n_bins):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        if i == n_bins - 1:
            mask = (y_proba >= lo) & (y_proba <= hi)
        else:
            mask = (y_proba >= lo) & (y_proba < hi)

        n_bin = mask.sum()
        if n_bin == 0:
            continue

        bins_data.append({
            "bin_center": round((lo + hi) / 2, 4),
            "bin_lo": round(lo, 4),
            "bin_hi": round(hi, 4),
            "avg_predicted": round(float(y_proba[mask].mean()), 6),
            "avg_observed": round(float(y_true[mask].mean()), 6),
            "count": int(n_bin),
        })

    return bins_data


def expected_calibration_error(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    n_bins: int = DEFAULT_N_BINS,
) -> float:
    """Expected Calibration Error (ECE).

    Moyenne ponderee de |accuracy - confidence| sur les bins.

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
    bins_data = reliability_diagram_data(y_true, y_proba, n_bins)
    n = len(y_true)
    ece = 0.0
    for b in bins_data:
        weight = b["count"] / n
        ece += weight * abs(b["avg_observed"] - b["avg_predicted"])
    return float(ece)


def maximum_calibration_error(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    n_bins: int = DEFAULT_N_BINS,
) -> float:
    """Maximum Calibration Error (MCE).

    Maximum de |accuracy - confidence| sur les bins.

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
        MCE.
    """
    bins_data = reliability_diagram_data(y_true, y_proba, n_bins)
    if not bins_data:
        return 0.0
    return float(max(abs(b["avg_observed"] - b["avg_predicted"]) for b in bins_data))


def calibration_report(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    n_bins: int = DEFAULT_N_BINS,
) -> dict:
    """Rapport complet de calibration.

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
    dict
        Rapport avec ECE, MCE, et donnees du diagramme de fiabilite.
    """
    y_true = np.asarray(y_true)
    y_proba = np.asarray(y_proba)

    return {
        "ece": round(expected_calibration_error(y_true, y_proba, n_bins), 6),
        "mce": round(maximum_calibration_error(y_true, y_proba, n_bins), 6),
        "n_bins": n_bins,
        "reliability_diagram": reliability_diagram_data(y_true, y_proba, n_bins),
        "mean_predicted": round(float(y_proba.mean()), 6),
        "mean_observed": round(float(y_true.mean()), 6),
    }


# ===========================================================================
# CALIBRATEURS
# ===========================================================================

class PlattCalibrator:
    """Calibration par Platt scaling (regression logistique sur les logits).

    Transforme les probabilites brutes via une regression logistique
    apprise sur un ensemble de calibration.
    """

    def __init__(self):
        self.lr = LogisticRegression(C=1e10, max_iter=5000, solver="lbfgs")
        self.is_fitted = False
        self.logger = setup_logging()

    def fit(self, y_true: np.ndarray, y_proba: np.ndarray) -> None:
        """Entraine la calibration Platt.

        Parameters
        ----------
        y_true : np.ndarray
            Labels binaires de l'ensemble de calibration.
        y_proba : np.ndarray
            Probabilites brutes du modele.
        """
        y_true = np.asarray(y_true)
        y_proba = np.asarray(y_proba).clip(1e-10, 1 - 1e-10)

        # Logits
        logits = np.log(y_proba / (1 - y_proba)).reshape(-1, 1)
        self.lr.fit(logits, y_true)
        self.is_fitted = True
        self.logger.info("PlattCalibrator entraine (%d samples)", len(y_true))

    def calibrate(self, y_proba: np.ndarray) -> np.ndarray:
        """Applique la calibration Platt.

        Parameters
        ----------
        y_proba : np.ndarray
            Probabilites brutes.

        Returns
        -------
        np.ndarray
            Probabilites calibrees.
        """
        if not self.is_fitted:
            raise RuntimeError("PlattCalibrator non entraine. Appeler fit() d'abord.")

        y_proba = np.asarray(y_proba).clip(1e-10, 1 - 1e-10)
        logits = np.log(y_proba / (1 - y_proba)).reshape(-1, 1)
        proba = self.lr.predict_proba(logits)
        return proba[:, 1] if proba.ndim > 1 else proba


class IsotonicCalibrator:
    """Calibration par regression isotonique.

    Methode non-parametrique, plus flexible que Platt scaling.
    Necessite davantage de donnees de calibration.
    """

    def __init__(self):
        self.ir = IsotonicRegression(y_min=0, y_max=1, out_of_bounds="clip")
        self.is_fitted = False
        self.logger = setup_logging()

    def fit(self, y_true: np.ndarray, y_proba: np.ndarray) -> None:
        """Entraine la calibration isotonique.

        Parameters
        ----------
        y_true : np.ndarray
            Labels binaires.
        y_proba : np.ndarray
            Probabilites brutes.
        """
        self.ir.fit(np.asarray(y_proba), np.asarray(y_true))
        self.is_fitted = True
        self.logger.info("IsotonicCalibrator entraine (%d samples)", len(y_true))

    def calibrate(self, y_proba: np.ndarray) -> np.ndarray:
        """Applique la calibration isotonique.

        Parameters
        ----------
        y_proba : np.ndarray
            Probabilites brutes.

        Returns
        -------
        np.ndarray
            Probabilites calibrees.
        """
        if not self.is_fitted:
            raise RuntimeError("IsotonicCalibrator non entraine.")
        return self.ir.predict(np.asarray(y_proba))


class TemperatureScaler:
    """Calibration par temperature scaling.

    Divise les logits par un parametre de temperature T > 0.
    T > 1 : adoucit les probabilites (moins confiant)
    T < 1 : accentue les probabilites (plus confiant)
    T est optimise par minimisation du NLL.
    """

    def __init__(self):
        self.temperature: float = 1.0
        self.is_fitted = False
        self.logger = setup_logging()

    def fit(
        self,
        y_true: np.ndarray,
        y_proba: np.ndarray,
        lr: float = 0.01,
        max_iter: int = 1000,
    ) -> None:
        """Optimise la temperature par descente de gradient.

        Parameters
        ----------
        y_true : np.ndarray
            Labels binaires.
        y_proba : np.ndarray
            Probabilites brutes.
        lr : float
            Taux d'apprentissage.
        max_iter : int
            Nombre maximum d'iterations.
        """
        y_true = np.asarray(y_true, dtype=float)
        y_proba = np.asarray(y_proba).clip(1e-10, 1 - 1e-10)
        logits = np.log(y_proba / (1 - y_proba))

        T = 1.0

        for _ in range(max_iter):
            scaled_logits = logits / T
            # Sigmoid
            probs = 1.0 / (1.0 + np.exp(-scaled_logits))
            probs = np.clip(probs, 1e-10, 1 - 1e-10)

            # NLL gradient w.r.t. T
            # d(NLL)/dT = -1/T^2 * sum( logits * (y - probs) )
            grad = -1.0 / (T * T) * np.sum(logits * (y_true - probs))
            T -= lr * grad
            T = max(T, 0.01)  # Eviter T <= 0

        self.temperature = T
        self.is_fitted = True
        self.logger.info("TemperatureScaler entraine: T=%.4f (%d samples)", T, len(y_true))

    def calibrate(self, y_proba: np.ndarray) -> np.ndarray:
        """Applique le temperature scaling.

        Parameters
        ----------
        y_proba : np.ndarray
            Probabilites brutes.

        Returns
        -------
        np.ndarray
            Probabilites calibrees.
        """
        if not self.is_fitted:
            raise RuntimeError("TemperatureScaler non entraine.")

        y_proba = np.asarray(y_proba).clip(1e-10, 1 - 1e-10)
        logits = np.log(y_proba / (1 - y_proba))
        scaled = logits / self.temperature
        return 1.0 / (1.0 + np.exp(-scaled))


# ===========================================================================
# FACTORY & UTILS
# ===========================================================================

CALIBRATOR_REGISTRY = {
    "platt": PlattCalibrator,
    "isotonic": IsotonicCalibrator,
    "temperature": TemperatureScaler,
}


def create_calibrator(method: str = "platt"):
    """Cree un calibrateur par nom.

    Parameters
    ----------
    method : str
        Methode de calibration (platt, isotonic, temperature).

    Returns
    -------
    PlattCalibrator | IsotonicCalibrator | TemperatureScaler
    """
    if method not in CALIBRATOR_REGISTRY:
        available = ", ".join(CALIBRATOR_REGISTRY.keys())
        raise ValueError(f"Methode inconnue: {method}. Disponibles: {available}")
    return CALIBRATOR_REGISTRY[method]()


def calibrate_probabilities(
    y_true_cal: np.ndarray,
    y_proba_cal: np.ndarray,
    y_proba_test: np.ndarray,
    method: str = "platt",
) -> np.ndarray:
    """Calibre les probabilites en une seule etape.

    Parameters
    ----------
    y_true_cal : np.ndarray
        Labels de l'ensemble de calibration.
    y_proba_cal : np.ndarray
        Probabilites brutes de l'ensemble de calibration.
    y_proba_test : np.ndarray
        Probabilites brutes a calibrer.
    method : str
        Methode de calibration.

    Returns
    -------
    np.ndarray
        Probabilites calibrees pour y_proba_test.
    """
    calibrator = create_calibrator(method)
    calibrator.fit(y_true_cal, y_proba_cal)
    return calibrator.calibrate(y_proba_test)


def compare_calibrations(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    y_true_test: np.ndarray,
    y_proba_test: np.ndarray,
    methods: Optional[list[str]] = None,
    n_bins: int = DEFAULT_N_BINS,
) -> dict:
    """Compare plusieurs methodes de calibration.

    Parameters
    ----------
    y_true : np.ndarray
        Labels de calibration.
    y_proba : np.ndarray
        Probabilites brutes de calibration.
    y_true_test : np.ndarray
        Labels de test.
    y_proba_test : np.ndarray
        Probabilites brutes de test.
    methods : list[str], optional
        Methodes a comparer. Par defaut toutes.
    n_bins : int
        Nombre de bins pour ECE/MCE.

    Returns
    -------
    dict
        Rapport comparatif avec ECE/MCE avant et apres calibration.
    """
    logger = setup_logging()
    methods = methods or list(CALIBRATOR_REGISTRY.keys())

    y_true_test = np.asarray(y_true_test)
    y_proba_test = np.asarray(y_proba_test)

    result = {
        "uncalibrated": calibration_report(y_true_test, y_proba_test, n_bins),
        "methods": {},
    }

    for method in methods:
        try:
            calibrated = calibrate_probabilities(
                np.asarray(y_true), np.asarray(y_proba),
                y_proba_test, method,
            )
            report = calibration_report(y_true_test, calibrated, n_bins)
            result["methods"][method] = report

            logger.info(
                "  %s — ECE: %.4f -> %.4f, MCE: %.4f -> %.4f",
                method,
                result["uncalibrated"]["ece"], report["ece"],
                result["uncalibrated"]["mce"], report["mce"],
            )
        except Exception as e:
            logger.warning("  %s erreur: %s", method, e)
            result["methods"][method] = {"error": str(e)}

    return result
