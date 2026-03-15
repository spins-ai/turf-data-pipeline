#!/usr/bin/env python3
"""
models/outsider_detector.py
=============================
Detecteur d'outsiders credibles pour les courses hippiques.

Compare les probabilites du modele aux probabilites du marche
pour identifier les coureurs sous-estimes (value bets).

Score de credibilite base sur :
  - Tendance de forme (amelioration recente)
  - Changement d'equipement
  - Descente de categorie (class drop)
  - Distance preferee

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path("logs")

# Seuils par defaut
DEFAULT_VALUE_THRESHOLD = 1.5   # model_prob / market_prob >= 1.5
DEFAULT_MIN_MARKET_ODDS = 5.0   # Cote minimum pour etre considere outsider
DEFAULT_MAX_MARKET_ODDS = 100.0


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("outsider_detector")
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
        fh = logging.FileHandler(LOG_DIR / "outsider_detector.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


# ===========================================================================
# CORE
# ===========================================================================

class OutsiderDetector:
    """Detecteur d'outsiders credibles.

    Parameters
    ----------
    value_threshold : float
        Ratio minimum model_prob / market_prob pour detecter un outsider.
    min_market_odds : float
        Cote marche minimale (filtre les favoris).
    max_market_odds : float
        Cote marche maximale (filtre les tres gros outsiders improbables).
    """

    def __init__(
        self,
        value_threshold: float = DEFAULT_VALUE_THRESHOLD,
        min_market_odds: float = DEFAULT_MIN_MARKET_ODDS,
        max_market_odds: float = DEFAULT_MAX_MARKET_ODDS,
    ):
        self.value_threshold = value_threshold
        self.min_market_odds = min_market_odds
        self.max_market_odds = max_market_odds
        self.logger = setup_logging()

    def detect(
        self,
        model_probs: np.ndarray | pd.Series,
        market_odds: np.ndarray | pd.Series,
        runner_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        """Detecte les outsiders ou le modele voit plus de valeur que le marche.

        Parameters
        ----------
        model_probs : array-like
            Probabilites du modele par coureur.
        market_odds : array-like
            Cotes du marche par coureur.
        runner_ids : list[str], optional
            Identifiants des coureurs.

        Returns
        -------
        list[dict]
            Outsiders detectes, tries par expected_value decroissant.
        """
        model_probs = np.asarray(model_probs, dtype=float)
        market_odds = np.asarray(market_odds, dtype=float)

        if runner_ids is None:
            runner_ids = [str(i) for i in range(len(model_probs))]

        outsiders = []

        for i in range(len(model_probs)):
            odds = market_odds[i]
            m_prob = model_probs[i]

            # Filtrer par cote
            if odds < self.min_market_odds or odds > self.max_market_odds:
                continue
            if np.isnan(odds) or np.isnan(m_prob) or odds <= 1:
                continue

            market_prob = 1.0 / odds
            ratio = m_prob / market_prob if market_prob > 0 else 0.0
            expected_value = m_prob * odds - 1.0

            if ratio >= self.value_threshold:
                outsiders.append({
                    "runner_id": runner_ids[i],
                    "model_prob": round(float(m_prob), 6),
                    "market_odds": round(float(odds), 2),
                    "market_prob": round(float(market_prob), 6),
                    "value_ratio": round(float(ratio), 4),
                    "expected_value": round(float(expected_value), 4),
                    "edge": round(float(m_prob - market_prob), 6),
                })

        outsiders.sort(key=lambda x: -x["expected_value"])

        self.logger.info("  %d outsiders detectes (ratio >= %.1f, cotes [%.0f, %.0f])",
                         len(outsiders), self.value_threshold,
                         self.min_market_odds, self.max_market_odds)

        return outsiders

    def score_credibility(
        self,
        outsider: dict,
        features: dict,
    ) -> dict:
        """Score de credibilite d'un outsider base sur des indicateurs contextuels.

        Parameters
        ----------
        outsider : dict
            Outsider detecte (resultat de detect()).
        features : dict
            Features du coureur. Colonnes attendues :
            - progression : "improving" / "stable" / "declining"
            - changement_equipement : bool
            - classe_actuelle / classe_precedente : pour class drop
            - distance_preferee / distance_course : pour match distance
            - forme_victoire_5 : taux victoire recent
            - jours_depuis_derniere : repos

        Returns
        -------
        dict
            Outsider enrichi avec credibility_score et facteurs.
        """
        score = 0.0
        factors: list[str] = []
        max_score = 0.0

        # 1. Tendance de forme (0-30 pts)
        max_score += 30
        progression = features.get("progression")
        if progression == "improving":
            score += 30
            factors.append("forme_en_amelioration")
        elif progression == "stable":
            score += 15
            factors.append("forme_stable")
        elif progression == "declining":
            score += 0
            factors.append("forme_en_declin")

        # 2. Changement d'equipement (0-15 pts)
        max_score += 15
        if features.get("changement_equipement"):
            score += 15
            factors.append("changement_equipement")

        # 3. Class drop (0-25 pts)
        max_score += 25
        classe_act = features.get("classe_actuelle")
        classe_prec = features.get("classe_precedente")
        if classe_act is not None and classe_prec is not None:
            try:
                if float(classe_act) < float(classe_prec):
                    score += 25
                    factors.append("descente_categorie")
                elif float(classe_act) == float(classe_prec):
                    score += 10
            except (ValueError, TypeError):
                pass

        # 4. Distance preferee (0-20 pts)
        max_score += 20
        dist_pref = features.get("distance_preferee")
        dist_course = features.get("distance_course")
        if dist_pref is not None and dist_course is not None:
            try:
                ecart = abs(float(dist_course) - float(dist_pref))
                if ecart <= 100:
                    score += 20
                    factors.append("distance_ideale")
                elif ecart <= 300:
                    score += 10
                    factors.append("distance_acceptable")
            except (ValueError, TypeError):
                pass

        # 5. Taux victoire recent (0-10 pts)
        max_score += 10
        forme_v5 = features.get("forme_victoire_5")
        if forme_v5 is not None:
            try:
                if float(forme_v5) >= 0.2:
                    score += 10
                    factors.append("bon_taux_victoire_recent")
                elif float(forme_v5) >= 0.1:
                    score += 5
            except (ValueError, TypeError):
                pass

        # Score normalise 0-100
        credibility = round(score / max_score * 100, 1) if max_score > 0 else 0.0

        result = {**outsider}
        result["credibility_score"] = credibility
        result["credibility_factors"] = factors
        result["credibility_raw"] = round(score, 1)
        result["credibility_max"] = round(max_score, 1)

        return result

    def detect_and_score(
        self,
        model_probs: np.ndarray | pd.Series,
        market_odds: np.ndarray | pd.Series,
        features_df: pd.DataFrame,
        runner_id_col: str = "partant_uid",
    ) -> list[dict]:
        """Detecte les outsiders et calcule leur score de credibilite.

        Parameters
        ----------
        model_probs : array-like
            Probabilites du modele.
        market_odds : array-like
            Cotes du marche.
        features_df : pd.DataFrame
            Features de chaque coureur (indexe par runner_id_col).
        runner_id_col : str
            Nom de la colonne identifiant le coureur.

        Returns
        -------
        list[dict]
            Outsiders detectes et scores, tries par expected_value.
        """
        runner_ids = features_df[runner_id_col].tolist() if runner_id_col in features_df.columns else None

        outsiders = self.detect(model_probs, market_odds, runner_ids)

        scored = []
        for outsider in outsiders:
            rid = outsider["runner_id"]
            # Trouver les features correspondantes
            if runner_id_col in features_df.columns:
                row = features_df[features_df[runner_id_col] == rid]
                if len(row) > 0:
                    features = row.iloc[0].to_dict()
                else:
                    features = {}
            else:
                features = {}

            scored_outsider = self.score_credibility(outsider, features)
            scored.append(scored_outsider)

        # Trier par expected_value decroissant
        scored.sort(key=lambda x: -x["expected_value"])

        self.logger.info("  %d outsiders scores", len(scored))
        for s in scored[:5]:
            self.logger.info(
                "    %s: EV=%.2f, cred=%.1f, odds=%.1f, facteurs=%s",
                s["runner_id"], s["expected_value"], s["credibility_score"],
                s["market_odds"], s.get("credibility_factors", []),
            )

        return scored

    def rank_by_expected_value(self, outsiders: list[dict]) -> list[dict]:
        """Classe les outsiders par expected value.

        Parameters
        ----------
        outsiders : list[dict]
            Outsiders detectes (avec ou sans score).

        Returns
        -------
        list[dict]
            Outsiders tries avec rang ajoute.
        """
        ranked = sorted(outsiders, key=lambda x: -x.get("expected_value", 0))
        for i, o in enumerate(ranked):
            o["rank"] = i + 1
        return ranked
