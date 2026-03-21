#!/usr/bin/env python3
"""
models/monte_carlo.py
======================
Simulation Monte Carlo pour les resultats de courses hippiques.

A partir de probabilites calibrees par coureur, simule N courses
et calcule :
  - Probabilites de victoire, place, exacta, tierce
  - Intervalles de confiance
  - Simulations conditionnelles (ex: si un cheval ne court pas)

Aucun appel API : traitement 100% local.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import numpy as np

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"

DEFAULT_N_SIMULATIONS = 10_000
DEFAULT_CONFIDENCE_LEVEL = 0.95


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("monte_carlo")
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
        fh = logging.FileHandler(LOG_DIR / "monte_carlo.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


# ===========================================================================
# CORE
# ===========================================================================

class MonteCarloSimulator:
    """Simulateur Monte Carlo pour courses hippiques.

    Parameters
    ----------
    n_simulations : int
        Nombre de simulations.
    confidence_level : float
        Niveau de confiance pour les intervalles (0.95 = 95%).
    random_state : int or None
        Graine aleatoire pour reproductibilite.
    """

    def __init__(
        self,
        n_simulations: int = DEFAULT_N_SIMULATIONS,
        confidence_level: float = DEFAULT_CONFIDENCE_LEVEL,
        random_state: Optional[int] = 42,
    ):
        self.n_simulations = n_simulations
        self.confidence_level = confidence_level
        self.rng = np.random.RandomState(random_state)
        self.logger = setup_logging()

    def _normalize_probabilities(self, probas: np.ndarray) -> np.ndarray:
        """Normalise les probabilites pour qu'elles somment a 1.

        Parameters
        ----------
        probas : np.ndarray
            Probabilites brutes (pas necessairement normalisees).

        Returns
        -------
        np.ndarray
            Probabilites normalisees.
        """
        probas = np.clip(probas, 1e-10, None)
        return probas / probas.sum()

    def _simulate_race(self, probas: np.ndarray) -> np.ndarray:
        """Simule une course et retourne l'ordre d'arrivee.

        Utilise la methode de Gumbel : ajouter un bruit Gumbel(0,1)
        aux log-probabilites et trier. Equivalent a un tirage
        multinomial sans remise.

        Parameters
        ----------
        probas : np.ndarray
            Probabilites normalisees de victoire par coureur.

        Returns
        -------
        np.ndarray
            Indices des coureurs dans l'ordre d'arrivee (1er, 2e, ...).
        """
        log_probas = np.log(probas)
        gumbel_noise = -np.log(-np.log(self.rng.uniform(size=len(probas))))
        scores = log_probas + gumbel_noise
        return np.argsort(-scores)  # Descending: meilleur score = 1er

    def simulate(
        self,
        probas: np.ndarray,
        runner_ids: Optional[list[str]] = None,
    ) -> dict:
        """Simule N courses et calcule les statistiques.

        Parameters
        ----------
        probas : np.ndarray
            Probabilites de victoire par coureur (seront normalisees).
        runner_ids : list[str], optional
            Identifiants des coureurs. Par defaut indices 0..N-1.

        Returns
        -------
        dict avec :
            - win_probs: probabilites de victoire
            - place_probs: probabilites de place (top 3)
            - show_probs: probabilites de show (top 5)
            - confidence_intervals: intervalles de confiance
            - exacta_probs: probabilites d'exacta (top 2 exact)
            - tierce_probs: probabilites de tierce (top 3 exact)
        """
        probas = self._normalize_probabilities(np.asarray(probas, dtype=float))
        n_runners = len(probas)

        if runner_ids is None:
            runner_ids = [str(i) for i in range(n_runners)]

        self.logger.info("Simulation MC: %d coureurs, %d simulations",
                         n_runners, self.n_simulations)

        # Matrices de resultats
        win_counts = np.zeros(n_runners)
        place_counts = np.zeros(n_runners)  # top 3
        show_counts = np.zeros(n_runners)   # top 5
        position_sums = np.zeros(n_runners)

        # Exacta (top 2 exact) et tierce (top 3 exact)
        exacta_counts: dict[tuple[int, int], int] = {}
        tierce_counts: dict[tuple[int, int, int], int] = {}

        for _ in range(self.n_simulations):
            order = self._simulate_race(probas)

            # Victoire
            win_counts[order[0]] += 1

            # Place (top 3)
            n_place = min(3, n_runners)
            for i in range(n_place):
                place_counts[order[i]] += 1

            # Show (top 5)
            n_show = min(5, n_runners)
            for i in range(n_show):
                show_counts[order[i]] += 1

            # Positions moyennes
            for pos, runner_idx in enumerate(order):
                position_sums[runner_idx] += pos + 1  # 1-indexed

            # Exacta
            if n_runners >= 2:
                key2 = (order[0], order[1])
                exacta_counts[key2] = exacta_counts.get(key2, 0) + 1

            # Tierce
            if n_runners >= 3:
                key3 = (order[0], order[1], order[2])
                tierce_counts[key3] = tierce_counts.get(key3, 0) + 1

        N = self.n_simulations

        # Probabilites
        win_probs = win_counts / N
        place_probs = place_counts / N
        show_probs = show_counts / N
        avg_positions = position_sums / N

        # Intervalles de confiance (approximation binomiale)
        alpha = 1 - self.confidence_level
        z = 1.96  # ~95%
        if self.confidence_level == 0.99:
            z = 2.576

        ci_data = {}
        for i, rid in enumerate(runner_ids):
            p = win_probs[i]
            margin = z * np.sqrt(p * (1 - p) / N)
            ci_data[rid] = {
                "win_prob": round(float(p), 6),
                "ci_lower": round(max(0, float(p - margin)), 6),
                "ci_upper": round(min(1, float(p + margin)), 6),
            }

        # Top exactas
        top_exactas = sorted(exacta_counts.items(), key=lambda x: -x[1])[:20]
        exacta_result = [
            {
                "first": runner_ids[k[0]],
                "second": runner_ids[k[1]],
                "probability": round(v / N, 6),
            }
            for k, v in top_exactas
        ]

        # Top tierces
        top_tierces = sorted(tierce_counts.items(), key=lambda x: -x[1])[:20]
        tierce_result = [
            {
                "first": runner_ids[k[0]],
                "second": runner_ids[k[1]],
                "third": runner_ids[k[2]],
                "probability": round(v / N, 6),
            }
            for k, v in top_tierces
        ]

        # Assemblage
        runners_result = []
        for i, rid in enumerate(runner_ids):
            runners_result.append({
                "runner_id": rid,
                "input_prob": round(float(probas[i]), 6),
                "win_prob": round(float(win_probs[i]), 6),
                "place_prob": round(float(place_probs[i]), 6),
                "show_prob": round(float(show_probs[i]), 6),
                "avg_position": round(float(avg_positions[i]), 2),
                "ci_lower": ci_data[rid]["ci_lower"],
                "ci_upper": ci_data[rid]["ci_upper"],
            })

        # Trier par win_prob descendant
        runners_result.sort(key=lambda r: -r["win_prob"])

        return {
            "n_simulations": self.n_simulations,
            "n_runners": n_runners,
            "confidence_level": self.confidence_level,
            "runners": runners_result,
            "top_exactas": exacta_result,
            "top_tierces": tierce_result,
        }

    def simulate_conditional(
        self,
        probas: np.ndarray,
        runner_ids: list[str],
        excluded_runners: list[str],
    ) -> dict:
        """Simulation conditionnelle : recalcule si certains coureurs ne partent pas.

        Parameters
        ----------
        probas : np.ndarray
            Probabilites originales de tous les coureurs.
        runner_ids : list[str]
            Identifiants de tous les coureurs.
        excluded_runners : list[str]
            Coureurs a exclure de la simulation.

        Returns
        -------
        dict
            Meme format que simulate(), sans les coureurs exclus.
        """
        self.logger.info("Simulation conditionnelle: exclusion de %s", excluded_runners)

        # Filtrer
        mask = [rid not in excluded_runners for rid in runner_ids]
        filtered_probas = np.asarray(probas)[mask]
        filtered_ids = [rid for rid, m in zip(runner_ids, mask) if m]

        if len(filtered_ids) == 0:
            self.logger.warning("Aucun coureur restant apres exclusion")
            return {"error": "aucun coureur restant"}

        return self.simulate(filtered_probas, filtered_ids)

    def compute_value_bets(
        self,
        simulation_result: dict,
        market_odds: dict[str, float],
    ) -> list[dict]:
        """Identifie les paris de valeur en comparant MC vs marche.

        Parameters
        ----------
        simulation_result : dict
            Resultat de simulate().
        market_odds : dict[str, float]
            Cotes du marche par runner_id.

        Returns
        -------
        list[dict]
            Paris de valeur tries par expected_value decroissant.
        """
        value_bets = []

        for runner in simulation_result["runners"]:
            rid = runner["runner_id"]
            mc_prob = runner["win_prob"]

            if rid not in market_odds or market_odds[rid] <= 1:
                continue

            odds = market_odds[rid]
            market_prob = 1.0 / odds
            expected_value = mc_prob * odds - 1.0

            value_bets.append({
                "runner_id": rid,
                "mc_win_prob": round(mc_prob, 6),
                "market_odds": odds,
                "market_prob": round(market_prob, 6),
                "expected_value": round(expected_value, 4),
                "edge": round(mc_prob - market_prob, 6),
                "ci_lower": runner["ci_lower"],
                "ci_upper": runner["ci_upper"],
            })

        # Trier par EV decroissant
        value_bets.sort(key=lambda x: -x["expected_value"])
        return value_bets
