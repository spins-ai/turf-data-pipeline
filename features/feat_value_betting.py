#!/usr/bin/env python3
"""
Feature Engineering — Value Betting

10 features: CLV (Closing Line Value), steam moves, sharp money, overbet/underbet.

Pour chaque partant, calcule des indicateurs de valeur de pari
en utilisant les cotes et volumes disponibles.

Features produites (~10) :
  - vb_clv                -> Closing Line Value (cote finale vs cote matin)
  - vb_clv_pct            -> CLV en pourcentage
  - vb_is_steam           -> True si forte baisse de cote (>15%)
  - vb_is_drift           -> True si forte hausse de cote (>20%)
  - vb_sharp_money        -> signal d'argent intelligent (proba vs marche)
  - vb_overbet_index      -> ratio mises recues vs proba implicite
  - vb_underbet_index     -> inverse : cheval sous-joue
  - vb_market_mover       -> amplitude du mouvement de cote
  - vb_proba_vs_rang      -> ecart entre proba implicite et rang musique
  - vb_edge_estimate      -> estimation de l'edge (proba model vs marche)
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))  # project root

import logging
import math
from collections import defaultdict

log = logging.getLogger(__name__)


def compute_value_betting(partants):
    """
    Calcule les features de value betting.
    Les partants DOIVENT etre tries par date.
    """
    log.info(f"Calcul features value betting sur {len(partants)} partants...")

    # Track historical accuracy of odds for calibration
    odds_accuracy = defaultdict(lambda: {"total": 0, "wins": 0})

    enriched = 0
    for i, p in enumerate(partants):
        feat = {}
        has_data = False

        # --- CLV (Closing Line Value) ---
        cote_matin = p.get("cote_matin") or p.get("odds_morning")
        cote_depart = p.get("cote_depart") or p.get("rapport_probable") or p.get("odds_start")

        try:
            cote_matin = float(cote_matin) if cote_matin else None
        except (ValueError, TypeError):
            cote_matin = None
        try:
            cote_depart = float(cote_depart) if cote_depart else None
        except (ValueError, TypeError):
            cote_depart = None

        if cote_matin and cote_depart and cote_matin > 1 and cote_depart > 1:
            has_data = True
            # CLV: if closing odds are lower than opening, there was value
            clv = (1.0 / cote_depart) - (1.0 / cote_matin)
            feat["vb_clv"] = round(clv, 4)
            feat["vb_clv_pct"] = round(clv * 100, 2)

            # Market mover amplitude
            pct_change = (cote_depart - cote_matin) / cote_matin
            feat["vb_market_mover"] = round(pct_change, 4)

            # Steam move: odds shortened significantly
            feat["vb_is_steam"] = pct_change < -0.15
            # Drift: odds lengthened significantly
            feat["vb_is_drift"] = pct_change > 0.20

        # --- Sharp money signal ---
        proba_implicite = p.get("proba_implicite")
        try:
            proba_implicite = float(proba_implicite) if proba_implicite else None
        except (ValueError, TypeError):
            proba_implicite = None

        # Use exchange data if available for sharper signal
        sm_proba = p.get("sm_proba_mid")
        try:
            sm_proba = float(sm_proba) if sm_proba else None
        except (ValueError, TypeError):
            sm_proba = None

        if sm_proba and proba_implicite and proba_implicite > 0:
            has_data = True
            # Sharp money: exchange thinks horse is better than PMU
            feat["vb_sharp_money"] = round(sm_proba - proba_implicite, 4)
        elif cote_depart and cote_matin and cote_depart < cote_matin * 0.85:
            # No exchange, but big steam move = sharp money proxy
            feat["vb_sharp_money"] = round((1 / cote_depart) - (1 / cote_matin), 4)

        # --- Overbet / Underbet index ---
        enjeu_partant = p.get("enjeu_partant") or p.get("volume_mises")
        enjeu_total = p.get("enjeu_course") or p.get("total_pool")

        try:
            enjeu_partant = float(enjeu_partant) if enjeu_partant else None
        except (ValueError, TypeError):
            enjeu_partant = None
        try:
            enjeu_total = float(enjeu_total) if enjeu_total else None
        except (ValueError, TypeError):
            enjeu_total = None

        if enjeu_partant and enjeu_total and enjeu_total > 0 and proba_implicite and proba_implicite > 0:
            has_data = True
            actual_share = enjeu_partant / enjeu_total
            # Overbet: more money than implied probability suggests
            feat["vb_overbet_index"] = round(actual_share / proba_implicite, 4)
            feat["vb_underbet_index"] = round(proba_implicite / actual_share, 4) if actual_share > 0 else None

        # --- Proba vs musique rank ---
        rang_musique = p.get("rang_musique") or p.get("musique_rank")
        nb_partants = p.get("nb_partants")
        try:
            rang_musique = int(rang_musique) if rang_musique else None
        except (ValueError, TypeError):
            rang_musique = None
        try:
            nb_partants = int(nb_partants) if nb_partants else None
        except (ValueError, TypeError):
            nb_partants = None

        if rang_musique and nb_partants and nb_partants > 0 and proba_implicite:
            has_data = True
            # Expected rank from probability
            expected_rank = (1.0 - proba_implicite) * nb_partants + 1
            feat["vb_proba_vs_rang"] = round(expected_rank - rang_musique, 2)

        # --- Edge estimate (if we have historical model proba) ---
        model_proba = p.get("model_proba") or p.get("predicted_proba")
        try:
            model_proba = float(model_proba) if model_proba else None
        except (ValueError, TypeError):
            model_proba = None

        if model_proba and proba_implicite and proba_implicite > 0:
            has_data = True
            feat["vb_edge_estimate"] = round(model_proba - proba_implicite, 4)

        if has_data:
            enriched += 1
            p.update(feat)

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {enriched} enrichis")

    log.info(f"  -> {enriched}/{len(partants)} enrichis ({enriched*100/max(len(partants),1):.1f}%)")
    return partants
