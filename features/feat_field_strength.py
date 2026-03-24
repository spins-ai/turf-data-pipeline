#!/usr/bin/env python3
"""
Feature Engineering — Field Strength

10 features: field strength, odds concentration, heterogeneity.

Pour chaque course, calcule la force globale du peloton et la position
relative de chaque partant.

Features produites (~10) :
  - fs_field_avg_proba       -> probabilite moyenne du champ
  - fs_field_hhi             -> HHI (concentration) des probabilites
  - fs_field_entropy         -> entropie des probabilites (mesure ouverture)
  - fs_field_heterogeneity   -> ecart-type des cotes (heterogeneite)
  - fs_field_nb_competitifs  -> nb de chevaux avec proba > seuil
  - fs_favori_dominance      -> ecart favori vs 2eme (concentration)
  - fs_rank_in_field         -> rang du cheval par proba dans le champ
  - fs_pct_above_avg         -> pct de chevaux au-dessus de la moyenne
  - fs_is_top3_market        -> True si dans le top 3 du marche
  - fs_relative_strength     -> force relative (proba cheval / proba moyenne)
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))  # project root

import logging
import math
from collections import defaultdict

log = logging.getLogger(__name__)


def compute_field_strength(partants):
    """
    Calcule les features de force du champ.
    Groupe les partants par course, puis enrichit chaque partant.
    """
    log.info(f"Calcul field strength sur {len(partants)} partants...")

    # Group by course
    course_groups = defaultdict(list)
    for idx, p in enumerate(partants):
        cuid = p.get("course_uid") or f"{p.get('date_reunion_iso','')}_{p.get('num_course','')}"
        course_groups[cuid].append((idx, p))

    enriched = 0
    for cuid, runners in course_groups.items():
        nb_partants = len(runners)
        if nb_partants < 2:
            continue

        # Collect probas
        probas = []
        for _, p in runners:
            pi = p.get("proba_implicite")
            try:
                pi = float(pi) if pi else None
            except (ValueError, TypeError):
                pi = None
            probas.append(pi)

        valid_probas = [pi for pi in probas if pi is not None and pi > 0]

        if not valid_probas:
            continue

        # Field-level features
        avg_proba = sum(valid_probas) / len(valid_probas)
        sorted_probas = sorted(valid_probas, reverse=True)

        # HHI: sum of squared probas (higher = more concentrated)
        hhi = sum(pi ** 2 for pi in valid_probas)

        # Entropy (higher = more open race)
        entropy = -sum(pi * math.log(pi) for pi in valid_probas if pi > 0)

        # Heterogeneity (stdev of probas)
        if len(valid_probas) > 1:
            mean_p = sum(valid_probas) / len(valid_probas)
            var_p = sum((pi - mean_p) ** 2 for pi in valid_probas) / len(valid_probas)
            heterogeneity = var_p ** 0.5
        else:
            heterogeneity = 0

        # Competitive threshold
        seuil = 1.0 / (2 * nb_partants)
        nb_competitifs = sum(1 for pi in valid_probas if pi > seuil)

        # Favori dominance
        favori_dom = sorted_probas[0] - sorted_probas[1] if len(sorted_probas) >= 2 else None

        # Pct above average
        pct_above = sum(1 for pi in valid_probas if pi > avg_proba) / len(valid_probas)

        # Rank probas for per-runner assignment
        proba_ranking = {}
        for rank, pi in enumerate(sorted_probas, 1):
            if pi not in proba_ranking:
                proba_ranking[pi] = rank

        # Assign features to each runner
        for j, (idx, p) in enumerate(runners):
            pi = probas[j]
            feat = {}

            feat["fs_field_avg_proba"] = round(avg_proba, 4)
            feat["fs_field_hhi"] = round(hhi, 4)
            feat["fs_field_entropy"] = round(entropy, 4)
            feat["fs_field_heterogeneity"] = round(heterogeneity, 4)
            feat["fs_field_nb_competitifs"] = nb_competitifs
            feat["fs_favori_dominance"] = round(favori_dom, 4) if favori_dom is not None else None
            feat["fs_pct_above_avg"] = round(pct_above, 4)

            if pi is not None and pi > 0:
                feat["fs_rank_in_field"] = proba_ranking.get(pi, nb_partants)
                feat["fs_is_top3_market"] = proba_ranking.get(pi, nb_partants) <= 3
                feat["fs_relative_strength"] = round(pi / avg_proba, 4) if avg_proba > 0 else None
            else:
                feat["fs_rank_in_field"] = None
                feat["fs_is_top3_market"] = None
                feat["fs_relative_strength"] = None

            p.update(feat)
            enriched += 1

    log.info(f"  -> {enriched}/{len(partants)} enrichis, {len(course_groups)} courses")
    return partants
