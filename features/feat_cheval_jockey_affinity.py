#!/usr/bin/env python3
"""
Feature Engineering — Cheval x Jockey Affinity

10 features: duo history, win rate together, affinity score.

Pour chaque partant, calcule les stats du couple cheval-jockey
en utilisant UNIQUEMENT les courses passees (point-in-time safe).

Features produites (~10) :
  - aff_cj_nb_courses       -> nb courses ensemble
  - aff_cj_victoires        -> victoires ensemble
  - aff_cj_places           -> places (top 3) ensemble
  - aff_cj_taux_vic         -> taux victoire ensemble
  - aff_cj_taux_place       -> taux place ensemble
  - aff_cj_gains_total      -> gains cumules ensemble
  - aff_cj_gains_moy        -> gains moyens ensemble
  - aff_cj_affinity_score   -> score affinite (vs taux individuels)
  - aff_cj_is_first_time    -> premiere course ensemble
  - aff_cj_last_result      -> dernier resultat ensemble
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))  # project root

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def _norm(name):
    if not name:
        return None
    n = str(name).upper().strip()
    return n if len(n) >= 2 and n not in ("INCONNU", "NC", "N/A") else None


def compute_cheval_jockey_affinity(partants):
    """
    Calcule les features d'affinite cheval x jockey.
    Les partants DOIVENT etre tries par date.
    """
    log.info(f"Calcul affinite cheval-jockey sur {len(partants)} partants...")

    # History: (cheval, jockey) -> list of past results
    duo_history = defaultdict(list)
    # Individual horse/jockey win rates for affinity comparison
    horse_stats = defaultdict(lambda: {"total": 0, "wins": 0})
    jockey_stats = defaultdict(lambda: {"total": 0, "wins": 0})

    enriched = 0
    for i, p in enumerate(partants):
        cheval = _norm(p.get("nom_cheval"))
        jockey = _norm(p.get("jockey") or p.get("nom_jockey"))

        if not cheval or not jockey:
            continue

        duo_key = f"{cheval}|{jockey}"
        history = duo_history[duo_key]

        if history:
            enriched += 1
            n = len(history)
            wins = sum(1 for r in history if r.get("cl") == 1)
            places = sum(1 for r in history if r.get("cl") is not None and r["cl"] <= 3)
            gains = sum(r.get("gains", 0) for r in history)

            p["aff_cj_nb_courses"] = n
            p["aff_cj_victoires"] = wins
            p["aff_cj_places"] = places
            p["aff_cj_taux_vic"] = round(wins / n, 4)
            p["aff_cj_taux_place"] = round(places / n, 4)
            p["aff_cj_gains_total"] = round(gains, 2)
            p["aff_cj_gains_moy"] = round(gains / n, 2)
            p["aff_cj_is_first_time"] = False
            p["aff_cj_last_result"] = history[-1].get("cl")

            # Affinity score: duo win rate vs average of individual rates
            h_stats = horse_stats[cheval]
            j_stats = jockey_stats[jockey]
            h_rate = h_stats["wins"] / h_stats["total"] if h_stats["total"] > 0 else 0
            j_rate = j_stats["wins"] / j_stats["total"] if j_stats["total"] > 0 else 0
            expected = (h_rate + j_rate) / 2
            duo_rate = wins / n
            if expected > 0:
                p["aff_cj_affinity_score"] = round(duo_rate / expected, 2)
            else:
                p["aff_cj_affinity_score"] = None
        else:
            p["aff_cj_is_first_time"] = True
            p["aff_cj_nb_courses"] = 0

        # Record result
        classement = None
        for key in ("classement", "arrivee", "place"):
            v = p.get(key)
            if v is not None:
                try:
                    classement = int(v)
                    break
                except (ValueError, TypeError):
                    pass

        gains_val = 0
        try:
            gains_val = float(p.get("gains_course") or p.get("gains") or 0)
        except (ValueError, TypeError):
            pass

        duo_history[duo_key].append({"cl": classement, "gains": gains_val})
        horse_stats[cheval]["total"] += 1
        jockey_stats[jockey]["total"] += 1
        if classement == 1:
            horse_stats[cheval]["wins"] += 1
            jockey_stats[jockey]["wins"] += 1

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {len(duo_history)} duos")

    log.info(f"  -> {enriched}/{len(partants)} enrichis, {len(duo_history)} duos uniques")
    return partants
