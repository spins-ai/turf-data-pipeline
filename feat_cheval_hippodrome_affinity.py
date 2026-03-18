#!/usr/bin/env python3
"""
Feature Engineering — Cheval x Hippodrome Affinity

8 features: track affinity, performance by track, first time at track.

Pour chaque partant, calcule les stats du cheval sur CET hippodrome
en utilisant UNIQUEMENT les courses passees (point-in-time safe).

Features produites (~8) :
  - aff_ch_nb_courses       -> nb courses sur cet hippodrome
  - aff_ch_victoires        -> victoires sur cet hippodrome
  - aff_ch_taux_vic         -> taux victoire hippodrome
  - aff_ch_taux_place       -> taux place hippodrome
  - aff_ch_classement_moy   -> classement moyen hippodrome
  - aff_ch_is_first_visit   -> premiere visite a cet hippodrome
  - aff_ch_track_specialist -> True si > 3 courses et taux_vic > 20%
  - aff_ch_gains_hippo      -> gains cumules sur cet hippodrome
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def _norm_horse(name):
    if not name:
        return None
    return str(name).upper().strip()


def _norm_hippo(name):
    if not name:
        return None
    return str(name).lower().strip()


def compute_cheval_hippodrome_affinity(partants):
    """
    Calcule les features d'affinite cheval x hippodrome.
    Les partants DOIVENT etre tries par date.
    """
    log.info(f"Calcul affinite cheval-hippodrome sur {len(partants)} partants...")

    # (cheval, hippodrome) -> list of past results
    track_history = defaultdict(list)

    enriched = 0
    for i, p in enumerate(partants):
        cheval = _norm_horse(p.get("nom_cheval"))
        hippo = _norm_hippo(p.get("hippodrome"))

        if not cheval or not hippo:
            continue

        key = f"{cheval}|{hippo}"
        history = track_history[key]

        if history:
            enriched += 1
            n = len(history)
            wins = sum(1 for r in history if r.get("cl") == 1)
            places = sum(1 for r in history if r.get("cl") is not None and r["cl"] <= 3)
            gains = sum(r.get("gains", 0) for r in history)
            classements = [r["cl"] for r in history if r.get("cl") is not None]

            p["aff_ch_nb_courses"] = n
            p["aff_ch_victoires"] = wins
            p["aff_ch_taux_vic"] = round(wins / n, 4)
            p["aff_ch_taux_place"] = round(places / n, 4)
            p["aff_ch_gains_hippo"] = round(gains, 2)
            p["aff_ch_is_first_visit"] = False

            if classements:
                p["aff_ch_classement_moy"] = round(sum(classements) / len(classements), 2)

            # Specialist: > 3 courses and win rate > 20%
            p["aff_ch_track_specialist"] = n >= 3 and (wins / n) > 0.20
        else:
            p["aff_ch_is_first_visit"] = True
            p["aff_ch_nb_courses"] = 0

        # Record result
        classement = None
        for k in ("classement", "arrivee", "place"):
            v = p.get(k)
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

        track_history[key].append({"cl": classement, "gains": gains_val})

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {len(track_history)} combos cheval-hippodrome")

    log.info(f"  -> {enriched}/{len(partants)} enrichis, {len(track_history)} combos uniques")
    return partants
