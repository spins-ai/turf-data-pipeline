#!/usr/bin/env python3
"""
Feature Engineering — Cheval x Terrain Affinity

6 features: performance by going/terrain, optimal terrain.

Pour chaque partant, calcule les stats du cheval par type de terrain
en utilisant UNIQUEMENT les courses passees (point-in-time safe).

Features produites (~6) :
  - aff_ct_nb_courses_terrain  -> nb courses sur ce type de terrain
  - aff_ct_taux_vic_terrain    -> taux victoire sur ce terrain
  - aff_ct_taux_place_terrain  -> taux place sur ce terrain
  - aff_ct_optimal_terrain     -> terrain optimal (meilleur taux victoire)
  - aff_ct_is_at_optimal       -> True si course sur terrain optimal
  - aff_ct_terrain_specialist  -> True si > 3 courses et taux > 25%
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def _norm_horse(name):
    if not name:
        return None
    return str(name).upper().strip()


def _get_terrain(partant):
    terrain = (
        partant.get("meteo_terrain_category")
        or partant.get("terrain_category")
        or partant.get("etat_terrain")
    )
    if not terrain:
        return None
    return str(terrain).lower().strip()


def compute_cheval_terrain_affinity(partants):
    """
    Calcule les features d'affinite cheval x terrain.
    Les partants DOIVENT etre tries par date.
    """
    log.info(f"Calcul affinite cheval-terrain sur {len(partants)} partants...")

    # cheval -> {terrain -> stats}
    horse_terrain = defaultdict(lambda: defaultdict(lambda: {"total": 0, "wins": 0, "places": 0}))

    enriched = 0
    for i, p in enumerate(partants):
        cheval = _norm_horse(p.get("nom_cheval"))
        terrain = _get_terrain(p)

        if not cheval:
            continue

        h = horse_terrain.get(cheval, {})
        t_stats = h.get(terrain) if terrain else None

        if t_stats and t_stats["total"] > 0:
            enriched += 1
            n = t_stats["total"]
            p["aff_ct_nb_courses_terrain"] = n
            p["aff_ct_taux_vic_terrain"] = round(t_stats["wins"] / n, 4)
            p["aff_ct_taux_place_terrain"] = round(t_stats["places"] / n, 4)
            p["aff_ct_terrain_specialist"] = n >= 3 and (t_stats["wins"] / n) > 0.25
        else:
            p["aff_ct_nb_courses_terrain"] = 0

        # Optimal terrain
        if h:
            best_terrain = None
            best_rate = -1
            for t, stats in h.items():
                if stats["total"] >= 2:
                    rate = stats["wins"] / stats["total"]
                    if rate > best_rate:
                        best_rate = rate
                        best_terrain = t
            p["aff_ct_optimal_terrain"] = best_terrain
            p["aff_ct_is_at_optimal"] = (best_terrain == terrain) if best_terrain else None

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

        if cheval and terrain:
            horse_terrain[cheval][terrain]["total"] += 1
            if classement == 1:
                horse_terrain[cheval][terrain]["wins"] += 1
            if classement is not None and classement <= 3:
                horse_terrain[cheval][terrain]["places"] += 1

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {len(horse_terrain)} chevaux")

    log.info(f"  -> {enriched}/{len(partants)} enrichis")
    return partants
