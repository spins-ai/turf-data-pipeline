#!/usr/bin/env python3
"""
Feature Engineering — Cheval x Distance Affinity

8 features: optimal distance, gap to optimal, performance by distance category.

Pour chaque partant, calcule les stats du cheval par categorie de distance
en utilisant UNIQUEMENT les courses passees (point-in-time safe).

Features produites (~8) :
  - aff_cd_nb_courses_cat   -> nb courses dans cette categorie de distance
  - aff_cd_taux_vic_cat     -> taux victoire dans cette categorie
  - aff_cd_taux_place_cat   -> taux place dans cette categorie
  - aff_cd_optimal_dist     -> distance optimale (cat avec meilleur taux vic)
  - aff_cd_gap_to_optimal   -> ecart en metres par rapport a la distance optimale
  - aff_cd_is_at_optimal    -> True si course a la distance optimale
  - aff_cd_classement_moy   -> classement moyen dans cette categorie
  - aff_cd_specialist       -> True si > 3 courses et taux_vic > 25% dans cette cat
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))  # project root

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def _norm_horse(name):
    if not name:
        return None
    return str(name).upper().strip()


def _get_distance_cat(partant):
    """Get distance category from partant data."""
    cat = partant.get("rapport_distance_category") or partant.get("distance_category")
    if cat:
        return str(cat).lower().strip()
    # Derive from distance in meters
    dist = partant.get("distance") or partant.get("rapport_distance_m")
    try:
        dist = int(dist)
    except (ValueError, TypeError):
        return None
    if dist < 1300:
        return "sprint"
    elif dist < 1800:
        return "mile"
    elif dist < 2200:
        return "intermediaire"
    elif dist < 2800:
        return "classique"
    else:
        return "longue"


def _get_distance_m(partant):
    dist = partant.get("distance") or partant.get("rapport_distance_m")
    try:
        return int(dist)
    except (ValueError, TypeError):
        return None


def compute_cheval_distance_affinity(partants):
    """
    Calcule les features d'affinite cheval x distance.
    Les partants DOIVENT etre tries par date.
    """
    log.info(f"Calcul affinite cheval-distance sur {len(partants)} partants...")

    # cheval -> {distance_cat -> stats}
    horse_dist = defaultdict(lambda: defaultdict(lambda: {"total": 0, "wins": 0, "places": 0, "cl_sum": 0, "cl_n": 0}))
    # cheval -> list of (distance_m, classement) for optimal distance calc
    horse_dists_detail = defaultdict(list)

    enriched = 0
    for i, p in enumerate(partants):
        cheval = _norm_horse(p.get("nom_cheval"))
        dist_cat = _get_distance_cat(p)
        dist_m = _get_distance_m(p)

        if not cheval:
            continue

        # Extract features from history
        h = horse_dist.get(cheval, {})
        cat_stats = h.get(dist_cat) if dist_cat else None

        if cat_stats and cat_stats["total"] > 0:
            enriched += 1
            n = cat_stats["total"]
            p["aff_cd_nb_courses_cat"] = n
            p["aff_cd_taux_vic_cat"] = round(cat_stats["wins"] / n, 4)
            p["aff_cd_taux_place_cat"] = round(cat_stats["places"] / n, 4)
            p["aff_cd_specialist"] = n >= 3 and (cat_stats["wins"] / n) > 0.25
            if cat_stats["cl_n"] > 0:
                p["aff_cd_classement_moy"] = round(cat_stats["cl_sum"] / cat_stats["cl_n"], 2)
        else:
            p["aff_cd_nb_courses_cat"] = 0

        # Find optimal distance category (best win rate with enough data)
        if h:
            best_cat = None
            best_rate = -1
            for cat, stats in h.items():
                if stats["total"] >= 2:
                    rate = stats["wins"] / stats["total"]
                    if rate > best_rate:
                        best_rate = rate
                        best_cat = cat
            p["aff_cd_optimal_dist"] = best_cat
            p["aff_cd_is_at_optimal"] = (best_cat == dist_cat) if best_cat else None

        # Gap to optimal distance (in meters)
        detail = horse_dists_detail.get(cheval, [])
        if detail and dist_m:
            # Find distance with best average classement
            dist_perf = defaultdict(list)
            for d, cl in detail:
                if cl is not None:
                    dist_perf[d].append(cl)
            if dist_perf:
                best_dist = min(dist_perf.keys(), key=lambda d: sum(dist_perf[d]) / len(dist_perf[d]))
                p["aff_cd_gap_to_optimal"] = abs(dist_m - best_dist)

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

        if cheval and dist_cat:
            horse_dist[cheval][dist_cat]["total"] += 1
            if classement == 1:
                horse_dist[cheval][dist_cat]["wins"] += 1
            if classement is not None and classement <= 3:
                horse_dist[cheval][dist_cat]["places"] += 1
            if classement is not None:
                horse_dist[cheval][dist_cat]["cl_sum"] += classement
                horse_dist[cheval][dist_cat]["cl_n"] += 1

        if cheval and dist_m:
            horse_dists_detail[cheval].append((dist_m, classement))

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {len(horse_dist)} chevaux")

    log.info(f"  -> {enriched}/{len(partants)} enrichis")
    return partants
