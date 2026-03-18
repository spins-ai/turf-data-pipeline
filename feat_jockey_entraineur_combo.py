#!/usr/bin/env python3
"""
Feature Engineering — Jockey x Entraineur Combo

6 features: winning combo, discipline specialty.

Pour chaque partant, calcule les stats du combo jockey-entraineur
en utilisant UNIQUEMENT les courses passees (point-in-time safe).

Features produites (~6) :
  - combo_je2_nb_courses     -> nb courses ensemble
  - combo_je2_victoires      -> victoires ensemble
  - combo_je2_taux_vic       -> taux victoire ensemble
  - combo_je2_taux_place     -> taux place ensemble
  - combo_je2_is_winning     -> True si taux_vic > 20% avec >= 5 courses
  - combo_je2_disc_specialty -> taux victoire ensemble dans CETTE discipline
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def _norm(name):
    if not name:
        return None
    n = str(name).upper().strip()
    return n if len(n) >= 2 and n not in ("INCONNU", "NC", "N/A") else None


def compute_jockey_entraineur_combo(partants):
    """
    Calcule les features combo jockey x entraineur.
    Les partants DOIVENT etre tries par date.
    """
    log.info(f"Calcul combo jockey-entraineur sur {len(partants)} partants...")

    # (jockey, entraineur) -> global stats
    combo_stats = defaultdict(lambda: {"total": 0, "wins": 0, "places": 0})
    # (jockey, entraineur, discipline) -> discipline stats
    combo_disc = defaultdict(lambda: {"total": 0, "wins": 0})

    enriched = 0
    for i, p in enumerate(partants):
        jockey = _norm(p.get("jockey") or p.get("nom_jockey"))
        entraineur = _norm(p.get("entraineur") or p.get("nom_entraineur"))
        discipline = p.get("rapport_discipline_norm") or p.get("discipline_norm")

        if not jockey or not entraineur:
            continue

        combo_key = f"{jockey}|{entraineur}"
        stats = combo_stats.get(combo_key)

        if stats and stats["total"] > 0:
            enriched += 1
            n = stats["total"]
            p["combo_je2_nb_courses"] = n
            p["combo_je2_victoires"] = stats["wins"]
            p["combo_je2_taux_vic"] = round(stats["wins"] / n, 4)
            p["combo_je2_taux_place"] = round(stats["places"] / n, 4)
            p["combo_je2_is_winning"] = n >= 5 and (stats["wins"] / n) > 0.20

            # Discipline specialty
            if discipline:
                disc_key = f"{combo_key}|{discipline}"
                ds = combo_disc.get(disc_key)
                if ds and ds["total"] > 0:
                    p["combo_je2_disc_specialty"] = round(ds["wins"] / ds["total"], 4)
        else:
            p["combo_je2_nb_courses"] = 0

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

        combo_stats[combo_key]["total"] += 1
        if classement == 1:
            combo_stats[combo_key]["wins"] += 1
        if classement is not None and classement <= 3:
            combo_stats[combo_key]["places"] += 1

        if discipline:
            disc_key = f"{combo_key}|{discipline}"
            combo_disc[disc_key]["total"] += 1
            if classement == 1:
                combo_disc[disc_key]["wins"] += 1

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {len(combo_stats)} combos")

    log.info(f"  -> {enriched}/{len(partants)} enrichis, {len(combo_stats)} combos uniques")
    return partants
