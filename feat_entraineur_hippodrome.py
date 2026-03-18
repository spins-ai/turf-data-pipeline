#!/usr/bin/env python3
"""
Feature Engineering — Entraineur x Hippodrome

5 features: track specialist trainer, travel/displacement.

Pour chaque partant, calcule les stats de l'entraineur sur CET hippodrome
en utilisant UNIQUEMENT les courses passees (point-in-time safe).

Features produites (~5) :
  - entr_hippo_nb_courses    -> nb courses de l'entraineur sur cet hippodrome
  - entr_hippo_taux_vic      -> taux victoire entraineur sur cet hippodrome
  - entr_hippo_taux_place    -> taux place entraineur sur cet hippodrome
  - entr_hippo_specialist    -> True si > 10 courses et taux_vic > 15%
  - entr_hippo_nb_hippodromes -> nb hippodromes differents (deplacement)
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def _norm(name):
    if not name:
        return None
    n = str(name).upper().strip()
    return n if len(n) >= 2 and n not in ("INCONNU", "NC", "N/A") else None


def _norm_hippo(name):
    if not name:
        return None
    return str(name).lower().strip()


def compute_entraineur_hippodrome(partants):
    """
    Calcule les features entraineur x hippodrome.
    Les partants DOIVENT etre tries par date.
    """
    log.info(f"Calcul entraineur-hippodrome sur {len(partants)} partants...")

    # entraineur -> {hippodrome -> stats}
    entr_hippo = defaultdict(lambda: defaultdict(lambda: {"total": 0, "wins": 0, "places": 0}))
    # entraineur -> set of hippodromes
    entr_hippos_set = defaultdict(set)

    enriched = 0
    for i, p in enumerate(partants):
        entraineur = _norm(p.get("entraineur") or p.get("nom_entraineur"))
        hippo = _norm_hippo(p.get("hippodrome"))

        if not entraineur:
            continue

        # Features from history
        h = entr_hippo.get(entraineur, {})
        h_stats = h.get(hippo) if hippo else None

        if h_stats and h_stats["total"] > 0:
            enriched += 1
            n = h_stats["total"]
            p["entr_hippo_nb_courses"] = n
            p["entr_hippo_taux_vic"] = round(h_stats["wins"] / n, 4)
            p["entr_hippo_taux_place"] = round(h_stats["places"] / n, 4)
            p["entr_hippo_specialist"] = n >= 10 and (h_stats["wins"] / n) > 0.15
        else:
            p["entr_hippo_nb_courses"] = 0

        # Number of different tracks visited
        p["entr_hippo_nb_hippodromes"] = len(entr_hippos_set.get(entraineur, set()))

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

        if hippo:
            entr_hippo[entraineur][hippo]["total"] += 1
            if classement == 1:
                entr_hippo[entraineur][hippo]["wins"] += 1
            if classement is not None and classement <= 3:
                entr_hippo[entraineur][hippo]["places"] += 1
            entr_hippos_set[entraineur].add(hippo)

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {len(entr_hippo)} entraineurs")

    log.info(f"  -> {enriched}/{len(partants)} enrichis, {len(entr_hippo)} entraineurs")
    return partants
