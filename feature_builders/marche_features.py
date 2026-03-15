"""
feature_builders.marche_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Market / odds-derived features per partant.
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from typing import Optional


def build_marche_features(partants: list[dict]) -> list[dict]:
    """Build market/odds-derived features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records. Expected fields: partant_uid, course_uid,
        cote_finale, cote_reference, proba_implicite.

    Returns
    -------
    list[dict]
        One dict per partant_uid with market features.
    """
    # Group partants by course_uid
    course_partants: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        course_partants[p.get("course_uid", "")].append(p)

    results = []

    for p in partants:
        uid = p.get("partant_uid")
        course_uid = p.get("course_uid", "")
        runners = course_partants.get(course_uid, [])

        # Get effective odds for this partant
        cote = p.get("cote_finale") or p.get("cote_reference")
        proba_imp = p.get("proba_implicite")

        # Collect all valid odds in the race
        race_cotes = []
        for r in runners:
            c = r.get("cote_finale") or r.get("cote_reference")
            if c is not None and c > 0:
                race_cotes.append((r.get("partant_uid"), c))

        # Sort by odds ascending (favorite first)
        race_cotes_sorted = sorted(race_cotes, key=lambda x: x[1])

        # rang_cote: rank by odds (1 = favorite)
        rang_cote = None
        for i, (r_uid, _) in enumerate(race_cotes_sorted, 1):
            if r_uid == uid:
                rang_cote = i
                break

        is_favori = rang_cote == 1 if rang_cote is not None else None
        is_deuxieme_favori = rang_cote == 2 if rang_cote is not None else None
        is_outsider = None
        if cote is not None:
            is_outsider = cote > 20

        # cote_relative: cote / median cote of race
        cote_values = [c for _, c in race_cotes]
        median_cote = statistics.median(cote_values) if cote_values else None
        cote_relative = None
        if cote is not None and median_cote is not None and median_cote > 0:
            cote_relative = cote / median_cote

        # ecart_favori: cote - cote du favori
        cote_favori = race_cotes_sorted[0][1] if race_cotes_sorted else None
        ecart_favori = None
        if cote is not None and cote_favori is not None:
            ecart_favori = cote - cote_favori

        # somme_probas (overround): sum of 1/cote for all runners
        somme_probas = None
        if cote_values:
            somme_probas = sum(1.0 / c for c in cote_values if c > 0)

        # proba_normalisee: proba_implicite / somme_probas
        proba_normalisee = None
        if proba_imp is not None and somme_probas is not None and somme_probas > 0:
            proba_normalisee = proba_imp / somme_probas

        # -- NEW: cote_median (median odds in the field) --
        cote_median = statistics.median(cote_values) if cote_values else None

        # -- NEW: cote_std (standard deviation of odds in the field) --
        cote_std = None
        if len(cote_values) >= 2:
            cote_std = statistics.stdev(cote_values)

        # -- NEW: overround (same as somme_probas, explicit naming) --
        overround = somme_probas

        # -- NEW: nb_chevaux_sous_10 (horses with odds < 10) --
        nb_chevaux_sous_10 = sum(1 for c in cote_values if c < 10)

        # -- NEW: cote_ratio_favori (horse odds / favorite odds) --
        cote_ratio_favori = None
        if cote is not None and cote_favori is not None and cote_favori > 0:
            cote_ratio_favori = cote / cote_favori

        feat = {
            "partant_uid": uid,
            "proba_implicite": proba_imp,
            "rang_cote": rang_cote,
            "is_favori": is_favori,
            "is_deuxieme_favori": is_deuxieme_favori,
            "is_outsider": is_outsider,
            "cote_relative": cote_relative,
            "ecart_favori": ecart_favori,
            "somme_probas": somme_probas,
            "proba_normalisee": proba_normalisee,
            "cote_median": cote_median,
            "cote_std": cote_std,
            "overround": overround,
            "nb_chevaux_sous_10": nb_chevaux_sous_10,
            "cote_ratio_favori": cote_ratio_favori,
        }
        results.append(feat)

    return results


if __name__ == "__main__":
    import os
    base = os.path.join(os.path.dirname(__file__), "..", "output", "02_liste_courses")
    path = os.path.join(base, "partants_normalises.json")
    with open(path, encoding="utf-8") as f:
        partants = json.load(f)
    feats = build_marche_features(partants)
    print(f"Built {len(feats)} marche feature records.")
    if feats:
        keys = [k for k in feats[0] if k != "partant_uid"]
        print(f"Features ({len(keys)}): {', '.join(keys)}")
        for k in keys:
            filled = sum(1 for r in feats if r.get(k) is not None)
            print(f"  {k}: {filled}/{len(feats)} ({100*filled/len(feats):.1f}%)")
