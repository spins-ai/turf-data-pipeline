"""
turf.handicap_weight_feature_builder
====================================

Compute weight-related features for galop / handicap races.

Weight carried is one of the most predictive features in flat racing.
This module computes relative weight within the race field, distance-adjusted
weight, and weight evolution for returning horses.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def compute_weight_features(partants: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compute weight features for each partant.

    Parameters
    ----------
    partants : list[dict]
        List of PartantNormalisé dicts.  Required keys: ``partant_uid``,
        ``course_uid``, ``nom_cheval``, ``date_reunion_iso``,
        ``poids_porte_kg``, ``distance``.

    Returns
    -------
    list[dict]
        One dict per partant with keys:
        partant_uid, course_uid, nom_cheval, date,
        poids_relatif, ecart_top_weight, poids_par_distance_km,
        evolution_poids.
    """
    # ---- Step 1: group partants by course_uid to compute intra-race stats --
    by_course: dict[str, list[dict]] = {}
    for p in partants:
        cid = p.get("course_uid", "")
        if cid:
            by_course.setdefault(cid, []).append(p)

    # Pre-compute per-course weight stats
    course_stats: dict[str, dict[str, float | None]] = {}
    for cid, runners in by_course.items():
        weights = [
            r["poids_porte_kg"]
            for r in runners
            if r.get("poids_porte_kg") is not None
        ]
        if weights:
            course_stats[cid] = {
                "mean": sum(weights) / len(weights),
                "max": max(weights),
            }
        else:
            course_stats[cid] = {"mean": None, "max": None}

    # ---- Step 2: group by horse for weight evolution --------------------
    by_horse: dict[str, list[dict]] = {}
    for p in partants:
        nom = p.get("nom_cheval", "")
        if nom:
            by_horse.setdefault(nom, []).append(p)

    # Sort each horse's history chronologically
    horse_sorted: dict[str, list[dict]] = {}
    for nom, runs in by_horse.items():
        horse_sorted[nom] = sorted(
            runs,
            key=lambda r: (r.get("date_reunion_iso", ""), r.get("numero_course", 0)),
        )

    # Build index: (nom_cheval, partant_uid) -> previous poids_porte_kg
    prev_weight: dict[str, float | None] = {}
    for nom, runs in horse_sorted.items():
        for i, curr in enumerate(runs):
            uid = curr.get("partant_uid", "")
            if i == 0:
                prev_weight[uid] = None
            else:
                prev_weight[uid] = runs[i - 1].get("poids_porte_kg")

    # ---- Step 3: compute features for every partant ---------------------
    results: list[dict[str, Any]] = []

    for p in partants:
        cid = p.get("course_uid", "")
        uid = p.get("partant_uid", "")
        poids = p.get("poids_porte_kg")
        distance = p.get("distance")

        stats = course_stats.get(cid, {"mean": None, "max": None})

        # poids_relatif = poids - mean(course)
        poids_relatif: float | None = None
        if poids is not None and stats["mean"] is not None:
            poids_relatif = round(poids - stats["mean"], 2)

        # ecart_top_weight = max(course) - poids
        ecart_top: float | None = None
        if poids is not None and stats["max"] is not None:
            ecart_top = round(stats["max"] - poids, 2)

        # poids_par_distance_km
        poids_par_km: float | None = None
        if poids is not None and distance and distance > 0:
            poids_par_km = round(poids / (distance / 1000.0), 2)

        # evolution_poids = current - previous (same horse)
        evolution: float | None = None
        pw = prev_weight.get(uid)
        if poids is not None and pw is not None:
            evolution = round(poids - pw, 2)

        results.append({
            "partant_uid": uid,
            "course_uid": cid,
            "nom_cheval": p.get("nom_cheval"),
            "date": p.get("date_reunion_iso"),
            "poids_relatif": poids_relatif,
            "ecart_top_weight": ecart_top,
            "poids_par_distance_km": poids_par_km,
            "evolution_poids": evolution,
        })

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data_path = Path(__file__).resolve().parent.parent / "output" / "02_liste_courses" / "partants_normalises.json"
    with open(data_path, encoding="utf-8") as f:
        partants = json.load(f)

    features = compute_weight_features(partants)

    print(f"Partants traités : {len(features)}")

    for key in ("poids_relatif", "ecart_top_weight", "poids_par_distance_km", "evolution_poids"):
        vals = [f[key] for f in features if f[key] is not None]
        if vals:
            print(f"{key:30s} – n={len(vals):>7d}, min={min(vals):>8.2f}, "
                  f"max={max(vals):>8.2f}, moy={sum(vals)/len(vals):>8.2f}")
        else:
            print(f"{key:30s} – aucune valeur")
