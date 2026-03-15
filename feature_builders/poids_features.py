"""
feature_builders.poids_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Builds weight and handicap features.
"""

from __future__ import annotations

from typing import Any


def build_poids_features(partants: list[dict]) -> list[dict]:
    """Build weight/handicap features for each partant.

    Features produced (15):
    - poids_porte_kg: absolute weight carried
    - poids_handicap_valeur: handicap value
    - poids_handicap_distance_m: distance handicap (trot)
    - poids_relatif_champ: weight relative to field average
    - poids_ecart_top_weight: difference from top weight in race
    - poids_ecart_min_weight: difference from minimum weight in race
    - poids_rang_poids: rank by weight (1 = heaviest)
    - poids_supplement: supplement paid (indicates confidence)
    - poids_diff_vs_avg: weight vs field average (alias for poids_relatif_champ)
    - poids_diff_vs_last: weight change from last race
    - poids_par_distance: weight per meter ratio (kg/m * 1000)
    - poids_surcharge_kg: extra weight (surcharge)
    - poids_is_top_weight: 1 if heaviest in field
    - poids_is_bottom_weight: 1 if lightest in field
    """
    # Group partants by course to compute relative features
    courses: dict[str, list[dict]] = {}
    for p in partants:
        cuid = p.get("course_uid")
        if cuid:
            courses.setdefault(cuid, []).append(p)

    # Pre-compute per-course weight stats
    course_stats: dict[str, dict] = {}
    for cuid, runners in courses.items():
        weights = [r.get("poids_porte_kg") for r in runners if r.get("poids_porte_kg") is not None]
        if weights:
            course_stats[cuid] = {
                "avg": sum(weights) / len(weights),
                "max": max(weights),
                "min": min(weights),
                "weights_sorted": sorted(weights, reverse=True),
            }

    # Build horse history for poids_diff_vs_last
    horse_history: dict[str, list[dict]] = {}
    for p in partants:
        nom = p.get("nom_cheval")
        if nom:
            horse_history.setdefault(nom, []).append(p)
    for nom in horse_history:
        horse_history[nom].sort(key=lambda x: x.get("date_reunion_iso", ""))

    horse_idx: dict[str, dict[str, int]] = {}
    for nom, races in horse_history.items():
        idx = {}
        for i, r in enumerate(races):
            uid = r.get("partant_uid")
            if uid:
                idx[uid] = i
        horse_idx[nom] = idx

    results = []
    for p in partants:
        uid = p.get("partant_uid")
        cuid = p.get("course_uid")
        nom = p.get("nom_cheval")
        distance = p.get("distance")
        row: dict[str, Any] = {"partant_uid": uid}

        poids = p.get("poids_porte_kg")
        row["poids_porte_kg"] = poids
        row["poids_handicap_valeur"] = p.get("handicap_valeur")
        row["poids_handicap_distance_m"] = p.get("handicap_distance_m")

        sup = p.get("supplement_euros")
        row["poids_supplement"] = sup if sup is not None else 0

        # Surcharge
        surcharge = p.get("surcharge_kg") or p.get("surcharge")
        row["poids_surcharge_kg"] = surcharge if surcharge is not None else 0

        stats = course_stats.get(cuid) if cuid else None
        if poids is not None and stats:
            row["poids_relatif_champ"] = round(poids - stats["avg"], 2)
            row["poids_diff_vs_avg"] = row["poids_relatif_champ"]
            row["poids_ecart_top_weight"] = round(poids - stats["max"], 2)
            row["poids_ecart_min_weight"] = round(poids - stats["min"], 2)
            # Rank: 1 = heaviest
            row["poids_rang_poids"] = sum(1 for w in stats["weights_sorted"] if w > poids) + 1
            row["poids_is_top_weight"] = 1 if poids == stats["max"] else 0
            row["poids_is_bottom_weight"] = 1 if poids == stats["min"] else 0
        else:
            row["poids_relatif_champ"] = None
            row["poids_diff_vs_avg"] = None
            row["poids_ecart_top_weight"] = None
            row["poids_ecart_min_weight"] = None
            row["poids_rang_poids"] = None
            row["poids_is_top_weight"] = None
            row["poids_is_bottom_weight"] = None

        # Weight per distance (kg per meter * 1000 for readability)
        if poids is not None and distance and distance > 0:
            row["poids_par_distance"] = round((poids / distance) * 1000, 3)
        else:
            row["poids_par_distance"] = None

        # Weight change from last race
        if nom and nom in horse_idx:
            cur_idx = horse_idx[nom].get(uid)
            if cur_idx is not None and cur_idx > 0:
                prev_race = horse_history[nom][cur_idx - 1]
                prev_poids = prev_race.get("poids_porte_kg")
                if poids is not None and prev_poids is not None:
                    row["poids_diff_vs_last"] = round(poids - prev_poids, 2)
                else:
                    row["poids_diff_vs_last"] = None
            else:
                row["poids_diff_vs_last"] = None
        else:
            row["poids_diff_vs_last"] = None

        results.append(row)

    return results
