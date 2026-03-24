"""
feature_builders.pedigree_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sire / dam / grandsire-maternal features per partant.
Temporal integrity: for any partant at date D, only races with date < D are used.
"""

from __future__ import annotations

import json
from collections import defaultdict

from utils.math import safe_rate as _safe_rate


def build_pedigree_features(partants: list[dict]) -> list[dict]:
    """Build pedigree-based features for every partant.

    For each sire/dam/grandsire, accumulates race stats of their offspring
    as we scan chronologically. Only past data (date < current) is used.

    Parameters
    ----------
    partants : list[dict]
        All partant records. Expected fields: partant_uid, date_reunion_iso,
        pere, mere, pere_mere, distance, discipline, is_gagnant, nom_cheval.

    Returns
    -------
    list[dict]
        One dict per partant_uid with pedigree features.
    """
    sorted_p = sorted(
        partants,
        key=lambda p: (p.get("date_reunion_iso", ""), p.get("course_uid", ""), p.get("num_pmu", 0)),
    )

    # Accumulate stats per sire/dam/grandsire: name -> list of race records
    sire_history: dict[str, list[dict]] = defaultdict(list)
    dam_history: dict[str, list[dict]] = defaultdict(list)
    grandsire_history: dict[str, list[dict]] = defaultdict(list)

    results = []

    for p in sorted_p:
        uid = p.get("partant_uid")
        date_iso = p.get("date_reunion_iso", "")
        pere = (p.get("pere") or "").strip()
        mere = (p.get("mere") or "").strip()
        pere_mere = (p.get("pere_mere") or "").strip()
        dist = p.get("distance")
        disc = p.get("discipline", "")
        cheval = p.get("nom_cheval", "")

        # --- Sire features ---
        pere_tv = None
        pere_nb = 0
        pere_tv_dist = None
        pere_tv_disc = None
        if pere:
            past_sire = [r for r in sire_history.get(pere, []) if r["date"] < date_iso]
            pere_nb = len(past_sire)
            pere_tv = _safe_rate(sum(1 for r in past_sire if r["gagnant"]), pere_nb)

            # At similar distance (+-300m)
            past_sire_dist = [
                r for r in past_sire
                if dist is not None and r["distance"] is not None and abs(r["distance"] - dist) <= 300
            ]
            pere_tv_dist = _safe_rate(
                sum(1 for r in past_sire_dist if r["gagnant"]),
                len(past_sire_dist),
            )

            # At same discipline
            past_sire_disc = [r for r in past_sire if r["discipline"] == disc]
            pere_tv_disc = _safe_rate(
                sum(1 for r in past_sire_disc if r["gagnant"]),
                len(past_sire_disc),
            )

        # --- Dam features ---
        mere_tv = None
        mere_nb = 0
        if mere:
            past_dam = [r for r in dam_history.get(mere, []) if r["date"] < date_iso]
            mere_nb = len(past_dam)
            mere_tv = _safe_rate(sum(1 for r in past_dam if r["gagnant"]), mere_nb)

        # --- Grandsire maternal (pere_mere) features ---
        pm_tv = None
        pm_nb = 0
        if pere_mere:
            past_pm = [r for r in grandsire_history.get(pere_mere, []) if r["date"] < date_iso]
            pm_nb = len(past_pm)
            pm_tv = _safe_rate(sum(1 for r in past_pm if r["gagnant"]), pm_nb)

        feat = {
            "partant_uid": uid,
            "pere_taux_victoire": pere_tv,
            "pere_nb_descendants_courses": pere_nb,
            "pere_taux_victoire_distance": pere_tv_dist,
            "pere_taux_victoire_discipline": pere_tv_disc,
            "mere_taux_victoire": mere_tv,
            "mere_nb_descendants_courses": mere_nb,
            "pere_mere_taux_victoire": pm_tv,
        }
        results.append(feat)

        # Record this race for future lookbacks (offspring performance)
        race_record = {
            "date": date_iso,
            "gagnant": bool(p.get("is_gagnant")),
            "distance": dist,
            "discipline": disc,
            "cheval": cheval,
        }
        if pere:
            sire_history[pere].append(race_record)
        if mere:
            dam_history[mere].append(race_record)
        if pere_mere:
            grandsire_history[pere_mere].append(race_record)

    return results


if __name__ == "__main__":
    import os
    base = os.path.join(os.path.dirname(__file__), "..", "output", "02_liste_courses")
    path = os.path.join(base, "partants_normalises.json")
    with open(path, encoding="utf-8") as f:
        partants = json.load(f)
    feats = build_pedigree_features(partants)
    print(f"Built {len(feats)} pedigree feature records.")
    if feats:
        keys = [k for k in feats[0] if k != "partant_uid"]
        print(f"Features ({len(keys)}): {', '.join(keys)}")
        for k in keys:
            filled = sum(1 for r in feats if r.get(k) is not None)
            print(f"  {k}: {filled}/{len(feats)} ({100*filled/len(feats):.1f}%)")
