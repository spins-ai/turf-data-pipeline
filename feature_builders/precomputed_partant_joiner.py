"""
feature_builders.precomputed_partant_joiner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Joins pre-computed per-partant data from scripts 07/09/10/11.
These files are indexed by partant_uid and contain richer features
than what we compute on the fly from raw partants.
"""

from __future__ import annotations

import json
import os
from typing import Any


_OUTPUT_BASE = os.path.join(os.path.dirname(__file__), "..", "output")


def _load_json_index(path: str, key: str = "partant_uid") -> dict[str, dict]:
    """Load a JSON file and build a lookup dict by key."""
    if not os.path.exists(path):
        print(f"  [joiner] Not found (skipped): {path}")
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    index = {}
    for rec in data:
        k = rec.get(key)
        if k:
            index[k] = rec
    print(f"  [joiner] Loaded {len(index)} records from {os.path.basename(path)}")
    return index


def build_precomputed_partant_features(partants: list[dict]) -> list[dict]:
    """Join pre-computed per-partant features from scripts 07, 09, 10, 11.

    Only adds fields that are NOT already computed by other builders,
    to avoid redundancy.

    New features added (prefix pc_ = pre-computed):

    From cotes_marche (script 07) - 3 new:
    - pc_cote_moyenne_course: average odds in race
    - pc_cote_mediane_course: median odds in race
    - pc_ecart_cote_moyenne: difference from race average odds

    From equipements (script 09) - 4 new:
    - pc_oeilleres_prev: previous race oeillères
    - pc_retrait_oeilleres: oeillères removed (1/0)
    - pc_nb_courses_sans_oeilleres: count prior races without oeillères
    - pc_deferre_prev: previous race déferré

    From poids_handicaps (script 10) - 3 new:
    - pc_poids_precedent: weight in previous race
    - pc_evolution_poids: weight change from previous race
    - pc_poids_par_km: weight per km of distance

    From sectionals (script 11) - 4 new:
    - pc_reduction_km_sec: reduction km in seconds (human-readable)
    - pc_vitesse_relative: speed relative to race average
    - pc_ecart_redkm_gagnant: reduction km gap to winner
    - pc_ecart_temps_gagnant: time gap to winner (ms)
    """
    # Load all 4 pre-computed files
    cotes_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "07_cotes_marche", "cotes_marche.json")
    )
    equip_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "09_equipements", "equipements_historique.json")
    )
    poids_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "10_poids_handicaps", "poids_handicaps.json")
    )
    sect_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "11_sectionals", "sectionals.json")
    )

    results = []
    stats = {"cotes": 0, "equip": 0, "poids": 0, "sect": 0}

    for p in partants:
        uid = p.get("partant_uid")
        row: dict[str, Any] = {"partant_uid": uid}

        # --- Cotes marché (script 07) ---
        cotes = cotes_idx.get(uid, {})
        if cotes:
            stats["cotes"] += 1
        row["pc_cote_moyenne_course"] = cotes.get("cote_moyenne_course")
        row["pc_cote_mediane_course"] = cotes.get("cote_mediane_course")
        row["pc_ecart_cote_moyenne"] = cotes.get("ecart_cote_moyenne")

        # --- Equipements (script 09) ---
        equip = equip_idx.get(uid, {})
        if equip:
            stats["equip"] += 1
        row["pc_oeilleres_prev"] = equip.get("oeilleres_prev")
        row["pc_retrait_oeilleres"] = 1 if equip.get("retrait_oeilleres") else 0
        row["pc_nb_courses_sans_oeilleres"] = equip.get("nb_courses_sans_oeilleres")
        row["pc_deferre_prev"] = equip.get("deferre_prev")

        # --- Poids handicaps (script 10) ---
        poids = poids_idx.get(uid, {})
        if poids:
            stats["poids"] += 1
        row["pc_poids_precedent"] = poids.get("poids_precedent")
        row["pc_evolution_poids"] = poids.get("evolution_poids")
        row["pc_poids_par_km"] = poids.get("poids_par_km")

        # --- Sectionals (script 11) ---
        sect = sect_idx.get(uid, {})
        if sect:
            stats["sect"] += 1
        row["pc_reduction_km_sec"] = sect.get("reduction_km_sec")
        row["pc_vitesse_relative"] = sect.get("vitesse_relative")
        row["pc_ecart_redkm_gagnant"] = sect.get("ecart_redkm_gagnant")
        row["pc_ecart_temps_gagnant"] = sect.get("ecart_temps_gagnant")

        results.append(row)

    n = len(partants)
    print(f"  [joiner] Match rates: cotes={stats['cotes']}/{n}, equip={stats['equip']}/{n}, "
          f"poids={stats['poids']}/{n}, sect={stats['sect']}/{n}")
    return results
