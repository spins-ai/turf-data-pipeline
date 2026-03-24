#!/usr/bin/env python3
"""
feature_builders.precomputed_partant_joiner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
14 features joined from pre-computed per-partant data (scripts 07/09/10/11).

These files are indexed by partant_uid and contain richer features
than what we compute on the fly from raw partants.

Usage:
    python feature_builders/precomputed_partant_joiner.py
    python feature_builders/precomputed_partant_joiner.py --input output/02_liste_courses/partants_normalises.jsonl
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "precomputed_partant_features")
_OUTPUT_BASE = os.path.join("output")

# ===========================================================================
# LOAD
# ===========================================================================

def _load_json_index(path: str, key: str, logger: logging.Logger) -> dict:
    """Load a JSON/JSONL file and build a lookup dict by key."""
    data = load_json_or_jsonl(path, logger)
    index = {}
    for rec in data:
        k = rec.get(key)
        if k:
            index[k] = rec
    if index:
        logger.info("  Index %s: %d records", os.path.basename(path), len(index))
    return index

# ===========================================================================
# BUILDER
# ===========================================================================

def build_precomputed_partant_features(partants: list, logger: logging.Logger = None) -> list:
    """Join 14 pre-computed per-partant features from scripts 07, 09, 10, 11."""
    if logger is None:
        logger = logging.getLogger(__name__)

    # Load all 4 pre-computed files
    cotes_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "07_cotes_marche", "cotes_marche.json"),
        "partant_uid", logger,
    )
    equip_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "09_equipements", "equipements_historique.json"),
        "partant_uid", logger,
    )
    poids_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "10_poids_handicaps", "poids_handicaps.json"),
        "partant_uid", logger,
    )
    sect_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "11_sectionals", "sectionals.json"),
        "partant_uid", logger,
    )

    enriched = 0
    stats = {"cotes": 0, "equip": 0, "poids": 0, "sect": 0}
    results = []

    for idx, p in enumerate(partants):
        uid = p.get("partant_uid")
        feat = {}

        # --- Cotes marche (script 07) ---
        cotes = cotes_idx.get(uid, {})
        if cotes:
            stats["cotes"] += 1
        feat["pc_cote_moyenne_course"] = cotes.get("cote_moyenne_course")
        feat["pc_cote_mediane_course"] = cotes.get("cote_mediane_course")
        feat["pc_ecart_cote_moyenne"] = cotes.get("ecart_cote_moyenne")

        # --- Equipements (script 09) ---
        equip = equip_idx.get(uid, {})
        if equip:
            stats["equip"] += 1
        feat["pc_oeilleres_prev"] = equip.get("oeilleres_prev")
        feat["pc_retrait_oeilleres"] = 1 if equip.get("retrait_oeilleres") else 0
        feat["pc_nb_courses_sans_oeilleres"] = equip.get("nb_courses_sans_oeilleres")
        feat["pc_deferre_prev"] = equip.get("deferre_prev")

        # --- Poids handicaps (script 10) ---
        poids = poids_idx.get(uid, {})
        if poids:
            stats["poids"] += 1
        feat["pc_poids_precedent"] = poids.get("poids_precedent")
        feat["pc_evolution_poids"] = poids.get("evolution_poids")
        feat["pc_poids_par_km"] = poids.get("poids_par_km")

        # --- Sectionals (script 11) ---
        sect = sect_idx.get(uid, {})
        if sect:
            stats["sect"] += 1
        feat["pc_reduction_km_sec"] = sect.get("reduction_km_sec")
        feat["pc_vitesse_relative"] = sect.get("vitesse_relative")
        feat["pc_ecart_redkm_gagnant"] = sect.get("ecart_redkm_gagnant")
        feat["pc_ecart_temps_gagnant"] = sect.get("ecart_temps_gagnant")

        if any(v is not None for v in feat.values()):
            enriched += 1

        p.update(feat)
        results.append(p)

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites", idx + 1, len(partants))

    n = len(partants)
    logger.info("Match rates: cotes=%d/%d, equip=%d/%d, poids=%d/%d, sect=%d/%d",
                stats["cotes"], n, stats["equip"], n, stats["poids"], n, stats["sect"], n)
    logger.info("Features precomputed_partant: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================



# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="14 pre-computed per-partant features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("precomputed_partant_joiner")
    logger.info("=" * 70)
    logger.info("precomputed_partant_joiner.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_precomputed_partant_features(partants, logger)

    out_path = os.path.join(args.output_dir, "precomputed_partant_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
