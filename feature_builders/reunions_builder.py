#!/usr/bin/env python3
"""
feature_builders.reunions_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
15-20 features from enriched reunions/meetings (output/39).

Direct PMU weather, incidents, betting types, audience data.

Temporal integrity: reunion metadata is attached to partants sharing the
same date + hippodrome. No future leakage (data is from the meeting itself).

Usage:
    python feature_builders/reunions_builder.py
    python feature_builders/reunions_builder.py --reunions output/39_reunions_enrichies/reunions.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging
from utils.loaders import load_json_or_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

REUNIONS_DEFAULT = os.path.join("output", "39_reunions_enrichies", "reunions.jsonl")
PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "reunions_features")

# ===========================================================================
# HELPERS
# ===========================================================================

# ===========================================================================
# INDEX REUNIONS
# ===========================================================================

def index_reunions(reunions: list, logger: logging.Logger) -> dict:
    """Index reunions by (date, hippodrome_norm)."""
    idx = {}
    for rec in reunions:
        date = str(rec.get("date_reunion_iso", "") or rec.get("date", "") or "")[:10]
        hippo = (rec.get("hippodrome") or rec.get("nom_hippodrome") or "").lower().strip()
        if date and hippo:
            idx[(date, hippo)] = rec
    # Also index by reunion_uid if available
    for rec in reunions:
        uid = rec.get("reunion_uid") or rec.get("id_reunion")
        if uid:
            idx[("uid", str(uid))] = rec
    logger.info("Index reunions: %d entrees", len(idx))
    return idx

# ===========================================================================
# BUILDER
# ===========================================================================

def build_reunions_features(partants: list, reunion_idx: dict, logger: logging.Logger) -> list:
    """Build 15-20 features from enriched reunion data."""

    enriched = 0
    for idx_i, p in enumerate(partants):
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        hippo = (p.get("hippodrome") or "").lower().strip()

        # Try multiple lookup strategies
        reu = reunion_idx.get((date_iso, hippo))
        if not reu:
            uid = p.get("reunion_uid") or p.get("id_reunion")
            if uid:
                reu = reunion_idx.get(("uid", str(uid)))

        if not reu:
            continue

        enriched += 1
        feat = {}

        # --- Weather features ---
        meteo = reu.get("meteo") or {}
        if isinstance(meteo, str):
            feat["reu_meteo_label"] = meteo
        else:
            feat["reu_temperature"] = meteo.get("temperature")
            feat["reu_vent_vitesse"] = meteo.get("vent_vitesse") or meteo.get("wind_speed")
            feat["reu_vent_direction"] = meteo.get("vent_direction") or meteo.get("wind_direction")
            feat["reu_precipitation"] = meteo.get("precipitation") or meteo.get("pluie")
            feat["reu_meteo_label"] = meteo.get("label") or meteo.get("description")

        # Terrain from reunion
        terrain = reu.get("terrain") or reu.get("etat_terrain") or reu.get("going")
        feat["reu_terrain"] = terrain
        if terrain:
            terrain_l = str(terrain).lower()
            feat["reu_terrain_souple"] = any(w in terrain_l for w in ("souple", "lourd", "collant", "soft", "heavy"))
            feat["reu_terrain_bon"] = any(w in terrain_l for w in ("bon", "good", "ferme", "firm"))

        # --- Incident flags ---
        feat["reu_nb_incidents"] = reu.get("nb_incidents") or reu.get("incidents_count") or 0
        feat["reu_has_incidents"] = (feat["reu_nb_incidents"] or 0) > 0
        feat["reu_non_partants"] = reu.get("nb_non_partants") or reu.get("non_partants_count") or 0

        # --- Betting information ---
        feat["reu_type_pari"] = reu.get("type_pari") or reu.get("bet_types")
        feat["reu_has_quinte"] = bool(reu.get("quinte") or reu.get("is_quinte"))
        feat["reu_has_tierce"] = bool(reu.get("tierce") or reu.get("is_tierce"))

        # --- Audience / importance ---
        audience = reu.get("audience") or reu.get("affluence")
        try:
            audience = float(audience) if audience else None
        except (ValueError, TypeError):
            audience = None
        feat["reu_audience"] = audience

        enjeu = reu.get("enjeu_total") or reu.get("total_pool") or reu.get("masse_enjeu")
        try:
            enjeu = float(enjeu) if enjeu else None
        except (ValueError, TypeError):
            enjeu = None
        feat["reu_enjeu_total"] = enjeu

        # --- Meeting type ---
        feat["reu_type"] = reu.get("type_reunion") or reu.get("meeting_type")
        feat["reu_pays"] = reu.get("pays") or reu.get("country")
        feat["reu_nb_courses"] = reu.get("nb_courses") or reu.get("races_count")

        # --- Piste info ---
        feat["reu_corde"] = reu.get("corde") or reu.get("rail_position")
        feat["reu_piste_type"] = reu.get("type_piste") or reu.get("track_surface")

        p.update(feat)

        if (idx_i + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx_i + 1, len(partants), enriched)

    logger.info("Features reunions: %d/%d enrichis (%.1f%%)",
                enriched, len(partants), 100 * enriched / max(len(partants), 1))
    return partants

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Features from enriched reunions (output/39)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--reunions", default=REUNIONS_DEFAULT, help="Reunions data JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging("reunions_builder")
    logger.info("=" * 70)
    logger.info("reunions_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    reunions = load_json_or_jsonl(args.reunions, logger)
    reunion_idx = index_reunions(reunions, logger)

    results = build_reunions_features(partants, reunion_idx, logger)

    out_path = os.path.join(args.output_dir, "reunions_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants", len(results))


if __name__ == "__main__":
    main()
