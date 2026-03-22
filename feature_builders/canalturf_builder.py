#!/usr/bin/env python3
"""
feature_builders.canalturf_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
10-15 features from CanalTurf data (output/24).

Alternative stats, cross-validation with PMU data.

Temporal integrity: CanalTurf data matched by date + horse. Only pre-race
published data is used.

Usage:
    python feature_builders/canalturf_builder.py
    python feature_builders/canalturf_builder.py --ct-data output/24_canalturf/canalturf.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

CT_DEFAULT = os.path.join("output", "24_canalturf", "canalturf.jsonl")
PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "canalturf_features")

# ===========================================================================
# HELPERS
# ===========================================================================

# ===========================================================================
# INDEX CT DATA
# ===========================================================================

def index_ct_data(ct_records: list, logger: logging.Logger) -> dict:
    """Index CanalTurf data by (date, horse_name_norm)."""
    idx = {}
    for rec in ct_records:
        date = str(rec.get("date", "") or rec.get("date_reunion_iso", "") or "")[:10]
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        if date and horse:
            idx[(date, horse)] = rec
    logger.info("Index CanalTurf: %d entrees", len(idx))
    return idx

# ===========================================================================
# BUILDER
# ===========================================================================

def build_canalturf_features(partants: list, ct_idx: dict, logger: logging.Logger) -> list:
    """Build 10-15 features from CanalTurf data."""

    enriched = 0
    for idx_i, p in enumerate(partants):
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        cheval = (p.get("nom_cheval") or "").upper().strip()

        ct = ct_idx.get((date_iso, cheval))
        if not ct:
            continue

        enriched += 1
        feat = {}

        # --- CanalTurf rating / note ---
        note = ct.get("note") or ct.get("ct_note") or ct.get("rating")
        try:
            note = float(note) if note else None
        except (ValueError, TypeError):
            note = None
        feat["ct_note"] = note

        # --- CanalTurf ranking in the race ---
        rang = ct.get("classement_ct") or ct.get("rang") or ct.get("rank")
        try:
            rang = int(rang) if rang else None
        except (ValueError, TypeError):
            rang = None
        feat["ct_rang"] = rang

        # --- CanalTurf odds / pronostic ---
        ct_cote = ct.get("cote") or ct.get("odds")
        try:
            ct_cote = float(ct_cote) if ct_cote else None
        except (ValueError, TypeError):
            ct_cote = None
        feat["ct_cote"] = ct_cote

        # Cross-validation: CT odds vs PMU odds
        pmu_cote = p.get("rapport_probable") or p.get("cote_probable")
        try:
            pmu_cote = float(pmu_cote) if pmu_cote else None
        except (ValueError, TypeError):
            pmu_cote = None

        if ct_cote and pmu_cote and pmu_cote > 0:
            feat["ct_vs_pmu_ratio"] = round(ct_cote / pmu_cote, 4)
            feat["ct_vs_pmu_diff"] = round(ct_cote - pmu_cote, 2)
            # Disagreement flag
            feat["ct_pmu_disagree"] = abs(ct_cote - pmu_cote) / pmu_cote > 0.3
        else:
            feat["ct_vs_pmu_ratio"] = None
            feat["ct_vs_pmu_diff"] = None
            feat["ct_pmu_disagree"] = None

        # --- CanalTurf pronostic selection ---
        is_selection = ct.get("selection") or ct.get("is_selection") or ct.get("pronostic")
        feat["ct_is_selection"] = bool(is_selection)

        # --- CanalTurf stats ---
        ct_win_rate = ct.get("taux_victoire") or ct.get("win_rate")
        try:
            ct_win_rate = float(ct_win_rate) if ct_win_rate else None
        except (ValueError, TypeError):
            ct_win_rate = None
        feat["ct_win_rate"] = ct_win_rate

        ct_place_rate = ct.get("taux_place") or ct.get("place_rate")
        try:
            ct_place_rate = float(ct_place_rate) if ct_place_rate else None
        except (ValueError, TypeError):
            ct_place_rate = None
        feat["ct_place_rate"] = ct_place_rate

        # --- CanalTurf comment / avis ---
        avis = ct.get("avis") or ct.get("comment") or ""
        if avis:
            avis_l = str(avis).lower()
            feat["ct_avis_positif"] = any(w in avis_l for w in ("favori", "chance", "gagne", "confiance", "forme"))
            feat["ct_avis_negatif"] = any(w in avis_l for w in ("doute", "decevant", "risque", "recul", "mefiance"))
        else:
            feat["ct_avis_positif"] = None
            feat["ct_avis_negatif"] = None

        # --- CanalTurf musique / form ---
        ct_musique = ct.get("musique") or ct.get("form_string")
        if ct_musique:
            feat["ct_has_musique"] = True
        else:
            feat["ct_has_musique"] = False

        p.update(feat)

        if (idx_i + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx_i + 1, len(partants), enriched)

    logger.info("Features CanalTurf: %d/%d enrichis (%.1f%%)",
                enriched, len(partants), 100 * enriched / max(len(partants), 1))
    return partants

# ===========================================================================
# EXPORT
# ===========================================================================



# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Features from CanalTurf data (output/24)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--ct-data", default=CT_DEFAULT, help="CanalTurf data JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging("canalturf_builder")
    logger.info("=" * 70)
    logger.info("canalturf_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    ct_data = load_json_or_jsonl(args.ct_data, logger)
    ct_idx = index_ct_data(ct_data, logger)

    results = build_canalturf_features(partants, ct_idx, logger)

    out_path = os.path.join(args.output_dir, "canalturf_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants", len(results))


if __name__ == "__main__":
    main()
