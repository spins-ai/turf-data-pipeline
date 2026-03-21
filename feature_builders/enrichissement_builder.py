#!/usr/bin/env python3
"""
feature_builders.enrichissement_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
8 features from enriched partants (output/40).

Decomposed gains, odds trend, large bet detection.

Temporal integrity: enrichment data is matched to partant by UID or
(date, horse). All features come from pre-race data.

Usage:
    python feature_builders/enrichissement_builder.py
    python feature_builders/enrichissement_builder.py --enriched output/40_partants_enrichis/partants_enrichis.jsonl
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

# ===========================================================================
# CONFIG
# ===========================================================================

ENRICHED_DEFAULT = os.path.join("output", "40_partants_enrichis", "partants_enrichis.jsonl")
PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "enrichissement_features")

# ===========================================================================
# HELPERS
# ===========================================================================

# ===========================================================================
# INDEX ENRICHED DATA
# ===========================================================================

def index_enriched(enriched_data: list, logger: logging.Logger) -> dict:
    """Index enriched partants by partant_uid or (date, horse_name_norm)."""
    idx = {}
    for rec in enriched_data:
        uid = rec.get("partant_uid")
        if uid:
            idx[("uid", str(uid))] = rec
        date = str(rec.get("date_reunion_iso", "") or "")[:10]
        horse = (rec.get("nom_cheval") or "").upper().strip()
        if date and horse:
            idx[(date, horse)] = rec
    logger.info("Index enriched: %d entrees", len(idx))
    return idx

# ===========================================================================
# BUILDER
# ===========================================================================

def build_enrichissement_features(partants: list, enr_idx: dict, logger: logging.Logger) -> list:
    """Build 8 features from enriched partant data."""

    enriched_count = 0
    for idx_i, p in enumerate(partants):
        # Lookup
        uid = p.get("partant_uid")
        enr = None
        if uid:
            enr = enr_idx.get(("uid", str(uid)))
        if not enr:
            date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
            cheval = (p.get("nom_cheval") or "").upper().strip()
            enr = enr_idx.get((date_iso, cheval))

        if not enr:
            continue

        enriched_count += 1
        feat = {}

        # --- Decomposed gains ---
        gains_place = enr.get("gains_place") or enr.get("gains_places")
        gains_victoire = enr.get("gains_victoire") or enr.get("gains_victoires")
        gains_total = enr.get("gains_total") or enr.get("gains_carriere_euros")

        try:
            gains_place = float(gains_place) if gains_place else None
        except (ValueError, TypeError):
            gains_place = None
        try:
            gains_victoire = float(gains_victoire) if gains_victoire else None
        except (ValueError, TypeError):
            gains_victoire = None
        try:
            gains_total = float(gains_total) if gains_total else None
        except (ValueError, TypeError):
            gains_total = None

        feat["enr_gains_place"] = gains_place
        feat["enr_gains_victoire"] = gains_victoire

        # Ratio victoire / total gains (quality of earnings)
        if gains_victoire is not None and gains_total and gains_total > 0:
            feat["enr_ratio_gains_vic"] = round(gains_victoire / gains_total, 4)
        else:
            feat["enr_ratio_gains_vic"] = None

        # --- Odds trend ---
        cote_matin = enr.get("cote_matin") or enr.get("odds_morning")
        cote_depart = enr.get("cote_depart") or enr.get("odds_start") or enr.get("rapport_probable")

        try:
            cote_matin = float(cote_matin) if cote_matin else None
        except (ValueError, TypeError):
            cote_matin = None
        try:
            cote_depart = float(cote_depart) if cote_depart else None
        except (ValueError, TypeError):
            cote_depart = None

        feat["enr_cote_matin"] = cote_matin
        feat["enr_cote_depart"] = cote_depart

        if cote_matin and cote_depart and cote_matin > 0:
            drift = (cote_depart - cote_matin) / cote_matin
            feat["enr_odds_drift_pct"] = round(drift, 4)
            # Significant steam move (odds shortened by >20%)
            feat["enr_steam_move"] = drift < -0.20
            # Significant drift (odds lengthened by >30%)
            feat["enr_big_drift"] = drift > 0.30
        else:
            feat["enr_odds_drift_pct"] = None
            feat["enr_steam_move"] = None
            feat["enr_big_drift"] = None

        # --- Large bet detection ---
        enjeu = enr.get("enjeu_partant") or enr.get("volume_mises")
        enjeu_course = enr.get("enjeu_course") or enr.get("total_pool")

        try:
            enjeu = float(enjeu) if enjeu else None
        except (ValueError, TypeError):
            enjeu = None
        try:
            enjeu_course = float(enjeu_course) if enjeu_course else None
        except (ValueError, TypeError):
            enjeu_course = None

        if enjeu and enjeu_course and enjeu_course > 0:
            share = enjeu / enjeu_course
            feat["enr_bet_share"] = round(share, 4)
            # Overbet: horse is receiving disproportionate money
            nb_partants = p.get("nb_partants") or 10
            expected_share = 1.0 / max(nb_partants, 1)
            feat["enr_overbet_ratio"] = round(share / expected_share, 2)
        else:
            feat["enr_bet_share"] = None
            feat["enr_overbet_ratio"] = None

        p.update(feat)

        if (idx_i + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx_i + 1, len(partants), enriched_count)

    logger.info("Features enrichissement: %d/%d enrichis (%.1f%%)",
                enriched_count, len(partants), 100 * enriched_count / max(len(partants), 1))
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
    parser = argparse.ArgumentParser(description="Features from enriched partants (output/40)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--enriched", default=ENRICHED_DEFAULT, help="Enriched partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging("enrichissement_builder")
    logger.info("=" * 70)
    logger.info("enrichissement_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    enriched_data = load_json_or_jsonl(args.enriched, logger)
    enr_idx = index_enriched(enriched_data, logger)

    results = build_enrichissement_features(partants, enr_idx, logger)

    out_path = os.path.join(args.output_dir, "enrichissement_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants", len(results))


if __name__ == "__main__":
    main()
