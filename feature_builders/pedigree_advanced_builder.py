#!/usr/bin/env python3
"""
feature_builders.pedigree_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
15-20 advanced pedigree features.

Grandparents analysis, inbreeding coefficient, lineage depth, stamina/speed
index derived from sire/dam performance profiles.

Temporal integrity: pedigree data is static (genetic) so no leakage risk.

Usage:
    python feature_builders/pedigree_advanced_builder.py
    python feature_builders/pedigree_advanced_builder.py --input output/02_liste_courses/partants_normalises.jsonl
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
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "pedigree_advanced_features")

# Known sire profiles (stamina/speed tendencies) — expandable lookup
# These are well-known French/international sires
SIRE_STAMINA_INDEX = {
    # Flat speed sires
    "SIYOUNI": 0.35, "WOOTTON BASSETT": 0.40, "LOPE DE VEGA": 0.45,
    "DUBAWI": 0.50, "FRANKEL": 0.55, "GALILEO": 0.65,
    "DEEP IMPACT": 0.55, "KINGMAN": 0.40,
    # Trotting sires
    "READY CASH": 0.60, "BOLD EAGLE": 0.55, "TIMOKO": 0.65,
    "LOVE YOU": 0.70, "OURASI": 0.75, "JASMIN DE FLORE": 0.60,
    # Stamina / steeplechase sires
    "SAINT DES SAINTS": 0.80, "NETWORK": 0.75, "KAPGARDE": 0.85,
    "TURGEON": 0.80, "POLIGLOTE": 0.70,
}

SIRE_PRECOCITY_INDEX = {
    "SIYOUNI": 0.80, "WOOTTON BASSETT": 0.75, "LOPE DE VEGA": 0.70,
    "DUBAWI": 0.65, "FRANKEL": 0.60, "GALILEO": 0.50,
    "KINGMAN": 0.75, "READY CASH": 0.50, "BOLD EAGLE": 0.55,
}

# ===========================================================================
# HELPERS
# ===========================================================================

def _norm_name(name) -> Optional[str]:
    if not name:
        return None
    n = str(name).upper().strip()
    return n if len(n) >= 2 else None

# ===========================================================================
# BUILDER
# ===========================================================================

def build_pedigree_advanced_features(partants: list, logger: logging.Logger) -> list:
    """Build 15-20 advanced pedigree features."""

    # Accumulate sire/dam offspring statistics across the dataset
    sire_stats: dict[str, dict] = defaultdict(lambda: {"total": 0, "wins": 0, "places": 0, "gains": 0.0})
    dam_stats: dict[str, dict] = defaultdict(lambda: {"total": 0, "wins": 0, "places": 0, "gains": 0.0})
    dam_sire_stats: dict[str, dict] = defaultdict(lambda: {"total": 0, "wins": 0})

    # Sort for point-in-time
    sorted_p = sorted(partants, key=lambda p: str(p.get("date_reunion_iso", "") or ""))

    enriched = 0
    for idx_i, p in enumerate(sorted_p):
        feat = {}

        pere = _norm_name(p.get("pere") or p.get("nom_pere") or p.get("sire"))
        mere = _norm_name(p.get("mere") or p.get("nom_mere") or p.get("dam"))
        pere_mere = _norm_name(p.get("pere_mere") or p.get("dam_sire") or p.get("broodmare_sire"))

        # Grandparents (if available)
        grand_pere_p = _norm_name(p.get("grand_pere_paternel") or p.get("grandsire"))
        grand_pere_m = _norm_name(p.get("grand_pere_maternel"))

        has_pedigree = pere or mere

        if has_pedigree:
            enriched += 1

            # --- Sire stats (point-in-time from accumulated) ---
            if pere and sire_stats[pere]["total"] > 0:
                ss = sire_stats[pere]
                feat["ped_sire_nb_offspring"] = ss["total"]
                feat["ped_sire_win_rate"] = round(ss["wins"] / ss["total"], 4)
                feat["ped_sire_place_rate"] = round(ss["places"] / ss["total"], 4)
                feat["ped_sire_avg_gains"] = round(ss["gains"] / ss["total"], 2)

            # --- Dam stats ---
            if mere and dam_stats[mere]["total"] > 0:
                ds = dam_stats[mere]
                feat["ped_dam_nb_offspring"] = ds["total"]
                feat["ped_dam_win_rate"] = round(ds["wins"] / ds["total"], 4)
                feat["ped_dam_place_rate"] = round(ds["places"] / ds["total"], 4)

            # --- Dam-sire (broodmare sire) stats ---
            if pere_mere and dam_sire_stats[pere_mere]["total"] > 0:
                dss = dam_sire_stats[pere_mere]
                feat["ped_damsire_nb"] = dss["total"]
                feat["ped_damsire_win_rate"] = round(dss["wins"] / dss["total"], 4)

            # --- Stamina / Speed index from known sires ---
            if pere:
                feat["ped_sire_stamina_idx"] = SIRE_STAMINA_INDEX.get(pere)
                feat["ped_sire_precocity_idx"] = SIRE_PRECOCITY_INDEX.get(pere)

            if pere_mere:
                feat["ped_damsire_stamina_idx"] = SIRE_STAMINA_INDEX.get(pere_mere)

            # --- Inbreeding detection (simple: shared ancestors) ---
            ancestors = set()
            inbreeding_detected = False
            for anc in [pere, mere, pere_mere, grand_pere_p, grand_pere_m]:
                if anc:
                    if anc in ancestors:
                        inbreeding_detected = True
                    ancestors.add(anc)
            feat["ped_inbreeding_detected"] = inbreeding_detected

            # Cross-nicking: sire x dam-sire combo
            if pere and pere_mere:
                nick_key = f"{pere}|{pere_mere}"
                feat["ped_nick_key"] = nick_key

            # Lineage depth (how many ancestors we know)
            depth = sum(1 for x in [pere, mere, pere_mere, grand_pere_p, grand_pere_m] if x)
            feat["ped_lineage_depth"] = depth
            feat["ped_has_full_pedigree"] = depth >= 4

            # --- Sire x Discipline match ---
            discipline = p.get("rapport_discipline_norm") or p.get("discipline_norm")
            if pere and discipline:
                stamina = SIRE_STAMINA_INDEX.get(pere)
                if stamina is not None:
                    if discipline in ("plat", "flat"):
                        feat["ped_discipline_match"] = 1.0 - stamina  # lower stamina = better for flat sprints
                    elif discipline in ("obstacle", "haies", "steeple", "hurdle", "chase"):
                        feat["ped_discipline_match"] = stamina
                    elif discipline in ("trot_attele", "trot_monte", "trot"):
                        feat["ped_discipline_match"] = 0.5  # neutral for trot

        p.update(feat)

        # --- Update sire/dam accumulators ---
        classement = None
        for key in ("classement", "arrivee", "place", "position_arrivee"):
            v = p.get(key)
            if v is not None:
                try:
                    classement = int(v)
                    break
                except (ValueError, TypeError):
                    pass

        gains_val = 0
        try:
            gains_val = float(p.get("gains_course") or p.get("gains") or 0)
        except (ValueError, TypeError):
            pass

        if pere:
            sire_stats[pere]["total"] += 1
            if classement == 1:
                sire_stats[pere]["wins"] += 1
            if classement is not None and classement <= 3:
                sire_stats[pere]["places"] += 1
            sire_stats[pere]["gains"] += gains_val

        if mere:
            dam_stats[mere]["total"] += 1
            if classement == 1:
                dam_stats[mere]["wins"] += 1
            if classement is not None and classement <= 3:
                dam_stats[mere]["places"] += 1
            dam_stats[mere]["gains"] += gains_val

        if pere_mere:
            dam_sire_stats[pere_mere]["total"] += 1
            if classement == 1:
                dam_sire_stats[pere_mere]["wins"] += 1

        if (idx_i + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis, %d sires", idx_i + 1, len(sorted_p), enriched, len(sire_stats))

    logger.info("Features pedigree_advanced: %d/%d enrichis (%.1f%%)",
                enriched, len(sorted_p), 100 * enriched / max(len(sorted_p), 1))
    return sorted_p

# ===========================================================================
# EXPORT
# ===========================================================================



# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Advanced pedigree features (grandparents, inbreeding, stamina/speed)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging("pedigree_advanced_builder")
    logger.info("=" * 70)
    logger.info("pedigree_advanced_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_pedigree_advanced_features(partants, logger)

    out_path = os.path.join(args.output_dir, "pedigree_advanced_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants", len(results))


if __name__ == "__main__":
    main()
