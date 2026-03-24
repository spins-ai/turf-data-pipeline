#!/usr/bin/env python3
"""
feature_builders.racing_post_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
10-15 features from Racing Post data (output/37).

RPR (Racing Post Rating), TopSpeed, international class rating.

Temporal integrity: Racing Post ratings are matched by date + horse.
Only pre-race published ratings are used.

Usage:
    python feature_builders/racing_post_builder.py
    python feature_builders/racing_post_builder.py --rp-data output/37_racing_post/racing_post.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from functools import partial
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging
from utils.loaders import load_json_or_jsonl
from utils.math import safe_mean
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

RP_DEFAULT = os.path.join("output", "37_racing_post", "racing_post_fr.jsonl")
PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "racing_post_features")

# ===========================================================================
# HELPERS
# ===========================================================================

_safe_mean = partial(safe_mean, ndigits=2)

# ===========================================================================
# INDEX RP DATA
# ===========================================================================

def index_rp_data(rp_records: list, logger: logging.Logger) -> dict:
    """Index Racing Post data by (date, horse_name_norm)."""
    idx = {}
    for rec in rp_records:
        date = str(rec.get("date", "") or rec.get("date_reunion_iso", "") or "")[:10]
        horse = (rec.get("nom_cheval") or rec.get("horse_name") or "").upper().strip()
        if date and horse:
            idx[(date, horse)] = rec
    logger.info("Index Racing Post: %d entrees", len(idx))
    return idx

# ===========================================================================
# BUILDER
# ===========================================================================

def build_racing_post_features(partants: list, rp_idx: dict, logger: logging.Logger) -> list:
    """Build 10-15 features from Racing Post data."""

    # Track horse RPR history for rolling averages
    horse_rpr_history: dict[str, list[float]] = defaultdict(list)
    horse_ts_history: dict[str, list[float]] = defaultdict(list)

    # Sort for point-in-time
    sorted_p = sorted(partants, key=lambda p: str(p.get("date_reunion_iso", "") or ""))

    enriched = 0
    for idx_i, p in enumerate(sorted_p):
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        cheval = (p.get("nom_cheval") or "").upper().strip()

        rp = rp_idx.get((date_iso, cheval))

        feat = {}

        # --- Current race RP data ---
        if rp:
            enriched += 1

            rpr = rp.get("rpr") or rp.get("racing_post_rating")
            ts = rp.get("topspeed") or rp.get("top_speed") or rp.get("ts")
            official_rating = rp.get("official_rating") or rp.get("or")

            try:
                rpr = float(rpr) if rpr else None
            except (ValueError, TypeError):
                rpr = None
            try:
                ts = float(ts) if ts else None
            except (ValueError, TypeError):
                ts = None
            try:
                official_rating = float(official_rating) if official_rating else None
            except (ValueError, TypeError):
                official_rating = None

            feat["rp_rpr"] = rpr
            feat["rp_topspeed"] = ts
            feat["rp_official_rating"] = official_rating

            # Class rating derived from RPR
            if rpr is not None:
                if rpr >= 140:
                    feat["rp_class"] = "group1"
                elif rpr >= 120:
                    feat["rp_class"] = "group2_3"
                elif rpr >= 100:
                    feat["rp_class"] = "listed"
                elif rpr >= 80:
                    feat["rp_class"] = "handicap_top"
                else:
                    feat["rp_class"] = "handicap_low"
            else:
                feat["rp_class"] = None

            # Combined rating
            if rpr is not None and ts is not None:
                feat["rp_combined_rating"] = round((rpr + ts) / 2, 2)
            else:
                feat["rp_combined_rating"] = rpr or ts

        # --- Historical RPR averages (point-in-time) ---
        if cheval:
            rpr_hist = horse_rpr_history.get(cheval, [])
            ts_hist = horse_ts_history.get(cheval, [])

            if rpr_hist:
                feat["rp_rpr_moy_5"] = _safe_mean(rpr_hist[-5:])
                feat["rp_rpr_moy_10"] = _safe_mean(rpr_hist[-10:])
                feat["rp_rpr_best_10"] = max(rpr_hist[-10:]) if rpr_hist else None
                feat["rp_rpr_trend"] = None
                if len(rpr_hist) >= 4:
                    recent = _safe_mean(rpr_hist[-3:])
                    older = _safe_mean(rpr_hist[-10:])
                    if recent is not None and older is not None:
                        diff = recent - older
                        feat["rp_rpr_trend"] = round(diff, 2)
                        feat["rp_rpr_improving"] = diff > 3

            if ts_hist:
                feat["rp_ts_moy_5"] = _safe_mean(ts_hist[-5:])
                feat["rp_ts_best_10"] = max(ts_hist[-10:]) if ts_hist else None

            feat["rp_nb_rated_runs"] = len(rpr_hist)

            # Update history after feature extraction
            if rp:
                rpr_val = feat.get("rp_rpr")
                ts_val = feat.get("rp_topspeed")
                if rpr_val is not None:
                    horse_rpr_history[cheval].append(rpr_val)
                if ts_val is not None:
                    horse_ts_history[cheval].append(ts_val)

        p.update(feat)

        if (idx_i + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx_i + 1, len(sorted_p), enriched)

    logger.info("Features Racing Post: %d/%d enrichis (%.1f%%)",
                enriched, len(sorted_p), 100 * enriched / max(len(sorted_p), 1))
    return sorted_p

# ===========================================================================
# EXPORT
# ===========================================================================



# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Features from Racing Post data (output/37)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--rp-data", default=RP_DEFAULT, help="Racing Post data JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging("racing_post_builder")
    logger.info("=" * 70)
    logger.info("racing_post_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    rp_data = load_json_or_jsonl(args.rp_data, logger)
    rp_idx = index_rp_data(rp_data, logger)

    results = build_racing_post_features(partants, rp_idx, logger)

    out_path = os.path.join(args.output_dir, "racing_post_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants", len(results))


if __name__ == "__main__":
    main()
