#!/usr/bin/env python3
"""
feature_builders.smarkets_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
15-20 features from Smarkets exchange data (output/30).

Back/lay spread, volume, market efficiency, sharp vs soft exchange bookmaker.

Temporal integrity: exchange data is matched to partant by race date and
horse name. Only pre-race snapshots are used (no in-running data).

Usage:
    python feature_builders/smarkets_builder.py
    python feature_builders/smarkets_builder.py --input output/30_smarkets/smarkets.jsonl
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging
from utils.loaders import load_json_or_jsonl
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

SMARKETS_DEFAULT = os.path.join("output", "30_smarkets_exchange", "smarkets_exchange.jsonl")
PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "smarkets_features")

# ===========================================================================
# HELPERS
# ===========================================================================

def _safe_div(a, b, decimals=4):
    if b is None or b == 0:
        return None
    return round(a / b, decimals)

# ===========================================================================
# INDEX SMARKETS DATA
# ===========================================================================

def index_smarkets(smarkets_data: list, logger: logging.Logger) -> dict:
    """Index smarkets data by (date, horse_name_norm) for fast lookup."""
    idx = {}
    for rec in smarkets_data:
        date = str(rec.get("date", "") or rec.get("event_date", "") or "")[:10]
        horse = (rec.get("nom_cheval") or rec.get("runner_name") or "").upper().strip()
        if date and horse:
            key = (date, horse)
            idx[key] = rec
    logger.info("Index Smarkets: %d entrees", len(idx))
    return idx

# ===========================================================================
# BUILDER
# ===========================================================================

def build_smarkets_features(partants: list, smarkets_idx: dict, logger: logging.Logger) -> list:
    """Build 15-20 features from Smarkets exchange data."""

    enriched = 0
    for idx, p in enumerate(partants):
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        cheval = (p.get("nom_cheval") or "").upper().strip()

        sm = smarkets_idx.get((date_iso, cheval))
        if not sm:
            continue

        enriched += 1
        feat = {}

        # Back / Lay prices
        back = sm.get("back_price") or sm.get("best_back") or sm.get("best_back_odds")
        lay = sm.get("lay_price") or sm.get("best_lay") or sm.get("best_lay_odds")

        try:
            back = float(back) if back else None
        except (ValueError, TypeError):
            back = None
        try:
            lay = float(lay) if lay else None
        except (ValueError, TypeError):
            lay = None

        feat["sm_back_price"] = back
        feat["sm_lay_price"] = lay

        # Back-lay spread (narrower = more liquid)
        if back and lay and lay > 0:
            feat["sm_spread"] = round(lay - back, 4)
            feat["sm_spread_pct"] = round((lay - back) / lay, 4)
        else:
            feat["sm_spread"] = None
            feat["sm_spread_pct"] = None

        # Implied probability from back
        if back and back > 0:
            feat["sm_proba_back"] = round(1.0 / back, 4)
        else:
            feat["sm_proba_back"] = None

        # Implied probability from lay
        if lay and lay > 0:
            feat["sm_proba_lay"] = round(1.0 / lay, 4)
        else:
            feat["sm_proba_lay"] = None

        # Mid-price implied probability
        if back and lay and back > 0 and lay > 0:
            mid = (back + lay) / 2
            feat["sm_proba_mid"] = round(1.0 / mid, 4)
        else:
            feat["sm_proba_mid"] = None

        # Volume
        volume = sm.get("volume") or sm.get("matched_amount") or sm.get("market_volume")
        try:
            volume = float(volume) if volume else None
        except (ValueError, TypeError):
            volume = None
        feat["sm_volume"] = volume

        # Volume log (for scaling)
        if volume and volume > 0:
            feat["sm_volume_log"] = round(math.log(volume), 4)
        else:
            feat["sm_volume_log"] = None

        # Market efficiency: compare exchange proba to PMU proba
        pmu_proba = p.get("proba_implicite")
        if feat.get("sm_proba_mid") and pmu_proba and pmu_proba > 0:
            feat["sm_vs_pmu_ratio"] = round(feat["sm_proba_mid"] / pmu_proba, 4)
            feat["sm_vs_pmu_diff"] = round(feat["sm_proba_mid"] - pmu_proba, 4)
            # Sharp money indicator: exchange thinks horse is better than PMU
            feat["sm_sharp_signal"] = feat["sm_proba_mid"] > pmu_proba * 1.1
        else:
            feat["sm_vs_pmu_ratio"] = None
            feat["sm_vs_pmu_diff"] = None
            feat["sm_sharp_signal"] = None

        # Overround estimate from exchange (should be close to 100%)
        feat["sm_overround"] = sm.get("overround") or sm.get("market_overround")

        # Last traded price
        ltp = sm.get("last_traded_price") or sm.get("ltp") or sm.get("last_executed_odds")
        try:
            ltp = float(ltp) if ltp else None
        except (ValueError, TypeError):
            ltp = None
        feat["sm_last_traded_price"] = ltp

        # Price movement (LTP vs back)
        if ltp and back and back > 0:
            feat["sm_price_drift"] = round(ltp - back, 4)
            feat["sm_is_steaming"] = ltp < back * 0.95  # price shortened
            feat["sm_is_drifting"] = ltp > back * 1.10  # price lengthened
        else:
            feat["sm_price_drift"] = None
            feat["sm_is_steaming"] = None
            feat["sm_is_drifting"] = None

        p.update(feat)

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(partants), enriched)

    logger.info("Features smarkets: %d/%d enrichis (%.1f%%)",
                enriched, len(partants), 100 * enriched / max(len(partants), 1))
    return partants

# ===========================================================================
# EXPORT
# ===========================================================================



# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Features from Smarkets exchange data (output/30)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--smarkets", default=SMARKETS_DEFAULT, help="Smarkets data JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging("smarkets_builder")
    logger.info("=" * 70)
    logger.info("smarkets_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    smarkets_data = load_json_or_jsonl(args.smarkets, logger)
    smarkets_idx = index_smarkets(smarkets_data, logger)

    results = build_smarkets_features(partants, smarkets_idx, logger)

    out_path = os.path.join(args.output_dir, "smarkets_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants", len(results))


if __name__ == "__main__":
    main()
