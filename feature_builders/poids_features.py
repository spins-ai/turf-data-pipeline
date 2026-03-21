#!/usr/bin/env python3
"""
feature_builders.poids_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
15 features from weight carried, handicap values, and relative weight.

Temporal integrity: for each partant at date D, only races with date < D
are used for weight change comparisons (no future leakage).

Usage:
    python feature_builders/poids_features.py
    python feature_builders/poids_features.py --input output/02_liste_courses/partants_normalises.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "poids_features")

# ===========================================================================
# LOAD
# ===========================================================================

# ===========================================================================
# BUILDER
# ===========================================================================

def build_poids_features(partants: list, logger: logging.Logger = None) -> list:
    """Build 15 weight/handicap features with point-in-time safety."""
    if logger is None:
        logger = logging.getLogger(__name__)

    # Sort chronologically
    sorted_p = sorted(
        partants,
        key=lambda p: (
            str(p.get("date_reunion_iso", "") or ""),
            str(p.get("course_uid", "") or ""),
            p.get("num_pmu", 0) or 0,
        ),
    )

    # Pre-compute per-course weight stats
    course_runners: dict[str, list[dict]] = defaultdict(list)
    for p in sorted_p:
        cuid = p.get("course_uid")
        if cuid:
            course_runners[cuid].append(p)

    course_stats: dict[str, dict] = {}
    for cuid, runners in course_runners.items():
        weights = [r.get("poids_porte_kg") for r in runners if r.get("poids_porte_kg") is not None]
        if weights:
            course_stats[cuid] = {
                "avg": sum(weights) / len(weights),
                "max": max(weights),
                "min": min(weights),
                "weights_sorted": sorted(weights, reverse=True),
            }

    # Horse history for weight change detection
    horse_history: dict[str, list[dict]] = defaultdict(list)
    enriched = 0
    results = []

    for idx, p in enumerate(sorted_p):
        cheval = (p.get("nom_cheval") or "").upper().strip()
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        cuid = p.get("course_uid")
        distance = p.get("distance")

        feat = {}
        poids = p.get("poids_porte_kg")

        feat["poids_porte_kg"] = poids
        feat["poids_handicap_valeur"] = p.get("handicap_valeur")
        feat["poids_handicap_distance_m"] = p.get("handicap_distance_m")

        sup = p.get("supplement_euros")
        feat["poids_supplement"] = sup if sup is not None else 0

        surcharge = p.get("surcharge_kg") or p.get("surcharge")
        feat["poids_surcharge_kg"] = surcharge if surcharge is not None else 0

        # Relative to field
        stats = course_stats.get(cuid) if cuid else None
        if poids is not None and stats:
            enriched += 1
            feat["poids_relatif_champ"] = round(poids - stats["avg"], 2)
            feat["poids_diff_vs_avg"] = feat["poids_relatif_champ"]
            feat["poids_ecart_top_weight"] = round(poids - stats["max"], 2)
            feat["poids_ecart_min_weight"] = round(poids - stats["min"], 2)
            feat["poids_rang_poids"] = sum(1 for w in stats["weights_sorted"] if w > poids) + 1
            feat["poids_is_top_weight"] = 1 if poids == stats["max"] else 0
            feat["poids_is_bottom_weight"] = 1 if poids == stats["min"] else 0
        else:
            feat["poids_relatif_champ"] = None
            feat["poids_diff_vs_avg"] = None
            feat["poids_ecart_top_weight"] = None
            feat["poids_ecart_min_weight"] = None
            feat["poids_rang_poids"] = None
            feat["poids_is_top_weight"] = None
            feat["poids_is_bottom_weight"] = None

        # Weight per distance
        if poids is not None and distance and distance > 0:
            feat["poids_par_distance"] = round((poids / distance) * 1000, 3)
        else:
            feat["poids_par_distance"] = None

        # Weight change from last race (point-in-time)
        if cheval:
            past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso]
            if past:
                prev_poids = past[-1].get("poids")
                if poids is not None and prev_poids is not None:
                    feat["poids_diff_vs_last"] = round(poids - prev_poids, 2)
                else:
                    feat["poids_diff_vs_last"] = None
            else:
                feat["poids_diff_vs_last"] = None
        else:
            feat["poids_diff_vs_last"] = None

        p.update(feat)
        results.append(p)

        # Update history
        if cheval:
            horse_history[cheval].append({
                "date": date_iso,
                "poids": poids,
            })

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(sorted_p), enriched)

    logger.info("Features poids: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

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
    parser = argparse.ArgumentParser(description="15 weight/handicap features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("poids_features")
    logger.info("=" * 70)
    logger.info("poids_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_poids_features(partants, logger)

    out_path = os.path.join(args.output_dir, "poids_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
