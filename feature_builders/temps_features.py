#!/usr/bin/env python3
"""
feature_builders.temps_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
15 features from race times, speeds, and reduction kilometrique.

Temporal integrity: for each partant at date D, only races with date < D
are used for historical stats (no future leakage).

Usage:
    python feature_builders/temps_features.py
    python feature_builders/temps_features.py --input output/02_liste_courses/partants_normalises.jsonl
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict

from functools import partial

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging
from utils.loaders import load_json_or_jsonl
from utils.math import safe_mean, safe_stdev
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "temps_features")

# ===========================================================================
# HELPERS (wrappers for rounding)
# ===========================================================================

_safe_mean = partial(safe_mean, ndigits=4)
_safe_stdev = partial(safe_stdev, ndigits=4)

# ===========================================================================
# LOAD
# ===========================================================================

# ===========================================================================
# BUILDER
# ===========================================================================

def build_temps_features(partants: list, logger: logging.Logger = None) -> list:
    """Build 15 time/speed features with point-in-time safety."""
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

    # Group by course for relative computations (pre-scan)
    course_runners: dict[str, list[dict]] = defaultdict(list)
    for p in sorted_p:
        cuid = p.get("course_uid")
        if cuid:
            course_runners[cuid].append(p)

    # Pre-compute per-course stats
    course_stats: dict[str, dict] = {}
    for cuid, runners in course_runners.items():
        times = [r.get("temps_ms") for r in runners if r.get("temps_ms")]
        reductions = [r.get("reduction_km_ms") for r in runners if r.get("reduction_km_ms")]
        winner_time = None
        for r in runners:
            pos = r.get("position_arrivee") or r.get("classement")
            if pos == 1 and r.get("temps_ms"):
                winner_time = r["temps_ms"]
                break
        stats = {}
        if times:
            stats["avg_time"] = sum(times) / len(times)
            stats["times_sorted"] = sorted(times)
        if reductions:
            stats["avg_reduction"] = sum(reductions) / len(reductions)
        stats["winner_time"] = winner_time
        course_stats[cuid] = stats

    # Horse history accumulator for rolling features
    horse_history: dict[str, list[dict]] = defaultdict(list)
    enriched = 0
    results = []

    for idx, p in enumerate(sorted_p):
        cheval = (p.get("nom_cheval") or "").upper().strip()
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        cuid = p.get("course_uid")
        distance = p.get("distance")

        feat = {}
        temps_ms = p.get("temps_ms")
        red_km = p.get("reduction_km_ms")

        feat["temps_temps_ms"] = temps_ms
        feat["temps_reduction_km_ms"] = red_km

        # Speed in km/h
        if temps_ms and distance and temps_ms > 0:
            speed = (distance / 1000) / (temps_ms / 3_600_000)
            feat["temps_vitesse_kmh"] = round(speed, 2)
        else:
            feat["temps_vitesse_kmh"] = None

        # Relative to race
        stats = course_stats.get(cuid, {})

        if temps_ms and stats.get("winner_time"):
            feat["temps_relatif_vainqueur"] = temps_ms - stats["winner_time"]
            if stats["winner_time"] > 0:
                feat["temps_ecart_gagnant_pct"] = round(
                    ((temps_ms - stats["winner_time"]) / stats["winner_time"]) * 100, 3
                )
            else:
                feat["temps_ecart_gagnant_pct"] = None
        else:
            feat["temps_relatif_vainqueur"] = None
            feat["temps_ecart_gagnant_pct"] = None

        if temps_ms and stats.get("avg_time"):
            feat["temps_ecart_moyen_champ"] = round(temps_ms - stats["avg_time"], 1)
        else:
            feat["temps_ecart_moyen_champ"] = None

        if temps_ms and stats.get("times_sorted"):
            feat["temps_rang_vitesse"] = sum(1 for t in stats["times_sorted"] if t < temps_ms) + 1
        else:
            feat["temps_rang_vitesse"] = None

        if red_km and stats.get("avg_reduction"):
            feat["temps_reduction_relative"] = round(red_km - stats["avg_reduction"], 1)
        else:
            feat["temps_reduction_relative"] = None

        # Historical reduction km stats (point-in-time: only past data)
        if cheval:
            past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso]
            prior_reds = [r["reduction_km"] for r in reversed(past) if r.get("reduction_km")]

            if prior_reds:
                enriched += 1
                last_5 = prior_reds[:5]
                last_10 = prior_reds[:10]
                feat["temps_avg_reduction_5"] = round(sum(last_5) / len(last_5), 1)
                feat["temps_avg_reduction_10"] = round(sum(last_10) / len(last_10), 1)
                feat["temps_best_reduction_5"] = min(last_5)
                feat["temps_best_reduction_10"] = min(last_10)

                if len(prior_reds) >= 3:
                    mean_red = sum(prior_reds) / len(prior_reds)
                    variance = sum((r - mean_red) ** 2 for r in prior_reds) / len(prior_reds)
                    feat["temps_speed_consistency"] = round(variance ** 0.5, 1)
                else:
                    feat["temps_speed_consistency"] = None

                recent_3 = prior_reds[:3]
                prev_3 = prior_reds[3:6]
                if len(recent_3) >= 2 and len(prev_3) >= 2:
                    feat["temps_reduction_trend"] = round(
                        sum(prev_3) / len(prev_3) - sum(recent_3) / len(recent_3), 1
                    )
                else:
                    feat["temps_reduction_trend"] = None
            else:
                for k in ("temps_avg_reduction_5", "temps_avg_reduction_10",
                           "temps_best_reduction_5", "temps_best_reduction_10",
                           "temps_speed_consistency", "temps_reduction_trend"):
                    feat[k] = None
        else:
            for k in ("temps_avg_reduction_5", "temps_avg_reduction_10",
                       "temps_best_reduction_5", "temps_best_reduction_10",
                       "temps_speed_consistency", "temps_reduction_trend"):
                feat[k] = None

        p.update(feat)
        results.append(p)

        # Update horse history
        if cheval:
            horse_history[cheval].append({
                "date": date_iso,
                "reduction_km": red_km,
            })

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(sorted_p), enriched)

    logger.info("Features temps: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================



# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="15 time/speed features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("temps_features")
    logger.info("=" * 70)
    logger.info("temps_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_temps_features(partants, logger)

    out_path = os.path.join(args.output_dir, "temps_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
