#!/usr/bin/env python3
"""
feature_builders.weight_handicap_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Advanced weight/handicap features for galop (flat & obstacle) races.
Weight is a CRITICAL factor in flat/obstacle racing.

Two-pass architecture:
  Pass 1 - Collect per-course weight stats (field avg, std, min, max).
  Pass 2 - Re-read and compute per-partant features using field context.

Reads partants_master.jsonl in streaming mode.

Produces:
  - weight_handicap_features.jsonl  in builder_outputs/weight_handicap/

Features per partant (10):
  - whp_weight_vs_field_avg   : (poids - field_avg) / field_std  (z-score)
  - whp_weight_per_meter      : poids_kg / (distance_km)
  - whp_is_top_weight         : 1 if heaviest in field
  - whp_is_bottom_weight      : 1 if lightest in field
  - whp_weight_advantage      : field_max - poids  (kg advantage)
  - whp_handicap_mark         : handicap_valeur if available
  - whp_surcharge_impact      : surcharge/decharge in kg
  - whp_weight_x_distance_sq  : poids * (distance_km)^2  (non-linear burden)
  - whp_weight_range_field    : max - min weight in field (spread)
  - whp_supplement_paid       : 1 if supplement_euros > 0 (confidence signal)

Usage:
    python feature_builders/weight_handicap_builder.py
    python feature_builders/weight_handicap_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/weight_handicap")
OUTPUT_FILE = OUTPUT_DIR / "weight_handicap_features.jsonl"

# Progress log every N records
_LOG_EVERY = 500_000

# Feature names for fill-rate tracking
_FEATURE_NAMES = [
    "whp_weight_vs_field_avg",
    "whp_weight_per_meter",
    "whp_is_top_weight",
    "whp_is_bottom_weight",
    "whp_weight_advantage",
    "whp_handicap_mark",
    "whp_surcharge_impact",
    "whp_weight_x_distance_sq",
    "whp_weight_range_field",
    "whp_supplement_paid",
]

# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_float_signed(val: Any) -> Optional[float]:
    """Convert a value to float allowing negatives, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PASS 1: COLLECT PER-COURSE WEIGHT STATS
# ===========================================================================


def _pass1_collect_course_stats(input_path: Path, logger) -> dict[str, dict]:
    """Read all records and collect weight stats per course_uid.

    Returns:
        dict mapping course_uid -> {
            "weights": list[float],  -- all valid poids values
            "avg": float, "std": float, "min": float, "max": float
        }
    """
    logger.info("=== Pass 1: Collecting per-course weight stats ===")
    t0 = time.time()

    # course_uid -> list of weights
    course_weights: dict[str, list[float]] = {}
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1: %d records lus...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid")
        if not course_uid:
            continue

        poids = _safe_float(rec.get("poids_porte"))
        if poids is None:
            continue

        if course_uid not in course_weights:
            course_weights[course_uid] = []
        course_weights[course_uid].append(poids)

    # Compute aggregates
    course_stats: dict[str, dict] = {}
    for cuid, weights in course_weights.items():
        n = len(weights)
        if n == 0:
            continue
        avg = sum(weights) / n
        min_w = min(weights)
        max_w = max(weights)
        if n > 1:
            variance = sum((w - avg) ** 2 for w in weights) / (n - 1)
            std = variance ** 0.5
        else:
            std = 0.0
        course_stats[cuid] = {
            "avg": round(avg, 4),
            "std": round(std, 4),
            "min": min_w,
            "max": max_w,
            "n": n,
        }

    # Free raw lists
    del course_weights
    gc.collect()

    logger.info(
        "Pass 1 terminee: %d records lus, %d courses avec poids, en %.1fs",
        n_read, len(course_stats), time.time() - t0,
    )
    return course_stats


# ===========================================================================
# FILL RATE REPORT
# ===========================================================================


def _log_fill_rates(n_written: int, fill_counts: dict[str, int], logger) -> None:
    """Log fill rate for each feature."""
    logger.info("=== Fill Rates ===")
    if n_written == 0:
        logger.warning("Aucun record ecrit, pas de fill rates a afficher.")
        return
    for name in _FEATURE_NAMES:
        count = fill_counts.get(name, 0)
        pct = round(100.0 * count / n_written, 2)
        logger.info("  %-30s : %d / %d  (%.2f%%)", name, count, n_written, pct)


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    logger = setup_logging("weight_handicap_builder")

    parser = argparse.ArgumentParser(description="Weight/Handicap feature builder")
    parser.add_argument("--input", type=str, default=None, help="Path to partants_master.jsonl")
    args = parser.parse_args()

    # Resolve input path
    if args.input:
        input_path = Path(args.input)
    else:
        input_path = INPUT_DEFAULT

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    logger.info("Input : %s", input_path)
    logger.info("Output: %s", OUTPUT_FILE)

    t_start = time.time()

    # Pass 1: collect per-course weight stats
    course_stats = _pass1_collect_course_stats(input_path, logger)

    # Pass 2: compute features and stream to disk
    # We need fill_counts from pass 2 for reporting, so we track them
    # inside _pass2_compute_features via a closure-friendly approach.
    # We re-implement the fill counting here for the report.

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp_out = OUTPUT_FILE.with_suffix(".tmp")

    n_read = 0
    n_written = 0
    fill_counts = {name: 0 for name in _FEATURE_NAMES}

    logger.info("=== Pass 2: Computing weight/handicap features ===")
    t2 = time.time()

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Pass 2: %d records traites, %d ecrits...", n_read, n_written)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid")
            date_iso = rec.get("date_reunion_iso")

            if not partant_uid or not course_uid:
                continue

            stats = course_stats.get(course_uid)

            poids = _safe_float(rec.get("poids_porte"))
            distance = _safe_float(rec.get("distance"))
            handicap_val = _safe_float_signed(rec.get("handicap_valeur"))
            surcharge = _safe_float_signed(rec.get("surcharge_decharge_kg"))
            supplement = _safe_float_signed(rec.get("supplement_euros"))

            features: dict[str, Any] = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_iso,
            }

            # whp_weight_vs_field_avg
            if poids is not None and stats and stats["std"] > 0:
                features["whp_weight_vs_field_avg"] = round((poids - stats["avg"]) / stats["std"], 4)
                fill_counts["whp_weight_vs_field_avg"] += 1
            else:
                features["whp_weight_vs_field_avg"] = None

            # whp_weight_per_meter
            if poids is not None and distance is not None and distance > 0:
                distance_km = distance / 1000.0
                features["whp_weight_per_meter"] = round(poids / distance_km, 4)
                fill_counts["whp_weight_per_meter"] += 1
            else:
                features["whp_weight_per_meter"] = None

            # whp_is_top_weight
            if poids is not None and stats:
                features["whp_is_top_weight"] = 1 if poids >= stats["max"] else 0
                fill_counts["whp_is_top_weight"] += 1
            else:
                features["whp_is_top_weight"] = None

            # whp_is_bottom_weight
            if poids is not None and stats:
                features["whp_is_bottom_weight"] = 1 if poids <= stats["min"] else 0
                fill_counts["whp_is_bottom_weight"] += 1
            else:
                features["whp_is_bottom_weight"] = None

            # whp_weight_advantage
            if poids is not None and stats:
                features["whp_weight_advantage"] = round(stats["max"] - poids, 4)
                fill_counts["whp_weight_advantage"] += 1
            else:
                features["whp_weight_advantage"] = None

            # whp_handicap_mark
            if handicap_val is not None:
                features["whp_handicap_mark"] = round(handicap_val, 4)
                fill_counts["whp_handicap_mark"] += 1
            else:
                features["whp_handicap_mark"] = None

            # whp_surcharge_impact
            if surcharge is not None:
                features["whp_surcharge_impact"] = round(surcharge, 4)
                fill_counts["whp_surcharge_impact"] += 1
            else:
                features["whp_surcharge_impact"] = None

            # whp_weight_x_distance_sq
            if poids is not None and distance is not None and distance > 0:
                distance_km = distance / 1000.0
                features["whp_weight_x_distance_sq"] = round(poids * (distance_km ** 2), 4)
                fill_counts["whp_weight_x_distance_sq"] += 1
            else:
                features["whp_weight_x_distance_sq"] = None

            # whp_weight_range_field
            if stats:
                features["whp_weight_range_field"] = round(stats["max"] - stats["min"], 4)
                fill_counts["whp_weight_range_field"] += 1
            else:
                features["whp_weight_range_field"] = None

            # whp_supplement_paid
            if supplement is not None:
                features["whp_supplement_paid"] = 1 if supplement > 0 else 0
                fill_counts["whp_supplement_paid"] += 1
            else:
                features["whp_supplement_paid"] = None

            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic rename
    if tmp_out.exists():
        if OUTPUT_FILE.exists():
            OUTPUT_FILE.unlink()
        tmp_out.rename(OUTPUT_FILE)

    logger.info(
        "Pass 2 terminee: %d records traites, %d ecrits en %.1fs",
        n_read, n_written, time.time() - t2,
    )

    # Fill rates
    _log_fill_rates(n_written, fill_counts, logger)

    # Free course stats
    del course_stats
    gc.collect()

    total_elapsed = time.time() - t_start
    logger.info(
        "=== Weight/Handicap Builder termine: %d features en %.1fs ===",
        n_written, total_elapsed,
    )


if __name__ == "__main__":
    main()
