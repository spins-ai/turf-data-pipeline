#!/usr/bin/env python3
"""
feature_builders.race_dynamic_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race dynamics features -- modeling the race field's competitive dynamics.

Reads partants_master.jsonl in streaming mode, aggregates per course,
then computes per-partant dynamic features in a second pass.

Temporal integrity: all features are derived from pre-race data
(odds, career stats, age, handicap) -- no future leakage.

Produces:
  - race_dynamic.jsonl   in builder_outputs/race_dynamic/

Features per partant (10):
  - rdy_herfindahl_index         : market concentration (sum of squared market shares)
  - rdy_top3_market_share        : combined market share of top 3 favorites
  - rdy_favorite_dominance       : market share of the #1 favorite
  - rdy_field_experience_spread  : std of nb_courses_carriere across the field
  - rdy_field_class_spread       : std of gains_carriere across the field
  - rdy_nb_contenders            : count of horses with cote < 10
  - rdy_horse_relative_class     : horse's gains_carriere / max in field
  - rdy_horse_relative_experience: horse's nb_courses / max in field
  - rdy_handicap_spread          : max - min handicap_valeur in the field
  - rdy_age_homogeneity          : 1 - (std_age / mean_age)

Usage:
    python feature_builders/race_dynamic_builder.py
    python feature_builders/race_dynamic_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_dynamic")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _stdev(values: list[float]) -> Optional[float]:
    """Population standard deviation. Returns None if fewer than 2 values."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return math.sqrt(variance)


def _mean(values: list[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


# ===========================================================================
# PASS 1 : AGGREGATE PER COURSE
# ===========================================================================


def _pass1_aggregate(input_path: Path, logger) -> tuple[
    dict[str, dict[str, Any]],
    list[dict],
]:
    """Read all records and build per-course aggregates.

    Returns
    -------
    course_agg : dict[course_uid, aggregate_dict]
        Per-course aggregated lists (cotes, gains, courses, ages, handicaps).
    slim_records : list[dict]
        Minimal per-partant records for pass 2.
    """
    logger.info("=== Pass 1: aggregation par course ===")
    t0 = time.time()

    course_agg: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "cotes": [],
        "gains": [],
        "courses": [],
        "ages": [],
        "handicaps": [],
        "nombre_partants": None,
    })

    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid", "")
        partant_uid = rec.get("partant_uid")

        cote = _safe_float(rec.get("cote_finale"))
        gains = _safe_float(rec.get("gains_carriere_euros"))
        nb_courses = _safe_int(rec.get("nb_courses_carriere"))
        age = _safe_int(rec.get("age"))
        handicap = _safe_float(rec.get("handicap_valeur"))
        nb_partants = _safe_int(rec.get("nombre_partants"))

        if course_uid:
            agg = course_agg[course_uid]
            if cote is not None and cote > 0:
                agg["cotes"].append(cote)
            if gains is not None:
                agg["gains"].append(gains)
            if nb_courses is not None:
                agg["courses"].append(nb_courses)
            if age is not None:
                agg["ages"].append(age)
            if handicap is not None:
                agg["handicaps"].append(handicap)
            if nb_partants is not None:
                agg["nombre_partants"] = nb_partants

        slim_records.append({
            "uid": partant_uid,
            "course": course_uid,
            "cote": cote,
            "gains": gains,
            "nb_courses": nb_courses,
        })

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        n_read, len(course_agg), time.time() - t0,
    )
    return dict(course_agg), slim_records


# ===========================================================================
# PRECOMPUTE COURSE-LEVEL FEATURES
# ===========================================================================


def _precompute_course_features(
    course_agg: dict[str, dict[str, Any]],
    logger,
) -> dict[str, dict[str, Any]]:
    """Compute course-level features from aggregated data.

    Returns
    -------
    dict[course_uid, feature_dict]
    """
    logger.info("=== Precompute course-level features ===")
    t0 = time.time()
    course_feats: dict[str, dict[str, Any]] = {}

    for course_uid, agg in course_agg.items():
        cotes = agg["cotes"]
        gains = agg["gains"]
        courses = agg["courses"]
        ages = agg["ages"]
        handicaps = agg["handicaps"]

        feats: dict[str, Any] = {}

        # --- Market-based features ---
        if cotes and len(cotes) >= 2:
            inv_cotes = [1.0 / c for c in cotes]
            sum_inv = sum(inv_cotes)

            if sum_inv > 0:
                market_shares = [ic / sum_inv for ic in inv_cotes]
                # 1. Herfindahl index
                feats["_hhi"] = sum(s ** 2 for s in market_shares)
                # Sort descending for top-N
                sorted_shares = sorted(market_shares, reverse=True)
                # 2. Top 3 market share
                feats["_top3_share"] = sum(sorted_shares[:3])
                # 3. Favorite dominance
                feats["_fav_dominance"] = sorted_shares[0]
            else:
                feats["_hhi"] = None
                feats["_top3_share"] = None
                feats["_fav_dominance"] = None

            # 6. Number of contenders (cote < 10)
            feats["_nb_contenders"] = sum(1 for c in cotes if c < 10.0)
        else:
            feats["_hhi"] = None
            feats["_top3_share"] = None
            feats["_fav_dominance"] = None
            feats["_nb_contenders"] = None

        # 4. Field experience spread
        feats["_exp_spread"] = _stdev([float(c) for c in courses]) if len(courses) >= 2 else None

        # 5. Field class spread
        feats["_class_spread"] = _stdev(gains) if len(gains) >= 2 else None

        # Max values for relative features (pass 2)
        feats["_max_gains"] = max(gains) if gains else None
        feats["_max_courses"] = max(courses) if courses else None

        # 9. Handicap spread
        if len(handicaps) >= 2:
            feats["_handicap_spread"] = max(handicaps) - min(handicaps)
        else:
            feats["_handicap_spread"] = None

        # 10. Age homogeneity: 1 - (std / mean)
        if len(ages) >= 2:
            age_floats = [float(a) for a in ages]
            age_std = _stdev(age_floats)
            age_mean = _mean(age_floats)
            if age_std is not None and age_mean is not None and age_mean > 0:
                feats["_age_homogeneity"] = 1.0 - (age_std / age_mean)
            else:
                feats["_age_homogeneity"] = None
        else:
            feats["_age_homogeneity"] = None

        course_feats[course_uid] = feats

    logger.info("Precomputed features pour %d courses en %.1fs", len(course_feats), time.time() - t0)
    return course_feats


# ===========================================================================
# PASS 2 : COMPUTE PER-PARTANT FEATURES
# ===========================================================================


def _pass2_compute(
    slim_records: list[dict],
    course_feats: dict[str, dict[str, Any]],
    logger,
) -> list[dict[str, Any]]:
    """Compute per-partant features using precomputed course data."""
    logger.info("=== Pass 2: features per-partant ===")
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_processed = 0

    for rec in slim_records:
        n_processed += 1
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, len(slim_records))
            gc.collect()

        course_uid = rec["course"]
        cf = course_feats.get(course_uid, {})

        feats: dict[str, Any] = {"partant_uid": rec["uid"]}

        # Course-level features (same for all runners in the course)
        feats["rdy_herfindahl_index"] = round(cf["_hhi"], 6) if cf.get("_hhi") is not None else None
        feats["rdy_top3_market_share"] = round(cf["_top3_share"], 6) if cf.get("_top3_share") is not None else None
        feats["rdy_favorite_dominance"] = round(cf["_fav_dominance"], 6) if cf.get("_fav_dominance") is not None else None
        feats["rdy_field_experience_spread"] = round(cf["_exp_spread"], 4) if cf.get("_exp_spread") is not None else None
        feats["rdy_field_class_spread"] = round(cf["_class_spread"], 2) if cf.get("_class_spread") is not None else None
        feats["rdy_nb_contenders"] = cf.get("_nb_contenders")
        feats["rdy_handicap_spread"] = round(cf["_handicap_spread"], 2) if cf.get("_handicap_spread") is not None else None
        feats["rdy_age_homogeneity"] = round(cf["_age_homogeneity"], 4) if cf.get("_age_homogeneity") is not None else None

        # Per-partant relative features
        horse_gains = rec["gains"]
        horse_courses = rec["nb_courses"]
        max_gains = cf.get("_max_gains")
        max_courses = cf.get("_max_courses")

        # 7. Relative class
        if horse_gains is not None and max_gains is not None and max_gains > 0:
            feats["rdy_horse_relative_class"] = round(horse_gains / max_gains, 6)
        else:
            feats["rdy_horse_relative_class"] = None

        # 8. Relative experience
        if horse_courses is not None and max_courses is not None and max_courses > 0:
            feats["rdy_horse_relative_experience"] = round(horse_courses / max_courses, 6)
        else:
            feats["rdy_horse_relative_experience"] = None

        results.append(feats)

    elapsed = time.time() - t0
    logger.info(
        "Pass 2 terminee: %d features en %.1fs",
        len(results), elapsed,
    )
    return results


# ===========================================================================
# SAVE (atomic .tmp -> rename)
# ===========================================================================


def _save_jsonl(records: list[dict], filepath: Path, logger) -> None:
    """Write JSONL with atomic .tmp -> rename pattern."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    tmp = filepath.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    tmp.replace(filepath)
    logger.info("Sauve JSONL: %s (%d records)", filepath, len(records))


# ===========================================================================
# CLI & MAIN
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features race dynamics a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("race_dynamic_builder")
    logger.info("=" * 70)
    logger.info("race_dynamic_builder.py -- Race Dynamics Features")
    logger.info("=" * 70)

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    # Pass 1: aggregate per course
    course_agg, slim_records = _pass1_aggregate(input_path, logger)

    # Precompute course-level features
    course_feats = _precompute_course_features(course_agg, logger)

    # Free raw aggregates
    del course_agg
    gc.collect()

    # Pass 2: per-partant features
    results = _pass2_compute(slim_records, course_feats, logger)

    # Free intermediates
    del slim_records, course_feats
    gc.collect()

    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "race_dynamic.jsonl"
    _save_jsonl(results, out_path, logger)

    # Fill rates
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info("  %s: %d/%d (%.1f%%)", k, filled, total_count, 100 * filled / total_count)

    logger.info("Termine -- %d partants traites", len(results))


if __name__ == "__main__":
    main()
