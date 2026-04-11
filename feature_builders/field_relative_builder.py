#!/usr/bin/env python3
"""
feature_builders.field_relative_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Within-race relative positioning features: ranks each horse against the
other horses in the same race on 12 key dimensions (age, experience,
gains, odds, weight, speed, class, form, wins).

Two-pass architecture to stay RAM-friendly on 2.93M records:
  Pass 1: stream partants_master, group key stats per course_uid
  Pass 2: compute ranks per course, stream again to emit features

Temporal integrity: all source fields (nb_courses_carriere, gains_carriere,
cote_finale, etc.) are snapshot values already available pre-race -- no
future leakage.

Produces:
  - field_relative_features.jsonl  in builder_outputs/field_relative/

Features per partant (12):
  - fld_age_rank              : age rank within field (1=youngest), normalised 0-1
  - fld_age_zscore            : (age - field_mean) / field_std
  - fld_experience_rank       : nb_courses_carriere rank (1=most experienced), 0-1
  - fld_experience_zscore     : career races z-score within field
  - fld_gains_rank            : gains_carriere rank (1=richest), 0-1
  - fld_gains_percentile      : gains percentile within field (0-1)
  - fld_cote_rank             : cote_finale rank (1=favourite), 0-1
  - fld_weight_rank           : poids_porte_kg rank (1=lightest), 0-1 (galop only)
  - fld_speed_rank            : spd_speed_figure rank (1=fastest), 0-1
  - fld_class_rank            : spd_class_rating rank (1=best), 0-1
  - fld_form_rank             : seq_serie_places rank (1=best form), 0-1
  - fld_nb_wins_rank          : nb_victoires_carriere rank (1=most wins), 0-1

Usage:
    python feature_builders/field_relative_builder.py
    python feature_builders/field_relative_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PATH = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_relative")

_LOG_EVERY = 500_000

# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger: logging.Logger):
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


# ===========================================================================
# RANKING HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _rank_ascending(values: list[Optional[float]]) -> list[Optional[int]]:
    """Rank where smallest value gets rank 1. None values get None rank."""
    indexed = [(i, v) for i, v in enumerate(values) if v is not None]
    indexed.sort(key=lambda x: x[1])
    ranks: list[Optional[int]] = [None] * len(values)
    for rank, (orig_idx, _) in enumerate(indexed, start=1):
        ranks[orig_idx] = rank
    return ranks


def _rank_descending(values: list[Optional[float]]) -> list[Optional[int]]:
    """Rank where largest value gets rank 1. None values get None rank."""
    indexed = [(i, v) for i, v in enumerate(values) if v is not None]
    indexed.sort(key=lambda x: x[1], reverse=True)
    ranks: list[Optional[int]] = [None] * len(values)
    for rank, (orig_idx, _) in enumerate(indexed, start=1):
        ranks[orig_idx] = rank
    return ranks


def _normalise_rank(rank: Optional[int], field_size: int) -> Optional[float]:
    """Normalise rank to 0-1: (rank - 1) / (field_size - 1). Returns 0.0 for single-horse fields."""
    if rank is None:
        return None
    if field_size <= 1:
        return 0.0
    return round((rank - 1) / (field_size - 1), 6)


def _zscore(value: Optional[float], mean: Optional[float], std: Optional[float]) -> Optional[float]:
    """Compute z-score. Returns None if any input is None or std is 0."""
    if value is None or mean is None or std is None or std == 0.0:
        return None
    return round((value - mean) / std, 6)


def _mean(values: list[float]) -> Optional[float]:
    """Mean of non-empty list, or None."""
    if not values:
        return None
    return sum(values) / len(values)


def _stdev(values: list[float]) -> Optional[float]:
    """Population standard deviation, or None if < 2 values."""
    if len(values) < 2:
        return None
    m = sum(values) / len(values)
    variance = sum((x - m) ** 2 for x in values) / len(values)
    return variance ** 0.5


def _percentile(value: Optional[float], sorted_vals: list[float]) -> Optional[float]:
    """Percentile of value within sorted_vals (0-1). 1.0 = highest."""
    if value is None or not sorted_vals:
        return None
    n_below = sum(1 for v in sorted_vals if v < value)
    return round(n_below / len(sorted_vals), 6)


# ===========================================================================
# PASS 1: COLLECT STATS PER COURSE
# ===========================================================================


def _pass1_collect(input_path: Path, logger: logging.Logger) -> dict[str, list[dict]]:
    """
    Stream partants_master, collect slim records grouped by course_uid.

    Returns: course_uid -> list of {num_pmu, age, nb_courses, gains, cote,
             poids, speed, class_rating, serie_places, nb_victoires}
    """
    logger.info("=== Pass 1: Collecte des stats par course ===")
    t0 = time.time()

    courses: dict[str, list[dict]] = {}
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 - lu %d records...", n_read)

        course_uid = rec.get("course_uid", "")
        if not course_uid:
            continue

        num_pmu = rec.get("num_pmu")

        slim = {
            "num_pmu": num_pmu,
            "age": _safe_int(rec.get("age")),
            "nb_courses": _safe_int(rec.get("nb_courses_carriere")),
            "gains": _safe_float(rec.get("gains_carriere") or rec.get("gains_carriere_euros")),
            "cote": _safe_float(rec.get("cote_finale") or rec.get("rapport_final")),
            "poids": _safe_float(rec.get("poids_porte_kg")),
            "speed": _safe_float(rec.get("spd_speed_figure")),
            "class_rating": _safe_float(rec.get("spd_class_rating")),
            "serie_places": _safe_float(rec.get("seq_serie_places")),
            "nb_victoires": _safe_int(rec.get("nb_victoires_carriere")),
        }

        if course_uid not in courses:
            courses[course_uid] = []
        courses[course_uid].append(slim)

    elapsed = time.time() - t0
    logger.info(
        "Pass 1 terminee: %d records lus, %d courses uniques en %.1fs",
        n_read, len(courses), elapsed,
    )

    return courses


# ===========================================================================
# COMPUTE RANKS PER COURSE
# ===========================================================================


def _compute_course_ranks(
    courses: dict[str, list[dict]],
    logger: logging.Logger,
) -> dict[str, dict[int, dict[str, Any]]]:
    """
    For each course, compute all 12 relative features per runner.

    Returns: course_uid -> {num_pmu -> {feature_name: value, ...}}
    """
    logger.info("=== Calcul des rangs par course ===")
    t0 = time.time()

    course_features: dict[str, dict[int, dict[str, Any]]] = {}
    n_courses = 0

    for course_uid, runners in courses.items():
        n_courses += 1
        field_size = len(runners)

        # Extract raw value lists
        ages = [float(r["age"]) if r["age"] is not None else None for r in runners]
        nb_courses_list = [float(r["nb_courses"]) if r["nb_courses"] is not None else None for r in runners]
        gains_list = [r["gains"] for r in runners]
        cotes = [r["cote"] for r in runners]
        poids_list = [r["poids"] for r in runners]
        speeds = [r["speed"] for r in runners]
        class_ratings = [r["class_rating"] for r in runners]
        serie_places = [r["serie_places"] for r in runners]
        nb_victoires_list = [float(r["nb_victoires"]) if r["nb_victoires"] is not None else None for r in runners]

        # --- Ranks ---
        # age: ascending (1=youngest)
        age_ranks = _rank_ascending(ages)
        # experience: descending (1=most experienced)
        exp_ranks = _rank_descending(nb_courses_list)
        # gains: descending (1=richest)
        gains_ranks = _rank_descending(gains_list)
        # cote: ascending (1=favourite = lowest odds)
        cote_ranks = _rank_ascending(cotes)
        # weight: ascending (1=lightest)
        weight_ranks = _rank_ascending(poids_list)
        # speed: descending (1=fastest)
        speed_ranks = _rank_descending(speeds)
        # class: descending (1=best)
        class_ranks = _rank_descending(class_ratings)
        # form (serie_places): descending (1=best form = highest score)
        form_ranks = _rank_descending(serie_places)
        # nb_victoires: descending (1=most wins)
        wins_ranks = _rank_descending(nb_victoires_list)

        # --- Z-scores for age and experience ---
        ages_clean = [v for v in ages if v is not None]
        age_mean = _mean(ages_clean)
        age_std = _stdev(ages_clean)

        exp_clean = [v for v in nb_courses_list if v is not None]
        exp_mean = _mean(exp_clean)
        exp_std = _stdev(exp_clean)

        # --- Gains percentile ---
        gains_sorted = sorted([v for v in gains_list if v is not None])

        # --- Check if any weight is present (galop detection) ---
        has_weight = any(r["poids"] is not None for r in runners)

        # --- Build per-runner features ---
        runner_map: dict[int, dict[str, Any]] = {}

        for i, r in enumerate(runners):
            num = r["num_pmu"]

            feat: dict[str, Any] = {
                "fld_age_rank": _normalise_rank(age_ranks[i], field_size),
                "fld_age_zscore": _zscore(ages[i], age_mean, age_std),
                "fld_experience_rank": _normalise_rank(exp_ranks[i], field_size),
                "fld_experience_zscore": _zscore(nb_courses_list[i], exp_mean, exp_std),
                "fld_gains_rank": _normalise_rank(gains_ranks[i], field_size),
                "fld_gains_percentile": _percentile(gains_list[i], gains_sorted),
                "fld_cote_rank": _normalise_rank(cote_ranks[i], field_size),
                "fld_weight_rank": _normalise_rank(weight_ranks[i], field_size) if has_weight else None,
                "fld_speed_rank": _normalise_rank(speed_ranks[i], field_size),
                "fld_class_rank": _normalise_rank(class_ranks[i], field_size),
                "fld_form_rank": _normalise_rank(form_ranks[i], field_size),
                "fld_nb_wins_rank": _normalise_rank(wins_ranks[i], field_size),
            }

            runner_map[num] = feat

        course_features[course_uid] = runner_map

    elapsed = time.time() - t0
    logger.info(
        "Rangs calcules: %d courses en %.1fs",
        n_courses, elapsed,
    )

    return course_features


# ===========================================================================
# PASS 2: EMIT FEATURES
# ===========================================================================


def _pass2_emit(
    input_path: Path,
    course_features: dict[str, dict[int, dict[str, Any]]],
    logger: logging.Logger,
) -> list[dict[str, Any]]:
    """
    Stream partants_master again, look up precomputed features by
    course_uid + num_pmu, emit one record per partant.
    """
    logger.info("=== Pass 2: Emission des features ===")
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0
    n_matched = 0
    n_missed = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 2 - lu %d records, emis %d...", n_read, n_matched)

        course_uid = rec.get("course_uid", "")
        num_pmu = rec.get("num_pmu")
        partant_uid = rec.get("partant_uid")
        date_iso = rec.get("date_reunion_iso", "")

        if not course_uid or not partant_uid:
            continue

        runner_map = course_features.get(course_uid)
        if runner_map is None:
            n_missed += 1
            continue

        feat = runner_map.get(num_pmu)
        if feat is None:
            n_missed += 1
            continue

        out: dict[str, Any] = {
            "partant_uid": partant_uid,
            "course_uid": course_uid,
            "date_reunion_iso": date_iso,
        }
        out.update(feat)
        results.append(out)
        n_matched += 1

    elapsed = time.time() - t0
    logger.info(
        "Pass 2 terminee: %d records lus, %d features emises, %d non-matchees en %.1fs",
        n_read, n_matched, n_missed, elapsed,
    )

    return results


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_field_relative_features(
    input_path: Path,
    logger: logging.Logger,
) -> list[dict[str, Any]]:
    """Orchestrate Pass 1 + rank computation + Pass 2."""

    logger.info("=" * 70)
    logger.info("field_relative_builder.py - Features relatives intra-course")
    logger.info("=" * 70)
    logger.info("Input: %s", input_path)

    t_global = time.time()

    # --- Pass 1: collect ---
    courses = _pass1_collect(input_path, logger)
    gc.collect()

    # --- Compute ranks ---
    course_features = _compute_course_ranks(courses, logger)

    # Free pass 1 raw data
    del courses
    gc.collect()

    # --- Pass 2: emit ---
    results = _pass2_emit(input_path, course_features, logger)

    # Free rank data
    del course_features
    gc.collect()

    elapsed = time.time() - t_global
    logger.info(
        "Build termine: %d features en %.1fs",
        len(results), elapsed,
    )

    return results


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Features relatives intra-course (rangs normalises + z-scores)"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(INPUT_PATH),
        help="Chemin vers partants_master.jsonl",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("field_relative_builder")

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_field_relative_features(input_path, logger)

    # --- Save ---
    out_path = output_dir / "field_relative_features.jsonl"
    save_jsonl(results, out_path, logger)

    # --- Fill rates ---
    if results:
        feature_keys = [
            k for k in results[0]
            if k not in ("partant_uid", "course_uid", "date_reunion_iso")
        ]
        total = len(results)
        logger.info("=== Fill rates (%d features) ===", len(feature_keys))
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info("  %s: %d/%d (%.1f%%)", k, filled, total, 100 * filled / total)

    logger.info("Termine - %d partants traites", len(results))


if __name__ == "__main__":
    main()
