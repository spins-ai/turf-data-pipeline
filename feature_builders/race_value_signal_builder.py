#!/usr/bin/env python3
"""
feature_builders.race_value_signal_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race value / prize signal features -- detecting race importance and
value opportunities.

Reads partants_master.jsonl in two streaming passes:
  Pass 1: aggregate per-course engagement/gains data.
  Pass 2: stream again and compute per-partant features.

Temporal integrity: all features are derived from pre-race data
(engagement, odds, career stats known before the race), no future leakage.

Produces:
  - race_value_signal.jsonl  in builder_outputs/race_value_signal/

Features per partant (8):
  - rvs_race_total_engagement   : sum of engagement across all runners in the race
  - rvs_race_avg_engagement     : average engagement per runner
  - rvs_horse_engagement_rank   : rank of horse's engagement within the race (1=highest)
  - rvs_value_score             : cote_finale * (nb_victoires / max(nb_courses,1))
  - rvs_overbet_score           : (1/cote) / (nb_victoires/max(nb_courses,1) + 0.01)
  - rvs_roi_if_win              : cote_finale - 1  (net profit per unit if win)
  - rvs_edge_estimate           : nb_victoires/max(nb_courses,1) - 1/cote_finale
  - rvs_field_total_gains       : sum of gains_carriere of all runners (race wealth)

Usage:
    python feature_builders/race_value_signal_builder.py
    python feature_builders/race_value_signal_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_value_signal")

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
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PASS 1 -- aggregate per-course
# ===========================================================================


def _pass1_aggregate(input_path: Path, logger) -> dict[str, dict[str, Any]]:
    """
    Build course_uid -> {
        engagements: list[float],
        gains: list[float],
        num_pmu_to_engagement: dict[int, float],
    }
    """
    logger.info("=== Pass 1: aggregation par course ===")
    t0 = time.time()

    course_data: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"engagements": [], "gains": [], "num_pmu_to_engagement": {}}
    )

    n_read = 0
    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 - lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid", "")
        if not course_uid:
            continue

        engagement = _safe_float(rec.get("engagement"))
        gains = _safe_float(rec.get("gains_carriere_euros"))
        num_pmu = _safe_int(rec.get("num_pmu"))

        cd = course_data[course_uid]
        if engagement is not None:
            cd["engagements"].append(engagement)
            if num_pmu is not None:
                cd["num_pmu_to_engagement"][num_pmu] = engagement
        if gains is not None:
            cd["gains"].append(gains)

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        n_read, len(course_data), time.time() - t0,
    )
    return dict(course_data)


# ===========================================================================
# PASS 2 -- per-partant features
# ===========================================================================


def _pass2_compute(
    input_path: Path,
    course_agg: dict[str, dict[str, Any]],
    output_path: Path,
    logger,
) -> int:
    """Stream partants again, compute features, write to JSONL."""
    logger.info("=== Pass 2: calcul features par partant ===")
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(".tmp")

    n_written = 0
    fill_counts: dict[str, int] = {
        "rvs_race_total_engagement": 0,
        "rvs_race_avg_engagement": 0,
        "rvs_horse_engagement_rank": 0,
        "rvs_value_score": 0,
        "rvs_overbet_score": 0,
        "rvs_roi_if_win": 0,
        "rvs_edge_estimate": 0,
        "rvs_field_total_gains": 0,
    }

    with open(tmp_path, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_written += 1
            if n_written % _LOG_EVERY == 0:
                logger.info("  Pass 2 - traite %d records...", n_written)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid", "")
            num_pmu = _safe_int(rec.get("num_pmu"))
            cote = _safe_float(rec.get("cote_finale"))
            nb_victoires = _safe_int(rec.get("nb_victoires_carriere"))
            nb_courses = _safe_int(rec.get("nb_courses_carriere"))

            agg = course_agg.get(course_uid)

            # -- Race-level engagement features --
            race_total_eng: Optional[float] = None
            race_avg_eng: Optional[float] = None
            horse_eng_rank: Optional[int] = None
            field_total_gains: Optional[float] = None

            if agg is not None:
                engagements = agg["engagements"]
                gains_list = agg["gains"]

                if engagements:
                    race_total_eng = round(sum(engagements), 2)
                    race_avg_eng = round(race_total_eng / len(engagements), 2)

                    # Rank: sort descending, find horse's position
                    if num_pmu is not None and num_pmu in agg["num_pmu_to_engagement"]:
                        horse_eng = agg["num_pmu_to_engagement"][num_pmu]
                        # Count how many have strictly higher engagement
                        rank = 1 + sum(1 for e in engagements if e > horse_eng)
                        horse_eng_rank = rank

                if gains_list:
                    field_total_gains = round(sum(gains_list), 2)

            # -- Per-partant value features --
            value_score: Optional[float] = None
            overbet_score: Optional[float] = None
            roi_if_win: Optional[float] = None
            edge_estimate: Optional[float] = None

            win_rate: Optional[float] = None
            if nb_victoires is not None and nb_courses is not None:
                win_rate = nb_victoires / max(nb_courses, 1)

            if cote is not None and cote > 0:
                roi_if_win = round(cote - 1.0, 4)

                if win_rate is not None:
                    value_score = round(cote * win_rate, 4)

                    implied_prob = 1.0 / cote
                    overbet_score = round(implied_prob / (win_rate + 0.01), 4)
                    edge_estimate = round(win_rate - implied_prob, 4)

            # -- Assemble output --
            features: dict[str, Any] = {
                "partant_uid": partant_uid,
                "rvs_race_total_engagement": race_total_eng,
                "rvs_race_avg_engagement": race_avg_eng,
                "rvs_horse_engagement_rank": horse_eng_rank,
                "rvs_value_score": value_score,
                "rvs_overbet_score": overbet_score,
                "rvs_roi_if_win": roi_if_win,
                "rvs_edge_estimate": edge_estimate,
                "rvs_field_total_gains": field_total_gains,
            }

            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")

            # Track fill rates
            for k in fill_counts:
                if features.get(k) is not None:
                    fill_counts[k] += 1

    # Atomic rename
    tmp_path.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Pass 2 terminee: %d features ecrites en %.1fs -> %s",
        n_written, elapsed, output_path,
    )

    # Fill rates
    if n_written > 0:
        logger.info("=== Fill rates ===")
        for k, v in fill_counts.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features race value signal a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    args = parser.parse_args()

    logger = setup_logging("race_value_signal_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "race_value_signal.jsonl"

    logger.info("=== Race Value Signal Builder ===")
    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", output_path)
    t_global = time.time()

    # Pass 1: aggregate
    course_agg = _pass1_aggregate(input_path, logger)
    gc.collect()

    # Pass 2: compute & write
    n = _pass2_compute(input_path, course_agg, output_path, logger)

    # Free memory
    del course_agg
    gc.collect()

    logger.info(
        "=== Race Value Signal Builder termine: %d records en %.1fs ===",
        n, time.time() - t_global,
    )


if __name__ == "__main__":
    main()
