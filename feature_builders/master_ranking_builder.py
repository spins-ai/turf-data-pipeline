#!/usr/bin/env python3
"""
feature_builders.master_ranking_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Master ranking features -- comprehensive rankings that combine multiple
signals (odds, gains, experience, win rate, weight, age) to produce
composite and surprise indicators.

Reads partants_master.jsonl in streaming mode, groups by course,
and computes per-partant ranking features using a two-pass architecture.

Temporal integrity: all features are derived from pre-race data
(known before the race starts), no future leakage.

Produces:
  - master_ranking_features.jsonl

Features per partant (10):
  - mrk_odds_rank             : rank by cote_finale (1 = favorite)
  - mrk_gains_rank            : rank by gains_carriere (1 = richest)
  - mrk_experience_rank       : rank by nb_courses_carriere (1 = most experienced)
  - mrk_win_rate_rank         : rank by win rate (1 = highest)
  - mrk_weight_rank           : rank by poids_porte_kg (1 = lightest)
  - mrk_age_rank              : rank by age (1 = youngest)
  - mrk_composite_rank        : average of odds + gains + wr ranks (lower = better)
  - mrk_rank_consistency      : std of all 6 ranks (lower = consistent)
  - mrk_is_multi_dimension_top3 : 1 if horse is top 3 in >= 3 ranking dimensions
  - mrk_rank_vs_odds_surprise : composite_rank - odds_rank
                                (positive = market rates horse better than fundamentals)

Usage:
    python feature_builders/master_ranking_builder.py
    python feature_builders/master_ranking_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/master_ranking")
OUTPUT_FILENAME = "master_ranking_features.jsonl"

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


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


def _rank_ascending(values: list[Optional[float]]) -> list[Optional[int]]:
    """Dense rank, ascending: smallest value gets rank 1.

    None values receive rank None.
    """
    indexed = [(i, v) for i, v in enumerate(values) if v is not None]
    indexed.sort(key=lambda x: x[1])
    ranks: list[Optional[int]] = [None] * len(values)
    for rank, (orig_idx, _) in enumerate(indexed, start=1):
        ranks[orig_idx] = rank
    return ranks


def _rank_descending(values: list[Optional[float]]) -> list[Optional[int]]:
    """Dense rank, descending: largest value gets rank 1.

    None values receive rank None.
    """
    indexed = [(i, v) for i, v in enumerate(values) if v is not None]
    indexed.sort(key=lambda x: x[1], reverse=True)
    ranks: list[Optional[int]] = [None] * len(values)
    for rank, (orig_idx, _) in enumerate(indexed, start=1):
        ranks[orig_idx] = rank
    return ranks


# ===========================================================================
# MAIN BUILD (two-pass + streaming output)
# ===========================================================================


def build_master_ranking_features(input_path: Path, output_path: Path, logger) -> int:
    """Build master ranking features from partants_master.jsonl.

    Architecture:
      Pass 1: stream through JSONL, group by course, collect per-partant
              raw values {num_pmu -> {cote, gains, courses, wins, weight, age}}.
      Pass 2: for each course compute ranks, composite, consistency,
              multi-dimension top-3, and surprise; write output in streaming.

    Returns the total number of feature records written.
    """
    logger.info("=== Master Ranking Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # -- Pass 1: per-course aggregation --
    # course_key -> list of {uid, num, cote, gains, courses, wins, weight, age}
    courses: dict[str, list[dict]] = {}
    n_read = 0
    n_json_errors = 0

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line_s = line.strip()
            if not line_s:
                continue
            try:
                rec = json.loads(line_s)
            except json.JSONDecodeError:
                n_json_errors += 1
                if n_json_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_json_errors)
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Pass 1: lu %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            course_key = f"{date_str}|{course_uid}"

            nb_courses = _safe_int(rec.get("nb_courses_carriere"))
            nb_victoires = _safe_int(rec.get("nb_victoires_carriere")) or _safe_int(rec.get("nb_victoires"))
            win_rate: Optional[float] = None
            if nb_courses is not None and nb_courses > 0 and nb_victoires is not None:
                win_rate = nb_victoires / nb_courses

            entry = {
                "uid": rec.get("partant_uid"),
                "num": _safe_int(rec.get("num_pmu")) or 0,
                "cote": _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference")),
                "gains": _safe_float(rec.get("gains_carriere_euros")) or _safe_float(rec.get("gains_carriere")),
                "courses": nb_courses,
                "wins": nb_victoires,
                "win_rate": win_rate,
                "weight": _safe_float(rec.get("poids_porte_kg")) or _safe_float(rec.get("poidsConditionMonteEnKg")),
                "age": _safe_int(rec.get("age")),
            }

            if course_key not in courses:
                courses[course_key] = []
            courses[course_key].append(entry)

    logger.info(
        "Pass 1 done: %d records, %d courses, %d JSON errors in %.1fs",
        n_read, len(courses), n_json_errors, time.time() - t0,
    )

    # -- Pass 2: compute ranks per course, stream output --
    t1 = time.time()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    fill_counts: dict[str, int] = {
        "mrk_odds_rank": 0,
        "mrk_gains_rank": 0,
        "mrk_experience_rank": 0,
        "mrk_win_rate_rank": 0,
        "mrk_weight_rank": 0,
        "mrk_age_rank": 0,
        "mrk_composite_rank": 0,
        "mrk_rank_consistency": 0,
        "mrk_is_multi_dimension_top3": 0,
        "mrk_rank_vs_odds_surprise": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for course_key, runners in courses.items():
            n_runners = len(runners)
            if n_runners == 0:
                continue

            # Extract raw values for ranking
            cotes = [r["cote"] for r in runners]
            gains = [r["gains"] for r in runners]
            experiences = [float(r["courses"]) if r["courses"] is not None else None for r in runners]
            win_rates = [r["win_rate"] for r in runners]
            weights = [r["weight"] for r in runners]
            ages = [float(r["age"]) if r["age"] is not None else None for r in runners]

            # Compute ranks
            # odds_rank: ascending (lowest cote = favorite = rank 1)
            odds_ranks = _rank_ascending(cotes)
            # gains_rank: descending (highest gains = rank 1)
            gains_ranks = _rank_descending(gains)
            # experience_rank: descending (most races = rank 1)
            experience_ranks = _rank_descending(experiences)
            # win_rate_rank: descending (highest wr = rank 1)
            wr_ranks = _rank_descending(win_rates)
            # weight_rank: ascending (lightest = rank 1)
            weight_ranks = _rank_ascending(weights)
            # age_rank: ascending (youngest = rank 1)
            age_ranks = _rank_ascending(ages)

            for idx in range(n_runners):
                runner = runners[idx]

                feat: dict[str, Any] = {"partant_uid": runner["uid"]}

                or_val = odds_ranks[idx]
                gr_val = gains_ranks[idx]
                er_val = experience_ranks[idx]
                wrr_val = wr_ranks[idx]
                wtr_val = weight_ranks[idx]
                ar_val = age_ranks[idx]

                feat["mrk_odds_rank"] = or_val
                feat["mrk_gains_rank"] = gr_val
                feat["mrk_experience_rank"] = er_val
                feat["mrk_win_rate_rank"] = wrr_val
                feat["mrk_weight_rank"] = wtr_val
                feat["mrk_age_rank"] = ar_val

                # composite_rank: average of odds + gains + win_rate ranks
                composite: Optional[float] = None
                if or_val is not None and gr_val is not None and wrr_val is not None:
                    composite = round((or_val + gr_val + wrr_val) / 3.0, 4)
                feat["mrk_composite_rank"] = composite

                # rank_consistency: std of all 6 ranks
                all_ranks = [or_val, gr_val, er_val, wrr_val, wtr_val, ar_val]
                valid_ranks = [r for r in all_ranks if r is not None]
                consistency: Optional[float] = None
                if len(valid_ranks) >= 2:
                    mean_r = sum(valid_ranks) / len(valid_ranks)
                    var_r = sum((r - mean_r) ** 2 for r in valid_ranks) / len(valid_ranks)
                    consistency = round(math.sqrt(var_r), 4)
                feat["mrk_rank_consistency"] = consistency

                # is_multi_dimension_top3: 1 if top 3 in >= 3 dimensions
                top3_count = sum(1 for r in all_ranks if r is not None and r <= 3)
                feat["mrk_is_multi_dimension_top3"] = 1 if top3_count >= 3 else 0

                # rank_vs_odds_surprise: composite - odds_rank
                surprise: Optional[float] = None
                if composite is not None and or_val is not None:
                    surprise = round(composite - or_val, 4)
                feat["mrk_rank_vs_odds_surprise"] = surprise

                # Update fill counts
                for k in fill_counts:
                    if feat.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(feat, ensure_ascii=False) + "\n")
                n_written += 1

            # Periodic GC
            if n_written % _LOG_EVERY < n_runners:
                logger.info("  Pass 2: written %d records...", n_written)
                gc.collect()

    # Atomic rename
    tmp_out.replace(output_path)

    # Free course data
    del courses
    gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Master ranking build done: %d features in %.1fs",
        n_written, elapsed,
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Input file not found: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Input file not found: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Master ranking features from partants_master.jsonl"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help=f"Path to partants_master.jsonl (default: {INPUT_PARTANTS})",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("master_ranking_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_master_ranking_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
