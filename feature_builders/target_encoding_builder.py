#!/usr/bin/env python3
"""
feature_builders.target_encoding_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Target encoding features with Bayesian smoothing.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes smoothed win-rate encodings per category.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the target encoding -- no future leakage.

Bayesian smoothing formula:
    smoothed_rate = (n_wins + prior * weight) / (n_races + weight)

    where prior = global_mean_win_rate (computed incrementally),
          weight = 20 (regularization strength).

Produces:
  - target_encoding.jsonl   in output/target_encoding/

Features per partant:
  - te_hippodrome_win_rate    : win rate historique par hippodrome (smoothed)
  - te_jockey_win_rate        : win rate par jockey (smoothed)
  - te_trainer_win_rate       : win rate par entraineur (smoothed)
  - te_discipline_win_rate    : win rate par discipline (smoothed)
  - te_distance_cat_win_rate  : win rate par categorie distance (smoothed)
  - te_month_win_rate         : win rate par mois (smoothed)

Usage:
    python feature_builders/target_encoding_builder.py
    python feature_builders/target_encoding_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "target_encoding"

# Bayesian smoothing weight (regularization strength)
SMOOTHING_WEIGHT = 20

# Distance categories (meters)
_DISTANCE_BINS = [
    (0, 1200, "sprint"),
    (1200, 1600, "mile"),
    (1600, 2100, "inter"),
    (2100, 2800, "classique"),
    (2800, 99999, "long"),
]

# Progress log every N records
_LOG_EVERY = 500_000


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


def _distance_category(distance_m) -> Optional[str]:
    """Map a distance in meters to a category string."""
    if distance_m is None:
        return None
    try:
        d = float(distance_m)
    except (ValueError, TypeError):
        return None
    for lo, hi, label in _DISTANCE_BINS:
        if lo <= d < hi:
            return label
    return None


def _extract_month(date_str: str) -> Optional[str]:
    """Extract month as 'MM' from ISO date string."""
    if not date_str or len(date_str) < 7:
        return None
    try:
        return date_str[5:7]
    except (IndexError, TypeError):
        return None


# ===========================================================================
# BAYESIAN SMOOTHING TRACKER
# ===========================================================================


class _WinTracker:
    """Tracks win counts and total races for a category."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def smoothed_rate(self, prior: float, weight: int) -> float:
        """Bayesian smoothed win rate."""
        return (self.wins + prior * weight) / (self.total + weight)


class _GlobalTracker:
    """Tracks global win rate incrementally."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    @property
    def mean(self) -> float:
        """Current global mean win rate."""
        if self.total == 0:
            return 0.1  # reasonable prior before any data
        return self.wins / self.total


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_target_encoding_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build target encoding features from partants_master.jsonl.

    Single-pass approach: read all records with minimal fields,
    sort chronologically, then process course-by-course.
    For each course, snapshot the current smoothed rates BEFORE updating
    with that course's results.
    """
    logger.info("=== Target Encoding Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Extract distance
        distance = rec.get("distance_m") or rec.get("distance")
        dist_cat = _distance_category(distance)

        # Extract hippodrome
        hippodrome = rec.get("hippodrome") or rec.get("nom_hippodrome")

        # Extract discipline
        discipline = rec.get("discipline") or rec.get("type_course")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "gagnant": bool(rec.get("is_gagnant")),
            "hippodrome": hippodrome,
            "jockey": rec.get("jockey_driver"),
            "entraineur": rec.get("entraineur"),
            "discipline": discipline,
            "dist_cat": dist_cat,
            "month": _extract_month(rec.get("date_reunion_iso", "")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()

    global_tracker = _GlobalTracker()
    hippo_trackers: dict[str, _WinTracker] = defaultdict(_WinTracker)
    jockey_trackers: dict[str, _WinTracker] = defaultdict(_WinTracker)
    trainer_trackers: dict[str, _WinTracker] = defaultdict(_WinTracker)
    discipline_trackers: dict[str, _WinTracker] = defaultdict(_WinTracker)
    dist_cat_trackers: dict[str, _WinTracker] = defaultdict(_WinTracker)
    month_trackers: dict[str, _WinTracker] = defaultdict(_WinTracker)

    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants for this course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (i < total
               and slim_records[i]["course"] == course_uid
               and slim_records[i]["date"] == course_date):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # Current global prior
        prior = global_tracker.mean
        w = SMOOTHING_WEIGHT

        # -- Emit features for each partant (pre-race snapshot) --
        for rec in course_group:
            hippo = rec["hippodrome"]
            jockey = rec["jockey"]
            trainer = rec["entraineur"]
            disc = rec["discipline"]
            dist_cat = rec["dist_cat"]
            month = rec["month"]

            te_hippo = None
            if hippo and hippo_trackers[hippo].total > 0:
                te_hippo = round(hippo_trackers[hippo].smoothed_rate(prior, w), 6)

            te_jockey = None
            if jockey and jockey_trackers[jockey].total > 0:
                te_jockey = round(jockey_trackers[jockey].smoothed_rate(prior, w), 6)

            te_trainer = None
            if trainer and trainer_trackers[trainer].total > 0:
                te_trainer = round(trainer_trackers[trainer].smoothed_rate(prior, w), 6)

            te_disc = None
            if disc and discipline_trackers[disc].total > 0:
                te_disc = round(discipline_trackers[disc].smoothed_rate(prior, w), 6)

            te_dist = None
            if dist_cat and dist_cat_trackers[dist_cat].total > 0:
                te_dist = round(dist_cat_trackers[dist_cat].smoothed_rate(prior, w), 6)

            te_month = None
            if month and month_trackers[month].total > 0:
                te_month = round(month_trackers[month].smoothed_rate(prior, w), 6)

            results.append({
                "partant_uid": rec["uid"],
                "te_hippodrome_win_rate": te_hippo,
                "te_jockey_win_rate": te_jockey,
                "te_trainer_win_rate": te_trainer,
                "te_discipline_win_rate": te_disc,
                "te_distance_cat_win_rate": te_dist,
                "te_month_win_rate": te_month,
            })

        # -- Update trackers after emitting features (no leakage) --
        for rec in course_group:
            is_win = rec["gagnant"]
            win_int = 1 if is_win else 0

            global_tracker.total += 1
            global_tracker.wins += win_int

            hippo = rec["hippodrome"]
            if hippo:
                hippo_trackers[hippo].total += 1
                hippo_trackers[hippo].wins += win_int

            jockey = rec["jockey"]
            if jockey:
                jockey_trackers[jockey].total += 1
                jockey_trackers[jockey].wins += win_int

            trainer = rec["entraineur"]
            if trainer:
                trainer_trackers[trainer].total += 1
                trainer_trackers[trainer].wins += win_int

            disc = rec["discipline"]
            if disc:
                discipline_trackers[disc].total += 1
                discipline_trackers[disc].wins += win_int

            dist_cat = rec["dist_cat"]
            if dist_cat:
                dist_cat_trackers[dist_cat].total += 1
                dist_cat_trackers[dist_cat].wins += win_int

            month = rec["month"]
            if month:
                month_trackers[month].total += 1
                month_trackers[month].wins += win_int

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Target encoding build termine: %d features en %.1fs",
        len(results), elapsed,
    )
    logger.info(
        "  Entites: hippodromes=%d, jockeys=%d, entraineurs=%d, "
        "disciplines=%d, dist_cats=%d, months=%d",
        len(hippo_trackers), len(jockey_trackers), len(trainer_trackers),
        len(discipline_trackers), len(dist_cat_trackers), len(month_trackers),
    )
    logger.info(
        "  Global win rate: %.4f (%d/%d)",
        global_tracker.mean, global_tracker.wins, global_tracker.total,
    )

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des target encoding features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/target_encoding/)",
    )
    args = parser.parse_args()

    logger = setup_logging("target_encoding_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_target_encoding_features(input_path, logger)

    # Save
    out_path = output_dir / "target_encoding.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
