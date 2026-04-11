#!/usr/bin/env python3
"""
feature_builders.race_class_history_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse performance across different race class levels.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant race class history features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Class levels are bucketed from the allocation field:
  1 = <5000         (lowest)
  2 = 5000-15000
  3 = 15000-40000
  4 = 40000-100000
  5 = >100000       (highest)

Produces:
  - race_class_history.jsonl   in output/race_class_history/

Features per partant:
  - rch_current_class       : class level of current race (1-5)
  - rch_horse_avg_class     : average class level the horse has raced at
  - rch_class_up            : 1 if current class > horse's average class (stepping up)
  - rch_class_down          : 1 if current class < horse's average class (dropping down)
  - rch_win_rate_at_class   : horse's win rate at this specific class level
  - rch_best_class_won      : highest class level where horse has won
  - rch_class_consistency   : std deviation of horse's class levels (low = consistent)
  - rch_class_trajectory    : slope of class levels over last 10 races (positive = moving up)

Usage:
    python feature_builders/race_class_history_builder.py
    python feature_builders/race_class_history_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_class_history")

_LOG_EVERY = 500_000

# Allocation thresholds for class bucketing
_CLASS_THRESHOLDS = [
    (5_000,   1),   # <5000 -> class 1
    (15_000,  2),   # 5000-15000 -> class 2
    (40_000,  3),   # 15000-40000 -> class 3
    (100_000, 4),   # 40000-100000 -> class 4
]
# >100000 -> class 5

# Window sizes
_CLASS_HISTORY_MAXLEN = 20   # for average and std
_TRAJECTORY_WINDOW = 10      # for slope calculation

# Minimum runs at a class level to compute win rate
_MIN_RUNS_AT_CLASS = 1


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
        return v if v == v else None  # exclude NaN
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _allocation_to_class(allocation: Optional[float]) -> Optional[int]:
    """Convert allocation value to class level 1-5."""
    if allocation is None or allocation < 0:
        return None
    for threshold, level in _CLASS_THRESHOLDS:
        if allocation < threshold:
            return level
    return 5


def _compute_slope(values: list[float]) -> Optional[float]:
    """Compute OLS slope of values over their index positions."""
    n = len(values)
    if n < 2:
        return None
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 6)


def _compute_std(values: list[float]) -> Optional[float]:
    """Population standard deviation."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return round(math.sqrt(variance), 4)


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseClassState:
    """Per-horse accumulated state for race class history."""

    __slots__ = ("classes", "per_class_wins", "per_class_total", "best_class_won")

    def __init__(self) -> None:
        # Sliding window of last 20 class levels raced at
        self.classes: deque = deque(maxlen=_CLASS_HISTORY_MAXLEN)
        # Per-class win/total counts
        self.per_class_wins: dict[int, int] = defaultdict(int)
        self.per_class_total: dict[int, int] = defaultdict(int)
        # Highest class level where this horse has won (0 = never won)
        self.best_class_won: int = 0

    def snapshot(self, current_class: Optional[int]) -> dict[str, Any]:
        """Compute features using only past races (strict temporal).

        current_class is the class level of the race being snapshotted,
        which has NOT yet been added to self.classes.
        """
        # rch_current_class
        rch_current_class = current_class

        # Need at least one past race for history-based features
        past_classes = list(self.classes)
        n_past = len(past_classes)

        # rch_horse_avg_class
        rch_horse_avg_class: Optional[float]
        if n_past > 0:
            rch_horse_avg_class = round(sum(past_classes) / n_past, 4)
        else:
            rch_horse_avg_class = None

        # rch_class_up / rch_class_down
        rch_class_up: Optional[int] = None
        rch_class_down: Optional[int] = None
        if current_class is not None and rch_horse_avg_class is not None:
            if current_class > rch_horse_avg_class:
                rch_class_up = 1
                rch_class_down = 0
            elif current_class < rch_horse_avg_class:
                rch_class_up = 0
                rch_class_down = 1
            else:
                rch_class_up = 0
                rch_class_down = 0

        # rch_win_rate_at_class
        rch_win_rate_at_class: Optional[float] = None
        if current_class is not None:
            total_at_class = self.per_class_total.get(current_class, 0)
            if total_at_class >= _MIN_RUNS_AT_CLASS:
                wins_at_class = self.per_class_wins.get(current_class, 0)
                rch_win_rate_at_class = round(wins_at_class / total_at_class, 4)

        # rch_best_class_won
        rch_best_class_won: Optional[int] = self.best_class_won if self.best_class_won > 0 else None

        # rch_class_consistency (std of past class levels)
        rch_class_consistency: Optional[float] = None
        if n_past >= 2:
            rch_class_consistency = _compute_std(past_classes)

        # rch_class_trajectory (slope over last 10 classes)
        rch_class_trajectory: Optional[float] = None
        trajectory_window = past_classes[-_TRAJECTORY_WINDOW:]
        if len(trajectory_window) >= 2:
            rch_class_trajectory = _compute_slope(trajectory_window)

        return {
            "rch_current_class": rch_current_class,
            "rch_horse_avg_class": rch_horse_avg_class,
            "rch_class_up": rch_class_up,
            "rch_class_down": rch_class_down,
            "rch_win_rate_at_class": rch_win_rate_at_class,
            "rch_best_class_won": rch_best_class_won,
            "rch_class_consistency": rch_class_consistency,
            "rch_class_trajectory": rch_class_trajectory,
        }

    def update(self, class_level: Optional[int], is_winner: bool) -> None:
        """Update state with a completed race result (post-race, no leakage)."""
        if class_level is None:
            return
        self.classes.append(class_level)
        self.per_class_total[class_level] += 1
        if is_winner:
            self.per_class_wins[class_level] += 1
            if class_level > self.best_class_won:
                self.best_class_won = class_level


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_race_class_history_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build race class history features from partants_master.jsonl."""
    logger.info("=== Race Class History Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        allocation = _safe_float(rec.get("allocation"))
        class_level = _allocation_to_class(allocation)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("horse_id") or rec.get("nom_cheval"),
            "class_level": class_level,
            "is_winner": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically (index + sort + seek pattern) --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process race by race, snapshot before update --
    t2 = time.time()
    horse_states: dict[str, _HorseClassState] = defaultdict(_HorseClassState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        # Group all partants belonging to the same course
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        # -- Snapshot pre-race features (BEFORE update) --
        for rec in course_group:
            cheval = rec["cheval"]
            class_level = rec["class_level"]

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "rch_current_class": class_level,
                "rch_horse_avg_class": None,
                "rch_class_up": None,
                "rch_class_down": None,
                "rch_win_rate_at_class": None,
                "rch_best_class_won": None,
                "rch_class_consistency": None,
                "rch_class_trajectory": None,
            }

            if cheval:
                state = horse_states[cheval]
                snap = state.snapshot(class_level)
                features.update(snap)

            results.append(features)

        # -- Update states after snapshotting (post-race, strict temporal) --
        for rec in course_group:
            cheval = rec["cheval"]
            if cheval:
                horse_states[cheval].update(rec["class_level"], rec["is_winner"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Race class history build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results), elapsed, len(horse_states),
    )

    del slim_records
    gc.collect()

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
        description="Construction des features race class history a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/.../race_class_history/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_class_history_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_race_class_history_features(input_path, logger)

    # Save
    out_path = output_dir / "race_class_history.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
