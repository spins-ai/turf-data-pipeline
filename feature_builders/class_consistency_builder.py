#!/usr/bin/env python3
"""
feature_builders.class_consistency_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Evaluate how consistently a horse performs at different class levels.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant class-consistency features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - class_consistency.jsonl   in output/class_consistency/

Features per partant:
  - class_level                : current race class level (1-6)
  - class_win_rate_at_level    : horse win rate at this specific class level
  - class_avg_position_at_level: average finish position at this class level
  - class_drop                 : 1 if running at lower class than avg career class
  - class_rise                 : 1 if running at higher class than avg career class
  - class_consistency_score    : CV of finish positions across all classes

Usage:
    python feature_builders/class_consistency_builder.py
    python feature_builders/class_consistency_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
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
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/class_consistency")

# Class level buckets based on allocation_totale
_CLASS_THRESHOLDS = [
    (5_000, 1),
    (15_000, 2),
    (30_000, 3),
    (60_000, 4),
    (150_000, 5),
]
_CLASS_TOP = 6

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# HELPERS
# ===========================================================================


def _allocation_to_class(allocation: Any) -> Optional[int]:
    """Convert allocation_totale to class level 1-6."""
    if allocation is None:
        return None
    try:
        val = float(allocation)
    except (TypeError, ValueError):
        return None
    for threshold, level in _CLASS_THRESHOLDS:
        if val < threshold:
            return level
    return _CLASS_TOP


class _HorseClassState:
    """Per-horse accumulator for class-level statistics.

    Tracks per-class-level: wins, races, and finish positions.
    Also tracks global finish positions across all classes for CV computation.
    """

    __slots__ = ("per_class_wins", "per_class_races", "per_class_positions",
                 "all_positions", "total_class_sum", "total_races")

    def __init__(self) -> None:
        # per class level -> counts
        self.per_class_wins: dict[int, int] = defaultdict(int)
        self.per_class_races: dict[int, int] = defaultdict(int)
        self.per_class_positions: dict[int, list[float]] = defaultdict(list)
        # across all classes
        self.all_positions: list[float] = []
        self.total_class_sum: float = 0.0
        self.total_races: int = 0

    @property
    def avg_career_class(self) -> Optional[float]:
        if self.total_races == 0:
            return None
        return self.total_class_sum / self.total_races

    def win_rate_at(self, level: int) -> Optional[float]:
        n = self.per_class_races.get(level, 0)
        if n == 0:
            return None
        return self.per_class_wins.get(level, 0) / n

    def avg_position_at(self, level: int) -> Optional[float]:
        positions = self.per_class_positions.get(level)
        if not positions:
            return None
        return sum(positions) / len(positions)

    def consistency_score(self) -> Optional[float]:
        """Coefficient of variation of finish positions across all classes."""
        if len(self.all_positions) < 2:
            return None
        mean = sum(self.all_positions) / len(self.all_positions)
        if mean == 0:
            return None
        variance = sum((p - mean) ** 2 for p in self.all_positions) / len(self.all_positions)
        std = math.sqrt(variance)
        return std / mean

    def update(self, class_level: int, position: Optional[float], is_winner: bool) -> None:
        """Record a completed race result (called AFTER feature emission)."""
        self.per_class_races[class_level] += 1
        if is_winner:
            self.per_class_wins[class_level] += 1
        if position is not None:
            self.per_class_positions[class_level].append(position)
            self.all_positions.append(position)
        self.total_class_sum += class_level
        self.total_races += 1


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


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_class_consistency_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build class consistency features from partants_master.jsonl.

    Single-pass approach: read minimal fields into memory, sort
    chronologically, then process race-by-race with strict temporal
    integrity (features use only prior race results).
    """
    logger.info("=== Class Consistency Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        allocation = rec.get("allocation_totale")
        class_level = _allocation_to_class(allocation)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": rec.get("position_arrivee"),
            "class_level": class_level,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_state: dict[str, _HorseClassState] = defaultdict(_HorseClassState)
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

        # -- Emit features (pre-race snapshot: no leakage) --
        for rec in course_group:
            cheval = rec["cheval"]
            cl = rec["class_level"]

            if cheval is None or cl is None:
                results.append({
                    "partant_uid": rec["uid"],
                    "class_level": cl,
                    "class_win_rate_at_level": None,
                    "class_avg_position_at_level": None,
                    "class_drop": None,
                    "class_rise": None,
                    "class_consistency_score": None,
                })
                continue

            state = horse_state[cheval]
            avg_career = state.avg_career_class

            # Determine drop / rise relative to career average
            if avg_career is not None:
                class_drop = 1 if cl < avg_career else 0
                class_rise = 1 if cl > avg_career else 0
            else:
                class_drop = None
                class_rise = None

            win_rate = state.win_rate_at(cl)
            avg_pos = state.avg_position_at(cl)
            consistency = state.consistency_score()

            results.append({
                "partant_uid": rec["uid"],
                "class_level": cl,
                "class_win_rate_at_level": round(win_rate, 4) if win_rate is not None else None,
                "class_avg_position_at_level": round(avg_pos, 2) if avg_pos is not None else None,
                "class_drop": class_drop,
                "class_rise": class_rise,
                "class_consistency_score": round(consistency, 4) if consistency is not None else None,
            })

        # -- Update state AFTER emitting features (temporal integrity) --
        for rec in course_group:
            cheval = rec["cheval"]
            cl = rec["class_level"]
            if cheval is None or cl is None:
                continue

            position = None
            raw_pos = rec["position"]
            if raw_pos is not None:
                try:
                    position = float(raw_pos)
                except (TypeError, ValueError):
                    pass

            horse_state[cheval].update(cl, position, rec["gagnant"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Class consistency build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_state),
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
        description="Construction des features de consistance de classe a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/class_consistency/)",
    )
    args = parser.parse_args()

    logger = setup_logging("class_consistency_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_class_consistency_features(input_path, logger)

    # Save
    out_path = output_dir / "class_consistency.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_r = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_r, 100 * v / total_r)


if __name__ == "__main__":
    main()
