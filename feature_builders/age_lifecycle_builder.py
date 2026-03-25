#!/usr/bin/env python3
"""
feature_builders.age_lifecycle_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse age lifecycle features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant lifecycle features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - age_lifecycle.jsonl   in output/age_lifecycle/

Features per partant:
  - peak_age_for_discipline     : is horse at peak age for its discipline?
                                  (flat=3-4, jump=6-8, trot=5-7)
  - races_since_peak            : nb races since horse's best position
  - career_phase                : early(0-10 races)/mid(10-30)/veteran(30+)
  - improving_or_declining_phase: based on position trend over last 20 races
                                  (positive = improving, negative = declining)
  - optimal_distance_age        : does horse's current distance match
                                  age-typical distances?

Usage:
    python feature_builders/age_lifecycle_builder.py
    python feature_builders/age_lifecycle_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "age_lifecycle"

_LOG_EVERY = 500_000

# Peak age ranges per discipline
_PEAK_AGES = {
    "plat": (3, 4),
    "galop": (3, 4),        # galop flat alias
    "obstacle": (6, 8),
    "haies": (6, 8),
    "steeple": (6, 8),
    "cross": (6, 8),
    "trot": (5, 7),
    "attele": (5, 7),       # trot attele
    "monte": (5, 7),        # trot monte
}

# Age-typical distance ranges (metres) -- younger horses run shorter
_AGE_DISTANCE = {
    2: (800, 1600),
    3: (1000, 2400),
    4: (1200, 3200),
    5: (1400, 3600),
    6: (1400, 4000),
    7: (1400, 4000),
}


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


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None
    except (ValueError, TypeError):
        return None


def _is_peak_age(age: int, discipline: str) -> Optional[bool]:
    """Return True if the horse is at peak age for its discipline."""
    disc = discipline.strip().lower() if discipline else ""
    peak_range = _PEAK_AGES.get(disc)
    if peak_range is None:
        return None
    return peak_range[0] <= age <= peak_range[1]


def _career_phase(nb_races: int) -> str:
    """Classify career phase based on number of past races."""
    if nb_races < 10:
        return "early"
    if nb_races < 30:
        return "mid"
    return "veteran"


def _position_trend(positions: list[int]) -> Optional[float]:
    """Compute linear trend of positions (last 20).

    Negative slope = improving (positions getting smaller / better).
    Returns slope sign flipped so positive = improving, negative = declining.
    """
    if len(positions) < 3:
        return None
    # Use last 20 at most
    recent = positions[-20:]
    n = len(recent)
    # Simple linear regression: slope of position vs index
    x_mean = (n - 1) / 2.0
    y_mean = sum(recent) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(recent):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if den == 0:
        return 0.0
    slope = num / den
    # Flip sign: negative slope (improving positions) -> positive value
    return round(-slope, 4)


def _optimal_distance_for_age(age: int, distance: float) -> Optional[bool]:
    """Return True if distance is within the age-typical range."""
    # For ages outside our lookup, use age 7 range (most permissive)
    if age < 2:
        return None
    age_key = min(age, 7)
    lo, hi = _AGE_DISTANCE.get(age_key, (1400, 4000))
    return lo <= distance <= hi


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseLifecycleState:
    """Per-horse accumulated state for lifecycle features."""

    __slots__ = ("nb_races", "positions", "best_position", "races_since_best")

    def __init__(self) -> None:
        self.nb_races: int = 0
        self.positions: list[int] = []
        self.best_position: Optional[int] = None
        self.races_since_best: int = 0

    def snapshot(self) -> dict[str, Any]:
        """Compute features using only past races (strict temporal)."""
        return {
            "nb_races": self.nb_races,
            "races_since_peak": self.races_since_best if self.best_position is not None else None,
            "career_phase": _career_phase(self.nb_races),
            "improving_or_declining_phase": _position_trend(self.positions),
        }

    def update(self, position: Optional[int]) -> None:
        """Update state with a new race result (post-race)."""
        self.nb_races += 1
        if position is not None and position > 0:
            self.positions.append(position)
            self.races_since_best += 1
            if self.best_position is None or position < self.best_position:
                self.best_position = position
                self.races_since_best = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_age_lifecycle_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build age lifecycle features from partants_master.jsonl."""
    logger.info("=== Age Lifecycle Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "age": _safe_int(rec.get("age")),
            "discipline": rec.get("discipline") or rec.get("type_course") or "",
            "distance": _safe_float(rec.get("distance")),
            "position": _safe_int(rec.get("position_arrivee")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process record by record --
    t2 = time.time()
    horse_states: dict[str, _HorseLifecycleState] = defaultdict(_HorseLifecycleState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by (date, course) for temporal integrity
    i = 0
    total = len(slim_records)

    while i < total:
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

        # -- Snapshot pre-race features --
        for rec in course_group:
            cheval = rec["cheval"]
            age = rec["age"]
            discipline = rec["discipline"]
            distance = rec["distance"]

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "peak_age_for_discipline": None,
                "races_since_peak": None,
                "career_phase": None,
                "improving_or_declining_phase": None,
                "optimal_distance_age": None,
            }

            # Peak age for discipline
            if age is not None and discipline:
                peak = _is_peak_age(age, discipline)
                features["peak_age_for_discipline"] = peak

            # Horse lifecycle state features
            if cheval:
                state = horse_states[cheval]
                snap = state.snapshot()
                features["races_since_peak"] = snap["races_since_peak"]
                features["career_phase"] = snap["career_phase"]
                features["improving_or_declining_phase"] = snap["improving_or_declining_phase"]

            # Optimal distance for age
            if age is not None and distance is not None and distance > 0:
                features["optimal_distance_age"] = _optimal_distance_for_age(age, distance)

            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            cheval = rec["cheval"]
            if cheval:
                horse_states[cheval].update(rec["position"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Age lifecycle build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results), elapsed, len(horse_states),
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
        description="Construction des features age lifecycle a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/age_lifecycle/)",
    )
    args = parser.parse_args()

    logger = setup_logging("age_lifecycle_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_age_lifecycle_features(input_path, logger)

    # Save
    out_path = output_dir / "age_lifecycle.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
