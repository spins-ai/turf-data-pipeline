#!/usr/bin/env python3
"""
feature_builders.age_performance_curve_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features modeling the age-performance relationship per horse.

Each horse ages differently: some peak early, some stay competitive late.
This builder captures where a horse sits on its age curve relative to
typical peaks for each discipline, plus field-relative age context.

Temporal integrity: for any partant at date D, only data from races
with date < D contribute to computed features -- no future leakage.

Architecture: Index + chronological sort + seek (single pass).

Features (8):
  - apc_years_from_peak          : age - typical peak age for discipline
  - apc_years_from_peak_sq       : squared -- captures non-linear decline
  - apc_is_improving_age         : 1 if age <= peak_age
  - apc_is_declining_age         : 1 if age > peak_age + 2
  - apc_age_vs_field_mean        : age minus field average age
  - apc_performance_vs_age_expectation : recent position vs expected for age group
  - apc_career_length_at_age     : nb_courses_carriere / age (races per year)
  - apc_win_rate_by_age_group    : historical win rate for this (discipline, age)

Usage:
    python feature_builders/age_performance_curve_builder.py
    python feature_builders/age_performance_curve_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/age_performance_curve")

_LOG_EVERY = 500_000

# Typical peak ages per discipline group
_PEAK_AGE = {
    "TROT": 7,
    "TROT_ATTELE": 7,
    "TROT_MONTE": 7,
    "PLAT": 4,
    "GALOP_PLAT": 4,
    "OBSTACLE": 6,
    "GALOP_OBSTACLE": 6,
    "HAIES": 6,
    "STEEPLE_CHASE": 6,
    "CROSS_COUNTRY": 6,
}
_DEFAULT_PEAK = 5  # fallback


def _get_peak_age(discipline: str) -> int:
    """Return typical peak age for a discipline."""
    d = discipline.strip().upper().replace("-", "_").replace(" ", "_")
    # Try exact match first, then prefix matching
    if d in _PEAK_AGE:
        return _PEAK_AGE[d]
    for key, val in _PEAK_AGE.items():
        if d.startswith(key) or key.startswith(d):
            return val
    # Broad grouping
    if "TROT" in d:
        return 7
    if "PLAT" in d:
        return 4
    if "OBSTACLE" in d or "HAIE" in d or "STEEPLE" in d or "CROSS" in d:
        return 6
    return _DEFAULT_PEAK


def _discipline_group(discipline: str) -> str:
    """Normalize discipline to a broad group key."""
    d = discipline.strip().upper()
    if "TROT" in d:
        return "TROT"
    if "PLAT" in d:
        return "PLAT"
    if "OBSTACLE" in d or "HAIE" in d or "STEEPLE" in d or "CROSS" in d:
        return "OBSTACLE"
    return d or "UNKNOWN"


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class _AgeDisciplineStats:
    """Rolling win/total counters per (discipline_group, age).

    Used to compute historical win rate for horses of a given age
    in a given discipline -- temporal integrity guaranteed by the
    chronological processing order.
    """

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        # (disc_group, age) -> [wins, total]
        self.wins: dict[tuple[str, int], int] = defaultdict(int)
        self.total: dict[tuple[str, int], int] = defaultdict(int)

    def win_rate(self, disc_group: str, age: int) -> Optional[float]:
        key = (disc_group, age)
        t = self.total.get(key, 0)
        if t < 5:
            return None
        return round(self.wins.get(key, 0) / t, 4)

    def avg_position(self, disc_group: str, age: int) -> Optional[float]:
        """Not tracked here -- separate tracker needed."""
        return None

    def update(self, disc_group: str, age: int, is_winner: bool) -> None:
        key = (disc_group, age)
        self.total[key] += 1
        if is_winner:
            self.wins[key] += 1


class _AgePositionStats:
    """Rolling sum/count of position_arrivee per (discipline_group, age).

    Used for apc_performance_vs_age_expectation: compares a horse's recent
    position to the average position for its age group historically.
    """

    __slots__ = ("pos_sum", "pos_count")

    def __init__(self) -> None:
        self.pos_sum: dict[tuple[str, int], float] = defaultdict(float)
        self.pos_count: dict[tuple[str, int], int] = defaultdict(int)

    def avg_position(self, disc_group: str, age: int) -> Optional[float]:
        key = (disc_group, age)
        c = self.pos_count.get(key, 0)
        if c < 5:
            return None
        return round(self.pos_sum.get(key, 0.0) / c, 2)

    def update(self, disc_group: str, age: int, position: float) -> None:
        key = (disc_group, age)
        self.pos_sum[key] += position
        self.pos_count[key] += 1


class _HorseRecentPositions:
    """Track last N positions per horse for recent performance measure."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        # horse_id -> list of last positions (max 5)
        self.data: dict[str, list[float]] = {}

    def avg_recent(self, horse_id: str) -> Optional[float]:
        positions = self.data.get(horse_id)
        if not positions:
            return None
        return round(sum(positions) / len(positions), 2)

    def update(self, horse_id: str, position: float) -> None:
        if horse_id not in self.data:
            self.data[horse_id] = []
        lst = self.data[horse_id]
        lst.append(position)
        if len(lst) > 5:
            lst.pop(0)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_age_performance_curve_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build age-performance curve features.

    Architecture: index + chronological sort + seek.
    Single chronological pass with rolling state trackers.

    Returns total number of feature records written.
    """
    logger.info("=== Age Performance Curve Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    # (date_str, course_uid, num_pmu, byte_offset)
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line_s = line.strip()
            if not line_s:
                continue
            try:
                rec = json.loads(line_s)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexation: %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()

    age_disc_stats = _AgeDisciplineStats()
    age_pos_stats = _AgePositionStats()
    horse_recent_pos = _HorseRecentPositions()

    # Pre-collect field avg age per course in first sub-pass?
    # Actually we compute it inline: collect ages of all runners in the course
    # group, compute mean, then use it. This is safe because field avg age
    # is a property of the current race, not future data.

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts = {
        "apc_years_from_peak": 0,
        "apc_years_from_peak_sq": 0,
        "apc_is_improving_age": 0,
        "apc_is_declining_age": 0,
        "apc_age_vs_field_mean": 0,
        "apc_performance_vs_age_expectation": 0,
        "apc_career_length_at_age": 0,
        "apc_win_rate_by_age_group": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            age = rec.get("age") or rec.get("pgr_age_ans")
            if age is not None:
                try:
                    age = int(age)
                except (ValueError, TypeError):
                    age = None

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            position = rec.get("position_arrivee")
            if position is not None:
                try:
                    position = float(position)
                except (ValueError, TypeError):
                    position = None

            nb_courses = rec.get("nb_courses_carriere")
            if nb_courses is not None:
                try:
                    nb_courses = int(nb_courses)
                except (ValueError, TypeError):
                    nb_courses = None

            horse_id = rec.get("horse_id") or rec.get("nom_cheval") or ""

            return {
                "partant_uid": rec.get("partant_uid"),
                "course_uid": rec.get("course_uid", ""),
                "date_reunion_iso": rec.get("date_reunion_iso", ""),
                "age": age,
                "discipline": discipline,
                "position": position,
                "nb_courses": nb_courses,
                "horse_id": horse_id,
                "is_gagnant": bool(rec.get("is_gagnant")),
                "nombre_partants": rec.get("nombre_partants"),
            }

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read this course's records from disk
            course_group = [
                _extract(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # Compute field average age for this course
            field_ages = [r["age"] for r in course_group if r["age"] is not None]
            field_avg_age = (
                sum(field_ages) / len(field_ages) if field_ages else None
            )

            # -- Compute features for each partant (pre-race snapshot) --
            for rec in course_group:
                age = rec["age"]
                discipline = rec["discipline"]
                disc_group = _discipline_group(discipline)
                horse_id = rec["horse_id"]
                nb_courses = rec["nb_courses"]

                features: dict[str, Any] = {
                    "partant_uid": rec["partant_uid"],
                    "course_uid": rec["course_uid"],
                    "date_reunion_iso": rec["date_reunion_iso"],
                }

                if age is not None and discipline:
                    peak_age = _get_peak_age(discipline)

                    # apc_years_from_peak
                    years_from_peak = age - peak_age
                    features["apc_years_from_peak"] = years_from_peak
                    fill_counts["apc_years_from_peak"] += 1

                    # apc_years_from_peak_sq
                    features["apc_years_from_peak_sq"] = years_from_peak * years_from_peak
                    fill_counts["apc_years_from_peak_sq"] += 1

                    # apc_is_improving_age
                    features["apc_is_improving_age"] = 1 if age <= peak_age else 0
                    fill_counts["apc_is_improving_age"] += 1

                    # apc_is_declining_age
                    features["apc_is_declining_age"] = 1 if age > peak_age + 2 else 0
                    fill_counts["apc_is_declining_age"] += 1

                else:
                    features["apc_years_from_peak"] = None
                    features["apc_years_from_peak_sq"] = None
                    features["apc_is_improving_age"] = None
                    features["apc_is_declining_age"] = None

                # apc_age_vs_field_mean
                if age is not None and field_avg_age is not None:
                    features["apc_age_vs_field_mean"] = round(age - field_avg_age, 2)
                    fill_counts["apc_age_vs_field_mean"] += 1
                else:
                    features["apc_age_vs_field_mean"] = None

                # apc_performance_vs_age_expectation
                # Compare horse's recent avg position to the historical avg
                # position for horses of this age in this discipline
                if age is not None and discipline and horse_id:
                    recent_pos = horse_recent_pos.avg_recent(horse_id)
                    expected_pos = age_pos_stats.avg_position(disc_group, age)
                    if recent_pos is not None and expected_pos is not None and expected_pos > 0:
                        # Negative = better than expected (lower position is better)
                        features["apc_performance_vs_age_expectation"] = round(
                            recent_pos - expected_pos, 2
                        )
                        fill_counts["apc_performance_vs_age_expectation"] += 1
                    else:
                        features["apc_performance_vs_age_expectation"] = None
                else:
                    features["apc_performance_vs_age_expectation"] = None

                # apc_career_length_at_age
                if nb_courses is not None and age is not None and age > 0:
                    features["apc_career_length_at_age"] = round(nb_courses / age, 2)
                    fill_counts["apc_career_length_at_age"] += 1
                else:
                    features["apc_career_length_at_age"] = None

                # apc_win_rate_by_age_group
                if age is not None and discipline:
                    wr = age_disc_stats.win_rate(disc_group, age)
                    features["apc_win_rate_by_age_group"] = wr
                    if wr is not None:
                        fill_counts["apc_win_rate_by_age_group"] += 1
                else:
                    features["apc_win_rate_by_age_group"] = None

                # Write feature record
                fout.write(
                    json.dumps(features, ensure_ascii=False, default=str) + "\n"
                )
                n_written += 1

            # -- Update state AFTER race (temporal integrity) --
            for rec in course_group:
                age = rec["age"]
                discipline = rec["discipline"]
                disc_group = _discipline_group(discipline)
                horse_id = rec["horse_id"]
                position = rec["position"]

                if age is not None and discipline:
                    age_disc_stats.update(disc_group, age, rec["is_gagnant"])

                    if position is not None and position > 0:
                        age_pos_stats.update(disc_group, age, position)

                if horse_id and position is not None and position > 0:
                    horse_recent_pos.update(horse_id, position)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features en %.1fs", n_written, elapsed,
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    # State sizes
    logger.info(
        "State: %d (disc,age) combos, %d horses tracked",
        len(age_disc_stats.total),
        len(horse_recent_pos.data),
    )

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features age-performance curve"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("age_performance_curve_builder")
    logger.info("=" * 70)
    logger.info("age_performance_curve_builder.py — Age-Performance Curve Features")
    logger.info("=" * 70)

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "age_performance_curve_features.jsonl"
    build_age_performance_curve_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
