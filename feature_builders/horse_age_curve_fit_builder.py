#!/usr/bin/env python3
"""
feature_builders.horse_age_curve_fit_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse age performance curve fitting -- where is each horse on its
age-performance curve?

Reads partants_master.jsonl via index + chronological sort + seek architecture.

Temporal integrity: for any partant at date D, only races with date < D
contribute to computed features -- no future leakage.

Produces:
  - horse_age_curve_fit.jsonl  in builder_outputs/horse_age_curve_fit/

Features per partant (8):
  - hac_age_wr_current          : horse's win rate at current age
  - hac_age_wr_previous         : horse's win rate at age-1 (previous year)
  - hac_age_improvement         : current age wr - previous age wr (positive = improving)
  - hac_peak_age                : age at which horse had best win rate (min 3 races at that age)
  - hac_is_past_peak            : 1 if current age > peak_age
  - hac_years_since_peak        : current age - peak_age (0 if at peak)
  - hac_age_total_races         : how many races at this age so far
  - hac_career_arc              : 0=rising, 1=peak, 2=declining, 3=unknown

Usage:
    python feature_builders/horse_age_curve_fit_builder.py
    python feature_builders/horse_age_curve_fit_builder.py --input path/to/partants_master.jsonl
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
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/horse_age_curve_fit")

# Progress / GC frequency
_LOG_EVERY = 500_000

# Minimum races at a given age to consider it for peak detection
_MIN_RACES_FOR_PEAK = 3

# ===========================================================================
# FEATURE NAMES
# ===========================================================================

_FEATURE_NAMES = [
    "hac_age_wr_current",
    "hac_age_wr_previous",
    "hac_age_improvement",
    "hac_peak_age",
    "hac_is_past_peak",
    "hac_years_since_peak",
    "hac_age_total_races",
    "hac_career_arc",
]


# ===========================================================================
# HORSE STATE TRACKER
# ===========================================================================


class _HorseAgeState:
    """Per-horse state tracking wins and races broken down by age.

    State:
      age_stats: {age -> [wins, total]}
      best_age:  age with best win rate (among ages with >= _MIN_RACES_FOR_PEAK races)
      best_age_wr: win rate at best_age
    """

    __slots__ = ("age_stats", "best_age", "best_age_wr")

    def __init__(self) -> None:
        # {age: [wins, total]}
        self.age_stats: dict[int, list[int]] = {}
        self.best_age: Optional[int] = None
        self.best_age_wr: float = -1.0

    def snapshot(self, current_age: Optional[int]) -> dict[str, Any]:
        """Compute features BEFORE updating with current race (temporal integrity)."""
        features: dict[str, Any] = {name: None for name in _FEATURE_NAMES}

        if current_age is None:
            return features

        # -- hac_age_wr_current --
        cur_stats = self.age_stats.get(current_age)
        if cur_stats and cur_stats[1] > 0:
            cur_wr = round(cur_stats[0] / cur_stats[1], 4)
            features["hac_age_wr_current"] = cur_wr
            features["hac_age_total_races"] = cur_stats[1]
        else:
            features["hac_age_wr_current"] = None
            features["hac_age_total_races"] = 0

        # -- hac_age_wr_previous --
        prev_age = current_age - 1
        prev_stats = self.age_stats.get(prev_age)
        if prev_stats and prev_stats[1] > 0:
            prev_wr = round(prev_stats[0] / prev_stats[1], 4)
            features["hac_age_wr_previous"] = prev_wr
        else:
            prev_wr = None
            features["hac_age_wr_previous"] = None

        # -- hac_age_improvement --
        if features["hac_age_wr_current"] is not None and prev_wr is not None:
            features["hac_age_improvement"] = round(
                features["hac_age_wr_current"] - prev_wr, 4
            )

        # -- hac_peak_age, hac_is_past_peak, hac_years_since_peak --
        if self.best_age is not None:
            features["hac_peak_age"] = self.best_age
            features["hac_is_past_peak"] = 1 if current_age > self.best_age else 0
            features["hac_years_since_peak"] = max(0, current_age - self.best_age)

        # -- hac_career_arc --
        features["hac_career_arc"] = self._compute_career_arc(current_age)

        return features

    def _compute_career_arc(self, current_age: int) -> int:
        """Determine career arc category.

        0 = rising (improving each year with data)
        1 = peak (at best year)
        2 = declining (past peak, getting worse)
        3 = unknown (not enough data)
        """
        # Need at least 2 ages with data to determine arc
        ages_with_data = sorted(
            age for age, stats in self.age_stats.items()
            if stats[1] >= _MIN_RACES_FOR_PEAK
        )

        if len(ages_with_data) < 2:
            return 3  # unknown

        if self.best_age is None:
            return 3

        if current_age == self.best_age:
            return 1  # peak

        if current_age > self.best_age:
            return 2  # declining

        # current_age < best_age: check if still improving
        # Compare last two ages with sufficient data
        recent_ages = [a for a in ages_with_data if a <= current_age]
        if len(recent_ages) >= 2:
            last_wr = self.age_stats[recent_ages[-1]][0] / self.age_stats[recent_ages[-1]][1]
            prev_wr = self.age_stats[recent_ages[-2]][0] / self.age_stats[recent_ages[-2]][1]
            if last_wr >= prev_wr:
                return 0  # rising
            else:
                return 2  # declining
        return 3  # unknown

    def update(self, age: Optional[int], is_winner: bool) -> None:
        """Update state AFTER computing features (post-race)."""
        if age is None:
            return

        if age not in self.age_stats:
            self.age_stats[age] = [0, 0]

        if is_winner:
            self.age_stats[age][0] += 1
        self.age_stats[age][1] += 1

        # Recompute best_age among ages with enough races
        self.best_age = None
        self.best_age_wr = -1.0
        for a, (wins, total) in self.age_stats.items():
            if total >= _MIN_RACES_FOR_PEAK:
                wr = wins / total
                if wr > self.best_age_wr or (wr == self.best_age_wr and a < (self.best_age or 999)):
                    self.best_age_wr = wr
                    self.best_age = a


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_horse_age_curve_fit_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build horse age curve fit features from partants_master.jsonl.

    Architecture: index + chronological sort + seek.
      1. Read only sort keys + byte offsets into memory.
      2. Sort chronologically.
      3. Process course by course, seek to read full records, stream output.

    Returns the total number of feature records written.
    """
    logger.info("=== Horse Age Curve Fit Builder (index + sort + seek) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseAgeState] = defaultdict(_HorseAgeState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {name: 0 for name in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            age = _safe_int(rec.get("age"))
            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "cheval": rec.get("nom_cheval"),
                "age": age,
                "gagnant": bool(rec.get("is_gagnant")),
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
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                age = rec["age"]

                features: dict[str, Any] = {
                    "partant_uid": rec["uid"],
                    "course_uid": rec["course"],
                    "date_reunion_iso": rec["date"],
                }

                if not cheval or age is None:
                    # No horse name or no age -> all None
                    for name in _FEATURE_NAMES:
                        features[name] = None
                    fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[cheval]
                snap = hs.snapshot(age)
                features.update(snap)

                # Count fills
                for name in _FEATURE_NAMES:
                    if features.get(name) is not None:
                        fill_counts[name] += 1

                # Write record
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER computing features (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                age = rec["age"]
                if cheval and age is not None:
                    horse_state[cheval].update(age, rec["gagnant"])

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Horse age curve fit build termine: %d features en %.1fs "
        "(chevaux uniques: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
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
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features age-performance curve a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/horse_age_curve_fit/)",
    )
    args = parser.parse_args()

    logger = setup_logging("horse_age_curve_fit_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "horse_age_curve_fit.jsonl"
    build_horse_age_curve_fit_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
