#!/usr/bin/env python3
"""
feature_builders.calibration_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Market calibration features for Platt scaling, isotonic calibration, and
inter-bloc calibration modules.  Measures how well-calibrated the market
is across different contexts (hippodrome, discipline, odds range, time
of day).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant calibration features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to calibration stats -- no future leakage.  State is
snapshotted BEFORE being updated with the current race.

Produces:
  - calibration_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/calibration_features/

Features per partant (10):
  - cal_hippo_calibration_error   : rolling |1/cote - actual_win_rate| at this hippodrome
  - cal_discipline_calibration_error : same but per discipline
  - cal_cote_range_accuracy       : actual win rate for this cote range at this hippo
  - cal_favorite_overperformance  : rolling (actual fav win rate - 1/fav_cote)
  - cal_longshot_bias             : actual vs implied win rate for cote > 15 at this hippo
  - cal_is_well_calibrated_context: 1 if both hippo and discipline cal errors < 0.05
  - cal_confidence_adjustment     : actual/implied ratio for this context
  - cal_time_of_day_effect        : per-hour-bucket win rate at this hippo
  - cal_recent_calibration_shift  : calibration error now vs 100 races ago
  - cal_market_wisdom_score       : inverse of calibration error (higher = more accurate)

Usage:
    python feature_builders/calibration_features_builder.py
    python feature_builders/calibration_features_builder.py --input path/to/partants_master.jsonl
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

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/calibration_features")

# Rolling window sizes
_ROLLING_RACES = 200       # main rolling window for hippo / discipline
_ROLLING_SHIFT = 100       # look-back for recent calibration shift
_GC_EVERY = 500_000        # gc.collect() frequency
_LOG_EVERY = 500_000       # progress log frequency


# ===========================================================================
# HELPERS
# ===========================================================================


def _cote_range(cote: float) -> str:
    """Bucket a cote value into a discrete range label."""
    if cote <= 2.0:
        return "1-2"
    elif cote <= 4.0:
        return "2-4"
    elif cote <= 7.0:
        return "4-7"
    elif cote <= 10.0:
        return "7-10"
    elif cote <= 15.0:
        return "10-15"
    elif cote <= 25.0:
        return "15-25"
    else:
        return "25+"


def _parse_hour(heure_str) -> Optional[int]:
    """Extract hour bucket from heure_depart or similar field.

    Handles formats: "14:30", "14h30", "1430", "14:30:00".
    Returns integer hour (0-23) or None.
    """
    if not heure_str:
        return None
    h = str(heure_str).strip()
    # "14:30" or "14:30:00"
    if ":" in h:
        try:
            return int(h.split(":")[0])
        except (ValueError, IndexError):
            return None
    # "14h30"
    if "h" in h.lower():
        try:
            return int(h.lower().split("h")[0])
        except (ValueError, IndexError):
            return None
    # "1430" (4 digits)
    if h.isdigit() and len(h) == 4:
        try:
            return int(h[:2])
        except ValueError:
            return None
    return None


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
# STATE TRACKERS
# ===========================================================================


class _CalibrationTracker:
    """Tracks rolling calibration stats per key (hippo or discipline).

    Maintains a deque of (implied_prob, won_flag) tuples with maxlen.
    """

    __slots__ = ("history",)

    def __init__(self, maxlen: int = _ROLLING_RACES) -> None:
        # Each entry: (implied_prob, is_winner: 0 or 1)
        self.history: deque = deque(maxlen=maxlen)

    def calibration_error(self) -> Optional[float]:
        """Average |implied_prob - actual_win_rate|."""
        if len(self.history) < 10:
            return None
        actual_wr = sum(w for _, w in self.history) / len(self.history)
        avg_implied = sum(p for p, _ in self.history) / len(self.history)
        return round(abs(avg_implied - actual_wr), 6)

    def actual_win_rate(self) -> Optional[float]:
        if len(self.history) < 5:
            return None
        return round(sum(w for _, w in self.history) / len(self.history), 6)

    def avg_implied(self) -> Optional[float]:
        if len(self.history) < 5:
            return None
        return round(sum(p for p, _ in self.history) / len(self.history), 6)

    def add(self, implied_prob: float, won: int) -> None:
        self.history.append((implied_prob, won))


class _CoteRangeTracker:
    """Tracks wins/runs per (hippo, cote_range) using rolling deque."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        # key: (hippo, cote_range) -> deque of (is_winner,)
        self.data: dict[tuple[str, str], deque] = defaultdict(
            lambda: deque(maxlen=_ROLLING_RACES)
        )

    def win_rate(self, hippo: str, cote_range: str) -> Optional[float]:
        key = (hippo, cote_range)
        d = self.data.get(key)
        if d is None or len(d) < 5:
            return None
        return round(sum(d) / len(d), 6)

    def add(self, hippo: str, cote_range: str, won: int) -> None:
        self.data[(hippo, cote_range)].append(won)


class _FavoriteTracker:
    """Tracks rolling favorite performance per hippo.

    A 'favorite' is the horse with the lowest cote in a race.
    We track: (is_fav_won, 1/fav_cote) in a rolling window.
    """

    __slots__ = ("data",)

    def __init__(self) -> None:
        # hippo -> deque of (fav_won: 0/1, implied_fav_prob: float)
        self.data: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=_ROLLING_RACES)
        )

    def overperformance(self, hippo: str) -> Optional[float]:
        d = self.data.get(hippo)
        if d is None or len(d) < 10:
            return None
        actual = sum(w for w, _ in d) / len(d)
        implied = sum(p for _, p in d) / len(d)
        return round(actual - implied, 6)

    def add(self, hippo: str, fav_won: int, implied_fav_prob: float) -> None:
        self.data[hippo].append((fav_won, implied_fav_prob))


class _LongshotTracker:
    """Tracks longshot (cote > 15) calibration per hippo."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        # hippo -> deque of (won: 0/1, implied_prob: float)
        self.data: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=_ROLLING_RACES)
        )

    def bias(self, hippo: str) -> Optional[float]:
        """actual win rate - implied win rate for longshots. Positive = longshots win more than expected."""
        d = self.data.get(hippo)
        if d is None or len(d) < 10:
            return None
        actual = sum(w for w, _ in d) / len(d)
        implied = sum(p for _, p in d) / len(d)
        return round(actual - implied, 6)

    def add(self, hippo: str, won: int, implied_prob: float) -> None:
        self.data[hippo].append((won, implied_prob))


class _HourBucketTracker:
    """Tracks win rates per (hippo, hour_bucket)."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        # (hippo, hour_bucket) -> deque of (won,)
        self.data: dict[tuple[str, int], deque] = defaultdict(
            lambda: deque(maxlen=_ROLLING_RACES)
        )

    def win_rate(self, hippo: str, hour: int) -> Optional[float]:
        key = (hippo, hour)
        d = self.data.get(key)
        if d is None or len(d) < 10:
            return None
        return round(sum(d) / len(d), 6)

    def add(self, hippo: str, hour: int, won: int) -> None:
        self.data[(hippo, hour)].append(won)


class _CalibrationShiftTracker:
    """Tracks calibration error history to detect improving/worsening trends.

    Stores per-hippo rolling list of recent calibration errors.
    """

    __slots__ = ("data",)

    def __init__(self) -> None:
        # hippo -> deque of cal_error values (length ~ _ROLLING_RACES)
        self.data: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=_ROLLING_RACES)
        )

    def shift(self, hippo: str) -> Optional[float]:
        """Current cal_error minus cal_error from _ROLLING_SHIFT races ago.

        Positive = calibration worsening, negative = improving.
        """
        d = self.data.get(hippo)
        if d is None or len(d) < _ROLLING_SHIFT + 1:
            return None
        current = d[-1]
        past = d[-(1 + _ROLLING_SHIFT)]
        return round(current - past, 6)

    def add(self, hippo: str, cal_error: float) -> None:
        self.data[hippo].append(cal_error)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_calibration_features(input_path: Path, output_path: Path, logger) -> int:
    """Build calibration features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         snapshot BEFORE update, and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Calibration Features Builder ===")
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

    # State trackers
    hippo_cal = defaultdict(lambda: _CalibrationTracker(_ROLLING_RACES))
    discipline_cal = defaultdict(lambda: _CalibrationTracker(_ROLLING_RACES))
    cote_range_tracker = _CoteRangeTracker()
    fav_tracker = _FavoriteTracker()
    longshot_tracker = _LongshotTracker()
    hour_tracker = _HourBucketTracker()
    shift_tracker = _CalibrationShiftTracker()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "cal_hippo_calibration_error",
        "cal_discipline_calibration_error",
        "cal_cote_range_accuracy",
        "cal_favorite_overperformance",
        "cal_longshot_bias",
        "cal_is_well_calibrated_context",
        "cal_confidence_adjustment",
        "cal_time_of_day_effect",
        "cal_recent_calibration_shift",
        "cal_market_wisdom_score",
    ]
    fill_counts = {f: 0 for f in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            cote = rec.get("rapport_final") or rec.get("cote_finale") or rec.get("cote_probable")
            try:
                cote = float(cote) if cote else None
            except (ValueError, TypeError):
                cote = None

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            heure = rec.get("heure_depart") or rec.get("heure_course") or ""

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "gagnant": bool(rec.get("is_gagnant")),
                "hippo": rec.get("hippodrome_normalise", ""),
                "discipline": discipline,
                "cote": cote,
                "heure": heure,
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
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            # Determine course-level info
            hippo = course_group[0]["hippo"] if course_group else ""
            discipline = course_group[0]["discipline"] if course_group else ""
            hour_bucket = _parse_hour(course_group[0]["heure"]) if course_group else None

            # Find the favorite (lowest cote) for this race
            fav_cote = None
            fav_won = 0
            for rec in course_group:
                c = rec["cote"]
                if c is not None and c > 0:
                    if fav_cote is None or c < fav_cote:
                        fav_cote = c
                        fav_won = 1 if rec["gagnant"] else 0

            # ------- SNAPSHOT BEFORE UPDATE (temporal integrity) -------
            for rec in course_group:
                cote = rec["cote"]
                is_winner = 1 if rec["gagnant"] else 0

                features: dict[str, Any] = {
                    "partant_uid": rec["uid"],
                    "course_uid": rec["course"],
                    "date_reunion_iso": rec["date"],
                }

                # --- cal_hippo_calibration_error ---
                hippo_err = hippo_cal[hippo].calibration_error() if hippo else None
                features["cal_hippo_calibration_error"] = hippo_err
                if hippo_err is not None:
                    fill_counts["cal_hippo_calibration_error"] += 1

                # --- cal_discipline_calibration_error ---
                disc_err = discipline_cal[discipline].calibration_error() if discipline else None
                features["cal_discipline_calibration_error"] = disc_err
                if disc_err is not None:
                    fill_counts["cal_discipline_calibration_error"] += 1

                # --- cal_cote_range_accuracy ---
                if cote is not None and cote > 0 and hippo:
                    cr = _cote_range(cote)
                    cra = cote_range_tracker.win_rate(hippo, cr)
                    features["cal_cote_range_accuracy"] = cra
                    if cra is not None:
                        fill_counts["cal_cote_range_accuracy"] += 1
                else:
                    features["cal_cote_range_accuracy"] = None

                # --- cal_favorite_overperformance ---
                fav_op = fav_tracker.overperformance(hippo) if hippo else None
                features["cal_favorite_overperformance"] = fav_op
                if fav_op is not None:
                    fill_counts["cal_favorite_overperformance"] += 1

                # --- cal_longshot_bias ---
                ls_bias = longshot_tracker.bias(hippo) if hippo else None
                features["cal_longshot_bias"] = ls_bias
                if ls_bias is not None:
                    fill_counts["cal_longshot_bias"] += 1

                # --- cal_is_well_calibrated_context ---
                if hippo_err is not None and disc_err is not None:
                    well_cal = 1 if (hippo_err < 0.05 and disc_err < 0.05) else 0
                    features["cal_is_well_calibrated_context"] = well_cal
                    fill_counts["cal_is_well_calibrated_context"] += 1
                else:
                    features["cal_is_well_calibrated_context"] = None

                # --- cal_confidence_adjustment ---
                if hippo:
                    h_actual = hippo_cal[hippo].actual_win_rate()
                    h_implied = hippo_cal[hippo].avg_implied()
                    if h_actual is not None and h_implied is not None and h_implied > 0:
                        features["cal_confidence_adjustment"] = round(h_actual / h_implied, 6)
                        fill_counts["cal_confidence_adjustment"] += 1
                    else:
                        features["cal_confidence_adjustment"] = None
                else:
                    features["cal_confidence_adjustment"] = None

                # --- cal_time_of_day_effect ---
                if hippo and hour_bucket is not None:
                    tod_wr = hour_tracker.win_rate(hippo, hour_bucket)
                    features["cal_time_of_day_effect"] = tod_wr
                    if tod_wr is not None:
                        fill_counts["cal_time_of_day_effect"] += 1
                else:
                    features["cal_time_of_day_effect"] = None

                # --- cal_recent_calibration_shift ---
                if hippo:
                    shift = shift_tracker.shift(hippo)
                    features["cal_recent_calibration_shift"] = shift
                    if shift is not None:
                        fill_counts["cal_recent_calibration_shift"] += 1
                else:
                    features["cal_recent_calibration_shift"] = None

                # --- cal_market_wisdom_score ---
                if hippo_err is not None and hippo_err > 0:
                    features["cal_market_wisdom_score"] = round(1.0 / hippo_err, 4)
                    fill_counts["cal_market_wisdom_score"] += 1
                elif hippo_err is not None and hippo_err == 0:
                    # Perfect calibration: cap at a high value
                    features["cal_market_wisdom_score"] = 1000.0
                    fill_counts["cal_market_wisdom_score"] += 1
                else:
                    features["cal_market_wisdom_score"] = None

                # Stream to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # ------- UPDATE STATE AFTER RACE -------
            for rec in course_group:
                cote = rec["cote"]
                is_winner = 1 if rec["gagnant"] else 0
                rec_hippo = rec["hippo"]
                rec_discipline = rec["discipline"]

                if cote is not None and cote > 0:
                    implied = 1.0 / cote

                    # Hippo calibration
                    if rec_hippo:
                        hippo_cal[rec_hippo].add(implied, is_winner)

                    # Discipline calibration
                    if rec_discipline:
                        discipline_cal[rec_discipline].add(implied, is_winner)

                    # Cote range
                    if rec_hippo:
                        cr = _cote_range(cote)
                        cote_range_tracker.add(rec_hippo, cr, is_winner)

                    # Longshot (cote > 15)
                    if cote > 15 and rec_hippo:
                        longshot_tracker.add(rec_hippo, is_winner, implied)

                # Hour bucket
                if rec_hippo and hour_bucket is not None:
                    hour_tracker.add(rec_hippo, hour_bucket, is_winner)

            # Favorite tracker: once per race
            if fav_cote is not None and fav_cote > 0 and hippo:
                fav_tracker.add(hippo, fav_won, 1.0 / fav_cote)

            # Calibration shift tracker: record hippo cal error after update
            if hippo:
                err_now = hippo_cal[hippo].calibration_error()
                if err_now is not None:
                    shift_tracker.add(hippo, err_now)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)

            if n_processed % _GC_EVERY < len(course_group):
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Calibration build termine: %d features en %.1fs",
        n_written, elapsed,
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
        description="Construction des features de calibration marche a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/calibration_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("calibration_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "calibration_features.jsonl"
    build_calibration_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
