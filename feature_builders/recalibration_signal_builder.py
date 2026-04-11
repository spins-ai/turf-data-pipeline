#!/usr/bin/env python3
"""
feature_builders.recalibration_signal_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Auto-recalibration signal features that detect when models/markets are
miscalibrated.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically (course-by-course), and computes per-partant recalibration
signals using rolling global statistics.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the rolling statistics -- no future leakage.  Global state
is updated AFTER computing features for all runners in a given race.

Produces:
  - recalibration_signal.jsonl  in builder_outputs/recalibration_signal/

Features per partant (8):
  - rcl_fav_win_rate_recent         : win rate of favourites (cote < 5) in
                                      last 100 races seen globally
  - rcl_fav_expected_vs_actual      : expected win rate from implied proba
                                      vs actual for favourites (last 200 races)
  - rcl_outsider_surprise_rate      : rate of outsiders (cote > 15) winning
                                      in last 100 races
  - rcl_market_accuracy_trend       : rolling accuracy (did lowest-odds horse
                                      win?) over last 50 races
  - rcl_discipline_bias             : win rate of favourites in this discipline
                                      vs overall (discipline-specific market
                                      inefficiency)
  - rcl_hippo_bias                  : win rate of favourites at this hippodrome
                                      vs overall (track-specific bias)
  - rcl_odds_bracket_calibration    : for this horse's odds bracket
                                      (1-3, 3-5, 5-10, 10-20, 20+), actual
                                      win rate vs expected from proba
  - rcl_recent_longshot_bias        : difference between actual and expected
                                      win rates for longshots (cote > 15)
                                      in last 200 races

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 streams output to disk via seek-based re-reading
  - .tmp then atomic rename, gc.collect() every 500K records

Usage:
    python feature_builders/recalibration_signal_builder.py
    python feature_builders/recalibration_signal_builder.py --input D:/path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/recalibration_signal")

_LOG_EVERY = 500_000

# Rolling window sizes
_FAV_WINDOW = 100
_FAV_EXPECTED_WINDOW = 200
_OUTSIDER_WINDOW = 100
_ACCURACY_WINDOW = 50
_LONGSHOT_WINDOW = 200

# Odds thresholds
_FAV_THRESHOLD = 5.0
_OUTSIDER_THRESHOLD = 15.0

# Odds brackets for calibration
_BRACKETS = [
    ("1-3", 1.0, 3.0),
    ("3-5", 3.0, 5.0),
    ("5-10", 5.0, 10.0),
    ("10-20", 10.0, 20.0),
    ("20+", 20.0, 9999.0),
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert to float, return None on failure."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v and v > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_rate(num: int, den: int) -> Optional[float]:
    """Safe division with None guard."""
    if den < 1:
        return None
    return round(num / den, 6)


def _get_bracket(cote: Optional[float]) -> Optional[str]:
    """Map odds to a bracket label."""
    if cote is None or cote <= 0:
        return None
    for label, lo, hi in _BRACKETS:
        if lo <= cote < hi:
            return label
    return None


# ===========================================================================
# GLOBAL ROLLING STATE
# ===========================================================================


class _RecalibrationState:
    """Global rolling state for recalibration signals.

    All deques store per-race outcomes (one entry per race, not per runner).
    Updated AFTER features are computed for the current race.
    """

    __slots__ = (
        "fav_outcomes",
        "fav_expected",
        "outsider_outcomes",
        "accuracy_outcomes",
        "longshot_actual",
        "longshot_expected",
        "disc_fav_wins",
        "disc_fav_total",
        "hippo_fav_wins",
        "hippo_fav_total",
        "overall_fav_wins",
        "overall_fav_total",
        "bracket_wins",
        "bracket_total",
        "bracket_expected_sum",
    )

    def __init__(self) -> None:
        # Rolling deques for recent race outcomes (one entry per race)
        # fav_outcomes: 1 if a favourite (cote<5) won, 0 otherwise
        self.fav_outcomes: deque = deque(maxlen=_FAV_WINDOW)
        # fav_expected: (expected_proba, did_win) for favourite in each race
        self.fav_expected: deque = deque(maxlen=_FAV_EXPECTED_WINDOW)
        # outsider_outcomes: 1 if outsider (cote>15) won, 0 otherwise
        self.outsider_outcomes: deque = deque(maxlen=_OUTSIDER_WINDOW)
        # accuracy: 1 if lowest-odds horse won, 0 otherwise
        self.accuracy_outcomes: deque = deque(maxlen=_ACCURACY_WINDOW)
        # longshot: (expected_proba, did_win) for longshots
        self.longshot_actual: deque = deque(maxlen=_LONGSHOT_WINDOW)
        self.longshot_expected: deque = deque(maxlen=_LONGSHOT_WINDOW)

        # Per-discipline favourite stats (cumulative, not rolling)
        self.disc_fav_wins: dict[str, int] = defaultdict(int)
        self.disc_fav_total: dict[str, int] = defaultdict(int)

        # Per-hippodrome favourite stats (cumulative)
        self.hippo_fav_wins: dict[str, int] = defaultdict(int)
        self.hippo_fav_total: dict[str, int] = defaultdict(int)

        # Overall favourite stats (cumulative)
        self.overall_fav_wins: int = 0
        self.overall_fav_total: int = 0

        # Per-bracket calibration (cumulative)
        self.bracket_wins: dict[str, int] = defaultdict(int)
        self.bracket_total: dict[str, int] = defaultdict(int)
        self.bracket_expected_sum: dict[str, float] = defaultdict(float)

    # --- Snapshot methods (read BEFORE update) ---

    def snapshot_fav_win_rate_recent(self) -> Optional[float]:
        """rcl_fav_win_rate_recent: win rate of favourites in recent races."""
        if not self.fav_outcomes:
            return None
        return round(sum(self.fav_outcomes) / len(self.fav_outcomes), 6)

    def snapshot_fav_expected_vs_actual(self) -> Optional[float]:
        """rcl_fav_expected_vs_actual: expected - actual win rate for favourites."""
        if len(self.fav_expected) < 5:
            return None
        actual_wins = sum(did_win for _, did_win in self.fav_expected)
        expected_sum = sum(exp_p for exp_p, _ in self.fav_expected)
        n = len(self.fav_expected)
        expected_rate = expected_sum / n
        actual_rate = actual_wins / n
        return round(expected_rate - actual_rate, 6)

    def snapshot_outsider_surprise_rate(self) -> Optional[float]:
        """rcl_outsider_surprise_rate: rate of outsider wins in recent races."""
        if not self.outsider_outcomes:
            return None
        return round(sum(self.outsider_outcomes) / len(self.outsider_outcomes), 6)

    def snapshot_market_accuracy_trend(self) -> Optional[float]:
        """rcl_market_accuracy_trend: rolling market accuracy."""
        if not self.accuracy_outcomes:
            return None
        return round(sum(self.accuracy_outcomes) / len(self.accuracy_outcomes), 6)

    def snapshot_discipline_bias(self, discipline: Optional[str]) -> Optional[float]:
        """rcl_discipline_bias: disc favourite win rate - overall favourite win rate."""
        if not discipline or self.overall_fav_total < 10:
            return None
        disc_total = self.disc_fav_total.get(discipline, 0)
        if disc_total < 5:
            return None
        disc_wr = self.disc_fav_wins.get(discipline, 0) / disc_total
        overall_wr = self.overall_fav_wins / self.overall_fav_total
        return round(disc_wr - overall_wr, 6)

    def snapshot_hippo_bias(self, hippo: Optional[str]) -> Optional[float]:
        """rcl_hippo_bias: hippo favourite win rate - overall favourite win rate."""
        if not hippo or self.overall_fav_total < 10:
            return None
        hippo_total = self.hippo_fav_total.get(hippo, 0)
        if hippo_total < 5:
            return None
        hippo_wr = self.hippo_fav_wins.get(hippo, 0) / hippo_total
        overall_wr = self.overall_fav_wins / self.overall_fav_total
        return round(hippo_wr - overall_wr, 6)

    def snapshot_bracket_calibration(self, bracket: Optional[str]) -> Optional[float]:
        """rcl_odds_bracket_calibration: actual win rate - expected for this bracket."""
        if not bracket:
            return None
        total = self.bracket_total.get(bracket, 0)
        if total < 10:
            return None
        actual_wr = self.bracket_wins.get(bracket, 0) / total
        expected_wr = self.bracket_expected_sum.get(bracket, 0.0) / total
        return round(actual_wr - expected_wr, 6)

    def snapshot_longshot_bias(self) -> Optional[float]:
        """rcl_recent_longshot_bias: actual - expected win rate for longshots."""
        n = len(self.longshot_actual)
        if n < 10:
            return None
        actual_wins = sum(self.longshot_actual)
        expected_sum = sum(self.longshot_expected)
        actual_rate = actual_wins / n
        expected_rate = expected_sum / n
        return round(actual_rate - expected_rate, 6)

    # --- Update method (called AFTER features for the race) ---

    def update_race(
        self,
        runners: list[dict],
        winner_horse: Optional[str],
    ) -> None:
        """Update global state with the results of one completed race.

        runners: list of slim dicts with keys: horse_id, cote, proba, is_gagnant,
                 discipline, hippo
        """
        if not runners:
            return

        # Find favourite (lowest cote) and determine if they won
        fav_cote = None
        fav_won = False
        fav_proba = None
        lowest_odds_horse = None
        lowest_odds = None

        has_outsider_winner = False
        outsider_expected = None

        for r in runners:
            cote = r.get("cote")
            if cote is None:
                continue

            # Track lowest odds horse (for accuracy)
            if lowest_odds is None or cote < lowest_odds:
                lowest_odds = cote
                lowest_odds_horse = r.get("horse_id")

            # Favourite stats (cote < 5)
            if cote < _FAV_THRESHOLD:
                if fav_cote is None or cote < fav_cote:
                    fav_cote = cote
                    fav_won = bool(r.get("is_gagnant"))
                    fav_proba = r.get("proba") or (1.0 / cote if cote > 0 else None)

            # Outsider stats (cote > 15)
            if cote > _OUTSIDER_THRESHOLD and r.get("is_gagnant"):
                has_outsider_winner = True

        # Update rolling favourite outcomes
        if fav_cote is not None:
            self.fav_outcomes.append(1 if fav_won else 0)
            if fav_proba is not None:
                self.fav_expected.append((fav_proba, 1 if fav_won else 0))

        # Update rolling outsider outcomes (per race: did any outsider win?)
        self.outsider_outcomes.append(1 if has_outsider_winner else 0)

        # Update market accuracy (did lowest-odds horse win?)
        if lowest_odds_horse is not None:
            lowest_won = any(
                r.get("horse_id") == lowest_odds_horse and r.get("is_gagnant")
                for r in runners
            )
            self.accuracy_outcomes.append(1 if lowest_won else 0)

        # Update per-runner stats
        discipline = None
        hippo = None
        for r in runners:
            cote = r.get("cote")
            proba = r.get("proba")
            is_gagnant = bool(r.get("is_gagnant"))
            discipline = r.get("discipline") or discipline
            hippo = r.get("hippo") or hippo

            # Per-bracket calibration
            bracket = _get_bracket(cote)
            if bracket is not None:
                self.bracket_total[bracket] += 1
                if is_gagnant:
                    self.bracket_wins[bracket] += 1
                if proba is not None:
                    self.bracket_expected_sum[bracket] += proba
                elif cote is not None and cote > 0:
                    self.bracket_expected_sum[bracket] += 1.0 / cote

            # Longshot stats
            if cote is not None and cote > _OUTSIDER_THRESHOLD:
                self.longshot_actual.append(1 if is_gagnant else 0)
                if proba is not None:
                    self.longshot_expected.append(proba)
                elif cote > 0:
                    self.longshot_expected.append(1.0 / cote)

        # Per-discipline / per-hippo favourite stats
        if fav_cote is not None:
            if discipline:
                self.disc_fav_total[discipline] += 1
                if fav_won:
                    self.disc_fav_wins[discipline] += 1
            if hippo:
                self.hippo_fav_total[hippo] += 1
                if fav_won:
                    self.hippo_fav_wins[hippo] += 1
            self.overall_fav_total += 1
            if fav_won:
                self.overall_fav_wins += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort+seek)
# ===========================================================================


def build_recalibration_signal_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build recalibration signal features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using seek offsets, process course by
         course, and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Recalibration Signal Builder (memory-optimised) ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
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

    # -- Phase 2: Sort the lightweight index --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    state = _RecalibrationState()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts = {
        "rcl_fav_win_rate_recent": 0,
        "rcl_fav_expected_vs_actual": 0,
        "rcl_outsider_surprise_rate": 0,
        "rcl_market_accuracy_trend": 0,
        "rcl_discipline_bias": 0,
        "rcl_hippo_bias": 0,
        "rcl_odds_bracket_calibration": 0,
        "rcl_recent_longshot_bias": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            cote = _safe_float(rec.get("cote_finale"))
            proba = _safe_float(rec.get("proba_implicite"))
            if proba is None and cote is not None and cote > 0:
                proba = round(1.0 / cote, 6)

            discipline = (rec.get("discipline") or "").strip().upper()
            hippo = (rec.get("hippodrome_normalise") or "").strip().upper()

            return {
                "uid": rec.get("partant_uid"),
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "cote": cote,
                "proba": proba,
                "is_gagnant": bool(rec.get("is_gagnant")),
                "discipline": discipline,
                "hippo": hippo,
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

            # Read only this course's records from disk
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race global signals for all runners --
            # Global signals are the same for all runners in this race
            rcl_fav_wr = state.snapshot_fav_win_rate_recent()
            rcl_fav_exp = state.snapshot_fav_expected_vs_actual()
            rcl_outsider = state.snapshot_outsider_surprise_rate()
            rcl_accuracy = state.snapshot_market_accuracy_trend()
            rcl_longshot = state.snapshot_longshot_bias()

            # Per-runner features depend on discipline, hippo, bracket
            for rec in course_group:
                discipline = rec["discipline"]
                hippo = rec["hippo"]
                cote = rec["cote"]
                bracket = _get_bracket(cote)

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                features["rcl_fav_win_rate_recent"] = rcl_fav_wr
                if rcl_fav_wr is not None:
                    fill_counts["rcl_fav_win_rate_recent"] += 1

                features["rcl_fav_expected_vs_actual"] = rcl_fav_exp
                if rcl_fav_exp is not None:
                    fill_counts["rcl_fav_expected_vs_actual"] += 1

                features["rcl_outsider_surprise_rate"] = rcl_outsider
                if rcl_outsider is not None:
                    fill_counts["rcl_outsider_surprise_rate"] += 1

                features["rcl_market_accuracy_trend"] = rcl_accuracy
                if rcl_accuracy is not None:
                    fill_counts["rcl_market_accuracy_trend"] += 1

                disc_bias = state.snapshot_discipline_bias(discipline)
                features["rcl_discipline_bias"] = disc_bias
                if disc_bias is not None:
                    fill_counts["rcl_discipline_bias"] += 1

                hippo_bias = state.snapshot_hippo_bias(hippo)
                features["rcl_hippo_bias"] = hippo_bias
                if hippo_bias is not None:
                    fill_counts["rcl_hippo_bias"] += 1

                bracket_cal = state.snapshot_bracket_calibration(bracket)
                features["rcl_odds_bracket_calibration"] = bracket_cal
                if bracket_cal is not None:
                    fill_counts["rcl_odds_bracket_calibration"] += 1

                features["rcl_recent_longshot_bias"] = rcl_longshot
                if rcl_longshot is not None:
                    fill_counts["rcl_recent_longshot_bias"] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

            # -- Update global state AFTER computing features for all runners --
            winner_horse = None
            for r in course_group:
                if r.get("is_gagnant"):
                    winner_horse = r.get("horse_id")
                    break

            state.update_race(course_group, winner_horse)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Recalibration signal build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de recalibration signal a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/recalibration_signal/)",
    )
    args = parser.parse_args()

    logger = setup_logging("recalibration_signal_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "recalibration_signal.jsonl"
    build_recalibration_signal_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
