#!/usr/bin/env python3
"""
feature_builders.jockey_form_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Evaluates recent jockey form / momentum using rolling time windows.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant jockey-form features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the jockey statistics -- no future leakage.

Produces:
  - jockey_form.jsonl   in output/jockey_form/

Features per partant:
  - jockey_win_rate_30j   : jockey win rate in last 30 days
  - jockey_win_rate_90j   : jockey win rate in last 90 days
  - jockey_rides_30j      : number of rides in last 30 days
  - jockey_hot_streak     : consecutive wins (0 if last lost)
  - jockey_roi_30j        : ROI backing jockey in last 30 days
  - jockey_form_trend     : win_rate_30j / win_rate_90j (>1 = improving)

Usage:
    python feature_builders/jockey_form_builder.py
    python feature_builders/jockey_form_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "jockey_form"

# Time windows (days)
WINDOW_SHORT = 30
WINDOW_LONG = 90

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


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# JOCKEY HISTORY TRACKER
# ===========================================================================


class _JockeyRecord:
    """A single past race result for a jockey."""

    __slots__ = ("date", "won", "odds")

    def __init__(self, date: datetime, won: bool, odds: Optional[float]) -> None:
        self.date = date
        self.won = won
        self.odds = odds


class _JockeyState:
    """Per-jockey accumulated state for rolling window computations."""

    __slots__ = ("history", "hot_streak")

    def __init__(self) -> None:
        # history is kept sorted chronologically (appended in order)
        self.history: list[_JockeyRecord] = []
        # Consecutive wins ending at most recent race; 0 if last race was a loss
        self.hot_streak: int = 0

    def snapshot(self, race_date: datetime) -> dict[str, Any]:
        """Compute features using only races strictly before race_date."""
        cutoff_30 = race_date - timedelta(days=WINDOW_SHORT)
        cutoff_90 = race_date - timedelta(days=WINDOW_LONG)

        wins_30 = 0
        total_30 = 0
        roi_sum_30 = 0.0  # sum of (odds - 1) for wins, -1 for losses
        roi_count_30 = 0

        wins_90 = 0
        total_90 = 0

        for rec in self.history:
            # Strict temporal: only races before current race date
            if rec.date >= race_date:
                break

            if rec.date >= cutoff_90:
                total_90 += 1
                if rec.won:
                    wins_90 += 1

                if rec.date >= cutoff_30:
                    total_30 += 1
                    if rec.won:
                        wins_30 += 1

                    # ROI: if we have odds, calculate profit/loss per unit staked
                    if rec.odds is not None and rec.odds > 0:
                        roi_count_30 += 1
                        if rec.won:
                            roi_sum_30 += rec.odds - 1.0
                        else:
                            roi_sum_30 -= 1.0

        # Win rates
        wr_30 = round(wins_30 / total_30, 4) if total_30 > 0 else None
        wr_90 = round(wins_90 / total_90, 4) if total_90 > 0 else None

        # Rides count
        rides_30 = total_30

        # ROI (profit / total staked, as a ratio)
        roi_30 = round(roi_sum_30 / roi_count_30, 4) if roi_count_30 > 0 else None

        # Form trend: wr_30 / wr_90
        if wr_30 is not None and wr_90 is not None and wr_90 > 0:
            form_trend = round(wr_30 / wr_90, 4)
        else:
            form_trend = None

        return {
            "jockey_win_rate_30j": wr_30,
            "jockey_win_rate_90j": wr_90,
            "jockey_rides_30j": rides_30,
            "jockey_hot_streak": self.hot_streak,
            "jockey_roi_30j": roi_30,
            "jockey_form_trend": form_trend,
        }

    def update(self, race_date: datetime, won: bool, odds: Optional[float]) -> None:
        """Add a race result to the jockey's history (post-race)."""
        self.history.append(_JockeyRecord(race_date, won, odds))

        # Update hot streak
        if won:
            self.hot_streak += 1
        else:
            self.hot_streak = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_jockey_form_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build jockey form features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory for sorting.
      2. Sort chronologically.
      3. Process record-by-record, snapshotting before update.
    """
    logger.info("=== Jockey Form Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Use nom_jockey field as specified
        jockey = rec.get("nom_jockey")

        # Extract odds for ROI calculation
        odds_val = rec.get("rapport_simple_gagnant")
        if odds_val is not None:
            try:
                odds_val = float(odds_val)
                if odds_val <= 0:
                    odds_val = None
            except (ValueError, TypeError):
                odds_val = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "jockey": jockey,
            "gagnant": bool(rec.get("is_gagnant")),
            "odds": odds_val,
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
    jockey_states: dict[str, _JockeyState] = defaultdict(_JockeyState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by (date, course) to handle all partants in a course together
    # This ensures snapshot is taken before any update within the same course
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

        race_date = _parse_date(course_date_str)

        # -- Snapshot pre-race features for all partants in this course --
        for rec in course_group:
            jockey = rec["jockey"]

            if jockey and race_date:
                features = jockey_states[jockey].snapshot(race_date)
            else:
                features = {
                    "jockey_win_rate_30j": None,
                    "jockey_win_rate_90j": None,
                    "jockey_rides_30j": None,
                    "jockey_hot_streak": None,
                    "jockey_roi_30j": None,
                    "jockey_form_trend": None,
                }

            features["partant_uid"] = rec["uid"]
            results.append(features)

        # -- Update jockey states after snapshotting (post-race) --
        for rec in course_group:
            jockey = rec["jockey"]
            if jockey and race_date:
                jockey_states[jockey].update(
                    race_date, rec["gagnant"], rec["odds"]
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Jockey form build termine: %d features en %.1fs (jockeys: %d)",
        len(results), elapsed, len(jockey_states),
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
        description="Construction des features de forme jockey a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/jockey_form/)",
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_form_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_jockey_form_features(input_path, logger)

    # Save
    out_path = output_dir / "jockey_form.jsonl"
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
