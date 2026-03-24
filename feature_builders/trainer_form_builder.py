#!/usr/bin/env python3
"""
feature_builders.trainer_form_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Evaluates recent trainer form / momentum using rolling time windows.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant trainer-form features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the trainer statistics — no future leakage.

Produces:
  - trainer_form.jsonl   in output/trainer_form/

Features per partant:
  - trainer_win_rate_30j   : trainer win rate in last 30 days
  - trainer_win_rate_90j   : trainer win rate in last 90 days
  - trainer_runners_30j    : number of runners trained in last 30 days
  - trainer_hot_streak     : consecutive wins by trainer's horses (0 if last lost)
  - trainer_roi_30j        : ROI of backing all trainer's horses in last 30 days
  - trainer_form_trend     : win_rate_30j / win_rate_90j (>1 = improving form)

Usage:
    python feature_builders/trainer_form_builder.py
    python feature_builders/trainer_form_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "trainer_form"

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
# TRAINER HISTORY TRACKER
# ===========================================================================


class _TrainerRecord:
    """A single past race result for a trainer."""

    __slots__ = ("date", "won", "odds")

    def __init__(self, date: datetime, won: bool, odds: Optional[float]) -> None:
        self.date = date
        self.won = won
        self.odds = odds


class _TrainerState:
    """Per-trainer accumulated state for rolling window computations."""

    __slots__ = ("history", "hot_streak")

    def __init__(self) -> None:
        # history is kept sorted chronologically (appended in order)
        self.history: list[_TrainerRecord] = []
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

        # Runners count
        runners_30 = total_30

        # ROI (profit / total staked, as a ratio)
        roi_30 = round(roi_sum_30 / roi_count_30, 4) if roi_count_30 > 0 else None

        # Form trend: wr_30 / wr_90
        if wr_30 is not None and wr_90 is not None and wr_90 > 0:
            form_trend = round(wr_30 / wr_90, 4)
        else:
            form_trend = None

        return {
            "trainer_win_rate_30j": wr_30,
            "trainer_win_rate_90j": wr_90,
            "trainer_runners_30j": runners_30,
            "trainer_hot_streak": self.hot_streak,
            "trainer_roi_30j": roi_30,
            "trainer_form_trend": form_trend,
        }

    def update(self, race_date: datetime, won: bool, odds: Optional[float]) -> None:
        """Add a race result to the trainer's history (post-race)."""
        self.history.append(_TrainerRecord(race_date, won, odds))

        # Update hot streak
        if won:
            self.hot_streak += 1
        else:
            self.hot_streak = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_trainer_form_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build trainer form features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory for sorting.
      2. Sort chronologically.
      3. Process record-by-record, snapshotting before update.

    Memory budget:
      - Slim records: ~16M records * ~180 bytes = ~2.9 GB
      - Trainer states: history lists grow but are bounded by data
    """
    logger.info("=== Trainer Form Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields into memory ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Handle both field names for trainer
        entraineur = rec.get("nom_entraineur") or rec.get("entraineur")

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
            "entraineur": entraineur,
            "gagnant": bool(rec.get("is_gagnant")),
            "odds": odds_val,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process record by record ──
    t2 = time.time()
    trainer_states: dict[str, _TrainerState] = defaultdict(_TrainerState)
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

        # ── Snapshot pre-race features for all partants in this course ──
        for rec in course_group:
            trainer = rec["entraineur"]

            if trainer and race_date:
                features = trainer_states[trainer].snapshot(race_date)
            else:
                features = {
                    "trainer_win_rate_30j": None,
                    "trainer_win_rate_90j": None,
                    "trainer_runners_30j": None,
                    "trainer_hot_streak": None,
                    "trainer_roi_30j": None,
                    "trainer_form_trend": None,
                }

            features["partant_uid"] = rec["uid"]
            results.append(features)

        # ── Update trainer states after snapshotting (post-race) ──
        for rec in course_group:
            trainer = rec["entraineur"]
            if trainer and race_date:
                trainer_states[trainer].update(
                    race_date, rec["gagnant"], rec["odds"]
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Trainer form build termine: %d features en %.1fs (entraineurs: %d)",
        len(results), elapsed, len(trainer_states),
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
        description="Construction des features de forme entraineur a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/trainer_form/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_form_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_trainer_form_features(input_path, logger)

    # Save
    out_path = output_dir / "trainer_form.jsonl"
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
