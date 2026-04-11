#!/usr/bin/env python3
"""
feature_builders.jockey_form_momentum_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Tracks jockey momentum and hot/cold streaks using a fixed sliding window
of the last 10 rides, plus career-level statistics and intra-day counters.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant jockey momentum features.

Temporal integrity: for any partant at date D, only races with date < D
(or earlier races on the same date in a different course) contribute to
jockey statistics -- no future leakage. Within the same course all
snapshots are captured before any update.

Produces:
  - jockey_form_momentum.jsonl   in OUTPUT_DIR

Features per partant (prefix: jfm_):
  - jfm_jockey_last10_wr       : jockey win rate in last 10 rides
  - jfm_jockey_last10_place_rate: jockey place rate (top 3) in last 10 rides
  - jfm_jockey_win_streak       : current consecutive wins (0 if last wasn't a win)
  - jfm_jockey_rides_today      : number of rides jockey has today before this race
  - jfm_jockey_wins_today       : number of wins jockey has today before this race
  - jfm_jockey_career_wr        : jockey's overall career win rate
  - jfm_jockey_momentum         : last10_wr - career_wr (positive = hot streak)
  - jfm_jockey_experience       : total number of rides (log-transformed, base e)

Usage:
    python feature_builders/jockey_form_momentum_builder.py
    python feature_builders/jockey_form_momentum_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/jockey_form_momentum_builder.py --output-dir /path/to/output
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

# Fallback candidates when the primary path is not found
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/jockey_form_momentum")

# Rolling window size (number of rides, not days)
_WINDOW_RIDES = 10

# Progress log every N records
_LOG_EVERY = 500_000

# Null feature template (all values None)
_NULL_FEATURES: dict[str, Any] = {
    "jfm_jockey_last10_wr": None,
    "jfm_jockey_last10_place_rate": None,
    "jfm_jockey_win_streak": None,
    "jfm_jockey_rides_today": None,
    "jfm_jockey_wins_today": None,
    "jfm_jockey_career_wr": None,
    "jfm_jockey_momentum": None,
    "jfm_jockey_experience": None,
}

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
    """Parse ISO date string (YYYY-MM-DD...) to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _parse_position(pos_val) -> Optional[int]:
    """Convert position_arrivee to int. Returns None if absent or non-numeric."""
    if pos_val is None:
        return None
    try:
        return int(pos_val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# JOCKEY STATE TRACKER
# ===========================================================================


class _JockeyState:
    """Per-jockey accumulated state for momentum computations.

    State per jockey:
        total_wins         : int   -- career total wins
        total_rides        : int   -- career total rides
        recent             : deque(maxlen=10) of (won: bool, placed: bool)
        current_win_streak : int   -- consecutive wins at end of history
        today_rides        : int   -- rides already completed today
        today_wins         : int   -- wins already completed today
        today_date         : str   -- ISO date string for today's tracking
    """

    __slots__ = (
        "total_wins",
        "total_rides",
        "recent",
        "current_win_streak",
        "today_rides",
        "today_wins",
        "today_date",
    )

    def __init__(self) -> None:
        self.total_wins: int = 0
        self.total_rides: int = 0
        self.recent: deque = deque(maxlen=_WINDOW_RIDES)
        self.current_win_streak: int = 0
        self.today_rides: int = 0
        self.today_wins: int = 0
        self.today_date: str = ""

    def snapshot(self, race_date_str: str) -> dict[str, Any]:
        """Compute features using only history strictly before this race.

        Called BEFORE update() for temporal integrity.
        today_rides / today_wins are already the count for earlier races
        on the same date (updated per-race within a day, not per-course).
        """
        # -- last-10 stats --
        n_recent = len(self.recent)
        if n_recent > 0:
            wins_10 = sum(1 for (w, _p) in self.recent if w)
            placed_10 = sum(1 for (_w, p) in self.recent if p)
            last10_wr = round(wins_10 / n_recent, 4)
            last10_place_rate = round(placed_10 / n_recent, 4)
        else:
            last10_wr = None
            last10_place_rate = None

        # -- career stats --
        if self.total_rides > 0:
            career_wr = round(self.total_wins / self.total_rides, 4)
        else:
            career_wr = None

        # -- momentum: last10_wr - career_wr --
        if last10_wr is not None and career_wr is not None:
            momentum = round(last10_wr - career_wr, 4)
        else:
            momentum = None

        # -- experience: log(total_rides + 1) to compress scale --
        experience = round(math.log(self.total_rides + 1), 4) if self.total_rides >= 0 else None

        # -- intra-day counters (today = race_date_str) --
        if race_date_str and self.today_date == race_date_str:
            rides_today = self.today_rides
            wins_today = self.today_wins
        else:
            # Different date: the jockey hasn't ridden today yet
            rides_today = 0
            wins_today = 0

        return {
            "jfm_jockey_last10_wr": last10_wr,
            "jfm_jockey_last10_place_rate": last10_place_rate,
            "jfm_jockey_win_streak": self.current_win_streak,
            "jfm_jockey_rides_today": rides_today,
            "jfm_jockey_wins_today": wins_today,
            "jfm_jockey_career_wr": career_wr,
            "jfm_jockey_momentum": momentum,
            "jfm_jockey_experience": experience,
        }

    def update(self, race_date_str: str, won: bool, position: Optional[int]) -> None:
        """Record the outcome of a completed race (called post-snapshot)."""
        placed = won or (position is not None and 1 <= position <= 3)

        # -- career totals --
        self.total_rides += 1
        if won:
            self.total_wins += 1

        # -- rolling last-10 --
        self.recent.append((won, placed))

        # -- consecutive win streak --
        if won:
            self.current_win_streak += 1
        else:
            self.current_win_streak = 0

        # -- intra-day counters --
        if race_date_str and self.today_date != race_date_str:
            # New day: reset counters
            self.today_date = race_date_str
            self.today_rides = 0
            self.today_wins = 0

        self.today_rides += 1
        if won:
            self.today_wins += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_jockey_form_momentum_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build jockey momentum features from partants_master.jsonl.

    Strategy (index + sort + seek):
      1. Read minimal fields into memory.
      2. Sort chronologically (date, course_uid, num_pmu).
      3. Iterate course-by-course:
         a. Snapshot features for ALL partants in the course (pre-race).
         b. Update jockey states for ALL partants in the course (post-race).

    Intra-day today_rides/today_wins are updated per individual partant
    within a day (not per-course), so earlier races on the same date
    contribute to later races' intra-day counters correctly.
    """
    logger.info("=== Jockey Form Momentum Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Support both field names for jockey
        jockey = rec.get("jockey") or rec.get("nom_jockey")

        # Determine winner flag
        pos_raw = rec.get("position_arrivee")
        position = _parse_position(pos_raw)
        is_winner = bool(rec.get("is_gagnant")) or position == 1

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "jockey": jockey,
            "won": is_winner,
            "position": position,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    jockey_states: dict[str, _JockeyState] = defaultdict(_JockeyState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants in this course
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

        # -- Step a: Snapshot pre-race features for every partant --
        for rec in course_group:
            jockey = rec["jockey"]
            if jockey:
                feats = jockey_states[jockey].snapshot(course_date_str)
            else:
                feats = dict(_NULL_FEATURES)

            feats["partant_uid"] = rec["uid"]
            results.append(feats)

        # -- Step b: Update jockey states (post-race, preserves temporal order) --
        for rec in course_group:
            jockey = rec["jockey"]
            if jockey:
                jockey_states[jockey].update(
                    course_date_str, rec["won"], rec["position"]
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Jockey momentum build termine: %d features en %.1fs (jockeys: %d)",
        len(results),
        elapsed,
        len(jockey_states),
    )

    # Free memory explicitly before returning
    del slim_records
    gc.collect()

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path from CLI arg or candidate list."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve parmi: "
        + str([str(c) for c in _INPUT_CANDIDATES])
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features de momentum jockey (last-10 rides) "
        "a partir de partants_master.jsonl"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: OUTPUT_DIR)",
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_form_momentum_builder")

    try:
        input_path = _find_input(args.input)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_jockey_form_momentum_features(input_path, logger)

    # Save output
    out_path = output_dir / "jockey_form_momentum.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info(
                "  %s: %d/%d (%.1f%%)", k, v, total_count, 100.0 * v / total_count
            )

    logger.info("Termine. Sortie: %s", out_path)


if __name__ == "__main__":
    main()
