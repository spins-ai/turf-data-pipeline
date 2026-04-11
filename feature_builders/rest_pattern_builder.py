#!/usr/bin/env python3
"""
feature_builders.rest_pattern_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Rest and recovery pattern features for optimal rest detection.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant rest pattern features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - rest_pattern.jsonl   in output/rest_pattern/

Features per partant (8):
  - rp_days_rest            : days since last race (nuanced, uses exact ISO date diff)
  - rp_rest_bucket          : 0=fresh(<7d), 1=short(7-14d), 2=medium(14-30d),
                              3=long(30-60d), 4=layoff(>60d)
  - rp_optimal_rest_win_rate: horse's historical win rate at this rest bucket
  - rp_horse_best_rest_bucket: rest bucket where horse has its highest win rate
  - rp_rest_match           : 1 if current rest bucket matches horse's optimal
  - rp_avg_rest_days        : horse's average rest period between races (historical)
  - rp_rest_regularity      : 1 - coefficient_of_variation of rest days
                              (high = regular racing pattern)
  - rp_freshness_after_win  : 1 if last race was a win AND rest < 21 days

Usage:
    python feature_builders/rest_pattern_builder.py
    python feature_builders/rest_pattern_builder.py --input data_master/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/rest_pattern")

_LOG_EVERY = 500_000

# Rest bucket boundaries (days)
_BUCKET_FRESH = 7       # 0: fresh  < 7 days
_BUCKET_SHORT = 14      # 1: short  7-14 days
_BUCKET_MEDIUM = 30     # 2: medium 14-30 days
_BUCKET_LONG = 60       # 3: long   30-60 days
# bucket 4: layoff > 60 days

_FRESHNESS_WIN_THRESHOLD = 21   # days, for rp_freshness_after_win


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


def _parse_date_to_ordinal(date_str: Optional[str]) -> Optional[int]:
    """Parse YYYY-MM-DD string to integer ordinal (days since epoch) for arithmetic."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        y = int(date_str[0:4])
        m = int(date_str[5:7])
        d = int(date_str[8:10])
        # Use datetime.toordinal for correct calendar arithmetic
        return datetime(y, m, d).toordinal()
    except (ValueError, IndexError):
        return None


def _days_rest_to_bucket(days: int) -> int:
    """Convert rest days to rest bucket integer (0-4)."""
    if days < _BUCKET_FRESH:
        return 0
    if days < _BUCKET_SHORT:
        return 1
    if days < _BUCKET_MEDIUM:
        return 2
    if days < _BUCKET_LONG:
        return 3
    return 4


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
        return v if v == v else None  # NaN guard
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseRestState:
    """Per-horse accumulated state for rest pattern features.

    State updated AFTER features are snapshotted, so no future leakage.

    Fields:
      last_race_ord  : ordinal date of the most recent race (or None)
      last_was_win   : True if the most recent race resulted in position == 1
      rest_days_hist : deque of last 20 rest-day intervals between consecutive races
      per_bucket_wins: dict[bucket_int -> int] count of wins at that bucket
      per_bucket_total: dict[bucket_int -> int] count of races at that bucket
    """

    __slots__ = (
        "last_race_ord",
        "last_was_win",
        "rest_days_hist",
        "per_bucket_wins",
        "per_bucket_total",
    )

    def __init__(self) -> None:
        self.last_race_ord: Optional[int] = None
        self.last_was_win: bool = False
        self.rest_days_hist: deque = deque(maxlen=20)
        self.per_bucket_wins: dict[int, int] = defaultdict(int)
        self.per_bucket_total: dict[int, int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Snapshot: compute features using ONLY past data
    # ------------------------------------------------------------------

    def snapshot(self, current_ord: Optional[int]) -> dict[str, Any]:
        """Return all 8 rest-pattern features for the upcoming race.

        Only past races (before current_ord) are used. No leakage.
        """
        # Default nulls
        rp_days_rest: Optional[int] = None
        rp_rest_bucket: Optional[int] = None
        rp_optimal_rest_win_rate: Optional[float] = None
        rp_horse_best_rest_bucket: Optional[int] = None
        rp_rest_match: Optional[int] = None
        rp_avg_rest_days: Optional[float] = None
        rp_rest_regularity: Optional[float] = None
        rp_freshness_after_win: int = 0

        # --- Days rest & bucket ---
        if self.last_race_ord is not None and current_ord is not None:
            days = current_ord - self.last_race_ord
            if days < 0:
                days = 0
            rp_days_rest = days
            rp_rest_bucket = _days_rest_to_bucket(days)

        # --- Bucket-based win rate ---
        if rp_rest_bucket is not None:
            total_at_bucket = self.per_bucket_total.get(rp_rest_bucket, 0)
            if total_at_bucket > 0:
                wins_at_bucket = self.per_bucket_wins.get(rp_rest_bucket, 0)
                rp_optimal_rest_win_rate = round(wins_at_bucket / total_at_bucket, 4)

        # --- Best bucket & rest match ---
        best_bucket: Optional[int] = None
        best_rate: float = -1.0
        for bucket in range(5):
            total = self.per_bucket_total.get(bucket, 0)
            if total > 0:
                rate = self.per_bucket_wins.get(bucket, 0) / total
                if rate > best_rate:
                    best_rate = rate
                    best_bucket = bucket

        rp_horse_best_rest_bucket = best_bucket
        if best_bucket is not None and rp_rest_bucket is not None:
            rp_rest_match = 1 if rp_rest_bucket == best_bucket else 0

        # --- Average rest days ---
        if len(self.rest_days_hist) > 0:
            hist = list(self.rest_days_hist)
            n = len(hist)
            mean = sum(hist) / n
            rp_avg_rest_days = round(mean, 2)

            # --- Rest regularity: 1 - CV ---
            if n >= 2 and mean > 0:
                variance = sum((x - mean) ** 2 for x in hist) / n
                std = math.sqrt(variance)
                cv = std / mean
                regularity = max(0.0, 1.0 - cv)
                rp_rest_regularity = round(regularity, 4)
            elif n == 1:
                # Single interval: perfectly regular by definition
                rp_rest_regularity = 1.0

        # --- Freshness after win ---
        if (
            self.last_was_win
            and rp_days_rest is not None
            and rp_days_rest < _FRESHNESS_WIN_THRESHOLD
        ):
            rp_freshness_after_win = 1

        return {
            "rp_days_rest": rp_days_rest,
            "rp_rest_bucket": rp_rest_bucket,
            "rp_optimal_rest_win_rate": rp_optimal_rest_win_rate,
            "rp_horse_best_rest_bucket": rp_horse_best_rest_bucket,
            "rp_rest_match": rp_rest_match,
            "rp_avg_rest_days": rp_avg_rest_days,
            "rp_rest_regularity": rp_rest_regularity,
            "rp_freshness_after_win": rp_freshness_after_win,
        }

    # ------------------------------------------------------------------
    # Update: record result of the race just snapshotted
    # ------------------------------------------------------------------

    def update(self, race_ord: Optional[int], position: Optional[int]) -> None:
        """Update state after a race is processed.

        Args:
            race_ord: ordinal date of the race just run.
            position: finishing position (1 = win, None if unknown).
        """
        is_win = position == 1

        # Record rest days interval if we have a previous race
        if self.last_race_ord is not None and race_ord is not None:
            days = race_ord - self.last_race_ord
            if days >= 0:
                # Update per-bucket stats for this rest interval
                bucket = _days_rest_to_bucket(days)
                self.per_bucket_total[bucket] += 1
                if is_win:
                    self.per_bucket_wins[bucket] += 1
                # Record interval in history
                self.rest_days_hist.append(days)

        # Advance state
        if race_ord is not None:
            self.last_race_ord = race_ord
        self.last_was_win = is_win


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_rest_pattern_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build rest pattern features from partants_master.jsonl."""
    logger.info("=== Rest Pattern Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        date_str = rec.get("date_reunion_iso") or ""
        date_ord = _parse_date_to_ordinal(date_str)
        position = _safe_int(rec.get("position_arrivee"))

        # Prefer horse_id if available, fall back to nom_cheval
        horse_id = rec.get("horse_id") or rec.get("nom_cheval")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": date_str,
            "date_ord": date_ord,
            "course": rec.get("course_uid", ""),
            "num": _safe_int(rec.get("num_pmu")) or 0,
            "cheval": horse_id,
            "position": position,
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
    horse_states: dict[str, _HorseRestState] = defaultdict(_HorseRestState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by (date, course_uid) for temporal integrity within a race card
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

        # -- Snapshot pre-race features BEFORE update --
        for rec in course_group:
            cheval = rec["cheval"]
            date_ord = rec["date_ord"]

            # Base feature dict with nulls
            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "rp_days_rest": None,
                "rp_rest_bucket": None,
                "rp_optimal_rest_win_rate": None,
                "rp_horse_best_rest_bucket": None,
                "rp_rest_match": None,
                "rp_avg_rest_days": None,
                "rp_rest_regularity": None,
                "rp_freshness_after_win": 0,
            }

            if cheval:
                state = horse_states[cheval]
                snap = state.snapshot(date_ord)
                features.update(snap)

            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            cheval = rec["cheval"]
            if cheval:
                horse_states[cheval].update(rec["date_ord"], rec["position"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Rest pattern build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results), elapsed, len(horse_states),
    )

    # Free memory
    del slim_records
    del horse_states
    gc.collect()

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path: CLI override, then candidate list."""
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
        description="Construction des features rest pattern a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/rest_pattern/)",
    )
    args = parser.parse_args()

    logger = setup_logging("rest_pattern_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_rest_pattern_features(input_path, logger)

    # Save
    out_path = output_dir / "rest_pattern.jsonl"
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
