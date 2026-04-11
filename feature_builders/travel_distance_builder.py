#!/usr/bin/env python3
"""
feature_builders.travel_distance_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Estimates travel distance / hippodrome familiarity for horses racing at
different tracks.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant hippodrome-travel features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the hippodrome statistics -- no future leakage.

Produces:
  - travel_distance_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/travel_distance/

Features per partant (prefix tvl_):
  - tvl_hippo_visits       : times this horse has raced at this hippodrome
  - tvl_hippo_win_rate     : win rate at this specific hippodrome
  - tvl_hippo_place_rate   : top-3 rate at this hippodrome
  - tvl_is_home_track      : 1 if this is the horse's most-visited hippodrome
  - tvl_hippo_diversity    : number of different hippodromes the horse has raced at
  - tvl_last_visit_days    : days since last race at this hippodrome
  - tvl_hippo_avg_position : average finishing position at this hippodrome
  - tvl_new_hippo          : 1 if horse has never raced at this hippodrome before

Usage:
    python feature_builders/travel_distance_builder.py
    python feature_builders/travel_distance_builder.py \\
        --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_DEFAULT_INPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
_DEFAULT_OUTPUT = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/travel_distance"
    "/travel_distance_features.jsonl"
)

# Progress / GC cadence
_LOG_EVERY = 500_000


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
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
# DATE HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[date]:
    """Parse ISO date string (YYYY-MM-DD) to date. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _days_between(a: Optional[date], b: Optional[date]) -> Optional[int]:
    """Return (b - a).days, or None if either is None."""
    if a is None or b is None:
        return None
    return (b - a).days


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_str(val) -> str:
    """Normalise a hippodrome name: strip, lower."""
    if not val:
        return ""
    return str(val).strip().lower()


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HippoStats:
    """Per-(horse, hippodrome) accumulated statistics."""

    __slots__ = ("visits", "wins", "places", "total", "last_date", "sum_positions")

    def __init__(self) -> None:
        self.visits: int = 0          # races at this hippodrome
        self.wins: int = 0            # position == 1
        self.places: int = 0          # position <= 3
        self.total: int = 0           # races with a valid position
        self.last_date: Optional[date] = None
        self.sum_positions: float = 0.0


class _HorseState:
    """All hippodrome history for a single horse."""

    __slots__ = ("hippo_stats",)

    def __init__(self) -> None:
        # hippodrome_name -> _HippoStats
        self.hippo_stats: dict[str, _HippoStats] = {}

    # ------------------------------------------------------------------
    # SNAPSHOT  (compute features using only past data — BEFORE update)
    # ------------------------------------------------------------------

    def snapshot(self, hippo: str, race_date: date) -> dict[str, Any]:
        """Return feature dict for one partant (pre-race state)."""
        hs = self.hippo_stats.get(hippo)

        # tvl_hippo_visits
        visits = hs.visits if hs is not None else 0

        # tvl_new_hippo
        new_hippo = 0 if visits > 0 else 1

        # tvl_hippo_win_rate
        if hs is not None and hs.total > 0:
            hippo_win_rate: Optional[float] = round(hs.wins / hs.total, 4)
        else:
            hippo_win_rate = None

        # tvl_hippo_place_rate
        if hs is not None and hs.total > 0:
            hippo_place_rate: Optional[float] = round(hs.places / hs.total, 4)
        else:
            hippo_place_rate = None

        # tvl_hippo_avg_position
        if hs is not None and hs.total > 0:
            hippo_avg_position: Optional[float] = round(hs.sum_positions / hs.total, 2)
        else:
            hippo_avg_position = None

        # tvl_last_visit_days
        if hs is not None and hs.last_date is not None:
            last_visit_days: Optional[int] = _days_between(hs.last_date, race_date)
        else:
            last_visit_days = None

        # tvl_is_home_track  (most-visited hippodrome)
        if self.hippo_stats:
            home = max(self.hippo_stats, key=lambda h: self.hippo_stats[h].visits)
            is_home_track: Optional[int] = 1 if home == hippo else 0
        else:
            is_home_track = None  # no history at all

        # tvl_hippo_diversity  (distinct hippodromes visited)
        hippo_diversity: int = len(self.hippo_stats)

        return {
            "tvl_hippo_visits": visits,
            "tvl_hippo_win_rate": hippo_win_rate,
            "tvl_hippo_place_rate": hippo_place_rate,
            "tvl_is_home_track": is_home_track,
            "tvl_hippo_diversity": hippo_diversity,
            "tvl_last_visit_days": last_visit_days,
            "tvl_hippo_avg_position": hippo_avg_position,
            "tvl_new_hippo": new_hippo,
        }

    # ------------------------------------------------------------------
    # UPDATE  (record results AFTER snapshot)
    # ------------------------------------------------------------------

    def update(self, hippo: str, race_date: date, position: Optional[int]) -> None:
        """Incorporate one race result into the horse's state."""
        if hippo not in self.hippo_stats:
            self.hippo_stats[hippo] = _HippoStats()

        hs = self.hippo_stats[hippo]
        hs.visits += 1
        hs.last_date = race_date

        if position is not None and position > 0:
            hs.total += 1
            hs.sum_positions += position
            if position == 1:
                hs.wins += 1
            if position <= 3:
                hs.places += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_travel_distance_features(input_path: Path, output_path: Path, logger) -> None:
    """Build travel/hippodrome familiarity features from partants_master.jsonl.

    Uses the index+sort+seek pattern:
      1. Stream minimal fields into memory.
      2. Sort chronologically.
      3. Process course by course; snapshot before update.

    Writes to a .tmp file then renames atomically.
    """
    logger.info("=== Travel Distance Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -----------------------------------------------------------------------
    # Phase 1 — Stream minimal fields into memory
    # -----------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        hippo_raw = rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval") or rec.get("horse_id") or "",
            "hippo": _safe_str(hippo_raw),
            "position": _safe_int(rec.get("position_arrivee")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -----------------------------------------------------------------------
    # Phase 2 — Sort chronologically by (date, course_uid, num_pmu)
    # -----------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -----------------------------------------------------------------------
    # Phase 3 — Process course by course; snapshot BEFORE update
    # -----------------------------------------------------------------------
    t2 = time.time()

    # horse_name -> _HorseState
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)

    # Prepare output file (atomic write: .tmp then rename)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    i = 0
    total = len(slim_records)

    with open(tmp_path, "w", encoding="utf-8") as out_fh:
        while i < total:
            course_uid = slim_records[i]["course"]
            course_date_str = slim_records[i]["date"]
            course_group: list[dict] = []

            # Collect all partants in this course
            while (
                i < total
                and slim_records[i]["course"] == course_uid
                and slim_records[i]["date"] == course_date_str
            ):
                course_group.append(slim_records[i])
                i += 1

            race_date = _parse_date(course_date_str)

            # -- Snapshot pre-race (BEFORE any update for this course) --
            for rec in course_group:
                cheval = rec["cheval"]
                hippo = rec["hippo"]

                if cheval and hippo and race_date is not None:
                    feats = horse_states[cheval].snapshot(hippo, race_date)
                else:
                    feats = {
                        "tvl_hippo_visits": 0 if not cheval else None,
                        "tvl_hippo_win_rate": None,
                        "tvl_hippo_place_rate": None,
                        "tvl_is_home_track": None,
                        "tvl_hippo_diversity": 0,
                        "tvl_last_visit_days": None,
                        "tvl_hippo_avg_position": None,
                        "tvl_new_hippo": 1 if not hippo else None,
                    }

                row: dict[str, Any] = {"partant_uid": rec["uid"]}
                row.update(feats)
                out_fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                n_written += 1

            # -- Update state after snapshotting (post-race) --
            for rec in course_group:
                cheval = rec["cheval"]
                hippo = rec["hippo"]
                if cheval and hippo and race_date is not None:
                    horse_states[cheval].update(hippo, race_date, rec["position"])

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic rename
    if output_path.exists():
        output_path.unlink()
    os.rename(tmp_path, output_path)

    elapsed = time.time() - t0
    logger.info(
        "Travel distance build termine: %d features ecrits en %.1fs "
        "(chevaux uniques: %d)",
        n_written,
        elapsed,
        len(horse_states),
    )
    logger.info("Fichier de sortie: %s", output_path)

    # -----------------------------------------------------------------------
    # Fill rates (re-read the output file to compute)
    # -----------------------------------------------------------------------
    _log_fill_rates(output_path, logger)


# ===========================================================================
# FILL RATE REPORTING
# ===========================================================================

_FEATURE_KEYS = [
    "tvl_hippo_visits",
    "tvl_hippo_win_rate",
    "tvl_hippo_place_rate",
    "tvl_is_home_track",
    "tvl_hippo_diversity",
    "tvl_last_visit_days",
    "tvl_hippo_avg_position",
    "tvl_new_hippo",
]


def _log_fill_rates(output_path: Path, logger) -> None:
    """Read the output JSONL and log fill rates for each feature."""
    filled: dict[str, int] = {k: 0 for k in _FEATURE_KEYS}
    total = 0

    try:
        with open(output_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total += 1
                for k in _FEATURE_KEYS:
                    if row.get(k) is not None:
                        filled[k] += 1
    except OSError as exc:
        logger.warning("Impossible de lire le fichier de sortie pour fill rates: %s", exc)
        return

    if total == 0:
        logger.warning("Aucun record dans le fichier de sortie.")
        return

    logger.info("=== Fill rates (%d records) ===", total)
    for k in _FEATURE_KEYS:
        n = filled[k]
        pct = 100.0 * n / total
        logger.info("  %-30s %d/%d (%.1f%%)", k, n, total, pct)


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features de familiarite hippodrome / travel distance "
            "a partir de partants_master"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help=(
            "Chemin vers partants_master.jsonl "
            f"(defaut: {_DEFAULT_INPUT})"
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=(
            "Chemin du fichier de sortie JSONL "
            f"(defaut: {_DEFAULT_OUTPUT})"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("travel_distance_builder")

    # Resolve input
    if args.input:
        input_path = Path(args.input)
    else:
        input_path = _DEFAULT_INPUT

    if not input_path.exists():
        logger.error("Fichier d'entree introuvable: %s", input_path)
        sys.exit(1)

    # Resolve output
    output_path = Path(args.output) if args.output else _DEFAULT_OUTPUT

    build_travel_distance_features(input_path, output_path, logger)


if __name__ == "__main__":
    main()
