#!/usr/bin/env python3
"""
feature_builders.layoff_return_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Analyzes horses returning from extended layoffs (>60 days between races).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant layoff/return features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - layoff_return.jsonl   in output/layoff_return/

Features per partant (8):
  - lr_is_layoff              : 1 if >60 days since last race (or first race ever)
  - lr_layoff_days            : exact days since last race (None if first race)
  - lr_layoff_bucket          : 0=active(<30d), 1=short(30-60d), 2=medium(60-120d),
                                3=long(120-365d), 4=very_long(>365d)
  - lr_horse_layoff_wr        : horse's historical win rate after layoffs (>60d)
  - lr_horse_fresh_wr         : horse's win rate when active (<30d rest)
  - lr_layoff_vs_fresh        : lr_horse_layoff_wr - lr_horse_fresh_wr
                                (positive = better after layoff than when active)
  - lr_prev_result_before_layoff : position_pct of last race before the layoff
                                    (was horse injured/fatigued? None if no prior layoff)
  - lr_nb_layoffs_career      : total number of layoffs (>60d) in career so far

Usage:
    python feature_builders/layoff_return_builder.py
    python feature_builders/layoff_return_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/layoff_return")
OUTPUT_DIR_FALLBACK = _PROJECT_ROOT / "output" / "layoff_return"

_LOG_EVERY = 500_000

# Layoff thresholds (days)
_THRESHOLD_LAYOFF = 60      # >60d = layoff
_THRESHOLD_ACTIVE = 30      # <30d = active/fresh

# Layoff bucket boundaries
_BUCKET_ACTIVE = 0      # < 30 days
_BUCKET_SHORT = 1       # 30-60 days
_BUCKET_MEDIUM = 2      # 60-120 days
_BUCKET_LONG = 3        # 120-365 days
_BUCKET_VERY_LONG = 4   # > 365 days


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


def _parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse YYYY-MM-DD date string to datetime object."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _date_to_sortkey(date_str: Optional[str]) -> str:
    """Return date string for sorting; empty string sorts first."""
    return date_str[:10] if date_str and len(date_str) >= 10 else ""


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
        return v if v == v else None  # guard NaN
    except (ValueError, TypeError):
        return None


def _layoff_bucket(days: Optional[int]) -> Optional[int]:
    """
    Classify rest period into bucket:
      0 = active     (< 30d)
      1 = short      (30-60d)
      2 = medium     (60-120d)
      3 = long       (120-365d)
      4 = very_long  (> 365d)
    Returns None if days is None (first race ever).
    """
    if days is None:
        return None
    if days < 30:
        return _BUCKET_ACTIVE
    if days < 60:
        return _BUCKET_SHORT
    if days < 120:
        return _BUCKET_MEDIUM
    if days <= 365:
        return _BUCKET_LONG
    return _BUCKET_VERY_LONG


def _safe_div(num: int, den: int) -> Optional[float]:
    """Return num/den rounded to 4 decimals, or None if den==0."""
    if den == 0:
        return None
    return round(num / den, 4)


def _position_pct(position: Optional[int], nb_partants: Optional[int]) -> Optional[float]:
    """
    Compute position percentile: 1.0 = winner, 0.0 = last.
    Returns None if either value is missing or invalid.
    """
    pos = _safe_int(position)
    nb = _safe_int(nb_partants)
    if pos is None or nb is None or nb <= 0 or pos <= 0:
        return None
    if nb == 1:
        return 1.0
    pct = 1.0 - (pos - 1) / (nb - 1)
    return round(max(0.0, min(1.0, pct)), 4)


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseLayoffState:
    """
    Per-horse accumulated state for layoff/return features.

    State fields:
      last_race_date      : datetime of the most recent completed race
      last_position_pct   : position_pct of the most recent completed race
      wins_after_layoff   : cumulative wins when returning from layoff (>60d)
      total_after_layoff  : cumulative starts when returning from layoff
      wins_fresh          : cumulative wins when active (<30d rest)
      total_fresh         : cumulative starts when active
      nb_layoffs          : total number of layoffs (>60d) encountered so far
      last_pos_before_layoff : position_pct of the race just before the last layoff began
                               (updated when we detect a new layoff)
    """

    __slots__ = (
        "last_race_date",
        "last_position_pct",
        "wins_after_layoff",
        "total_after_layoff",
        "wins_fresh",
        "total_fresh",
        "nb_layoffs",
        "last_pos_before_layoff",
    )

    def __init__(self) -> None:
        self.last_race_date: Optional[datetime] = None
        self.last_position_pct: Optional[float] = None
        self.wins_after_layoff: int = 0
        self.total_after_layoff: int = 0
        self.wins_fresh: int = 0
        self.total_fresh: int = 0
        self.nb_layoffs: int = 0
        self.last_pos_before_layoff: Optional[float] = None

    def snapshot(self, current_date: Optional[datetime]) -> dict[str, Any]:
        """
        Compute layoff features using only past races (strict temporal integrity).
        This is called BEFORE the current race is recorded.
        """
        # Days since last race
        layoff_days: Optional[int] = None
        if self.last_race_date is not None and current_date is not None:
            delta = current_date - self.last_race_date
            layoff_days = max(0, delta.days)

        # lr_is_layoff: 1 if first race ever OR >60 days since last race
        if self.last_race_date is None:
            is_layoff = 1
        else:
            is_layoff = 1 if (layoff_days is not None and layoff_days > _THRESHOLD_LAYOFF) else 0

        # lr_layoff_bucket
        if self.last_race_date is None:
            bucket = None
        else:
            bucket = _layoff_bucket(layoff_days)

        # lr_horse_layoff_wr: win rate in races after layoff
        horse_layoff_wr = _safe_div(self.wins_after_layoff, self.total_after_layoff)

        # lr_horse_fresh_wr: win rate when active (<30d rest)
        horse_fresh_wr = _safe_div(self.wins_fresh, self.total_fresh)

        # lr_layoff_vs_fresh: layoff_wr - fresh_wr (positive = better after layoff)
        if horse_layoff_wr is not None and horse_fresh_wr is not None:
            layoff_vs_fresh: Optional[float] = round(horse_layoff_wr - horse_fresh_wr, 4)
        else:
            layoff_vs_fresh = None

        # lr_prev_result_before_layoff: position_pct from the race before the current layoff
        # Only meaningful when this IS a layoff start
        if is_layoff == 1 and self.last_race_date is not None:
            prev_result = self.last_position_pct
        else:
            prev_result = None

        return {
            "lr_is_layoff": is_layoff,
            "lr_layoff_days": layoff_days,
            "lr_layoff_bucket": bucket,
            "lr_horse_layoff_wr": horse_layoff_wr,
            "lr_horse_fresh_wr": horse_fresh_wr,
            "lr_layoff_vs_fresh": layoff_vs_fresh,
            "lr_prev_result_before_layoff": prev_result,
            "lr_nb_layoffs_career": self.nb_layoffs,
        }

    def update(
        self,
        race_date: Optional[datetime],
        position: Optional[int],
        nb_partants: Optional[int],
    ) -> None:
        """
        Update state after the current race result is known.
        Called AFTER snapshot() for all horses in the same course.
        """
        pos_pct = _position_pct(position, nb_partants)
        is_winner = (position == 1) if position is not None else False

        # Determine rest category for this start
        if self.last_race_date is not None and race_date is not None:
            delta = race_date - self.last_race_date
            days = max(0, delta.days)

            if days > _THRESHOLD_LAYOFF:
                # This was a layoff return
                self.nb_layoffs += 1
                self.total_after_layoff += 1
                if is_winner:
                    self.wins_after_layoff += 1
                # Record the position from the race just before this layoff
                self.last_pos_before_layoff = self.last_position_pct
            elif days < _THRESHOLD_ACTIVE:
                # Active / fresh
                self.total_fresh += 1
                if is_winner:
                    self.wins_fresh += 1
            # Short layoff (30-60d) does not update either counter — neutral zone

        # Always update last_race_date and last_position_pct
        if race_date is not None:
            self.last_race_date = race_date
        self.last_position_pct = pos_pct


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_layoff_return_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build layoff/return features from partants_master.jsonl."""
    logger.info("=== Layoff Return Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Read minimal fields (streaming)
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        date_str = rec.get("date_reunion_iso", "")
        race_date = _parse_date(date_str)

        # Horse identifier: prefer horse_id, fall back to nom_cheval
        horse_id = (
            rec.get("horse_id")
            or rec.get("cheval_id")
            or rec.get("nom_cheval")
        )

        slim = {
            "uid": rec.get("partant_uid"),
            "date_str": date_str,
            "date": race_date,
            "course": rec.get("course_uid", ""),
            "num": _safe_int(rec.get("num_pmu")) or 0,
            "horse": horse_id,
            "position": _safe_int(rec.get("position_arrivee")),
            "nb_partants": _safe_int(rec.get("nombre_partants") or rec.get("nb_partants")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2: Sort chronologically (date, course_uid, num_pmu)
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date_str"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3: Process record-by-record with temporal integrity
    # ------------------------------------------------------------------
    t2 = time.time()
    horse_states: dict[str, _HorseLayoffState] = defaultdict(_HorseLayoffState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    # Group by (date_str, course_uid) to enforce course-level atomicity.
    # All horses in the same course snapshot BEFORE any state updates.
    i = 0
    while i < total:
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date_str"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date_str"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        # -- Step A: Snapshot pre-race features for all horses in group --
        for rec in course_group:
            horse = rec["horse"]
            race_date = rec["date"]

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "lr_is_layoff": None,
                "lr_layoff_days": None,
                "lr_layoff_bucket": None,
                "lr_horse_layoff_wr": None,
                "lr_horse_fresh_wr": None,
                "lr_layoff_vs_fresh": None,
                "lr_prev_result_before_layoff": None,
                "lr_nb_layoffs_career": 0,
            }

            if horse:
                state = horse_states[horse]
                snap = state.snapshot(race_date)
                features.update(snap)

            results.append(features)

        # -- Step B: Update states with race results (post-race) --
        for rec in course_group:
            horse = rec["horse"]
            if horse:
                horse_states[horse].update(
                    race_date=rec["date"],
                    position=rec["position"],
                    nb_partants=rec["nb_partants"],
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    # Free memory before returning
    del slim_records
    gc.collect()

    elapsed = time.time() - t0
    n_unique = len(horse_states)
    del horse_states
    gc.collect()

    logger.info(
        "Layoff return build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        n_unique,
    )
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path: CLI arg > known candidates."""
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


def _resolve_output_dir(cli_path: Optional[str]) -> Path:
    """Resolve output directory: CLI arg > D:/... primary > local fallback."""
    if cli_path:
        return Path(cli_path)
    if OUTPUT_DIR.parent.exists():
        return OUTPUT_DIR
    return OUTPUT_DIR_FALLBACK


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features layoff/return a partir de partants_master.\n"
            "Analyse les chevaux revenant d'une longue absence (>60 jours)."
        )
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
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/.../layoff_return/)",
    )
    args = parser.parse_args()

    logger = setup_logging("layoff_return_builder")

    input_path = _find_input(args.input)
    output_dir = _resolve_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_layoff_return_features(input_path, logger)

    # Save output
    out_path = output_dir / "layoff_return.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                v = r.get(k)
                if v is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates (%d records) ===", total_count)
        for k in feature_keys:
            cnt = filled[k]
            logger.info(
                "  %-38s %d/%d (%.1f%%)",
                k + ":",
                cnt,
                total_count,
                100.0 * cnt / total_count,
            )

        # Quick sanity: layoff distribution
        buckets: dict[Optional[int], int] = defaultdict(int)
        for r in results:
            buckets[r.get("lr_layoff_bucket")] += 1
        logger.info("=== Layoff bucket distribution ===")
        labels = {0: "active(<30d)", 1: "short(30-60d)", 2: "medium(60-120d)",
                  3: "long(120-365d)", 4: "very_long(>365d)", None: "first_race/unknown"}
        for b in sorted(buckets, key=lambda x: (x is None, x)):
            logger.info("  bucket %s %-18s : %d", b, labels.get(b, ""), buckets[b])

        n_layoffs = sum(1 for r in results if r.get("lr_is_layoff") == 1)
        logger.info(
            "Layoff returns: %d / %d (%.1f%%)",
            n_layoffs, total_count, 100.0 * n_layoffs / total_count,
        )


if __name__ == "__main__":
    main()
