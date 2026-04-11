#!/usr/bin/env python3
"""
feature_builders.engagement_financial_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Financial engagement and race-value features derived from supplement/engagement data.

Two-pass strategy:
  Pass 1 (streaming): collect all records, keeping only the slim fields needed.
  Pass 2 (in-memory): sort chronologically, group by course, compute per-partant
                      features that require knowledge of the full field (gains
                      rank within race, allocation quintile across recent races).

Temporal integrity: ef_allocation_rank_in_field is computed using only races
that occurred BEFORE the current race's date, so there is no future leakage.
ef_gains_rank_in_race is computed across the current race's field (race-day
information available before the start).

Produces:
  - engagement_financial.jsonl  in OUTPUT_DIR

Features per partant (8):
  - ef_allocation_log           : log(1 + allocation) – scale-normalised prize
  - ef_allocation_rank_in_field : quintile (0-4) of this race's allocation
                                  relative to the last ~500 races seen so far
  - ef_supplement_flag          : 1 if horse paid supplement to enter (supplement > 0)
  - ef_supplement_ratio         : supplement / allocation (investment ratio)
  - ef_gains_vs_allocation      : gains_carriere_euros / allocation (ROI potential)
  - ef_prize_per_partant        : allocation / nombre_partants (expected value per starter)
  - ef_is_high_value_race       : 1 if allocation >= median of recent races seen so far
  - ef_gains_rank_in_race       : horse's earnings percentile within this race's field

Usage:
    python feature_builders/engagement_financial_builder.py
    python feature_builders/engagement_financial_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/engagement_financial_builder.py --input ... --output-dir /path/to/out
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from bisect import bisect_left, insort
from collections import deque
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

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/engagement_financial")

_LOG_EVERY = 500_000

# Rolling window of recent allocation values used for quintile / median ranking.
# Keeping the last 500 unique races gives a stable reference without
# consuming too much RAM (each entry is a single float).
_ALLOC_WINDOW_SIZE = 500


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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN guard
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _quintile(value: float, sorted_window: list[float]) -> int:
    """Return the quintile (0-4) of *value* relative to *sorted_window*."""
    n = len(sorted_window)
    if n == 0:
        return 2  # neutral middle quintile when no history
    rank = bisect_left(sorted_window, value)
    # Map rank to quintile 0-4
    return min(4, int(rank / n * 5))


def _percentile_rank(value: float, sorted_list: list[float]) -> float:
    """Return the fraction (0.0-1.0) of elements strictly below *value*."""
    n = len(sorted_list)
    if n == 0:
        return 0.5
    rank = bisect_left(sorted_list, value)
    return round(rank / n, 4)


# ===========================================================================
# ROLLING ALLOCATION WINDOW
# ===========================================================================


class _AllocWindow:
    """Maintains a rolling sorted window of the last N allocation values.

    The window stores one value per *race* (course_uid), not per partant,
    to avoid the same race inflating the distribution.
    """

    __slots__ = ("_maxsize", "_queue", "_sorted")

    def __init__(self, maxsize: int = _ALLOC_WINDOW_SIZE) -> None:
        self._maxsize = maxsize
        self._queue: deque[float] = deque()   # FIFO of insertion order
        self._sorted: list[float] = []         # always sorted

    def query_quintile(self, alloc: float) -> int:
        return _quintile(alloc, self._sorted)

    def query_median(self) -> Optional[float]:
        n = len(self._sorted)
        if n == 0:
            return None
        mid = n // 2
        if n % 2 == 1:
            return self._sorted[mid]
        return (self._sorted[mid - 1] + self._sorted[mid]) / 2.0

    def add(self, alloc: float) -> None:
        if len(self._queue) >= self._maxsize:
            old = self._queue.popleft()
            # Remove from sorted list
            idx = bisect_left(self._sorted, old)
            if idx < len(self._sorted) and self._sorted[idx] == old:
                self._sorted.pop(idx)
        self._queue.append(alloc)
        insort(self._sorted, alloc)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_engagement_financial_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Two-pass build of engagement/financial features.

    Pass 1: stream partants_master.jsonl, retain only required fields.
    Pass 2: sort chronologically, group by course_uid, compute features.
    """
    logger.info("=== Engagement Financial Builder ===")
    logger.info("Lecture en streaming (Pass 1): %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1 – stream and collect slim records
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", "") or "",
            "course": rec.get("course_uid", "") or "",
            "num": rec.get("num_pmu", 0) or 0,
            "allocation": _safe_float(rec.get("allocation")),
            "supplement": _safe_float(rec.get("supplement")),
            "gains": _safe_float(rec.get("gains_carriere_euros")),
            "nb_partants": _safe_int(rec.get("nombre_partants")),
        }
        slim_records.append(slim)

    logger.info(
        "Pass 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0
    )

    # ------------------------------------------------------------------
    # Sort chronologically then by course then by num_pmu
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Pass 2 – group by course, compute features
    # ------------------------------------------------------------------
    logger.info("Pass 2: calcul des features par course...")
    t2 = time.time()

    alloc_window = _AllocWindow(_ALLOC_WINDOW_SIZE)
    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        # Collect all partants for this course
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        # --- Race-level allocation (use first non-None value in the group) ---
        race_alloc: Optional[float] = None
        for rec in course_group:
            if rec["allocation"] is not None:
                race_alloc = rec["allocation"]
                break

        # --- Snapshot of rolling window BEFORE adding this race ---
        alloc_quintile: Optional[int] = None
        is_high_value: Optional[int] = None
        if race_alloc is not None and race_alloc >= 0:
            alloc_quintile = alloc_window.query_quintile(race_alloc)
            median_alloc = alloc_window.query_median()
            if median_alloc is not None:
                is_high_value = int(race_alloc >= median_alloc)

        # --- Gains rank within race field ---
        gains_list: list[float] = [
            r["gains"] for r in course_group if r["gains"] is not None
        ]
        gains_sorted: list[float] = sorted(gains_list)

        for rec in course_group:
            alloc = rec["allocation"]
            supplement = rec["supplement"]
            gains = rec["gains"]
            nb_partants = rec["nb_partants"]

            feats: dict[str, Any] = {"partant_uid": rec["uid"]}

            # ef_allocation_log
            if alloc is not None and alloc >= 0:
                feats["ef_allocation_log"] = round(math.log1p(alloc), 6)
            else:
                feats["ef_allocation_log"] = None

            # ef_allocation_rank_in_field (quintile 0-4, pre-computed above)
            feats["ef_allocation_rank_in_field"] = alloc_quintile

            # ef_supplement_flag
            if supplement is not None:
                feats["ef_supplement_flag"] = int(supplement > 0)
            else:
                feats["ef_supplement_flag"] = None

            # ef_supplement_ratio
            if supplement is not None and alloc is not None and alloc > 0:
                feats["ef_supplement_ratio"] = round(supplement / alloc, 6)
            else:
                feats["ef_supplement_ratio"] = None

            # ef_gains_vs_allocation
            if gains is not None and alloc is not None and alloc > 0:
                feats["ef_gains_vs_allocation"] = round(gains / alloc, 4)
            else:
                feats["ef_gains_vs_allocation"] = None

            # ef_prize_per_partant
            if alloc is not None and nb_partants is not None and nb_partants > 0:
                feats["ef_prize_per_partant"] = round(alloc / nb_partants, 2)
            else:
                feats["ef_prize_per_partant"] = None

            # ef_is_high_value_race
            feats["ef_is_high_value_race"] = is_high_value

            # ef_gains_rank_in_race
            if gains is not None and len(gains_sorted) > 0:
                feats["ef_gains_rank_in_race"] = _percentile_rank(gains, gains_sorted)
            else:
                feats["ef_gains_rank_in_race"] = None

            results.append(feats)

        # --- Update rolling window with this race's allocation AFTER snapshot ---
        if race_alloc is not None and race_alloc >= 0:
            alloc_window.add(race_alloc)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Engagement financial build termine: %d features en %.1fs",
        len(results),
        elapsed,
    )

    # Free memory
    del slim_records
    gc.collect()

    return results


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features engagement financier "
            "a partir de partants_master.jsonl"
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
        help="Repertoire de sortie (defaut: OUTPUT_DIR)",
    )
    args = parser.parse_args()

    logger = setup_logging("engagement_financial_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_engagement_financial_features(input_path, logger)

    # Atomic write via save_jsonl (tmp + replace)
    out_path = output_dir / "engagement_financial.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rates
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info(
                "  %s: %d/%d (%.1f%%)", k, v, total_count, 100.0 * v / total_count
            )

    logger.info("Termine.")


if __name__ == "__main__":
    main()
