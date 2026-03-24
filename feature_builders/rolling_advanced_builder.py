#!/usr/bin/env python3
"""
feature_builders.rolling_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
6 advanced rolling statistics over each horse's career history.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant rolling career features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the rolling windows -- no future leakage.

Produces:
  - rolling_advanced.jsonl   in output/rolling_advanced/

Features per partant:
  - rolling_earnings_5       : sum of gains from last 5 races
  - rolling_win_rate_10      : win rate over last 10 races
  - rolling_place_rate_10    : place rate (top 3) over last 10 races
  - rolling_avg_cote_5       : average cote_finale over last 5 races
  - rolling_avg_field_size_5 : average nb_partants over last 5 races
  - rolling_distance_variety : nb unique distance categories in last 10 races

Usage:
    python feature_builders/rolling_advanced_builder.py
    python feature_builders/rolling_advanced_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict, deque
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "rolling_advanced"

# Rolling window sizes
_WINDOW_5 = 5
_WINDOW_10 = 10

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


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    """Convert value to float or return None."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Convert value to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _classify_distance(distance_m) -> Optional[str]:
    """Classify distance in metres into a category."""
    d = _safe_float(distance_m)
    if d is None:
        return None
    if d < 1200:
        return "sprint"
    if d < 1600:
        return "mile"
    if d < 2100:
        return "intermediate"
    if d < 2800:
        return "staying"
    return "long"


def _is_placed(position) -> bool:
    """Return True if position is 1, 2 or 3."""
    p = _safe_int(position)
    return p is not None and 1 <= p <= 3


# ===========================================================================
# ROLLING HISTORY TRACKER
# ===========================================================================


class _HorseHistory:
    """Maintains a bounded rolling history for one horse.

    Stores the last N race outcomes for efficient rolling computation.
    Uses deques with maxlen for automatic eviction.
    """

    __slots__ = ("last_10", "last_5_gains", "last_5_cote", "last_5_field")

    def __init__(self) -> None:
        # last_10 stores tuples: (is_winner, is_placed, distance_category)
        self.last_10: deque[tuple[bool, bool, Optional[str]]] = deque(maxlen=_WINDOW_10)
        # Separate deques for last-5 numeric values (gains, cote, field_size)
        self.last_5_gains: deque[float] = deque(maxlen=_WINDOW_5)
        self.last_5_cote: deque[float] = deque(maxlen=_WINDOW_5)
        self.last_5_field: deque[float] = deque(maxlen=_WINDOW_5)

    def snapshot(self) -> dict[str, Any]:
        """Return rolling features from current history (pre-race snapshot)."""
        n10 = len(self.last_10)
        n5_gains = len(self.last_5_gains)
        n5_cote = len(self.last_5_cote)
        n5_field = len(self.last_5_field)

        # rolling_earnings_5
        rolling_earnings_5 = round(sum(self.last_5_gains), 2) if n5_gains > 0 else None

        # rolling_win_rate_10
        rolling_win_rate_10 = None
        if n10 >= 1:
            wins = sum(1 for w, _, _ in self.last_10 if w)
            rolling_win_rate_10 = round(wins / n10, 4)

        # rolling_place_rate_10
        rolling_place_rate_10 = None
        if n10 >= 1:
            places = sum(1 for _, p, _ in self.last_10 if p)
            rolling_place_rate_10 = round(places / n10, 4)

        # rolling_avg_cote_5
        rolling_avg_cote_5 = None
        if n5_cote > 0:
            rolling_avg_cote_5 = round(sum(self.last_5_cote) / n5_cote, 2)

        # rolling_avg_field_size_5
        rolling_avg_field_size_5 = None
        if n5_field > 0:
            rolling_avg_field_size_5 = round(sum(self.last_5_field) / n5_field, 2)

        # rolling_distance_variety: unique distance categories in last 10
        rolling_distance_variety = None
        if n10 >= 1:
            cats = {c for _, _, c in self.last_10 if c is not None}
            rolling_distance_variety = len(cats)

        return {
            "rolling_earnings_5": rolling_earnings_5,
            "rolling_win_rate_10": rolling_win_rate_10,
            "rolling_place_rate_10": rolling_place_rate_10,
            "rolling_avg_cote_5": rolling_avg_cote_5,
            "rolling_avg_field_size_5": rolling_avg_field_size_5,
            "rolling_distance_variety": rolling_distance_variety,
        }

    def update(
        self,
        is_winner: bool,
        is_placed: bool,
        distance_category: Optional[str],
        gains: Optional[float],
        cote: Optional[float],
        field_size: Optional[float],
    ) -> None:
        """Add a race result to the history (post-race)."""
        self.last_10.append((is_winner, is_placed, distance_category))
        if gains is not None:
            self.last_5_gains.append(gains)
        else:
            self.last_5_gains.append(0.0)
        if cote is not None:
            self.last_5_cote.append(cote)
        if field_size is not None:
            self.last_5_field.append(field_size)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_rolling_advanced_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build rolling advanced features from partants_master.jsonl.

    Two-phase approach:
      1. Read minimal fields into memory and sort chronologically.
      2. Process course-by-course, snapshotting rolling features before
         updating history with race results.
    """
    logger.info("=== Rolling Advanced Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        distance = _safe_float(rec.get("distance"))
        dist_cat = rec.get("distance_category") or _classify_distance(distance)
        position = rec.get("position_arrivee")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "place": _is_placed(position),
            "distance_category": dist_cat,
            "gains": _safe_float(rec.get("gains_partant") or rec.get("gains_course") or rec.get("gains")),
            "cote": _safe_float(rec.get("cote_finale") or rec.get("cote_probable")),
            "nb_partants": _safe_float(rec.get("nombre_partants") or rec.get("nb_partants")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()

    horse_history: dict[str, _HorseHistory] = defaultdict(_HorseHistory)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total_recs = len(slim_records)

    while i < total_recs:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while i < total_recs and slim_records[i]["course"] == course_uid and slim_records[i]["date"] == course_date:
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # -- Snapshot rolling features for each partant (pre-race) --
        for rec in course_group:
            uid = rec["uid"]
            cheval = rec["cheval"]

            if cheval:
                features = horse_history[cheval].snapshot()
            else:
                features = {
                    "rolling_earnings_5": None,
                    "rolling_win_rate_10": None,
                    "rolling_place_rate_10": None,
                    "rolling_avg_cote_5": None,
                    "rolling_avg_field_size_5": None,
                    "rolling_distance_variety": None,
                }

            features["partant_uid"] = uid
            results.append(features)

        # -- Post-race: update horse histories --
        for rec in course_group:
            cheval = rec["cheval"]
            if cheval:
                horse_history[cheval].update(
                    is_winner=rec["gagnant"],
                    is_placed=rec["place"],
                    distance_category=rec["distance_category"],
                    gains=rec["gains"],
                    cote=rec["cote"],
                    field_size=rec["nb_partants"],
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total_recs)

    elapsed = time.time() - t0
    logger.info(
        "Rolling advanced build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_history),
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
        description="Construction des features rolling avancees a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/rolling_advanced/)",
    )
    args = parser.parse_args()

    logger = setup_logging("rolling_advanced_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_rolling_advanced_features(input_path, logger)

    # Save
    out_path = output_dir / "rolling_advanced.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
