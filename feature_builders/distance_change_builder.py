#!/usr/bin/env python3
"""
feature_builders.distance_change_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Tracks distance changes between consecutive races and their impact on horse
performance.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - distance_change.jsonl  in output/distance_change/

Features per partant (8):
  - dc_distance_delta           : current distance - last race distance (metres)
  - dc_distance_pct_change      : (current - last) / last * 100
  - dc_stepping_up              : 1 if current distance > last by > 200 m
  - dc_stepping_down            : 1 if current distance < last by > 200 m
  - dc_same_distance            : 1 if abs(distance_delta) <= 100 m
  - dc_horse_wr_when_stepping_up   : horse's historical win rate when dist increased
  - dc_horse_wr_when_stepping_down : horse's historical win rate when dist decreased
  - dc_distance_changes_last5   : count of distance changes (>200 m) in last 5 races

State per horse:
  last_distance      : int | None
  distances          : deque(maxlen=10)  -- distances of last 10 races
  wins_step_up       : int
  total_step_up      : int
  wins_step_down     : int
  total_step_down    : int

Usage:
    python feature_builders/distance_change_builder.py
    python feature_builders/distance_change_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/distance_change")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# A distance change of more than this threshold (in metres) counts as a
# "step up" or "step down".
_CHANGE_THRESHOLD = 200

# The window for dc_distance_changes_last5.
_LAST_N = 5


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield parsed dicts from a JSONL file one line at a time (streaming)."""
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


def _safe_int(val: Any) -> Optional[int]:
    """Safely convert a value to a positive integer, or return None."""
    if val is None:
        return None
    try:
        v = int(float(val))
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseDistChangeState:
    """Accumulates per-horse distance-change history and win-rate counters.

    The ``distances`` deque stores the distances of the last 10 races so that
    ``dc_distance_changes_last5`` can be derived from it.

    All fields use __slots__ so that millions of instances stay cheap.
    """

    __slots__ = (
        "last_distance",
        "distances",
        "wins_step_up",
        "total_step_up",
        "wins_step_down",
        "total_step_down",
    )

    def __init__(self) -> None:
        self.last_distance: Optional[int] = None
        # Keep distances of last 10 races (enough to compute last-5 window)
        self.distances: deque = deque(maxlen=10)
        self.wins_step_up: int = 0
        self.total_step_up: int = 0
        self.wins_step_down: int = 0
        self.total_step_down: int = 0

    # ------------------------------------------------------------------
    # Snapshot (called BEFORE updating with current race result)
    # ------------------------------------------------------------------

    def snapshot(self, current_distance: Optional[int]) -> dict[str, Any]:
        """Return feature dict for the current race *before* updating state."""
        feats: dict[str, Any] = {
            "dc_distance_delta": None,
            "dc_distance_pct_change": None,
            "dc_stepping_up": None,
            "dc_stepping_down": None,
            "dc_same_distance": None,
            "dc_horse_wr_when_stepping_up": None,
            "dc_horse_wr_when_stepping_down": None,
            "dc_distance_changes_last5": None,
        }

        if current_distance is None or self.last_distance is None:
            # Can only compute delta/direction when we have a previous race.
            # dc_distance_changes_last5 can still be computed if we have history.
            feats["dc_distance_changes_last5"] = self._count_changes_last5()
            return feats

        last = self.last_distance
        delta = current_distance - last

        feats["dc_distance_delta"] = delta
        feats["dc_distance_pct_change"] = round(delta / last * 100, 4) if last != 0 else None

        stepping_up = int(delta > _CHANGE_THRESHOLD)
        stepping_down = int(delta < -_CHANGE_THRESHOLD)
        same_distance = int(abs(delta) <= 100)

        feats["dc_stepping_up"] = stepping_up
        feats["dc_stepping_down"] = stepping_down
        feats["dc_same_distance"] = same_distance

        # Historical win rates when this type of change has occurred before
        if self.total_step_up > 0:
            feats["dc_horse_wr_when_stepping_up"] = round(
                self.wins_step_up / self.total_step_up, 4
            )
        if self.total_step_down > 0:
            feats["dc_horse_wr_when_stepping_down"] = round(
                self.wins_step_down / self.total_step_down, 4
            )

        feats["dc_distance_changes_last5"] = self._count_changes_last5()
        return feats

    def _count_changes_last5(self) -> Optional[int]:
        """Count how many of the last _LAST_N races involved a distance change > 200 m."""
        distances = list(self.distances)
        # We need at least 2 races to compute one change
        if len(distances) < 2:
            return None
        # Take the last _LAST_N distances for the window
        window = distances[-_LAST_N:] if len(distances) >= _LAST_N else distances
        count = 0
        for i in range(1, len(window)):
            if abs(window[i] - window[i - 1]) > _CHANGE_THRESHOLD:
                count += 1
        return count

    # ------------------------------------------------------------------
    # Update (called AFTER snapshot, with current race result)
    # ------------------------------------------------------------------

    def update(
        self,
        current_distance: Optional[int],
        is_winner: bool,
    ) -> None:
        """Update state with the result of the current race."""
        if current_distance is None:
            return

        if self.last_distance is not None:
            delta = current_distance - self.last_distance
            if delta > _CHANGE_THRESHOLD:
                # Horse stepped up in distance
                self.total_step_up += 1
                if is_winner:
                    self.wins_step_up += 1
            elif delta < -_CHANGE_THRESHOLD:
                # Horse stepped down in distance
                self.total_step_down += 1
                if is_winner:
                    self.wins_step_down += 1

        self.distances.append(current_distance)
        self.last_distance = current_distance


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_distance_change_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build distance-change features from partants_master.jsonl.

    Three-phase approach (index + sort + seek):
      1. Read minimal fields into memory (streaming).
      2. Sort chronologically.
      3. Process course-by-course: snapshot features BEFORE updating state.
    """
    logger.info("=== Distance Change Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: streaming read of minimal fields
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
            "distance": _safe_int(rec.get("distance")),
            "is_gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0
    )

    # ------------------------------------------------------------------
    # Phase 2: sort chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3: process course-by-course (snapshot THEN update)
    # ------------------------------------------------------------------
    t2 = time.time()

    horse_states: dict[str, _HorseDistChangeState] = defaultdict(_HorseDistChangeState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        # Collect all partants in this course
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # --- Snapshot features BEFORE updating state ---
        for rec in course_group:
            hid = rec["horse_id"]
            row: dict[str, Any] = {"partant_uid": rec["uid"]}
            if hid:
                row.update(horse_states[hid].snapshot(rec["distance"]))
            else:
                row.update(
                    {
                        "dc_distance_delta": None,
                        "dc_distance_pct_change": None,
                        "dc_stepping_up": None,
                        "dc_stepping_down": None,
                        "dc_same_distance": None,
                        "dc_horse_wr_when_stepping_up": None,
                        "dc_horse_wr_when_stepping_down": None,
                        "dc_distance_changes_last5": None,
                    }
                )
            results.append(row)

        # --- Update state after snapshotting ---
        for rec in course_group:
            hid = rec["horse_id"]
            if hid:
                horse_states[hid].update(rec["distance"], rec["is_gagnant"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Distance change build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        len(horse_states),
    )

    gc.collect()
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file, trying cli override then known candidates."""
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
            "Construction des features de changement de distance a partir de "
            "partants_master"
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
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/.../distance_change/)",
    )
    args = parser.parse_args()

    logger = setup_logging("distance_change_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_distance_change_features(input_path, logger)

    out_path = output_dir / "distance_change.jsonl"
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
                "  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count
            )

    logger.info("Done.")


if __name__ == "__main__":
    main()
