#!/usr/bin/env python3
"""
feature_builders.course_record_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Track record / best time features for specific hippodrome+distance combinations.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant course-record features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the time statistics -- no future leakage.

Produces:
  - course_record_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/course_record/

Features per partant:
  - crec_horse_best_time_here    : horse's personal best temps_course at this hippo+distance
  - crec_horse_vs_track_record   : ratio horse_best / track_best at this hippo+distance
  - crec_track_record_time       : best known temps_course at this hippo+distance combo
  - crec_horse_avg_time_here     : horse's average temps_course at this hippo+distance
  - crec_nb_runs_here            : number of times horse has run at this exact hippo+distance
  - crec_improving_here          : 1 if horse's last time here was better than average
  - crec_time_consistency        : std dev of horse's times at this hippo+distance
  - crec_field_avg_best_time     : average of all competitors' best times here (field quality proxy)

Usage:
    python feature_builders/course_record_builder.py
    python feature_builders/course_record_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_DEFAULT_INPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
_DEFAULT_OUTPUT = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/course_record/course_record_features.jsonl"
)

# Fallback candidates relative to the project root (for local dev)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    _DEFAULT_INPUT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000


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
    """Convert val to float, returning None on failure or NaN."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # filter out NaN
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Convert val to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_temps_course(val) -> Optional[float]:
    """Parse temps_course to seconds (float).

    Handles:
      - None / empty -> None
      - Already a number (int/float) -> return as float
      - Formatted strings:
          "1:23.45"  -> 83.45
          "1m23s"    -> 83.0
          "83.45"    -> 83.45
          "83"       -> 83.0
    """
    if val is None:
        return None

    # Already numeric
    v = _safe_float(val)
    if v is not None:
        return v if v > 0 else None

    s = str(val).strip()
    if not s:
        return None

    # "mm:ss.xx" or "mm:ss"
    if ":" in s:
        try:
            parts = s.split(":")
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = float(parts[1])
                total = minutes * 60.0 + seconds
                return total if total > 0 else None
            elif len(parts) == 3:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = float(parts[2])
                total = hours * 3600.0 + minutes * 60.0 + seconds
                return total if total > 0 else None
        except (ValueError, TypeError):
            pass

    # "XmYs" format
    if "m" in s.lower() and "s" in s.lower():
        try:
            s_lower = s.lower().replace("s", "")
            m_idx = s_lower.index("m")
            minutes = int(s_lower[:m_idx])
            seconds = float(s_lower[m_idx + 1:])
            total = minutes * 60.0 + seconds
            return total if total > 0 else None
        except (ValueError, TypeError, IndexError):
            pass

    return None


def _std_dev(values: list[float]) -> Optional[float]:
    """Population standard deviation. Returns None if < 2 values."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    return round(math.sqrt(variance), 4)


# ===========================================================================
# STATE STRUCTURES
# ===========================================================================


class _HorseTrackState:
    """Per-(horse, hippodrome, distance) time history.

    Tracks a chronological list of temps_course values (in seconds).
    """

    __slots__ = ("times",)

    def __init__(self) -> None:
        self.times: list[float] = []

    def snapshot(self) -> dict[str, Any]:
        """Compute features from times accumulated BEFORE current race."""
        n = len(self.times)
        if n == 0:
            return {
                "horse_best": None,
                "horse_avg": None,
                "nb_runs": 0,
                "improving": None,
                "consistency": None,
            }

        best = min(self.times)
        avg = sum(self.times) / n
        last = self.times[-1]

        # improving: 1 if last time < average (lower = faster)
        improving = 1 if last < avg else 0

        return {
            "horse_best": round(best, 4),
            "horse_avg": round(avg, 4),
            "nb_runs": n,
            "improving": improving,
            "consistency": _std_dev(self.times),
        }

    def update(self, t: float) -> None:
        """Add a race time (post-race)."""
        self.times.append(t)


class _TrackState:
    """Per-(hippodrome, distance) global state.

    Tracks:
      - record_time: the fastest ever time at this combo (float or None)
      - horse_bests: dict[horse_id -> best_time] for field quality computation
    """

    __slots__ = ("record_time", "horse_bests")

    def __init__(self) -> None:
        self.record_time: Optional[float] = None
        # horse_id -> personal best time at this track+distance
        self.horse_bests: dict[str, float] = {}

    def get_record(self) -> Optional[float]:
        return self.record_time

    def get_horse_best(self, horse_id: str) -> Optional[float]:
        return self.horse_bests.get(horse_id)

    def update(self, horse_id: str, t: float) -> None:
        """Update global record and per-horse best (post-race)."""
        # Update overall track record
        if self.record_time is None or t < self.record_time:
            self.record_time = t

        # Update horse personal best at this track
        current_best = self.horse_bests.get(horse_id)
        if current_best is None or t < current_best:
            self.horse_bests[horse_id] = t

    def field_avg_best(self, horse_ids: list[str]) -> Optional[float]:
        """Average of personal bests for the given list of horse IDs.

        Only includes horses with at least one recorded time here.
        The requesting horse is included if it has a prior best.
        """
        bests = [
            self.horse_bests[hid]
            for hid in horse_ids
            if hid in self.horse_bests
        ]
        if not bests:
            return None
        return round(sum(bests) / len(bests), 4)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_course_record_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build course record features from partants_master.jsonl.

    Three-phase approach:
      1. Read minimal fields into memory for sorting.
      2. Sort chronologically by (date_reunion_iso, course_uid, num_pmu).
      3. Process race-by-race: snapshot before update (temporal integrity).
    """
    logger.info("=== Course Record Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        # Resolve horse identifier
        horse_id = (
            rec.get("horse_id")
            or rec.get("partant_uid")
            or rec.get("nom_cheval")
        )

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": _safe_int(rec.get("num_pmu")) or 0,
            "horse_id": (str(horse_id).strip() if horse_id else None),
            "hippo": (rec.get("hippodrome_normalise") or rec.get("hippodrome") or "").strip(),
            "distance": _safe_float(rec.get("distance")),
            "temps": _parse_temps_course(rec.get("temps_course")),
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

    # -- Phase 3: Process race-by-race --
    t2 = time.time()

    # State: per-(horse_id, hippo, distance) time history
    horse_track_states: dict[tuple[str, str, float], _HorseTrackState] = defaultdict(
        _HorseTrackState
    )
    # State: per-(hippo, distance) global state
    track_states: dict[tuple[str, float], _TrackState] = defaultdict(_TrackState)

    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date"]
        course_group: list[dict] = []

        # Collect all partants in this race
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        # Build field horse list for this race (for field_avg_best_time)
        # Use all horses in the race group, keyed by (hippo, distance)
        # We'll compute per-record using the current track state snapshot

        # -- Snapshot pre-race features for all partants --
        for rec in course_group:
            horse_id = rec["horse_id"]
            hippo = rec["hippo"]
            distance = rec["distance"]

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "crec_horse_best_time_here": None,
                "crec_horse_vs_track_record": None,
                "crec_track_record_time": None,
                "crec_horse_avg_time_here": None,
                "crec_nb_runs_here": 0,
                "crec_improving_here": None,
                "crec_time_consistency": None,
                "crec_field_avg_best_time": None,
            }

            if not horse_id or not hippo or distance is None or distance <= 0:
                results.append(features)
                continue

            track_key = (hippo, distance)
            horse_key = (horse_id, hippo, distance)

            # Per-horse snapshot
            horse_state = horse_track_states[horse_key]
            snap = horse_state.snapshot()

            features["crec_nb_runs_here"] = snap["nb_runs"]
            features["crec_horse_best_time_here"] = snap["horse_best"]
            features["crec_horse_avg_time_here"] = snap["horse_avg"]
            features["crec_improving_here"] = snap["improving"]
            features["crec_time_consistency"] = snap["consistency"]

            # Track record snapshot
            track_state = track_states[track_key]
            track_record = track_state.get_record()
            features["crec_track_record_time"] = (
                round(track_record, 4) if track_record is not None else None
            )

            # Horse vs track record ratio
            if snap["horse_best"] is not None and track_record is not None and track_record > 0:
                features["crec_horse_vs_track_record"] = round(
                    snap["horse_best"] / track_record, 6
                )

            # Field average best time (all competitors with prior time at this combo)
            field_horse_ids = [
                r["horse_id"]
                for r in course_group
                if r["horse_id"] and r["distance"] == distance and r["hippo"] == hippo
            ]
            features["crec_field_avg_best_time"] = track_state.field_avg_best(field_horse_ids)

            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            horse_id = rec["horse_id"]
            hippo = rec["hippo"]
            distance = rec["distance"]
            t = rec["temps"]

            if not horse_id or not hippo or distance is None or distance <= 0:
                continue
            if t is None:
                continue  # no time recorded, skip update

            track_key = (hippo, distance)
            horse_key = (horse_id, hippo, distance)

            horse_track_states[horse_key].update(t)
            track_states[track_key].update(horse_id, t)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Course record build termine: %d features en %.1fs "
        "(horse-track pairs: %d, track combos: %d)",
        len(results), elapsed,
        len(horse_track_states), len(track_states),
    )

    return results


# ===========================================================================
# SAUVEGARDE ATOMIQUE
# ===========================================================================


def _write_atomic_jsonl(records: list[dict[str, Any]], out_path: Path, logger) -> None:
    """Write records to JSONL with atomic .tmp -> rename."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".tmp")
    logger.info("Ecriture atomique: %s (%d records)", out_path, len(records))
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    tmp_path.replace(out_path)
    logger.info("Fichier ecrit avec succes: %s", out_path)


# ===========================================================================
# FILL RATE LOG
# ===========================================================================


def _log_fill_rates(results: list[dict[str, Any]], logger) -> None:
    """Log fill rates for all feature columns."""
    if not results:
        logger.warning("Aucun resultat a analyser pour les fill rates")
        return

    feature_keys = [k for k in results[0] if k != "partant_uid"]
    total = len(results)
    filled: dict[str, int] = {k: 0 for k in feature_keys}

    for rec in results:
        for k in feature_keys:
            v = rec.get(k)
            if v is not None:
                # For crec_nb_runs_here=0, it IS filled (0 is meaningful)
                filled[k] += 1

    logger.info("=== Fill rates (%d records) ===", total)
    for k in feature_keys:
        pct = 100.0 * filled[k] / total if total > 0 else 0.0
        logger.info("  %-35s %d/%d (%.1f%%)", k, filled[k], total, pct)


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in _INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features course record (meilleur temps par hippodrome+distance)"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help=(
            "Chemin vers partants_master.jsonl "
            f"(defaut: {_DEFAULT_INPUT})"
        ),
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help=(
            "Chemin de sortie du fichier JSONL "
            f"(defaut: {_DEFAULT_OUTPUT})"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("course_record_builder")

    input_path = _find_input(args.input)
    output_path = Path(args.output) if args.output else _DEFAULT_OUTPUT

    results = build_course_record_features(input_path, logger)

    _write_atomic_jsonl(results, output_path, logger)
    _log_fill_rates(results, logger)


if __name__ == "__main__":
    main()
