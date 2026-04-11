#!/usr/bin/env python3
"""
feature_builders.fatigue_model_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse fatigue and recovery pattern features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant fatigue/recovery features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the fatigue state -- no future leakage.  State is
snapshotted BEFORE the current race updates it.

Produces:
  - fatigue_model.jsonl   in builder_outputs/fatigue_model/

Features per partant (10):
  - ftg_days_since_last           : days since last race (from ecart_precedent or dates)
  - ftg_optimal_rest              : 1 if rest in [14,35] days, 0 otherwise
  - ftg_is_fresh                  : 1 if >60 days rest, 0 otherwise
  - ftg_is_backed_up              : 1 if <10 days rest, 0 otherwise
  - ftg_cumulative_distance_30d   : total distance raced in last 30 days
  - ftg_cumulative_distance_90d   : total distance raced in last 90 days
  - ftg_races_last_14d            : number of races in last 14 days
  - ftg_recovery_score            : exponential decay sum over last 5 races (exp(-days/20))
  - ftg_win_rate_after_rest_bucket: horse's historical win rate for similar rest bucket
  - ftg_fatigue_trend             : position worsening signal (last 2 vs earlier 2)

Memory-optimised version:
  - Phase 1 reads only sort keys + byte offsets into memory
  - Phase 2 streams output to disk via seek-based record reading
  - gc.collect() every 500K records

Usage:
    python feature_builders/fatigue_model_builder.py
    python feature_builders/fatigue_model_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/fatigue_model")

_LOG_EVERY = 500_000

# Rest buckets for win-rate tracking
_REST_BUCKETS = {
    "0-14": (0, 14),
    "14-35": (14, 35),
    "35-60": (35, 60),
    "60+": (60, 999_999),
}


def _classify_rest_bucket(days: int) -> str:
    """Classify days of rest into a bucket label."""
    if days < 14:
        return "0-14"
    elif days < 35:
        return "14-35"
    elif days < 60:
        return "35-60"
    else:
        return "60+"


# ===========================================================================
# DATE HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _parse_ecart(ecart_val) -> Optional[int]:
    """Parse ecart_precedent to integer days. Returns None on failure."""
    if ecart_val is None:
        return None
    try:
        val = int(ecart_val)
        return val if val >= 0 else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# HORSE STATE (per-horse tracker)
# ===========================================================================


class _HorseState:
    """Lightweight per-horse fatigue state tracker.

    Uses __slots__ and deques with maxlen for bounded memory.
    """

    __slots__ = ("last_race_dates", "race_distances", "positions", "rest_bucket_stats")

    def __init__(self) -> None:
        # deque of datetime objects (most recent last), maxlen=10
        self.last_race_dates: deque = deque(maxlen=10)
        # deque of (datetime, int_distance) tuples, maxlen=10
        self.race_distances: deque = deque(maxlen=10)
        # deque of int positions (most recent last), maxlen=10
        self.positions: deque = deque(maxlen=10)
        # {bucket_label -> [wins, total]}
        self.rest_bucket_stats: dict[str, list[int]] = {}


# ===========================================================================
# FEATURE COMPUTATION (from snapshot)
# ===========================================================================


def _compute_features(
    state: _HorseState,
    race_date: Optional[datetime],
    ecart_val,
    distance: Optional[int],
) -> dict[str, Any]:
    """Compute fatigue features from the CURRENT (pre-update) state."""
    feats: dict[str, Any] = {}

    # ---- ftg_days_since_last ----
    days_since: Optional[int] = _parse_ecart(ecart_val)
    if days_since is None and race_date is not None and state.last_race_dates:
        last_dt = state.last_race_dates[-1]
        delta = (race_date - last_dt).days
        if delta >= 0:
            days_since = delta
    feats["ftg_days_since_last"] = days_since

    # ---- ftg_optimal_rest ----
    if days_since is not None:
        feats["ftg_optimal_rest"] = 1 if 14 <= days_since <= 35 else 0
    else:
        feats["ftg_optimal_rest"] = None

    # ---- ftg_is_fresh ----
    if days_since is not None:
        feats["ftg_is_fresh"] = 1 if days_since > 60 else 0
    else:
        feats["ftg_is_fresh"] = None

    # ---- ftg_is_backed_up ----
    if days_since is not None:
        feats["ftg_is_backed_up"] = 1 if days_since < 10 else 0
    else:
        feats["ftg_is_backed_up"] = None

    # ---- ftg_cumulative_distance_30d / 90d ----
    cum_30 = 0
    cum_90 = 0
    has_any_dist = False
    if race_date is not None:
        for dt, dist in state.race_distances:
            delta_days = (race_date - dt).days
            if 0 < delta_days <= 90:
                cum_90 += dist
                has_any_dist = True
                if delta_days <= 30:
                    cum_30 += dist

    feats["ftg_cumulative_distance_30d"] = cum_30 if has_any_dist else None
    feats["ftg_cumulative_distance_90d"] = cum_90 if has_any_dist else None

    # ---- ftg_races_last_14d ----
    races_14d = 0
    has_dates = False
    if race_date is not None:
        for dt in state.last_race_dates:
            delta_days = (race_date - dt).days
            if 0 < delta_days <= 14:
                races_14d += 1
                has_dates = True

    feats["ftg_races_last_14d"] = races_14d if (has_dates or state.last_race_dates) else None

    # ---- ftg_recovery_score ----
    # Exponential decay: sum of exp(-days/20) for last 5 races
    if race_date is not None and state.last_race_dates:
        score = 0.0
        count = 0
        for dt in reversed(state.last_race_dates):
            if count >= 5:
                break
            delta_days = (race_date - dt).days
            if delta_days > 0:
                score += math.exp(-delta_days / 20.0)
            count += 1
        feats["ftg_recovery_score"] = round(score, 4) if count > 0 else None
    else:
        feats["ftg_recovery_score"] = None

    # ---- ftg_win_rate_after_rest_bucket ----
    if days_since is not None:
        bucket = _classify_rest_bucket(days_since)
        stats = state.rest_bucket_stats.get(bucket)
        if stats and stats[1] > 0:
            feats["ftg_win_rate_after_rest_bucket"] = round(stats[0] / stats[1], 4)
        else:
            feats["ftg_win_rate_after_rest_bucket"] = None
    else:
        feats["ftg_win_rate_after_rest_bucket"] = None

    # ---- ftg_fatigue_trend ----
    # Compare last 2 positions vs earlier 2 positions
    # Higher position number = worse finish, so rising = fatigue
    if len(state.positions) >= 4:
        recent_2 = list(state.positions)[-2:]
        earlier_2 = list(state.positions)[-4:-2]
        avg_recent = sum(recent_2) / 2.0
        avg_earlier = sum(earlier_2) / 2.0
        # Positive = positions worsening (fatigue), negative = improving
        feats["ftg_fatigue_trend"] = round(avg_recent - avg_earlier, 2)
    elif len(state.positions) >= 2:
        # Not enough for full comparison, use None
        feats["ftg_fatigue_trend"] = None
    else:
        feats["ftg_fatigue_trend"] = None

    return feats


# ===========================================================================
# STATE UPDATE (post-race)
# ===========================================================================


def _update_state(
    state: _HorseState,
    race_date: Optional[datetime],
    distance: Optional[int],
    position: Optional[int],
    is_winner: bool,
    days_since: Optional[int],
) -> None:
    """Update horse state AFTER the race (post-snapshot)."""
    if race_date is not None:
        state.last_race_dates.append(race_date)
        if distance is not None and distance > 0:
            state.race_distances.append((race_date, distance))

    if position is not None and position > 0:
        state.positions.append(position)

    # Update rest bucket stats
    if days_since is not None:
        bucket = _classify_rest_bucket(days_since)
        if bucket not in state.rest_bucket_stats:
            state.rest_bucket_stats[bucket] = [0, 0]
        state.rest_bucket_stats[bucket][1] += 1
        if is_winner:
            state.rest_bucket_stats[bucket][0] += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek-based processing)
# ===========================================================================


def build_fatigue_features(input_path: Path, output_path: Path, logger) -> int:
    """Build fatigue/recovery features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Fatigue Model Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort the lightweight index --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_states: dict[str, _HorseState] = {}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "ftg_days_since_last",
        "ftg_optimal_rest",
        "ftg_is_fresh",
        "ftg_is_backed_up",
        "ftg_cumulative_distance_30d",
        "ftg_cumulative_distance_90d",
        "ftg_races_last_14d",
        "ftg_recovery_score",
        "ftg_win_rate_after_rest_bucket",
        "ftg_fatigue_trend",
    ]
    fill_counts = {k: 0 for k in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            race_date = _parse_date(course_date_str)

            # Read this course's records from disk
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                course_records.append(rec)

            # -- Snapshot pre-race stats, emit features, then update --
            post_updates: list[tuple[str, Optional[datetime], Optional[int], Optional[int], bool, Optional[int]]] = []

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval") or ""
                uid = rec.get("partant_uid")
                ecart_val = rec.get("ecart_precedent")

                # Parse distance
                dist_raw = rec.get("distance")
                distance: Optional[int] = None
                if dist_raw is not None:
                    try:
                        distance = int(dist_raw)
                    except (ValueError, TypeError):
                        distance = None

                # Parse position
                pos_raw = rec.get("position_arrivee")
                position: Optional[int] = None
                if pos_raw is not None:
                    try:
                        position = int(pos_raw)
                    except (ValueError, TypeError):
                        position = None

                is_winner = bool(rec.get("is_gagnant"))

                # Get or create state
                if horse_id not in horse_states:
                    horse_states[horse_id] = _HorseState()
                state = horse_states[horse_id]

                # SNAPSHOT: compute features from current state (before update)
                feats = _compute_features(state, race_date, ecart_val, distance)
                feats["partant_uid"] = uid

                # Track fill counts
                for k in feature_names:
                    if feats.get(k) is not None:
                        fill_counts[k] += 1

                # Stream to output
                fout.write(json.dumps(feats, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Parse days_since for state update
                days_since = _parse_ecart(ecart_val)
                if days_since is None and race_date is not None and state.last_race_dates:
                    last_dt = state.last_race_dates[-1]
                    delta = (race_date - last_dt).days
                    if delta >= 0:
                        days_since = delta

                post_updates.append((horse_id, race_date, distance, position, is_winner, days_since))

            # -- Update states AFTER all features for this course are emitted --
            for horse_id, rd, dist, pos, is_win, ds in post_updates:
                if horse_id:
                    _update_state(horse_states[horse_id], rd, dist, pos, is_win, ds)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Fatigue model build termine: %d features en %.1fs (chevaux uniques: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k in feature_names:
        v = fill_counts[k]
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features fatigue/recuperation a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=str(INPUT_PARTANTS),
        help="Chemin vers partants_master.jsonl",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("fatigue_model_builder")
    logger.info("=" * 70)
    logger.info("fatigue_model_builder.py -- Features fatigue & recuperation")
    logger.info("=" * 70)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "fatigue_model.jsonl"

    build_fatigue_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
