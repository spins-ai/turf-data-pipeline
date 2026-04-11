#!/usr/bin/env python3
"""
feature_builders.momentum_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Multi-timeframe momentum analysis inspired by trading indicators.

Reads partants_master.jsonl in streaming mode (index + chronological sort + seek),
processes all records chronologically, and computes per-partant momentum features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the momentum stats -- no future leakage.

Produces:
  - momentum_advanced_features.jsonl

Features per partant (10):
  - mom_position_ema_3       : EMA of finishing positions (span=3, most reactive)
  - mom_position_ema_10      : EMA of finishing positions (span=10, slower)
  - mom_ema_crossover        : ema_3 - ema_10 (negative = improving short-term form)
  - mom_speed_ema_3          : EMA of speed figures (span=3)
  - mom_speed_ema_10         : EMA of speed figures (span=10)
  - mom_speed_crossover      : speed_ema_3 - speed_ema_10
  - mom_win_rate_recent_vs_career : win rate last 10 minus career win rate
  - mom_place_acceleration   : places in last 5 minus places in previous 5
  - mom_golden_cross         : 1 if ema_3 just crossed below ema_10 (improving)
  - mom_death_cross          : 1 if ema_3 just crossed above ema_10 (declining)

EMA formula: ema_new = alpha * value + (1 - alpha) * ema_old
             alpha = 2 / (span + 1)

Usage:
    python feature_builders/momentum_advanced_builder.py
    python feature_builders/momentum_advanced_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/momentum_advanced")
OUTPUT_FILENAME = "momentum_advanced_features.jsonl"

_LOG_EVERY = 500_000

# EMA parameters
_ALPHA_3 = 2.0 / (3 + 1)    # 0.5
_ALPHA_10 = 2.0 / (10 + 1)  # ~0.1818

FEATURE_NAMES = [
    "mom_position_ema_3",
    "mom_position_ema_10",
    "mom_ema_crossover",
    "mom_speed_ema_3",
    "mom_speed_ema_10",
    "mom_speed_crossover",
    "mom_win_rate_recent_vs_career",
    "mom_place_acceleration",
    "mom_golden_cross",
    "mom_death_cross",
]


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Tracks running momentum state for a single horse."""

    __slots__ = (
        "ema_pos_3", "ema_pos_10",
        "ema_spd_3", "ema_spd_10",
        "prev_crossover",
        "career_wins", "career_total",
        "recent_wins_10",
        "recent_places_10",
    )

    def __init__(self) -> None:
        self.ema_pos_3: Optional[float] = None
        self.ema_pos_10: Optional[float] = None
        self.ema_spd_3: Optional[float] = None
        self.ema_spd_10: Optional[float] = None
        self.prev_crossover: Optional[float] = None  # previous ema_3 - ema_10 for position
        self.career_wins: int = 0
        self.career_total: int = 0
        self.recent_wins_10: deque = deque(maxlen=10)   # 1/0 per race
        self.recent_places_10: deque = deque(maxlen=10)  # finishing position per race


def _update_ema(old_ema: Optional[float], value: float, alpha: float) -> float:
    """Compute new EMA value. If no prior EMA, seed with current value."""
    if old_ema is None:
        return value
    return alpha * value + (1.0 - alpha) * old_ema


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_momentum_features(input_path: Path, output_path: Path, logger) -> int:
    """Build momentum features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Momentum Advanced Builder (index + sort + seek) ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    # (date_str, course_uid, num_pmu, byte_offset)
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line_stripped = line.strip()
            if not line_stripped:
                continue
            try:
                rec = json.loads(line_stripped)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexed %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 done: %d records indexed in %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Chronological sort in %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_states: dict[str, _HorseState] = {}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {name: 0 for name in FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(byte_offset: int) -> dict:
            fin.seek(byte_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields needed for momentum computation."""
            position = rec.get("place_arrivee") or rec.get("place") or rec.get("position")
            if position is not None:
                try:
                    position = int(position)
                except (ValueError, TypeError):
                    position = None

            speed = rec.get("speed_figure") or rec.get("vitesse_moyenne")
            if speed is not None:
                try:
                    speed = float(speed)
                except (ValueError, TypeError):
                    speed = None

            return {
                "partant_uid": rec.get("partant_uid"),
                "course_uid": rec.get("course_uid", ""),
                "date": rec.get("date_reunion_iso", ""),
                "cheval": rec.get("nom_cheval"),
                "position": position,
                "speed": speed,
                "gagnant": bool(rec.get("is_gagnant")),
            }

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

            # Read this course's records from disk
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race features (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                features: dict[str, Any] = {
                    "partant_uid": rec["partant_uid"],
                    "course_uid": rec["course_uid"],
                    "date_reunion_iso": rec["date"],
                }

                if not cheval or cheval not in horse_states:
                    # No history yet -- all None
                    for name in FEATURE_NAMES:
                        features[name] = None
                else:
                    st = horse_states[cheval]

                    # Position EMAs
                    features["mom_position_ema_3"] = round(st.ema_pos_3, 4) if st.ema_pos_3 is not None else None
                    features["mom_position_ema_10"] = round(st.ema_pos_10, 4) if st.ema_pos_10 is not None else None

                    # Position crossover
                    if st.ema_pos_3 is not None and st.ema_pos_10 is not None:
                        crossover = round(st.ema_pos_3 - st.ema_pos_10, 4)
                        features["mom_ema_crossover"] = crossover
                    else:
                        features["mom_ema_crossover"] = None

                    # Speed EMAs
                    features["mom_speed_ema_3"] = round(st.ema_spd_3, 4) if st.ema_spd_3 is not None else None
                    features["mom_speed_ema_10"] = round(st.ema_spd_10, 4) if st.ema_spd_10 is not None else None

                    # Speed crossover
                    if st.ema_spd_3 is not None and st.ema_spd_10 is not None:
                        features["mom_speed_crossover"] = round(st.ema_spd_3 - st.ema_spd_10, 4)
                    else:
                        features["mom_speed_crossover"] = None

                    # Win rate recent vs career
                    if st.career_total > 0 and len(st.recent_wins_10) > 0:
                        recent_wr = sum(st.recent_wins_10) / len(st.recent_wins_10)
                        career_wr = st.career_wins / st.career_total
                        features["mom_win_rate_recent_vs_career"] = round(recent_wr - career_wr, 4)
                    else:
                        features["mom_win_rate_recent_vs_career"] = None

                    # Place acceleration: last 5 places minus previous 5 places
                    places = list(st.recent_places_10)
                    if len(places) >= 10:
                        prev_5 = places[:5]   # older 5
                        last_5 = places[5:]   # recent 5
                        features["mom_place_acceleration"] = round(
                            sum(last_5) / 5.0 - sum(prev_5) / 5.0, 4
                        )
                    else:
                        features["mom_place_acceleration"] = None

                    # Golden cross / Death cross (based on position EMAs)
                    if st.ema_pos_3 is not None and st.ema_pos_10 is not None and st.prev_crossover is not None:
                        current_cross = st.ema_pos_3 - st.ema_pos_10
                        # Golden cross: ema_3 just crossed BELOW ema_10
                        # (previous crossover >= 0, current < 0) = improving
                        features["mom_golden_cross"] = 1 if (st.prev_crossover >= 0 and current_cross < 0) else 0
                        # Death cross: ema_3 just crossed ABOVE ema_10
                        # (previous crossover <= 0, current > 0) = declining
                        features["mom_death_cross"] = 1 if (st.prev_crossover <= 0 and current_cross > 0) else 0
                    else:
                        features["mom_golden_cross"] = None
                        features["mom_death_cross"] = None

                # Fill rate tracking
                for name in FEATURE_NAMES:
                    if features.get(name) is not None:
                        fill_counts[name] += 1

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER snapshotting (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                if not cheval:
                    continue

                if cheval not in horse_states:
                    horse_states[cheval] = _HorseState()
                st = horse_states[cheval]

                position = rec["position"]
                speed = rec["speed"]
                is_win = rec["gagnant"]

                # Save previous crossover before updating EMAs
                if st.ema_pos_3 is not None and st.ema_pos_10 is not None:
                    st.prev_crossover = st.ema_pos_3 - st.ema_pos_10

                # Update position EMAs
                if position is not None and position > 0:
                    st.ema_pos_3 = _update_ema(st.ema_pos_3, float(position), _ALPHA_3)
                    st.ema_pos_10 = _update_ema(st.ema_pos_10, float(position), _ALPHA_10)
                    st.recent_places_10.append(position)

                # Update speed EMAs
                if speed is not None and speed > 0:
                    st.ema_spd_3 = _update_ema(st.ema_spd_3, speed, _ALPHA_3)
                    st.ema_spd_10 = _update_ema(st.ema_spd_10, speed, _ALPHA_10)

                # Update career and recent win tracking
                st.career_total += 1
                if is_win:
                    st.career_wins += 1
                st.recent_wins_10.append(1 if is_win else 0)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Processed %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Momentum Advanced build done: %d features in %.1fs (horses tracked: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for name in FEATURE_NAMES:
        v = fill_counts[name]
        pct = 100.0 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", name, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Input file not found: {p}")
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Input file not found: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Build advanced momentum features from partants_master.jsonl"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Path to partants_master.jsonl (default: auto-detect)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("momentum_advanced_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_momentum_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
