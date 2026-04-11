#!/usr/bin/env python3
"""
feature_builders.volatility_surface_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Odds volatility surface features -- modeling how volatile each horse's
odds are across recent races and what the current market move implies.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant volatility features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the volatility stats -- no future leakage.

Produces:
  - volatility_surface.jsonl   in builder_outputs/volatility_surface/

Features per partant (8):
  - vol_odds_range             : max(odds) - min(odds) from horse's last 5 races
  - vol_odds_cv                : coefficient of variation of horse's last 5 odds (std/mean)
  - vol_odds_trend_slope       : simple linear slope of odds over last 5 races
                                 (rising = losing form, falling = gaining)
  - vol_market_move            : (cote_finale - cote_reference) / cote_reference for current race
  - vol_market_move_abs        : absolute value of market move
  - vol_horse_avg_market_move  : average absolute market move for this horse historically
  - vol_is_steam_horse         : 1 if cote_finale < cote_reference * 0.85
  - vol_is_drifter             : 1 if cote_finale > cote_reference * 1.2

Memory-optimised version:
  - Phase 1 reads only minimal tuples (not full dicts) for sorting
  - Phase 2 streams output to disk instead of accumulating in a list
  - gc.collect() called every 500K records

Usage:
    python feature_builders/volatility_surface_builder.py
    python feature_builders/volatility_surface_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/volatility_surface")

# Progress log every N records
_LOG_EVERY = 500_000

# Thresholds
_STEAM_THRESHOLD = 0.85   # cote_finale < cote_reference * 0.85
_DRIFT_THRESHOLD = 1.20   # cote_finale > cote_reference * 1.20
_ODDS_HISTORY_LEN = 5
_MARKET_MOVES_HISTORY_LEN = 10


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseVolState:
    """Per-horse volatility state tracker.

    Maintains:
      - odds_history: deque(maxlen=5) of recent cote_finale values
      - market_moves_history: deque(maxlen=10) of abs(market_move) values
    """

    __slots__ = ("odds_history", "market_moves_history")

    def __init__(self) -> None:
        self.odds_history: deque = deque(maxlen=_ODDS_HISTORY_LEN)
        self.market_moves_history: deque = deque(maxlen=_MARKET_MOVES_HISTORY_LEN)

    def snapshot_odds_range(self) -> Optional[float]:
        """max(odds) - min(odds) from history."""
        if len(self.odds_history) < 2:
            return None
        return round(max(self.odds_history) - min(self.odds_history), 4)

    def snapshot_odds_cv(self) -> Optional[float]:
        """Coefficient of variation: std / mean."""
        if len(self.odds_history) < 2:
            return None
        n = len(self.odds_history)
        mean = sum(self.odds_history) / n
        if mean <= 0:
            return None
        variance = sum((x - mean) ** 2 for x in self.odds_history) / n
        std = math.sqrt(variance)
        return round(std / mean, 4)

    def snapshot_odds_trend_slope(self) -> Optional[float]:
        """Simple linear regression slope over the odds history.

        x = 0, 1, 2, ... (oldest to newest)
        slope = cov(x, y) / var(x)
        Positive slope = odds rising (losing form).
        Negative slope = odds falling (gaining form).
        """
        n = len(self.odds_history)
        if n < 2:
            return None
        # x values: 0..n-1
        x_mean = (n - 1) / 2.0
        y_mean = sum(self.odds_history) / n
        cov_xy = 0.0
        var_x = 0.0
        for i, y in enumerate(self.odds_history):
            dx = i - x_mean
            cov_xy += dx * (y - y_mean)
            var_x += dx * dx
        if var_x == 0:
            return None
        return round(cov_xy / var_x, 4)

    def snapshot_avg_market_move(self) -> Optional[float]:
        """Average absolute market move from history."""
        if not self.market_moves_history:
            return None
        return round(sum(self.market_moves_history) / len(self.market_moves_history), 4)


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort + seek-based processing)
# ===========================================================================


def build_volatility_surface_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build volatility surface features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Volatility Surface Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseVolState] = defaultdict(_HorseVolState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "vol_odds_range": 0,
        "vol_odds_cv": 0,
        "vol_odds_trend_slope": 0,
        "vol_market_move": 0,
        "vol_market_move_abs": 0,
        "vol_horse_avg_market_move": 0,
        "vol_is_steam_horse": 0,
        "vol_is_drifter": 0,
    }

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

            # Read only this course's records from disk
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                course_records.append(rec)

            # -- Snapshot pre-race stats and emit features --
            post_updates: list[tuple[str, Optional[float], Optional[float]]] = []

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                partant_uid = rec.get("partant_uid")

                cote_finale = rec.get("cote_finale")
                cote_reference = rec.get("cote_reference")

                # Parse odds safely
                try:
                    cote_finale = float(cote_finale) if cote_finale is not None else None
                    if cote_finale is not None and cote_finale <= 0:
                        cote_finale = None
                except (ValueError, TypeError):
                    cote_finale = None

                try:
                    cote_reference = float(cote_reference) if cote_reference is not None else None
                    if cote_reference is not None and cote_reference <= 0:
                        cote_reference = None
                except (ValueError, TypeError):
                    cote_reference = None

                features: dict[str, Any] = {"partant_uid": partant_uid}

                # --- Historical features (snapshot BEFORE update) ---
                if horse_id:
                    hs = horse_state[horse_id]

                    # 1. vol_odds_range
                    odds_range = hs.snapshot_odds_range()
                    features["vol_odds_range"] = odds_range
                    if odds_range is not None:
                        fill_counts["vol_odds_range"] += 1

                    # 2. vol_odds_cv
                    odds_cv = hs.snapshot_odds_cv()
                    features["vol_odds_cv"] = odds_cv
                    if odds_cv is not None:
                        fill_counts["vol_odds_cv"] += 1

                    # 3. vol_odds_trend_slope
                    trend_slope = hs.snapshot_odds_trend_slope()
                    features["vol_odds_trend_slope"] = trend_slope
                    if trend_slope is not None:
                        fill_counts["vol_odds_trend_slope"] += 1

                    # 6. vol_horse_avg_market_move
                    avg_mm = hs.snapshot_avg_market_move()
                    features["vol_horse_avg_market_move"] = avg_mm
                    if avg_mm is not None:
                        fill_counts["vol_horse_avg_market_move"] += 1
                else:
                    features["vol_odds_range"] = None
                    features["vol_odds_cv"] = None
                    features["vol_odds_trend_slope"] = None
                    features["vol_horse_avg_market_move"] = None

                # --- Current race features ---
                # 4. vol_market_move
                market_move: Optional[float] = None
                if cote_finale is not None and cote_reference is not None and cote_reference > 0:
                    market_move = round((cote_finale - cote_reference) / cote_reference, 4)
                features["vol_market_move"] = market_move
                if market_move is not None:
                    fill_counts["vol_market_move"] += 1

                # 5. vol_market_move_abs
                market_move_abs: Optional[float] = None
                if market_move is not None:
                    market_move_abs = round(abs(market_move), 4)
                features["vol_market_move_abs"] = market_move_abs
                if market_move_abs is not None:
                    fill_counts["vol_market_move_abs"] += 1

                # 7. vol_is_steam_horse
                is_steam: Optional[int] = None
                if cote_finale is not None and cote_reference is not None and cote_reference > 0:
                    is_steam = 1 if cote_finale < cote_reference * _STEAM_THRESHOLD else 0
                features["vol_is_steam_horse"] = is_steam
                if is_steam is not None:
                    fill_counts["vol_is_steam_horse"] += 1

                # 8. vol_is_drifter
                is_drifter: Optional[int] = None
                if cote_finale is not None and cote_reference is not None and cote_reference > 0:
                    is_drifter = 1 if cote_finale > cote_reference * _DRIFT_THRESHOLD else 0
                features["vol_is_drifter"] = is_drifter
                if is_drifter is not None:
                    fill_counts["vol_is_drifter"] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                # Prepare deferred update
                post_updates.append((horse_id, cote_finale, market_move_abs))

            # -- Update states after race (post-race, no leakage) --
            for horse_id, cote_finale, mm_abs in post_updates:
                if not horse_id:
                    continue
                hs = horse_state[horse_id]
                if cote_finale is not None:
                    hs.odds_history.append(cote_finale)
                if mm_abs is not None:
                    hs.market_moves_history.append(mm_abs)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Volatility surface build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features volatilite odds a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("volatility_surface_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "volatility_surface.jsonl"
    build_volatility_surface_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
