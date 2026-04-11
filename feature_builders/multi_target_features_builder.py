#!/usr/bin/env python3
"""
feature_builders.multi_target_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Multi-target prediction features -- features designed to help predict
multiple outcomes (win, place, exacta, etc.).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically course-by-course, and computes per-partant multi-target
features combining historical rates with per-race context.

Temporal integrity: for any partant at date D, only races with date < D
contribute to historical rates -- no future leakage.
Snapshot BEFORE update.

Produces:
  - multi_target_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/multi_target_features/

Features per partant (10):
  - mtf_horse_win_rate         : career win rate from past data
  - mtf_horse_place_rate       : career place rate (top 3) from past
  - mtf_horse_top5_rate        : career top 5 rate from past
  - mtf_horse_exacta_rate      : how often horse finishes 1st or 2nd
  - mtf_horse_show_consistency : std of (is_place) over last 10 races
                                  (lower = more consistent placer)
  - mtf_horse_win_given_place  : P(win | placed) = wins / places historically
  - mtf_horse_overperform_rate : fraction of races finishing better than
                                  odds rank implied
  - mtf_horse_underperform_rate: fraction finishing worse
  - mtf_horse_expected_position: historical average position normalized
                                  by field size
  - mtf_horse_position_vs_expected: last race position - expected_position
                                  (positive = underperformed)

State per horse: wins, places, top5, exacta, total, positions deque(10),
is_placed deque(10), odds_ranks deque(10), finish_ranks deque(10),
overperform_count, underperform_count, total_ranked.

Usage:
    python feature_builders/multi_target_features_builder.py
    python feature_builders/multi_target_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/multi_target_features")

_LOG_EVERY = 500_000

_DEQUE_SIZE = 10


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _stdev(values: deque) -> Optional[float]:
    """Standard deviation with at least 2 values required."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / (n - 1)
    return round(math.sqrt(var), 6)


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseMultiTargetState:
    """Track per-horse multi-target historical state."""

    __slots__ = (
        "wins", "places", "top5", "exacta", "total",
        "positions", "is_placed",
        "odds_ranks", "finish_ranks",
        "overperform_count", "underperform_count", "total_ranked",
        "norm_positions",
    )

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0       # top 3
        self.top5: int = 0
        self.exacta: int = 0       # 1st or 2nd
        self.total: int = 0
        self.positions: deque = deque(maxlen=_DEQUE_SIZE)
        self.is_placed: deque = deque(maxlen=_DEQUE_SIZE)
        self.odds_ranks: deque = deque(maxlen=_DEQUE_SIZE)
        self.finish_ranks: deque = deque(maxlen=_DEQUE_SIZE)
        self.overperform_count: int = 0
        self.underperform_count: int = 0
        self.total_ranked: int = 0
        self.norm_positions: list[float] = []  # position / field_size, all history

    def snapshot(self) -> dict[str, Any]:
        """Capture features BEFORE updating with current race."""
        # Win rate
        win_rate = round(self.wins / self.total, 6) if self.total >= 1 else None
        # Place rate (top 3)
        place_rate = round(self.places / self.total, 6) if self.total >= 1 else None
        # Top 5 rate
        top5_rate = round(self.top5 / self.total, 6) if self.total >= 1 else None
        # Exacta rate (1st or 2nd)
        exacta_rate = round(self.exacta / self.total, 6) if self.total >= 1 else None
        # Show consistency: std of is_placed over last 10
        show_consistency = _stdev(self.is_placed)
        # P(win | placed)
        win_given_place = round(self.wins / self.places, 6) if self.places >= 1 else None
        # Overperform / underperform rates
        over_rate = round(self.overperform_count / self.total_ranked, 6) if self.total_ranked >= 1 else None
        under_rate = round(self.underperform_count / self.total_ranked, 6) if self.total_ranked >= 1 else None
        # Expected position (normalized)
        expected_pos = None
        if self.norm_positions:
            expected_pos = round(sum(self.norm_positions) / len(self.norm_positions), 6)
        # Position vs expected: last race position (normalized) - expected
        pos_vs_expected = None
        if expected_pos is not None and self.norm_positions:
            last_norm = self.norm_positions[-1]
            pos_vs_expected = round(last_norm - expected_pos, 6)

        return {
            "mtf_horse_win_rate": win_rate,
            "mtf_horse_place_rate": place_rate,
            "mtf_horse_top5_rate": top5_rate,
            "mtf_horse_exacta_rate": exacta_rate,
            "mtf_horse_show_consistency": show_consistency,
            "mtf_horse_win_given_place": win_given_place,
            "mtf_horse_overperform_rate": over_rate,
            "mtf_horse_underperform_rate": under_rate,
            "mtf_horse_expected_position": expected_pos,
            "mtf_horse_position_vs_expected": pos_vs_expected,
        }

    def update(
        self,
        finish_pos: Optional[int],
        field_size: Optional[int],
        is_winner: bool,
        is_placed: bool,
        is_top5: bool,
        is_exacta: bool,
        odds_rank: Optional[int],
    ) -> None:
        """Update state AFTER feature extraction."""
        self.total += 1
        if is_winner:
            self.wins += 1
        if is_placed:
            self.places += 1
        if is_top5:
            self.top5 += 1
        if is_exacta:
            self.exacta += 1

        # Deques
        self.is_placed.append(1.0 if is_placed else 0.0)

        if finish_pos is not None:
            self.positions.append(finish_pos)
            self.finish_ranks.append(finish_pos)

            # Normalized position
            if field_size is not None and field_size > 0:
                self.norm_positions.append(finish_pos / field_size)

        if odds_rank is not None:
            self.odds_ranks.append(odds_rank)

        # Over/underperform: compare finish position to odds-implied rank
        if finish_pos is not None and odds_rank is not None:
            self.total_ranked += 1
            if finish_pos < odds_rank:
                self.overperform_count += 1
            elif finish_pos > odds_rank:
                self.underperform_count += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_multi_target_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build multi-target prediction features from partants_master.jsonl.

    Two-phase approach:
      1. Index + sort chronologically (lightweight tuples).
      2. Process course-by-course via seek, snapshot BEFORE update,
         stream to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Multi-Target Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date, course_uid, num_pmu, offset) --
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

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseMultiTargetState] = defaultdict(_HorseMultiTargetState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "mtf_horse_win_rate",
        "mtf_horse_place_rate",
        "mtf_horse_top5_rate",
        "mtf_horse_exacta_rate",
        "mtf_horse_show_consistency",
        "mtf_horse_win_given_place",
        "mtf_horse_overperform_rate",
        "mtf_horse_underperform_rate",
        "mtf_horse_expected_position",
        "mtf_horse_position_vs_expected",
    ]
    fill_counts: dict[str, int] = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
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

            # Read this course's records from disk
            course_group = [_read_record_at(index[ci][3]) for ci in course_indices]
            field_size = len(course_group)

            # --- Compute odds-based rank for this race ---
            # Build list of (idx, cote_finale) for ranking
            odds_for_rank: list[tuple[int, float]] = []
            for idx_r, rec in enumerate(course_group):
                cote = _safe_float(rec.get("cote_finale"))
                if cote is not None and cote > 0:
                    odds_for_rank.append((idx_r, cote))

            # Sort by cote ascending (lowest odds = favourite = rank 1)
            odds_for_rank.sort(key=lambda x: x[1])
            odds_rank_map: dict[int, int] = {}
            for rank, (idx_r, _) in enumerate(odds_for_rank, 1):
                odds_rank_map[idx_r] = rank

            # --- Snapshot BEFORE update (temporal integrity) ---
            post_updates: list[tuple[str, Optional[int], Optional[int], bool, bool, bool, bool, Optional[int]]] = []

            for idx_r, rec in enumerate(course_group):
                cheval = rec.get("nom_cheval") or ""
                uid = rec.get("partant_uid")
                is_winner = bool(rec.get("is_gagnant"))

                # Determine finish position
                finish_pos = _safe_int(rec.get("place_officielle"))
                if finish_pos is None:
                    finish_pos = _safe_int(rec.get("position_arrivee"))

                is_placed_flag = finish_pos is not None and finish_pos <= 3
                is_top5_flag = finish_pos is not None and finish_pos <= 5
                is_exacta_flag = finish_pos is not None and finish_pos <= 2

                odds_rank = odds_rank_map.get(idx_r)

                # Snapshot features BEFORE update
                features: dict[str, Any] = {"partant_uid": uid}

                if cheval:
                    snap = horse_state[cheval].snapshot()
                    features.update(snap)
                else:
                    for k in feature_keys:
                        features[k] = None

                # Track fill rates
                for k in feature_keys:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Queue post-race update
                post_updates.append((
                    cheval, finish_pos, field_size,
                    is_winner, is_placed_flag, is_top5_flag, is_exacta_flag,
                    odds_rank,
                ))

            # --- Update horse state AFTER snapshot ---
            for cheval, finish_pos, fs, is_win, is_pl, is_t5, is_ex, odds_r in post_updates:
                if cheval:
                    horse_state[cheval].update(
                        finish_pos, fs, is_win, is_pl, is_t5, is_ex, odds_r,
                    )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Multi-target features build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features multi-target prediction a partir de partants_master"
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

    logger = setup_logging("multi_target_features_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "multi_target_features.jsonl"
    build_multi_target_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
