#!/usr/bin/env python3
"""
feature_builders.regime_detection_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Regime detection features -- identifying different market/performance
regimes over rolling windows of recent races.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically course-by-course, and computes per-partant regime features
using GLOBAL rolling deques (across all races, not per-horse).

Temporal integrity: for any partant at date D, only completed races
with date < D contribute to rolling regime stats -- no future leakage.
Snapshot BEFORE update.

Produces:
  - regime_detection.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/regime_detection/

Features per partant (8):
  - rgm_fav_win_regime           : rolling 50-race favourite win rate
                                   (high = predictable, low = chaos)
  - rgm_longshot_regime          : rolling 50-race longshot (cote>15) win rate
  - rgm_avg_field_size_regime    : rolling 100-race average field size
  - rgm_avg_odds_regime          : rolling 100-race average favourite odds
  - rgm_upset_regime             : rolling 50-race rate of favourites NOT winning
  - rgm_regime_label             : categorical 0=chalk (fav>35%), 1=normal (25-35%),
                                   2=chaos (<25%)
  - rgm_market_efficiency_regime : rolling correlation between odds rank and finish
                                   rank over 100 races
  - rgm_regime_shift_signal      : 1 if current regime_label differs from
                                   50-races-ago label

Usage:
    python feature_builders/regime_detection_builder.py
    python feature_builders/regime_detection_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/regime_detection")

_LOG_EVERY = 500_000

# Rolling window sizes
_SHORT_WINDOW = 50    # for fav_win, longshot, upset
_LONG_WINDOW = 100    # for field_size, avg_odds, market_efficiency


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v and v > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _pearson_correlation(xs: list[float], ys: list[float]) -> Optional[float]:
    """Compute Pearson correlation between two lists. Returns None if < 5 pairs."""
    n = min(len(xs), len(ys))
    if n < 5:
        return None
    sx = sum(xs[:n])
    sy = sum(ys[:n])
    sxx = sum(x * x for x in xs[:n])
    syy = sum(y * y for y in ys[:n])
    sxy = sum(xs[i] * ys[i] for i in range(n))
    denom = math.sqrt((n * sxx - sx * sx) * (n * syy - sy * sy))
    if denom == 0:
        return None
    return round((n * sxy - sx * sy) / denom, 4)


# ===========================================================================
# GLOBAL REGIME STATE
# ===========================================================================


class _RegimeState:
    """Global rolling state across all races for regime detection.

    Maintains deques of recent race outcomes:
      - fav_won (bool): did the favourite win this race?
      - longshot_won (bool): did a longshot (cote > 15) win?
      - field_size (int): number of partants in the race
      - fav_odds (float): favourite's final odds
      - upset (bool): favourite did NOT win
      - odds_ranks (list[float]): odds-based rank for each finisher
      - finish_ranks (list[float]): actual finish rank for each finisher
      - regime_labels (deque): rolling regime labels for shift detection
    """

    __slots__ = (
        "fav_won", "longshot_won", "field_size", "fav_odds", "upset",
        "odds_rank_list", "finish_rank_list", "regime_labels",
    )

    def __init__(self) -> None:
        self.fav_won: deque[int] = deque(maxlen=_SHORT_WINDOW)
        self.longshot_won: deque[int] = deque(maxlen=_SHORT_WINDOW)
        self.field_size: deque[int] = deque(maxlen=_LONG_WINDOW)
        self.fav_odds: deque[float] = deque(maxlen=_LONG_WINDOW)
        self.upset: deque[int] = deque(maxlen=_SHORT_WINDOW)
        # For market efficiency: store (odds_rank, finish_rank) pairs per race winner
        self.odds_rank_list: deque[float] = deque(maxlen=_LONG_WINDOW)
        self.finish_rank_list: deque[float] = deque(maxlen=_LONG_WINDOW)
        # For regime shift detection
        self.regime_labels: deque[int] = deque(maxlen=_SHORT_WINDOW)

    def snapshot(self) -> dict[str, Any]:
        """Capture current regime features BEFORE updating with new race."""
        feats: dict[str, Any] = {
            "rgm_fav_win_regime": None,
            "rgm_longshot_regime": None,
            "rgm_avg_field_size_regime": None,
            "rgm_avg_odds_regime": None,
            "rgm_upset_regime": None,
            "rgm_regime_label": None,
            "rgm_market_efficiency_regime": None,
            "rgm_regime_shift_signal": None,
        }

        # --- Short window features (50 races) ---
        if len(self.fav_won) >= 5:
            fav_wr = sum(self.fav_won) / len(self.fav_won)
            feats["rgm_fav_win_regime"] = round(fav_wr, 4)

            # Regime label based on fav win rate
            if fav_wr > 0.35:
                feats["rgm_regime_label"] = 0   # chalk
            elif fav_wr >= 0.25:
                feats["rgm_regime_label"] = 1   # normal
            else:
                feats["rgm_regime_label"] = 2   # chaos

        if len(self.longshot_won) >= 5:
            feats["rgm_longshot_regime"] = round(
                sum(self.longshot_won) / len(self.longshot_won), 4
            )

        if len(self.upset) >= 5:
            feats["rgm_upset_regime"] = round(
                sum(self.upset) / len(self.upset), 4
            )

        # --- Long window features (100 races) ---
        if len(self.field_size) >= 5:
            feats["rgm_avg_field_size_regime"] = round(
                sum(self.field_size) / len(self.field_size), 2
            )

        if len(self.fav_odds) >= 5:
            feats["rgm_avg_odds_regime"] = round(
                sum(self.fav_odds) / len(self.fav_odds), 4
            )

        # --- Market efficiency (correlation odds rank vs finish rank) ---
        if len(self.odds_rank_list) >= 10:
            corr = _pearson_correlation(
                list(self.odds_rank_list), list(self.finish_rank_list)
            )
            feats["rgm_market_efficiency_regime"] = corr

        # --- Regime shift signal ---
        if (
            feats["rgm_regime_label"] is not None
            and len(self.regime_labels) >= _SHORT_WINDOW
        ):
            old_label = self.regime_labels[0]  # 50 races ago
            feats["rgm_regime_shift_signal"] = (
                1 if feats["rgm_regime_label"] != old_label else 0
            )

        return feats

    def update(
        self,
        fav_won: bool,
        longshot_won: bool,
        field_sz: int,
        fav_odds_val: Optional[float],
        odds_ranks: list[float],
        finish_ranks: list[float],
    ) -> None:
        """Update rolling state with a completed race."""
        self.fav_won.append(1 if fav_won else 0)
        self.longshot_won.append(1 if longshot_won else 0)
        self.upset.append(0 if fav_won else 1)
        self.field_size.append(field_sz)
        if fav_odds_val is not None:
            self.fav_odds.append(fav_odds_val)

        # Store per-runner odds rank vs finish rank pairs
        for orank, frank in zip(odds_ranks, finish_ranks):
            self.odds_rank_list.append(orank)
            self.finish_rank_list.append(frank)

        # Store current regime label for shift detection
        if len(self.fav_won) >= 5:
            fav_wr = sum(self.fav_won) / len(self.fav_won)
            if fav_wr > 0.35:
                self.regime_labels.append(0)
            elif fav_wr >= 0.25:
                self.regime_labels.append(1)
            else:
                self.regime_labels.append(2)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_regime_detection_features(input_path: Path, output_path: Path, logger) -> int:
    """Build regime detection features from partants_master.jsonl.

    Two-phase approach:
      1. Index + sort chronologically (lightweight tuples).
      2. Process course-by-course, snapshot BEFORE update, stream to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Regime Detection Builder ===")
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
    regime = _RegimeState()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts: dict[str, int] = {
        "rgm_fav_win_regime": 0,
        "rgm_longshot_regime": 0,
        "rgm_avg_field_size_regime": 0,
        "rgm_avg_odds_regime": 0,
        "rgm_upset_regime": 0,
        "rgm_regime_label": 0,
        "rgm_market_efficiency_regime": 0,
        "rgm_regime_shift_signal": 0,
    }

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

            # --- Snapshot BEFORE update (temporal integrity) ---
            regime_feats = regime.snapshot()

            for rec in course_group:
                features: dict[str, Any] = {"partant_uid": rec.get("partant_uid")}
                features.update(regime_feats)

                # Track fill rates
                for k in fill_counts:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # --- Update regime state AFTER snapshot ---
            # Determine race outcome: who won, favourite, longshot, etc.
            field_size = len(course_group)

            # Find favourite (lowest cote_finale) and winner
            best_cote = None
            fav_uid = None
            winner_uid = None
            winner_cote = None

            runners_with_odds: list[tuple[str, float]] = []
            runners_with_finish: list[tuple[str, int]] = []

            for rec in course_group:
                uid = rec.get("partant_uid")
                cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference"))
                is_winner = bool(rec.get("is_gagnant"))
                cl_arrivee = _safe_int(rec.get("cl_arrivee"))

                if cote is not None:
                    runners_with_odds.append((uid, cote))
                    if best_cote is None or cote < best_cote:
                        best_cote = cote
                        fav_uid = uid

                if is_winner:
                    winner_uid = uid
                    winner_cote = cote

                if cl_arrivee is not None and cote is not None:
                    runners_with_finish.append((uid, cote, cl_arrivee))

            # Did favourite win?
            fav_won = (fav_uid is not None and winner_uid is not None and fav_uid == winner_uid)

            # Did a longshot win?
            longshot_won = (winner_cote is not None and winner_cote > 15.0)

            # Odds ranks vs finish ranks for market efficiency
            odds_ranks: list[float] = []
            finish_ranks: list[float] = []
            if runners_with_finish:
                # Sort by odds ascending to get odds rank
                sorted_by_odds = sorted(runners_with_finish, key=lambda x: x[1])
                uid_to_odds_rank = {uid: rank + 1 for rank, (uid, _, _) in enumerate(sorted_by_odds)}
                for uid, _, cl in runners_with_finish:
                    odds_ranks.append(float(uid_to_odds_rank[uid]))
                    finish_ranks.append(float(cl))

            regime.update(
                fav_won=fav_won,
                longshot_won=longshot_won,
                field_sz=field_size,
                fav_odds_val=best_cote,
                odds_ranks=odds_ranks,
                finish_ranks=finish_ranks,
            )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Regime detection build termine: %d features en %.1fs",
        n_written, elapsed,
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
        description="Construction des features regime detection a partir de partants_master"
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

    logger = setup_logging("regime_detection_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "regime_detection.jsonl"
    build_regime_detection_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
