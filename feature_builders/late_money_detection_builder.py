#!/usr/bin/env python3
"""
feature_builders.late_money_detection_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Late money detection features -- detecting smart money signals from
odds movements between cote_reference (opening) and cote_finale (final).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically course-by-course, and computes per-partant late money
features combining current-race data with horse-level historical state.

Temporal integrity: for any partant at date D, only races with date < D
contribute to historical win rates and persistence counts -- no future
leakage.  Snapshot BEFORE update.

Produces:
  - late_money_detection.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/late_money_detection/

Features per partant (8):
  - lmd_cote_drop_pct          : (cote_reference - cote_finale) / cote_reference * 100
                                  Positive = odds shortened (money came in).
  - lmd_is_significant_drop    : 1 if cote dropped > 20%
  - lmd_is_significant_rise    : 1 if cote rose > 25%
  - lmd_horse_drop_win_rate    : horse's historical win rate when cote drops > 15%
  - lmd_horse_rise_win_rate    : horse's historical win rate when cote rises > 15%
  - lmd_smart_money_persistence: count of consecutive races where this horse
                                  had a cote drop > 10%
  - lmd_field_biggest_mover    : 1 if this horse had the biggest absolute cote
                                  change in the race
  - lmd_drop_vs_field          : horse's cote_drop_pct minus average cote_drop_pct
                                  in the race (bigger = more targeted money)

State per horse: drop_wins, drop_total, rise_wins, rise_total, consecutive_drops.
Process course-by-course for race-level features.

Usage:
    python feature_builders/late_money_detection_builder.py
    python feature_builders/late_money_detection_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/late_money_detection")

_LOG_EVERY = 500_000

# Thresholds
_SIGNIFICANT_DROP_PCT = 20.0   # > 20% drop = significant
_SIGNIFICANT_RISE_PCT = 25.0   # > 25% rise = significant
_HISTORICAL_DROP_THRESH = 15.0 # > 15% drop for historical win rate tracking
_HISTORICAL_RISE_THRESH = 15.0 # > 15% rise for historical win rate tracking
_PERSISTENCE_THRESH = 10.0     # > 10% drop counts toward consecutive streak


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


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseSmartMoneyState:
    """Track per-horse late money historical state."""

    __slots__ = ("drop_wins", "drop_total", "rise_wins", "rise_total",
                 "consecutive_drops")

    def __init__(self) -> None:
        self.drop_wins: int = 0
        self.drop_total: int = 0
        self.rise_wins: int = 0
        self.rise_total: int = 0
        self.consecutive_drops: int = 0

    def snapshot(self) -> dict[str, Any]:
        """Capture historical features BEFORE updating with current race."""
        drop_wr = round(self.drop_wins / self.drop_total, 4) if self.drop_total >= 1 else None
        rise_wr = round(self.rise_wins / self.rise_total, 4) if self.rise_total >= 1 else None
        return {
            "lmd_horse_drop_win_rate": drop_wr,
            "lmd_horse_rise_win_rate": rise_wr,
            "lmd_smart_money_persistence": self.consecutive_drops,
        }

    def update(self, drop_pct: Optional[float], is_winner: bool) -> None:
        """Update state AFTER feature extraction."""
        if drop_pct is None:
            return

        # Track drop history (> 15% drop)
        if drop_pct > _HISTORICAL_DROP_THRESH:
            self.drop_total += 1
            if is_winner:
                self.drop_wins += 1

        # Track rise history (> 15% rise, i.e. drop_pct < -15)
        if drop_pct < -_HISTORICAL_RISE_THRESH:
            self.rise_total += 1
            if is_winner:
                self.rise_wins += 1

        # Consecutive drop streak (> 10% drop)
        if drop_pct > _PERSISTENCE_THRESH:
            self.consecutive_drops += 1
        else:
            self.consecutive_drops = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_late_money_detection_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build late money detection features from partants_master.jsonl.

    Two-phase approach:
      1. Index + sort chronologically (lightweight tuples).
      2. Process course-by-course, snapshot BEFORE update, stream to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Late Money Detection Builder ===")
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
    horse_state: dict[str, _HorseSmartMoneyState] = defaultdict(_HorseSmartMoneyState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts: dict[str, int] = {
        "lmd_cote_drop_pct": 0,
        "lmd_is_significant_drop": 0,
        "lmd_is_significant_rise": 0,
        "lmd_horse_drop_win_rate": 0,
        "lmd_horse_rise_win_rate": 0,
        "lmd_smart_money_persistence": 0,
        "lmd_field_biggest_mover": 0,
        "lmd_drop_vs_field": 0,
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

            # --- Compute per-runner cote_drop_pct for race-level features ---
            runner_drops: list[tuple[str, Optional[float], float]] = []
            # (horse_name, drop_pct, abs_change)

            for rec in course_group:
                cheval = rec.get("nom_cheval") or ""
                cote_ref = _safe_float(rec.get("cote_reference"))
                cote_fin = _safe_float(rec.get("cote_finale"))

                drop_pct: Optional[float] = None
                abs_change: float = 0.0
                if cote_ref is not None and cote_fin is not None:
                    drop_pct = round((cote_ref - cote_fin) / cote_ref * 100, 4)
                    abs_change = abs(cote_ref - cote_fin)

                runner_drops.append((cheval, drop_pct, abs_change))

            # Field average drop
            valid_drops = [d for _, d, _ in runner_drops if d is not None]
            field_avg_drop = sum(valid_drops) / len(valid_drops) if valid_drops else None

            # Biggest mover (max absolute change)
            max_abs_change = max((ac for _, _, ac in runner_drops), default=0.0)

            # --- Snapshot BEFORE update (temporal integrity) ---
            post_updates: list[tuple[str, Optional[float], bool]] = []

            for idx_r, rec in enumerate(course_group):
                cheval = rec.get("nom_cheval") or ""
                is_winner = bool(rec.get("is_gagnant"))
                uid = rec.get("partant_uid")

                _horse_name, drop_pct, abs_change = runner_drops[idx_r]

                # Current-race features
                features: dict[str, Any] = {"partant_uid": uid}
                features["lmd_cote_drop_pct"] = drop_pct

                if drop_pct is not None:
                    features["lmd_is_significant_drop"] = 1 if drop_pct > _SIGNIFICANT_DROP_PCT else 0
                    features["lmd_is_significant_rise"] = 1 if drop_pct < -_SIGNIFICANT_RISE_PCT else 0
                else:
                    features["lmd_is_significant_drop"] = None
                    features["lmd_is_significant_rise"] = None

                # Historical per-horse features (snapshot before update)
                if cheval:
                    snap = horse_state[cheval].snapshot()
                    features["lmd_horse_drop_win_rate"] = snap["lmd_horse_drop_win_rate"]
                    features["lmd_horse_rise_win_rate"] = snap["lmd_horse_rise_win_rate"]
                    features["lmd_smart_money_persistence"] = snap["lmd_smart_money_persistence"]
                else:
                    features["lmd_horse_drop_win_rate"] = None
                    features["lmd_horse_rise_win_rate"] = None
                    features["lmd_smart_money_persistence"] = 0

                # Race-level: biggest mover
                if drop_pct is not None and max_abs_change > 0:
                    features["lmd_field_biggest_mover"] = 1 if abs_change >= max_abs_change else 0
                else:
                    features["lmd_field_biggest_mover"] = None

                # Race-level: drop vs field average
                if drop_pct is not None and field_avg_drop is not None:
                    features["lmd_drop_vs_field"] = round(drop_pct - field_avg_drop, 4)
                else:
                    features["lmd_drop_vs_field"] = None

                # Track fill rates
                for k in fill_counts:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Queue post-race update
                post_updates.append((cheval, drop_pct, is_winner))

            # --- Update horse state AFTER snapshot ---
            for cheval, drop_pct, is_winner in post_updates:
                if cheval:
                    horse_state[cheval].update(drop_pct, is_winner)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Late money detection build termine: %d features en %.1fs (chevaux: %d)",
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
        description="Construction des features late money detection a partir de partants_master"
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

    logger = setup_logging("late_money_detection_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "late_money_detection.jsonl"
    build_late_money_detection_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
