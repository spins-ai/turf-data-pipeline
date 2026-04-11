#!/usr/bin/env python3
"""
feature_builders.trainer_jockey_switch_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer and jockey change features: detect switches and evaluate their impact
on horse performance.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - trainer_jockey_switch.jsonl   in builder_outputs/trainer_jockey_switch/

Features per partant (10):
  - tjs_jockey_changed             : 1 if jockey is different from last race for this horse
  - tjs_jockey_change_win_boost    : win rate after jockey change vs without change
  - tjs_new_jockey_with_horse_runs : how many times this jockey has ridden this horse before
  - tjs_new_jockey_with_horse_wr   : win rate of this jockey-horse pair from past
  - tjs_trainer_changed            : 1 if trainer changed from last race
  - tjs_trainer_change_win_boost   : win rate after trainer change vs stable trainer
  - tjs_jockey_switch_frequency    : switches / total_races ratio for this horse
  - tjs_jockey_upgrade_signal      : 1 if new jockey has higher overall win rate than previous
  - tjs_stable_partnership_bonus   : 1 if same jockey-horse pair for 3+ consecutive races
  - tjs_days_since_jockey_change   : days since last jockey change for this horse

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full records)
  - Phase 2 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records

Usage:
    python feature_builders/trainer_jockey_switch_builder.py
    python feature_builders/trainer_jockey_switch_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_jockey_switch")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _norm(name: Optional[str]) -> Optional[str]:
    """Normalise a jockey / trainer name for comparison."""
    if not name or not isinstance(name, str):
        return None
    v = name.strip().upper()
    return v if v else None


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _days_between(d1: Optional[datetime], d2: Optional[datetime]) -> Optional[int]:
    """Return number of days between two dates, or None."""
    if d1 is None or d2 is None:
        return None
    return abs((d2 - d1).days)


# ===========================================================================
# GLOBAL JOCKEY STATE
# ===========================================================================


class _GlobalJockeyStats:
    """Global jockey win rate tracker: jockey -> [wins, total]."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        self.data: dict[str, list[int]] = defaultdict(lambda: [0, 0])

    def win_rate(self, jockey: str) -> Optional[float]:
        rec = self.data.get(jockey)
        if rec is None or rec[1] == 0:
            return None
        return rec[0] / rec[1]

    def update(self, jockey: str, is_winner: bool) -> None:
        rec = self.data[jockey]
        rec[1] += 1
        if is_winner:
            rec[0] += 1


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Per-horse state tracking jockey/trainer switches.

    State:
      - last_jockey: normalised name of last jockey
      - last_trainer: normalised name of last trainer
      - last_date: datetime of last race
      - jockey_pair_stats: {jockey -> [wins, total]} for this horse-jockey pair
      - after_jchange_wins / after_jchange_total: wins/total for races after a jockey change
      - stable_wins / stable_total: wins/total for races without a jockey change
      - consecutive_same_jockey: consecutive races with same jockey (current streak)
      - last_jockey_change_date: datetime of last jockey change
      - total_races: total races seen for this horse
      - total_jockey_switches: number of jockey switches
    """

    __slots__ = (
        "last_jockey", "last_trainer", "last_date",
        "jockey_pair_stats",
        "after_jchange_wins", "after_jchange_total",
        "stable_wins", "stable_total",
        "consecutive_same_jockey", "last_jockey_change_date",
        "total_races", "total_jockey_switches",
        # trainer change stats
        "after_tchange_wins", "after_tchange_total",
        "stable_trainer_wins", "stable_trainer_total",
    )

    def __init__(self) -> None:
        self.last_jockey: Optional[str] = None
        self.last_trainer: Optional[str] = None
        self.last_date: Optional[datetime] = None
        self.jockey_pair_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.after_jchange_wins: int = 0
        self.after_jchange_total: int = 0
        self.stable_wins: int = 0
        self.stable_total: int = 0
        self.consecutive_same_jockey: int = 0
        self.last_jockey_change_date: Optional[datetime] = None
        self.total_races: int = 0
        self.total_jockey_switches: int = 0
        self.after_tchange_wins: int = 0
        self.after_tchange_total: int = 0
        self.stable_trainer_wins: int = 0
        self.stable_trainer_total: int = 0

    def snapshot(
        self,
        current_jockey: Optional[str],
        current_trainer: Optional[str],
        race_date: Optional[datetime],
        global_jockey: _GlobalJockeyStats,
    ) -> dict[str, Any]:
        """Compute all 10 features from PAST data only, BEFORE update."""

        feats: dict[str, Any] = {
            "tjs_jockey_changed": None,
            "tjs_jockey_change_win_boost": None,
            "tjs_new_jockey_with_horse_runs": None,
            "tjs_new_jockey_with_horse_wr": None,
            "tjs_trainer_changed": None,
            "tjs_trainer_change_win_boost": None,
            "tjs_jockey_switch_frequency": None,
            "tjs_jockey_upgrade_signal": None,
            "tjs_stable_partnership_bonus": None,
            "tjs_days_since_jockey_change": None,
        }

        # Need at least one prior race to detect changes
        if self.last_jockey is None and self.last_trainer is None:
            return feats

        # --- Jockey changed ---
        if current_jockey is not None and self.last_jockey is not None:
            jockey_changed = int(current_jockey != self.last_jockey)
            feats["tjs_jockey_changed"] = jockey_changed

            # Jockey change win boost: wr after change vs wr without change
            if self.after_jchange_total > 0 and self.stable_total > 0:
                wr_after = self.after_jchange_wins / self.after_jchange_total
                wr_stable = self.stable_wins / self.stable_total
                feats["tjs_jockey_change_win_boost"] = round(wr_after - wr_stable, 4)

            # Jockey upgrade signal: new jockey wr > old jockey wr
            if jockey_changed:
                new_wr = global_jockey.win_rate(current_jockey)
                old_wr = global_jockey.win_rate(self.last_jockey)
                if new_wr is not None and old_wr is not None:
                    feats["tjs_jockey_upgrade_signal"] = int(new_wr > old_wr)

        # --- Jockey-horse pair stats ---
        if current_jockey is not None:
            pair = self.jockey_pair_stats.get(current_jockey)
            if pair is not None:
                feats["tjs_new_jockey_with_horse_runs"] = pair[1]
                if pair[1] > 0:
                    feats["tjs_new_jockey_with_horse_wr"] = round(pair[0] / pair[1], 4)

        # --- Trainer changed ---
        if current_trainer is not None and self.last_trainer is not None:
            trainer_changed = int(current_trainer != self.last_trainer)
            feats["tjs_trainer_changed"] = trainer_changed

            # Trainer change win boost
            if self.after_tchange_total > 0 and self.stable_trainer_total > 0:
                wr_after_t = self.after_tchange_wins / self.after_tchange_total
                wr_stable_t = self.stable_trainer_wins / self.stable_trainer_total
                feats["tjs_trainer_change_win_boost"] = round(wr_after_t - wr_stable_t, 4)

        # --- Jockey switch frequency ---
        if self.total_races > 0:
            feats["tjs_jockey_switch_frequency"] = round(
                self.total_jockey_switches / self.total_races, 4
            )

        # --- Stable partnership bonus ---
        # 1 if same jockey-horse pair for 3+ consecutive races (current streak)
        if current_jockey is not None and self.last_jockey is not None:
            if current_jockey == self.last_jockey:
                # streak will be consecutive_same_jockey (already includes this streak)
                feats["tjs_stable_partnership_bonus"] = int(self.consecutive_same_jockey >= 3)
            else:
                feats["tjs_stable_partnership_bonus"] = 0

        # --- Days since jockey change ---
        if self.last_jockey_change_date is not None and race_date is not None:
            feats["tjs_days_since_jockey_change"] = _days_between(
                self.last_jockey_change_date, race_date
            )

        return feats

    def update(
        self,
        jockey: Optional[str],
        trainer: Optional[str],
        is_winner: bool,
        race_date: Optional[datetime],
    ) -> None:
        """Update state AFTER race snapshot."""

        # Jockey change tracking
        if jockey is not None:
            if self.last_jockey is not None:
                if jockey != self.last_jockey:
                    # Jockey changed
                    self.total_jockey_switches += 1
                    self.after_jchange_total += 1
                    if is_winner:
                        self.after_jchange_wins += 1
                    self.consecutive_same_jockey = 1
                    self.last_jockey_change_date = race_date
                else:
                    # Same jockey
                    self.stable_total += 1
                    if is_winner:
                        self.stable_wins += 1
                    self.consecutive_same_jockey += 1
            else:
                # First known jockey
                self.consecutive_same_jockey = 1

            # Update jockey-horse pair stats
            pair = self.jockey_pair_stats[jockey]
            pair[1] += 1
            if is_winner:
                pair[0] += 1

            self.last_jockey = jockey

        # Trainer change tracking
        if trainer is not None:
            if self.last_trainer is not None:
                if trainer != self.last_trainer:
                    self.after_tchange_total += 1
                    if is_winner:
                        self.after_tchange_wins += 1
                else:
                    self.stable_trainer_total += 1
                    if is_winner:
                        self.stable_trainer_wins += 1

            self.last_trainer = trainer

        self.last_date = race_date
        self.total_races += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_trainer_jockey_switch_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build trainer/jockey switch features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Trainer Jockey Switch Builder (memory-optimised) ===")
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
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)
    global_jockey = _GlobalJockeyStats()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    _FEATURE_NAMES = [
        "tjs_jockey_changed",
        "tjs_jockey_change_win_boost",
        "tjs_new_jockey_with_horse_runs",
        "tjs_new_jockey_with_horse_wr",
        "tjs_trainer_changed",
        "tjs_trainer_change_win_boost",
        "tjs_jockey_switch_frequency",
        "tjs_jockey_upgrade_signal",
        "tjs_stable_partnership_bonus",
        "tjs_days_since_jockey_change",
    ]
    fill_counts = {k: 0 for k in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", "") or "",
                "course": rec.get("course_uid", "") or "",
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "jockey": _norm(rec.get("jockey_driver")),
                "trainer": _norm(rec.get("entraineur")),
                "is_gagnant": bool(rec.get("is_gagnant")),
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

            # Read only this course's records from disk
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            race_date = _parse_date(course_date_str)

            # -- Snapshot pre-race features (temporal integrity) --
            for rec in course_group:
                hid = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if hid:
                    state = horse_states[hid]
                    snap = state.snapshot(
                        rec["jockey"], rec["trainer"], race_date, global_jockey
                    )
                    features.update(snap)
                else:
                    for k in _FEATURE_NAMES:
                        features[k] = None

                # Track fill counts
                for k in _FEATURE_NAMES:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after snapshotting (post-race) --
            for rec in course_group:
                hid = rec["horse_id"]
                jockey = rec["jockey"]
                trainer = rec["trainer"]
                is_win = rec["is_gagnant"]

                if hid:
                    horse_states[hid].update(jockey, trainer, is_win, race_date)

                # Update global jockey stats
                if jockey:
                    global_jockey.update(jockey, is_win)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Trainer jockey switch build termine: %d features en %.1fs "
        "(chevaux: %d, jockeys: %d)",
        n_written, elapsed, len(horse_states), len(global_jockey.data),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)", k, v, n_written,
            100 * v / n_written if n_written else 0,
        )

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features trainer/jockey switch a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: D:/turf-data-pipeline/03_DONNEES_MASTER/)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/trainer_jockey_switch/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_jockey_switch_builder")

    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Fichier introuvable: {input_path}")
    else:
        input_path = INPUT_PARTANTS
        if not input_path.exists():
            raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "trainer_jockey_switch.jsonl"
    build_trainer_jockey_switch_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
