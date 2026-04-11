#!/usr/bin/env python3
"""
feature_builders.trainer_horse_history_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer-horse history features tracking how long and how well a trainer
has managed each horse.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant trainer-horse history features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - trainer_horse_history.jsonl   in output/trainer_horse_history/

Features per partant (8):
  - thh_trainer_horse_runs        : how many times this trainer has run this horse
  - thh_trainer_horse_wr          : win rate of this trainer with this horse
  - thh_trainer_horse_pr          : place rate (top 3) of this trainer with this horse
  - thh_trainer_horse_days_together : days since first race together
  - thh_trainer_first_run_with_horse : 1 if this is the trainer's first time with this horse
  - thh_trainer_horse_improving   : 1 if last 2 positions better than earlier 2 positions
  - thh_trainer_stable_size       : number of distinct horses trainer runs (proxy for stable size)
  - thh_trainer_horse_best_pos    : best position achieved by this horse under this trainer

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 streams output to disk instead of accumulating in a list
  - gc.collect() called every 500K records
  - .tmp then atomic rename

Usage:
    python feature_builders/trainer_horse_history_builder.py
    python feature_builders/trainer_horse_history_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_horse_history")

_LOG_EVERY = 500_000

# Feature names
_FEATURE_NAMES = [
    "thh_trainer_horse_runs",
    "thh_trainer_horse_wr",
    "thh_trainer_horse_pr",
    "thh_trainer_horse_days_together",
    "thh_trainer_first_run_with_horse",
    "thh_trainer_horse_improving",
    "thh_trainer_stable_size",
    "thh_trainer_horse_best_pos",
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class _TrainerHorseState:
    """Per (trainer, horse) pair state.

    Tracks:
      - runs: number of races together
      - wins: number of wins
      - places: number of top-3 finishes
      - first_date: date of first race together
      - positions: deque(4) of last 4 positions (for improving signal)
      - best_pos: best position achieved
    """

    __slots__ = ("runs", "wins", "places", "first_date", "positions", "best_pos")

    def __init__(self) -> None:
        self.runs: int = 0
        self.wins: int = 0
        self.places: int = 0
        self.first_date: Optional[datetime] = None
        self.positions: deque = deque(maxlen=4)
        self.best_pos: Optional[int] = None

    def snapshot(self, race_date: Optional[datetime]) -> dict[str, Any]:
        """Compute features BEFORE updating with the current race."""
        feats: dict[str, Any] = {k: None for k in _FEATURE_NAMES}

        if self.runs == 0:
            # First time together
            feats["thh_trainer_horse_runs"] = 0
            feats["thh_trainer_first_run_with_horse"] = 1
            return feats

        feats["thh_trainer_horse_runs"] = self.runs
        feats["thh_trainer_first_run_with_horse"] = 0
        feats["thh_trainer_horse_wr"] = round(self.wins / self.runs, 4)
        feats["thh_trainer_horse_pr"] = round(self.places / self.runs, 4)
        feats["thh_trainer_horse_best_pos"] = self.best_pos

        # Days together
        if race_date is not None and self.first_date is not None:
            delta = (race_date - self.first_date).days
            feats["thh_trainer_horse_days_together"] = delta

        # Improving: compare last 2 vs earlier 2
        pos_list = list(self.positions)
        if len(pos_list) >= 4:
            earlier_avg = (pos_list[0] + pos_list[1]) / 2.0
            recent_avg = (pos_list[2] + pos_list[3]) / 2.0
            feats["thh_trainer_horse_improving"] = int(recent_avg < earlier_avg)
        elif len(pos_list) >= 3:
            # Use 1 earlier vs 2 recent
            earlier_avg = pos_list[0]
            recent_avg = (pos_list[1] + pos_list[2]) / 2.0
            feats["thh_trainer_horse_improving"] = int(recent_avg < earlier_avg)

        return feats

    def update(self, race_date: Optional[datetime], position: Optional[int], is_winner: bool) -> None:
        """Update state AFTER snapshotting."""
        self.runs += 1
        if is_winner:
            self.wins += 1
        if position is not None and 1 <= position <= 3:
            self.places += 1
        if race_date is not None and self.first_date is None:
            self.first_date = race_date
        if position is not None and position > 0:
            self.positions.append(position)
            if self.best_pos is None or position < self.best_pos:
                self.best_pos = position


class _TrainerState:
    """Per-trainer state tracking distinct horses (stable size proxy)."""

    __slots__ = ("horses",)

    def __init__(self) -> None:
        self.horses: set[str] = set()

    def stable_size(self) -> int:
        return len(self.horses)

    def update(self, horse_id: str) -> None:
        self.horses.add(horse_id)


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort + seek-based processing)
# ===========================================================================


def build_trainer_horse_history(input_path: Path, output_path: Path, logger) -> int:
    """Build trainer-horse history features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Trainer Horse History Builder (memory-optimised) ===")
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
    pair_states: dict[tuple[str, str], _TrainerHorseState] = defaultdict(_TrainerHorseState)
    trainer_states: dict[str, _TrainerState] = defaultdict(_TrainerState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {k: 0 for k in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(off: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(off)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            trainer = rec.get("nom_entraineur") or rec.get("entraineur")
            horse_id = rec.get("horse_id") or rec.get("nom_cheval")
            position = _safe_int(rec.get("position_arrivee"))

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "trainer": trainer,
                "horse_id": horse_id,
                "position": position,
                "gagnant": bool(rec.get("is_gagnant")),
            }

        i = 0
        while i < total:
            # Group by date
            day_str = index[i][0]
            day_start = i

            while i < total and index[i][0] == day_str:
                i += 1
            day_end = i

            # Read all records for this day
            day_slims: list[dict] = []
            for di in range(day_start, day_end):
                slim = _extract_slim(_read_record_at(index[di][3]))
                day_slims.append(slim)

            race_date = _parse_date(day_str)

            # Process course by course within this day
            ci = 0
            day_total = len(day_slims)

            while ci < day_total:
                course_uid = day_slims[ci]["course"]
                course_group: list[dict] = []

                while ci < day_total and day_slims[ci]["course"] == course_uid:
                    course_group.append(day_slims[ci])
                    ci += 1

                # -- Snapshot pre-race features for all partants --
                for rec in course_group:
                    trainer = rec["trainer"]
                    horse_id = rec["horse_id"]

                    if trainer and horse_id:
                        pair_key = (trainer, horse_id)
                        features = pair_states[pair_key].snapshot(race_date)
                        features["thh_trainer_stable_size"] = trainer_states[trainer].stable_size()
                    else:
                        features = {k: None for k in _FEATURE_NAMES}

                    features["partant_uid"] = rec["uid"]

                    # Track fill rates
                    for k in _FEATURE_NAMES:
                        if features.get(k) is not None:
                            fill_counts[k] += 1

                    fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                    n_written += 1

                # -- Update states after snapshotting (post-race) --
                for rec in course_group:
                    trainer = rec["trainer"]
                    horse_id = rec["horse_id"]
                    if trainer and horse_id:
                        pair_key = (trainer, horse_id)
                        pair_states[pair_key].update(
                            race_date=race_date,
                            position=rec["position"],
                            is_winner=rec["gagnant"],
                        )
                        trainer_states[trainer].update(horse_id)

                n_processed += len(course_group)
                if n_processed % _LOG_EVERY < len(course_group):
                    logger.info("  Traite %d / %d records...", n_processed, total)
                    gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Trainer Horse History build termine: %d features en %.1fs "
        "(paires trainer-horse: %d, entraineurs: %d)",
        n_written, elapsed, len(pair_states), len(trainer_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written if n_written else 0)

    return n_written


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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features trainer-horse history a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/trainer_horse_history/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_horse_history_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "trainer_horse_history.jsonl"
    build_trainer_horse_history(input_path, out_path, logger)


if __name__ == "__main__":
    main()
