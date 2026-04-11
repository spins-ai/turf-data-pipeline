#!/usr/bin/env python3
"""
feature_builders.trainer_form_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep trainer form features tracking trainer performance patterns over time.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant deep trainer-form features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the trainer statistics -- no future leakage.

Produces:
  - trainer_form_deep.jsonl   in output/trainer_form_deep/

Features per partant (10):
  - tfd_trainer_win_rate_7d      : trainer win rate last 7 days
  - tfd_trainer_win_rate_30d     : trainer win rate last 30 days
  - tfd_trainer_win_rate_90d     : trainer win rate last 90 days
  - tfd_trainer_runners_today    : number of runners for this trainer today
  - tfd_trainer_hot_streak       : current consecutive wins
  - tfd_trainer_discipline_wr    : trainer win rate in this discipline
  - tfd_trainer_hippo_wr         : trainer win rate at this hippodrome
  - tfd_trainer_distance_wr      : trainer win rate at this distance bucket
  - tfd_trainer_fav_strike       : trainer's win rate when horse is favorite (cote < 5)
  - tfd_trainer_debutant_wr      : trainer's win rate with first-time runners (nb_courses < 3)

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 streams output to disk instead of accumulating in a list
  - gc.collect() called every 500K records
  - .tmp then atomic rename

Usage:
    python feature_builders/trainer_form_deep_builder.py
    python feature_builders/trainer_form_deep_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_form_deep")

_LOG_EVERY = 500_000

# Time windows
_WINDOW_7D = timedelta(days=7)
_WINDOW_30D = timedelta(days=30)
_WINDOW_90D = timedelta(days=90)

# Feature names
_FEATURE_NAMES = [
    "tfd_trainer_win_rate_7d",
    "tfd_trainer_win_rate_30d",
    "tfd_trainer_win_rate_90d",
    "tfd_trainer_runners_today",
    "tfd_trainer_hot_streak",
    "tfd_trainer_discipline_wr",
    "tfd_trainer_hippo_wr",
    "tfd_trainer_distance_wr",
    "tfd_trainer_fav_strike",
    "tfd_trainer_debutant_wr",
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _distance_bucket(distance: Optional[int]) -> str:
    """Bucket distance into categories."""
    if distance is None:
        return "unknown"
    if distance <= 1200:
        return "sprint"
    if distance <= 1600:
        return "mile"
    if distance <= 2200:
        return "inter"
    if distance <= 3000:
        return "long"
    return "marathon"


# ===========================================================================
# COUNTER HELPER
# ===========================================================================


class _WinCounter:
    """Simple wins/total counter."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def win_rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return round(self.wins / self.total, 4)

    def update(self, won: bool) -> None:
        self.total += 1
        if won:
            self.wins += 1


# ===========================================================================
# TRAINER STATE
# ===========================================================================


class _TrainerState:
    """Per-trainer accumulated state for deep form features.

    State:
      - recent_results: deque(200) of (date_dt, won) for time-windowed rates
      - discipline_stats: dict[discipline -> _WinCounter]
      - hippo_stats: dict[hippo -> _WinCounter]
      - distance_stats: dict[dist_bucket -> _WinCounter]
      - fav_stats: _WinCounter (when horse is favorite, cote < 5)
      - debutant_stats: _WinCounter (first-time runners, nb_courses < 3)
      - streak: current consecutive wins
    """

    __slots__ = (
        "recent_results",
        "discipline_stats",
        "hippo_stats",
        "distance_stats",
        "fav_stats",
        "debutant_stats",
        "streak",
    )

    def __init__(self) -> None:
        self.recent_results: deque = deque(maxlen=200)
        self.discipline_stats: dict[str, _WinCounter] = defaultdict(_WinCounter)
        self.hippo_stats: dict[str, _WinCounter] = defaultdict(_WinCounter)
        self.distance_stats: dict[str, _WinCounter] = defaultdict(_WinCounter)
        self.fav_stats = _WinCounter()
        self.debutant_stats = _WinCounter()
        self.streak: int = 0

    def snapshot(
        self,
        race_date: datetime,
        date_str: str,
        discipline: str,
        hippo: str,
        dist_bucket: str,
        is_fav: bool,
        is_debutant: bool,
        runners_today: int,
    ) -> dict[str, Any]:
        """Compute features using only data strictly before race_date."""

        cutoff_7 = race_date - _WINDOW_7D
        cutoff_30 = race_date - _WINDOW_30D
        cutoff_90 = race_date - _WINDOW_90D

        wins_7 = 0
        total_7 = 0
        wins_30 = 0
        total_30 = 0
        wins_90 = 0
        total_90 = 0

        for dt, won in self.recent_results:
            if dt >= race_date:
                continue
            if dt >= cutoff_90:
                total_90 += 1
                if won:
                    wins_90 += 1
                if dt >= cutoff_30:
                    total_30 += 1
                    if won:
                        wins_30 += 1
                    if dt >= cutoff_7:
                        total_7 += 1
                        if won:
                            wins_7 += 1

        wr_7 = round(wins_7 / total_7, 4) if total_7 > 0 else None
        wr_30 = round(wins_30 / total_30, 4) if total_30 > 0 else None
        wr_90 = round(wins_90 / total_90, 4) if total_90 > 0 else None

        # Discipline win rate
        disc_wr = None
        if discipline:
            counter = self.discipline_stats.get(discipline)
            if counter is not None:
                disc_wr = counter.win_rate()

        # Hippodrome win rate
        hippo_wr = None
        if hippo:
            counter = self.hippo_stats.get(hippo)
            if counter is not None:
                hippo_wr = counter.win_rate()

        # Distance bucket win rate
        dist_wr = None
        if dist_bucket != "unknown":
            counter = self.distance_stats.get(dist_bucket)
            if counter is not None:
                dist_wr = counter.win_rate()

        # Favourite strike rate
        fav_wr = self.fav_stats.win_rate() if is_fav else None

        # Debutant win rate
        deb_wr = self.debutant_stats.win_rate() if is_debutant else None

        return {
            "tfd_trainer_win_rate_7d": wr_7,
            "tfd_trainer_win_rate_30d": wr_30,
            "tfd_trainer_win_rate_90d": wr_90,
            "tfd_trainer_runners_today": runners_today,
            "tfd_trainer_hot_streak": self.streak,
            "tfd_trainer_discipline_wr": disc_wr,
            "tfd_trainer_hippo_wr": hippo_wr,
            "tfd_trainer_distance_wr": dist_wr,
            "tfd_trainer_fav_strike": fav_wr,
            "tfd_trainer_debutant_wr": deb_wr,
        }

    def update(
        self,
        race_date: datetime,
        won: bool,
        discipline: str,
        hippo: str,
        dist_bucket: str,
        is_fav: bool,
        is_debutant: bool,
    ) -> None:
        """Add a race result to the trainer's state (post-race)."""
        self.recent_results.append((race_date, won))

        if discipline:
            self.discipline_stats[discipline].update(won)
        if hippo:
            self.hippo_stats[hippo].update(won)
        if dist_bucket != "unknown":
            self.distance_stats[dist_bucket].update(won)
        if is_fav:
            self.fav_stats.update(won)
        if is_debutant:
            self.debutant_stats.update(won)

        # Streak
        if won:
            self.streak += 1
        else:
            self.streak = 0


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort + seek-based processing)
# ===========================================================================


def build_trainer_form_deep(input_path: Path, output_path: Path, logger) -> int:
    """Build deep trainer form features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Trainer Form Deep Builder (memory-optimised) ===")
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

    # -- Phase 2b: Pre-count runners per trainer per date --
    # We need a quick pass to know how many runners each trainer has today.
    # Build from the index by reading trainer names.
    # Instead, we do it inside the course loop (count within course group).

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    trainer_states: dict[str, _TrainerState] = defaultdict(_TrainerState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {k: 0 for k in _FEATURE_NAMES}

    # We also need to count runners per trainer per date across all courses.
    # We do a two-sub-pass per date: first collect trainer counts for the day,
    # then process courses for that day.
    # Since index is sorted by (date, course, num), we group by date first.

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(off: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(off)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            trainer = rec.get("nom_entraineur") or rec.get("entraineur")
            distance = _safe_int(rec.get("distance"))
            cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_probable"))
            nb_courses = _safe_int(rec.get("nb_courses"))
            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()
            hippo = rec.get("hippodrome_normalise", "") or ""

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "trainer": trainer,
                "gagnant": bool(rec.get("is_gagnant")),
                "discipline": discipline,
                "hippo": hippo,
                "dist_bucket": _distance_bucket(distance),
                "is_fav": cote is not None and 0 < cote < 5,
                "is_debutant": nb_courses is not None and nb_courses < 3,
            }

        i = 0
        while i < total:
            # Group by date first to count runners per trainer today
            day_str = index[i][0]
            day_start = i

            while i < total and index[i][0] == day_str:
                i += 1
            day_end = i

            # Read all records for this day and count runners per trainer
            day_slims: list[dict] = []
            trainer_count_today: dict[str, int] = defaultdict(int)

            for di in range(day_start, day_end):
                slim = _extract_slim(_read_record_at(index[di][3]))
                day_slims.append(slim)
                trainer = slim["trainer"]
                if trainer:
                    trainer_count_today[trainer] += 1

            # Now process course by course within this day
            ci = 0
            day_total = len(day_slims)

            while ci < day_total:
                course_uid = day_slims[ci]["course"]
                course_group: list[dict] = []

                while ci < day_total and day_slims[ci]["course"] == course_uid:
                    course_group.append(day_slims[ci])
                    ci += 1

                race_date = _parse_date(day_str)

                # -- Snapshot pre-race features for all partants --
                for rec in course_group:
                    trainer = rec["trainer"]

                    if trainer and race_date:
                        runners_today = trainer_count_today.get(trainer, 0)
                        features = trainer_states[trainer].snapshot(
                            race_date=race_date,
                            date_str=day_str,
                            discipline=rec["discipline"],
                            hippo=rec["hippo"],
                            dist_bucket=rec["dist_bucket"],
                            is_fav=rec["is_fav"],
                            is_debutant=rec["is_debutant"],
                            runners_today=runners_today,
                        )
                    else:
                        features = {k: None for k in _FEATURE_NAMES}

                    features["partant_uid"] = rec["uid"]

                    # Track fill rates
                    for k in _FEATURE_NAMES:
                        if features.get(k) is not None:
                            fill_counts[k] += 1

                    fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                    n_written += 1

                # -- Update trainer states after snapshotting (post-race) --
                for rec in course_group:
                    trainer = rec["trainer"]
                    if trainer and race_date:
                        trainer_states[trainer].update(
                            race_date=race_date,
                            won=rec["gagnant"],
                            discipline=rec["discipline"],
                            hippo=rec["hippo"],
                            dist_bucket=rec["dist_bucket"],
                            is_fav=rec["is_fav"],
                            is_debutant=rec["is_debutant"],
                        )

                n_processed += len(course_group)
                if n_processed % _LOG_EVERY < len(course_group):
                    logger.info("  Traite %d / %d records...", n_processed, total)
                    gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Trainer Form Deep build termine: %d features en %.1fs (entraineurs: %d)",
        n_written, elapsed, len(trainer_states),
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
        description="Construction des features trainer form deep a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/trainer_form_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_form_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "trainer_form_deep.jsonl"
    build_trainer_form_deep(input_path, out_path, logger)


if __name__ == "__main__":
    main()
