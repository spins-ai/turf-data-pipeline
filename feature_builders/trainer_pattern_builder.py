#!/usr/bin/env python3
"""
feature_builders.trainer_pattern_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer strategy pattern features -- trainers have distinctive patterns
(class movement, race spacing, hippodrome preferences, distance choices)
that are predictive of runner outcomes.

Temporal integrity: for any partant at date D, only races with date < D
contribute to trainer statistics -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - trainer_pattern_features.jsonl  in builder_outputs/trainer_pattern/

Features per partant (10):
  - trp_trainer_win_rate_30d       : trainer win rate in last 30 days
  - trp_trainer_runners_30d        : number of runners in last 30 days (activity level)
  - trp_trainer_class_preference   : avg field_strength of trainer's last 20 runners
  - trp_trainer_distance_preference: avg distance of trainer's last 20 runners
  - trp_trainer_hippo_loyalty      : proportion of last 20 runners at current hippodrome
  - trp_trainer_spacing_avg        : avg days between trainer's runners
  - trp_trainer_horse_rotation     : distinct horses in trainer's last 20 runners
  - trp_trainer_improving_horses   : proportion of last 10 horses that improved vs previous
  - trp_trainer_first_time_hippo   : 1 if trainer has never run at this hippodrome
  - trp_trainer_discipline_specialist: proportion of last 50 runners in current discipline

Usage:
    python feature_builders/trainer_pattern_builder.py
    python feature_builders/trainer_pattern_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_pattern")

_LOG_EVERY = 500_000
_WINDOW_50 = 50
_WINDOW_20 = 20
_WINDOW_10 = 10
_DAYS_30 = 30


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date_days(date_str: str) -> Optional[int]:
    """Convert YYYY-MM-DD to an integer day count for gap calculations."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


# ===========================================================================
# PER-TRAINER STATE (memory-efficient with __slots__)
# ===========================================================================


class _TrainerState:
    """Track rolling state for one trainer."""

    __slots__ = (
        "runners",          # deque(maxlen=50) of tuples: (date_days, hippodrome, discipline,
                            #   horse_id, field_strength, distance, position, prev_position)
        "hippodromes_set",  # set of all hippodromes ever used by this trainer
        "wins_30d",         # list of date_days for wins in rolling window
        "runs_30d",         # list of date_days for all runs in rolling window
        "last_date",        # int or None: date_days of last run
    )

    def __init__(self) -> None:
        self.runners: deque = deque(maxlen=_WINDOW_50)
        self.hippodromes_set: set = set()
        self.wins_30d: list = []
        self.runs_30d: list = []
        self.last_date: Optional[int] = None


# ===========================================================================
# FEATURE COMPUTATION (from snapshot BEFORE update)
# ===========================================================================


def _compute_features(
    ts: _TrainerState,
    current_hippodrome: Optional[str],
    current_discipline: Optional[str],
    current_date_days: Optional[int],
) -> dict[str, Any]:
    """Compute all 10 trainer-pattern features from pre-race state."""
    feats: dict[str, Any] = {}

    # Prune 30d lists to current window
    if current_date_days is not None:
        cutoff_30d = current_date_days - _DAYS_30
        ts.wins_30d = [d for d in ts.wins_30d if d >= cutoff_30d]
        ts.runs_30d = [d for d in ts.runs_30d if d >= cutoff_30d]

    runners_list = list(ts.runners)

    # 1. trp_trainer_win_rate_30d
    runs_30 = len(ts.runs_30d)
    wins_30 = len(ts.wins_30d)
    if runs_30 >= 1:
        feats["trp_trainer_win_rate_30d"] = round(wins_30 / runs_30, 4)
    else:
        feats["trp_trainer_win_rate_30d"] = None

    # 2. trp_trainer_runners_30d
    feats["trp_trainer_runners_30d"] = runs_30 if runs_30 > 0 else None

    # -- Last 20 runners for features 3-7 --
    last_20 = runners_list[-_WINDOW_20:] if len(runners_list) >= 1 else []

    # 3. trp_trainer_class_preference: avg field_strength of last 20
    fs_vals = [r[4] for r in last_20 if r[4] is not None]
    if fs_vals:
        feats["trp_trainer_class_preference"] = round(sum(fs_vals) / len(fs_vals), 4)
    else:
        feats["trp_trainer_class_preference"] = None

    # 4. trp_trainer_distance_preference: avg distance of last 20
    dist_vals = [r[5] for r in last_20 if r[5] is not None]
    if dist_vals:
        feats["trp_trainer_distance_preference"] = round(sum(dist_vals) / len(dist_vals), 1)
    else:
        feats["trp_trainer_distance_preference"] = None

    # 5. trp_trainer_hippo_loyalty: proportion of last 20 at current hippodrome
    if current_hippodrome and last_20:
        hippo_count = sum(1 for r in last_20 if r[1] == current_hippodrome)
        feats["trp_trainer_hippo_loyalty"] = round(hippo_count / len(last_20), 4)
    else:
        feats["trp_trainer_hippo_loyalty"] = None

    # 6. trp_trainer_spacing_avg: avg days between consecutive runners
    dates_sorted = [r[0] for r in runners_list if r[0] is not None]
    if len(dates_sorted) >= 2:
        gaps = [dates_sorted[j] - dates_sorted[j - 1]
                for j in range(1, len(dates_sorted)) if dates_sorted[j] > dates_sorted[j - 1]]
        if gaps:
            feats["trp_trainer_spacing_avg"] = round(sum(gaps) / len(gaps), 2)
        else:
            feats["trp_trainer_spacing_avg"] = None
    else:
        feats["trp_trainer_spacing_avg"] = None

    # 7. trp_trainer_horse_rotation: distinct horses in last 20
    if last_20:
        distinct_horses = set(r[3] for r in last_20 if r[3] is not None)
        feats["trp_trainer_horse_rotation"] = len(distinct_horses) if distinct_horses else None
    else:
        feats["trp_trainer_horse_rotation"] = None

    # 8. trp_trainer_improving_horses: proportion of last 10 that improved
    last_10 = runners_list[-_WINDOW_10:] if len(runners_list) >= 1 else []
    if last_10:
        improved = 0
        with_comparison = 0
        for r in last_10:
            pos = r[6]       # position
            prev_pos = r[7]  # prev_position
            if pos is not None and prev_pos is not None and pos > 0 and prev_pos > 0:
                with_comparison += 1
                if pos < prev_pos:
                    improved += 1
        if with_comparison >= 1:
            feats["trp_trainer_improving_horses"] = round(improved / with_comparison, 4)
        else:
            feats["trp_trainer_improving_horses"] = None
    else:
        feats["trp_trainer_improving_horses"] = None

    # 9. trp_trainer_first_time_hippo: 1 if trainer has never run here
    if current_hippodrome:
        feats["trp_trainer_first_time_hippo"] = 1 if current_hippodrome not in ts.hippodromes_set else 0
    else:
        feats["trp_trainer_first_time_hippo"] = None

    # 10. trp_trainer_discipline_specialist: proportion of last 50 in current discipline
    if current_discipline and runners_list:
        disc_count = sum(1 for r in runners_list if r[2] == current_discipline)
        feats["trp_trainer_discipline_specialist"] = round(disc_count / len(runners_list), 4)
    else:
        feats["trp_trainer_discipline_specialist"] = None

    return feats


# ===========================================================================
# UPDATE TRAINER STATE (post-race)
# ===========================================================================


def _update_state(
    ts: _TrainerState,
    date_days: Optional[int],
    hippodrome: Optional[str],
    discipline: Optional[str],
    horse_id: Optional[str],
    field_strength: Optional[float],
    distance: Optional[float],
    position: Optional[int],
    prev_position: Optional[int],
    is_winner: bool,
) -> None:
    """Update the trainer's rolling state after a race."""
    ts.runners.append((
        date_days, hippodrome, discipline, horse_id,
        field_strength, distance, position, prev_position,
    ))

    if hippodrome:
        ts.hippodromes_set.add(hippodrome)

    if date_days is not None:
        ts.runs_30d.append(date_days)
        if is_winner:
            ts.wins_30d.append(date_days)
        ts.last_date = date_days


# ===========================================================================
# HORSE PREVIOUS POSITION TRACKER
# ===========================================================================


class _HorsePrevPosition:
    """Track each horse's last finishing position for improvement detection."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        self.data: dict[str, Optional[int]] = {}

    def get(self, horse_id: str) -> Optional[int]:
        return self.data.get(horse_id)

    def update(self, horse_id: str, position: Optional[int]) -> None:
        if horse_id and position is not None and position > 0:
            self.data[horse_id] = position


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_trainer_pattern_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build trainer pattern features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Trainer Pattern Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
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
    trainer_state: dict[str, _TrainerState] = defaultdict(_TrainerState)
    horse_prev_pos = _HorsePrevPosition()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "trp_trainer_win_rate_30d",
        "trp_trainer_runners_30d",
        "trp_trainer_class_preference",
        "trp_trainer_distance_preference",
        "trp_trainer_hippo_loyalty",
        "trp_trainer_spacing_avg",
        "trp_trainer_horse_rotation",
        "trp_trainer_improving_horses",
        "trp_trainer_first_time_hippo",
        "trp_trainer_discipline_specialist",
    ]
    fill_counts = {k: 0 for k in feature_keys}

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

            current_date_days = _parse_date_days(course_date_str)

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                trainer = rec.get("entraineur_normalise", "") or rec.get("entraineur", "") or ""
                partant_uid = rec.get("partant_uid")
                course_uid_rec = rec.get("course_uid")
                date_iso = rec.get("date_reunion_iso")

                hippodrome = rec.get("hippodrome_normalise", "") or rec.get("hippodrome", "") or ""
                discipline = rec.get("discipline", "") or ""
                horse_id = rec.get("nom_cheval", "") or ""

                if not trainer:
                    # Emit record with Nones
                    out_rec = {
                        "partant_uid": partant_uid,
                        "course_uid": course_uid_rec,
                        "date_reunion_iso": date_iso,
                    }
                    for k in feature_keys:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                ts = trainer_state[trainer]

                # Compute features from pre-race snapshot
                feats = _compute_features(ts, hippodrome, discipline, current_date_days)

                out_rec = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_rec,
                    "date_reunion_iso": date_iso,
                }
                for k in feature_keys:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # Extract fields for deferred state update
                field_strength = _safe_float(
                    rec.get("field_strength") or rec.get("force_champ")
                )
                distance = _safe_float(
                    rec.get("distance") or rec.get("distance_metres")
                )
                position = _safe_int(
                    rec.get("place_arrivee") or rec.get("position_arrivee")
                )
                is_winner = bool(rec.get("is_gagnant"))

                prev_position = horse_prev_pos.get(horse_id) if horse_id else None

                post_updates.append((
                    trainer, current_date_days, hippodrome, discipline,
                    horse_id, field_strength, distance, position,
                    prev_position, is_winner,
                ))

            # -- Update trainer states after race (no leakage) --
            for (
                trainer, date_days, hippo, disc,
                h_id, fs, dist, pos,
                prev_pos, is_win,
            ) in post_updates:
                _update_state(
                    trainer_state[trainer],
                    date_days, hippo, disc, h_id,
                    fs, dist, pos, prev_pos, is_win,
                )
                # Update horse previous position tracker
                if h_id and pos is not None and pos > 0:
                    horse_prev_pos.update(h_id, pos)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Trainer pattern build termine: %d features en %.1fs (entraineurs suivis: %d)",
        n_written, elapsed, len(trainer_state),
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


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features pattern entraineur a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/trainer_pattern/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_pattern_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "trainer_pattern_features.jsonl"
    build_trainer_pattern_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
