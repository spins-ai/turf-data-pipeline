#!/usr/bin/env python3
"""
feature_builders.progressive_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Progressive / cumulative features that grow with the horse's career.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant progressive features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the state -- no future leakage. Snapshot BEFORE update.

Produces:
  - progressive_features.jsonl   in builder_outputs/progressive_features/

Features per partant (8):
  - prg_career_race_number      : how many races this horse has run so far (running count)
  - prg_days_since_debut        : days since horse's first race
  - prg_races_per_year          : career_race_number / years_active (race frequency)
  - prg_win_interval_avg        : average number of races between wins
  - prg_last_win_races_ago      : how many races since last win
  - prg_wins_this_year          : count of wins in the current calendar year so far
  - prg_is_improving_career     : 1 if win rate in last 10 > win rate in first 10 races
  - prg_experience_vs_field     : horse's career_race_number / avg career_races of field

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records
  - Write to .tmp then atomic rename

Usage:
    python feature_builders/progressive_features_builder.py
    python feature_builders/progressive_features_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/progressive_features")

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseState:
    """Tracks progressive career state for a single horse.

    State fields:
      - first_date       : date of first ever race
      - career_count     : total number of races run
      - wins             : total wins
      - win_indices      : list of career_count values at which wins occurred
      - last_win_idx     : career_count at last win (None if no win)
      - yearly_wins      : dict {year: win_count}
      - early_wins       : wins in first 10 races
      - early_total      : min(career_count, 10) -- races counted toward early stats
      - recent_results   : deque(maxlen=10) of 1=win / 0=loss (most recent 10)
    """

    __slots__ = (
        "first_date", "career_count", "wins", "win_indices",
        "last_win_idx", "yearly_wins", "early_wins", "early_total",
        "recent_results",
    )

    def __init__(self) -> None:
        self.first_date: Optional[datetime] = None
        self.career_count: int = 0
        self.wins: int = 0
        self.win_indices: list[int] = []
        self.last_win_idx: Optional[int] = None
        self.yearly_wins: dict[int, int] = {}
        self.early_wins: int = 0
        self.early_total: int = 0
        self.recent_results: deque = deque(maxlen=10)


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_progressive_features(input_path: Path, output_path: Path, logger) -> int:
    """Build progressive career features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Progressive Features Builder ===")
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
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "prg_career_race_number",
        "prg_days_since_debut",
        "prg_races_per_year",
        "prg_win_interval_avg",
        "prg_last_win_races_ago",
        "prg_wins_this_year",
        "prg_is_improving_career",
        "prg_experience_vs_field",
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

            # Read only this course's records from disk
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                course_records.append(rec)

            race_date = _parse_date(course_date_str)
            race_year = race_date.year if race_date else None

            # -- Collect career_count for all horses in field (for experience_vs_field) --
            field_career_counts: list[int] = []
            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                if horse_id:
                    field_career_counts.append(horse_state[horse_id].career_count)

            avg_field_career = (
                sum(field_career_counts) / len(field_career_counts)
                if field_career_counts
                else 0.0
            )

            # -- Snapshot pre-race stats & emit features (temporal integrity) --
            post_updates: list[tuple[str, bool, Optional[datetime], Optional[int]]] = []

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                partant_uid = rec.get("partant_uid")
                is_winner = bool(rec.get("is_gagnant"))

                features: dict[str, Any] = {"partant_uid": partant_uid}

                if horse_id:
                    st = horse_state[horse_id]

                    # --- Feature 1: prg_career_race_number ---
                    features["prg_career_race_number"] = st.career_count
                    if st.career_count > 0:
                        fill_counts["prg_career_race_number"] += 1

                    # --- Feature 2: prg_days_since_debut ---
                    if st.first_date is not None and race_date is not None:
                        delta = (race_date - st.first_date).days
                        features["prg_days_since_debut"] = delta
                        fill_counts["prg_days_since_debut"] += 1
                    else:
                        features["prg_days_since_debut"] = None

                    # --- Feature 3: prg_races_per_year ---
                    if (
                        st.career_count > 0
                        and st.first_date is not None
                        and race_date is not None
                    ):
                        days_active = (race_date - st.first_date).days
                        years_active = days_active / 365.25 if days_active > 0 else 0.0
                        if years_active > 0:
                            features["prg_races_per_year"] = round(
                                st.career_count / years_active, 2
                            )
                            fill_counts["prg_races_per_year"] += 1
                        else:
                            features["prg_races_per_year"] = None
                    else:
                        features["prg_races_per_year"] = None

                    # --- Feature 4: prg_win_interval_avg ---
                    if len(st.win_indices) >= 2:
                        intervals = [
                            st.win_indices[j] - st.win_indices[j - 1]
                            for j in range(1, len(st.win_indices))
                        ]
                        features["prg_win_interval_avg"] = round(
                            sum(intervals) / len(intervals), 2
                        )
                        fill_counts["prg_win_interval_avg"] += 1
                    else:
                        features["prg_win_interval_avg"] = None

                    # --- Feature 5: prg_last_win_races_ago ---
                    if st.last_win_idx is not None:
                        features["prg_last_win_races_ago"] = st.career_count - st.last_win_idx
                        fill_counts["prg_last_win_races_ago"] += 1
                    else:
                        features["prg_last_win_races_ago"] = None

                    # --- Feature 6: prg_wins_this_year ---
                    if race_year is not None:
                        features["prg_wins_this_year"] = st.yearly_wins.get(race_year, 0)
                        fill_counts["prg_wins_this_year"] += 1
                    else:
                        features["prg_wins_this_year"] = None

                    # --- Feature 7: prg_is_improving_career ---
                    # Compare win rate in last 10 vs first 10 races
                    if st.early_total >= 10 and len(st.recent_results) >= 10:
                        early_wr = st.early_wins / st.early_total if st.early_total > 0 else 0.0
                        recent_wr = sum(st.recent_results) / len(st.recent_results)
                        features["prg_is_improving_career"] = 1 if recent_wr > early_wr else 0
                        fill_counts["prg_is_improving_career"] += 1
                    else:
                        features["prg_is_improving_career"] = None

                    # --- Feature 8: prg_experience_vs_field ---
                    if st.career_count > 0 and avg_field_career > 0:
                        features["prg_experience_vs_field"] = round(
                            st.career_count / avg_field_career, 4
                        )
                        fill_counts["prg_experience_vs_field"] += 1
                    else:
                        features["prg_experience_vs_field"] = None

                else:
                    # No horse_id: all features null
                    for fname in feature_names:
                        features[fname] = None

                # Write to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Queue post-race update
                post_updates.append((horse_id, is_winner, race_date, race_year))

            # -- Update states AFTER race (no leakage) --
            for horse_id, is_winner, r_date, r_year in post_updates:
                if not horse_id:
                    continue
                st = horse_state[horse_id]

                # Set first_date on debut
                if st.first_date is None and r_date is not None:
                    st.first_date = r_date

                # Increment career count
                st.career_count += 1

                # Track early results (first 10)
                if st.early_total < 10:
                    st.early_total += 1
                    if is_winner:
                        st.early_wins += 1

                # Recent results deque
                st.recent_results.append(1 if is_winner else 0)

                # Win tracking
                if is_winner:
                    st.wins += 1
                    st.win_indices.append(st.career_count)
                    st.last_win_idx = st.career_count

                    # Yearly wins
                    if r_year is not None:
                        st.yearly_wins[r_year] = st.yearly_wins.get(r_year, 0) + 1

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Progressive features build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
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
        description="Construction des features progressives a partir de partants_master"
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

    logger = setup_logging("progressive_features_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "progressive_features.jsonl"
    build_progressive_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
