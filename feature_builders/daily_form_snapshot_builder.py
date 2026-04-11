#!/usr/bin/env python3
"""
feature_builders.daily_form_snapshot_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Daily form snapshot features -- capturing the state of each horse on race day.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant daily form snapshot features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the snapshot -- no future leakage.  Snapshot is taken BEFORE
updating state with the current race result.

Produces:
  - daily_form_snapshot.jsonl   in builder_outputs/daily_form_snapshot/

Features per partant (10):
  - dfs_days_inactive           : days since last race
  - dfs_last_3_positions_avg    : average of last 3 finishing positions
  - dfs_last_5_win_count        : wins in last 5 races
  - dfs_last_5_place_count      : places in last 5 races
  - dfs_current_win_streak      : current consecutive wins (0 if last race wasn't a win)
  - dfs_current_losing_streak   : consecutive non-wins
  - dfs_days_since_last_win     : days since last win
  - dfs_races_since_last_win    : number of races since last win
  - dfs_current_form_label      : 0=hot (won last), 1=warm (placed last),
                                  2=cold (not placed last 3), 3=unknown
  - dfs_snap_quality_score      : composite = (last3_avg_norm * 0.5)
                                  + (win_streak * 0.3) + (freshness * 0.2)

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets into memory
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 streams output to disk via seek-based re-read
  - gc.collect() every 500K records

Usage:
    python feature_builders/daily_form_snapshot_builder.py
    python feature_builders/daily_form_snapshot_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/daily_form_snapshot")

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseFormState:
    """Tracks running form state for a single horse.

    Fields:
      last_date       : date of last race (datetime or None)
      positions       : deque(maxlen=5) of finishing positions (ints)
      wins            : deque(maxlen=5) of bools (True = won)
      places          : deque(maxlen=5) of bools (True = placed)
      last_win_date   : date of last win (datetime or None)
      last_win_race_idx : total_races value at last win
      current_streak  : length of current streak
      streak_type     : "win" or "lose" or None
      total_races     : total races run so far
    """

    __slots__ = (
        "last_date",
        "positions",
        "wins",
        "places",
        "last_win_date",
        "last_win_race_idx",
        "current_streak",
        "streak_type",
        "total_races",
    )

    def __init__(self) -> None:
        self.last_date: Optional[datetime] = None
        self.positions: deque = deque(maxlen=5)
        self.wins: deque = deque(maxlen=5)
        self.places: deque = deque(maxlen=5)
        self.last_win_date: Optional[datetime] = None
        self.last_win_race_idx: int = 0
        self.current_streak: int = 0
        self.streak_type: Optional[str] = None
        self.total_races: int = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_daily_form_snapshot(input_path: Path, output_path: Path, logger) -> int:
    """Build daily form snapshot features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Daily Form Snapshot Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseFormState] = defaultdict(_HorseFormState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "dfs_days_inactive": 0,
        "dfs_last_3_positions_avg": 0,
        "dfs_last_5_win_count": 0,
        "dfs_last_5_place_count": 0,
        "dfs_current_win_streak": 0,
        "dfs_current_losing_streak": 0,
        "dfs_days_since_last_win": 0,
        "dfs_races_since_last_win": 0,
        "dfs_current_form_label": 0,
        "dfs_snap_quality_score": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            pos = rec.get("place_arrivee") or rec.get("position") or rec.get("cl")
            try:
                pos = int(pos)
            except (ValueError, TypeError):
                pos = None

            is_gagnant = bool(rec.get("is_gagnant"))
            # Place = top 3 (or top 2 for small fields, but we use top 3 as default)
            is_place = False
            if pos is not None and pos >= 1:
                is_place = pos <= 3
            elif is_gagnant:
                is_place = True

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "cheval": rec.get("nom_cheval"),
                "gagnant": is_gagnant,
                "place": is_place,
                "position": pos,
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
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            race_date = _parse_date(course_date_str)

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            post_updates: list[tuple[str, Optional[datetime], Optional[int], bool, bool]] = []

            for rec in course_group:
                cheval = rec["cheval"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if cheval and cheval in horse_state:
                    st = horse_state[cheval]

                    # 1. dfs_days_inactive
                    if st.last_date is not None and race_date is not None:
                        days_inactive = (race_date - st.last_date).days
                        features["dfs_days_inactive"] = days_inactive
                        fill_counts["dfs_days_inactive"] += 1
                    else:
                        features["dfs_days_inactive"] = None

                    # 2. dfs_last_3_positions_avg
                    recent_pos = [p for p in list(st.positions)[-3:] if p is not None]
                    if recent_pos:
                        features["dfs_last_3_positions_avg"] = round(sum(recent_pos) / len(recent_pos), 4)
                        fill_counts["dfs_last_3_positions_avg"] += 1
                    else:
                        features["dfs_last_3_positions_avg"] = None

                    # 3. dfs_last_5_win_count
                    features["dfs_last_5_win_count"] = sum(st.wins)
                    fill_counts["dfs_last_5_win_count"] += 1

                    # 4. dfs_last_5_place_count
                    features["dfs_last_5_place_count"] = sum(st.places)
                    fill_counts["dfs_last_5_place_count"] += 1

                    # 5. dfs_current_win_streak
                    if st.streak_type == "win":
                        features["dfs_current_win_streak"] = st.current_streak
                    else:
                        features["dfs_current_win_streak"] = 0
                    fill_counts["dfs_current_win_streak"] += 1

                    # 6. dfs_current_losing_streak
                    if st.streak_type == "lose":
                        features["dfs_current_losing_streak"] = st.current_streak
                    else:
                        features["dfs_current_losing_streak"] = 0
                    fill_counts["dfs_current_losing_streak"] += 1

                    # 7. dfs_days_since_last_win
                    if st.last_win_date is not None and race_date is not None:
                        features["dfs_days_since_last_win"] = (race_date - st.last_win_date).days
                        fill_counts["dfs_days_since_last_win"] += 1
                    else:
                        features["dfs_days_since_last_win"] = None

                    # 8. dfs_races_since_last_win
                    if st.last_win_date is not None:
                        features["dfs_races_since_last_win"] = st.total_races - st.last_win_race_idx
                        fill_counts["dfs_races_since_last_win"] += 1
                    else:
                        features["dfs_races_since_last_win"] = None

                    # 9. dfs_current_form_label
                    # 0=hot (won last), 1=warm (placed last), 2=cold (not placed last 3), 3=unknown
                    if st.total_races > 0:
                        wins_list = list(st.wins)
                        places_list = list(st.places)
                        if wins_list and wins_list[-1]:
                            features["dfs_current_form_label"] = 0
                        elif places_list and places_list[-1]:
                            features["dfs_current_form_label"] = 1
                        elif len(places_list) >= 3 and not any(places_list[-3:]):
                            features["dfs_current_form_label"] = 2
                        else:
                            features["dfs_current_form_label"] = 3
                        fill_counts["dfs_current_form_label"] += 1
                    else:
                        features["dfs_current_form_label"] = 3
                        fill_counts["dfs_current_form_label"] += 1

                    # 10. dfs_snap_quality_score
                    # composite = (last3_avg_normalized * 0.5) + (win_streak * 0.3) + (freshness * 0.2)
                    score_parts = 0
                    score_valid = False

                    # last3_avg_normalized: lower position = better, normalize to 0..1
                    # Use 1 - (avg-1)/19 clamped to [0,1] (position 1->1.0, position 20->0.0)
                    if recent_pos:
                        avg_pos = sum(recent_pos) / len(recent_pos)
                        norm = max(0.0, min(1.0, 1.0 - (avg_pos - 1.0) / 19.0))
                        score_parts += norm * 0.5
                        score_valid = True

                    # win_streak contribution: streak / 5 clamped to [0,1]
                    if st.streak_type == "win" and st.current_streak > 0:
                        streak_norm = min(1.0, st.current_streak / 5.0)
                        score_parts += streak_norm * 0.3
                        score_valid = True

                    # freshness: 1 for very recent (0 days), 0 for 365+ days inactive
                    if st.last_date is not None and race_date is not None:
                        days_off = (race_date - st.last_date).days
                        freshness = max(0.0, min(1.0, 1.0 - days_off / 365.0))
                        score_parts += freshness * 0.2
                        score_valid = True

                    if score_valid:
                        features["dfs_snap_quality_score"] = round(score_parts, 4)
                        fill_counts["dfs_snap_quality_score"] += 1
                    else:
                        features["dfs_snap_quality_score"] = None

                else:
                    # No prior history for this horse
                    features["dfs_days_inactive"] = None
                    features["dfs_last_3_positions_avg"] = None
                    features["dfs_last_5_win_count"] = None
                    features["dfs_last_5_place_count"] = None
                    features["dfs_current_win_streak"] = None
                    features["dfs_current_losing_streak"] = None
                    features["dfs_days_since_last_win"] = None
                    features["dfs_races_since_last_win"] = None
                    features["dfs_current_form_label"] = 3
                    fill_counts["dfs_current_form_label"] += 1
                    features["dfs_snap_quality_score"] = None

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update
                post_updates.append((
                    cheval, race_date, rec["position"], rec["gagnant"], rec["place"],
                ))

            # -- Update states after race (post-race, no leakage) --
            for cheval, r_date, position, is_winner, is_place in post_updates:
                if not cheval:
                    continue

                st = horse_state[cheval]
                st.last_date = r_date
                st.positions.append(position)
                st.wins.append(is_winner)
                st.places.append(is_place)
                st.total_races += 1

                if is_winner:
                    st.last_win_date = r_date
                    st.last_win_race_idx = st.total_races

                # Update streak
                if is_winner:
                    if st.streak_type == "win":
                        st.current_streak += 1
                    else:
                        st.streak_type = "win"
                        st.current_streak = 1
                else:
                    if st.streak_type == "lose":
                        st.current_streak += 1
                    else:
                        st.streak_type = "lose"
                        st.current_streak = 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Daily form snapshot build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    if n_written:
        logger.info("=== Fill rates ===")
        for k, v in fill_counts.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written)

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
        description="Construction des features daily form snapshot a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/daily_form_snapshot/)",
    )
    args = parser.parse_args()

    logger = setup_logging("daily_form_snapshot_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "daily_form_snapshot.jsonl"
    build_daily_form_snapshot(input_path, out_path, logger)


if __name__ == "__main__":
    main()
