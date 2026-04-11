#!/usr/bin/env python3
"""
feature_builders.race_replay_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features derived from race replay data (previous race outcomes and positions
of competitors).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant race-replay features.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.  State is snapshotted BEFORE update.

Produces:
  - race_replay_features.jsonl   in builder_outputs/race_replay_features/

Features per partant (10):
  - rpl_last_race_beaten_count     : horses beaten in last race (nombre_partants - position)
  - rpl_last_race_beaten_pct       : beaten_count / (nombre_partants - 1) in last race
  - rpl_avg_beaten_pct_3           : average beaten_pct over last 3 races
  - rpl_improvement_vs_last        : change in beaten_pct from 2nd-to-last to last race
  - rpl_last_race_time_diff        : difference from best time in last race (ms, if available)
  - rpl_last_race_distance_diff    : |current distance - last race distance|
  - rpl_class_change_indicator     : +1 smaller field (higher class), -1 bigger, 0 similar
  - rpl_last_race_margins          : position / nombre_partants in last race
  - rpl_win_after_place_rate       : how often horse wins after placing (but not winning)
  - rpl_bounce_rate                : how often horse follows a win with a loss

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (lightweight index)
  - Phase 2 sorts the index chronologically
  - Phase 3 seeks back into the file, processes course by course,
    and streams output directly to disk
  - gc.collect() called every 500K records
  - Writes to .tmp then atomic rename

Usage:
    python feature_builders/race_replay_features_builder.py
    python feature_builders/race_replay_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_replay_features")

# Progress log every N records
_LOG_EVERY = 500_000

# Field-size similarity threshold for class change indicator
_FIELD_SIZE_SIMILAR_THRESHOLD = 2


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ===========================================================================
# STATE TRACKER (per horse)
# ===========================================================================


class _HorseReplayState:
    """Tracks race replay state for a single horse.

    Kept minimal with __slots__ to save memory across hundreds of thousands
    of horses.
    """

    __slots__ = (
        "last_position",
        "last_partants",
        "last_distance",
        "last_temps",
        "last_time_diff",
        "last_is_gagnant",
        "last_is_place",
        "positions_history",    # deque of (position, nombre_partants) tuples, maxlen=5
        "place_then_win_count",
        "place_then_win_opps",
        "win_then_loss_count",
        "wins_count",
    )

    def __init__(self) -> None:
        self.last_position: Optional[int] = None
        self.last_partants: Optional[int] = None
        self.last_distance: Optional[int] = None
        self.last_temps: Optional[float] = None
        self.last_time_diff: Optional[float] = None
        self.last_is_gagnant: bool = False
        self.last_is_place: bool = False
        # deque of (position, partants) tuples for last 5 races
        self.positions_history: deque = deque(maxlen=5)
        # win_after_place tracking
        self.place_then_win_count: int = 0  # times horse won after placing (not winning)
        self.place_then_win_opps: int = 0   # opportunities (placed but didn't win, then ran again)
        # bounce tracking
        self.win_then_loss_count: int = 0   # times horse lost after a win
        self.wins_count: int = 0            # total wins so far


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek-based processing)
# ===========================================================================


def build_race_replay_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build race replay features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Race Replay Features Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseReplayState] = defaultdict(_HorseReplayState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "rpl_last_race_beaten_count": 0,
        "rpl_last_race_beaten_pct": 0,
        "rpl_avg_beaten_pct_3": 0,
        "rpl_improvement_vs_last": 0,
        "rpl_last_race_time_diff": 0,
        "rpl_last_race_distance_diff": 0,
        "rpl_class_change_indicator": 0,
        "rpl_last_race_margins": 0,
        "rpl_win_after_place_rate": 0,
        "rpl_bounce_rate": 0,
    }

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
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "position": _safe_int(rec.get("position_arrivee")),
                "is_gagnant": bool(rec.get("is_gagnant")),
                "is_place": bool(rec.get("is_place")),
                "cote_finale": _safe_float(rec.get("cote_finale")),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num_pmu": _safe_int(rec.get("num_pmu")) or 0,
                "nombre_partants": _safe_int(rec.get("nombre_partants")),
                "distance": _safe_int(rec.get("distance")),
                "discipline": rec.get("discipline", ""),
                "temps_ms": _safe_float(rec.get("temps_ms")),
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

            # Compute best time in this race (for time diff feature later update)
            race_temps = [
                r["temps_ms"] for r in course_group
                if r["temps_ms"] is not None and r["temps_ms"] > 0
            ]
            best_temps = min(race_temps) if race_temps else None

            current_distance = None
            for r in course_group:
                if r["distance"] is not None:
                    current_distance = r["distance"]
                    break

            current_nb_partants = None
            for r in course_group:
                if r["nombre_partants"] is not None:
                    current_nb_partants = r["nombre_partants"]
                    break

            # -- Snapshot BEFORE update: emit features for all partants --
            post_updates: list[tuple] = []

            for rec in course_group:
                horse_id = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if horse_id is None:
                    # No horse ID, emit all nulls
                    for k in fill_counts:
                        features[k] = None
                    fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                    n_written += 1
                    post_updates.append((rec, None))
                    continue

                st = horse_state[horse_id]

                # --- Feature 1: rpl_last_race_beaten_count ---
                if st.last_position is not None and st.last_partants is not None:
                    beaten_count = max(0, st.last_partants - st.last_position)
                    features["rpl_last_race_beaten_count"] = beaten_count
                    fill_counts["rpl_last_race_beaten_count"] += 1
                else:
                    beaten_count = None
                    features["rpl_last_race_beaten_count"] = None

                # --- Feature 2: rpl_last_race_beaten_pct ---
                if (
                    beaten_count is not None
                    and st.last_partants is not None
                    and st.last_partants > 1
                ):
                    beaten_pct = round(beaten_count / (st.last_partants - 1), 4)
                    features["rpl_last_race_beaten_pct"] = beaten_pct
                    fill_counts["rpl_last_race_beaten_pct"] += 1
                else:
                    beaten_pct = None
                    features["rpl_last_race_beaten_pct"] = None

                # --- Feature 3: rpl_avg_beaten_pct_3 ---
                # Compute beaten_pct for each of the last 3 entries in history
                history_pcts: list[float] = []
                for pos, npart in list(st.positions_history)[-3:]:
                    if pos is not None and npart is not None and npart > 1:
                        history_pcts.append(max(0, npart - pos) / (npart - 1))
                if len(history_pcts) >= 1:
                    features["rpl_avg_beaten_pct_3"] = round(
                        sum(history_pcts) / len(history_pcts), 4
                    )
                    fill_counts["rpl_avg_beaten_pct_3"] += 1
                else:
                    features["rpl_avg_beaten_pct_3"] = None

                # --- Feature 4: rpl_improvement_vs_last ---
                # Change in beaten_pct from 2nd-to-last to last race
                hist_list = list(st.positions_history)
                if len(hist_list) >= 2:
                    pos_prev, npart_prev = hist_list[-2]
                    pos_last, npart_last = hist_list[-1]
                    if (
                        pos_prev is not None and npart_prev is not None and npart_prev > 1
                        and pos_last is not None and npart_last is not None and npart_last > 1
                    ):
                        pct_prev = max(0, npart_prev - pos_prev) / (npart_prev - 1)
                        pct_last = max(0, npart_last - pos_last) / (npart_last - 1)
                        features["rpl_improvement_vs_last"] = round(pct_last - pct_prev, 4)
                        fill_counts["rpl_improvement_vs_last"] += 1
                    else:
                        features["rpl_improvement_vs_last"] = None
                else:
                    features["rpl_improvement_vs_last"] = None

                # --- Feature 5: rpl_last_race_time_diff ---
                # This uses the horse's last_temps vs best_temps from the HORSE's last race
                # We don't have the best time of the horse's last race stored, so we store
                # the horse's own time and compare to field later.
                # Actually, we need to store the best time of the last race the horse ran in.
                # For simplicity, we store the horse's own temps and the best temps of
                # its last race. We'll adjust state to hold both.
                # NOTE: st.last_temps here is the horse's own time in its last race.
                # We don't have best time of that race stored — omit or approximate.
                # Instead: store time_diff directly when updating.
                # For now: this feature uses stored last_temps as the horse's own time
                # and we'd need best_time from the same race. Since we process course by course,
                # we can compute best_time at update time. Let's store the diff directly.
                # We'll add a last_time_diff attribute to state.
                features["rpl_last_race_time_diff"] = None
                if st.last_time_diff is not None:
                    features["rpl_last_race_time_diff"] = st.last_time_diff
                    fill_counts["rpl_last_race_time_diff"] += 1

                # --- Feature 6: rpl_last_race_distance_diff ---
                if st.last_distance is not None and current_distance is not None:
                    features["rpl_last_race_distance_diff"] = abs(
                        current_distance - st.last_distance
                    )
                    fill_counts["rpl_last_race_distance_diff"] += 1
                else:
                    features["rpl_last_race_distance_diff"] = None

                # --- Feature 7: rpl_class_change_indicator ---
                if st.last_partants is not None and current_nb_partants is not None:
                    diff = current_nb_partants - st.last_partants
                    if abs(diff) <= _FIELD_SIZE_SIMILAR_THRESHOLD:
                        features["rpl_class_change_indicator"] = 0
                    elif diff < 0:
                        features["rpl_class_change_indicator"] = 1   # smaller field = higher class
                    else:
                        features["rpl_class_change_indicator"] = -1  # bigger field = lower class
                    fill_counts["rpl_class_change_indicator"] += 1
                else:
                    features["rpl_class_change_indicator"] = None

                # --- Feature 8: rpl_last_race_margins ---
                if st.last_position is not None and st.last_partants is not None and st.last_partants > 0:
                    features["rpl_last_race_margins"] = round(
                        st.last_position / st.last_partants, 4
                    )
                    fill_counts["rpl_last_race_margins"] += 1
                else:
                    features["rpl_last_race_margins"] = None

                # --- Feature 9: rpl_win_after_place_rate ---
                if st.place_then_win_opps > 0:
                    features["rpl_win_after_place_rate"] = round(
                        st.place_then_win_count / st.place_then_win_opps, 4
                    )
                    fill_counts["rpl_win_after_place_rate"] += 1
                else:
                    features["rpl_win_after_place_rate"] = None

                # --- Feature 10: rpl_bounce_rate ---
                if st.wins_count > 0:
                    features["rpl_bounce_rate"] = round(
                        st.win_then_loss_count / st.wins_count, 4
                    )
                    fill_counts["rpl_bounce_rate"] += 1
                else:
                    features["rpl_bounce_rate"] = None

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                # Queue for post-race update
                post_updates.append((rec, horse_id))

            # -- Update states AFTER snapshotting all features (no leakage) --
            for rec, horse_id in post_updates:
                if horse_id is None:
                    continue

                st = horse_state[horse_id]
                position = rec["position"]
                nb_partants = rec["nombre_partants"]
                is_gagnant = rec["is_gagnant"]
                is_place = rec["is_place"]
                temps_ms = rec["temps_ms"]
                distance = rec["distance"]

                # Update win_after_place tracking:
                # If last race was "placed but not won", and this race is a win,
                # that's a win_after_place event.
                if st.last_position is not None:
                    was_placed_not_won = (
                        st.last_is_place and not st.last_is_gagnant
                    )
                    if was_placed_not_won:
                        st.place_then_win_opps += 1
                        if is_gagnant:
                            st.place_then_win_count += 1

                # Update bounce tracking:
                # If last race was a win and this race is not a win -> bounce
                if st.last_is_gagnant:
                    if not is_gagnant:
                        st.win_then_loss_count += 1

                if is_gagnant:
                    st.wins_count += 1

                # Compute time diff for this race (to be used in future snapshots)
                if temps_ms is not None and best_temps is not None and best_temps > 0:
                    st.last_time_diff = round(temps_ms - best_temps, 2)
                else:
                    st.last_time_diff = None

                # Update core state
                st.last_position = position
                st.last_partants = nb_partants
                st.last_distance = distance
                st.last_temps = temps_ms
                st.last_is_gagnant = is_gagnant
                st.last_is_place = is_place

                # Update positions history
                st.positions_history.append((position, nb_partants))

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Race replay build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

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
        description="Construction des features race-replay a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/race_replay_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_replay_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "race_replay_features.jsonl"
    build_race_replay_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
