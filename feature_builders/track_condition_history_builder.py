#!/usr/bin/env python3
"""
feature_builders.track_condition_history_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Historical track condition patterns and horse performance correlation.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant track-condition features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the stats -- no future leakage.

Produces:
  - track_condition_history.jsonl   in builder_outputs/track_condition_history/

Features per partant (10):
  - tch_horse_heavy_ground_wr       : horse win rate when met_impact_meteo_score > 5
  - tch_horse_good_ground_wr        : horse win rate when met_impact_meteo_score <= 3
  - tch_horse_ground_preference     : good_wr - heavy_wr (positive = prefers good going)
  - tch_track_recent_form           : avg position of last 3 races at this hippodrome
  - tch_track_specialist_score      : 1 if horse has >3 wins at hippodrome, else 0
  - tch_distance_track_combo_wr     : horse win rate at this track + similar distance bucket
  - tch_conditions_match_score      : composite how many current conditions match best
  - tch_horse_nb_tracks_raced       : number of unique hippodromes raced at
  - tch_horse_track_loyalty         : pct of races at current hippodrome / total races
  - tch_hippo_surface_interaction   : horse win rate on this hippodrome surface type

Memory-optimised version:
  - Phase 1 reads only minimal tuples (not full dicts) for sorting
  - Phase 2 streams output to disk instead of accumulating in a list
  - gc.collect() called every 500K records

Usage:
    python feature_builders/track_condition_history_builder.py
    python feature_builders/track_condition_history_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/track_condition_history")

# Progress log every N records
_LOG_EVERY = 500_000

# Distance buckets (metres)
_DIST_BUCKETS = [
    (0, 1300, "sprint"),
    (1300, 1800, "mile"),
    (1800, 2400, "inter"),
    (2400, 999999, "stayer"),
]


def _distance_bucket(dist: Optional[int]) -> str:
    """Map raw distance (metres) to a bucket label."""
    if dist is None or dist <= 0:
        return "unknown"
    for lo, hi, label in _DIST_BUCKETS:
        if lo <= dist < hi:
            return label
    return "unknown"


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseTrackState:
    """Per-horse historical state for track condition features.

    Tracks:
      - ground_heavy: [wins, total] when met_impact_meteo_score > 5
      - ground_good: [wins, total] when met_impact_meteo_score <= 3
      - hippo_stats: {hippo -> [wins, total, positions_list]}
      - dist_track: {(hippo, dist_bucket) -> [wins, total]}
      - tracks_set: set of unique hippodromes
      - total_races: int
      - surface_stats: {surface -> [wins, total]}
      - best_conditions: {condition_key -> best_value}
    """

    __slots__ = (
        "ground_heavy",
        "ground_good",
        "hippo_stats",
        "dist_track",
        "tracks_set",
        "total_races",
        "surface_stats",
        "best_conditions",
    )

    def __init__(self) -> None:
        self.ground_heavy: list[int] = [0, 0]  # [wins, total]
        self.ground_good: list[int] = [0, 0]   # [wins, total]
        self.hippo_stats: dict[str, list] = {}  # hippo -> [wins, total, [positions]]
        self.dist_track: dict[tuple[str, str], list[int]] = {}  # (hippo, bucket) -> [wins, total]
        self.tracks_set: set[str] = set()
        self.total_races: int = 0
        self.surface_stats: dict[str, list[int]] = {}  # surface -> [wins, total]
        self.best_conditions: dict[str, Any] = {}  # best_surface, best_hippo, best_dist_bucket


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_track_condition_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build track condition history features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Track Condition History Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseTrackState] = defaultdict(_HorseTrackState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "tch_horse_heavy_ground_wr": 0,
        "tch_horse_good_ground_wr": 0,
        "tch_horse_ground_preference": 0,
        "tch_track_recent_form": 0,
        "tch_track_specialist_score": 0,
        "tch_distance_track_combo_wr": 0,
        "tch_conditions_match_score": 0,
        "tch_horse_nb_tracks_raced": 0,
        "tch_horse_track_loyalty": 0,
        "tch_hippo_surface_interaction": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            # Parse met_impact_meteo_score
            meteo_score = rec.get("met_impact_meteo_score")
            if meteo_score is not None:
                try:
                    meteo_score = float(meteo_score)
                except (ValueError, TypeError):
                    meteo_score = None

            # Parse distance
            distance = rec.get("distance")
            if distance is not None:
                try:
                    distance = int(distance)
                except (ValueError, TypeError):
                    distance = None

            # Parse position
            pos = rec.get("position_arrivee")
            if pos is not None:
                try:
                    pos = int(pos)
                except (ValueError, TypeError):
                    pos = None

            # Surface: type_piste or met_is_psf
            surface = rec.get("type_piste") or ""
            surface = surface.strip().upper()
            is_psf = rec.get("met_is_psf")
            if is_psf:
                surface = "PSF"
            elif not surface:
                surface = "UNKNOWN"

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "gagnant": bool(rec.get("is_gagnant")),
                "place": bool(rec.get("is_place")),
                "hippo": rec.get("hippodrome_normalise", "") or "",
                "distance": distance,
                "discipline": (rec.get("discipline") or "").strip().upper(),
                "corde": rec.get("corde"),
                "meteo_score": meteo_score,
                "position": pos,
                "surface": surface,
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

            # -- Snapshot BEFORE update: emit features for all partants --
            for rec in course_group:
                horse = rec["horse_id"]
                hippo = rec["hippo"]
                distance = rec["distance"]
                meteo_score = rec["meteo_score"]
                surface = rec["surface"]
                dist_bucket = _distance_bucket(distance)

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if horse:
                    st = horse_state[horse]

                    # 1. tch_horse_heavy_ground_wr
                    if st.ground_heavy[1] > 0:
                        val = round(st.ground_heavy[0] / st.ground_heavy[1], 4)
                        features["tch_horse_heavy_ground_wr"] = val
                        fill_counts["tch_horse_heavy_ground_wr"] += 1
                    else:
                        features["tch_horse_heavy_ground_wr"] = None

                    # 2. tch_horse_good_ground_wr
                    if st.ground_good[1] > 0:
                        val = round(st.ground_good[0] / st.ground_good[1], 4)
                        features["tch_horse_good_ground_wr"] = val
                        fill_counts["tch_horse_good_ground_wr"] += 1
                    else:
                        features["tch_horse_good_ground_wr"] = None

                    # 3. tch_horse_ground_preference
                    good_wr = features["tch_horse_good_ground_wr"]
                    heavy_wr = features["tch_horse_heavy_ground_wr"]
                    if good_wr is not None and heavy_wr is not None:
                        features["tch_horse_ground_preference"] = round(good_wr - heavy_wr, 4)
                        fill_counts["tch_horse_ground_preference"] += 1
                    else:
                        features["tch_horse_ground_preference"] = None

                    # 4. tch_track_recent_form (avg position of last 3 races at this hippo)
                    hippo_data = st.hippo_stats.get(hippo) if hippo else None
                    if hippo_data and hippo_data[2]:
                        last3 = hippo_data[2][-3:]
                        features["tch_track_recent_form"] = round(sum(last3) / len(last3), 2)
                        fill_counts["tch_track_recent_form"] += 1
                    else:
                        features["tch_track_recent_form"] = None

                    # 5. tch_track_specialist_score
                    if hippo_data:
                        features["tch_track_specialist_score"] = 1 if hippo_data[0] > 3 else 0
                        fill_counts["tch_track_specialist_score"] += 1
                    else:
                        features["tch_track_specialist_score"] = None

                    # 6. tch_distance_track_combo_wr
                    dt_key = (hippo, dist_bucket) if hippo else None
                    dt_data = st.dist_track.get(dt_key) if dt_key else None
                    if dt_data and dt_data[1] > 0:
                        features["tch_distance_track_combo_wr"] = round(dt_data[0] / dt_data[1], 4)
                        fill_counts["tch_distance_track_combo_wr"] += 1
                    else:
                        features["tch_distance_track_combo_wr"] = None

                    # 7. tch_conditions_match_score
                    bc = st.best_conditions
                    if bc and st.total_races >= 3:
                        score = 0
                        n_checks = 0
                        # Surface match
                        if "best_surface" in bc and surface:
                            n_checks += 1
                            if bc["best_surface"] == surface:
                                score += 1
                        # Hippo match
                        if "best_hippo" in bc and hippo:
                            n_checks += 1
                            if bc["best_hippo"] == hippo:
                                score += 1
                        # Distance bucket match
                        if "best_dist_bucket" in bc and dist_bucket != "unknown":
                            n_checks += 1
                            if bc["best_dist_bucket"] == dist_bucket:
                                score += 1
                        if n_checks > 0:
                            features["tch_conditions_match_score"] = round(score / n_checks, 4)
                            fill_counts["tch_conditions_match_score"] += 1
                        else:
                            features["tch_conditions_match_score"] = None
                    else:
                        features["tch_conditions_match_score"] = None

                    # 8. tch_horse_nb_tracks_raced
                    if st.total_races > 0:
                        features["tch_horse_nb_tracks_raced"] = len(st.tracks_set)
                        fill_counts["tch_horse_nb_tracks_raced"] += 1
                    else:
                        features["tch_horse_nb_tracks_raced"] = None

                    # 9. tch_horse_track_loyalty
                    if st.total_races > 0 and hippo and hippo_data:
                        features["tch_horse_track_loyalty"] = round(
                            hippo_data[1] / st.total_races, 4
                        )
                        fill_counts["tch_horse_track_loyalty"] += 1
                    else:
                        features["tch_horse_track_loyalty"] = None

                    # 10. tch_hippo_surface_interaction
                    surf_data = st.surface_stats.get(surface) if surface else None
                    if surf_data and surf_data[1] > 0:
                        features["tch_hippo_surface_interaction"] = round(
                            surf_data[0] / surf_data[1], 4
                        )
                        fill_counts["tch_hippo_surface_interaction"] += 1
                    else:
                        features["tch_hippo_surface_interaction"] = None

                else:
                    # No horse ID: all features null
                    for feat_name in fill_counts:
                        features[feat_name] = None

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER snapshot (no leakage) --
            for rec in course_group:
                horse = rec["horse_id"]
                if not horse:
                    continue

                hippo = rec["hippo"]
                distance = rec["distance"]
                meteo_score = rec["meteo_score"]
                surface = rec["surface"]
                is_winner = rec["gagnant"]
                position = rec["position"]
                dist_bucket = _distance_bucket(distance)

                st = horse_state[horse]
                st.total_races += 1

                # Ground condition stats
                if meteo_score is not None:
                    if meteo_score > 5:
                        st.ground_heavy[1] += 1
                        if is_winner:
                            st.ground_heavy[0] += 1
                    if meteo_score <= 3:
                        st.ground_good[1] += 1
                        if is_winner:
                            st.ground_good[0] += 1

                # Hippodrome stats
                if hippo:
                    st.tracks_set.add(hippo)
                    if hippo not in st.hippo_stats:
                        st.hippo_stats[hippo] = [0, 0, []]  # [wins, total, positions]
                    hd = st.hippo_stats[hippo]
                    hd[1] += 1
                    if is_winner:
                        hd[0] += 1
                    if position is not None and position > 0:
                        hd[2].append(position)
                        # Keep only last 10 positions to bound memory
                        if len(hd[2]) > 10:
                            hd[2] = hd[2][-10:]

                # Distance + track combo
                if hippo and dist_bucket != "unknown":
                    dt_key = (hippo, dist_bucket)
                    if dt_key not in st.dist_track:
                        st.dist_track[dt_key] = [0, 0]
                    st.dist_track[dt_key][1] += 1
                    if is_winner:
                        st.dist_track[dt_key][0] += 1

                # Surface stats
                if surface and surface != "UNKNOWN":
                    if surface not in st.surface_stats:
                        st.surface_stats[surface] = [0, 0]
                    st.surface_stats[surface][1] += 1
                    if is_winner:
                        st.surface_stats[surface][0] += 1

                # Update best_conditions (recalculate after each race)
                _update_best_conditions(st)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Track condition history build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)", k, v, n_written,
            100 * v / n_written if n_written else 0,
        )

    return n_written


def _update_best_conditions(st: _HorseTrackState) -> None:
    """Recalculate best performing conditions for a horse."""
    bc = st.best_conditions

    # Best surface (highest win rate, min 2 races)
    best_surf = None
    best_surf_wr = -1.0
    for surf, (wins, total) in st.surface_stats.items():
        if total >= 2:
            wr = wins / total
            if wr > best_surf_wr:
                best_surf_wr = wr
                best_surf = surf
    if best_surf:
        bc["best_surface"] = best_surf

    # Best hippodrome (highest win rate, min 2 races)
    best_hippo = None
    best_hippo_wr = -1.0
    for hippo, (wins, total, _) in st.hippo_stats.items():
        if total >= 2:
            wr = wins / total
            if wr > best_hippo_wr:
                best_hippo_wr = wr
                best_hippo = hippo
    if best_hippo:
        bc["best_hippo"] = best_hippo

    # Best distance bucket (highest win rate, min 2 races)
    best_db = None
    best_db_wr = -1.0
    for (_, db), (wins, total) in st.dist_track.items():
        if total >= 2:
            wr = wins / total
            if wr > best_db_wr:
                best_db_wr = wr
                best_db = db
    if best_db:
        bc["best_dist_bucket"] = best_db


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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features track condition history a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/track_condition_history/)",
    )
    args = parser.parse_args()

    logger = setup_logging("track_condition_history_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "track_condition_history.jsonl"
    build_track_condition_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
