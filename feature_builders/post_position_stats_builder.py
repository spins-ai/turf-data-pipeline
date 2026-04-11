#!/usr/bin/env python3
"""
feature_builders.post_position_stats_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Post position (draw / num_pmu) statistical features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically course-by-course, and computes per-partant draw features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the draw statistics -- no future leakage.

Produces:
  - post_position_stats.jsonl   in builder_outputs/post_position_stats/

Features per partant (10):
  - pp_draw_position           : num_pmu as int
  - pp_draw_normalized         : num_pmu / nombre_partants (0-1 scale)
  - pp_is_rail                 : 1 if num_pmu <= 3 (close to rail)
  - pp_is_wide                 : 1 if num_pmu > nombre_partants * 0.75
  - pp_draw_win_rate_global    : win rate for this draw position across all past
  - pp_draw_win_rate_hippo     : win rate for this draw at this hippodrome
  - pp_draw_win_rate_distance  : win rate for this draw at this distance bucket
  - pp_horse_preferred_draw    : horse win rate at similar draw bucket (inner/mid/outer)
  - pp_draw_place_rate_hippo   : place rate for this draw at this hippodrome
  - pp_draw_advantage          : draw win rate at hippo / overall draw win rate (>1 = adv)

Usage:
    python feature_builders/post_position_stats_builder.py
    python feature_builders/post_position_stats_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/post_position_stats")
OUTPUT_FILENAME = "post_position_stats.jsonl"

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(val: Any) -> Optional[int]:
    """Convert value to int, return None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> Optional[float]:
    """Convert value to float, return None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _distance_bucket(distance: Optional[int]) -> Optional[str]:
    """Bucket distance into categories for aggregation."""
    if distance is None or distance <= 0:
        return None
    if distance < 1400:
        return "short"
    elif distance < 2000:
        return "mid_short"
    elif distance < 2600:
        return "mid"
    elif distance < 3200:
        return "mid_long"
    else:
        return "long"


def _draw_bucket(num_pmu: int) -> str:
    """Classify draw into inner / middle / outer."""
    if num_pmu <= 4:
        return "inner"
    elif num_pmu <= 10:
        return "middle"
    else:
        return "outer"


# ===========================================================================
# MAIN BUILD (two-phase: index+sort, then seek-based processing)
# ===========================================================================


def build_post_position_features(input_path: Path, output_path: Path, logger) -> int:
    """Build post position features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=" * 70)
    logger.info("post_position_stats_builder.py -- Draw / post position features")
    logger.info("=" * 70)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date_str, course_uid, num_pmu, byte_offset) --
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
            num_pmu = _safe_int(rec.get("num_pmu")) or 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 1b: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 2: Process course by course, streaming output --
    t2 = time.time()

    # Global state: draw_global  {num_pmu -> [wins, total]}
    draw_global: dict[int, list] = defaultdict(lambda: [0, 0])

    # draw_hippo  {(hippo, num_pmu) -> [wins, places, total]}
    draw_hippo: dict[tuple[str, int], list] = defaultdict(lambda: [0, 0, 0])

    # draw_dist  {(dist_bucket, num_pmu) -> [wins, total]}
    draw_dist: dict[tuple[str, int], list] = defaultdict(lambda: [0, 0])

    # Per-horse state: horse_draw_bucket  {horse_id -> {draw_bucket -> [wins, total]}}
    horse_draw_bucket: dict[str, dict[str, list]] = defaultdict(
        lambda: defaultdict(lambda: [0, 0])
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts = {
        "pp_draw_position": 0,
        "pp_draw_normalized": 0,
        "pp_is_rail": 0,
        "pp_is_wide": 0,
        "pp_draw_win_rate_global": 0,
        "pp_draw_win_rate_hippo": 0,
        "pp_draw_win_rate_distance": 0,
        "pp_horse_preferred_draw": 0,
        "pp_draw_place_rate_hippo": 0,
        "pp_draw_advantage": 0,
    }

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

            # Read this course's records from disk
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                course_records.append(rec)

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            for rec in course_records:
                partant_uid = rec.get("partant_uid")
                if not partant_uid:
                    continue

                num_pmu = _safe_int(rec.get("num_pmu"))
                nb_partants = _safe_int(rec.get("nombre_partants"))
                hippo = (rec.get("hippodrome_normalise") or "").strip()
                distance = _safe_int(rec.get("distance"))
                horse_id = rec.get("horse_id") or ""
                dist_bkt = _distance_bucket(distance)

                features: dict[str, Any] = {"partant_uid": partant_uid}

                # --- pp_draw_position ---
                if num_pmu is not None and num_pmu > 0:
                    features["pp_draw_position"] = num_pmu
                    fill_counts["pp_draw_position"] += 1
                else:
                    features["pp_draw_position"] = None

                # --- pp_draw_normalized ---
                if num_pmu is not None and num_pmu > 0 and nb_partants is not None and nb_partants > 0:
                    features["pp_draw_normalized"] = round(num_pmu / nb_partants, 4)
                    fill_counts["pp_draw_normalized"] += 1
                else:
                    features["pp_draw_normalized"] = None

                # --- pp_is_rail ---
                if num_pmu is not None and num_pmu > 0:
                    features["pp_is_rail"] = 1 if num_pmu <= 3 else 0
                    fill_counts["pp_is_rail"] += 1
                else:
                    features["pp_is_rail"] = None

                # --- pp_is_wide ---
                if num_pmu is not None and num_pmu > 0 and nb_partants is not None and nb_partants > 0:
                    features["pp_is_wide"] = 1 if num_pmu > nb_partants * 0.75 else 0
                    fill_counts["pp_is_wide"] += 1
                else:
                    features["pp_is_wide"] = None

                # --- pp_draw_win_rate_global ---
                if num_pmu is not None and num_pmu > 0:
                    g = draw_global[num_pmu]
                    if g[1] >= 5:
                        features["pp_draw_win_rate_global"] = round(g[0] / g[1], 4)
                        fill_counts["pp_draw_win_rate_global"] += 1
                    else:
                        features["pp_draw_win_rate_global"] = None
                else:
                    features["pp_draw_win_rate_global"] = None

                # --- pp_draw_win_rate_hippo ---
                if num_pmu is not None and num_pmu > 0 and hippo:
                    h = draw_hippo[(hippo, num_pmu)]
                    if h[2] >= 5:
                        features["pp_draw_win_rate_hippo"] = round(h[0] / h[2], 4)
                        fill_counts["pp_draw_win_rate_hippo"] += 1
                    else:
                        features["pp_draw_win_rate_hippo"] = None
                else:
                    features["pp_draw_win_rate_hippo"] = None

                # --- pp_draw_win_rate_distance ---
                if num_pmu is not None and num_pmu > 0 and dist_bkt:
                    d = draw_dist[(dist_bkt, num_pmu)]
                    if d[1] >= 5:
                        features["pp_draw_win_rate_distance"] = round(d[0] / d[1], 4)
                        fill_counts["pp_draw_win_rate_distance"] += 1
                    else:
                        features["pp_draw_win_rate_distance"] = None
                else:
                    features["pp_draw_win_rate_distance"] = None

                # --- pp_horse_preferred_draw ---
                if horse_id and num_pmu is not None and num_pmu > 0:
                    dbkt = _draw_bucket(num_pmu)
                    hbs = horse_draw_bucket[horse_id][dbkt]
                    if hbs[1] >= 3:
                        features["pp_horse_preferred_draw"] = round(hbs[0] / hbs[1], 4)
                        fill_counts["pp_horse_preferred_draw"] += 1
                    else:
                        features["pp_horse_preferred_draw"] = None
                else:
                    features["pp_horse_preferred_draw"] = None

                # --- pp_draw_place_rate_hippo ---
                if num_pmu is not None and num_pmu > 0 and hippo:
                    h = draw_hippo[(hippo, num_pmu)]
                    if h[2] >= 5:
                        features["pp_draw_place_rate_hippo"] = round(h[1] / h[2], 4)
                        fill_counts["pp_draw_place_rate_hippo"] += 1
                    else:
                        features["pp_draw_place_rate_hippo"] = None
                else:
                    features["pp_draw_place_rate_hippo"] = None

                # --- pp_draw_advantage ---
                if num_pmu is not None and num_pmu > 0 and hippo:
                    g = draw_global[num_pmu]
                    h = draw_hippo[(hippo, num_pmu)]
                    global_wr = g[0] / g[1] if g[1] >= 5 else None
                    hippo_wr = h[0] / h[2] if h[2] >= 5 else None
                    if global_wr is not None and global_wr > 0 and hippo_wr is not None:
                        features["pp_draw_advantage"] = round(hippo_wr / global_wr, 4)
                        fill_counts["pp_draw_advantage"] += 1
                    else:
                        features["pp_draw_advantage"] = None
                else:
                    features["pp_draw_advantage"] = None

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after race results --
            for rec in course_records:
                num_pmu = _safe_int(rec.get("num_pmu"))
                if num_pmu is None or num_pmu <= 0:
                    continue

                is_gagnant = bool(rec.get("is_gagnant"))
                is_place = bool(rec.get("is_place"))
                hippo = (rec.get("hippodrome_normalise") or "").strip()
                distance = _safe_int(rec.get("distance"))
                horse_id = rec.get("horse_id") or ""
                dist_bkt = _distance_bucket(distance)

                # draw_global
                draw_global[num_pmu][1] += 1
                if is_gagnant:
                    draw_global[num_pmu][0] += 1

                # draw_hippo
                if hippo:
                    draw_hippo[(hippo, num_pmu)][2] += 1
                    if is_gagnant:
                        draw_hippo[(hippo, num_pmu)][0] += 1
                    if is_place:
                        draw_hippo[(hippo, num_pmu)][1] += 1

                # draw_dist
                if dist_bkt:
                    draw_dist[(dist_bkt, num_pmu)][1] += 1
                    if is_gagnant:
                        draw_dist[(dist_bkt, num_pmu)][0] += 1

                # horse_draw_bucket
                if horse_id:
                    dbkt = _draw_bucket(num_pmu)
                    horse_draw_bucket[horse_id][dbkt][1] += 1
                    if is_gagnant:
                        horse_draw_bucket[horse_id][dbkt][0] += 1

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features ecrites en %.1fs (draws globaux: %d, hippo combos: %d, chevaux: %d)",
        n_written, elapsed, len(draw_global), len(draw_hippo), len(horse_draw_bucket),
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
        description="Construction des features de position au depart (draw / num_pmu)"
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

    logger = setup_logging("post_position_stats_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_post_position_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
