#!/usr/bin/env python3
"""
feature_builders.track_bias_temporal_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Temporal track bias features -- how track conditions change over a racing
day and season.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically course-by-course, and computes per-partant track bias
features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - track_bias_temporal.jsonl   in builder_outputs/track_bias_temporal/

Features per partant (8):
  - tbt_hippo_fav_win_rate_30d    : favorite (lowest cote) win rate at this
                                    hippodrome in last 30 days
  - tbt_hippo_rail_bias_30d       : inner draw (num_pmu <= 4) win rate at
                                    this hippodrome in last 30 days
  - tbt_hippo_avg_field_size_30d  : average field size at this hippodrome
                                    in last 30 days
  - tbt_hippo_longshot_rate_30d   : rate of longshots (cote > 15) winning
                                    at this hippodrome recently
  - tbt_race_number_effect        : historical win rate for favorites at
                                    this race number position in the reunion
  - tbt_late_race_bias            : 1 if race number > 5 and historically
                                    inner draw wins more often late in the day
  - tbt_seasonal_hippo_form       : win rate of favorites at this hippo this
                                    month vs overall (seasonal track bias)
  - tbt_ground_deterioration      : estimated ground condition change =
                                    number of races already run today at
                                    this hippo (proxy for ground wear)

Usage:
    python feature_builders/track_bias_temporal_builder.py
    python feature_builders/track_bias_temporal_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/track_bias_temporal")
OUTPUT_FILENAME = "track_bias_temporal.jsonl"

_LOG_EVERY = 500_000

# Minimum observations before producing a stat
_MIN_RACES_30D = 5
_MIN_RACES_RACE_NUM = 10
_MIN_RACES_MONTH = 5

# Rolling window for hippo recent results
_ROLLING_DAYS = 30


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


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD (two-phase: index+sort, then seek-based processing)
# ===========================================================================


def build_track_bias_temporal_features(input_path: Path, output_path: Path, logger) -> int:
    """Build temporal track bias features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=" * 70)
    logger.info("track_bias_temporal_builder.py -- Temporal track bias features")
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

    # Global state:
    # hippo_recent_results: {hippo -> deque of (date, fav_won, rail_won, field_size, longshot_won)}
    hippo_recent: dict[str, deque] = defaultdict(deque)

    # race_number_stats: {race_num -> [fav_wins, total]}
    race_number_stats: dict[int, list] = defaultdict(lambda: [0, 0])

    # hippo_month_stats: {(hippo, month) -> [fav_wins, total]}
    hippo_month_stats: dict[tuple[str, int], list] = defaultdict(lambda: [0, 0])

    # hippo_overall_fav_stats: {hippo -> [fav_wins, total]}  (all-time for seasonal comparison)
    hippo_overall_fav: dict[str, list] = defaultdict(lambda: [0, 0])

    # Today tracker: {(hippo, date_str) -> races_already_run_count}
    hippo_day_races: dict[tuple[str, str], int] = defaultdict(int)

    # Late race rail stats: {race_num -> [rail_wins, rail_total]}
    late_race_rail: dict[int, list] = defaultdict(lambda: [0, 0])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts = {
        "tbt_hippo_fav_win_rate_30d": 0,
        "tbt_hippo_rail_bias_30d": 0,
        "tbt_hippo_avg_field_size_30d": 0,
        "tbt_hippo_longshot_rate_30d": 0,
        "tbt_race_number_effect": 0,
        "tbt_late_race_bias": 0,
        "tbt_seasonal_hippo_form": 0,
        "tbt_ground_deterioration": 0,
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

            race_date = _parse_date(course_date_str)
            race_month = race_date.month if race_date else None

            # Extract hippo and race number from first record
            hippo = (course_records[0].get("hippodrome_normalise") or "").strip()
            numero_course = _safe_int(course_records[0].get("numero_course"))

            # Purge old entries from hippo_recent for this hippo (older than 30 days)
            if hippo and race_date:
                cutoff = race_date - timedelta(days=_ROLLING_DAYS)
                dq = hippo_recent[hippo]
                while dq and dq[0][0] < cutoff:
                    dq.popleft()

            # -- Compute 30-day hippo aggregates (snapshot BEFORE update) --
            fav_wins_30d = 0
            rail_wins_30d = 0
            total_30d = 0
            field_sum_30d = 0
            longshot_wins_30d = 0

            if hippo:
                dq = hippo_recent[hippo]
                for entry in dq:
                    # entry: (date, fav_won, rail_won, field_size, longshot_won)
                    total_30d += 1
                    fav_wins_30d += entry[1]
                    rail_wins_30d += entry[2]
                    field_sum_30d += entry[3]
                    longshot_wins_30d += entry[4]

            # -- Ground deterioration: races already run today at this hippo --
            ground_races_today = hippo_day_races.get((hippo, course_date_str), 0) if hippo else 0

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            for rec in course_records:
                partant_uid = rec.get("partant_uid")
                if not partant_uid:
                    continue

                features: dict[str, Any] = {"partant_uid": partant_uid}

                # --- tbt_hippo_fav_win_rate_30d ---
                if hippo and total_30d >= _MIN_RACES_30D:
                    features["tbt_hippo_fav_win_rate_30d"] = round(fav_wins_30d / total_30d, 4)
                    fill_counts["tbt_hippo_fav_win_rate_30d"] += 1
                else:
                    features["tbt_hippo_fav_win_rate_30d"] = None

                # --- tbt_hippo_rail_bias_30d ---
                if hippo and total_30d >= _MIN_RACES_30D:
                    features["tbt_hippo_rail_bias_30d"] = round(rail_wins_30d / total_30d, 4)
                    fill_counts["tbt_hippo_rail_bias_30d"] += 1
                else:
                    features["tbt_hippo_rail_bias_30d"] = None

                # --- tbt_hippo_avg_field_size_30d ---
                if hippo and total_30d >= _MIN_RACES_30D:
                    features["tbt_hippo_avg_field_size_30d"] = round(field_sum_30d / total_30d, 2)
                    fill_counts["tbt_hippo_avg_field_size_30d"] += 1
                else:
                    features["tbt_hippo_avg_field_size_30d"] = None

                # --- tbt_hippo_longshot_rate_30d ---
                if hippo and total_30d >= _MIN_RACES_30D:
                    features["tbt_hippo_longshot_rate_30d"] = round(longshot_wins_30d / total_30d, 4)
                    fill_counts["tbt_hippo_longshot_rate_30d"] += 1
                else:
                    features["tbt_hippo_longshot_rate_30d"] = None

                # --- tbt_race_number_effect ---
                if numero_course is not None and numero_course > 0:
                    rns = race_number_stats[numero_course]
                    if rns[1] >= _MIN_RACES_RACE_NUM:
                        features["tbt_race_number_effect"] = round(rns[0] / rns[1], 4)
                        fill_counts["tbt_race_number_effect"] += 1
                    else:
                        features["tbt_race_number_effect"] = None
                else:
                    features["tbt_race_number_effect"] = None

                # --- tbt_late_race_bias ---
                if numero_course is not None and numero_course > 5:
                    lr = late_race_rail.get(numero_course)
                    if lr is not None and lr[1] >= _MIN_RACES_RACE_NUM:
                        rail_wr_late = lr[0] / lr[1]
                        features["tbt_late_race_bias"] = 1 if rail_wr_late > 0.5 else 0
                        fill_counts["tbt_late_race_bias"] += 1
                    else:
                        features["tbt_late_race_bias"] = None
                else:
                    features["tbt_late_race_bias"] = None

                # --- tbt_seasonal_hippo_form ---
                if hippo and race_month is not None:
                    mstats = hippo_month_stats.get((hippo, race_month))
                    overall = hippo_overall_fav.get(hippo)
                    if (
                        mstats is not None and mstats[1] >= _MIN_RACES_MONTH
                        and overall is not None and overall[1] >= _MIN_RACES_RACE_NUM
                    ):
                        month_wr = mstats[0] / mstats[1]
                        overall_wr = overall[0] / overall[1]
                        if overall_wr > 0:
                            features["tbt_seasonal_hippo_form"] = round(month_wr / overall_wr, 4)
                            fill_counts["tbt_seasonal_hippo_form"] += 1
                        else:
                            features["tbt_seasonal_hippo_form"] = None
                    else:
                        features["tbt_seasonal_hippo_form"] = None
                else:
                    features["tbt_seasonal_hippo_form"] = None

                # --- tbt_ground_deterioration ---
                features["tbt_ground_deterioration"] = ground_races_today
                fill_counts["tbt_ground_deterioration"] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after race results --
            # Identify favorite (lowest cote) and whether rail/longshot won
            best_cote = None
            fav_partant_uid = None
            winner_num_pmu = None
            winner_cote = None

            for rec in course_records:
                cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_probable"))
                if cote is not None and cote > 0:
                    if best_cote is None or cote < best_cote:
                        best_cote = cote
                        fav_partant_uid = rec.get("partant_uid")

                if bool(rec.get("is_gagnant")):
                    winner_num_pmu = _safe_int(rec.get("num_pmu"))
                    winner_cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_probable"))

            # Did the favorite win?
            fav_won = 0
            if fav_partant_uid is not None:
                for rec in course_records:
                    if rec.get("partant_uid") == fav_partant_uid and bool(rec.get("is_gagnant")):
                        fav_won = 1
                        break

            # Did a rail horse (num_pmu <= 4) win?
            rail_won = 1 if (winner_num_pmu is not None and winner_num_pmu <= 4) else 0

            # Did a longshot (cote > 15) win?
            longshot_won = 1 if (winner_cote is not None and winner_cote > 15) else 0

            # Field size
            field_size = _safe_int(course_records[0].get("nombre_partants")) or len(course_records)

            # Update hippo_recent
            if hippo and race_date:
                hippo_recent[hippo].append((race_date, fav_won, rail_won, field_size, longshot_won))

            # Update race_number_stats
            if numero_course is not None and numero_course > 0:
                race_number_stats[numero_course][1] += 1
                race_number_stats[numero_course][0] += fav_won

            # Update hippo_month_stats
            if hippo and race_month is not None:
                hippo_month_stats[(hippo, race_month)][1] += 1
                hippo_month_stats[(hippo, race_month)][0] += fav_won

            # Update hippo_overall_fav
            if hippo:
                hippo_overall_fav[hippo][1] += 1
                hippo_overall_fav[hippo][0] += fav_won

            # Update hippo_day_races (ground deterioration proxy)
            if hippo and course_date_str:
                hippo_day_races[(hippo, course_date_str)] += 1

            # Update late_race_rail stats
            if numero_course is not None and numero_course > 5 and winner_num_pmu is not None:
                late_race_rail[numero_course][1] += 1
                if winner_num_pmu <= 4:
                    late_race_rail[numero_course][0] += 1

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features ecrites en %.1fs (hippos: %d, race_nums: %d, hippo_months: %d)",
        n_written, elapsed, len(hippo_recent), len(race_number_stats), len(hippo_month_stats),
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
        description="Construction des features de biais temporel de piste"
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

    logger = setup_logging("track_bias_temporal_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_track_bias_temporal_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
