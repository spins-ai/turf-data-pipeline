#!/usr/bin/env python3
"""
feature_builders.race_timing_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race timing and speed analysis features per partant.

Reads partants_master.jsonl in streaming mode, processes all records
in two passes, and computes per-partant timing/speed features.

Pass 1: build course_uid -> {temps list with num_pmu, min_temps (winner),
        avg_temps, std_temps, speed stats}.
Pass 2: stream and compute features per partant using precomputed race stats.

Temporal integrity: all features are computed from the same race's data
(no future leakage -- timing is observed at race time).

Produces:
  - race_timing.jsonl   in builder_outputs/race_timing/

Features per partant (10):
  - rtm_speed_kmh            : (distance / temps_ms) * 3_600_000
  - rtm_reduction_km         : reduction_km_ms as float
  - rtm_time_vs_winner       : pct behind winner  ((horse - winner) / winner * 100)
  - rtm_time_vs_field_avg    : pct vs field avg    ((horse - avg) / avg * 100)
  - rtm_is_fastest           : 1 if this horse has the lowest temps_ms in race
  - rtm_speed_rank           : rank by temps_ms within race (1 = fastest)
  - rtm_field_avg_speed      : average speed_kmh across all timed runners
  - rtm_field_speed_spread   : max_speed - min_speed in the race
  - rtm_pace_figure          : standardized speed = (horse_speed - field_avg) / field_std
  - rtm_has_time             : 1 if temps_ms is available, 0 otherwise

Usage:
    python feature_builders/race_timing_builder.py
    python feature_builders/race_timing_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_timing")

_LOG_EVERY = 500_000

# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    if v is None:
        return None
    try:
        val = float(v)
        return val if val > 0 else None
    except (TypeError, ValueError):
        return None


def _speed_kmh(distance: Optional[float], temps_ms: Optional[float]) -> Optional[float]:
    """Compute speed in km/h from distance (m) and time (ms)."""
    if distance is None or temps_ms is None or distance <= 0 or temps_ms <= 0:
        return None
    return round((distance / temps_ms) * 3_600_000, 4)


def _safe_mean(values: list[float]) -> Optional[float]:
    """Mean of non-empty list, or None."""
    if not values:
        return None
    return sum(values) / len(values)


def _safe_stdev(values: list[float]) -> Optional[float]:
    """Population stdev of non-empty list (len >= 2), or None."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return math.sqrt(variance) if variance > 0 else 0.0


# ===========================================================================
# MAIN BUILD (two-pass, memory-optimised, streaming output)
# ===========================================================================


def build_race_timing_features(input_path: Path, output_path: Path, logger) -> int:
    """Build race timing features from partants_master.jsonl.

    Two-pass approach:
      Pass 1: aggregate per-course timing stats (min, avg, std, speeds).
      Pass 2: stream records again, compute per-partant features using
              precomputed race stats, write directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Race Timing Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Pass 1: Aggregate per-course timing stats --
    logger.info("Pass 1: aggregation des stats de timing par course...")

    # course_uid -> list of (num_pmu, temps_ms, distance)
    course_data: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1: lu %d records...", n_read)

        course_uid = rec.get("course_uid", "")
        if not course_uid:
            continue

        temps_ms = _safe_float(rec.get("temps_ms"))
        distance = _safe_float(rec.get("distance"))
        num_pmu = rec.get("num_pmu", 0) or 0
        try:
            num_pmu = int(num_pmu)
        except (ValueError, TypeError):
            num_pmu = 0

        if temps_ms is not None and distance is not None:
            course_data[course_uid].append((num_pmu, temps_ms, distance))

    logger.info(
        "Pass 1 terminee: %d records lus, %d courses avec timing en %.1fs",
        n_read, len(course_data), time.time() - t0,
    )

    # Precompute race-level stats
    # course_uid -> {min_temps, avg_temps, speeds[], avg_speed, std_speed, min_speed, max_speed}
    course_stats: dict[str, dict[str, Any]] = {}

    for cuid, entries in course_data.items():
        temps_list = [t for _, t, _ in entries]
        speeds = []
        for _, t, d in entries:
            s = _speed_kmh(d, t)
            if s is not None:
                speeds.append(s)

        min_temps = min(temps_list) if temps_list else None
        avg_temps = _safe_mean(temps_list)
        avg_speed = _safe_mean(speeds)
        std_speed = _safe_stdev(speeds)

        course_stats[cuid] = {
            "min_temps": min_temps,
            "avg_temps": avg_temps,
            "avg_speed": avg_speed,
            "std_speed": std_speed,
            "min_speed": min(speeds) if speeds else None,
            "max_speed": max(speeds) if speeds else None,
            "temps_by_num": {num: t for num, t, _ in entries},
        }

    # Free pass-1 raw data
    del course_data
    gc.collect()

    logger.info("Stats precomputees pour %d courses.", len(course_stats))

    # -- Pass 2: Stream records, compute per-partant features, write to disk --
    logger.info("Pass 2: calcul des features par partant...")
    t1 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    fill_counts = {
        "rtm_speed_kmh": 0,
        "rtm_reduction_km": 0,
        "rtm_time_vs_winner": 0,
        "rtm_time_vs_field_avg": 0,
        "rtm_is_fastest": 0,
        "rtm_speed_rank": 0,
        "rtm_field_avg_speed": 0,
        "rtm_field_speed_spread": 0,
        "rtm_pace_figure": 0,
        "rtm_has_time": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Pass 2: traite %d records...", n_processed)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid", "")
            temps_ms = _safe_float(rec.get("temps_ms"))
            distance = _safe_float(rec.get("distance"))
            reduction_km = _safe_float(rec.get("reduction_km_ms"))
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            stats = course_stats.get(course_uid)

            # --- Feature computation ---
            features: dict[str, Any] = {"partant_uid": partant_uid}

            # 10. rtm_has_time (data quality flag)
            has_time = 1 if temps_ms is not None else 0
            features["rtm_has_time"] = has_time
            fill_counts["rtm_has_time"] += 1  # always filled

            # 1. rtm_speed_kmh
            speed = _speed_kmh(distance, temps_ms)
            features["rtm_speed_kmh"] = speed
            if speed is not None:
                fill_counts["rtm_speed_kmh"] += 1

            # 2. rtm_reduction_km
            features["rtm_reduction_km"] = round(reduction_km, 4) if reduction_km is not None else None
            if reduction_km is not None:
                fill_counts["rtm_reduction_km"] += 1

            # Features requiring race stats
            if stats and temps_ms is not None:
                min_temps = stats["min_temps"]
                avg_temps = stats["avg_temps"]

                # 3. rtm_time_vs_winner
                if min_temps is not None and min_temps > 0:
                    val = round((temps_ms - min_temps) / min_temps * 100, 4)
                    features["rtm_time_vs_winner"] = val
                    fill_counts["rtm_time_vs_winner"] += 1
                else:
                    features["rtm_time_vs_winner"] = None

                # 4. rtm_time_vs_field_avg
                if avg_temps is not None and avg_temps > 0:
                    val = round((temps_ms - avg_temps) / avg_temps * 100, 4)
                    features["rtm_time_vs_field_avg"] = val
                    fill_counts["rtm_time_vs_field_avg"] += 1
                else:
                    features["rtm_time_vs_field_avg"] = None

                # 5. rtm_is_fastest
                if min_temps is not None:
                    features["rtm_is_fastest"] = 1 if abs(temps_ms - min_temps) < 0.5 else 0
                    fill_counts["rtm_is_fastest"] += 1
                else:
                    features["rtm_is_fastest"] = None

                # 6. rtm_speed_rank
                temps_by_num = stats.get("temps_by_num", {})
                if temps_by_num:
                    sorted_times = sorted(temps_by_num.values())
                    rank = sorted_times.index(temps_ms) + 1 if temps_ms in sorted_times else None
                    features["rtm_speed_rank"] = rank
                    if rank is not None:
                        fill_counts["rtm_speed_rank"] += 1
                else:
                    features["rtm_speed_rank"] = None
            else:
                features["rtm_time_vs_winner"] = None
                features["rtm_time_vs_field_avg"] = None
                features["rtm_is_fastest"] = None
                features["rtm_speed_rank"] = None

            # 7. rtm_field_avg_speed (race-level, always available if stats exist)
            if stats:
                avg_spd = stats["avg_speed"]
                features["rtm_field_avg_speed"] = round(avg_spd, 4) if avg_spd is not None else None
                if avg_spd is not None:
                    fill_counts["rtm_field_avg_speed"] += 1

                # 8. rtm_field_speed_spread
                min_spd = stats["min_speed"]
                max_spd = stats["max_speed"]
                if min_spd is not None and max_spd is not None:
                    features["rtm_field_speed_spread"] = round(max_spd - min_spd, 4)
                    fill_counts["rtm_field_speed_spread"] += 1
                else:
                    features["rtm_field_speed_spread"] = None
            else:
                features["rtm_field_avg_speed"] = None
                features["rtm_field_speed_spread"] = None

            # 9. rtm_pace_figure (standardized speed)
            if speed is not None and stats:
                avg_spd = stats["avg_speed"]
                std_spd = stats["std_speed"]
                if avg_spd is not None and std_spd is not None and std_spd > 0:
                    features["rtm_pace_figure"] = round((speed - avg_spd) / std_spd, 4)
                    fill_counts["rtm_pace_figure"] += 1
                else:
                    features["rtm_pace_figure"] = None
            else:
                features["rtm_pace_figure"] = None

            # Write to output
            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Race timing build termine: %d features en %.1fs (%d courses)",
        n_written, elapsed, len(course_stats),
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
        description="Construction des features de timing/vitesse a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/race_timing/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_timing_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "race_timing.jsonl"
    build_race_timing_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
