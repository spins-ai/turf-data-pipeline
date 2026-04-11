#!/usr/bin/env python3
"""
feature_builders.distance_speed_curve_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Distance-Speed Curve features for horses.

Each horse has a unique speed profile across different distances.
Modeling this curve helps predict performance at any distance.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant distance-speed curve features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the stats -- no future leakage.

Produces:
  - distance_speed_curve_features.jsonl

Features per partant (10):
  - dsc_optimal_distance        : distance at which horse has best avg position
  - dsc_distance_from_optimal   : abs(current_distance - optimal_distance) / 1000
  - dsc_speed_at_distance       : horse's avg reduction_km at this distance band (+/- 200m)
  - dsc_speed_improvement       : speed at this distance vs overall avg speed
  - dsc_distance_versatility    : number of distinct distance bands with >= 2 runs
  - dsc_short_distance_rating   : avg position at distances < 1800m, normalized
  - dsc_medium_distance_rating  : avg position at 1800-2400m, normalized
  - dsc_long_distance_rating    : avg position at > 2400m, normalized
  - dsc_distance_experience     : number of races at this distance band
  - dsc_stamina_curve_slope     : slope of position vs distance (positive = sprinter)

Memory-optimised:
  - Phase 1: index + chronological sort (lightweight tuples)
  - Phase 2: seek-based streaming output
  - gc.collect() every 500K records

Usage:
    python feature_builders/distance_speed_curve_builder.py
    python feature_builders/distance_speed_curve_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/distance_speed_curve")
OUTPUT_FILENAME = "distance_speed_curve_features.jsonl"

_LOG_EVERY = 500_000

# Distance bands: round to nearest 400m
_BAND_STEP = 400

# Short / medium / long thresholds
_SHORT_MAX = 1800
_MEDIUM_MIN = 1800
_MEDIUM_MAX = 2400
_LONG_MIN = 2400


def _distance_to_band(distance_m: int) -> int:
    """Round distance to nearest 400m band (e.g. 1200, 1600, 2000, ...)."""
    if distance_m <= 0:
        return 0
    return round(distance_m / _BAND_STEP) * _BAND_STEP


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _BandStats:
    """Stats for a single distance band."""

    __slots__ = ("positions", "speeds", "count")

    def __init__(self) -> None:
        self.positions: list[float] = []
        self.speeds: list[float] = []
        self.count: int = 0

    def avg_position(self) -> Optional[float]:
        if not self.positions:
            return None
        return sum(self.positions) / len(self.positions)

    def avg_speed(self) -> Optional[float]:
        if not self.speeds:
            return None
        return sum(self.speeds) / len(self.speeds)


class _HorseDistanceProfile:
    """Per-horse distance profile tracking."""

    __slots__ = ("bands", "all_positions", "all_speeds")

    def __init__(self) -> None:
        self.bands: dict[int, _BandStats] = {}
        self.all_positions: list[float] = []
        self.all_speeds: list[float] = []

    def get_or_create_band(self, band: int) -> _BandStats:
        if band not in self.bands:
            self.bands[band] = _BandStats()
        return self.bands[band]

    def overall_avg_speed(self) -> Optional[float]:
        if not self.all_speeds:
            return None
        return sum(self.all_speeds) / len(self.all_speeds)

    def overall_avg_position(self) -> Optional[float]:
        if not self.all_positions:
            return None
        return sum(self.all_positions) / len(self.all_positions)

    def optimal_distance(self) -> Optional[int]:
        """Band with best (lowest) avg position, min 2 races."""
        best_band = None
        best_avg = float("inf")
        for band, stats in self.bands.items():
            if stats.count < 2:
                continue
            avg_pos = stats.avg_position()
            if avg_pos is not None and avg_pos < best_avg:
                best_avg = avg_pos
                best_band = band
        return best_band

    def distance_versatility(self) -> int:
        """Number of distinct bands with at least 2 runs."""
        return sum(1 for s in self.bands.values() if s.count >= 2)

    def _range_rating(self, lo: Optional[int], hi: Optional[int]) -> Optional[float]:
        """Avg position for bands in [lo, hi), normalized by overall avg position.

        Returns a value where lower = better. Normalized so 1.0 = average.
        None if no data.
        """
        total_pos = 0.0
        total_count = 0
        for band, stats in self.bands.items():
            if lo is not None and band < lo:
                continue
            if hi is not None and band >= hi:
                continue
            if stats.count == 0:
                continue
            total_pos += sum(stats.positions)
            total_count += stats.count
        if total_count == 0:
            return None
        avg = total_pos / total_count
        overall = self.overall_avg_position()
        if overall is None or overall == 0:
            return None
        return round(avg / overall, 4)

    def short_rating(self) -> Optional[float]:
        return self._range_rating(None, _SHORT_MAX)

    def medium_rating(self) -> Optional[float]:
        return self._range_rating(_MEDIUM_MIN, _MEDIUM_MAX)

    def long_rating(self) -> Optional[float]:
        return self._range_rating(_LONG_MIN, None)

    def stamina_curve_slope(self) -> Optional[float]:
        """Slope of position vs distance band.

        Positive slope = position worsens at longer distances (sprinter).
        Negative slope = position improves at longer distances (stayer).
        Uses simple linear regression.
        """
        points: list[tuple[float, float]] = []
        for band, stats in self.bands.items():
            if stats.count < 2:
                continue
            avg_pos = stats.avg_position()
            if avg_pos is not None:
                points.append((float(band), avg_pos))
        if len(points) < 2:
            return None
        n = len(points)
        sum_x = sum(p[0] for p in points)
        sum_y = sum(p[1] for p in points)
        sum_xy = sum(p[0] * p[1] for p in points)
        sum_x2 = sum(p[0] ** 2 for p in points)
        denom = n * sum_x2 - sum_x ** 2
        if denom == 0:
            return None
        slope = (n * sum_xy - sum_x * sum_y) / denom
        # Normalize per 1000m
        return round(slope * 1000, 4)

    def add_race(self, band: int, position: float, speed: Optional[float]) -> None:
        """Update state AFTER snapshot."""
        bs = self.get_or_create_band(band)
        bs.positions.append(position)
        bs.count += 1
        self.all_positions.append(position)
        if speed is not None:
            bs.speeds.append(speed)
            self.all_speeds.append(speed)


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        v = int(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_distance_speed_curve_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build distance-speed curve features from partants_master.jsonl.

    Index + chronological sort + seek approach.
    Returns total number of feature records written.
    """
    logger.info("=== Distance-Speed Curve Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    # (date_str, course_uid, num_pmu, byte_offset)
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line_s = line.strip()
            if not line_s:
                continue
            try:
                rec = json.loads(line_s)
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
        "Phase 1: %d records indexes en %.1fs", len(index), time.time() - t0
    )

    # -- Phase 2: Sort --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_profiles: dict[str, _HorseDistanceProfile] = defaultdict(_HorseDistanceProfile)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    feature_names = [
        "dsc_optimal_distance",
        "dsc_distance_from_optimal",
        "dsc_speed_at_distance",
        "dsc_speed_improvement",
        "dsc_distance_versatility",
        "dsc_short_distance_rating",
        "dsc_medium_distance_rating",
        "dsc_long_distance_rating",
        "dsc_distance_experience",
        "dsc_stamina_curve_slope",
    ]
    fill_counts = {fn: 0 for fn in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect all entries for this course
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

            # Read records for this course
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                course_records.append(rec)

            # -- SNAPSHOT: compute features BEFORE updating state --
            update_queue: list[tuple[str, int, float, Optional[float]]] = []

            for rec in course_records:
                partant_uid = rec.get("partant_uid")
                course_uid_val = rec.get("course_uid", "")
                date_iso = rec.get("date_reunion_iso", "")
                cheval = rec.get("nom_cheval")

                # Parse distance
                distance_m = _safe_int(rec.get("distance"))
                if distance_m is None:
                    distance_m = _safe_int(rec.get("distance_metres"))

                # Parse position (place_arrivee)
                position = _safe_float(rec.get("place_arrivee"))
                if position is None:
                    position = _safe_float(rec.get("rang_arrivee"))

                # Parse speed
                speed = _safe_float(rec.get("reduction_km"))

                # Current distance band
                band = _distance_to_band(distance_m) if distance_m else None

                features: dict[str, Any] = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_val,
                    "date_reunion_iso": date_iso,
                }

                if cheval and band and band > 0:
                    profile = horse_profiles[cheval]

                    # dsc_optimal_distance
                    opt_dist = profile.optimal_distance()
                    features["dsc_optimal_distance"] = opt_dist
                    if opt_dist is not None:
                        fill_counts["dsc_optimal_distance"] += 1

                    # dsc_distance_from_optimal
                    if opt_dist is not None and distance_m:
                        features["dsc_distance_from_optimal"] = round(
                            abs(distance_m - opt_dist) / 1000.0, 4
                        )
                        fill_counts["dsc_distance_from_optimal"] += 1
                    else:
                        features["dsc_distance_from_optimal"] = None

                    # dsc_speed_at_distance: avg speed at this band (+/- 200m = same band)
                    band_stats = profile.bands.get(band)
                    if band_stats and band_stats.count > 0:
                        features["dsc_speed_at_distance"] = (
                            round(band_stats.avg_speed(), 4)
                            if band_stats.avg_speed() is not None
                            else None
                        )
                        if features["dsc_speed_at_distance"] is not None:
                            fill_counts["dsc_speed_at_distance"] += 1
                    else:
                        features["dsc_speed_at_distance"] = None

                    # dsc_speed_improvement: speed at this band vs overall
                    overall_spd = profile.overall_avg_speed()
                    band_spd = (
                        band_stats.avg_speed()
                        if band_stats and band_stats.count > 0
                        else None
                    )
                    if band_spd is not None and overall_spd is not None and overall_spd > 0:
                        features["dsc_speed_improvement"] = round(
                            band_spd - overall_spd, 4
                        )
                        fill_counts["dsc_speed_improvement"] += 1
                    else:
                        features["dsc_speed_improvement"] = None

                    # dsc_distance_versatility
                    versatility = profile.distance_versatility()
                    features["dsc_distance_versatility"] = versatility
                    if versatility > 0:
                        fill_counts["dsc_distance_versatility"] += 1

                    # dsc_short_distance_rating
                    short_r = profile.short_rating()
                    features["dsc_short_distance_rating"] = short_r
                    if short_r is not None:
                        fill_counts["dsc_short_distance_rating"] += 1

                    # dsc_medium_distance_rating
                    medium_r = profile.medium_rating()
                    features["dsc_medium_distance_rating"] = medium_r
                    if medium_r is not None:
                        fill_counts["dsc_medium_distance_rating"] += 1

                    # dsc_long_distance_rating
                    long_r = profile.long_rating()
                    features["dsc_long_distance_rating"] = long_r
                    if long_r is not None:
                        fill_counts["dsc_long_distance_rating"] += 1

                    # dsc_distance_experience
                    exp = band_stats.count if band_stats else 0
                    features["dsc_distance_experience"] = exp
                    if exp > 0:
                        fill_counts["dsc_distance_experience"] += 1

                    # dsc_stamina_curve_slope
                    slope = profile.stamina_curve_slope()
                    features["dsc_stamina_curve_slope"] = slope
                    if slope is not None:
                        fill_counts["dsc_stamina_curve_slope"] += 1

                    # Queue update for after snapshot
                    if position is not None:
                        update_queue.append((cheval, band, position, speed))

                else:
                    # No horse or no distance -- fill nulls
                    for fn in feature_names:
                        features[fn] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- UPDATE state after all snapshots for this course --
            for cheval, band, position, speed in update_queue:
                horse_profiles[cheval].add_race(band, position, speed)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Distance-Speed Curve build termine: %d features en %.1fs (chevaux profiles: %d)",
        n_written, elapsed, len(horse_profiles),
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features distance-speed curve a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("distance_speed_curve_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_distance_speed_curve_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
