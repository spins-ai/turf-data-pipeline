#!/usr/bin/env python3
"""
feature_builders.speed_distance_profile_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Speed x distance profiling features -- how a horse's speed varies by distance.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant speed-distance profile features.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.  Snapshot BEFORE update.

Memory-optimised version:
  - Phase 1 reads only minimal tuples (sort keys + byte offsets)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 re-reads records from disk via seek, streams output to .tmp
  - gc.collect() every 500K records

Produces:
  - speed_distance_profile.jsonl   in builder_outputs/speed_distance_profile/

Features per partant (8):
  - sdp_avg_speed_short        : average reduction_km_ms at distances < 1600m
  - sdp_avg_speed_mid          : average reduction_km_ms at distances 1600-2400m
  - sdp_avg_speed_long         : average reduction_km_ms at distances > 2400m
  - sdp_speed_at_current_dist  : average speed at current distance bucket
  - sdp_speed_vs_career_avg    : speed at current distance / overall career average speed
  - sdp_is_speed_type          : 1 if short_speed < mid_speed (faster at sprint = speed type)
  - sdp_is_stamina_type        : 1 if long_speed < short_speed (faster at long = stamina type)
  - sdp_speed_profile_hash     : categorical 0=speed, 1=balanced, 2=stamina, 3=unknown

State per horse:
  speed_by_bucket {bucket -> [sum_speed, count]}, overall_speed_sum, overall_count.

Usage:
    python feature_builders/speed_distance_profile_builder.py
    python feature_builders/speed_distance_profile_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/speed_distance_profile")

_LOG_EVERY = 500_000

# Distance bucket thresholds (metres)
_SHORT_MAX = 1600
_MID_MAX = 2400


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _SpeedDistState:
    """Per-horse speed-distance profile state, memory-optimised with __slots__."""

    __slots__ = ("speed_by_bucket", "overall_speed_sum", "overall_count")

    def __init__(self) -> None:
        # bucket -> [sum_speed, count]
        # buckets: "short" (<1600), "mid" (1600-2400), "long" (>2400)
        self.speed_by_bucket: dict[str, list[float]] = {}
        self.overall_speed_sum: float = 0.0
        self.overall_count: int = 0

    # -----------------------------------------------------------------
    # Snapshot BEFORE update
    # -----------------------------------------------------------------
    def snapshot(self, current_bucket: Optional[str]) -> dict[str, Any]:
        """Return features from CURRENT state (before this race's update)."""
        feats: dict[str, Any] = {
            "sdp_avg_speed_short": None,
            "sdp_avg_speed_mid": None,
            "sdp_avg_speed_long": None,
            "sdp_speed_at_current_dist": None,
            "sdp_speed_vs_career_avg": None,
            "sdp_is_speed_type": None,
            "sdp_is_stamina_type": None,
            "sdp_speed_profile_hash": None,
        }

        if self.overall_count == 0:
            feats["sdp_speed_profile_hash"] = 3  # unknown
            return feats

        # Average speed per bucket
        short_data = self.speed_by_bucket.get("short")
        mid_data = self.speed_by_bucket.get("mid")
        long_data = self.speed_by_bucket.get("long")

        short_avg = (short_data[0] / short_data[1]) if short_data and short_data[1] > 0 else None
        mid_avg = (mid_data[0] / mid_data[1]) if mid_data and mid_data[1] > 0 else None
        long_avg = (long_data[0] / long_data[1]) if long_data and long_data[1] > 0 else None

        if short_avg is not None:
            feats["sdp_avg_speed_short"] = round(short_avg, 6)
        if mid_avg is not None:
            feats["sdp_avg_speed_mid"] = round(mid_avg, 6)
        if long_avg is not None:
            feats["sdp_avg_speed_long"] = round(long_avg, 6)

        # Speed at current distance bucket
        if current_bucket is not None:
            bucket_data = self.speed_by_bucket.get(current_bucket)
            if bucket_data and bucket_data[1] > 0:
                feats["sdp_speed_at_current_dist"] = round(bucket_data[0] / bucket_data[1], 6)

        # Speed vs career average
        career_avg = self.overall_speed_sum / self.overall_count
        if current_bucket is not None and career_avg > 0:
            bucket_data = self.speed_by_bucket.get(current_bucket)
            if bucket_data and bucket_data[1] > 0:
                bucket_avg = bucket_data[0] / bucket_data[1]
                feats["sdp_speed_vs_career_avg"] = round(bucket_avg / career_avg, 4)

        # Speed type: lower reduction_km_ms = faster
        # speed type: faster at short (short_avg < mid_avg)
        if short_avg is not None and mid_avg is not None:
            feats["sdp_is_speed_type"] = 1 if short_avg < mid_avg else 0

        # Stamina type: faster at long (long_avg < short_avg)
        if long_avg is not None and short_avg is not None:
            feats["sdp_is_stamina_type"] = 1 if long_avg < short_avg else 0

        # Profile hash: 0=speed, 1=balanced, 2=stamina, 3=unknown
        feats["sdp_speed_profile_hash"] = _classify_profile(short_avg, mid_avg, long_avg)

        return feats

    # -----------------------------------------------------------------
    # Update AFTER snapshot
    # -----------------------------------------------------------------
    def update(self, bucket: Optional[str], speed: Optional[float]) -> None:
        """Update state AFTER snapshot has been taken."""
        if bucket is None or speed is None:
            return
        if bucket not in self.speed_by_bucket:
            self.speed_by_bucket[bucket] = [0.0, 0]
        self.speed_by_bucket[bucket][0] += speed
        self.speed_by_bucket[bucket][1] += 1
        self.overall_speed_sum += speed
        self.overall_count += 1


def _classify_profile(
    short_avg: Optional[float],
    mid_avg: Optional[float],
    long_avg: Optional[float],
) -> int:
    """Classify horse speed profile.

    Returns 0=speed, 1=balanced, 2=stamina, 3=unknown.
    Lower reduction_km_ms = faster.
    """
    if short_avg is None and mid_avg is None and long_avg is None:
        return 3  # unknown

    # Need at least two buckets to classify
    avgs = {}
    if short_avg is not None:
        avgs["short"] = short_avg
    if mid_avg is not None:
        avgs["mid"] = mid_avg
    if long_avg is not None:
        avgs["long"] = long_avg

    if len(avgs) < 2:
        return 3  # unknown -- not enough data

    # Speed type: best (lowest) speed is at short distances
    best_bucket = min(avgs, key=avgs.get)
    worst_bucket = max(avgs, key=avgs.get)

    if best_bucket == "short" and worst_bucket in ("mid", "long"):
        return 0  # speed
    if best_bucket == "long" and worst_bucket in ("short", "mid"):
        return 2  # stamina
    return 1  # balanced


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    if v is None:
        return None
    try:
        val = float(v)
        return val if val == val else None  # NaN check
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _distance_bucket(dist_metres: Optional[int]) -> Optional[str]:
    """Classify distance into short/mid/long bucket."""
    if dist_metres is None or dist_metres <= 0:
        return None
    if dist_metres < _SHORT_MAX:
        return "short"
    if dist_metres <= _MID_MAX:
        return "mid"
    return "long"


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


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort+seek)
# ===========================================================================


def build_speed_distance_profile(input_path: Path, output_path: Path, logger) -> int:
    """Build speed-distance profile features from partants_master.jsonl.

    Two-phase approach:
      1. Index: read sort keys + byte offsets (lightweight).
      2. Sort chronologically, then seek-read records course by course,
         streaming output to .tmp, then atomic rename.

    Returns the total number of feature records written.
    """
    logger.info("=== Speed Distance Profile Builder (memory-optimised) ===")
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

    # -- Phase 3: Seek-based processing, streaming output --
    t2 = time.time()
    horse_state: dict[str, _SpeedDistState] = defaultdict(_SpeedDistState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    _FEATURE_KEYS = [
        "sdp_avg_speed_short",
        "sdp_avg_speed_mid",
        "sdp_avg_speed_long",
        "sdp_speed_at_current_dist",
        "sdp_speed_vs_career_avg",
        "sdp_is_speed_type",
        "sdp_is_stamina_type",
        "sdp_speed_profile_hash",
    ]
    fill_counts: dict[str, int] = {k: 0 for k in _FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(byte_offset: int) -> dict:
            fin.seek(byte_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            distance = _safe_int(rec.get("distance"))
            if distance is None:
                distance = _safe_int(rec.get("distance_metres"))
            speed = _safe_float(rec.get("reduction_km_ms"))

            return {
                "uid": rec.get("partant_uid"),
                "cheval": rec.get("nom_cheval"),
                "distance": distance,
                "bucket": _distance_bucket(distance),
                "speed": speed,
            }

        i = 0
        while i < total:
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
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot BEFORE update for all partants --
            post_updates: list[tuple[str, Optional[str], Optional[float]]] = []

            for rec in course_group:
                cheval = rec["cheval"]
                if not cheval:
                    # Write empty features
                    features: dict[str, Any] = {"partant_uid": rec["uid"]}
                    for k in _FEATURE_KEYS:
                        features[k] = None
                    fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                # Snapshot
                hs = horse_state[cheval]
                features = hs.snapshot(rec["bucket"])
                features["partant_uid"] = rec["uid"]

                # Track fill rates
                for k in _FEATURE_KEYS:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                # Defer update
                post_updates.append((cheval, rec["bucket"], rec["speed"]))

            # -- Update states AFTER all snapshots --
            for cheval, bucket, speed in post_updates:
                horse_state[cheval].update(bucket, speed)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Speed distance profile build termine: %d features en %.1fs (chevaux: %d)",
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
        description="Construction des features speed distance profile a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/speed_distance_profile/)",
    )
    args = parser.parse_args()

    logger = setup_logging("speed_distance_profile_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "speed_distance_profile.jsonl"
    build_speed_distance_profile(input_path, out_path, logger)


if __name__ == "__main__":
    main()
