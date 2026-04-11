#!/usr/bin/env python3
"""
feature_builders.distance_aptitude_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep distance aptitude features -- how well suited a horse is to a specific
distance based on its full racing history.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant distance aptitude features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the distance stats -- no future leakage.

Produces:
  - distance_aptitude_deep.jsonl   in builder_outputs/distance_aptitude_deep/

Features per partant (10):
  - dad_horse_exact_dist_wr       : win rate at this exact distance (+/- 50m)
  - dad_horse_exact_dist_runs     : number of runs at this exact distance
  - dad_horse_dist_bucket_wr      : win rate at this distance bucket
  - dad_horse_best_distance       : distance bucket with best win rate (0-3)
  - dad_is_best_distance          : 1 if current distance matches best bucket
  - dad_distance_versatility      : buckets won / buckets tried
  - dad_first_time_distance       : 1 if horse has never run at this exact distance
  - dad_distance_shortening       : 1 shorter, 0 same, -1 longer vs last race
  - dad_horse_speed_at_distance   : avg reduction_km_ms at this distance bucket
  - dad_distance_form_recent      : win rate at this bucket in last 5 races at bucket

Distance buckets:
  0 = sprint   (<1400m)
  1 = mile     (1400-1800m)
  2 = mid      (1800-2400m)
  3 = route    (>2400m)

Memory-optimised version:
  - Phase 1 reads only minimal tuples (sort keys + byte offsets)
  - Phase 2 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records

Usage:
    python feature_builders/distance_aptitude_deep_builder.py
    python feature_builders/distance_aptitude_deep_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/distance_aptitude_deep")

# Progress log every N records
_LOG_EVERY = 500_000

# Distance bucket thresholds
_BUCKET_SPRINT = 0   # < 1400m
_BUCKET_MILE = 1     # 1400-1800m
_BUCKET_MID = 2      # 1800-2400m
_BUCKET_ROUTE = 3    # > 2400m

# Exact distance tolerance
_EXACT_DIST_TOLERANCE = 50  # metres


def _distance_bucket(dist_m: int) -> int:
    """Map a distance in metres to a bucket code (0-3)."""
    if dist_m < 1400:
        return _BUCKET_SPRINT
    elif dist_m <= 1800:
        return _BUCKET_MILE
    elif dist_m <= 2400:
        return _BUCKET_MID
    else:
        return _BUCKET_ROUTE


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseDistState:
    """Per-horse distance aptitude state.

    dist_stats: {exact_dist -> [wins, places, total, [speeds]]}
    bucket_stats: {bucket -> [wins, total, deque(maxlen=5) of recent is_win]}
    last_distance: last race distance in metres
    buckets_won: set of buckets where horse has won
    buckets_tried: set of buckets horse has raced in
    """

    __slots__ = ("dist_stats", "bucket_stats", "last_distance",
                 "buckets_won", "buckets_tried")

    def __init__(self) -> None:
        # exact_dist -> [wins, places, total, list_of_speeds]
        self.dist_stats: dict[int, list] = {}
        # bucket -> [wins, total, deque(maxlen=5)]
        self.bucket_stats: dict[int, list] = {}
        self.last_distance: Optional[int] = None
        self.buckets_won: set[int] = set()
        self.buckets_tried: set[int] = set()

    def get_exact_dist_stats(self, dist_m: int) -> tuple[int, int, int]:
        """Return (wins, places, total) for exact distance (+/- tolerance)."""
        wins = 0
        places = 0
        total = 0
        for d, stats in self.dist_stats.items():
            if abs(d - dist_m) <= _EXACT_DIST_TOLERANCE:
                wins += stats[0]
                places += stats[1]
                total += stats[2]
        return wins, places, total

    def get_exact_dist_speeds(self, dist_m: int) -> list[float]:
        """Return all speeds for exact distance (+/- tolerance)."""
        speeds: list[float] = []
        for d, stats in self.dist_stats.items():
            if abs(d - dist_m) <= _EXACT_DIST_TOLERANCE:
                speeds.extend(stats[3])
        return speeds

    def has_run_exact_distance(self, dist_m: int) -> bool:
        """True if horse has run at this exact distance (+/- tolerance)."""
        for d in self.dist_stats:
            if abs(d - dist_m) <= _EXACT_DIST_TOLERANCE:
                return True
        return False

    def best_bucket(self) -> Optional[int]:
        """Return bucket with best win rate (min 2 races). None if insufficient data."""
        best_b = None
        best_wr = -1.0
        for b, stats in self.bucket_stats.items():
            total = stats[1]
            if total < 2:
                continue
            wr = stats[0] / total
            if wr > best_wr:
                best_wr = wr
                best_b = b
        return best_b

    def update(self, dist_m: int, bucket: int, is_win: bool, is_place: bool,
               speed: Optional[float]) -> None:
        """Update state AFTER snapshot has been taken."""
        # Exact distance stats
        if dist_m not in self.dist_stats:
            self.dist_stats[dist_m] = [0, 0, 0, []]
        entry = self.dist_stats[dist_m]
        if is_win:
            entry[0] += 1
        if is_place:
            entry[1] += 1
        entry[2] += 1
        if speed is not None:
            entry[3].append(speed)

        # Bucket stats
        if bucket not in self.bucket_stats:
            self.bucket_stats[bucket] = [0, 0, deque(maxlen=5)]
        bentry = self.bucket_stats[bucket]
        if is_win:
            bentry[0] += 1
        bentry[1] += 1
        bentry[2].append(is_win)

        # Sets
        if is_win:
            self.buckets_won.add(bucket)
        self.buckets_tried.add(bucket)

        # Last distance
        self.last_distance = dist_m


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_distance_aptitude_deep(input_path: Path, output_path: Path, logger) -> int:
    """Build deep distance aptitude features.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Distance Aptitude Deep Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseDistState] = defaultdict(_HorseDistState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "dad_horse_exact_dist_wr": 0,
        "dad_horse_exact_dist_runs": 0,
        "dad_horse_dist_bucket_wr": 0,
        "dad_horse_best_distance": 0,
        "dad_is_best_distance": 0,
        "dad_distance_versatility": 0,
        "dad_first_time_distance": 0,
        "dad_distance_shortening": 0,
        "dad_horse_speed_at_distance": 0,
        "dad_distance_form_recent": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            distance = rec.get("distance")
            try:
                distance = int(distance) if distance is not None else None
            except (ValueError, TypeError):
                distance = None

            reduction = rec.get("reduction_km_ms")
            try:
                reduction = float(reduction) if reduction is not None else None
            except (ValueError, TypeError):
                reduction = None

            horse_id = rec.get("horse_id") or rec.get("nom_cheval")

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "horse": horse_id,
                "distance": distance,
                "discipline": (rec.get("discipline") or "").strip().upper(),
                "gagnant": bool(rec.get("is_gagnant")),
                "place": bool(rec.get("is_place")),
                "reduction": reduction,
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

            # -- Snapshot pre-race stats (temporal integrity) --
            post_updates: list[tuple[str, int, int, bool, bool, Optional[float]]] = []

            for rec in course_group:
                horse = rec["horse"]
                dist_m = rec["distance"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if not horse or dist_m is None or dist_m <= 0:
                    # Cannot compute features without horse or distance
                    for k in fill_counts:
                        features[k] = None
                    fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                    n_written += 1
                    # Still need to update post-race if possible
                    if horse and dist_m and dist_m > 0:
                        bucket = _distance_bucket(dist_m)
                        reduction = rec["reduction"]
                        post_updates.append((horse, dist_m, bucket, rec["gagnant"], rec["place"], reduction))
                    continue

                bucket = _distance_bucket(dist_m)
                hs = horse_state[horse]

                # 1. dad_horse_exact_dist_wr
                exact_wins, exact_places, exact_total = hs.get_exact_dist_stats(dist_m)
                if exact_total > 0:
                    features["dad_horse_exact_dist_wr"] = round(exact_wins / exact_total, 4)
                    fill_counts["dad_horse_exact_dist_wr"] += 1
                else:
                    features["dad_horse_exact_dist_wr"] = None

                # 2. dad_horse_exact_dist_runs
                features["dad_horse_exact_dist_runs"] = exact_total
                if exact_total > 0:
                    fill_counts["dad_horse_exact_dist_runs"] += 1

                # 3. dad_horse_dist_bucket_wr
                bstats = hs.bucket_stats.get(bucket)
                if bstats and bstats[1] > 0:
                    features["dad_horse_dist_bucket_wr"] = round(bstats[0] / bstats[1], 4)
                    fill_counts["dad_horse_dist_bucket_wr"] += 1
                else:
                    features["dad_horse_dist_bucket_wr"] = None

                # 4. dad_horse_best_distance
                best_b = hs.best_bucket()
                if best_b is not None:
                    features["dad_horse_best_distance"] = best_b
                    fill_counts["dad_horse_best_distance"] += 1
                else:
                    features["dad_horse_best_distance"] = None

                # 5. dad_is_best_distance
                if best_b is not None:
                    features["dad_is_best_distance"] = 1 if bucket == best_b else 0
                    fill_counts["dad_is_best_distance"] += 1
                else:
                    features["dad_is_best_distance"] = None

                # 6. dad_distance_versatility
                n_tried = len(hs.buckets_tried)
                if n_tried > 0:
                    n_won = len(hs.buckets_won)
                    features["dad_distance_versatility"] = round(n_won / n_tried, 4)
                    fill_counts["dad_distance_versatility"] += 1
                else:
                    features["dad_distance_versatility"] = None

                # 7. dad_first_time_distance
                if hs.dist_stats:
                    # Horse has run before -- check if at this distance
                    features["dad_first_time_distance"] = 0 if hs.has_run_exact_distance(dist_m) else 1
                    fill_counts["dad_first_time_distance"] += 1
                else:
                    # Horse has never run -- first time everything
                    features["dad_first_time_distance"] = None

                # 8. dad_distance_shortening
                if hs.last_distance is not None:
                    if dist_m < hs.last_distance:
                        features["dad_distance_shortening"] = 1
                    elif dist_m == hs.last_distance:
                        features["dad_distance_shortening"] = 0
                    else:
                        features["dad_distance_shortening"] = -1
                    fill_counts["dad_distance_shortening"] += 1
                else:
                    features["dad_distance_shortening"] = None

                # 9. dad_horse_speed_at_distance
                if bstats and bstats[1] > 0:
                    # Collect speeds from all exact distances in this bucket
                    bucket_speeds: list[float] = []
                    for d, dstats in hs.dist_stats.items():
                        if _distance_bucket(d) == bucket and dstats[3]:
                            bucket_speeds.extend(dstats[3])
                    if bucket_speeds:
                        features["dad_horse_speed_at_distance"] = round(
                            sum(bucket_speeds) / len(bucket_speeds), 2
                        )
                        fill_counts["dad_horse_speed_at_distance"] += 1
                    else:
                        features["dad_horse_speed_at_distance"] = None
                else:
                    features["dad_horse_speed_at_distance"] = None

                # 10. dad_distance_form_recent
                if bstats and len(bstats[2]) > 0:
                    recent_deque = bstats[2]
                    features["dad_distance_form_recent"] = round(
                        sum(recent_deque) / len(recent_deque), 4
                    )
                    fill_counts["dad_distance_form_recent"] += 1
                else:
                    features["dad_distance_form_recent"] = None

                # Stream to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update
                reduction = rec["reduction"]
                post_updates.append((horse, dist_m, bucket, rec["gagnant"], rec["place"], reduction))

            # -- Update states after race (no leakage) --
            for horse, dist_m, bucket, is_win, is_place, speed in post_updates:
                horse_state[horse].update(dist_m, bucket, is_win, is_place, speed)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Distance aptitude deep build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written if n_written else 0)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features distance aptitude deep a partir de partants_master"
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

    logger = setup_logging("distance_aptitude_deep_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "distance_aptitude_deep.jsonl"
    build_distance_aptitude_deep(input_path, out_path, logger)


if __name__ == "__main__":
    main()
