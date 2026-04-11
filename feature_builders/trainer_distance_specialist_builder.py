#!/usr/bin/env python3
"""
feature_builders.trainer_distance_specialist_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer distance specialization features (advanced).

Reads partants_master.jsonl in streaming mode, builds a lightweight
byte-offset index, sorts chronologically, then seeks back to produce
features -- keeping RAM usage proportional to the index, not the data.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - trainer_distance_specialist.jsonl  in output/trainer_distance_specialist/

Features per partant (8):
  - tds_trainer_dist_bucket_wr     : trainer's win rate at this distance bucket
  - tds_trainer_best_distance      : distance bucket where trainer has best win rate
  - tds_is_trainer_best_distance   : 1 if current distance matches trainer's best bucket
  - tds_trainer_distance_specialist: 1 if best distance wr > 1.5x worst distance wr (min 10 each)
  - tds_trainer_exact_dist_wr      : trainer's win rate at this exact distance (+/-100m)
  - tds_trainer_dist_experience    : number of runners at this distance bucket
  - tds_trainer_dist_advantage     : dist_bucket_wr - overall_wr (positive = better at this dist)
  - tds_trainer_dist_x_discipline  : trainer's wr at this distance + discipline combo

Usage:
    python feature_builders/trainer_distance_specialist_builder.py
    python feature_builders/trainer_distance_specialist_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_distance_specialist")

_LOG_EVERY = 500_000

# Distance buckets
_BUCKET_THRESHOLDS = {
    "sprint": (0, 1300),
    "mile": (1300, 1900),
    "mid": (1900, 2500),
    "long": (2500, 99999),
}


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _distance_bucket(distance: float) -> Optional[str]:
    """Classify distance into sprint/mile/mid/long."""
    for bucket, (lo, hi) in _BUCKET_THRESHOLDS.items():
        if lo <= distance < hi:
            return bucket
    return None


def _round_distance(distance: float) -> int:
    """Round distance to nearest 100m for exact distance matching."""
    return round(distance / 100) * 100


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _TrainerDistSpecState:
    """Per-trainer accumulated state for distance specialization.

    State:
      dist_bucket_stats  : {bucket -> [wins, total]}
      exact_dist_stats   : {dist_rounded -> [wins, total]}
      dist_disc_stats    : {(bucket, discipline) -> [wins, total]}
      overall            : [wins, total]
    """

    __slots__ = ("dist_bucket_stats", "exact_dist_stats", "dist_disc_stats", "overall")

    def __init__(self) -> None:
        self.dist_bucket_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.exact_dist_stats: dict[int, list[int]] = defaultdict(lambda: [0, 0])
        self.dist_disc_stats: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.overall: list[int] = [0, 0]  # [wins, total]

    def snapshot(self, bucket: Optional[str], dist_rounded: Optional[int],
                 discipline: Optional[str]) -> dict[str, Any]:
        """Compute features using only past data (strict temporal)."""
        feats: dict[str, Any] = {
            "tds_trainer_dist_bucket_wr": None,
            "tds_trainer_best_distance": None,
            "tds_is_trainer_best_distance": None,
            "tds_trainer_distance_specialist": None,
            "tds_trainer_exact_dist_wr": None,
            "tds_trainer_dist_experience": None,
            "tds_trainer_dist_advantage": None,
            "tds_trainer_dist_x_discipline": None,
        }

        # --- dist bucket win rate ---
        if bucket is not None:
            bstats = self.dist_bucket_stats.get(bucket)
            if bstats and bstats[1] > 0:
                feats["tds_trainer_dist_bucket_wr"] = round(bstats[0] / bstats[1], 4)
            feats["tds_trainer_dist_experience"] = bstats[1] if bstats else 0

        # --- best distance bucket ---
        best_bucket = None
        best_wr = -1.0
        for b, (w, t) in self.dist_bucket_stats.items():
            if t >= 3:
                wr = w / t
                if wr > best_wr:
                    best_wr = wr
                    best_bucket = b
        feats["tds_trainer_best_distance"] = best_bucket

        # --- is best distance ---
        if best_bucket is not None and bucket is not None:
            feats["tds_is_trainer_best_distance"] = 1 if bucket == best_bucket else 0

        # --- distance specialist flag ---
        # 1 if best distance wr > 1.5x worst distance wr (min 10 each)
        wrs_with_min = []
        for b, (w, t) in self.dist_bucket_stats.items():
            if t >= 10:
                wrs_with_min.append(w / t)
        if len(wrs_with_min) >= 2:
            best_val = max(wrs_with_min)
            worst_val = min(wrs_with_min)
            if worst_val > 0 and best_val > 1.5 * worst_val:
                feats["tds_trainer_distance_specialist"] = 1
            else:
                feats["tds_trainer_distance_specialist"] = 0

        # --- exact distance win rate (+/-100m) ---
        if dist_rounded is not None:
            estats = self.exact_dist_stats.get(dist_rounded)
            if estats and estats[1] > 0:
                feats["tds_trainer_exact_dist_wr"] = round(estats[0] / estats[1], 4)

        # --- dist advantage: bucket_wr - overall_wr ---
        if bucket is not None:
            bstats = self.dist_bucket_stats.get(bucket)
            overall_wr = self.overall[0] / self.overall[1] if self.overall[1] > 0 else None
            bucket_wr = bstats[0] / bstats[1] if (bstats and bstats[1] > 0) else None
            if bucket_wr is not None and overall_wr is not None:
                feats["tds_trainer_dist_advantage"] = round(bucket_wr - overall_wr, 4)

        # --- dist x discipline combo ---
        if bucket is not None and discipline:
            key = (bucket, discipline)
            dstats = self.dist_disc_stats.get(key)
            if dstats and dstats[1] > 0:
                feats["tds_trainer_dist_x_discipline"] = round(dstats[0] / dstats[1], 4)

        return feats

    def update(self, bucket: Optional[str], dist_rounded: Optional[int],
               discipline: Optional[str], is_winner: bool) -> None:
        """Update state with a new race result (post-race)."""
        self.overall[1] += 1
        if is_winner:
            self.overall[0] += 1

        if bucket is not None:
            self.dist_bucket_stats[bucket][1] += 1
            if is_winner:
                self.dist_bucket_stats[bucket][0] += 1

        if dist_rounded is not None:
            self.exact_dist_stats[dist_rounded][1] += 1
            if is_winner:
                self.exact_dist_stats[dist_rounded][0] += 1

        if bucket is not None and discipline:
            key = (bucket, discipline)
            self.dist_disc_stats[key][1] += 1
            if is_winner:
                self.dist_disc_stats[key][0] += 1


# ===========================================================================
# MAIN BUILD (index + sort + seek-based)
# ===========================================================================


def build_trainer_distance_specialist_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build trainer distance specialist features.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Seek back to disk to read full records, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Trainer Distance Specialist Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date, course, num, offset) --
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
    trainer_states: dict[str, _TrainerDistSpecState] = defaultdict(_TrainerDistSpecState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "tds_trainer_dist_bucket_wr": 0,
        "tds_trainer_best_distance": 0,
        "tds_is_trainer_best_distance": 0,
        "tds_trainer_distance_specialist": 0,
        "tds_trainer_exact_dist_wr": 0,
        "tds_trainer_dist_experience": 0,
        "tds_trainer_dist_advantage": 0,
        "tds_trainer_dist_x_discipline": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            distance = _safe_float(rec.get("distance"))
            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            bucket = None
            dist_rounded = None
            if distance is not None and distance > 0:
                bucket = _distance_bucket(distance)
                dist_rounded = _round_distance(distance)

            return {
                "uid": rec.get("partant_uid"),
                "entraineur": (rec.get("entraineur") or "").strip(),
                "distance": distance,
                "bucket": bucket,
                "dist_rounded": dist_rounded,
                "discipline": discipline,
                "is_gagnant": bool(rec.get("is_gagnant")),
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

            # -- Snapshot pre-race features (BEFORE update) --
            for rec in course_group:
                entraineur = rec["entraineur"]
                bucket = rec["bucket"]
                dist_rounded = rec["dist_rounded"]
                discipline = rec["discipline"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if entraineur:
                    state = trainer_states[entraineur]
                    snap = state.snapshot(bucket, dist_rounded, discipline)
                    features.update(snap)

                    # Update fill counts
                    for k in fill_counts:
                        if snap.get(k) is not None:
                            fill_counts[k] += 1
                else:
                    features.update({
                        "tds_trainer_dist_bucket_wr": None,
                        "tds_trainer_best_distance": None,
                        "tds_is_trainer_best_distance": None,
                        "tds_trainer_distance_specialist": None,
                        "tds_trainer_exact_dist_wr": None,
                        "tds_trainer_dist_experience": None,
                        "tds_trainer_dist_advantage": None,
                        "tds_trainer_dist_x_discipline": None,
                    })

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after race (post-race, no leakage) --
            for rec in course_group:
                entraineur = rec["entraineur"]
                if entraineur:
                    trainer_states[entraineur].update(
                        rec["bucket"], rec["dist_rounded"],
                        rec["discipline"], rec["is_gagnant"],
                    )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Trainer distance specialist build termine: %d features en %.1fs (entraineurs: %d)",
        n_written, elapsed, len(trainer_states),
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
        description="Construction des features trainer distance specialist a partir de partants_master"
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

    logger = setup_logging("trainer_distance_specialist_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "trainer_distance_specialist.jsonl"
    build_trainer_distance_specialist_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
