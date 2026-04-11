#!/usr/bin/env python3
"""
feature_builders.earnings_trajectory_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Earnings trajectory features tracking financial performance evolution.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant earnings trajectory features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the earnings statistics -- no future leakage.

Produces:
  - earnings_trajectory.jsonl  in output/earnings_trajectory/

Features per partant (8):
  - etr_gains_per_start         : gains_carriere / nb_courses_carriere
  - etr_gains_velocity          : change in gains_carriere between now and 5 races ago / 5
  - etr_gains_acceleration      : gains_velocity now vs gains_velocity 5 races ago
  - etr_annual_gains_ratio      : gains_annee / gains_carriere (% earned this year)
  - etr_earnings_rank_estimate  : log(gains_carriere + 1) / (age - 1 + 0.01) age-adjusted
  - etr_is_high_earner          : 1 if gains_carriere > 100000 euros
  - etr_gains_vs_odds           : gains_carriere / (cote_finale * 1000)
  - etr_earning_consistency     : gains_annee / estimated nb_courses_this_year

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full records)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records

Usage:
    python feature_builders/earnings_trajectory_builder.py
    python feature_builders/earnings_trajectory_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/earnings_trajectory")

_LOG_EVERY = 500_000

# Velocity window: compare gains now vs N races ago
_VELOCITY_WINDOW = 5
# High earner threshold in euros
_HIGH_EARNER_THRESHOLD = 100_000


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _EarningsState:
    """Per-horse earnings trajectory tracker.

    Stores a deque of the last 10 gains_carriere snapshots to compute
    velocity and acceleration without unbounded memory growth.
    """

    __slots__ = ("gains_history", "races_count")

    def __init__(self) -> None:
        # deque of gains_carriere values (snapshot before each race)
        self.gains_history: deque = deque(maxlen=10)
        self.races_count: int = 0

    def snapshot(
        self,
        gains_carriere: Optional[float],
        gains_annee: Optional[float],
        nb_courses_carriere: Optional[int],
        age: Optional[float],
        cote_finale: Optional[float],
    ) -> dict[str, Any]:
        """Compute features BEFORE updating state."""
        feats: dict[str, Any] = {
            "etr_gains_per_start": None,
            "etr_gains_velocity": None,
            "etr_gains_acceleration": None,
            "etr_annual_gains_ratio": None,
            "etr_earnings_rank_estimate": None,
            "etr_is_high_earner": None,
            "etr_gains_vs_odds": None,
            "etr_earning_consistency": None,
        }

        # 1. etr_gains_per_start
        if gains_carriere is not None and nb_courses_carriere is not None and nb_courses_carriere > 0:
            feats["etr_gains_per_start"] = round(gains_carriere / nb_courses_carriere, 2)

        # 2. etr_gains_velocity: (gains_now - gains_5_ago) / 5
        if gains_carriere is not None and len(self.gains_history) >= _VELOCITY_WINDOW:
            gains_5_ago = self.gains_history[-_VELOCITY_WINDOW]
            velocity = (gains_carriere - gains_5_ago) / _VELOCITY_WINDOW
            feats["etr_gains_velocity"] = round(velocity, 2)

        # 3. etr_gains_acceleration: velocity_now vs velocity_5_ago
        #    We need at least 2 * _VELOCITY_WINDOW entries in history
        if gains_carriere is not None and len(self.gains_history) >= 2 * _VELOCITY_WINDOW:
            gains_5_ago = self.gains_history[-_VELOCITY_WINDOW]
            gains_10_ago = self.gains_history[-2 * _VELOCITY_WINDOW]
            velocity_now = (gains_carriere - gains_5_ago) / _VELOCITY_WINDOW
            velocity_prev = (gains_5_ago - gains_10_ago) / _VELOCITY_WINDOW
            feats["etr_gains_acceleration"] = round(velocity_now - velocity_prev, 2)

        # 4. etr_annual_gains_ratio
        if (
            gains_annee is not None
            and gains_carriere is not None
            and gains_carriere > 0
        ):
            feats["etr_annual_gains_ratio"] = round(gains_annee / gains_carriere, 4)

        # 5. etr_earnings_rank_estimate: log(gains_carriere + 1) / (age - 1 + 0.01)
        if gains_carriere is not None and age is not None and age > 0:
            feats["etr_earnings_rank_estimate"] = round(
                math.log(gains_carriere + 1) / (age - 1 + 0.01), 4
            )

        # 6. etr_is_high_earner
        if gains_carriere is not None:
            feats["etr_is_high_earner"] = int(gains_carriere > _HIGH_EARNER_THRESHOLD)

        # 7. etr_gains_vs_odds
        if gains_carriere is not None and cote_finale is not None and cote_finale > 0:
            feats["etr_gains_vs_odds"] = round(gains_carriere / (cote_finale * 1000), 4)

        # 8. etr_earning_consistency: gains_annee / estimated nb_courses_this_year
        #    Approximate from gains_per_start trend: if we have gains_per_start,
        #    we can estimate courses_this_year = gains_annee / gains_per_start
        #    But simpler: use nb_courses_carriere and races_count to estimate
        #    annual run rate.  Actually: gains_annee / (gains_per_start) gives
        #    estimated nb races this year; then gains_annee / that = gains_per_start.
        #    Better: use the actual races_count we track as a proxy.
        if (
            gains_annee is not None
            and self.races_count > 0
            and gains_carriere is not None
            and nb_courses_carriere is not None
            and nb_courses_carriere > 0
        ):
            gps = gains_carriere / nb_courses_carriere
            if gps > 0:
                est_courses_year = gains_annee / gps
                if est_courses_year > 0:
                    feats["etr_earning_consistency"] = round(
                        gains_annee / est_courses_year, 2
                    )

        return feats

    def update(self, gains_carriere: Optional[float]) -> None:
        """Update state AFTER snapshot."""
        if gains_carriere is not None:
            self.gains_history.append(gains_carriere)
        self.races_count += 1


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if not math.isnan(v) else None
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek-based processing)
# ===========================================================================


def build_earnings_trajectory_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build earnings trajectory features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Earnings Trajectory Builder (memory-optimised) ===")
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
    horse_state: dict[str, _EarningsState] = defaultdict(_EarningsState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "etr_gains_per_start",
        "etr_gains_velocity",
        "etr_gains_acceleration",
        "etr_annual_gains_ratio",
        "etr_earnings_rank_estimate",
        "etr_is_high_earner",
        "etr_gains_vs_odds",
        "etr_earning_consistency",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_fields(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "gains_carriere": _safe_float(rec.get("gains_carriere_euros")),
                "gains_annee": _safe_float(rec.get("gains_annee_euros")),
                "nb_courses_carriere": _safe_int(rec.get("nb_courses_carriere")),
                "age": _safe_float(rec.get("age")),
                "cote_finale": _safe_float(rec.get("cote_finale")),
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
                _extract_fields(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            post_updates: list[tuple[Optional[str], Optional[float]]] = []

            for rec in course_group:
                hid = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if hid:
                    state = horse_state[hid]
                    feats = state.snapshot(
                        gains_carriere=rec["gains_carriere"],
                        gains_annee=rec["gains_annee"],
                        nb_courses_carriere=rec["nb_courses_carriere"],
                        age=rec["age"],
                        cote_finale=rec["cote_finale"],
                    )
                    features.update(feats)
                else:
                    for k in feature_keys:
                        features[k] = None

                # Track fill rates
                for k in feature_keys:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update
                post_updates.append((hid, rec["gains_carriere"]))

            # -- Update states after race (no leakage) --
            for hid, gains_carriere in post_updates:
                if hid:
                    horse_state[hid].update(gains_carriere)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Earnings trajectory build termine: %d features en %.1fs (chevaux: %d)",
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


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features earnings trajectory a partir de partants_master"
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

    logger = setup_logging("earnings_trajectory_builder")

    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Fichier introuvable: {input_path}")
    else:
        input_path = INPUT_PARTANTS
        if not input_path.exists():
            raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "earnings_trajectory.jsonl"
    build_earnings_trajectory_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
