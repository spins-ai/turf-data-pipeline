#!/usr/bin/env python3
"""
feature_builders.target_proximity_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Near-miss analysis: how close a horse historically gets to winning/placing.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant target-proximity features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the features -- no future leakage.

Produces:
  - target_proximity_features.jsonl   in builder_outputs/target_proximity/

Features per partant (8):
  - tgt_avg_margin_to_win      : average (position - 1) / (nb_partants - 1)
                                  0 = always wins, 1 = always last
  - tgt_near_miss_rate         : proportion of races finishing 2nd or 3rd
  - tgt_win_or_close_rate      : proportion of races finishing 1st, 2nd, or 3rd
  - tgt_avg_beaten_distance    : average (position - 1) -- positions behind winner
  - tgt_improving_margin       : slope of last 5 normalised margins (negative = improving)
  - tgt_top_half_rate          : proportion of races finishing in top half of field
  - tgt_last_race_margin       : normalised margin in the most recent race
  - tgt_best_margin_recent     : best (smallest) normalised margin in last 5 races

Per-horse state:
  - margins deque(maxlen=20)   : normalised margins (position-1)/(nb_partants-1)
  - raw_margins deque(maxlen=20) : raw (position-1) for beaten distance
  - near_miss_count            : finishes in 2nd or 3rd
  - win_or_close_count         : finishes in 1st, 2nd, or 3rd
  - top_half_count             : finishes in top half
  - total_count                : total qualifying races

Memory-optimised version:
  - Phase 1 reads only minimal tuples (sort keys + byte offsets) into memory
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 streams output to disk, course by course via seek
  - gc.collect() called every 500K records

Usage:
    python feature_builders/target_proximity_builder.py
    python feature_builders/target_proximity_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/target_proximity")

# Progress log / gc.collect every N records
_LOG_EVERY = 500_000

# Deque size for recent margins
_MARGIN_HISTORY = 20
# Window for slope / best recent
_RECENT_WINDOW = 5


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
# PER-HORSE STATE TRACKER
# ===========================================================================


class _HorseProximityState:
    """Lightweight per-horse state for target proximity features."""

    __slots__ = (
        "margins", "raw_margins",
        "near_miss_count", "win_or_close_count",
        "top_half_count", "total_count",
    )

    def __init__(self) -> None:
        self.margins: deque = deque(maxlen=_MARGIN_HISTORY)
        self.raw_margins: deque = deque(maxlen=_MARGIN_HISTORY)
        self.near_miss_count: int = 0
        self.win_or_close_count: int = 0
        self.top_half_count: int = 0
        self.total_count: int = 0

    def snapshot(self) -> dict[str, Any]:
        """Return feature dict from current state (pre-race snapshot)."""
        features: dict[str, Any] = {}

        if self.total_count == 0:
            # No history yet -- all None
            features["tgt_avg_margin_to_win"] = None
            features["tgt_near_miss_rate"] = None
            features["tgt_win_or_close_rate"] = None
            features["tgt_avg_beaten_distance"] = None
            features["tgt_improving_margin"] = None
            features["tgt_top_half_rate"] = None
            features["tgt_last_race_margin"] = None
            features["tgt_best_margin_recent"] = None
            return features

        n = self.total_count

        # tgt_avg_margin_to_win: mean of normalised margins
        features["tgt_avg_margin_to_win"] = round(sum(self.margins) / len(self.margins), 4)

        # tgt_near_miss_rate: proportion 2nd or 3rd
        features["tgt_near_miss_rate"] = round(self.near_miss_count / n, 4)

        # tgt_win_or_close_rate: proportion 1st-3rd
        features["tgt_win_or_close_rate"] = round(self.win_or_close_count / n, 4)

        # tgt_avg_beaten_distance: mean of raw margins (position - 1)
        features["tgt_avg_beaten_distance"] = round(
            sum(self.raw_margins) / len(self.raw_margins), 4
        )

        # tgt_top_half_rate
        features["tgt_top_half_rate"] = round(self.top_half_count / n, 4)

        # tgt_last_race_margin: most recent normalised margin
        features["tgt_last_race_margin"] = round(self.margins[-1], 4)

        # tgt_best_margin_recent: smallest margin in last 5
        recent = list(self.margins)[-_RECENT_WINDOW:]
        features["tgt_best_margin_recent"] = round(min(recent), 4)

        # tgt_improving_margin: slope of last 5 normalised margins
        # Using simple linear regression slope: sum((xi - xbar)(yi - ybar)) / sum((xi - xbar)^2)
        recent_margins = list(self.margins)[-_RECENT_WINDOW:]
        if len(recent_margins) >= 3:
            n_pts = len(recent_margins)
            x_mean = (n_pts - 1) / 2.0
            y_mean = sum(recent_margins) / n_pts
            num = 0.0
            den = 0.0
            for idx, val in enumerate(recent_margins):
                dx = idx - x_mean
                num += dx * (val - y_mean)
                den += dx * dx
            if den > 0:
                features["tgt_improving_margin"] = round(num / den, 4)
            else:
                features["tgt_improving_margin"] = None
        else:
            features["tgt_improving_margin"] = None

        return features

    def update(self, position: int, nb_partants: int) -> None:
        """Update state after a race result is observed."""
        # Normalised margin: (position - 1) / (nb_partants - 1)
        if nb_partants > 1:
            norm_margin = (position - 1) / (nb_partants - 1)
        else:
            norm_margin = 0.0

        raw_margin = position - 1

        self.margins.append(norm_margin)
        self.raw_margins.append(raw_margin)
        self.total_count += 1

        if position in (2, 3):
            self.near_miss_count += 1

        if position <= 3:
            self.win_or_close_count += 1

        if nb_partants > 0 and position <= nb_partants / 2:
            self.top_half_count += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_target_proximity_features(input_path: Path, output_path: Path, logger) -> int:
    """Build target proximity features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Target Proximity Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseProximityState] = defaultdict(_HorseProximityState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "tgt_avg_margin_to_win": 0,
        "tgt_near_miss_rate": 0,
        "tgt_win_or_close_rate": 0,
        "tgt_avg_beaten_distance": 0,
        "tgt_improving_margin": 0,
        "tgt_top_half_rate": 0,
        "tgt_last_race_margin": 0,
        "tgt_best_margin_recent": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            nb_partants = rec.get("nombre_partants") or 0
            try:
                nb_partants = int(nb_partants)
            except (ValueError, TypeError):
                nb_partants = 0

            position = rec.get("position") or rec.get("place") or 0
            try:
                position = int(position)
            except (ValueError, TypeError):
                position = 0

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "cheval": rec.get("nom_cheval"),
                "position": position,
                "nb_partants": nb_partants,
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
                _extract_slim(_read_record_at(index[ci][3])) for ci in course_indices
            ]

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]

                features: dict[str, Any] = {
                    "partant_uid": rec["uid"],
                    "course_uid": rec["course"],
                    "date_reunion_iso": rec["date"],
                }

                if cheval:
                    state = horse_state[cheval]
                    snap = state.snapshot()
                    features.update(snap)

                    # Track fill rates
                    for k in fill_counts:
                        if snap.get(k) is not None:
                            fill_counts[k] += 1
                else:
                    # No horse name -- all None
                    for k in fill_counts:
                        features[k] = None

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after race (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                position = rec["position"]
                nb_partants = rec["nb_partants"]

                if not cheval or position <= 0 or nb_partants <= 0:
                    continue

                horse_state[cheval].update(position, nb_partants)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Target proximity build termine: %d features en %.1fs (chevaux uniques: %d)",
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


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features target proximity a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/target_proximity/)",
    )
    args = parser.parse_args()

    logger = setup_logging("target_proximity_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "target_proximity_features.jsonl"
    build_target_proximity_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
