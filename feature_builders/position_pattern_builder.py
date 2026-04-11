#!/usr/bin/env python3
"""
feature_builders.position_pattern_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Position pattern features -- analyzing finishing position distributions
and patterns for each horse.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the position pattern stats -- no future leakage.  Features
are snapshotted BEFORE the current race updates the state.

Produces:
  - position_pattern.jsonl   in builder_outputs/position_pattern/

Features per partant (10):
  - ppt_modal_position          : most frequent finishing position for this horse
  - ppt_position_entropy        : entropy of position distribution (higher = more unpredictable)
  - ppt_top3_frequency          : fraction of races finishing top 3
  - ppt_midfield_frequency      : fraction finishing 4th to (partants/2)
  - ppt_tail_frequency          : fraction finishing in bottom quartile
  - ppt_position_skew           : skewness of position distribution (positive = many early finishes)
  - ppt_win_after_top3_rate     : probability of winning given placed (top 3) in previous race
  - ppt_position_autocorrelation: correlation between consecutive positions (lag-1 autocorrelation)
  - ppt_best_worst_spread       : worst position - best position (range of ability)
  - ppt_recent_top3_ratio       : fraction of top-3 finishes in last 5 races vs career average

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 streams output to disk via seek-based reads
  - gc.collect() called every 500K records
  - .tmp then atomic rename

Usage:
    python feature_builders/position_pattern_builder.py
    python feature_builders/position_pattern_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/position_pattern")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        v = int(val)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


# ===========================================================================
# PER-HORSE STATE TRACKER
# ===========================================================================


class _HorsePositionState:
    """Compact per-horse position history tracker.

    Maintains:
      - positions       : deque(maxlen=20)  last 20 finishing positions
      - position_counts : dict[int, int]    frequency of each position (all time)
      - top3_count      : int               number of top-3 finishes
      - total_races     : int               total number of races with valid position
      - wins_after_place: int               wins where previous race was top-3
      - placed_count    : int               races where prev race was top-3 (denominator)
      - best_pos        : int               best (lowest) finishing position
      - worst_pos       : int               worst (highest) finishing position
      - recent_results  : deque(maxlen=5)   last 5 positions for recent ratio
    """

    __slots__ = (
        "positions", "position_counts", "top3_count", "total_races",
        "wins_after_place", "placed_count", "best_pos", "worst_pos",
        "recent_results",
    )

    def __init__(self) -> None:
        self.positions: deque = deque(maxlen=20)
        self.position_counts: dict[int, int] = defaultdict(int)
        self.top3_count: int = 0
        self.total_races: int = 0
        self.wins_after_place: int = 0
        self.placed_count: int = 0
        self.best_pos: int = 999
        self.worst_pos: int = 0
        self.recent_results: deque = deque(maxlen=5)

    def snapshot(self, nombre_partants: Optional[int]) -> dict[str, Any]:
        """Return the 10 features from current state (BEFORE update)."""
        feats: dict[str, Any] = {}

        if self.total_races == 0:
            feats["ppt_modal_position"] = None
            feats["ppt_position_entropy"] = None
            feats["ppt_top3_frequency"] = None
            feats["ppt_midfield_frequency"] = None
            feats["ppt_tail_frequency"] = None
            feats["ppt_position_skew"] = None
            feats["ppt_win_after_top3_rate"] = None
            feats["ppt_position_autocorrelation"] = None
            feats["ppt_best_worst_spread"] = None
            feats["ppt_recent_top3_ratio"] = None
            return feats

        total = self.total_races

        # 1. ppt_modal_position: most frequent finishing position
        modal_pos = max(self.position_counts, key=self.position_counts.get)
        feats["ppt_modal_position"] = modal_pos

        # 2. ppt_position_entropy: Shannon entropy of position distribution
        entropy = 0.0
        for count in self.position_counts.values():
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)
        feats["ppt_position_entropy"] = round(entropy, 4)

        # 3. ppt_top3_frequency
        feats["ppt_top3_frequency"] = round(self.top3_count / total, 4)

        # 4. ppt_midfield_frequency: fraction finishing 4th to (partants/2)
        #    Use the current race's nombre_partants as reference for midfield boundary
        if nombre_partants and nombre_partants > 6:
            midfield_upper = nombre_partants // 2
        else:
            midfield_upper = 8  # reasonable default
        midfield_count = sum(
            c for pos, c in self.position_counts.items()
            if 4 <= pos <= midfield_upper
        )
        feats["ppt_midfield_frequency"] = round(midfield_count / total, 4)

        # 5. ppt_tail_frequency: fraction finishing in bottom quartile
        #    Bottom quartile = position > 0.75 * nombre_partants (approx)
        if nombre_partants and nombre_partants >= 4:
            tail_threshold = max(int(nombre_partants * 0.75), 4)
        else:
            tail_threshold = 10  # reasonable default
        tail_count = sum(
            c for pos, c in self.position_counts.items()
            if pos > tail_threshold
        )
        feats["ppt_tail_frequency"] = round(tail_count / total, 4)

        # 6. ppt_position_skew: skewness of positions in the deque
        positions_list = list(self.positions)
        if len(positions_list) >= 3:
            n = len(positions_list)
            mean = sum(positions_list) / n
            m2 = sum((x - mean) ** 2 for x in positions_list) / n
            m3 = sum((x - mean) ** 3 for x in positions_list) / n
            if m2 > 0:
                feats["ppt_position_skew"] = round(m3 / (m2 ** 1.5), 4)
            else:
                feats["ppt_position_skew"] = 0.0
        else:
            feats["ppt_position_skew"] = None

        # 7. ppt_win_after_top3_rate
        if self.placed_count > 0:
            feats["ppt_win_after_top3_rate"] = round(
                self.wins_after_place / self.placed_count, 4
            )
        else:
            feats["ppt_win_after_top3_rate"] = None

        # 8. ppt_position_autocorrelation: lag-1 autocorrelation
        if len(positions_list) >= 3:
            n = len(positions_list)
            mean = sum(positions_list) / n
            var = sum((x - mean) ** 2 for x in positions_list) / n
            if var > 0:
                cov = sum(
                    (positions_list[j] - mean) * (positions_list[j + 1] - mean)
                    for j in range(n - 1)
                ) / (n - 1)
                feats["ppt_position_autocorrelation"] = round(cov / var, 4)
            else:
                feats["ppt_position_autocorrelation"] = 0.0
        else:
            feats["ppt_position_autocorrelation"] = None

        # 9. ppt_best_worst_spread
        if self.best_pos < 999 and self.worst_pos > 0:
            feats["ppt_best_worst_spread"] = self.worst_pos - self.best_pos
        else:
            feats["ppt_best_worst_spread"] = None

        # 10. ppt_recent_top3_ratio: ratio of recent top-3 rate to career top-3 rate
        recent = list(self.recent_results)
        if len(recent) >= 2 and self.top3_count > 0:
            recent_top3 = sum(1 for p in recent if p <= 3) / len(recent)
            career_top3 = self.top3_count / total
            if career_top3 > 0:
                feats["ppt_recent_top3_ratio"] = round(recent_top3 / career_top3, 4)
            else:
                feats["ppt_recent_top3_ratio"] = None
        else:
            feats["ppt_recent_top3_ratio"] = None

        return feats

    def update(self, position: int, is_gagnant: bool) -> None:
        """Update state AFTER snapshotting features."""
        # Check if previous race was a top-3 finish (for win_after_top3_rate)
        if self.positions:
            prev_pos = self.positions[-1]  # most recent before this update
            if prev_pos <= 3:
                self.placed_count += 1
                if is_gagnant:
                    self.wins_after_place += 1

        self.positions.append(position)
        self.position_counts[position] += 1
        self.total_races += 1

        if position <= 3:
            self.top3_count += 1

        if position < self.best_pos:
            self.best_pos = position
        if position > self.worst_pos:
            self.worst_pos = position

        self.recent_results.append(position)


# ===========================================================================
# MAIN BUILD (index+sort+seek, streaming output)
# ===========================================================================


def build_position_pattern_features(input_path: Path, output_path: Path, logger) -> int:
    """Build position pattern features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Position Pattern Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorsePositionState] = defaultdict(_HorsePositionState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "ppt_modal_position",
        "ppt_position_entropy",
        "ppt_top3_frequency",
        "ppt_midfield_frequency",
        "ppt_tail_frequency",
        "ppt_position_skew",
        "ppt_win_after_top3_rate",
        "ppt_position_autocorrelation",
        "ppt_best_worst_spread",
        "ppt_recent_top3_ratio",
    ]
    fill_counts = {k: 0 for k in feature_names}

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

            # Read only this course's records from disk
            course_records = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                position = _safe_int(rec.get("position_arrivee"))
                is_gagnant = bool(rec.get("is_gagnant"))
                is_place = bool(rec.get("is_place"))
                nb_partants = _safe_int(rec.get("nombre_partants"))
                partant_uid = rec.get("partant_uid")

                course_records.append({
                    "uid": partant_uid,
                    "horse_id": horse_id,
                    "position": position,
                    "is_gagnant": is_gagnant,
                    "is_place": is_place,
                    "nb_partants": nb_partants,
                })

            # -- Snapshot BEFORE update for all partants in this course --
            post_updates: list[tuple[str, int, bool]] = []

            for rec in course_records:
                horse_id = rec["horse_id"]
                position = rec["position"]
                nb_partants = rec["nb_partants"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if horse_id:
                    state = horse_state[horse_id]
                    feats = state.snapshot(nb_partants)
                    features.update(feats)
                else:
                    for fn in feature_names:
                        features[fn] = None

                # Count fill rates
                for fn in feature_names:
                    if features.get(fn) is not None:
                        fill_counts[fn] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Queue state update for after snapshot
                if horse_id and position is not None:
                    post_updates.append((horse_id, position, rec["is_gagnant"]))

            # -- Update states AFTER snapshotting (post-race, no leakage) --
            for horse_id, position, is_gagnant in post_updates:
                horse_state[horse_id].update(position, is_gagnant)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Position pattern build termine: %d features en %.1fs (chevaux: %d)",
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
        description="Construction des features position pattern a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/position_pattern/)",
    )
    args = parser.parse_args()

    logger = setup_logging("position_pattern_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "position_pattern.jsonl"
    build_position_pattern_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
