#!/usr/bin/env python3
"""
feature_builders.connection_strength_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Connection/network strength features -- how strong the jockey-trainer-owner-horse
connections are.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - connection_strength.jsonl   in builder_outputs/connection_strength/

Features per partant (8):
  - con_jt_combo_wr           : jockey+trainer combination win rate
  - con_jt_combo_runs         : number of runs for this jockey+trainer pair
  - con_jo_combo_wr           : jockey+owner combination win rate
  - con_to_combo_wr           : trainer+owner combination win rate
  - con_jto_trio_runs         : number of runs for this jockey+trainer+owner trio
  - con_jto_trio_wr           : win rate for the complete trio
  - con_connection_score      : weighted composite of all pair/trio win rates
                                (0.3*jt + 0.2*jo + 0.2*to + 0.3*jto)
  - con_is_new_connection     : 1 if any of the pairs (jt, jo, to) has <3 runs together

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full records)
  - Phase 2 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records

Usage:
    python feature_builders/connection_strength_builder.py
    python feature_builders/connection_strength_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/connection_strength")

_LOG_EVERY = 500_000

# Minimum runs to consider a pair "established" (below = new connection)
_NEW_CONNECTION_THRESHOLD = 3


# ===========================================================================
# HELPERS
# ===========================================================================


def _norm(name: Optional[str]) -> Optional[str]:
    """Normalise a jockey / trainer / owner name for comparison."""
    if not name or not isinstance(name, str):
        return None
    v = name.strip().upper()
    return v if v else None


# ===========================================================================
# COMBO STATE
# ===========================================================================


class _ComboTracker:
    """Tracks win-rate statistics for connection pairs and trios.

    State dicts:
      - jt_stats: {(jockey, trainer) -> [wins, total]}
      - jo_stats: {(jockey, owner)   -> [wins, total]}
      - to_stats: {(trainer, owner)  -> [wins, total]}
      - jto_stats: {(jockey, trainer, owner) -> [wins, total]}
    """

    __slots__ = ("jt_stats", "jo_stats", "to_stats", "jto_stats")

    def __init__(self) -> None:
        self.jt_stats: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.jo_stats: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.to_stats: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.jto_stats: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])

    def snapshot(
        self,
        jockey: Optional[str],
        trainer: Optional[str],
        owner: Optional[str],
    ) -> dict[str, Any]:
        """Compute all 8 features from PAST data only, BEFORE update."""

        feats: dict[str, Any] = {
            "con_jt_combo_wr": None,
            "con_jt_combo_runs": None,
            "con_jo_combo_wr": None,
            "con_to_combo_wr": None,
            "con_jto_trio_runs": None,
            "con_jto_trio_wr": None,
            "con_connection_score": None,
            "con_is_new_connection": None,
        }

        # --- Jockey + Trainer ---
        jt_wr: Optional[float] = None
        jt_runs: int = 0
        if jockey and trainer:
            rec = self.jt_stats.get((jockey, trainer))
            if rec is not None and rec[1] > 0:
                jt_runs = rec[1]
                jt_wr = rec[0] / rec[1]
                feats["con_jt_combo_wr"] = round(jt_wr, 4)
                feats["con_jt_combo_runs"] = jt_runs

        # --- Jockey + Owner ---
        jo_wr: Optional[float] = None
        jo_runs: int = 0
        if jockey and owner:
            rec = self.jo_stats.get((jockey, owner))
            if rec is not None and rec[1] > 0:
                jo_runs = rec[1]
                jo_wr = rec[0] / rec[1]
                feats["con_jo_combo_wr"] = round(jo_wr, 4)

        # --- Trainer + Owner ---
        to_wr: Optional[float] = None
        to_runs: int = 0
        if trainer and owner:
            rec = self.to_stats.get((trainer, owner))
            if rec is not None and rec[1] > 0:
                to_runs = rec[1]
                to_wr = rec[0] / rec[1]
                feats["con_to_combo_wr"] = round(to_wr, 4)

        # --- Jockey + Trainer + Owner trio ---
        jto_wr: Optional[float] = None
        if jockey and trainer and owner:
            rec = self.jto_stats.get((jockey, trainer, owner))
            if rec is not None and rec[1] > 0:
                feats["con_jto_trio_runs"] = rec[1]
                jto_wr = rec[0] / rec[1]
                feats["con_jto_trio_wr"] = round(jto_wr, 4)

        # --- Connection score: weighted composite ---
        # 0.3*jt + 0.2*jo + 0.2*to + 0.3*jto
        components: list[tuple[Optional[float], float]] = [
            (jt_wr, 0.3),
            (jo_wr, 0.2),
            (to_wr, 0.2),
            (jto_wr, 0.3),
        ]
        total_weight = 0.0
        weighted_sum = 0.0
        for wr, w in components:
            if wr is not None:
                weighted_sum += wr * w
                total_weight += w

        if total_weight > 0:
            feats["con_connection_score"] = round(weighted_sum / total_weight, 4)

        # --- Is new connection ---
        # 1 if ANY of the pairs (jt, jo, to) has < _NEW_CONNECTION_THRESHOLD runs
        if jockey and trainer and owner:
            is_new = int(
                jt_runs < _NEW_CONNECTION_THRESHOLD
                or jo_runs < _NEW_CONNECTION_THRESHOLD
                or to_runs < _NEW_CONNECTION_THRESHOLD
            )
            feats["con_is_new_connection"] = is_new
        elif jockey and trainer:
            feats["con_is_new_connection"] = int(jt_runs < _NEW_CONNECTION_THRESHOLD)
        elif jockey and owner:
            feats["con_is_new_connection"] = int(jo_runs < _NEW_CONNECTION_THRESHOLD)
        elif trainer and owner:
            feats["con_is_new_connection"] = int(to_runs < _NEW_CONNECTION_THRESHOLD)

        return feats

    def update(
        self,
        jockey: Optional[str],
        trainer: Optional[str],
        owner: Optional[str],
        is_winner: bool,
    ) -> None:
        """Update state AFTER race snapshot."""
        if jockey and trainer:
            rec = self.jt_stats[(jockey, trainer)]
            rec[1] += 1
            if is_winner:
                rec[0] += 1

        if jockey and owner:
            rec = self.jo_stats[(jockey, owner)]
            rec[1] += 1
            if is_winner:
                rec[0] += 1

        if trainer and owner:
            rec = self.to_stats[(trainer, owner)]
            rec[1] += 1
            if is_winner:
                rec[0] += 1

        if jockey and trainer and owner:
            rec = self.jto_stats[(jockey, trainer, owner)]
            rec[1] += 1
            if is_winner:
                rec[0] += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_connection_strength_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build connection strength features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Connection Strength Builder (memory-optimised) ===")
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
    tracker = _ComboTracker()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    _FEATURE_NAMES = [
        "con_jt_combo_wr",
        "con_jt_combo_runs",
        "con_jo_combo_wr",
        "con_to_combo_wr",
        "con_jto_trio_runs",
        "con_jto_trio_wr",
        "con_connection_score",
        "con_is_new_connection",
    ]
    fill_counts = {k: 0 for k in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            return {
                "uid": rec.get("partant_uid"),
                "course": rec.get("course_uid", "") or "",
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "jockey": _norm(rec.get("jockey_driver")),
                "trainer": _norm(rec.get("entraineur")),
                "owner": _norm(rec.get("proprietaire")),
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
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race features (temporal integrity) --
            for rec in course_group:
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                jockey = rec["jockey"]
                trainer = rec["trainer"]
                owner = rec["owner"]

                if jockey or trainer or owner:
                    snap = tracker.snapshot(jockey, trainer, owner)
                    features.update(snap)
                else:
                    for k in _FEATURE_NAMES:
                        features[k] = None

                # Track fill counts
                for k in _FEATURE_NAMES:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after snapshotting (post-race) --
            for rec in course_group:
                tracker.update(
                    rec["jockey"],
                    rec["trainer"],
                    rec["owner"],
                    rec["is_gagnant"],
                )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Connection strength build termine: %d features en %.1fs "
        "(paires JT: %d, paires JO: %d, paires TO: %d, trios JTO: %d)",
        n_written, elapsed,
        len(tracker.jt_stats), len(tracker.jo_stats),
        len(tracker.to_stats), len(tracker.jto_stats),
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


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features connection strength a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: D:/turf-data-pipeline/03_DONNEES_MASTER/)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/connection_strength/)",
    )
    args = parser.parse_args()

    logger = setup_logging("connection_strength_builder")

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

    out_path = output_dir / "connection_strength.jsonl"
    build_connection_strength_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
