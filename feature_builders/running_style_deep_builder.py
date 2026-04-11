#!/usr/bin/env python3
"""
feature_builders.running_style_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep running style features -- inferring a horse's running style from
position patterns (num_pmu as draw proxy) and finishing data.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant running-style features.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.  Snapshot BEFORE update.

Memory-optimised version:
  - Phase 1 reads only minimal tuples (sort keys + byte offsets)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 re-reads records from disk via seek, streams output to .tmp
  - gc.collect() every 500K records

Produces:
  - running_style_deep.jsonl   in builder_outputs/running_style_deep/

Features per partant (8):
  - rsd_avg_early_position       : average num_pmu across last 20 races (lower = more frontal draws)
  - rsd_win_from_front           : wins from inner draws (num_pmu <= 4) / total wins
  - rsd_win_from_back            : wins from outer draws (num_pmu > partants*0.6) / total wins
  - rsd_closing_ability          : avg improvement rate (num_pmu - finish_position) / num_pmu
  - rsd_frontrunner_score        : top-3 finish rate when drawn in inner positions
  - rsd_closer_score             : top-3 finish rate when drawn in outer positions
  - rsd_style_encoded            : 0=frontrunner, 1=stalker, 2=closer (based on where wins come from)
  - rsd_style_consistency        : 1 - variance of (num_pmu / partants) across last 20 races

Usage:
    python feature_builders/running_style_deep_builder.py
    python feature_builders/running_style_deep_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/running_style_deep")

_LOG_EVERY = 500_000

_WINDOW = 20  # rolling window for deques


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseRunningState:
    """Per-horse running style state, memory-optimised with __slots__."""

    __slots__ = (
        "draws", "positions", "partants",
        "inner_results", "outer_results", "mid_results",
        "inner_top3", "outer_top3",
    )

    def __init__(self) -> None:
        # Rolling windows (last 20 races)
        self.draws: deque = deque(maxlen=_WINDOW)        # num_pmu values
        self.positions: deque = deque(maxlen=_WINDOW)     # finish position (place)
        self.partants: deque = deque(maxlen=_WINDOW)      # field sizes

        # Lifetime counters: [wins, places (top3), total]
        self.inner_results: list[int] = [0, 0, 0]   # draws <= 4
        self.outer_results: list[int] = [0, 0, 0]   # draws > partants * 0.6
        self.mid_results: list[int] = [0, 0, 0]      # everything else

        # Top-3 when drawn inner/outer
        self.inner_top3: int = 0
        self.outer_top3: int = 0

    def snapshot(self) -> dict[str, Any]:
        """Return features from CURRENT state (before this race's update)."""
        feats: dict[str, Any] = {}

        # 1. rsd_avg_early_position
        if self.draws:
            feats["rsd_avg_early_position"] = round(
                sum(self.draws) / len(self.draws), 4
            )
        else:
            feats["rsd_avg_early_position"] = None

        # Total wins across all zones
        total_wins = (
            self.inner_results[0] + self.outer_results[0] + self.mid_results[0]
        )

        # 2. rsd_win_from_front
        if total_wins > 0:
            feats["rsd_win_from_front"] = round(
                self.inner_results[0] / total_wins, 4
            )
        else:
            feats["rsd_win_from_front"] = None

        # 3. rsd_win_from_back
        if total_wins > 0:
            feats["rsd_win_from_back"] = round(
                self.outer_results[0] / total_wins, 4
            )
        else:
            feats["rsd_win_from_back"] = None

        # 4. rsd_closing_ability : avg (num_pmu - finish_pos) / num_pmu
        if self.draws and self.positions and len(self.draws) == len(self.positions):
            improvements = []
            for draw, pos in zip(self.draws, self.positions):
                if draw and draw > 0 and pos is not None:
                    improvements.append((draw - pos) / draw)
            if improvements:
                feats["rsd_closing_ability"] = round(
                    sum(improvements) / len(improvements), 4
                )
            else:
                feats["rsd_closing_ability"] = None
        else:
            feats["rsd_closing_ability"] = None

        # 5. rsd_frontrunner_score : top3 rate from inner positions
        if self.inner_results[2] > 0:
            feats["rsd_frontrunner_score"] = round(
                self.inner_top3 / self.inner_results[2], 4
            )
        else:
            feats["rsd_frontrunner_score"] = None

        # 6. rsd_closer_score : top3 rate from outer positions
        if self.outer_results[2] > 0:
            feats["rsd_closer_score"] = round(
                self.outer_top3 / self.outer_results[2], 4
            )
        else:
            feats["rsd_closer_score"] = None

        # 7. rsd_style_encoded
        if total_wins >= 2:
            inner_w = self.inner_results[0]
            outer_w = self.outer_results[0]
            mid_w = self.mid_results[0]
            if inner_w >= outer_w and inner_w >= mid_w:
                feats["rsd_style_encoded"] = 0  # frontrunner
            elif outer_w >= inner_w and outer_w >= mid_w:
                feats["rsd_style_encoded"] = 2  # closer
            else:
                feats["rsd_style_encoded"] = 1  # stalker
        else:
            feats["rsd_style_encoded"] = None

        # 8. rsd_style_consistency : 1 - var(num_pmu / partants)
        if len(self.draws) >= 3 and len(self.partants) >= 3:
            ratios = []
            for d, p in zip(self.draws, self.partants):
                if p and p > 0:
                    ratios.append(d / p)
            if len(ratios) >= 3:
                mean_r = sum(ratios) / len(ratios)
                var_r = sum((r - mean_r) ** 2 for r in ratios) / len(ratios)
                feats["rsd_style_consistency"] = round(max(0.0, 1.0 - var_r), 4)
            else:
                feats["rsd_style_consistency"] = None
        else:
            feats["rsd_style_consistency"] = None

        return feats

    def update(
        self,
        num_pmu: int,
        finish_pos: Optional[int],
        nb_partants: int,
        is_gagnant: bool,
    ) -> None:
        """Update state AFTER snapshot has been taken."""
        self.draws.append(num_pmu)
        self.positions.append(finish_pos)
        self.partants.append(nb_partants)

        is_top3 = finish_pos is not None and finish_pos <= 3

        # Classify draw zone
        if num_pmu <= 4:
            zone = self.inner_results
            if is_gagnant:
                zone[0] += 1
            if is_top3:
                zone[1] += 1
                self.inner_top3 += 1
            zone[2] += 1
        elif nb_partants > 0 and num_pmu > nb_partants * 0.6:
            zone = self.outer_results
            if is_gagnant:
                zone[0] += 1
            if is_top3:
                zone[1] += 1
                self.outer_top3 += 1
            zone[2] += 1
        else:
            zone = self.mid_results
            if is_gagnant:
                zone[0] += 1
            if is_top3:
                zone[1] += 1
            zone[2] += 1


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


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


def build_running_style_deep(input_path: Path, output_path: Path, logger) -> int:
    """Build deep running style features from partants_master.jsonl.

    Two-phase approach:
      1. Index: read sort keys + byte offsets (lightweight).
      2. Sort chronologically, then seek-read records course by course,
         streaming output to .tmp, then atomic rename.

    Returns the total number of feature records written.
    """
    logger.info("=== Running Style Deep Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseRunningState] = defaultdict(_HorseRunningState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts: dict[str, int] = {
        "rsd_avg_early_position": 0,
        "rsd_win_from_front": 0,
        "rsd_win_from_back": 0,
        "rsd_closing_ability": 0,
        "rsd_frontrunner_score": 0,
        "rsd_closer_score": 0,
        "rsd_style_encoded": 0,
        "rsd_style_consistency": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(byte_offset: int) -> dict:
            fin.seek(byte_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            nb_partants = _safe_int(rec.get("nombre_partants")) or 0
            num_pmu = _safe_int(rec.get("num_pmu")) or 0
            # finish position: use place_arrivee or arrivee_ordre
            finish_pos = _safe_int(rec.get("place_arrivee"))
            if finish_pos is None:
                finish_pos = _safe_int(rec.get("arrivee_ordre"))

            return {
                "uid": rec.get("partant_uid"),
                "cheval": rec.get("nom_cheval"),
                "gagnant": bool(rec.get("is_gagnant")),
                "num_pmu": num_pmu,
                "finish_pos": finish_pos,
                "nb_partants": nb_partants,
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
            post_updates: list[tuple[str, int, Optional[int], int, bool]] = []

            for rec in course_group:
                cheval = rec["cheval"]
                if not cheval:
                    # Write empty features
                    features = {"partant_uid": rec["uid"]}
                    for k in fill_counts:
                        features[k] = None
                    fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                # Snapshot
                hs = horse_state[cheval]
                features = hs.snapshot()
                features["partant_uid"] = rec["uid"]

                # Track fill rates
                for k in fill_counts:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                # Defer update
                post_updates.append((
                    cheval,
                    rec["num_pmu"],
                    rec["finish_pos"],
                    rec["nb_partants"],
                    rec["gagnant"],
                ))

            # -- Update states AFTER all snapshots --
            for cheval, num_pmu, finish_pos, nb_partants, is_gagnant in post_updates:
                horse_state[cheval].update(num_pmu, finish_pos, nb_partants, is_gagnant)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Running style deep build termine: %d features en %.1fs (chevaux: %d)",
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
        description="Construction des features running style deep a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/running_style_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("running_style_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "running_style_deep.jsonl"
    build_running_style_deep(input_path, out_path, logger)


if __name__ == "__main__":
    main()
