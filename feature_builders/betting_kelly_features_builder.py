#!/usr/bin/env python3
"""
feature_builders.betting_kelly_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Kelly criterion and advanced bet sizing features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the horse's historical win rate -- no future leakage.

Produces:
  - betting_kelly_features.jsonl  in builder_outputs/betting_kelly_features/

Features per partant (8):
  - bkf_full_kelly             : (wr * cote - 1) / (cote - 1), capped [0, 0.5]
  - bkf_half_kelly             : full_kelly / 2 (conservative sizing)
  - bkf_expected_value         : wr * cote_finale (>1 = positive EV)
  - bkf_ev_per_unit_risk       : (wr * cote - 1) -- net EV per unit bet
  - bkf_variance_of_returns    : wr * (cote-1)^2 + (1-wr) * 1
  - bkf_sharpe_ratio           : ev_per_unit / sqrt(variance) if variance > 0
  - bkf_edge_confidence        : 1 if horse has 10+ races AND kelly > 0.05
  - bkf_optimal_bet_type_score : 0=avoid, 1=small, 2=medium, 3=large

State per horse: wins, total (for win rate estimation).
Process course-by-course; snapshot BEFORE update.

Usage:
    python feature_builders/betting_kelly_features_builder.py
    python feature_builders/betting_kelly_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/betting_kelly_features")

_LOG_EVERY = 500_000

# Kelly caps
KELLY_CAP_FULL = 0.50
KELLY_THRESHOLD_CONFIDENT = 0.05
MIN_RACES_CONFIDENT = 10


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseStats:
    """Lightweight per-horse win/total tracker."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def win_rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return self.wins / self.total


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _compute_kelly_features(
    wr: Optional[float],
    cote: Optional[float],
    total_races: int,
) -> dict[str, Any]:
    """Compute all 8 Kelly features from win rate and final odds.

    Returns dict with bkf_* keys (values may be None).
    """
    feats: dict[str, Any] = {
        "bkf_full_kelly": None,
        "bkf_half_kelly": None,
        "bkf_expected_value": None,
        "bkf_ev_per_unit_risk": None,
        "bkf_variance_of_returns": None,
        "bkf_sharpe_ratio": None,
        "bkf_edge_confidence": None,
        "bkf_optimal_bet_type_score": None,
    }

    if wr is None or cote is None or cote <= 1.0:
        return feats

    # --- Full Kelly: (wr * cote - 1) / (cote - 1), capped [0, 0.5] ---
    denom = cote - 1.0
    if denom <= 0:
        return feats

    raw_kelly = (wr * cote - 1.0) / denom
    full_kelly = max(0.0, min(raw_kelly, KELLY_CAP_FULL))
    feats["bkf_full_kelly"] = round(full_kelly, 6)

    # --- Half Kelly ---
    feats["bkf_half_kelly"] = round(full_kelly / 2.0, 6)

    # --- Expected value: wr * cote ---
    ev = wr * cote
    feats["bkf_expected_value"] = round(ev, 6)

    # --- EV per unit risk: (wr * cote - 1) ---
    ev_per_unit = wr * cote - 1.0
    feats["bkf_ev_per_unit_risk"] = round(ev_per_unit, 6)

    # --- Variance of returns: wr * (cote-1)^2 + (1-wr) * 1 ---
    variance = wr * (denom ** 2) + (1.0 - wr) * 1.0
    feats["bkf_variance_of_returns"] = round(variance, 6)

    # --- Sharpe ratio: ev_per_unit / sqrt(variance) ---
    if variance > 0:
        feats["bkf_sharpe_ratio"] = round(ev_per_unit / math.sqrt(variance), 6)

    # --- Edge confidence: 1 if 10+ races AND kelly > 0.05 ---
    if total_races >= MIN_RACES_CONFIDENT and full_kelly > KELLY_THRESHOLD_CONFIDENT:
        feats["bkf_edge_confidence"] = 1
    else:
        feats["bkf_edge_confidence"] = 0

    # --- Optimal bet type score ---
    if full_kelly <= 0:
        feats["bkf_optimal_bet_type_score"] = 0  # avoid
    elif full_kelly <= 0.05:
        feats["bkf_optimal_bet_type_score"] = 1  # small
    elif full_kelly <= 0.15:
        feats["bkf_optimal_bet_type_score"] = 2  # medium
    else:
        feats["bkf_optimal_bet_type_score"] = 3  # large

    return feats


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort, seek-based, streaming output)
# ===========================================================================


def build_betting_kelly_features(input_path: Path, output_path: Path, logger) -> int:
    """Build Kelly criterion features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Betting Kelly Features Builder ===")
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
    horse_stats: dict[str, _HorseStats] = defaultdict(_HorseStats)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "bkf_full_kelly": 0,
        "bkf_half_kelly": 0,
        "bkf_expected_value": 0,
        "bkf_ev_per_unit_risk": 0,
        "bkf_variance_of_returns": 0,
        "bkf_sharpe_ratio": 0,
        "bkf_edge_confidence": 0,
        "bkf_optimal_bet_type_score": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(byte_offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(byte_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            cote_finale = rec.get("cote_finale") or rec.get("rapport_final")
            if cote_finale is not None:
                try:
                    cote_finale = float(cote_finale)
                    if cote_finale <= 1.0:
                        cote_finale = None
                except (ValueError, TypeError):
                    cote_finale = None

            return {
                "uid": rec.get("partant_uid"),
                "cheval": rec.get("nom_cheval"),
                "gagnant": bool(rec.get("is_gagnant")),
                "cote_finale": cote_finale,
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

            # -- Snapshot pre-race stats and emit features --
            post_updates: list[tuple[Optional[str], bool]] = []

            for rec in course_group:
                cheval = rec["cheval"]
                cote = rec["cote_finale"]

                # Get win rate from PAST data only
                if cheval and cheval in horse_stats:
                    wr = horse_stats[cheval].win_rate()
                    total_races = horse_stats[cheval].total
                else:
                    wr = None
                    total_races = 0

                feats = _compute_kelly_features(wr, cote, total_races)
                feats["partant_uid"] = rec["uid"]

                # Track fill counts
                for k in fill_counts:
                    if feats.get(k) is not None:
                        fill_counts[k] += 1

                # Stream directly to output file
                fout.write(json.dumps(feats, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                post_updates.append((cheval, rec["gagnant"]))

            # -- Update horse stats AFTER race (no leakage) --
            for cheval, is_winner in post_updates:
                if not cheval:
                    continue
                horse_stats[cheval].total += 1
                if is_winner:
                    horse_stats[cheval].wins += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Kelly features build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_stats),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)",
            k, v, n_written, 100 * v / n_written if n_written else 0,
        )

    return n_written


# ===========================================================================
# SAUVEGARDE & CLI
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
        description="Construction des features Kelly criterion a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/betting_kelly_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("betting_kelly_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "betting_kelly_features.jsonl"
    build_betting_kelly_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
