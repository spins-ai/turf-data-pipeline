#!/usr/bin/env python3
"""
feature_builders.betting_pattern_sequence_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Betting pattern sequence features for temporal / embedding models.

Reads partants_master.jsonl in memory-optimised mode (index + sort + seek),
processes all records chronologically, and computes per-partant betting
sequence features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the sequence state -- no future leakage.  State is
snapshotted BEFORE the current race updates it.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - betting_pattern_sequence.jsonl  in builder_outputs/betting_pattern_sequence/

Features per partant (8):
  - bps_odds_sequence_5          : comma-separated last 5 cote_finale values
                                   (string for embedding models)
  - bps_odds_direction_seq       : "U"/"D"/"S" sequence for last 5 races
                                   (Up/Down/Same odds vs previous)
  - bps_avg_odds_last_3          : average cote_finale over last 3 races
  - bps_avg_odds_last_5          : average cote_finale over last 5 races
  - bps_odds_trend_direction     : +1 if odds trending down (improving),
                                   -1 if trending up, 0 if flat
  - bps_market_confidence_trend  : avg (cote_ref - cote_finale) / cote_ref
                                   over last 3 races (positive = market backing)
  - bps_pnl_last_5              : simulated P&L betting this horse last 5 races
                                   sum of (cote_finale-1) if won, -1 if lost
  - bps_roi_last_10             : simulated ROI over last 10 bets on this horse

Usage:
    python feature_builders/betting_pattern_sequence_builder.py
    python feature_builders/betting_pattern_sequence_builder.py --input path/to/partants_master.jsonl
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
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/betting_pattern_sequence")

_LOG_EVERY = 500_000


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseBettingState:
    """Track rolling betting pattern state for one horse."""

    __slots__ = (
        "odds_history",        # deque(maxlen=10): last 10 cote_finale values
        "market_moves",        # deque(maxlen=5): last 5 (cote_ref - cote_finale)/cote_ref
        "results_with_odds",   # deque(maxlen=10): last 10 (cote_finale, is_gagnant)
    )

    def __init__(self) -> None:
        self.odds_history: deque = deque(maxlen=10)
        self.market_moves: deque = deque(maxlen=5)
        self.results_with_odds: deque = deque(maxlen=10)


# ===========================================================================
# FEATURE COMPUTATION (from snapshot BEFORE update)
# ===========================================================================


def _compute_features(hs: _HorseBettingState) -> dict[str, Any]:
    """Compute all 8 betting pattern sequence features from pre-race state."""
    feats: dict[str, Any] = {}

    odds = list(hs.odds_history)

    # 1. bps_odds_sequence_5: comma-separated last 5 cote_finale
    if len(odds) >= 1:
        last5 = odds[-5:] if len(odds) >= 5 else odds
        feats["bps_odds_sequence_5"] = ",".join(f"{v:.2f}" for v in last5)
    else:
        feats["bps_odds_sequence_5"] = None

    # 2. bps_odds_direction_seq: U/D/S sequence for last 5 races
    if len(odds) >= 2:
        directions = []
        seq_odds = odds[-5:] if len(odds) >= 5 else odds
        for j in range(1, len(seq_odds)):
            diff = seq_odds[j] - seq_odds[j - 1]
            if diff > 0.5:
                directions.append("U")
            elif diff < -0.5:
                directions.append("D")
            else:
                directions.append("S")
        feats["bps_odds_direction_seq"] = "".join(directions) if directions else None
    else:
        feats["bps_odds_direction_seq"] = None

    # 3. bps_avg_odds_last_3
    if len(odds) >= 3:
        feats["bps_avg_odds_last_3"] = round(sum(odds[-3:]) / 3, 4)
    else:
        feats["bps_avg_odds_last_3"] = None

    # 4. bps_avg_odds_last_5
    if len(odds) >= 5:
        feats["bps_avg_odds_last_5"] = round(sum(odds[-5:]) / 5, 4)
    else:
        feats["bps_avg_odds_last_5"] = None

    # 5. bps_odds_trend_direction: +1 if odds trending down (improving),
    #    -1 if trending up (drifting), 0 if flat.
    #    Use linear comparison of first half vs second half of last 5.
    if len(odds) >= 4:
        recent = odds[-5:] if len(odds) >= 5 else odds
        mid = len(recent) // 2
        first_half_avg = sum(recent[:mid]) / mid
        second_half_avg = sum(recent[mid:]) / (len(recent) - mid)
        diff = second_half_avg - first_half_avg
        if diff < -0.5:
            feats["bps_odds_trend_direction"] = 1   # improving (lower odds)
        elif diff > 0.5:
            feats["bps_odds_trend_direction"] = -1  # drifting (higher odds)
        else:
            feats["bps_odds_trend_direction"] = 0
    else:
        feats["bps_odds_trend_direction"] = None

    # 6. bps_market_confidence_trend: avg (cote_ref - cote_finale)/cote_ref
    #    over last 3 market moves (positive = market consistently backing)
    moves = list(hs.market_moves)
    if len(moves) >= 3:
        feats["bps_market_confidence_trend"] = round(sum(moves[-3:]) / 3, 4)
    else:
        feats["bps_market_confidence_trend"] = None

    # 7. bps_pnl_last_5: simulated P&L if you always bet on this horse
    #    last 5 races.  +cote_finale-1 if won, -1 if lost.
    rwo = list(hs.results_with_odds)
    if len(rwo) >= 1:
        last5_rwo = rwo[-5:] if len(rwo) >= 5 else rwo
        pnl = 0.0
        for cote, won in last5_rwo:
            if won:
                pnl += cote - 1.0
            else:
                pnl -= 1.0
        feats["bps_pnl_last_5"] = round(pnl, 4)
    else:
        feats["bps_pnl_last_5"] = None

    # 8. bps_roi_last_10: simulated ROI over last 10 bets
    if len(rwo) >= 1:
        total_return = 0.0
        for cote, won in rwo:
            if won:
                total_return += cote
        roi = (total_return / len(rwo)) - 1.0
        feats["bps_roi_last_10"] = round(roi, 4)
    else:
        feats["bps_roi_last_10"] = None

    return feats


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_betting_pattern_sequence_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build betting pattern sequence features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Betting Pattern Sequence Builder (memory-optimised) ===")
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

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseBettingState] = defaultdict(_HorseBettingState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "bps_odds_sequence_5",
        "bps_odds_direction_seq",
        "bps_avg_odds_last_3",
        "bps_avg_odds_last_5",
        "bps_odds_trend_direction",
        "bps_market_confidence_trend",
        "bps_pnl_last_5",
        "bps_roi_last_10",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
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

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")

                out_rec: dict[str, Any] = {
                    "partant_uid": rec.get("partant_uid"),
                    "course_uid": rec.get("course_uid"),
                    "date_reunion_iso": rec.get("date_reunion_iso"),
                }

                if not horse_id:
                    for k in feature_keys:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[horse_id]

                # Compute features from pre-race state (snapshot BEFORE update)
                feats = _compute_features(hs)

                for k in feature_keys:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # Prepare deferred state update
                cote_finale = None
                cf_raw = rec.get("cote_finale")
                if cf_raw is not None:
                    try:
                        cote_finale = float(cf_raw)
                        if cote_finale <= 0:
                            cote_finale = None
                    except (ValueError, TypeError):
                        cote_finale = None

                cote_reference = None
                cr_raw = rec.get("cote_reference")
                if cr_raw is not None:
                    try:
                        cote_reference = float(cr_raw)
                        if cote_reference <= 0:
                            cote_reference = None
                    except (ValueError, TypeError):
                        cote_reference = None

                is_gagnant = bool(rec.get("is_gagnant"))

                post_updates.append((
                    horse_id, cote_finale, cote_reference, is_gagnant,
                ))

            # -- Update horse states after race (no leakage) --
            for horse_id, cote_finale, cote_reference, is_gagnant in post_updates:
                hs = horse_state[horse_id]

                if cote_finale is not None:
                    hs.odds_history.append(cote_finale)

                    # Market move: (cote_ref - cote_finale) / cote_ref
                    if cote_reference is not None and cote_reference > 0:
                        move = (cote_reference - cote_finale) / cote_reference
                        hs.market_moves.append(round(move, 6))

                    hs.results_with_odds.append((cote_finale, is_gagnant))

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Betting pattern sequence build termine: %d features en %.1fs (chevaux suivis: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0
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
        description="Construction des features betting pattern sequence a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/betting_pattern_sequence/)",
    )
    args = parser.parse_args()

    logger = setup_logging("betting_pattern_sequence_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "betting_pattern_sequence.jsonl"
    build_betting_pattern_sequence_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
