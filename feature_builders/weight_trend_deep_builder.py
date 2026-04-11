#!/usr/bin/env python3
"""
feature_builders.weight_trend_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep weight analysis features tracking poids evolution per horse.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the weight metrics -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - weight_trend_deep.jsonl  in builder_outputs/weight_trend_deep/

Features per partant (8):
  - wtd_weight_current         : poids_porte_kg as float
  - wtd_weight_delta_last      : current weight - last race weight
  - wtd_weight_avg_3           : average weight over last 3 races
  - wtd_weight_max_career      : maximum weight carried in career
  - wtd_weight_vs_avg          : current weight / career average weight
  - wtd_optimal_weight_wr      : horse's win rate at similar weight (+/-1kg bracket)
  - wtd_weight_class_indicator : poids_base_kg quintile (higher base = higher class)
  - wtd_light_weight_advantage : 1 if poids_porte < 54kg, 0 otherwise

Usage:
    python feature_builders/weight_trend_deep_builder.py
    python feature_builders/weight_trend_deep_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/weight_trend_deep")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _weight_bracket(weight: float) -> int:
    """Round weight to nearest integer for +/-1kg bracket grouping."""
    return round(weight)


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseState:
    """Track rolling weight state for one horse."""

    __slots__ = (
        "weight_history",       # deque(maxlen=5): last 5 poids_porte_kg values
        "max_weight",           # float or None: maximum weight carried in career
        "weight_sum",           # float: sum of all weights (for career average)
        "weight_count",         # int: number of races with weight data
        "weight_bracket_stats", # dict[int, list[int, int]]: bracket -> [wins, total]
    )

    def __init__(self) -> None:
        self.weight_history: deque = deque(maxlen=5)
        self.max_weight: Optional[float] = None
        self.weight_sum: float = 0.0
        self.weight_count: int = 0
        self.weight_bracket_stats: dict[int, list] = defaultdict(lambda: [0, 0])


# ===========================================================================
# FEATURE COMPUTATION (from snapshot BEFORE update)
# ===========================================================================


def _compute_features(
    hs: _HorseState,
    current_weight: Optional[float],
    current_base_weight: Optional[float],
    all_base_weights: list[Optional[float]],
) -> dict[str, Any]:
    """Compute all 8 weight trend deep features from pre-race state."""
    feats: dict[str, Any] = {}

    # 1. wtd_weight_current: poids_porte_kg as float
    feats["wtd_weight_current"] = round(current_weight, 2) if current_weight is not None else None

    # 2. wtd_weight_delta_last: current weight - last race weight
    if current_weight is not None and len(hs.weight_history) > 0:
        feats["wtd_weight_delta_last"] = round(current_weight - hs.weight_history[-1], 2)
    else:
        feats["wtd_weight_delta_last"] = None

    # 3. wtd_weight_avg_3: average weight over last 3 races (from history)
    hist = list(hs.weight_history)
    recent = hist[-3:] if len(hist) >= 3 else None
    if recent:
        feats["wtd_weight_avg_3"] = round(sum(recent) / len(recent), 2)
    else:
        feats["wtd_weight_avg_3"] = None

    # 4. wtd_weight_max_career: maximum weight carried in career (pre-race)
    feats["wtd_weight_max_career"] = round(hs.max_weight, 2) if hs.max_weight is not None else None

    # 5. wtd_weight_vs_avg: current weight / career average weight
    if current_weight is not None and hs.weight_count > 0:
        career_avg = hs.weight_sum / hs.weight_count
        if career_avg > 0:
            feats["wtd_weight_vs_avg"] = round(current_weight / career_avg, 4)
        else:
            feats["wtd_weight_vs_avg"] = None
    else:
        feats["wtd_weight_vs_avg"] = None

    # 6. wtd_optimal_weight_wr: win rate at similar weight (+/-1kg bracket)
    if current_weight is not None:
        bk = _weight_bracket(current_weight)
        stats = hs.weight_bracket_stats.get(bk)
        if stats and stats[1] >= 3:
            feats["wtd_optimal_weight_wr"] = round(stats[0] / stats[1], 4)
        else:
            feats["wtd_optimal_weight_wr"] = None
    else:
        feats["wtd_optimal_weight_wr"] = None

    # 7. wtd_weight_class_indicator: poids_base_kg quintile across the race field
    #    Higher base weight = higher class in handicaps
    #    We compute quintile relative to the race field (all_base_weights)
    if current_base_weight is not None:
        valid_bases = sorted([w for w in all_base_weights if w is not None])
        if len(valid_bases) >= 5:
            rank = 0
            for w in valid_bases:
                if w < current_base_weight:
                    rank += 1
                elif w == current_base_weight:
                    rank += 0.5
            pct = rank / len(valid_bases)
            feats["wtd_weight_class_indicator"] = min(int(pct * 5) + 1, 5)
        else:
            feats["wtd_weight_class_indicator"] = None
    else:
        feats["wtd_weight_class_indicator"] = None

    # 8. wtd_light_weight_advantage: 1 if poids_porte < 54kg, 0 otherwise
    if current_weight is not None:
        feats["wtd_light_weight_advantage"] = 1 if current_weight < 54.0 else 0
    else:
        feats["wtd_light_weight_advantage"] = None

    return feats


# ===========================================================================
# UPDATE HORSE STATE (post-race, AFTER feature extraction)
# ===========================================================================


def _update_state(
    hs: _HorseState,
    weight: Optional[float],
    is_winner: bool,
) -> None:
    """Update the horse's rolling weight state after a race."""
    if weight is not None:
        hs.weight_history.append(weight)
        hs.weight_sum += weight
        hs.weight_count += 1
        if hs.max_weight is None or weight > hs.max_weight:
            hs.max_weight = weight

        # Update bracket win/total stats
        bk = _weight_bracket(weight)
        hs.weight_bracket_stats[bk][1] += 1
        if is_winner:
            hs.weight_bracket_stats[bk][0] += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_weight_trend_deep_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build weight trend deep features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Weight Trend Deep Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "wtd_weight_current",
        "wtd_weight_delta_last",
        "wtd_weight_avg_3",
        "wtd_weight_max_career",
        "wtd_weight_vs_avg",
        "wtd_optimal_weight_wr",
        "wtd_weight_class_indicator",
        "wtd_light_weight_advantage",
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

            # Pre-compute all base weights for this race (for quintile calculation)
            all_base_weights = [
                _safe_float(rec.get("poids_base_kg")) for rec in course_records
            ]

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                if not horse_id:
                    # Still emit a record with Nones
                    out_rec = {
                        "partant_uid": rec.get("partant_uid"),
                        "course_uid": rec.get("course_uid"),
                        "date_reunion_iso": rec.get("date_reunion_iso"),
                    }
                    for k in feature_keys:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[horse_id]

                # Extract current race fields
                current_weight = _safe_float(rec.get("poids_porte_kg"))
                current_base_weight = _safe_float(rec.get("poids_base_kg"))

                # Compute features from pre-race state (snapshot BEFORE update)
                feats = _compute_features(
                    hs, current_weight, current_base_weight, all_base_weights
                )

                out_rec = {
                    "partant_uid": rec.get("partant_uid"),
                    "course_uid": rec.get("course_uid"),
                    "date_reunion_iso": rec.get("date_reunion_iso"),
                }
                for k in feature_keys:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # Prepare deferred state update
                is_winner = bool(rec.get("is_gagnant"))
                post_updates.append((horse_id, current_weight, is_winner))

            # -- Update horse states after race (no leakage) --
            for horse_id, weight, is_winner in post_updates:
                _update_state(horse_state[horse_id], weight, is_winner)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Weight trend deep build termine: %d features en %.1fs (chevaux suivis: %d)",
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
        description="Construction des features poids evolution a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/weight_trend_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("weight_trend_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "weight_trend_deep.jsonl"
    build_weight_trend_deep_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
