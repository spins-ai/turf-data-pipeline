#!/usr/bin/env python3
"""
feature_builders.field_quality_trend_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Field quality trend features -- tracking whether a horse is moving up or
down in class based on the quality of fields it runs against.

Reads partants_master.jsonl in streaming mode (index + chronological sort
+ seek).  Tracks per-horse field quality history to detect class
movements, trends, and appropriateness.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the state -- no future leakage.  Snapshot is taken BEFORE
the state is updated with the current race result.

Produces:
  - field_quality_trend.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_quality_trend/

Features per partant (8):
  - fqt_current_field_quality     : average gains_carriere of horses in the
                                    current race (race-level metric)
  - fqt_field_quality_vs_avg      : current field quality / horse's average
                                    field quality over career (>1 = facing
                                    tougher field than usual)
  - fqt_class_trend_3             : slope of field quality over last 3 races
                                    (positive = moving up in class)
  - fqt_is_class_rise             : 1 if current field quality > average of
                                    last 3 field qualities
  - fqt_is_class_drop             : 1 if current field quality < 0.7x average
                                    of last 3 field qualities
  - fqt_best_class_faced          : maximum field quality horse has ever faced
  - fqt_class_range               : best - worst field quality faced
  - fqt_class_appropriate         : 1 if current field quality is within 1 std
                                    of horse's average field quality

Usage:
    python feature_builders/field_quality_trend_builder.py
    python feature_builders/field_quality_trend_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_quality_trend")

# Progress / gc every N records
_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Lightweight per-horse field quality tracker.

    Uses __slots__ to minimise memory across ~200K+ horses.

    State:
      field_qualities : deque(maxlen=10) of float -- recent field qualities
      total_quality_sum : running sum of all field qualities faced
      quality_count     : total number of races with known field quality
      best_quality      : maximum field quality ever faced
      worst_quality     : minimum field quality ever faced
    """

    __slots__ = (
        "field_qualities",
        "total_quality_sum",
        "quality_count",
        "best_quality",
        "worst_quality",
    )

    def __init__(self) -> None:
        self.field_qualities: deque[float] = deque(maxlen=10)
        self.total_quality_sum: float = 0.0
        self.quality_count: int = 0
        self.best_quality: float = -1.0
        self.worst_quality: float = float("inf")


# ===========================================================================
# FEATURE COMPUTATION (snapshot BEFORE update)
# ===========================================================================


def _compute_features(
    state: _HorseState,
    current_field_quality: Optional[float],
) -> dict[str, Any]:
    """Compute 8 field quality trend features from horse state snapshot.

    All values are based on state BEFORE this race (temporal integrity).
    """
    feats: dict[str, Any] = {}

    # 1. fqt_current_field_quality (race-level, always available if computable)
    feats["fqt_current_field_quality"] = (
        round(current_field_quality, 2) if current_field_quality is not None else None
    )

    # If horse has no history, remaining features are None
    if state.quality_count == 0 or current_field_quality is None:
        feats["fqt_field_quality_vs_avg"] = None
        feats["fqt_class_trend_3"] = None
        feats["fqt_is_class_rise"] = None
        feats["fqt_is_class_drop"] = None
        feats["fqt_best_class_faced"] = None
        feats["fqt_class_range"] = None
        feats["fqt_class_appropriate"] = None
        return feats

    avg_quality = state.total_quality_sum / state.quality_count

    # 2. fqt_field_quality_vs_avg
    if avg_quality > 0:
        feats["fqt_field_quality_vs_avg"] = round(current_field_quality / avg_quality, 4)
    else:
        feats["fqt_field_quality_vs_avg"] = None

    # 3. fqt_class_trend_3 (slope over last 3)
    last3 = list(state.field_qualities)[-3:] if len(state.field_qualities) >= 3 else None
    if last3 is not None and len(last3) == 3:
        # Simple linear slope: (y3 - y1) / 2
        slope = (last3[2] - last3[0]) / 2.0
        feats["fqt_class_trend_3"] = round(slope, 2)
    else:
        feats["fqt_class_trend_3"] = None

    # Average of last 3 for class rise/drop (use all available if < 3)
    recent_vals = list(state.field_qualities)[-3:]
    if recent_vals:
        avg_last3 = sum(recent_vals) / len(recent_vals)

        # 4. fqt_is_class_rise
        feats["fqt_is_class_rise"] = 1 if current_field_quality > avg_last3 else 0

        # 5. fqt_is_class_drop
        feats["fqt_is_class_drop"] = 1 if current_field_quality < 0.7 * avg_last3 else 0
    else:
        feats["fqt_is_class_rise"] = None
        feats["fqt_is_class_drop"] = None

    # 6. fqt_best_class_faced
    if state.best_quality >= 0:
        feats["fqt_best_class_faced"] = round(state.best_quality, 2)
    else:
        feats["fqt_best_class_faced"] = None

    # 7. fqt_class_range
    if state.best_quality >= 0 and state.worst_quality < float("inf"):
        feats["fqt_class_range"] = round(state.best_quality - state.worst_quality, 2)
    else:
        feats["fqt_class_range"] = None

    # 8. fqt_class_appropriate (within 1 std of average)
    if state.quality_count >= 2:
        # Compute running std from deque values
        vals = list(state.field_qualities)
        if len(vals) >= 2:
            mean_v = sum(vals) / len(vals)
            variance = sum((v - mean_v) ** 2 for v in vals) / len(vals)
            std_v = math.sqrt(variance)
            feats["fqt_class_appropriate"] = (
                1 if abs(current_field_quality - avg_quality) <= std_v else 0
            )
        else:
            feats["fqt_class_appropriate"] = None
    else:
        feats["fqt_class_appropriate"] = None

    return feats


# ===========================================================================
# STATE UPDATE (after snapshot)
# ===========================================================================


def _update_state(state: _HorseState, field_quality: Optional[float]) -> None:
    """Update horse state after this race."""
    if field_quality is None:
        return
    state.field_qualities.append(field_quality)
    state.total_quality_sum += field_quality
    state.quality_count += 1
    if field_quality > state.best_quality:
        state.best_quality = field_quality
    if field_quality < state.worst_quality:
        state.worst_quality = field_quality


# ===========================================================================
# MAIN BUILD (index + sort + seek, two-pass)
# ===========================================================================


def build_field_quality_trend_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build field quality trend features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, group by course,
         precompute course-level field quality, then process per-horse
         with snapshot-before-update and stream output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Field Quality Trend Builder (index + sort + seek) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    index: list[tuple[str, str, int, int]] = []  # (date, course_uid, num_pmu, offset)
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
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "fqt_current_field_quality",
        "fqt_field_quality_vs_avg",
        "fqt_class_trend_3",
        "fqt_is_class_rise",
        "fqt_is_class_drop",
        "fqt_best_class_faced",
        "fqt_class_range",
        "fqt_class_appropriate",
    ]
    fill_counts = {k: 0 for k in feature_names}

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

            # -- Precompute course-level field quality --
            # Average gains_carriere of all horses in this race
            gains_values: list[float] = []
            for rec in course_records:
                g = _safe_float(rec.get("gains_carriere_euros"))
                if g is not None and g >= 0:
                    gains_values.append(g)

            current_field_quality: Optional[float] = None
            if gains_values:
                current_field_quality = sum(gains_values) / len(gains_values)

            # -- Snapshot BEFORE update (temporal integrity) --
            snapshots: list[tuple[Optional[str], Optional[float]]] = []
            # Store (cheval, field_quality) for deferred update

            for rec in course_records:
                cheval = rec.get("nom_cheval") or ""
                partant_uid = rec.get("partant_uid") or ""
                course_uid_rec = rec.get("course_uid") or ""
                date_iso = rec.get("date_reunion_iso") or ""

                if not cheval:
                    # No horse name => emit empty features
                    out_rec: dict[str, Any] = {
                        "partant_uid": partant_uid,
                        "course_uid": course_uid_rec,
                        "date_reunion_iso": date_iso,
                    }
                    for fn in feature_names:
                        out_rec[fn] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                    n_written += 1
                    snapshots.append((None, None))
                    continue

                state = horse_states[cheval]

                # Compute features from PRE-RACE state
                feats = _compute_features(state, current_field_quality)

                # Write output
                out_rec = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_rec,
                    "date_reunion_iso": date_iso,
                }
                for fn in feature_names:
                    val = feats.get(fn)
                    out_rec[fn] = val
                    if val is not None:
                        fill_counts[fn] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Store info needed for state update
                snapshots.append((cheval, current_field_quality))

            # -- Update states AFTER all snapshots for this course --
            for cheval, fq in snapshots:
                if cheval is None:
                    continue
                state = horse_states[cheval]
                _update_state(state, fq)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Field quality trend build termine: %d features en %.1fs (chevaux uniques: %d)",
        n_written, elapsed, len(horse_states),
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
        description="Field quality trend: features de tendance de classe"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/field_quality_trend/)",
    )
    args = parser.parse_args()

    logger = setup_logging("field_quality_trend_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "field_quality_trend.jsonl"
    build_field_quality_trend_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
