#!/usr/bin/env python3
"""
feature_builders.field_pace_projection_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Projects the likely race pace from the field composition using each horse's
historical running patterns.

Reads partants_master.jsonl in index+seek streaming mode, processes all
records chronologically, and computes per-partant field pace projection
features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the speed statistics -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - field_pace_projection.jsonl  in builder_outputs/field_pace_projection/

Features per partant (10):
  - fpp_horse_avg_speed          : average reduction_km_ms (or temps_ms/distance)
                                   from horse's past races
  - fpp_horse_speed_consistency  : std of horse's speeds (lower = more consistent)
  - fpp_field_avg_speed          : average of all horses' avg_speed in this race
  - fpp_horse_vs_field_speed     : horse avg speed minus field avg speed
  - fpp_field_speed_spread       : std of field avg speeds (wide = mixed pace)
  - fpp_nb_fast_horses           : count of horses with above-median speed
  - fpp_pace_pressure            : nb_fast_horses / nombre_partants
  - fpp_horse_early_speed_rank   : horse's speed rank within field (1 = fastest)
  - fpp_lone_speed               : 1 if horse is only one in top 25% of speed
  - fpp_horse_speed_improving    : 1 if last 2 speeds > career average

Usage:
    python feature_builders/field_pace_projection_builder.py
    python feature_builders/field_pace_projection_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_pace_projection"
)

_LOG_EVERY = 500_000
_SPEED_WINDOW = 10  # maxlen for deque of recent speeds


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Safely convert a value to int."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_stdev(values: list[float]) -> Optional[float]:
    """Population standard deviation. None if fewer than 2 values."""
    n = len(values)
    if n < 2:
        return None
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / n
    return round(math.sqrt(var), 4)


def _compute_speed(rec: dict) -> Optional[float]:
    """Compute a speed value from a record.

    Prefers reduction_km_ms (lower = faster); falls back to temps_ms / distance.
    Returns None if no speed can be computed.
    """
    rkm = _safe_float(rec.get("reduction_km_ms") or rec.get("reduction_km"))
    if rkm is not None and rkm > 0:
        return rkm

    temps = _safe_float(rec.get("temps_ms"))
    dist = _safe_int(rec.get("distance"))
    if temps is not None and dist is not None and dist > 0 and temps > 0:
        return round(temps / dist, 4)

    return None


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseSpeedState:
    """Track rolling speed state for one horse."""

    __slots__ = ("speeds",)

    def __init__(self) -> None:
        self.speeds: deque = deque(maxlen=_SPEED_WINDOW)

    def avg_speed(self) -> Optional[float]:
        """Average of all recorded speeds. None if no history."""
        if not self.speeds:
            return None
        return sum(self.speeds) / len(self.speeds)

    def speed_consistency(self) -> Optional[float]:
        """Std of speeds. None if fewer than 2."""
        if len(self.speeds) < 2:
            return None
        vals = list(self.speeds)
        return _safe_stdev(vals)

    def is_improving(self) -> Optional[int]:
        """1 if last 2 speeds are better (lower) than career average, else 0.

        For reduction_km_ms: lower is faster, so improving means last 2 < avg.
        Returns None if not enough data.
        """
        if len(self.speeds) < 3:
            return None
        avg = sum(self.speeds) / len(self.speeds)
        last_two = list(self.speeds)[-2:]
        # Lower reduction_km_ms = faster, so both must be below average
        if all(s < avg for s in last_two):
            return 1
        return 0


# ===========================================================================
# FEATURE COMPUTATION (field-level, from snapshots before update)
# ===========================================================================


def _compute_field_features(
    horse_names: list[Optional[str]],
    horse_states: dict[str, _HorseSpeedState],
    nombre_partants: Optional[int],
) -> tuple[
    list[dict[str, Any]],           # per-runner features list
    list[Optional[float]],          # per-runner avg speeds (for update reference)
]:
    """Compute all 10 field pace projection features for one course.

    Parameters
    ----------
    horse_names : list of horse identifiers (one per runner in course)
    horse_states : global dict of horse speed states
    nombre_partants : field size

    Returns
    -------
    per_runner_features : list of feature dicts, one per runner
    avg_speeds : list of avg speeds (or None) per runner
    """
    n_runners = len(horse_names)
    np_val = nombre_partants if nombre_partants and nombre_partants > 0 else n_runners

    # -- Collect each runner's avg speed from state snapshot --
    avg_speeds: list[Optional[float]] = []
    for name in horse_names:
        if name and name in horse_states:
            avg_speeds.append(horse_states[name].avg_speed())
        else:
            avg_speeds.append(None)

    # -- Field-level aggregates from available avg speeds --
    valid_speeds = [s for s in avg_speeds if s is not None]

    if valid_speeds:
        field_avg = sum(valid_speeds) / len(valid_speeds)
        field_spread = _safe_stdev(valid_speeds)
        median_speed = sorted(valid_speeds)[len(valid_speeds) // 2]
        nb_fast = sum(1 for s in valid_speeds if s < median_speed)
        # For reduction_km_ms: lower = faster, so "fast" means < median
        # Top 25% threshold
        sorted_speeds = sorted(valid_speeds)
        q25_idx = max(0, len(sorted_speeds) // 4 - 1)
        q25_threshold = sorted_speeds[q25_idx] if sorted_speeds else None
    else:
        field_avg = None
        field_spread = None
        median_speed = None
        nb_fast = None
        q25_threshold = None

    # -- Rank runners by avg speed (1 = fastest = lowest reduction_km_ms) --
    # Build (index, speed) pairs for ranked runners
    speed_rank: dict[int, int] = {}
    indexed_speeds = [(idx, s) for idx, s in enumerate(avg_speeds) if s is not None]
    indexed_speeds.sort(key=lambda x: x[1])  # ascending = fastest first
    for rank, (idx, _) in enumerate(indexed_speeds, 1):
        speed_rank[idx] = rank

    # Count runners in top 25%
    top25_count = 0
    if q25_threshold is not None:
        top25_count = sum(1 for s in valid_speeds if s <= q25_threshold)

    # -- Build per-runner feature dicts --
    per_runner: list[dict[str, Any]] = []

    for idx, name in enumerate(horse_names):
        feats: dict[str, Any] = {}

        my_avg = avg_speeds[idx]

        # 1. fpp_horse_avg_speed
        feats["fpp_horse_avg_speed"] = round(my_avg, 4) if my_avg is not None else None

        # 2. fpp_horse_speed_consistency
        if name and name in horse_states:
            feats["fpp_horse_speed_consistency"] = horse_states[name].speed_consistency()
        else:
            feats["fpp_horse_speed_consistency"] = None

        # 3. fpp_field_avg_speed
        feats["fpp_field_avg_speed"] = round(field_avg, 4) if field_avg is not None else None

        # 4. fpp_horse_vs_field_speed
        if my_avg is not None and field_avg is not None:
            feats["fpp_horse_vs_field_speed"] = round(my_avg - field_avg, 4)
        else:
            feats["fpp_horse_vs_field_speed"] = None

        # 5. fpp_field_speed_spread
        feats["fpp_field_speed_spread"] = field_spread

        # 6. fpp_nb_fast_horses
        feats["fpp_nb_fast_horses"] = nb_fast

        # 7. fpp_pace_pressure
        if nb_fast is not None and np_val > 0:
            feats["fpp_pace_pressure"] = round(nb_fast / np_val, 4)
        else:
            feats["fpp_pace_pressure"] = None

        # 8. fpp_horse_early_speed_rank
        feats["fpp_horse_early_speed_rank"] = speed_rank.get(idx)

        # 9. fpp_lone_speed
        if idx in speed_rank and q25_threshold is not None and my_avg is not None:
            in_top25 = my_avg <= q25_threshold
            feats["fpp_lone_speed"] = 1 if (in_top25 and top25_count == 1) else 0
        else:
            feats["fpp_lone_speed"] = None

        # 10. fpp_horse_speed_improving
        if name and name in horse_states:
            feats["fpp_horse_speed_improving"] = horse_states[name].is_improving()
        else:
            feats["fpp_horse_speed_improving"] = None

        per_runner.append(feats)

    return per_runner, avg_speeds


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_field_pace_projection_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build field pace projection features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Field Pace Projection Builder (memory-optimised) ===")
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
    horse_state: dict[str, _HorseSpeedState] = defaultdict(_HorseSpeedState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "fpp_horse_avg_speed",
        "fpp_horse_speed_consistency",
        "fpp_field_avg_speed",
        "fpp_horse_vs_field_speed",
        "fpp_field_speed_spread",
        "fpp_nb_fast_horses",
        "fpp_pace_pressure",
        "fpp_horse_early_speed_rank",
        "fpp_lone_speed",
        "fpp_horse_speed_improving",
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
            course_records = [
                _read_record_at(index[ci][3]) for ci in course_indices
            ]

            # Extract nombre_partants from field
            nombre_partants = None
            for rec in course_records:
                np_val = rec.get("nombre_partants")
                if np_val is not None:
                    try:
                        nombre_partants = int(np_val)
                    except (ValueError, TypeError):
                        pass
                    break
            if nombre_partants is None:
                nombre_partants = len(course_records)

            # Collect horse names for this course
            horse_names: list[Optional[str]] = []
            for rec in course_records:
                horse_names.append(
                    rec.get("nom_cheval") or rec.get("horse_id")
                )

            # -- Snapshot pre-race state and compute features --
            per_runner_feats, _ = _compute_field_features(
                horse_names, horse_state, nombre_partants
            )

            # -- Emit features and prepare deferred updates --
            post_updates: list[tuple[Optional[str], Optional[float]]] = []

            for rec_idx, rec in enumerate(course_records):
                cheval = horse_names[rec_idx]
                feats = per_runner_feats[rec_idx]

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

                # Compute speed for state update
                speed = _compute_speed(rec)
                post_updates.append((cheval, speed))

            # -- Update horse states after race (no leakage) --
            for cheval, speed in post_updates:
                if not cheval:
                    continue
                if speed is not None:
                    horse_state[cheval].speeds.append(speed)
                elif cheval not in horse_state:
                    # Ensure horse exists in state even without speed
                    _ = horse_state[cheval]

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Field pace projection build termine: %d features en %.1fs (chevaux suivis: %d)",
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
        description="Construction des features field pace projection a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/field_pace_projection/)",
    )
    args = parser.parse_args()

    logger = setup_logging("field_pace_projection_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "field_pace_projection.jsonl"
    build_field_pace_projection_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
