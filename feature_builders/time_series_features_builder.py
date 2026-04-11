#!/usr/bin/env python3
"""
feature_builders.time_series_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Time-series statistical features -- autocorrelation, stationarity indicators,
and temporal patterns designed for LSTM/GRU/TFT sequence models.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the time-series statistics -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     stream output directly to disk.

Produces:
  - time_series_features.jsonl  in builder_outputs/time_series_features/

Features per partant (10):
  - tsf_position_autocorr_lag1 : autocorrelation of positions at lag 1
  - tsf_position_autocorr_lag2 : autocorrelation of positions at lag 2
  - tsf_cote_autocorr_lag1     : autocorrelation of odds at lag 1
  - tsf_position_runs_test     : number of runs in position sequence
  - tsf_mean_reversion_speed   : recent deviation / past deviation ratio
  - tsf_position_diff_1        : first difference (position_n - position_n-1)
  - tsf_position_diff_2        : second difference (acceleration)
  - tsf_seasonal_component     : horse performance in this month minus overall avg
  - tsf_trend_component        : linear trend over last 10 races
  - tsf_residual_volatility    : std of residuals after removing trend

Usage:
    python feature_builders/time_series_features_builder.py
    python feature_builders/time_series_features_builder.py --input path/to/partants_master.jsonl
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
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/time_series_features")

_LOG_EVERY = 500_000
_DEQUE_MAX = 15


# ===========================================================================
# HELPERS
# ===========================================================================


def _autocorrelation(values: list[float], lag: int) -> Optional[float]:
    """Compute autocorrelation at the given lag.

    corr(x[:-lag], x[lag:]) using Pearson formula.
    Returns None if fewer than lag + 3 values.
    """
    n = len(values)
    if n < lag + 3:
        return None

    x = values[:-lag]
    y = values[lag:]
    k = len(x)

    mx = sum(x) / k
    my = sum(y) / k

    num = 0.0
    dx2 = 0.0
    dy2 = 0.0
    for i in range(k):
        dx = x[i] - mx
        dy = y[i] - my
        num += dx * dy
        dx2 += dx * dx
        dy2 += dy * dy

    den = math.sqrt(dx2 * dy2)
    if den == 0:
        return None
    return round(num / den, 6)


def _count_runs(values: list[float]) -> Optional[int]:
    """Count the number of 'runs' in a sequence.

    A run is a maximal consecutive subsequence of values all above or all
    below the median.  Alternating good/bad = many runs = random;
    long streaks = few runs = persistent form.
    Returns None if fewer than 3 values.
    """
    n = len(values)
    if n < 3:
        return None

    sorted_vals = sorted(values)
    median = sorted_vals[n // 2]

    runs = 1
    above = values[0] >= median
    for i in range(1, n):
        current_above = values[i] >= median
        if current_above != above:
            runs += 1
            above = current_above

    return runs


def _slope_and_residuals(values: list[float]) -> tuple[Optional[float], Optional[float]]:
    """Linear regression slope + std of residuals.

    Returns (slope, residual_std). Both None if fewer than 3 values.
    """
    n = len(values)
    if n < 3:
        return None, None

    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n

    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx

    if den == 0:
        return None, None

    slope = num / den
    intercept = y_mean - slope * x_mean

    # Residuals: y_i - (slope * i + intercept)
    residuals = []
    for i, y in enumerate(values):
        predicted = slope * i + intercept
        residuals.append(y - predicted)

    # Population std of residuals
    r_mean = sum(residuals) / n
    var = sum((r - r_mean) ** 2 for r in residuals) / n
    residual_std = round(math.sqrt(var), 4) if var >= 0 else None

    return round(slope, 6), residual_std


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseState:
    """Track rolling time-series state for one horse."""

    __slots__ = ("positions", "cotes", "month_positions")

    def __init__(self) -> None:
        self.positions: deque = deque(maxlen=_DEQUE_MAX)
        self.cotes: deque = deque(maxlen=_DEQUE_MAX)
        # month -> list of positions (uncapped, but bounded by career length)
        self.month_positions: dict[int, list[float]] = defaultdict(list)


# ===========================================================================
# FEATURE COMPUTATION (from snapshot before update)
# ===========================================================================


def _compute_features(
    hs: _HorseState,
    current_month: Optional[int],
) -> dict[str, Any]:
    """Compute all 10 time-series features from the horse's pre-race state."""
    feats: dict[str, Any] = {}
    pos_list = [v for v in hs.positions if v is not None]
    cote_list = [v for v in hs.cotes if v is not None]

    # 1. tsf_position_autocorr_lag1
    feats["tsf_position_autocorr_lag1"] = _autocorrelation(pos_list, 1)

    # 2. tsf_position_autocorr_lag2
    feats["tsf_position_autocorr_lag2"] = _autocorrelation(pos_list, 2)

    # 3. tsf_cote_autocorr_lag1
    feats["tsf_cote_autocorr_lag1"] = _autocorrelation(cote_list, 1)

    # 4. tsf_position_runs_test
    feats["tsf_position_runs_test"] = _count_runs(pos_list)

    # 5. tsf_mean_reversion_speed
    # Ratio of recent deviation to past deviation relative to career mean.
    # Split positions into two halves; if recent half is closer to mean,
    # reversion is fast (ratio < 1).
    if len(pos_list) >= 6:
        overall_mean = sum(pos_list) / len(pos_list)
        mid = len(pos_list) // 2
        past_vals = pos_list[:mid]
        recent_vals = pos_list[mid:]
        past_dev = sum(abs(v - overall_mean) for v in past_vals) / len(past_vals)
        recent_dev = sum(abs(v - overall_mean) for v in recent_vals) / len(recent_vals)
        if past_dev > 0:
            feats["tsf_mean_reversion_speed"] = round(recent_dev / past_dev, 4)
        else:
            feats["tsf_mean_reversion_speed"] = None
    else:
        feats["tsf_mean_reversion_speed"] = None

    # 6. tsf_position_diff_1: first difference (last position - previous)
    if len(pos_list) >= 2:
        feats["tsf_position_diff_1"] = round(pos_list[-1] - pos_list[-2], 4)
    else:
        feats["tsf_position_diff_1"] = None

    # 7. tsf_position_diff_2: second difference (acceleration)
    if len(pos_list) >= 3:
        d1_last = pos_list[-1] - pos_list[-2]
        d1_prev = pos_list[-2] - pos_list[-3]
        feats["tsf_position_diff_2"] = round(d1_last - d1_prev, 4)
    else:
        feats["tsf_position_diff_2"] = None

    # 8. tsf_seasonal_component: horse's avg position in this month minus overall avg
    if current_month is not None and current_month in hs.month_positions:
        month_vals = hs.month_positions[current_month]
        if month_vals and pos_list:
            month_avg = sum(month_vals) / len(month_vals)
            overall_avg = sum(pos_list) / len(pos_list)
            feats["tsf_seasonal_component"] = round(month_avg - overall_avg, 4)
        else:
            feats["tsf_seasonal_component"] = None
    else:
        feats["tsf_seasonal_component"] = None

    # 9. tsf_trend_component: linear trend over last 10 races
    last10 = pos_list[-10:] if len(pos_list) >= 3 else pos_list
    slope, residual_std = _slope_and_residuals(last10)
    feats["tsf_trend_component"] = slope

    # 10. tsf_residual_volatility: std of residuals after removing trend
    feats["tsf_residual_volatility"] = residual_std

    return feats


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_time_series_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build time-series features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Time Series Features Builder (memory-optimised) ===")
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
        "tsf_position_autocorr_lag1",
        "tsf_position_autocorr_lag2",
        "tsf_cote_autocorr_lag1",
        "tsf_position_runs_test",
        "tsf_mean_reversion_speed",
        "tsf_position_diff_1",
        "tsf_position_diff_2",
        "tsf_seasonal_component",
        "tsf_trend_component",
        "tsf_residual_volatility",
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

            # Parse month for seasonal component
            current_month: Optional[int] = None
            if course_date_str and len(course_date_str) >= 7:
                try:
                    current_month = int(course_date_str[5:7])
                except (ValueError, IndexError):
                    pass

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                cheval = rec.get("nom_cheval")
                if not cheval:
                    # Emit record with Nones
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

                hs = horse_state[cheval]

                # Compute features from pre-race state
                feats = _compute_features(hs, current_month)

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
                position: Optional[float] = None
                pos_raw = rec.get("place_arrivee") or rec.get("position_arrivee")
                if pos_raw is not None:
                    try:
                        position = float(pos_raw)
                    except (ValueError, TypeError):
                        pass

                cote: Optional[float] = None
                cote_raw = rec.get("cote_finale") or rec.get("rapport_final")
                if cote_raw is not None:
                    try:
                        cote = float(cote_raw)
                    except (ValueError, TypeError):
                        pass

                post_updates.append((cheval, position, cote, current_month))

            # -- Update horse states after race (no leakage) --
            for cheval, position, cote, month in post_updates:
                hs = horse_state[cheval]
                hs.positions.append(position)
                hs.cotes.append(cote)
                if position is not None and month is not None:
                    hs.month_positions[month].append(position)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Time series features build termine: %d features en %.1fs (chevaux suivis: %d)",
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
        description="Construction des features time-series a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/time_series_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("time_series_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "time_series_features.jsonl"
    build_time_series_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
