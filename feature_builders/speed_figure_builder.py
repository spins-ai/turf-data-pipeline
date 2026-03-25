#!/usr/bin/env python3
"""
feature_builders.speed_figure_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Standardized speed ratings per horse based on race times and conditions.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant speed figure features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the speed figures -- no future leakage.

Produces:
  - speed_figures.jsonl   in output/speed_figures/

Features per partant (7):
  - speed_figure          : standardized speed rating for this run (0-200 scale,
                            100 = average, each point ~ 0.1s per km).
                            Based on reduction_km_ms normalized by
                            hippo+distance+terrain average.
  - speed_figure_best     : best speed figure in horse's career (before this race)
  - speed_figure_avg      : average of last 5 speed figures
  - speed_figure_trend    : linear regression slope of last 5 speed figures
                            (positive = improving)
  - speed_figure_rank     : rank of this horse's best figure among the field
  - speed_vs_class        : speed_figure_avg / average speed at this allocation
                            level (class indicator)
  - speed_consistency     : standard deviation of last 5 speed figures
                            (low = consistent)

Usage:
    python feature_builders/speed_figure_builder.py
    python feature_builders/speed_figure_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "speed_figures"

# Scale parameters: 100 = average, each point ~ 0.1s per km
# reduction_km_ms is ms/km; lower = faster.  We invert so higher figure = faster.
SCALE_CENTER = 100.0
# 1 point = 0.1s/km = 100 ms/km
MS_PER_POINT = 100.0

# Rolling window for recent figures
WINDOW = 5

# Minimum sample size for a condition bucket to be usable
MIN_BUCKET_SIZE = 10

# Progress log every N records
_LOG_EVERY = 500_000


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
# HELPERS
# ===========================================================================


def _linreg_slope(values: list[float]) -> Optional[float]:
    """Compute slope of simple linear regression over indexed values.

    Returns None if fewer than 2 values.
    """
    n = len(values)
    if n < 2:
        return None
    # x = 0, 1, 2, ..., n-1
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if den == 0.0:
        return 0.0
    return num / den


def _safe_stdev(values: list[float]) -> Optional[float]:
    """Standard deviation of values, None if fewer than 2."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return var ** 0.5


# ===========================================================================
# CONDITION BUCKET TRACKER
# ===========================================================================


class _ConditionAverages:
    """Accumulates running averages of reduction_km_ms per condition bucket.

    Bucket key = (hippodrome, distance_rounded, type_piste).
    Distance is rounded to nearest 100m to group similar distances.

    Also tracks per-allocation-level averages for speed_vs_class.
    """

    __slots__ = ("_sums", "_counts", "_alloc_sums", "_alloc_counts", "_global_sum", "_global_count")

    def __init__(self) -> None:
        self._sums: dict[tuple, float] = defaultdict(float)
        self._counts: dict[tuple, int] = defaultdict(int)
        self._alloc_sums: dict[str, float] = defaultdict(float)
        self._alloc_counts: dict[str, int] = defaultdict(int)
        self._global_sum: float = 0.0
        self._global_count: int = 0

    def get_condition_avg(self, hippo: str, distance: int, terrain: str) -> Optional[float]:
        """Return current average for this condition bucket, or global fallback."""
        key = (hippo, distance, terrain)
        if self._counts[key] >= MIN_BUCKET_SIZE:
            return self._sums[key] / self._counts[key]
        # Fallback: hippo + distance only
        key_hd = (hippo, distance, "")
        if self._counts[key_hd] >= MIN_BUCKET_SIZE:
            return self._sums[key_hd] / self._counts[key_hd]
        # Fallback: global average
        if self._global_count >= MIN_BUCKET_SIZE:
            return self._global_sum / self._global_count
        return None

    def get_alloc_avg(self, alloc_bucket: str) -> Optional[float]:
        """Return average speed figure for this allocation bucket."""
        if self._alloc_counts[alloc_bucket] >= MIN_BUCKET_SIZE:
            return self._alloc_sums[alloc_bucket] / self._alloc_counts[alloc_bucket]
        return None

    def update(self, hippo: str, distance: int, terrain: str, red_km_ms: float) -> None:
        """Record a new observation after the race is processed."""
        key = (hippo, distance, terrain)
        self._sums[key] += red_km_ms
        self._counts[key] += 1
        # Also update hippo+distance-only bucket for fallback
        key_hd = (hippo, distance, "")
        self._sums[key_hd] += red_km_ms
        self._counts[key_hd] += 1
        # Global
        self._global_sum += red_km_ms
        self._global_count += 1

    def update_alloc(self, alloc_bucket: str, speed_fig: float) -> None:
        """Record a speed figure for an allocation bucket."""
        self._alloc_sums[alloc_bucket] += speed_fig
        self._alloc_counts[alloc_bucket] += 1


# ===========================================================================
# HORSE HISTORY TRACKER
# ===========================================================================


class _HorseHistory:
    """Lightweight per-horse speed figure history (FIFO of last N figures)."""

    __slots__ = ("figures",)

    def __init__(self) -> None:
        self.figures: list[float] = []

    def add(self, fig: float) -> None:
        self.figures.append(fig)

    def last_n(self, n: int = WINDOW) -> list[float]:
        return self.figures[-n:]

    def best(self) -> Optional[float]:
        return max(self.figures) if self.figures else None


# ===========================================================================
# ALLOCATION BUCKETING
# ===========================================================================


def _alloc_bucket(alloc: Any) -> Optional[str]:
    """Bucket allocation_totale into class categories."""
    try:
        val = float(alloc)
    except (TypeError, ValueError):
        return None
    if val <= 10000:
        return "low"
    if val <= 30000:
        return "mid_low"
    if val <= 60000:
        return "mid"
    if val <= 100000:
        return "mid_high"
    return "high"


def _round_distance(dist: Any) -> Optional[int]:
    """Round distance to nearest 100m."""
    try:
        d = int(dist)
    except (TypeError, ValueError):
        return None
    return round(d / 100) * 100


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_speed_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build speed figure features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory.
      2. Sort chronologically.
      3. Process course-by-course, computing speed figures with strict
         temporal integrity.

    Memory budget:
      - Slim records: ~16M records * ~250 bytes ~ 4 GB
      - Condition averages: bounded dict ~ 50 MB
      - Horse histories: ~200K horses * ~200 bytes ~ 40 MB
    """
    logger.info("=== Speed Figure Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # reduction_km_ms: use explicit field or estimate from temps_ms / distance
        red_km_ms = rec.get("reduction_km_ms")
        if red_km_ms is None:
            temps_ms = rec.get("temps_ms")
            dist_raw = rec.get("distance")
            if temps_ms is not None and dist_raw is not None:
                try:
                    t_ms = float(temps_ms)
                    d_m = float(dist_raw)
                    if t_ms > 0 and d_m > 0:
                        # reduction_km_ms = time_ms / distance_km
                        red_km_ms = t_ms / (d_m / 1000.0)
                except (TypeError, ValueError):
                    pass

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "red_km_ms": red_km_ms,
            "hippo": rec.get("hippodrome_normalise", ""),
            "distance": rec.get("distance"),
            "terrain": rec.get("type_piste", ""),
            "alloc": rec.get("allocation_totale"),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    cond_avgs = _ConditionAverages()
    horse_hist: dict[str, _HorseHistory] = defaultdict(_HorseHistory)

    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    i = 0
    while i < total:
        # Collect all partants for this course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # -- Compute pre-race features for each partant (temporal integrity) --
        pre_race_data: list[dict[str, Any]] = []

        for rec in course_group:
            cheval = rec["cheval"]
            red_km_ms = rec["red_km_ms"]
            hippo = rec["hippo"] or ""
            dist_raw = rec["distance"]
            terrain = rec["terrain"] or ""
            alloc = rec["alloc"]
            dist_rounded = _round_distance(dist_raw)

            feat: dict[str, Any] = {"partant_uid": rec["uid"]}

            # -- speed_figure for this run --
            speed_fig: Optional[float] = None
            if red_km_ms is not None and dist_rounded is not None:
                try:
                    red_val = float(red_km_ms)
                except (TypeError, ValueError):
                    red_val = None  # type: ignore[assignment]

                if red_val is not None and red_val > 0:
                    cond_avg = cond_avgs.get_condition_avg(hippo, dist_rounded, terrain)
                    if cond_avg is not None and cond_avg > 0:
                        # Lower reduction = faster. Invert difference so higher fig = faster.
                        diff_ms = cond_avg - red_val
                        speed_fig = round(SCALE_CENTER + diff_ms / MS_PER_POINT, 2)
                    else:
                        # No condition average yet; assign baseline 100
                        speed_fig = SCALE_CENTER

            feat["speed_figure"] = speed_fig

            # -- Historical features (pre-race snapshot, strict temporal) --
            hist = horse_hist.get(cheval) if cheval else None
            past_figs = hist.last_n(WINDOW) if hist else []

            feat["speed_figure_best"] = round(hist.best(), 2) if (hist and hist.best() is not None) else None
            feat["speed_figure_avg"] = (
                round(sum(past_figs) / len(past_figs), 2) if past_figs else None
            )
            feat["speed_figure_trend"] = (
                round(_linreg_slope(past_figs), 4) if _linreg_slope(past_figs) is not None else None
            )
            feat["speed_consistency"] = (
                round(_safe_stdev(past_figs), 2) if _safe_stdev(past_figs) is not None else None
            )

            # -- speed_vs_class --
            ab = _alloc_bucket(alloc)
            if feat["speed_figure_avg"] is not None and ab is not None:
                alloc_avg = cond_avgs.get_alloc_avg(ab)
                if alloc_avg is not None and alloc_avg > 0:
                    feat["speed_vs_class"] = round(feat["speed_figure_avg"] / alloc_avg, 4)
                else:
                    feat["speed_vs_class"] = None
            else:
                feat["speed_vs_class"] = None

            pre_race_data.append({
                "rec": rec,
                "feat": feat,
                "speed_fig": speed_fig,
                "best": hist.best() if hist else None,
            })

        # -- speed_figure_rank: rank horses by best figure among field --
        ranked = [
            (idx, d["best"])
            for idx, d in enumerate(pre_race_data)
            if d["best"] is not None
        ]
        if ranked:
            ranked.sort(key=lambda x: x[1], reverse=True)  # highest first
            for rank, (idx, _) in enumerate(ranked, start=1):
                pre_race_data[idx]["feat"]["speed_figure_rank"] = rank
        # Fill None for horses with no history
        for d in pre_race_data:
            if "speed_figure_rank" not in d["feat"]:
                d["feat"]["speed_figure_rank"] = None

        # -- Emit features --
        for d in pre_race_data:
            results.append(d["feat"])

        # -- Post-race update: add figures to histories and condition averages --
        for d in pre_race_data:
            rec = d["rec"]
            cheval = rec["cheval"]
            sf = d["speed_fig"]
            hippo = rec["hippo"] or ""
            dist_rounded = _round_distance(rec["distance"])
            terrain = rec["terrain"] or ""
            alloc = rec["alloc"]

            if sf is not None and cheval:
                horse_hist[cheval].add(sf)

            # Update condition averages with actual reduction time
            red_km_ms = rec["red_km_ms"]
            if red_km_ms is not None and dist_rounded is not None:
                try:
                    red_val = float(red_km_ms)
                except (TypeError, ValueError):
                    red_val = None  # type: ignore[assignment]
                if red_val is not None and red_val > 0:
                    cond_avgs.update(hippo, dist_rounded, terrain, red_val)

            # Update allocation-level speed averages
            ab = _alloc_bucket(alloc)
            if sf is not None and ab is not None:
                cond_avgs.update_alloc(ab, sf)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Speed figure build termine: %d features en %.1fs (chevaux: %d, buckets conditions: %d)",
        len(results), elapsed,
        len(horse_hist), len(cond_avgs._counts),
    )

    return results


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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features speed figure a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/speed_figures/)",
    )
    args = parser.parse_args()

    logger = setup_logging("speed_figure_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_speed_features(input_path, logger)

    # Save
    out_path = output_dir / "speed_figures.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
