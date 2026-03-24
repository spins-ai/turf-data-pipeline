#!/usr/bin/env python3
"""
feature_builders.momentum_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
5 features capturing momentum and form trajectory of a horse.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant momentum features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the momentum calculation -- no future leakage.

Produces:
  - momentum_features.jsonl   in output/momentum_features/

Features per partant:
  - momentum_3          : average of last 3 position ranks (lower = better)
  - momentum_5          : average of last 5 position ranks
  - momentum_trend      : linear regression slope of last 5 positions (negative = improving)
  - regression_to_mean  : how far current form is from career average (positive = above avg)
  - form_volatility     : standard deviation of last 5 positions

Usage:
    python feature_builders/momentum_builder.py
    python feature_builders/momentum_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "momentum_features"

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# MATH HELPERS
# ===========================================================================


def _mean(values: list[float]) -> Optional[float]:
    """Return mean or None if empty."""
    if not values:
        return None
    return sum(values) / len(values)


def _stddev(values: list[float]) -> Optional[float]:
    """Population standard deviation, or None if fewer than 2 values."""
    if len(values) < 2:
        return None
    m = sum(values) / len(values)
    variance = sum((x - m) ** 2 for x in values) / len(values)
    return math.sqrt(variance)


def _linreg_slope(values: list[float]) -> Optional[float]:
    """Slope of OLS linear regression y = a + b*x over indices 0..n-1.

    Negative slope means positions are decreasing (= improving form).
    Returns None if fewer than 3 data points.
    """
    n = len(values)
    if n < 3:
        return None
    # x = 0, 1, ..., n-1
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if den == 0:
        return 0.0
    return num / den


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
# PER-HORSE STATE
# ===========================================================================


class _HorsePositions:
    """Lightweight per-horse position history accumulator."""

    __slots__ = ("positions",)

    def __init__(self) -> None:
        self.positions: list[float] = []

    @property
    def career_mean(self) -> Optional[float]:
        return _mean(self.positions) if self.positions else None

    def last_n(self, n: int) -> list[float]:
        """Return the last n positions (chronological order)."""
        return self.positions[-n:] if len(self.positions) >= n else list(self.positions)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_momentum_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build momentum features from partants_master.jsonl.

    Single-pass approach: read minimal fields, sort chronologically,
    then process sequentially accumulating per-horse positions.
    """
    logger.info("=== Momentum Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Parse position -- only keep valid numeric finishes
        pos_raw = rec.get("position_arrivee")
        pos = None
        if pos_raw is not None:
            try:
                pos = float(pos_raw)
                if pos <= 0 or pos > 50:
                    pos = None  # discard invalid positions
            except (TypeError, ValueError):
                pos = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "horse_id": rec.get("horse_id"),
            "position": pos,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process sequentially --
    t2 = time.time()
    horse_state: dict[str, _HorsePositions] = defaultdict(_HorsePositions)
    results: list[dict[str, Any]] = []
    n_enriched = 0

    for idx, rec in enumerate(slim_records):
        cheval = rec["horse_id"] or rec["cheval"]

        if not cheval:
            results.append({
                "partant_uid": rec["uid"],
                "momentum_3": None,
                "momentum_5": None,
                "momentum_trend": None,
                "regression_to_mean": None,
                "form_volatility": None,
            })
            continue

        state = horse_state[cheval]

        # Compute features from pre-race history (no leakage)
        if len(state.positions) >= 3:
            n_enriched += 1
            last_3 = state.last_n(3)
            last_5 = state.last_n(5)

            momentum_3 = round(_mean(last_3), 3) if last_3 else None
            momentum_5 = round(_mean(last_5), 3) if len(last_5) >= 3 else None

            # Linear regression slope over last 5 (or all available if 3-4)
            trend_data = last_5 if len(last_5) >= 3 else None
            momentum_trend = round(_linreg_slope(trend_data), 4) if trend_data else None

            # Regression to mean: career_avg - recent_avg
            # Positive = recent form worse than career (likely to improve)
            # But per spec: positive = above average, likely to regress
            # So: recent_avg - career_avg (lower position = better, so invert)
            career_avg = state.career_mean
            recent_avg = _mean(last_5)
            if career_avg is not None and recent_avg is not None:
                # Negative value means recent positions are lower (better) than career
                # Positive means recent positions are higher (worse) than career
                regression_to_mean = round(career_avg - recent_avg, 3)
            else:
                regression_to_mean = None

            # Volatility = stddev of last 5 positions
            form_volatility = _stddev(last_5)
            if form_volatility is not None:
                form_volatility = round(form_volatility, 3)
        else:
            momentum_3 = None
            momentum_5 = None
            momentum_trend = None
            regression_to_mean = None
            form_volatility = None

        results.append({
            "partant_uid": rec["uid"],
            "momentum_3": momentum_3,
            "momentum_5": momentum_5,
            "momentum_trend": momentum_trend,
            "regression_to_mean": regression_to_mean,
            "form_volatility": form_volatility,
        })

        # -- Update state after emitting features (no leakage) --
        if rec["position"] is not None:
            state.positions.append(rec["position"])

        if (idx + 1) % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", idx + 1, len(slim_records))

    elapsed = time.time() - t0
    logger.info(
        "Momentum build termine: %d features en %.1fs (chevaux: %d, enrichis: %d)",
        len(results), elapsed, len(horse_state), n_enriched,
    )
    return results


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
        description="Construction des features momentum a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/momentum_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("momentum_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_momentum_features(input_path, logger)

    # Save
    out_path = output_dir / "momentum_features.jsonl"
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
