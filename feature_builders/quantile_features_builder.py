#!/usr/bin/env python3
"""
feature_builders.quantile_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Quantile-based features for horses computed from historical race data.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant distributional features using
a sorted-list approach (no numpy required).

Temporal integrity: for any partant at date D, only races with date < D
contribute to the quantile computation -- no future leakage.

Produces:
  - quantile_features.jsonl   in output/quantile_features/

Features per partant:
  - position_q10      : 10th percentile of historical positions (best case)
  - position_q50      : median historical position
  - position_q90      : 90th percentile (worst case)
  - earnings_q75      : 75th percentile of earnings per race
  - cote_q25          : 25th percentile of cotes (when horse is well-backed)

Usage:
    python feature_builders/quantile_features_builder.py
    python feature_builders/quantile_features_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "quantile_features"

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# QUANTILE HELPERS (no numpy)
# ===========================================================================


def _quantile(sorted_vals: list[float], q: float) -> float:
    """Compute quantile from a pre-sorted list using linear interpolation.

    Parameters
    ----------
    sorted_vals : list[float]
        Already sorted in ascending order.
    q : float
        Quantile in [0, 1].

    Returns
    -------
    float
        The interpolated quantile value.
    """
    n = len(sorted_vals)
    if n == 0:
        return float("nan")
    if n == 1:
        return sorted_vals[0]
    # Index using the "exclusive" method (like numpy default)
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def _insort(lst: list[float], val: float) -> None:
    """Insert val into a sorted list maintaining sort order (bisect insort)."""
    lo, hi = 0, len(lst)
    while lo < hi:
        mid = (lo + hi) // 2
        if lst[mid] < val:
            lo = mid + 1
        else:
            hi = mid
    lst.insert(lo, val)


# ===========================================================================
# HORSE HISTORY TRACKER
# ===========================================================================


class _HorseHistory:
    """Per-horse accumulator of sorted historical values."""

    __slots__ = ("positions", "earnings", "cotes")

    def __init__(self) -> None:
        self.positions: list[float] = []   # sorted
        self.earnings: list[float] = []    # sorted
        self.cotes: list[float] = []       # sorted


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


def _sort_key(rec: dict) -> tuple:
    """Sort key: date, course_uid, num_pmu for determinism."""
    return (
        rec.get("date_reunion_iso", ""),
        rec.get("course_uid", ""),
        rec.get("num_pmu", 0) or 0,
    )


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_quantile_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build quantile features from partants_master.jsonl.

    Single-pass approach: read minimal fields, sort chronologically,
    then process course-by-course. For each partant, quantiles are
    computed from the horse's history BEFORE the current race.
    After emitting features, update the horse's history with the
    current race results.
    """
    logger.info("=== Quantile Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "position": rec.get("position_arrivee"),
            "gains": rec.get("gains_participant"),
            "cote": rec.get("rapport_pmu", rec.get("cote_probable")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_hist: dict[str, _HorseHistory] = defaultdict(_HorseHistory)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

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

        # -- Emit features from pre-race history (no leakage) --
        for rec in course_group:
            cheval = rec["cheval"]

            if not cheval or cheval not in horse_hist:
                # No history yet
                results.append({
                    "partant_uid": rec["uid"],
                    "position_q10": None,
                    "position_q50": None,
                    "position_q90": None,
                    "earnings_q75": None,
                    "cote_q25": None,
                })
                continue

            hist = horse_hist[cheval]

            pos_q10 = (
                round(_quantile(hist.positions, 0.10), 2)
                if hist.positions else None
            )
            pos_q50 = (
                round(_quantile(hist.positions, 0.50), 2)
                if hist.positions else None
            )
            pos_q90 = (
                round(_quantile(hist.positions, 0.90), 2)
                if hist.positions else None
            )
            earn_q75 = (
                round(_quantile(hist.earnings, 0.75), 2)
                if hist.earnings else None
            )
            cote_q25 = (
                round(_quantile(hist.cotes, 0.25), 2)
                if hist.cotes else None
            )

            results.append({
                "partant_uid": rec["uid"],
                "position_q10": pos_q10,
                "position_q50": pos_q50,
                "position_q90": pos_q90,
                "earnings_q75": earn_q75,
                "cote_q25": cote_q25,
            })

        # -- Update histories AFTER emitting (temporal integrity) --
        for rec in course_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            hist = horse_hist[cheval]

            pos = rec["position"]
            if pos is not None:
                try:
                    pos_f = float(pos)
                    if pos_f > 0:
                        _insort(hist.positions, pos_f)
                except (ValueError, TypeError):
                    pass

            gains = rec["gains"]
            if gains is not None:
                try:
                    gains_f = float(gains)
                    _insort(hist.earnings, gains_f)
                except (ValueError, TypeError):
                    pass

            cote = rec["cote"]
            if cote is not None:
                try:
                    cote_f = float(cote)
                    if cote_f > 0:
                        _insort(hist.cotes, cote_f)
                except (ValueError, TypeError):
                    pass

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Quantile build termine: %d features en %.1fs (chevaux suivis: %d)",
        len(results), elapsed, len(horse_hist),
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
        description="Construction des features quantiles a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/quantile_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("quantile_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_quantile_features(input_path, logger)

    # Save
    out_path = output_dir / "quantile_features.jsonl"
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
