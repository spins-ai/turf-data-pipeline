#!/usr/bin/env python3
"""
feature_builders.prediction_calibration_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Calibration-related features useful for probability calibration in ML models.

Reads partants_master.jsonl in streaming mode, computes per-partant calibration
signals derived from market odds and historical outcomes at similar odds buckets.

Temporal integrity: for any partant at date D, only races with date < D
contribute to historical win-rate statistics (pc_historical_win_at_odds,
pc_calibration_error). Course-level features (normalized probs, overround)
are computed within the race field itself and thus contain no future leakage.

Produces:
  - prediction_calibration.jsonl   in builder_outputs/prediction_calibration/

Features per partant (8):
  - pc_implied_prob              : 1 / cote_finale
  - pc_implied_prob_normalized   : implied_prob / sum(all 1/cote in race)
  - pc_historical_win_at_odds    : historical win rate for this odds bucket
  - pc_odds_bucket               : categorical bucket for cote_finale
                                   (1-2, 2-4, 4-8, 8-15, 15-30, 30+)
  - pc_market_overround          : sum of 1/cote for all runners in the race
  - pc_calibration_error         : abs(pc_implied_prob_normalized -
                                       pc_historical_win_at_odds)
  - pc_sharp_vs_public           : 1 if cote_reference < cote_finale
                                   (sharp money backed this horse)
  - pc_odds_movement_dir         : sign(cote_finale - cote_reference)
                                   encoded as -1, 0, or +1

Usage:
    python feature_builders/prediction_calibration_builder.py
    python feature_builders/prediction_calibration_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/prediction_calibration_builder.py --output-dir /path/to/output
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/prediction_calibration"
)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_FALLBACK_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# Minimum historical samples before trusting a bucket's win rate
_MIN_BUCKET_SAMPLES = 10


# ===========================================================================
# ODDS BUCKET HELPERS
# ===========================================================================

# Bucket boundaries: (lower_inclusive, upper_exclusive, label)
_BUCKET_DEFS: list[tuple[float, float, str]] = [
    (1.0, 2.0, "1-2"),
    (2.0, 4.0, "2-4"),
    (4.0, 8.0, "4-8"),
    (8.0, 15.0, "8-15"),
    (15.0, 30.0, "15-30"),
    (30.0, float("inf"), "30+"),
]


def _odds_bucket(cote: float) -> str:
    """Return the string bucket label for a given cote_finale value."""
    for lo, hi, label in _BUCKET_DEFS:
        if lo <= cote < hi:
            return label
    return "30+"


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to a positive finite float, or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (ValueError, TypeError):
        return None


def _sign(x: float) -> int:
    """Return -1, 0, or +1."""
    if x < 0:
        return -1
    if x > 0:
        return 1
    return 0


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file line by line (streaming)."""
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
# MAIN BUILD
# ===========================================================================


def build_prediction_calibration_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build prediction calibration features from partants_master.jsonl.

    Three-phase approach:
      1. Read minimal fields into memory.
      2. Sort chronologically (date, course_uid, num_pmu).
      3. Process date by date:
           a. Group each date's records by course.
           b. Compute course-level aggregates (implied probs, overround).
           c. Emit features using historical bucket stats from BEFORE this date.
           d. Update bucket stats with this date's outcomes.
    """
    logger.info("=== Prediction Calibration Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read slim records --
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
            "cote_fin": _safe_float(rec.get("cote_finale")),
            "cote_ref": _safe_float(rec.get("cote_reference")),
            "position": rec.get("position_arrivee"),
            "gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process date by date --
    t2 = time.time()

    # Historical win-rate accumulator per odds bucket
    # bucket_label -> {"wins": int, "total": int}
    bucket_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"wins": 0, "total": 0}
    )

    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        current_date = slim_records[i]["date"]
        date_group: list[dict] = []

        while i < total and slim_records[i]["date"] == current_date:
            date_group.append(slim_records[i])
            i += 1

        # Group by course for field-level features
        courses: dict[str, list[dict]] = defaultdict(list)
        for rec in date_group:
            courses[rec["course"]].append(rec)

        # ---- Emit features (using bucket_stats from BEFORE this date) ----
        for course_uid, field in courses.items():
            # Compute race-level sum of implied probabilities (for overround)
            implied_probs: list[Optional[float]] = []
            overround_sum = 0.0
            for rec in field:
                cf = rec["cote_fin"]
                if cf is not None:
                    ip = 1.0 / cf
                    implied_probs.append(ip)
                    overround_sum += ip
                else:
                    implied_probs.append(None)

            pc_market_overround = round(overround_sum, 6) if overround_sum > 0 else None

            for idx, rec in enumerate(field):
                uid = rec["uid"]
                cote_fin = rec["cote_fin"]
                cote_ref = rec["cote_ref"]
                ip = implied_probs[idx]

                # pc_implied_prob
                pc_implied_prob = round(ip, 6) if ip is not None else None

                # pc_implied_prob_normalized
                pc_implied_prob_normalized: Optional[float]
                if ip is not None and overround_sum > 0:
                    pc_implied_prob_normalized = round(ip / overround_sum, 6)
                else:
                    pc_implied_prob_normalized = None

                # pc_odds_bucket
                pc_odds_bucket: Optional[str]
                if cote_fin is not None:
                    pc_odds_bucket = _odds_bucket(cote_fin)
                else:
                    pc_odds_bucket = None

                # pc_historical_win_at_odds (strictly past data)
                pc_historical_win_at_odds: Optional[float]
                if pc_odds_bucket is not None:
                    bstats = bucket_stats.get(pc_odds_bucket)
                    if bstats is not None and bstats["total"] >= _MIN_BUCKET_SAMPLES:
                        pc_historical_win_at_odds = round(
                            bstats["wins"] / bstats["total"], 6
                        )
                    else:
                        pc_historical_win_at_odds = None
                else:
                    pc_historical_win_at_odds = None

                # pc_calibration_error
                pc_calibration_error: Optional[float]
                if (
                    pc_implied_prob_normalized is not None
                    and pc_historical_win_at_odds is not None
                ):
                    pc_calibration_error = round(
                        abs(pc_implied_prob_normalized - pc_historical_win_at_odds), 6
                    )
                else:
                    pc_calibration_error = None

                # pc_sharp_vs_public: 1 if sharp money backed (cote_ref < cote_fin)
                pc_sharp_vs_public: Optional[int]
                if cote_ref is not None and cote_fin is not None:
                    pc_sharp_vs_public = 1 if cote_ref < cote_fin else 0
                else:
                    pc_sharp_vs_public = None

                # pc_odds_movement_dir: sign(cote_fin - cote_ref)
                pc_odds_movement_dir: Optional[int]
                if cote_ref is not None and cote_fin is not None:
                    pc_odds_movement_dir = _sign(cote_fin - cote_ref)
                else:
                    pc_odds_movement_dir = None

                results.append(
                    {
                        "partant_uid": uid,
                        "pc_implied_prob": pc_implied_prob,
                        "pc_implied_prob_normalized": pc_implied_prob_normalized,
                        "pc_historical_win_at_odds": pc_historical_win_at_odds,
                        "pc_odds_bucket": pc_odds_bucket,
                        "pc_market_overround": pc_market_overround,
                        "pc_calibration_error": pc_calibration_error,
                        "pc_sharp_vs_public": pc_sharp_vs_public,
                        "pc_odds_movement_dir": pc_odds_movement_dir,
                    }
                )

        # ---- Update bucket stats with this date's outcomes ----
        for rec in date_group:
            cf = rec["cote_fin"]
            if cf is None:
                continue
            bucket = _odds_bucket(cf)
            bucket_stats[bucket]["total"] += 1
            if rec["gagnant"]:
                bucket_stats[bucket]["wins"] += 1

        n_processed += len(date_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Calibration build termine: %d features en %.1fs (buckets: %d)",
        len(results),
        elapsed,
        len(bucket_stats),
    )

    # Log bucket stats for reference
    for bucket_label, stats in sorted(bucket_stats.items()):
        win_rate = (
            stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
        )
        logger.info(
            "  Bucket %s: %d courses, %.1f%% victoires",
            bucket_label,
            stats["total"],
            100.0 * win_rate,
        )

    gc.collect()
    return results


# ===========================================================================
# INPUT RESOLUTION & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file, checking CLI arg then canonical path then fallbacks."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    for candidate in _FALLBACK_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve. "
        f"Attendu: {INPUT_PARTANTS} ou l'un de {[str(c) for c in _FALLBACK_CANDIDATES]}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features de calibration de prediction "
            "a partir de partants_master.jsonl"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help=(
            "Repertoire de sortie "
            "(defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/"
            "prediction_calibration)"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("prediction_calibration_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_prediction_calibration_features(input_path, logger)

    out_path = output_dir / "prediction_calibration.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        n_total = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info(
                "  %-35s : %d / %d (%.1f%%)",
                k,
                v,
                n_total,
                100.0 * v / n_total,
            )


if __name__ == "__main__":
    main()
