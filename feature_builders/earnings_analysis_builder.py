#!/usr/bin/env python3
"""
feature_builders.earnings_analysis_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep analysis of prize money patterns.

Simple single-pass streaming -- all data available in partants_master.

Features per partant (8):
  - earn_gains_per_year          : gains_carriere / max(1, age - 1)
  - earn_gains_ratio_year_career : gains_annee / (gains_carriere + 1)
  - earn_log_gains               : log(gains_carriere + 1)
  - earn_log_gains_annee         : log(gains_annee + 1)
  - earn_earnings_momentum       : annualized current year vs historical rate
  - earn_is_high_earner          : 1 if gains_carriere > 100000
  - earn_earnings_per_win        : gains_carriere / (nb_victoires + 1)
  - earn_earnings_momentum_ratio : ratio of momentum components

Usage:
    python feature_builders/earnings_analysis_builder.py
    python feature_builders/earnings_analysis_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path
from typing import Optional

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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/earnings_analysis")

# Progress / GC every N records
_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val, default=None) -> Optional[float]:
    """Convert a value to float, returning default on failure."""
    if val is None:
        return default
    try:
        v = float(val)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=None) -> Optional[int]:
    """Convert a value to int, returning default on failure."""
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _parse_month(date_str: str) -> Optional[int]:
    """Extract month (1-12) from ISO date string."""
    if not date_str or len(date_str) < 7:
        return None
    try:
        return int(date_str[5:7])
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_earnings_features(input_path: Path, output_path: Path, logger) -> int:
    """Build earnings analysis features from partants_master.jsonl.

    Single-pass streaming: read each record, compute features, write immediately.
    No state tracking needed -- all inputs come from each record's own fields.

    Returns the total number of feature records written.
    """
    logger.info("=== Earnings Analysis Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_read = 0
    n_written = 0
    n_errors = 0

    fill_counts = {
        "earn_gains_per_year": 0,
        "earn_gains_ratio_year_career": 0,
        "earn_log_gains": 0,
        "earn_log_gains_annee": 0,
        "earn_earnings_momentum": 0,
        "earn_is_high_earner": 0,
        "earn_earnings_per_win": 0,
        "earn_earnings_momentum_ratio": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
                continue

            n_read += 1

            # -- Extract source fields --
            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid")
            date_iso = rec.get("date_reunion_iso", "")

            gains_carriere = _safe_float(rec.get("gains_carriere_euros"))
            gains_annee = _safe_float(rec.get("gains_annee_euros"))
            age = _safe_int(rec.get("age"))
            nb_victoires = _safe_int(rec.get("nb_victoires_carriere"))
            month = _parse_month(date_iso or "")

            # -- Compute features --
            features = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_iso,
            }

            # 1. earn_gains_per_year: gains_carriere / max(1, age - 1)
            if gains_carriere is not None and age is not None and age >= 1:
                years_racing = max(1, age - 1)
                features["earn_gains_per_year"] = round(gains_carriere / years_racing, 2)
                fill_counts["earn_gains_per_year"] += 1
            else:
                features["earn_gains_per_year"] = None

            # 2. earn_gains_ratio_year_career: gains_annee / (gains_carriere + 1)
            if gains_annee is not None and gains_carriere is not None:
                features["earn_gains_ratio_year_career"] = round(
                    gains_annee / (gains_carriere + 1), 4
                )
                fill_counts["earn_gains_ratio_year_career"] += 1
            else:
                features["earn_gains_ratio_year_career"] = None

            # 3. earn_log_gains: log(gains_carriere + 1)
            if gains_carriere is not None:
                features["earn_log_gains"] = round(math.log(gains_carriere + 1), 4)
                fill_counts["earn_log_gains"] += 1
            else:
                features["earn_log_gains"] = None

            # 4. earn_log_gains_annee: log(gains_annee + 1)
            if gains_annee is not None:
                features["earn_log_gains_annee"] = round(math.log(gains_annee + 1), 4)
                fill_counts["earn_log_gains_annee"] += 1
            else:
                features["earn_log_gains_annee"] = None

            # 5. earn_earnings_momentum:
            #    annualized_current = gains_annee * 12 / max(1, month)
            #    historical_rate = gains_carriere / max(1, age - 1)
            #    momentum = annualized_current - historical_rate
            if (gains_annee is not None and gains_carriere is not None
                    and age is not None and age >= 1 and month is not None and month >= 1):
                annualized_current = gains_annee * 12.0 / max(1, month)
                historical_rate = gains_carriere / max(1, age - 1)
                features["earn_earnings_momentum"] = round(
                    annualized_current - historical_rate, 2
                )
                fill_counts["earn_earnings_momentum"] += 1
            else:
                features["earn_earnings_momentum"] = None

            # 6. earn_is_high_earner: 1 if gains_carriere > 100000
            if gains_carriere is not None:
                features["earn_is_high_earner"] = 1 if gains_carriere > 100000 else 0
                fill_counts["earn_is_high_earner"] += 1
            else:
                features["earn_is_high_earner"] = None

            # 7. earn_earnings_per_win: gains_carriere / (nb_victoires + 1)
            if gains_carriere is not None and nb_victoires is not None:
                features["earn_earnings_per_win"] = round(
                    gains_carriere / (nb_victoires + 1), 2
                )
                fill_counts["earn_earnings_per_win"] += 1
            else:
                features["earn_earnings_per_win"] = None

            # 8. earn_earnings_momentum_ratio:
            #    annualized_current / (historical_rate + 1)
            #    Captures whether the horse is earning faster or slower than career avg
            if (gains_annee is not None and gains_carriere is not None
                    and age is not None and age >= 1 and month is not None and month >= 1):
                annualized_current = gains_annee * 12.0 / max(1, month)
                historical_rate = gains_carriere / max(1, age - 1)
                features["earn_earnings_momentum_ratio"] = round(
                    annualized_current / (historical_rate + 1), 4
                )
                fill_counts["earn_earnings_momentum_ratio"] += 1
            else:
                features["earn_earnings_momentum_ratio"] = None

            # -- Write output --
            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

            # -- Periodic logging & GC --
            if n_read % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_read)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Earnings analysis build termine: %d records lus, %d features ecrites en %.1fs",
        n_read, n_written, elapsed,
    )
    if n_errors:
        logger.warning("  %d erreurs JSON ignorees", n_errors)

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
        description="Construction des features earnings analysis a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/earnings_analysis/)",
    )
    args = parser.parse_args()

    logger = setup_logging("earnings_analysis_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "earnings_analysis_features.jsonl"
    build_earnings_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
