#!/usr/bin/env python3
"""
feature_builders.odds_transform_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Mathematical transformations of odds for ML models.

Reads partants_master.jsonl in a single streaming pass and computes
per-partant odds transformation features.

Temporal integrity: all features are derived from race-day odds
(cote_finale / cote_reference), known before the race starts.
No future leakage.

Produces:
  - odds_transform.jsonl   in OUTPUT_DIR

Features per partant (10):
  ot_log_cote         : log(cote_finale)
  ot_sqrt_cote        : sqrt(cote_finale)
  ot_inv_cote         : 1 / cote_finale  (implied raw probability)
  ot_logit_cote       : log(inv_cote / (1 - inv_cote)), clipped to [-5, 5]
  ot_cote_bucketized  : 0(<2), 1(2-4), 2(4-8), 3(8-15), 4(15-30), 5(>30)
  ot_cote_ref_log     : log(cote_reference)
  ot_cote_drift       : cote_finale - cote_reference  (absolute movement)
  ot_cote_drift_pct   : (cote_finale - cote_reference) / cote_reference * 100
  ot_odds_ratio       : cote_finale / cote_reference  (relative movement)
  ot_sharp_signal     : 1 if cote_finale < cote_reference * 0.9 else 0

Usage:
    python feature_builders/odds_transform_builder.py
    python feature_builders/odds_transform_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/odds_transform_builder.py --output-dir /path/to/output
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/odds_transform")

_LOG_EVERY = 500_000

# Logit clip bounds to avoid ±infinity when inv_cote is near 0 or 1
_LOGIT_MIN = -5.0
_LOGIT_MAX = 5.0


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    """Convert val to float; return None on None, zero, or invalid."""
    if val is None:
        return None
    try:
        v = float(val)
        if v != v:  # NaN
            return None
        return v
    except (ValueError, TypeError):
        return None


def _safe_positive_float(val) -> Optional[float]:
    """Convert val to positive float; return None if None, zero, or negative."""
    v = _safe_float(val)
    if v is None or v <= 0.0:
        return None
    return v


def _bucketize_cote(cote: float) -> int:
    """Map a cote (odds) value to a bucket index.

    Buckets:
        0 : cote < 2       (heavy favourite)
        1 : 2  <= cote < 4
        2 : 4  <= cote < 8
        3 : 8  <= cote < 15
        4 : 15 <= cote < 30
        5 : cote >= 30     (outsider)
    """
    if cote < 2.0:
        return 0
    elif cote < 4.0:
        return 1
    elif cote < 8.0:
        return 2
    elif cote < 15.0:
        return 3
    elif cote < 30.0:
        return 4
    else:
        return 5


def _compute_features(
    partant_uid: Any,
    cote_finale_raw,
    cote_reference_raw,
) -> dict[str, Any]:
    """Compute all 10 odds-transform features for a single partant."""

    cote_f = _safe_positive_float(cote_finale_raw)
    cote_r = _safe_positive_float(cote_reference_raw)

    # ot_log_cote
    ot_log_cote: Optional[float]
    if cote_f is not None:
        ot_log_cote = round(math.log(cote_f), 6)
    else:
        ot_log_cote = None

    # ot_sqrt_cote
    ot_sqrt_cote: Optional[float]
    if cote_f is not None:
        ot_sqrt_cote = round(math.sqrt(cote_f), 6)
    else:
        ot_sqrt_cote = None

    # ot_inv_cote  (raw implied probability)
    ot_inv_cote: Optional[float]
    if cote_f is not None:
        ot_inv_cote = round(1.0 / cote_f, 6)
    else:
        ot_inv_cote = None

    # ot_logit_cote = log(p / (1 - p)), clipped to [-5, 5]
    ot_logit_cote: Optional[float]
    if ot_inv_cote is not None:
        p = ot_inv_cote
        # Guard against p >= 1 (cote <= 1) to avoid log(0) or negative denominator
        if p <= 0.0 or p >= 1.0:
            # clip to boundary
            ot_logit_cote = _LOGIT_MAX if p >= 1.0 else _LOGIT_MIN
        else:
            raw_logit = math.log(p / (1.0 - p))
            ot_logit_cote = round(max(_LOGIT_MIN, min(_LOGIT_MAX, raw_logit)), 6)
    else:
        ot_logit_cote = None

    # ot_cote_bucketized
    ot_cote_bucketized: Optional[int]
    if cote_f is not None:
        ot_cote_bucketized = _bucketize_cote(cote_f)
    else:
        ot_cote_bucketized = None

    # ot_cote_ref_log
    ot_cote_ref_log: Optional[float]
    if cote_r is not None:
        ot_cote_ref_log = round(math.log(cote_r), 6)
    else:
        ot_cote_ref_log = None

    # ot_cote_drift  (absolute movement: positive = odds lengthened)
    ot_cote_drift: Optional[float]
    if cote_f is not None and cote_r is not None:
        ot_cote_drift = round(cote_f - cote_r, 4)
    else:
        ot_cote_drift = None

    # ot_cote_drift_pct
    ot_cote_drift_pct: Optional[float]
    if cote_f is not None and cote_r is not None:
        ot_cote_drift_pct = round((cote_f - cote_r) / cote_r * 100.0, 4)
    else:
        ot_cote_drift_pct = None

    # ot_odds_ratio
    ot_odds_ratio: Optional[float]
    if cote_f is not None and cote_r is not None:
        ot_odds_ratio = round(cote_f / cote_r, 6)
    else:
        ot_odds_ratio = None

    # ot_sharp_signal  (1 if odds shortened significantly, i.e. sharp money)
    ot_sharp_signal: Optional[int]
    if cote_f is not None and cote_r is not None:
        ot_sharp_signal = 1 if cote_f < cote_r * 0.9 else 0
    else:
        ot_sharp_signal = None

    return {
        "partant_uid": partant_uid,
        "ot_log_cote": ot_log_cote,
        "ot_sqrt_cote": ot_sqrt_cote,
        "ot_inv_cote": ot_inv_cote,
        "ot_logit_cote": ot_logit_cote,
        "ot_cote_bucketized": ot_cote_bucketized,
        "ot_cote_ref_log": ot_cote_ref_log,
        "ot_cote_drift": ot_cote_drift,
        "ot_cote_drift_pct": ot_cote_drift_pct,
        "ot_odds_ratio": ot_odds_ratio,
        "ot_sharp_signal": ot_sharp_signal,
    }


# ===========================================================================
# MAIN BUILD  (single-pass streaming)
# ===========================================================================


def build_odds_transform_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Stream partants_master.jsonl and compute odds-transform features."""
    logger.info("=== Odds Transform Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0
    n_errors = 0

    with open(input_path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                rec = json.loads(raw_line)
            except json.JSONDecodeError:
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Lu %d records...", n_read)

            features = _compute_features(
                partant_uid=rec.get("partant_uid"),
                cote_finale_raw=rec.get("cote_finale"),
                cote_reference_raw=rec.get("cote_reference"),
            )
            results.append(features)

    elapsed = time.time() - t0
    logger.info(
        "Streaming termine: %d records lus, %d erreurs JSON, %.1fs",
        n_read, n_errors, elapsed,
    )
    logger.info("Features construites: %d", len(results))

    # Free parsed records from memory before returning
    gc.collect()

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file: CLI argument first, then the canonical path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}\n"
        "Utilisez --input pour specifier un chemin alternatif."
    )


def _print_fill_rates(results: list[dict[str, Any]], logger) -> None:
    """Log fill rates for each feature column."""
    if not results:
        return
    feature_keys = [k for k in results[0] if k != "partant_uid"]
    filled = {k: 0 for k in feature_keys}
    for r in results:
        for k in feature_keys:
            if r.get(k) is not None:
                filled[k] += 1
    total = len(results)
    logger.info("=== Fill rates (%d records) ===", total)
    for k in feature_keys:
        pct = 100.0 * filled[k] / total if total else 0.0
        logger.info("  %-30s %d / %d  (%.1f%%)", k, filled[k], total, pct)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features odds-transform a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help=(
            "Chemin vers partants_master.jsonl "
            f"(defaut: {INPUT_PARTANTS})"
        ),
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("odds_transform_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_odds_transform_features(input_path, logger)

    out_path = output_dir / "odds_transform.jsonl"
    save_jsonl(results, out_path, logger)

    _print_fill_rates(results, logger)

    logger.info("=== Termine ===")


if __name__ == "__main__":
    main()
