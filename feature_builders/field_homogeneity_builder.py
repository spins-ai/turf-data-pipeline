#!/usr/bin/env python3
"""
feature_builders.field_homogeneity_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Field homogeneity / heterogeneity features.

Reads partants_master.jsonl in two passes, groups by course_uid, and
computes dispersion metrics that measure how similar or different the
horses in a race are.

Temporal integrity: all inputs (gains_carriere, nb_courses_carriere,
cote_finale, age) are known before the race starts — no future leakage.

Produces:
  - field_homogeneity.jsonl   in builder_outputs/field_homogeneity/

Features per partant (8):
  - fh_gains_cv          : coefficient of variation of gains_carriere_euros
  - fh_experience_cv     : coefficient of variation of nb_courses_carriere
  - fh_odds_cv           : coefficient of variation of cote_finale
  - fh_age_range         : max_age - min_age in the field
  - fh_horse_gains_zscore: this horse's gains z-score within the field
  - fh_horse_exp_zscore  : this horse's nb_courses z-score within the field
  - fh_field_entropy     : entropy of implied-prob distribution (high = competitive)
  - fh_top_heavy         : top-2 favorites implied-prob ratio vs rest (>0.5 = top-heavy)

Usage:
    python feature_builders/field_homogeneity_builder.py
    python feature_builders/field_homogeneity_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/field_homogeneity_builder.py --input /path/to/partants_master.jsonl --output-dir /path/to/out
"""

from __future__ import annotations

import argparse
import gc
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

# Fallback candidates relative to the repo root (for local dev / CI)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_homogeneity")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield parsed dicts from a JSONL file, streaming one line at a time."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return None if math.isnan(v) or math.isinf(v) else v
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _std(values: list[float], mean: float) -> float:
    """Population standard deviation."""
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _cv(values: list[float]) -> Optional[float]:
    """Coefficient of variation (std / mean), None when mean == 0 or < 2 values."""
    if len(values) < 2:
        return None
    m = _mean(values)
    if m == 0.0:
        return None
    s = _std(values, m)
    return round(s / abs(m), 6)


def _zscore(value: float, values: list[float]) -> Optional[float]:
    """Z-score of *value* within *values*.  None when std == 0 or < 2 values."""
    if len(values) < 2:
        return None
    m = _mean(values)
    s = _std(values, m)
    if s == 0.0:
        return None
    return round((value - m) / s, 6)


def _entropy(probs: list[float]) -> Optional[float]:
    """Shannon entropy of a probability distribution (in nats).

    Normalises the raw implied probabilities so they sum to 1 before
    computing entropy.  Returns None if fewer than 2 non-zero values.
    """
    non_zero = [p for p in probs if p > 0.0]
    if len(non_zero) < 2:
        return None
    total = sum(non_zero)
    if total == 0.0:
        return None
    normalised = [p / total for p in non_zero]
    h = -sum(p * math.log(p) for p in normalised)
    return round(h, 6)


def _top_heavy(odds_sorted_asc: list[float]) -> Optional[float]:
    """Ratio of the top-2 favorites' implied probability vs the whole field.

    odds_sorted_asc: cote values sorted from lowest (favorite) to highest.
    Returns None if fewer than 3 runners have valid odds.
    """
    if len(odds_sorted_asc) < 3:
        return None
    implied = [1.0 / c for c in odds_sorted_asc if c > 0]
    if len(implied) < 3:
        return None
    total = sum(implied)
    if total == 0.0:
        return None
    top2 = sum(implied[:2])
    return round(top2 / total, 6)


# ===========================================================================
# TWO-PASS BUILD
# ===========================================================================


def build_field_homogeneity_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Main feature builder — two-pass approach over partants_master.

    Pass 1: Stream the file, collect per-horse stats grouped by course_uid.
    Pass 2: Iterate over courses; compute field-level dispersion; emit one
            output record per partant.
    """
    logger.info("=== Field Homogeneity Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1 — collect slim records
    # ------------------------------------------------------------------
    logger.info("Pass 1/2: lecture des champs necessaires...")

    # Each record stores the minimal fields needed for the 8 features.
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # cote_finale preferred; fall back to cote_reference
        cote = _safe_float(rec.get("cote_finale"))
        if cote is None or cote <= 0:
            cote = _safe_float(rec.get("cote_reference"))
            if cote is not None and cote <= 0:
                cote = None

        gains_raw = _safe_float(rec.get("gains_carriere_euros"))

        slim_records.append(
            {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "gains": gains_raw,
                "nb_courses": _safe_float(rec.get("nb_courses_carriere")),
                "cote": cote,
                "age": _safe_float(rec.get("age")),
            }
        )

    logger.info(
        "Pass 1 terminee: %d records lus en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # Sort chronologically so adjacent records belong to the same course
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique: %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Pass 2 — group by course, compute dispersion metrics
    # ------------------------------------------------------------------
    logger.info("Pass 2/2: calcul des features par course...")
    t2 = time.time()

    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)
    i = 0

    while i < total:
        # Collect all runners for the current course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            group.append(slim_records[i])
            i += 1

        # ----------------------------------------------------------------
        # Gather per-field vectors (only valid, non-None values)
        # ----------------------------------------------------------------
        gains_vals: list[float] = [r["gains"] for r in group if r["gains"] is not None]
        exp_vals: list[float] = [r["nb_courses"] for r in group if r["nb_courses"] is not None]
        cote_vals: list[float] = [r["cote"] for r in group if r["cote"] is not None and r["cote"] > 0]
        age_vals: list[float] = [r["age"] for r in group if r["age"] is not None]

        # ----------------------------------------------------------------
        # Field-level metrics (same for every runner in the course)
        # ----------------------------------------------------------------

        # Coefficients of variation
        fh_gains_cv = _cv(gains_vals) if len(gains_vals) >= 2 else None
        fh_experience_cv = _cv(exp_vals) if len(exp_vals) >= 2 else None
        fh_odds_cv = _cv(cote_vals) if len(cote_vals) >= 2 else None

        # Age range
        fh_age_range: Optional[float] = None
        if len(age_vals) >= 2:
            fh_age_range = round(max(age_vals) - min(age_vals), 1)

        # Implied probabilities for entropy / top-heavy (sorted asc by cote)
        cote_sorted = sorted(cote_vals)  # lowest cote = biggest favorite
        implied_probs = [1.0 / c for c in cote_sorted if c > 0]

        fh_field_entropy = _entropy(implied_probs)
        fh_top_heavy = _top_heavy(cote_sorted)

        # ----------------------------------------------------------------
        # Per-horse z-scores (require the horse's own value to be present)
        # ----------------------------------------------------------------
        for rec in group:
            fh_horse_gains_zscore: Optional[float] = None
            if rec["gains"] is not None and len(gains_vals) >= 2:
                fh_horse_gains_zscore = _zscore(rec["gains"], gains_vals)

            fh_horse_exp_zscore: Optional[float] = None
            if rec["nb_courses"] is not None and len(exp_vals) >= 2:
                fh_horse_exp_zscore = _zscore(rec["nb_courses"], exp_vals)

            results.append(
                {
                    "partant_uid": rec["uid"],
                    "fh_gains_cv": fh_gains_cv,
                    "fh_experience_cv": fh_experience_cv,
                    "fh_odds_cv": fh_odds_cv,
                    "fh_age_range": fh_age_range,
                    "fh_horse_gains_zscore": fh_horse_gains_zscore,
                    "fh_horse_exp_zscore": fh_horse_exp_zscore,
                    "fh_field_entropy": fh_field_entropy,
                    "fh_top_heavy": fh_top_heavy,
                }
            )

        n_processed += len(group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features calcules en %.1fs",
        len(results),
        elapsed,
    )

    # Free the large intermediate list early
    del slim_records
    gc.collect()

    return results


# ===========================================================================
# INPUT RESOLUTION
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve. Candidates: "
        + ", ".join(str(c) for c in _INPUT_CANDIDATES)
    )


# ===========================================================================
# FILL-RATE REPORT
# ===========================================================================


def _fill_rates(results: list[dict], logger) -> None:
    if not results:
        logger.info("Aucun resultat — fill rates non disponibles")
        return
    feature_keys = [k for k in results[0] if k != "partant_uid"]
    counts: dict[str, int] = defaultdict(int)
    for rec in results:
        for k in feature_keys:
            if rec.get(k) is not None:
                counts[k] += 1
    total = len(results)
    logger.info("=== Fill rates (%d partants) ===", total)
    for k in feature_keys:
        pct = 100.0 * counts[k] / total if total else 0.0
        logger.info("  %-30s %d / %d  (%.1f%%)", k, counts[k], total, pct)


# ===========================================================================
# CLI / MAIN
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Field homogeneity feature builder — dispersion metrics per course"
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
            "(defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_homogeneity)"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("field_homogeneity_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_field_homogeneity_features(input_path, logger)

    out_path = output_dir / "field_homogeneity.jsonl"
    save_jsonl(results, out_path, logger)

    _fill_rates(results, logger)

    logger.info("Done. Output: %s", out_path)


if __name__ == "__main__":
    main()
