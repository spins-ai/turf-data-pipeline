#!/usr/bin/env python3
"""
feature_builders.rank_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Within-race rank features for key numerical fields.

Reads partants_master.jsonl in two passes:
  Pass 1: Group by course_uid, collect key values per horse.
  Pass 2: Compute ranks within each race, emit features.

Temporal integrity: all ranked fields (gains_carriere, nb_courses, age, poids,
cote_finale) are known before the race starts -- no post-race data is used,
so there is no future leakage.

Produces:
  - rank_features.jsonl   in builder_outputs/rank_features/

Features per partant (10):
  - rf_rank_by_gains       : rank within race by gains_carriere_euros (1=highest)
  - rf_rank_by_experience  : rank within race by nb_courses_carriere (1=most experienced)
  - rf_rank_by_wins        : rank within race by nb_victoires_carriere (1=most wins)
  - rf_rank_by_odds        : rank within race by cote_finale (1=shortest/favorite)
  - rf_rank_by_age         : rank within race by age (1=youngest)
  - rf_rank_by_weight      : rank within race by poids_porte (1=lightest)
  - rf_pct_rank_gains      : percentile rank for gains (0=best, 1=worst)
  - rf_pct_rank_odds       : percentile rank for odds (0=best/shortest, 1=worst/longest)
  - rf_pct_rank_experience : percentile rank for experience (0=most experienced, 1=least)
  - rf_avg_rank            : mean of all available integer ranks (composite quality)

Usage:
    python feature_builders/rank_features_builder.py
    python feature_builders/rank_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
    python feature_builders/rank_features_builder.py --input path/to/file --output-dir path/to/dir
"""

from __future__ import annotations

import argparse
import gc
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/rank_features")

# Fallback candidates when the canonical path is unavailable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_FALLBACKS = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
_OUTPUT_FALLBACK = _PROJECT_ROOT / "output" / "rank_features"

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield parsed dicts from a JSONL file, streaming one line at a time."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return f if f == f else None  # reject NaN
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _rank_asc(values: list[Optional[float]]) -> list[Optional[int]]:
    """Return 1-based ranks in ascending order (1 = smallest value).

    Ties share the lowest rank; None values remain None.
    """
    indexed = [(v, i) for i, v in enumerate(values) if v is not None]
    indexed.sort(key=lambda x: x[0])

    ranks: list[Optional[int]] = [None] * len(values)
    r = 1
    i = 0
    while i < len(indexed):
        j = i
        # Find the end of the tied group
        while j < len(indexed) - 1 and indexed[j + 1][0] == indexed[j][0]:
            j += 1
        for k in range(i, j + 1):
            ranks[indexed[k][1]] = r
        r = j + 2  # next rank after the tied block
        i = j + 1

    return ranks


def _rank_desc(values: list[Optional[float]]) -> list[Optional[int]]:
    """Return 1-based ranks in descending order (1 = largest value).

    Ties share the lowest rank; None values remain None.
    """
    negated = [(-v if v is not None else None) for v in values]
    return _rank_asc(negated)


def _pct_rank(ranks: list[Optional[int]], n_valid: int) -> list[Optional[float]]:
    """Convert 1-based integer ranks to percentile ranks in [0, 1].

    0 = best (rank 1), 1 = worst (rank n_valid).
    Returns None where rank is None.
    """
    if n_valid <= 1:
        return [None if r is None else 0.0 for r in ranks]
    return [
        None if r is None else (r - 1) / (n_valid - 1)
        for r in ranks
    ]


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_rank_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Two-pass builder: collect per-course data, then rank within each race."""
    logger.info("=== Rank Features Builder ===")
    logger.info("Lecture en streaming (Pass 1): %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1 -- Read all records, keep minimal fields
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Resolve weight field: prefer poids_porte_kg, fall back to poids_porte
        poids_raw = rec.get("poids_porte_kg") or rec.get("poids_porte")

        slim = {
            "uid": rec.get("partant_uid"),
            "course": rec.get("course_uid", ""),
            "date": rec.get("date_reunion_iso", ""),
            "num": _safe_int(rec.get("num_pmu")) or 0,
            "nb_partants": _safe_int(rec.get("nombre_partants")),
            "gains": _safe_float(rec.get("gains_carriere_euros")),
            "experience": _safe_float(rec.get("nb_courses_carriere")),
            "wins": _safe_float(rec.get("nb_victoires_carriere")),
            "odds": _safe_float(rec.get("cote_finale")),
            "age": _safe_float(rec.get("age")),
            "weight": _safe_float(poids_raw),
        }
        slim_records.append(slim)

    logger.info(
        "Pass 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # Sort chronologically so course groups are contiguous
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Pass 2 -- Group by course_uid and compute ranks
    # ------------------------------------------------------------------
    logger.info("Pass 2: calcul des rangs par course...")
    t2 = time.time()

    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)
    i = 0

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        group: list[dict] = []

        # Collect all partants for this course
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            group.append(slim_records[i])
            i += 1

        n = len(group)

        # Extract per-field value lists
        gains_vals = [r["gains"] for r in group]
        exp_vals = [r["experience"] for r in group]
        wins_vals = [r["wins"] for r in group]
        odds_vals = [r["odds"] for r in group]
        age_vals = [r["age"] for r in group]
        weight_vals = [r["weight"] for r in group]

        # Integer ranks
        ranks_gains = _rank_desc(gains_vals)       # 1 = highest gains
        ranks_exp = _rank_desc(exp_vals)            # 1 = most experienced
        ranks_wins = _rank_desc(wins_vals)          # 1 = most wins
        ranks_odds = _rank_asc(odds_vals)           # 1 = shortest (favourite)
        ranks_age = _rank_asc(age_vals)             # 1 = youngest
        ranks_weight = _rank_asc(weight_vals)       # 1 = lightest

        # Count valid values for percentile normalisation
        n_valid_gains = sum(1 for v in gains_vals if v is not None)
        n_valid_odds = sum(1 for v in odds_vals if v is not None)
        n_valid_exp = sum(1 for v in exp_vals if v is not None)

        # Percentile ranks (0 = best, 1 = worst)
        pct_gains = _pct_rank(ranks_gains, n_valid_gains)
        pct_odds = _pct_rank(ranks_odds, n_valid_odds)
        pct_exp = _pct_rank(ranks_exp, n_valid_exp)

        for idx, rec in enumerate(group):
            # Composite average rank -- only include ranks that exist
            rank_values = [
                ranks_gains[idx],
                ranks_exp[idx],
                ranks_wins[idx],
                ranks_odds[idx],
                ranks_age[idx],
                ranks_weight[idx],
            ]
            valid_ranks = [rv for rv in rank_values if rv is not None]
            avg_rank = round(sum(valid_ranks) / len(valid_ranks), 4) if valid_ranks else None

            feature: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "rf_rank_by_gains": ranks_gains[idx],
                "rf_rank_by_experience": ranks_exp[idx],
                "rf_rank_by_wins": ranks_wins[idx],
                "rf_rank_by_odds": ranks_odds[idx],
                "rf_rank_by_age": ranks_age[idx],
                "rf_rank_by_weight": ranks_weight[idx],
                "rf_pct_rank_gains": (
                    round(pct_gains[idx], 4) if pct_gains[idx] is not None else None
                ),
                "rf_pct_rank_odds": (
                    round(pct_odds[idx], 4) if pct_odds[idx] is not None else None
                ),
                "rf_pct_rank_experience": (
                    round(pct_exp[idx], 4) if pct_exp[idx] is not None else None
                ),
                "rf_avg_rank": avg_rank,
            }
            results.append(feature)

        n_processed += n
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    # Free slim_records early
    del slim_records
    gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Pass 2 terminee: %d features generes en %.1fs",
        len(results), elapsed,
    )
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file, preferring the canonical path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    for candidate in _INPUT_FALLBACKS:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve. Essaye: {INPUT_PARTANTS}, "
        + ", ".join(str(c) for c in _INPUT_FALLBACKS)
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des rank features (within-race) a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("rank_features_builder")

    input_path = _find_input(args.input)
    logger.info("Fichier d'entree: %s", input_path)

    if args.output_dir:
        output_dir = Path(args.output_dir)
    elif OUTPUT_DIR.parent.exists() or OUTPUT_DIR.exists():
        output_dir = OUTPUT_DIR
    else:
        output_dir = _OUTPUT_FALLBACK
        logger.warning(
            "Dossier canonical introuvable (%s), sortie vers: %s",
            OUTPUT_DIR, output_dir,
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Repertoire de sortie: %s", output_dir)

    results = build_rank_features(input_path, logger)

    out_path = output_dir / "rank_features.jsonl"
    save_jsonl(results, out_path, logger)

    logger.info("Done. Fichier ecrit: %s (%d lignes)", out_path, len(results))


if __name__ == "__main__":
    main()
