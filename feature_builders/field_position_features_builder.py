#!/usr/bin/env python3
"""
feature_builders.field_position_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Within-race positioning features based on odds ranking and other attributes.

For each partant, computes rank-based features relative to the other runners
in the same race. Requires a two-pass approach:
  - Pass 1: stream partants_master.jsonl, group records by course_uid
  - Pass 2: for each race, rank runners and compute percentile features

No temporal leakage risk: all features are computed from race-level data
that is known at race time (odds, declared weight, earnings, etc.).

Produces:
  - field_position_features.jsonl   in OUTPUT_DIR

Features per partant (10):
  - fpf_odds_rank             : rank by cote_finale (1 = favorite)
  - fpf_experience_rank       : rank by nb_courses_carriere (1 = most experienced)
  - fpf_earnings_rank         : rank by gains_carriere_euros (1 = richest)
  - fpf_weight_rank           : rank by poids_porte_kg (1 = lightest)
  - fpf_age_rank              : rank by age (1 = youngest)
  - fpf_combined_rank         : unweighted average of all five ranks above
  - fpf_is_top3_odds          : 1 if fpf_odds_rank <= 3 else 0
  - fpf_is_bottom3_odds       : 1 if odds rank is in the bottom 3 (outsider) else 0
  - fpf_percentile_earnings   : gains_carriere_euros percentile within race (0-1)
  - fpf_percentile_experience : nb_courses_carriere percentile within race (0-1)

Usage:
    python feature_builders/field_position_features_builder.py
    python feature_builders/field_position_features_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/field_position_features_builder.py --output-dir /path/to/output
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

# Fallback candidates when running from a different working tree
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_position_features")

_LOG_EVERY = 500_000

# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield parsed dicts from a JSONL file line by line (streaming)."""
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
                    logger.warning("Ligne JSON invalide ignoree (erreur #%d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# RANKING HELPERS
# ===========================================================================


def _rank_ascending(values: list[Optional[float]]) -> list[Optional[int]]:
    """Return 1-based ascending ranks (1 = smallest value).

    None values produce None rank. Ties share the lower rank (dense rank).
    """
    indexed = [(v, i) for i, v in enumerate(values) if v is not None]
    indexed.sort(key=lambda t: t[0])
    rank_map: dict[int, int] = {}
    current_rank = 1
    prev_val: Optional[float] = None
    for pos, (val, orig_idx) in enumerate(indexed):
        if prev_val is None or val != prev_val:
            current_rank = pos + 1
        rank_map[orig_idx] = current_rank
        prev_val = val
    return [rank_map.get(i) for i in range(len(values))]


def _rank_descending(values: list[Optional[float]]) -> list[Optional[int]]:
    """Return 1-based descending ranks (1 = largest value)."""
    negated = [(-v if v is not None else None) for v in values]
    return _rank_ascending(negated)


def _percentile_within(values: list[Optional[float]]) -> list[Optional[float]]:
    """Return 0-1 percentile of each value within the list.

    None values produce None. Uses linear interpolation style:
      percentile = (rank - 1) / (n_valid - 1) when n_valid >= 2,
      0.5 when n_valid == 1.
    """
    indexed = [(v, i) for i, v in enumerate(values) if v is not None]
    n_valid = len(indexed)
    if n_valid == 0:
        return [None] * len(values)

    indexed.sort(key=lambda t: t[0])
    pct_map: dict[int, float] = {}

    if n_valid == 1:
        pct_map[indexed[0][1]] = 0.5
    else:
        for pos, (_, orig_idx) in enumerate(indexed):
            pct_map[orig_idx] = round(pos / (n_valid - 1), 6)

    return [pct_map.get(i) for i in range(len(values))]


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_field_position_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Two-pass build of within-race positioning features."""
    logger.info("=== Field Position Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1: read only the fields we need, group by course_uid
    # ------------------------------------------------------------------
    # Structure: course_uid -> list of slim records (ordered by arrival in file)
    courses: dict[str, list[dict]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 – lu %d records...", n_read)

        course_uid = rec.get("course_uid")
        if not course_uid:
            continue

        # Parse numeric fields defensively
        def _float(key: str) -> Optional[float]:
            val = rec.get(key)
            if val is None:
                return None
            try:
                f = float(val)
                return f if f >= 0 else None
            except (ValueError, TypeError):
                return None

        cote = _float("cote_finale")
        gains = _float("gains_carriere_euros")
        nb_courses = _float("nb_courses_carriere")
        age = _float("age")
        weight = _float("poids_porte_kg")

        slim = {
            "partant_uid": rec.get("partant_uid"),
            "date_reunion_iso": rec.get("date_reunion_iso", ""),
            "nombre_partants": rec.get("nombre_partants"),
            "cote_finale": cote,
            "gains": gains,
            "nb_courses": nb_courses,
            "age": age,
            "weight": weight,
        }
        courses[course_uid].append(slim)

    logger.info(
        "Pass 1 terminee: %d records, %d courses distinctes (%.1fs)",
        n_read,
        len(courses),
        time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Pass 2: compute within-race features for every runner
    # ------------------------------------------------------------------
    t1 = time.time()
    results: list[dict[str, Any]] = []
    n_courses = 0
    n_runners_total = 0

    for course_uid, runners in courses.items():
        n_courses += 1
        n = len(runners)

        # Extract per-field value vectors
        cotes      = [r["cote_finale"] for r in runners]
        gains_v    = [r["gains"]       for r in runners]
        nb_cours_v = [r["nb_courses"]  for r in runners]
        ages       = [r["age"]         for r in runners]
        weights    = [r["weight"]      for r in runners]

        # Ranks
        odds_rank   = _rank_ascending(cotes)        # 1 = lowest cote = favorite
        exp_rank    = _rank_descending(nb_cours_v)  # 1 = most experienced
        earn_rank   = _rank_descending(gains_v)     # 1 = highest earnings
        weight_rank = _rank_ascending(weights)      # 1 = lightest
        age_rank    = _rank_ascending(ages)         # 1 = youngest

        # Percentiles
        pct_earnings   = _percentile_within(gains_v)
        pct_experience = _percentile_within(nb_cours_v)

        for idx, runner in enumerate(runners):
            o_rank  = odds_rank[idx]
            e_rank  = exp_rank[idx]
            ea_rank = earn_rank[idx]
            w_rank  = weight_rank[idx]
            a_rank  = age_rank[idx]

            # Combined rank: mean of available component ranks
            components = [r for r in (o_rank, e_rank, ea_rank, w_rank, a_rank) if r is not None]
            combined = round(sum(components) / len(components), 4) if components else None

            # Top / bottom 3 by odds
            is_top3    = int(o_rank <= 3)     if o_rank is not None else None
            is_bottom3 = int(o_rank >= n - 2) if o_rank is not None else None
            # Edge case: if n <= 3, every runner can't be "bottom 3" outsider meaningfully
            # Keep the flag but it reflects a very small field.

            results.append({
                "partant_uid":              runner["partant_uid"],
                "fpf_odds_rank":            o_rank,
                "fpf_experience_rank":      e_rank,
                "fpf_earnings_rank":        ea_rank,
                "fpf_weight_rank":          w_rank,
                "fpf_age_rank":             a_rank,
                "fpf_combined_rank":        combined,
                "fpf_is_top3_odds":         is_top3,
                "fpf_is_bottom3_odds":      is_bottom3,
                "fpf_percentile_earnings":  pct_earnings[idx],
                "fpf_percentile_experience": pct_experience[idx],
            })

        n_runners_total += n

        if n_courses % 20_000 == 0:
            logger.info(
                "  Pass 2 – %d courses traitees, %d runners...",
                n_courses,
                n_runners_total,
            )

    # Free the grouping structure
    del courses
    gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Field position build termine: %d features pour %d courses en %.1fs",
        len(results),
        n_courses,
        elapsed,
    )

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file from CLI arg or auto-detection."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in _INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de positionnement dans la course (field position)"
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
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("field_position_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_field_position_features(input_path, logger)

    # Save
    out_path = output_dir / "field_position_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled = {k: 0 for k in feature_keys}
        for row in results:
            for k in feature_keys:
                if row.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %-35s %d/%d (%.1f%%)", k, v, total, 100.0 * v / total)


if __name__ == "__main__":
    main()
