#!/usr/bin/env python3
"""
feature_builders.ranking_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Within-race ranking features.

Reads partants_master.jsonl in streaming mode, groups partants by course,
and computes per-partant ranking features within each race field.

Temporal integrity: all features are computed from data available at race
time (age, gains_carriere, nb_courses, poids, cote) -- no post-race data
is used for rankings, so there is no future leakage.

Produces:
  - ranking_features.jsonl   in output/ranking_features/

Features per partant:
  - rank_age              : rank of age within this race (1 = oldest)
  - rank_gains            : rank of gains_carriere within race (1 = highest)
  - rank_nb_courses       : rank of experience within race (1 = most experienced)
  - rank_poids            : rank of weight within race (1 = heaviest)
  - percentile_cote       : percentile of cote within race (0-1, lower = shorter price)
  - field_homogeneity     : std(cotes) / mean(cotes) in race (coefficient of variation)
  - is_most_experienced   : 1 if highest nb_courses in field, else 0
  - is_youngest           : 1 if lowest age in field, else 0

Usage:
    python feature_builders/ranking_features_builder.py
    python feature_builders/ranking_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "ranking_features"

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


def _safe_float(val) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _rank_descending(values: list[tuple[int, Optional[float]]]) -> dict[int, Optional[int]]:
    """Rank by value descending (1 = highest). Entries with None get None rank.

    Args:
        values: list of (index, value) tuples
    Returns:
        dict mapping index -> rank (or None)
    """
    result: dict[int, Optional[int]] = {}
    valid = [(idx, v) for idx, v in values if v is not None]
    valid.sort(key=lambda x: -x[1])
    for rank, (idx, _) in enumerate(valid, start=1):
        result[idx] = rank
    for idx, v in values:
        if v is None:
            result[idx] = None
    return result


def _rank_ascending(values: list[tuple[int, Optional[float]]]) -> dict[int, Optional[int]]:
    """Rank by value ascending (1 = lowest). Entries with None get None rank."""
    result: dict[int, Optional[int]] = {}
    valid = [(idx, v) for idx, v in values if v is not None]
    valid.sort(key=lambda x: x[1])
    for rank, (idx, _) in enumerate(valid, start=1):
        result[idx] = rank
    for idx, v in values:
        if v is None:
            result[idx] = None
    return result


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_ranking_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build within-race ranking features from partants_master.jsonl.

    Two-step approach:
      1. Read all records with minimal fields, sort chronologically.
      2. Process course-by-course: compute rankings within each field.
    """
    logger.info("=== Ranking Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        cote = _safe_float(
            rec.get("cote_probable") or rec.get("rapport_final") or rec.get("cote_finale")
        )

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "age": _safe_int(rec.get("age")),
            "gains": _safe_float(
                rec.get("gains_carriere_euros")
                or rec.get("gains_carriere")
                or rec.get("gains_total")
                or rec.get("gains_prix_euros")
                or rec.get("gainsCarriere")
                or rec.get("gains")
            ),
            "nb_courses": _safe_int(rec.get("nb_courses") or rec.get("nb_courses_carriere")),
            "poids": _safe_float(rec.get("poids_porte_kg")),
            "cote": cote,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    _null_row = {
        "rank_age": None,
        "rank_gains": None,
        "rank_nb_courses": None,
        "rank_poids": None,
        "percentile_cote": None,
        "field_homogeneity": None,
        "is_most_experienced": None,
        "is_youngest": None,
    }

    while i < total:
        # Collect all partants for this course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (i < total
               and slim_records[i]["course"] == course_uid
               and slim_records[i]["date"] == course_date):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        n_field = len(course_group)

        # Skip courses with no valid identifier
        if not course_uid:
            for rec in course_group:
                results.append({"partant_uid": rec["uid"], **_null_row})
            n_processed += n_field
            continue

        # -- Compute rankings within this race --
        # Prepare indexed values
        ages = [(j, rec["age"]) for j, rec in enumerate(course_group)]
        gains = [(j, rec["gains"]) for j, rec in enumerate(course_group)]
        nb_courses = [(j, rec["nb_courses"]) for j, rec in enumerate(course_group)]
        poids = [(j, rec["poids"]) for j, rec in enumerate(course_group)]
        cotes = [(j, rec["cote"]) for j, rec in enumerate(course_group)]

        # rank_age: 1 = oldest (descending)
        rank_age_map = _rank_descending(ages)
        # rank_gains: 1 = highest gains (descending)
        rank_gains_map = _rank_descending(gains)
        # rank_nb_courses: 1 = most experienced (descending)
        rank_nb_courses_map = _rank_descending(nb_courses)
        # rank_poids: 1 = heaviest (descending)
        rank_poids_map = _rank_descending(poids)

        # Percentile of cote within race: ascending rank / n_valid
        # Lower percentile = shorter price (favourite)
        cote_rank_asc = _rank_ascending(cotes)
        valid_cotes = [v for _, v in cotes if v is not None]
        n_valid_cotes = len(valid_cotes)

        # Field homogeneity: CV = std(cotes) / mean(cotes)
        field_homo: Optional[float] = None
        if n_valid_cotes >= 2:
            cote_mean = sum(valid_cotes) / n_valid_cotes
            if cote_mean > 0:
                cote_var = sum((c - cote_mean) ** 2 for c in valid_cotes) / (n_valid_cotes - 1)
                cote_std = math.sqrt(cote_var)
                field_homo = round(cote_std / cote_mean, 4)

        # is_most_experienced: 1 if highest nb_courses
        valid_nb = [v for _, v in nb_courses if v is not None]
        max_nb = max(valid_nb) if valid_nb else None

        # is_youngest: 1 if lowest age
        valid_ages = [v for _, v in ages if v is not None]
        min_age = min(valid_ages) if valid_ages else None

        # -- Emit features --
        for j, rec in enumerate(course_group):
            pct_cote: Optional[float] = None
            if cote_rank_asc.get(j) is not None and n_valid_cotes > 0:
                pct_cote = round((cote_rank_asc[j] - 1) / max(n_valid_cotes - 1, 1), 4)

            is_exp: Optional[int] = None
            if rec["nb_courses"] is not None and max_nb is not None:
                is_exp = 1 if rec["nb_courses"] == max_nb else 0

            is_young: Optional[int] = None
            if rec["age"] is not None and min_age is not None:
                is_young = 1 if rec["age"] == min_age else 0

            results.append({
                "partant_uid": rec["uid"],
                "rank_age": rank_age_map.get(j),
                "rank_gains": rank_gains_map.get(j),
                "rank_nb_courses": rank_nb_courses_map.get(j),
                "rank_poids": rank_poids_map.get(j),
                "percentile_cote": pct_cote,
                "field_homogeneity": field_homo,
                "is_most_experienced": is_exp,
                "is_youngest": is_young,
            })

        n_processed += n_field
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Ranking features build termine: %d features en %.1fs",
        len(results), elapsed,
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
        description="Construction des ranking features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/ranking_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("ranking_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_ranking_features(input_path, logger)

    # Save
    out_path = output_dir / "ranking_features.jsonl"
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
