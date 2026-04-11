#!/usr/bin/env python3
"""
feature_builders.field_diversity_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Field diversity features.

Reads partants_master.jsonl in streaming mode, groups by course,
and computes per-course field diversity metrics.

Temporal integrity: these features are derived from the field composition
(known before the race starts), no future leakage.

Produces:
  - field_diversity.jsonl   in output/field_diversity/

Features per partant:
  - nb_breeds_in_field       : nb races differentes dans le peloton
  - nb_countries_in_field    : nb pays d'origine differents
  - age_spread               : max_age - min_age dans le champ
  - experience_spread        : max_nb_courses - min_nb_courses dans le champ

Usage:
    python feature_builders/field_diversity_builder.py
    python feature_builders/field_diversity_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "field_diversity"

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
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


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_field_diversity_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build field diversity features from partants_master.jsonl."""
    logger.info("=== Field Diversity Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
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
            "race": (rec.get("pgr_race") or rec.get("race") or "").strip(),
            "pays": (rec.get("pays_cheval") or rec.get("pgr_pays_cheval") or "").strip(),
            "age": _safe_int(rec.get("age")),
            "nb_courses": _safe_int(rec.get("nb_courses_carriere")),
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

    # -- Phase 3: Group by course and compute --
    t2 = time.time()
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        # Compute field-level stats
        breeds = set()
        countries = set()
        ages: list[int] = []
        experiences: list[int] = []

        for rec in course_group:
            if rec["race"]:
                breeds.add(rec["race"].lower())
            if rec["pays"]:
                countries.add(rec["pays"].lower())
            if rec["age"] is not None:
                ages.append(rec["age"])
            if rec["nb_courses"] is not None:
                experiences.append(rec["nb_courses"])

        nb_breeds = len(breeds) if breeds else None
        nb_countries = len(countries) if countries else None
        age_spread = (max(ages) - min(ages)) if len(ages) >= 2 else None
        exp_spread = (max(experiences) - min(experiences)) if len(experiences) >= 2 else None

        for rec in course_group:
            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "nb_breeds_in_field": nb_breeds,
                "nb_countries_in_field": nb_countries,
                "age_spread": age_spread,
                "experience_spread": exp_spread,
            }
            results.append(features)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Field diversity build termine: %d features en %.1fs",
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
        description="Construction des features field diversity a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/field_diversity/)",
    )
    args = parser.parse_args()

    logger = setup_logging("field_diversity_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_field_diversity_features(input_path, logger)

    # Save
    out_path = output_dir / "field_diversity.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
