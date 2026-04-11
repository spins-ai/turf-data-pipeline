#!/usr/bin/env python3
"""
feature_builders.race_context_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Advanced race context features.

Reads partants_master.jsonl in streaming mode, groups by reunion,
and computes per-partant race context features.

Temporal integrity: these features are derived from the reunion structure
(known before any race starts), no future leakage.

Also reads courses_master.jsonl for allocation data when available.

Produces:
  - race_context_advanced.jsonl   in output/race_context_advanced/

Features per partant:
  - is_last_race_of_day    : 1 si derniere course de la reunion
  - is_first_race_of_day   : 1 si premiere
  - nb_courses_in_reunion  : nb total courses dans la reunion
  - reunion_quality        : allocation totale de la reunion / nb courses

Usage:
    python feature_builders/race_context_advanced_builder.py
    python feature_builders/race_context_advanced_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
COURSES_MASTER = _PROJECT_ROOT / "data_master" / "courses_master.jsonl"
OUTPUT_DIR = _PROJECT_ROOT / "output" / "race_context_advanced"

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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# LOAD COURSES MASTER FOR ALLOCATION DATA
# ===========================================================================


def _load_course_allocations(logger) -> dict[str, float]:
    """Load allocation_totale per course_uid from courses_master.jsonl."""
    alloc_map: dict[str, float] = {}
    if not COURSES_MASTER.exists():
        logger.warning("courses_master.jsonl non trouve, reunion_quality sera None")
        return alloc_map

    logger.info("Chargement allocations depuis %s", COURSES_MASTER)
    for rec in _iter_jsonl(COURSES_MASTER, logger):
        cuid = rec.get("course_uid")
        alloc = _safe_float(rec.get("allocation_totale"))
        if cuid and alloc is not None:
            alloc_map[cuid] = alloc

    logger.info("Allocations chargees: %d courses", len(alloc_map))
    return alloc_map


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_race_context_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build race context advanced features from partants_master.jsonl."""
    logger.info("=== Race Context Advanced Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # Load allocation data
    alloc_map = _load_course_allocations(logger)

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
            "reunion": rec.get("reunion_uid", ""),
            "course": rec.get("course_uid", ""),
            "num_course": _safe_int(rec.get("numero_course")) or 0,
            "num": rec.get("num_pmu", 0) or 0,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort by date, reunion, course number, num --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["reunion"], r["num_course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Group by reunion --
    t2 = time.time()

    # First pass: identify reunion structure
    # reunion_uid -> set of (course_uid, num_course)
    reunion_courses: dict[str, dict[str, int]] = defaultdict(dict)
    for rec in slim_records:
        reunion_courses[rec["reunion"]][rec["course"]] = rec["num_course"]

    # Compute reunion-level stats
    # reunion_uid -> {nb_courses, min_num, max_num, total_alloc}
    reunion_stats: dict[str, dict[str, Any]] = {}
    for reunion_uid, courses in reunion_courses.items():
        course_nums = list(courses.values())
        course_uids = list(courses.keys())
        nb_courses = len(courses)
        min_num = min(course_nums) if course_nums else 0
        max_num = max(course_nums) if course_nums else 0

        # Total allocation for reunion
        total_alloc = 0.0
        alloc_found = 0
        for cuid in course_uids:
            a = alloc_map.get(cuid)
            if a is not None:
                total_alloc += a
                alloc_found += 1

        reunion_quality = None
        if alloc_found > 0 and nb_courses > 0:
            reunion_quality = round(total_alloc / nb_courses, 2)

        reunion_stats[reunion_uid] = {
            "nb_courses": nb_courses,
            "min_num": min_num,
            "max_num": max_num,
            "reunion_quality": reunion_quality,
        }

    # -- Phase 4: Assign features --
    results: list[dict[str, Any]] = []
    for rec in slim_records:
        reunion_uid = rec["reunion"]
        stats = reunion_stats.get(reunion_uid, {})
        num_course = rec["num_course"]

        nb_courses = stats.get("nb_courses")
        is_first = None
        is_last = None
        if nb_courses is not None and stats.get("min_num") is not None:
            is_first = 1 if num_course == stats["min_num"] else 0
            is_last = 1 if num_course == stats["max_num"] else 0

        features: dict[str, Any] = {
            "partant_uid": rec["uid"],
            "is_last_race_of_day": is_last,
            "is_first_race_of_day": is_first,
            "nb_courses_in_reunion": nb_courses,
            "reunion_quality": stats.get("reunion_quality"),
        }
        results.append(features)

    elapsed = time.time() - t0
    logger.info(
        "Race context advanced build termine: %d features en %.1fs (reunions: %d)",
        len(results), elapsed, len(reunion_stats),
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
        description="Construction des features race context advanced a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/race_context_advanced/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_context_advanced_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_race_context_features(input_path, logger)

    # Save
    out_path = output_dir / "race_context_advanced.jsonl"
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
