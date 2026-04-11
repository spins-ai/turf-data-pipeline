#!/usr/bin/env python3
"""
feature_builders.course_sequence_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features about the race's position within its reunion (meeting).

Reads partants_master.jsonl in two streaming passes:
  - Pass 1: aggregate per reunion (date + numero_reunion)
  - Pass 2: compute features for each partant using reunion stats

No temporal integrity concern here -- these are structural features of the
reunion programme, available before the races start.

Produces:
  - course_sequence_features.jsonl

Features per partant (8):
  - csq_race_number           : numero_course as int
  - csq_is_first_race         : 1 if numero_course == 1
  - csq_is_last_race          : 1 if numero_course == max in reunion
  - csq_total_races_reunion   : total distinct races in this reunion
  - csq_race_position_pct     : numero_course / total_races (normalised 0-1)
  - csq_is_quinte_race        : 1 if this race has the most partants in the reunion
  - csq_avg_field_size_reunion: average nombre_partants across reunion races
  - csq_discipline_mix        : nb distinct disciplines in reunion (1=pure, 2+=mixed)

Usage:
    python feature_builders/course_sequence_builder.py
    python feature_builders/course_sequence_builder.py --input path/to/partants_master.jsonl
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

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/course_sequence")
OUTPUT_FILENAME = "course_sequence_features.jsonl"

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(val: Any) -> Optional[int]:
    """Convert value to int, return None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


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


# ===========================================================================
# REUNION STATS (populated in Pass 1)
# ===========================================================================
# reunion_key = (date_reunion_iso, numero_reunion)
# Value = dict with:
#   courses        : set of numero_course seen
#   max_course     : max numero_course
#   disciplines    : set of disciplines
#   partants_per_course : dict[numero_course -> nombre_partants]
#   sum_partants   : total partants across all courses (for avg)
#   count_courses  : number of distinct courses counted for avg


def _build_reunion_stats(input_path: Path, logger) -> dict:
    """Pass 1: stream through all records, aggregate per reunion."""
    logger.info("=== Pass 1: Aggregation par reunion ===")
    t0 = time.time()

    stats: dict[tuple, dict] = {}
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1: %d records lus...", n_read)
            gc.collect()

        date_str = rec.get("date_reunion_iso", "") or ""
        num_reunion = _safe_int(rec.get("numero_reunion"))
        num_course = _safe_int(rec.get("numero_course"))
        nb_partants = _safe_int(rec.get("nombre_partants"))
        discipline = (rec.get("discipline") or "").strip().upper()

        if not date_str or num_reunion is None:
            continue

        key = (date_str, num_reunion)

        if key not in stats:
            stats[key] = {
                "courses": set(),
                "max_course": 0,
                "disciplines": set(),
                "partants_per_course": {},
                "sum_partants": 0,
                "count_courses": 0,
            }

        s = stats[key]

        if num_course is not None:
            if num_course not in s["courses"]:
                s["courses"].add(num_course)
                s["count_courses"] += 1
                if nb_partants is not None and nb_partants > 0:
                    s["partants_per_course"][num_course] = nb_partants
                    s["sum_partants"] += nb_partants
            else:
                # Update partants_per_course if we get a value and didn't have one
                if num_course not in s["partants_per_course"] and nb_partants is not None and nb_partants > 0:
                    s["partants_per_course"][num_course] = nb_partants
                    s["sum_partants"] += nb_partants

            if num_course > s["max_course"]:
                s["max_course"] = num_course

        if discipline:
            s["disciplines"].add(discipline)

    elapsed = time.time() - t0
    logger.info(
        "Pass 1 terminee: %d records, %d reunions en %.1fs",
        n_read, len(stats), elapsed,
    )
    return stats


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_course_sequence_features(input_path: Path, output_path: Path, logger) -> int:
    """Two-pass build of course sequence features.

    Returns total number of feature records written.
    """
    logger.info("=" * 70)
    logger.info("course_sequence_builder.py -- Features position course dans reunion")
    logger.info("=" * 70)
    t0 = time.time()

    # -- Pass 1: gather reunion-level stats --
    reunion_stats = _build_reunion_stats(input_path, logger)

    # Precompute derived values per reunion to avoid recomputing in Pass 2
    # reunion_key -> {max_course, total_courses, quinte_course, avg_field_size, discipline_mix}
    reunion_derived: dict[tuple, dict] = {}
    for key, s in reunion_stats.items():
        total_courses = len(s["courses"])
        avg_field = (
            round(s["sum_partants"] / s["count_courses"], 2)
            if s["count_courses"] > 0 else None
        )
        # Quinte race = the course with the most partants
        quinte_course = None
        max_partants = 0
        for c_num, nb in s["partants_per_course"].items():
            if nb > max_partants:
                max_partants = nb
                quinte_course = c_num

        reunion_derived[key] = {
            "max_course": s["max_course"],
            "total_courses": total_courses,
            "quinte_course": quinte_course,
            "avg_field_size": avg_field,
            "discipline_mix": len(s["disciplines"]) if s["disciplines"] else None,
        }

    # Free pass-1 raw stats
    del reunion_stats
    gc.collect()

    # -- Pass 2: stream through again, compute features --
    logger.info("=== Pass 2: Calcul des features ===")
    t1 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    n_read = 0
    n_skipped = 0

    fill_counts = {
        "csq_race_number": 0,
        "csq_is_first_race": 0,
        "csq_is_last_race": 0,
        "csq_total_races_reunion": 0,
        "csq_race_position_pct": 0,
        "csq_is_quinte_race": 0,
        "csq_avg_field_size_reunion": 0,
        "csq_discipline_mix": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Pass 2: %d records traites, %d ecrits...", n_read, n_written)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            if not partant_uid:
                n_skipped += 1
                continue

            date_str = rec.get("date_reunion_iso", "") or ""
            num_reunion = _safe_int(rec.get("numero_reunion"))
            num_course = _safe_int(rec.get("numero_course"))

            features: dict[str, Any] = {"partant_uid": partant_uid}

            key = (date_str, num_reunion) if date_str and num_reunion is not None else None
            derived = reunion_derived.get(key) if key else None

            # --- csq_race_number ---
            if num_course is not None:
                features["csq_race_number"] = num_course
                fill_counts["csq_race_number"] += 1
            else:
                features["csq_race_number"] = None

            # --- csq_is_first_race ---
            if num_course is not None:
                features["csq_is_first_race"] = 1 if num_course == 1 else 0
                fill_counts["csq_is_first_race"] += 1
            else:
                features["csq_is_first_race"] = None

            # --- csq_is_last_race ---
            if num_course is not None and derived is not None:
                features["csq_is_last_race"] = 1 if num_course == derived["max_course"] else 0
                fill_counts["csq_is_last_race"] += 1
            else:
                features["csq_is_last_race"] = None

            # --- csq_total_races_reunion ---
            if derived is not None and derived["total_courses"] > 0:
                features["csq_total_races_reunion"] = derived["total_courses"]
                fill_counts["csq_total_races_reunion"] += 1
            else:
                features["csq_total_races_reunion"] = None

            # --- csq_race_position_pct ---
            if num_course is not None and derived is not None and derived["total_courses"] > 0:
                features["csq_race_position_pct"] = round(
                    num_course / derived["total_courses"], 4
                )
                fill_counts["csq_race_position_pct"] += 1
            else:
                features["csq_race_position_pct"] = None

            # --- csq_is_quinte_race ---
            if num_course is not None and derived is not None and derived["quinte_course"] is not None:
                features["csq_is_quinte_race"] = 1 if num_course == derived["quinte_course"] else 0
                fill_counts["csq_is_quinte_race"] += 1
            else:
                features["csq_is_quinte_race"] = None

            # --- csq_avg_field_size_reunion ---
            if derived is not None and derived["avg_field_size"] is not None:
                features["csq_avg_field_size_reunion"] = derived["avg_field_size"]
                fill_counts["csq_avg_field_size_reunion"] += 1
            else:
                features["csq_avg_field_size_reunion"] = None

            # --- csq_discipline_mix ---
            if derived is not None and derived["discipline_mix"] is not None:
                features["csq_discipline_mix"] = derived["discipline_mix"]
                fill_counts["csq_discipline_mix"] += 1
            else:
                features["csq_discipline_mix"] = None

            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features ecrites en %.1fs (%d skipped)",
        n_written, elapsed, n_skipped,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de sequence de course dans une reunion"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("course_sequence_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_course_sequence_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
