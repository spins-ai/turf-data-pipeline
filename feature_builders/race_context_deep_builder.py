#!/usr/bin/env python3
"""
feature_builders.race_context_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep race context features -- characteristics of the race itself that
affect all horses.  Simple aggregations over the field, no chronological
processing required.

Architecture (two passes, no temporal dependency):
  Pass 1 : Stream partants_master.jsonl, collect minimal fields per
           course_uid into a dict-of-lists.
  Pass 2 : For each course compute field-level aggregates, then emit
           one record per partant with the 12 features.

Produces:
  - race_context_deep_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_context_deep/

Features per partant (12):
  - rcd_field_avg_age            : average age in field
  - rcd_field_age_spread         : max_age - min_age
  - rcd_field_avg_career_races   : average nb_courses_carriere
  - rcd_field_experience_spread  : max - min nb_courses_carriere
  - rcd_field_avg_wins           : average nb_victoires
  - rcd_field_max_wins           : max nb_victoires (strongest competitor)
  - rcd_field_avg_cote           : average cote
  - rcd_field_cote_spread        : max_cote - min_cote
  - rcd_field_nb_first_timers    : count horses with nb_courses < 3
  - rcd_field_nb_veterans        : count horses with nb_courses > 50
  - rcd_is_competitive_race      : 1 if cote_spread < 20
  - rcd_horse_relative_wins      : nb_victoires / field_max_wins

Usage:
    python feature_builders/race_context_deep_builder.py
    python feature_builders/race_context_deep_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_context_deep")

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
        return v if v == v else None  # reject NaN
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
# MAIN BUILD
# ===========================================================================


def build_race_context_deep_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build deep race context features.

    Returns the number of records written.
    """
    logger.info("=== Race Context Deep Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1 : Read minimal fields, group by course_uid
    # ------------------------------------------------------------------
    # course_uid -> list of slim dicts
    course_data: dict[str, list[dict]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid", "")
        if not course_uid:
            continue

        slim = {
            "partant_uid": rec.get("partant_uid"),
            "course_uid": course_uid,
            "date": rec.get("date_reunion_iso", ""),
            "age": _safe_int(rec.get("age")),
            "nb_courses": _safe_int(
                rec.get("nb_courses_carriere")
                or rec.get("nb_courses")
            ),
            "nb_victoires": _safe_int(
                rec.get("nb_victoires_carriere")
                or rec.get("nb_victoires")
            ),
            "cote": _safe_float(
                rec.get("cote_finale")
            ) or _safe_float(
                rec.get("cote_reference")
            ),
        }
        course_data[course_uid].append(slim)

    logger.info(
        "Pass 1 terminee: %d records lus, %d courses uniques en %.1fs",
        n_read, len(course_data), time.time() - t0,
    )
    gc.collect()

    # ------------------------------------------------------------------
    # Pass 2 : Compute field-level aggregates and emit per-partant records
    # ------------------------------------------------------------------
    t1 = time.time()
    n_written = 0

    feature_keys = [
        "rcd_field_avg_age",
        "rcd_field_age_spread",
        "rcd_field_avg_career_races",
        "rcd_field_experience_spread",
        "rcd_field_avg_wins",
        "rcd_field_max_wins",
        "rcd_field_avg_cote",
        "rcd_field_cote_spread",
        "rcd_field_nb_first_timers",
        "rcd_field_nb_veterans",
        "rcd_is_competitive_race",
        "rcd_horse_relative_wins",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    tmp_out = output_path.with_suffix(".tmp")

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for course_uid, runners in course_data.items():
            # -- Collect field-level values --
            ages: list[int] = [r["age"] for r in runners if r["age"] is not None]
            nb_courses_list: list[int] = [r["nb_courses"] for r in runners if r["nb_courses"] is not None]
            nb_victoires_list: list[int] = [r["nb_victoires"] for r in runners if r["nb_victoires"] is not None]
            cotes: list[float] = [r["cote"] for r in runners if r["cote"] is not None and r["cote"] > 0]

            # -- Field aggregates --
            field_avg_age: Optional[float] = None
            field_age_spread: Optional[int] = None
            if ages:
                field_avg_age = round(sum(ages) / len(ages), 4)
                field_age_spread = max(ages) - min(ages)

            field_avg_career_races: Optional[float] = None
            field_experience_spread: Optional[int] = None
            if nb_courses_list:
                field_avg_career_races = round(sum(nb_courses_list) / len(nb_courses_list), 4)
                field_experience_spread = max(nb_courses_list) - min(nb_courses_list)

            field_avg_wins: Optional[float] = None
            field_max_wins: Optional[int] = None
            if nb_victoires_list:
                field_avg_wins = round(sum(nb_victoires_list) / len(nb_victoires_list), 4)
                field_max_wins = max(nb_victoires_list)

            field_avg_cote: Optional[float] = None
            field_cote_spread: Optional[float] = None
            is_competitive: Optional[int] = None
            if cotes:
                field_avg_cote = round(sum(cotes) / len(cotes), 4)
                field_cote_spread = round(max(cotes) - min(cotes), 4)
                is_competitive = 1 if field_cote_spread < 20 else 0

            # Count first-timers (nb_courses < 3) and veterans (nb_courses > 50)
            nb_first_timers = 0
            nb_veterans = 0
            for r in runners:
                nc = r["nb_courses"]
                if nc is not None:
                    if nc < 3:
                        nb_first_timers += 1
                    if nc > 50:
                        nb_veterans += 1

            # -- Emit per-partant records --
            for r in runners:
                # Horse-relative feature
                horse_relative_wins: Optional[float] = None
                if field_max_wins is not None and field_max_wins > 0 and r["nb_victoires"] is not None:
                    horse_relative_wins = round(r["nb_victoires"] / field_max_wins, 4)

                out_rec: dict[str, Any] = {
                    "partant_uid": r["partant_uid"],
                    "course_uid": r["course_uid"],
                    "date_reunion_iso": r["date"],
                    "rcd_field_avg_age": field_avg_age,
                    "rcd_field_age_spread": field_age_spread,
                    "rcd_field_avg_career_races": field_avg_career_races,
                    "rcd_field_experience_spread": field_experience_spread,
                    "rcd_field_avg_wins": field_avg_wins,
                    "rcd_field_max_wins": field_max_wins,
                    "rcd_field_avg_cote": field_avg_cote,
                    "rcd_field_cote_spread": field_cote_spread,
                    "rcd_field_nb_first_timers": nb_first_timers,
                    "rcd_field_nb_veterans": nb_veterans,
                    "rcd_is_competitive_race": is_competitive,
                    "rcd_horse_relative_wins": horse_relative_wins,
                }

                # Track fill rates
                for k in feature_keys:
                    if out_rec.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

            # GC every 500K written records
            if n_written % _LOG_EVERY < len(runners):
                logger.info("  Ecrit %d records...", n_written)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Race context deep build termine: %d features en %.1fs (%d courses)",
        n_written, elapsed, len(course_data),
    )

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
        description="Construction des features deep race context a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/race_context_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_context_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "race_context_deep_features.jsonl"
    build_race_context_deep_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
