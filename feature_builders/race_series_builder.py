#!/usr/bin/env python3
"""
feature_builders.race_series_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race series / reunion sequence features.

Detects:
  - Position of each race within a reunion (1st, 2nd, ... Nth race of the day)
  - Whether a horse runs multiple times on the same day
  - Reunion-level aggregate stats (avg field size, avg prize money)

Two-pass approach:
  Pass 1  -- Stream partants_master.jsonl, collect slim records.
  Pass 2  -- Group by (date_reunion_iso, numero_reunion) to compute reunion
             stats, then group by (date, horse_id) to detect multi-race horses.
             Finally emit one feature row per partant.

Temporal integrity: all features are derived from pre-race data visible at
race time (field composition, programme information). No result leakage.

Produces:
  - race_series.jsonl   in output/race_series/

Features per partant (prefix ``rser_``):
  - rser_race_position_in_day      : ordinal position of this race within the reunion
  - rser_total_races_in_reunion    : total number of races in the reunion
  - rser_is_first_race             : 1 if this is the first race of the reunion
  - rser_is_last_race              : 1 if this is the last race of the reunion
  - rser_horse_multi_race_day      : 1 if the horse has >= 2 races on the same day
  - rser_horse_race_number_today   : ordinal for THIS horse today (1 = first start, 2 = second, ...)
  - rser_avg_field_size_reunion    : mean nombre_partants across all races in the reunion
  - rser_reunion_quality           : mean allocation/prix across all races in the reunion (0 if unavailable)

Usage:
    python feature_builders/race_series_builder.py
    python feature_builders/race_series_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/race_series_builder.py --input /path/to/partants_master.jsonl --output-dir /path/to/out/
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_series")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
_OUTPUT_DIR_FALLBACK = _PROJECT_ROOT / "output" / "race_series"

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
        v = float(val)
        return v if v == v else None  # guard NaN
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _horse_key(rec: dict) -> Optional[str]:
    """Return a stable horse identifier string, or None if unavailable."""
    hid = rec.get("horse_id")
    if hid is not None:
        return str(hid)
    nom = rec.get("nom_cheval")
    if nom:
        return str(nom).strip().upper()
    return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_race_series_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Two-pass build of race series features."""
    logger.info("=== Race Series Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # PASS 1 – collect slim records
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Allocation / prize: prefer explicit allocation field, then prix
        alloc = _safe_float(rec.get("allocation")) or _safe_float(rec.get("prix"))

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", "") or "",
            "num_reunion": _safe_int(rec.get("numero_reunion")) or 0,
            "num_course": _safe_int(rec.get("numero_course")) or 0,
            "course_uid": rec.get("course_uid", "") or "",
            "nb_partants": _safe_int(rec.get("nombre_partants")),
            "alloc": alloc,
            "horse_key": _horse_key(rec),
            "num_pmu": _safe_int(rec.get("num_pmu")) or 0,
        }
        slim_records.append(slim)

    logger.info(
        "Pass 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Sort: chronological by (date, reunion, course, num_pmu)
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(
        key=lambda r: (r["date"], r["num_reunion"], r["num_course"], r["num_pmu"])
    )
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # PASS 2a – build reunion index
    # Key: (date, num_reunion)  -> list of distinct course_uid values (ordered)
    # ------------------------------------------------------------------
    t2 = time.time()

    # reunion_courses[(date, num_reunion)] = ordered list of unique course_uid
    # We use an ordered dict trick: insertion order from sorted records is correct.
    reunion_courses: dict[tuple, list[str]] = defaultdict(list)
    # Track seen course_uid per reunion to avoid duplicates
    reunion_seen_courses: dict[tuple, set] = defaultdict(set)

    # reunion stats: (date, num_reunion) -> {nb_partants_list, alloc_list}
    reunion_nb_partants: dict[tuple, list[float]] = defaultdict(list)
    reunion_alloc: dict[tuple, list[float]] = defaultdict(list)

    # course_uid -> reunion_key (for fast lookup in pass 2b)
    course_to_reunion: dict[str, tuple] = {}

    for rec in slim_records:
        rk = (rec["date"], rec["num_reunion"])
        cu = rec["course_uid"]
        if cu and cu not in reunion_seen_courses[rk]:
            reunion_courses[rk].append(cu)
            reunion_seen_courses[rk].add(cu)
        if cu:
            course_to_reunion[cu] = rk
        # Accumulate per-reunion stats (one value per partant is fine for field size,
        # we will deduplicate per course when building averages)
    # Clear the seen-set dict; it's no longer needed
    del reunion_seen_courses
    gc.collect()

    # Build per-course aggregates to then average per reunion
    # course_stats[course_uid] = (nb_partants, alloc_or_None)
    course_nb_partants: dict[str, Optional[float]] = {}
    course_alloc: dict[str, Optional[float]] = {}

    for rec in slim_records:
        cu = rec["course_uid"]
        if not cu:
            continue
        if cu not in course_nb_partants:
            course_nb_partants[cu] = float(rec["nb_partants"]) if rec["nb_partants"] is not None else None
            course_alloc[cu] = rec["alloc"]  # take first occurrence (all same for a course)

    # Now compute reunion-level aggregates
    # reunion_avg_field_size[(date, num_reunion)]
    reunion_avg_field: dict[tuple, Optional[float]] = {}
    reunion_avg_alloc: dict[tuple, float] = {}

    for rk, courses in reunion_courses.items():
        field_sizes = [course_nb_partants[c] for c in courses if course_nb_partants.get(c) is not None]
        allocs = [course_alloc[c] for c in courses if course_alloc.get(c) is not None]

        reunion_avg_field[rk] = round(sum(field_sizes) / len(field_sizes), 2) if field_sizes else None
        reunion_avg_alloc[rk] = round(sum(allocs) / len(allocs), 2) if allocs else 0.0

    logger.info(
        "Pass 2a terminee: %d reunions indexees en %.1fs",
        len(reunion_courses),
        time.time() - t2,
    )

    # ------------------------------------------------------------------
    # PASS 2b – build horse × day index
    # horse_day_courses[(horse_key, date)] = ordered list of course_uid
    # ------------------------------------------------------------------
    t3 = time.time()
    horse_day_courses: dict[tuple, list[str]] = defaultdict(list)
    horse_day_seen: dict[tuple, set] = defaultdict(set)

    for rec in slim_records:
        hk = rec["horse_key"]
        if not hk:
            continue
        dk = (hk, rec["date"])
        cu = rec["course_uid"]
        if cu and cu not in horse_day_seen[dk]:
            horse_day_courses[dk].append(cu)
            horse_day_seen[dk].add(cu)

    del horse_day_seen
    gc.collect()

    logger.info(
        "Pass 2b terminee: %d (cheval, date) paires en %.1fs",
        len(horse_day_courses),
        time.time() - t3,
    )

    # ------------------------------------------------------------------
    # PASS 2c – emit one feature row per slim record
    # ------------------------------------------------------------------
    t4 = time.time()
    results: list[dict[str, Any]] = []
    n_processed = 0

    for rec in slim_records:
        n_processed += 1
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Emission features %d / %d...", n_processed, len(slim_records))

        cu = rec["course_uid"]
        rk = course_to_reunion.get(cu) if cu else None

        # ---- Reunion-level features ----
        if rk and cu:
            course_list = reunion_courses.get(rk, [])
            total_races = len(course_list)

            if cu in course_list:
                race_pos = course_list.index(cu) + 1  # 1-based
            else:
                race_pos = None
                total_races = None

            is_first = int(race_pos == 1) if race_pos is not None else None
            is_last = int(race_pos == total_races) if (race_pos is not None and total_races is not None) else None
            avg_field = reunion_avg_field.get(rk)
            avg_alloc = reunion_avg_alloc.get(rk, 0.0)
        else:
            race_pos = None
            total_races = None
            is_first = None
            is_last = None
            avg_field = None
            avg_alloc = 0.0

        # ---- Horse × day features ----
        hk = rec["horse_key"]
        if hk and cu:
            dk = (hk, rec["date"])
            horse_courses = horse_day_courses.get(dk, [])
            multi = int(len(horse_courses) >= 2)
            if cu in horse_courses:
                horse_race_num = horse_courses.index(cu) + 1  # 1-based
            else:
                horse_race_num = None
        else:
            multi = None
            horse_race_num = None

        feat: dict[str, Any] = {
            "partant_uid": rec["uid"],
            "rser_race_position_in_day": race_pos,
            "rser_total_races_in_reunion": total_races,
            "rser_is_first_race": is_first,
            "rser_is_last_race": is_last,
            "rser_horse_multi_race_day": multi,
            "rser_horse_race_number_today": horse_race_num,
            "rser_avg_field_size_reunion": avg_field,
            "rser_reunion_quality": avg_alloc,
        }
        results.append(feat)

    elapsed = time.time() - t0
    logger.info(
        "Race series build termine: %d features en %.1fs",
        len(results),
        elapsed,
    )
    return results


# ===========================================================================
# INPUT RESOLUTION & CLI
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
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in _INPUT_CANDIDATES]}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features race series a partir de partants_master"
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
        help="Repertoire de sortie (defaut: output/race_series/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_series_builder")

    input_path = _find_input(args.input)

    if args.output_dir:
        output_dir = Path(args.output_dir)
    elif OUTPUT_DIR.parent.exists():
        output_dir = OUTPUT_DIR
    else:
        output_dir = _OUTPUT_DIR_FALLBACK

    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_race_series_features(input_path, logger)

    out_path = output_dir / "race_series.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate report
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info(
                "  %-40s %d/%d (%.1f%%)",
                k,
                v,
                total_count,
                100.0 * v / total_count,
            )


if __name__ == "__main__":
    main()
