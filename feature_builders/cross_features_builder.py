#!/usr/bin/env python3
"""
feature_builders.cross_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
6 cross-entity interaction features that combine horse/jockey/trainer/sire
performance with race context (weather, discipline, distance, terrain, etc.).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant cross-features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - cross_features.jsonl   in output/cross_features/

Features per partant:
  - horse_meteo_win_rate           : horse's win rate in similar weather (rain/dry)
  - trainer_type_win_rate          : trainer's win rate in this race type
  - age_month_factor               : performance factor for horse's age in this month
  - sire_distance_terrain_score    : sire's offspring win rate at distance+terrain
  - same_course_history            : horse's win rate at hippo+distance+discipline
  - jockey_discipline_win_rate     : jockey's win rate in this discipline

Usage:
    python feature_builders/cross_features_builder.py
    python feature_builders/cross_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
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
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/cross_features")

# Minimum observations before emitting a rate (smoothing threshold)
_MIN_OBS = 3

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


# ===========================================================================
# HELPERS
# ===========================================================================

_HANDICAP_RE = re.compile(r"handicap", re.IGNORECASE)
_GROUPE_RE = re.compile(r"groupe?\s", re.IGNORECASE)
_LISTED_RE = re.compile(r"list[eé]e?|listed", re.IGNORECASE)
_CLAIMER_RE = re.compile(r"r[eé]clamer|claiming|claimer", re.IGNORECASE)


def _classify_race_type(conditions: str) -> str:
    """Classify race into type: handicap, groupe, listed, claimer, or other."""
    if not conditions:
        return "other"
    if _HANDICAP_RE.search(conditions):
        return "handicap"
    if _GROUPE_RE.search(conditions):
        return "groupe"
    if _LISTED_RE.search(conditions):
        return "listed"
    if _CLAIMER_RE.search(conditions):
        return "claimer"
    return "other"


def _classify_weather(rec: dict) -> str:
    """Classify weather as 'rain' or 'dry' from meteo fields.

    Uses met_impact_meteo_score (higher = worse weather) and met_is_psf.
    Falls back to penetrometre-based inference if meteo data unavailable.
    """
    # met_impact_meteo_score: 0=dry, higher=wet
    met_score = rec.get("met_impact_meteo_score")
    if met_score is not None:
        try:
            if float(met_score) >= 2:
                return "rain"
            return "dry"
        except (ValueError, TypeError):
            pass

    # PSF (polytrack/all-weather) is always "dry" conditions
    if rec.get("met_is_psf"):
        return "dry"

    # Fallback: check penetrometre if available
    penetro = (rec.get("penetrometre") or "").lower().strip()
    if penetro in ("lourd", "collant", "tres souple", "très souple"):
        return "rain"
    if penetro in ("bon", "sec", "leger", "léger", "standard", "souple"):
        return "dry"

    return "unknown"


def _distance_bucket(distance: Optional[int]) -> str:
    """Bucket distance into categories for sire cross feature."""
    if distance is None:
        return "unknown"
    try:
        d = int(distance)
    except (ValueError, TypeError):
        return "unknown"
    if d <= 1200:
        return "sprint"
    if d <= 1600:
        return "mile"
    if d <= 2200:
        return "inter"
    if d <= 3000:
        return "long"
    return "marathon"


def _safe_rate(wins: int, runs: int) -> Optional[float]:
    """Compute win rate with minimum observation threshold."""
    if runs < _MIN_OBS:
        return None
    return round(wins / runs, 4)


# ===========================================================================
# ACCUMULATOR
# ===========================================================================


class _WinRunCounter:
    """Lightweight counter for wins and total runs."""

    __slots__ = ("wins", "runs")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0

    def rate(self) -> Optional[float]:
        return _safe_rate(self.wins, self.runs)

    def update(self, is_winner: bool) -> None:
        self.runs += 1
        if is_winner:
            self.wins += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_cross_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build 6 cross-entity features from partants_master.jsonl.

    Two-phase approach:
      1. Read all records with minimal fields, sort chronologically.
      2. Process course-by-course: snapshot pre-race stats, emit features,
         then update accumulators.
    """
    logger.info("=== Cross Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        conditions = rec.get("cnd_conditions_texte_original") or ""
        distance = rec.get("distance")
        try:
            distance = int(distance) if distance is not None else None
        except (ValueError, TypeError):
            distance = None

        date_iso = rec.get("date_reunion_iso", "")
        month = None
        if date_iso and len(date_iso) >= 7:
            try:
                month = int(date_iso[5:7])
            except (ValueError, TypeError):
                pass

        age = rec.get("age")
        try:
            age = int(age) if age is not None else None
        except (ValueError, TypeError):
            age = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": date_iso,
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "jockey": rec.get("jockey_driver"),
            "entraineur": rec.get("entraineur"),
            "pere": rec.get("pere"),
            "gagnant": bool(rec.get("is_gagnant")),
            "discipline": (rec.get("discipline") or "").lower().strip(),
            "hippo": (rec.get("hippodrome_normalise") or "").lower().strip(),
            "distance": distance,
            "type_piste": (rec.get("type_piste") or "").lower().strip(),
            "conditions": conditions,
            "weather": _classify_weather(rec),
            "race_type": _classify_race_type(conditions),
            "dist_bucket": _distance_bucket(distance),
            "age": age,
            "month": month,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process course by course ──
    t2 = time.time()

    # Accumulators: entity+context -> _WinRunCounter
    horse_weather: dict[tuple[str, str], _WinRunCounter] = defaultdict(_WinRunCounter)
    trainer_type: dict[tuple[str, str], _WinRunCounter] = defaultdict(_WinRunCounter)
    age_month: dict[tuple[int, int], _WinRunCounter] = defaultdict(_WinRunCounter)
    sire_dist_terrain: dict[tuple[str, str, str], _WinRunCounter] = defaultdict(_WinRunCounter)
    horse_course_combo: dict[tuple[str, str, str, str], _WinRunCounter] = defaultdict(_WinRunCounter)
    jockey_discipline: dict[tuple[str, str], _WinRunCounter] = defaultdict(_WinRunCounter)

    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants for this course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # ── Snapshot pre-race features for all partants ──
        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]
            entraineur = rec["entraineur"]
            pere = rec["pere"]
            weather = rec["weather"]
            race_type = rec["race_type"]
            discipline = rec["discipline"]
            hippo = rec["hippo"]
            distance = rec["distance"]
            dist_bucket = rec["dist_bucket"]
            type_piste = rec["type_piste"]
            age = rec["age"]
            month = rec["month"]

            # 1. horse_meteo_win_rate: horse's win rate in similar weather
            f_horse_meteo = None
            if cheval and weather != "unknown":
                counter = horse_weather.get((cheval, weather))
                if counter:
                    f_horse_meteo = counter.rate()

            # 2. trainer_type_win_rate: trainer's win rate in this race type
            f_trainer_type = None
            if entraineur and race_type:
                counter = trainer_type.get((entraineur, race_type))
                if counter:
                    f_trainer_type = counter.rate()

            # 3. age_month_factor: performance factor for this age in this month
            f_age_month = None
            if age is not None and month is not None:
                counter = age_month.get((age, month))
                if counter:
                    f_age_month = counter.rate()

            # 4. sire_distance_terrain_score: sire offspring win rate at dist+terrain
            f_sire_dt = None
            if pere and dist_bucket != "unknown" and type_piste:
                counter = sire_dist_terrain.get((pere, dist_bucket, type_piste))
                if counter:
                    f_sire_dt = counter.rate()

            # 5. same_course_history: horse win rate at hippo+distance+discipline
            f_same_course = None
            dist_str = str(distance) if distance is not None else ""
            if cheval and hippo and dist_str and discipline:
                counter = horse_course_combo.get(
                    (cheval, hippo, dist_str, discipline)
                )
                if counter:
                    f_same_course = counter.rate()

            # 6. jockey_discipline_win_rate: jockey's win rate in this discipline
            f_jockey_disc = None
            if jockey and discipline:
                counter = jockey_discipline.get((jockey, discipline))
                if counter:
                    f_jockey_disc = counter.rate()

            results.append({
                "partant_uid": rec["uid"],
                "horse_meteo_win_rate": f_horse_meteo,
                "trainer_type_win_rate": f_trainer_type,
                "age_month_factor": f_age_month,
                "sire_distance_terrain_score": f_sire_dt,
                "same_course_history": f_same_course,
                "jockey_discipline_win_rate": f_jockey_disc,
            })

        # ── Update accumulators after race (post-race) ──
        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]
            entraineur = rec["entraineur"]
            pere = rec["pere"]
            is_winner = rec["gagnant"]
            weather = rec["weather"]
            race_type = rec["race_type"]
            discipline = rec["discipline"]
            hippo = rec["hippo"]
            distance = rec["distance"]
            dist_bucket = rec["dist_bucket"]
            type_piste = rec["type_piste"]
            age = rec["age"]
            month = rec["month"]

            # 1. horse + weather
            if cheval and weather != "unknown":
                horse_weather[(cheval, weather)].update(is_winner)

            # 2. trainer + race type
            if entraineur and race_type:
                trainer_type[(entraineur, race_type)].update(is_winner)

            # 3. age + month (global, not per-horse)
            if age is not None and month is not None:
                age_month[(age, month)].update(is_winner)

            # 4. sire + distance bucket + terrain
            if pere and dist_bucket != "unknown" and type_piste:
                sire_dist_terrain[(pere, dist_bucket, type_piste)].update(is_winner)

            # 5. horse + hippo + distance + discipline
            dist_str = str(distance) if distance is not None else ""
            if cheval and hippo and dist_str and discipline:
                horse_course_combo[(cheval, hippo, dist_str, discipline)].update(
                    is_winner
                )

            # 6. jockey + discipline
            if jockey and discipline:
                jockey_discipline[(jockey, discipline)].update(is_winner)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Cross features build termine: %d features en %.1fs",
        len(results), elapsed,
    )
    logger.info(
        "Accumulateurs — horse_weather: %d, trainer_type: %d, age_month: %d, "
        "sire_dist_terrain: %d, horse_course_combo: %d, jockey_discipline: %d",
        len(horse_weather), len(trainer_type), len(age_month),
        len(sire_dist_terrain), len(horse_course_combo), len(jockey_discipline),
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
        description="Construction des cross-features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/cross_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("cross_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_cross_features(input_path, logger)

    # Save
    out_path = output_dir / "cross_features.jsonl"
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
