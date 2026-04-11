#!/usr/bin/env python3
"""
feature_builders.horse_sex_age_combo_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Detailed sex x age combination performance features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant sex/age population features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the population statistics -- no future leakage.
Features are captured BEFORE the state is updated with the current result.

Produces:
  - horse_sex_age_combo.jsonl   in output/horse_sex_age_combo/

Features per partant (prefix hsac_):
  - hsac_sex_code            : numeric sex code (male=0, female=1, gelding=2)
  - hsac_sex_age_wr          : population win rate for this sex+age combo
  - hsac_sex_distance_wr     : population win rate for this sex at this distance bucket
  - hsac_female_advantage    : 1 if female in female-only race, 0 otherwise
  - hsac_age_prime           : 1 if horse is in peak age range for its discipline
                               (flat 3-5, trot 5-8)
  - hsac_sex_age_field_pct   : proportion of field that is same sex+age as this horse
  - hsac_gelding_late_career : 1 if gelding and age >= 6, else 0
  - hsac_sex_terrain_wr      : population win rate for this sex on this terrain type

Usage:
    python feature_builders/horse_sex_age_combo_builder.py
    python feature_builders/horse_sex_age_combo_builder.py --input /path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/horse_sex_age_combo")

_LOG_EVERY = 500_000

# Distance bucket boundaries (metres)
_DIST_BUCKETS = [0, 1400, 1800, 2200, 2800, 3500, 9999]

# Peak age ranges per discipline (inclusive)
# flat/galop: 3-5, trot/attele/monte: 5-8, obstacle/haies/steeple/cross: 5-8
_PEAK_AGES: dict[str, Tuple[int, int]] = {
    "plat": (3, 5),
    "galop": (3, 5),
    "trot": (5, 8),
    "attele": (5, 8),
    "monte": (5, 8),
    "obstacle": (5, 8),
    "haies": (5, 8),
    "steeple": (5, 8),
    "cross": (5, 8),
}

# Sex normalization -> code
# PMU uses: H (hongre/gelding), F/Jm (female/jument), M/E (male/entier)
_SEX_CODE: dict[str, int] = {
    # Males
    "m": 0, "e": 0, "entier": 0, "male": 0, "etalon": 0,
    # Females
    "f": 1, "jm": 1, "jument": 1, "pouliche": 1, "female": 1, "femelle": 1,
    # Geldings
    "h": 2, "hongre": 2, "gelding": 2,
}

# Female-only race keywords (in libelle or type_course or conditions)
_FEMALE_RACE_KEYWORDS = (
    "pouliches", "juments", "femelles", "fillies", "mares",
    "poulain femelle",
)


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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN guard
    except (ValueError, TypeError):
        return None


def _normalize_sex(raw: Any) -> Optional[str]:
    """Return normalized sex key: 'm', 'f', or 'h', or None."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    code = _SEX_CODE.get(s)
    if code is not None:
        if code == 0:
            return "m"
        if code == 1:
            return "f"
        return "h"
    # Try first letter
    if s and s[0] in ("m", "e"):
        return "m"
    if s and s[0] in ("f", "j"):
        return "f"
    if s and s[0] == "h":
        return "h"
    return None


def _sex_code(normalized_sex: Optional[str]) -> Optional[int]:
    """Return numeric sex code from normalized sex string."""
    if normalized_sex == "m":
        return 0
    if normalized_sex == "f":
        return 1
    if normalized_sex == "h":
        return 2
    return None


def _distance_bucket(dist_m: Optional[float]) -> Optional[int]:
    """Return distance bucket index (0-based) for a distance in metres."""
    if dist_m is None or dist_m <= 0:
        return None
    for i in range(len(_DIST_BUCKETS) - 1):
        if _DIST_BUCKETS[i] <= dist_m < _DIST_BUCKETS[i + 1]:
            return i
    return len(_DIST_BUCKETS) - 2  # last bucket


def _is_female_only_race(rec: dict) -> bool:
    """Detect female-only race from libelle / conditions fields."""
    fields_to_check = [
        rec.get("libelle_course") or "",
        rec.get("conditions") or "",
        rec.get("type_course") or "",
        rec.get("specialite") or "",
        rec.get("libelle") or "",
    ]
    for field in fields_to_check:
        fl = field.lower()
        for kw in _FEMALE_RACE_KEYWORDS:
            if kw in fl:
                return True
    return False


def _is_age_prime(age: Optional[int], discipline: str) -> Optional[int]:
    """Return 1 if horse is in peak age range for its discipline, else 0, None if unknown."""
    if age is None:
        return None
    disc = discipline.strip().lower() if discipline else ""
    peak = _PEAK_AGES.get(disc)
    if peak is None:
        # Unknown discipline: use a generic 3-7 window
        return 1 if 3 <= age <= 7 else 0
    return 1 if peak[0] <= age <= peak[1] else 0


def _safe_wr(wins: int, total: int) -> Optional[float]:
    """Compute win rate, return None if total == 0."""
    if total == 0:
        return None
    return round(wins / total, 5)


# ===========================================================================
# POPULATION STATE
# ===========================================================================


class _WinLossAccum:
    """Lightweight wins/total accumulator."""
    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def add(self, is_win: bool) -> None:
        self.total += 1
        if is_win:
            self.wins += 1

    def win_rate(self) -> Optional[float]:
        return _safe_wr(self.wins, self.total)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_horse_sex_age_combo_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build sex x age combo features from partants_master.jsonl."""
    logger.info("=== Horse Sex x Age Combo Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -------------------------------------------------------------------------
    # Phase 1: Read minimal fields
    # -------------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        raw_sexe = rec.get("sexe") or rec.get("sex") or rec.get("pgr_sexe")
        norm_sex = _normalize_sex(raw_sexe)
        age = _safe_int(rec.get("age"))
        distance = _safe_float(rec.get("distance"))
        dist_bucket = _distance_bucket(distance)
        terrain = (
            rec.get("etat_terrain") or rec.get("terrain") or rec.get("etat_piste") or ""
        ).strip().lower() or None
        discipline = (
            rec.get("discipline") or rec.get("specialite") or rec.get("type_course") or ""
        ).strip()
        position = _safe_int(rec.get("position_arrivee"))
        is_winner = position == 1 if position is not None else None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "sex": norm_sex,
            "age": age,
            "dist_bucket": dist_bucket,
            "terrain": terrain,
            "discipline": discipline,
            "is_winner": is_winner,
            "is_female_only": _is_female_only_race(rec),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -------------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # -------------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -------------------------------------------------------------------------
    # Phase 3: Process record by record, keeping population state
    # -------------------------------------------------------------------------
    t2 = time.time()

    # Population accumulators
    # key: (sex, age)  -> _WinLossAccum
    pop_sex_age: dict[tuple, _WinLossAccum] = defaultdict(_WinLossAccum)
    # key: (sex, dist_bucket) -> _WinLossAccum
    pop_sex_dist: dict[tuple, _WinLossAccum] = defaultdict(_WinLossAccum)
    # key: (sex, terrain) -> _WinLossAccum
    pop_sex_terrain: dict[tuple, _WinLossAccum] = defaultdict(_WinLossAccum)

    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    i = 0
    while i < total:
        # Collect all partants in this course
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

        # --- Compute field-level sex+age composition (pre-race, no future info) ---
        # Count (sex, age) groups within this field
        sex_age_counts: dict[tuple, int] = defaultdict(int)
        field_size = len(course_group)
        for rec in course_group:
            if rec["sex"] is not None and rec["age"] is not None:
                sex_age_counts[(rec["sex"], rec["age"])] += 1

        # --- Snapshot features BEFORE state update ---
        for rec in course_group:
            sex = rec["sex"]
            age = rec["age"]
            dist_bucket = rec["dist_bucket"]
            terrain = rec["terrain"]
            discipline = rec["discipline"]
            is_female_only = rec["is_female_only"]

            # hsac_sex_code
            sc = _sex_code(sex)

            # hsac_sex_age_wr
            sex_age_wr: Optional[float] = None
            if sex is not None and age is not None:
                acc = pop_sex_age.get((sex, age))
                if acc is not None:
                    sex_age_wr = acc.win_rate()

            # hsac_sex_distance_wr
            sex_dist_wr: Optional[float] = None
            if sex is not None and dist_bucket is not None:
                acc = pop_sex_dist.get((sex, dist_bucket))
                if acc is not None:
                    sex_dist_wr = acc.win_rate()

            # hsac_female_advantage: female in female-only race
            female_advantage: Optional[int] = None
            if sex is not None:
                female_advantage = 1 if (sex == "f" and is_female_only) else 0

            # hsac_age_prime
            age_prime = _is_age_prime(age, discipline)

            # hsac_sex_age_field_pct
            sex_age_field_pct: Optional[float] = None
            if sex is not None and age is not None and field_size > 0:
                same_count = sex_age_counts.get((sex, age), 0)
                sex_age_field_pct = round(same_count / field_size, 5)

            # hsac_gelding_late_career
            gelding_late: Optional[int] = None
            if sex is not None and age is not None:
                gelding_late = 1 if (sex == "h" and age >= 6) else 0

            # hsac_sex_terrain_wr
            sex_terrain_wr: Optional[float] = None
            if sex is not None and terrain is not None:
                acc = pop_sex_terrain.get((sex, terrain))
                if acc is not None:
                    sex_terrain_wr = acc.win_rate()

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "hsac_sex_code": sc,
                "hsac_sex_age_wr": sex_age_wr,
                "hsac_sex_distance_wr": sex_dist_wr,
                "hsac_female_advantage": female_advantage,
                "hsac_age_prime": age_prime,
                "hsac_sex_age_field_pct": sex_age_field_pct,
                "hsac_gelding_late_career": gelding_late,
                "hsac_sex_terrain_wr": sex_terrain_wr,
            }
            results.append(features)

        # --- Update population state AFTER snapshotting ---
        for rec in course_group:
            sex = rec["sex"]
            age = rec["age"]
            dist_bucket = rec["dist_bucket"]
            terrain = rec["terrain"]
            is_winner = rec["is_winner"]

            if is_winner is None:
                continue  # no result known; skip update

            if sex is not None and age is not None:
                pop_sex_age[(sex, age)].add(is_winner)

            if sex is not None and dist_bucket is not None:
                pop_sex_dist[(sex, dist_bucket)].add(is_winner)

            if sex is not None and terrain is not None:
                pop_sex_terrain[(sex, terrain)].add(is_winner)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Sex x Age Combo build termine: %d features en %.1fs",
        len(results), elapsed,
    )

    # Free slim records
    del slim_records
    gc.collect()

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
        description="Construction des features sex x age combo a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/horse_sex_age_combo/)",
    )
    args = parser.parse_args()

    logger = setup_logging("horse_sex_age_combo_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_horse_sex_age_combo_features(input_path, logger)

    # Save
    out_path = output_dir / "horse_sex_age_combo.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary fill rates
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
