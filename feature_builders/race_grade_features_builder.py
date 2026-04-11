#!/usr/bin/env python3
"""
feature_builders.race_grade_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race grade / quality classification features.

Reads partants_master.jsonl in two streaming passes:
  Pass 1 -- aggregate per-course stats (gains, experience, wins,
            handicaps, claiming flag, quinte/tierce, nombre_partants).
  Pass 2 -- stream each partant, compute features using precomputed
            course-level aggregates, and write directly to disk.

Temporal integrity: all features are derived from the field composition
visible before the race (career counters, handicap, claiming flag,
cote conditions) -- no future leakage.

Produces:
  - race_grade_features.jsonl   in builder_outputs/race_grade_features/

Features per partant (10):
  - rgf_estimated_prize_class     : percentile of avg gains_carriere of field
  - rgf_field_experience_avg      : avg nb_courses_carriere in the race
  - rgf_field_wins_avg            : avg nb_victoires_carriere in the race
  - rgf_is_quinte_race            : cnd_cond_is_quinte flag
  - rgf_is_tierce_race            : cnd_cond_is_tierce flag
  - rgf_is_claiming               : 1 if any horse has taux_reclamation > 0
  - rgf_field_handicap_spread     : max - min handicap_valeur in the race
  - rgf_horse_vs_field_class      : horse gains_carriere / field average
  - rgf_horse_vs_field_experience : horse nb_courses / field average
  - rgf_field_quality_composite   : (field_wins_avg * log(field_gains_avg+1))
                                    / nombre_partants

Usage:
    python feature_builders/race_grade_features_builder.py
    python feature_builders/race_grade_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_grade_features")

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


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PASS 1: AGGREGATE PER-COURSE STATS
# ===========================================================================


def _pass1_aggregate(input_path: Path, logger) -> dict[str, dict]:
    """Read all records, build per-course aggregate dictionaries.

    Returns course_uid -> {
        gains: list[float],
        courses: list[int],
        wins: list[int],
        handicaps: list[float],
        has_claimer: bool,
        is_quinte: int,
        is_tierce: int,
        nombre_partants: int,
    }
    """
    logger.info("--- Pass 1: aggregation par course ---")
    t0 = time.time()

    course_stats: dict[str, dict] = defaultdict(lambda: {
        "gains": [],
        "courses": [],
        "wins": [],
        "handicaps": [],
        "has_claimer": False,
        "is_quinte": 0,
        "is_tierce": 0,
        "nombre_partants": 0,
    })

    n_read = 0
    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1: lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid")
        if not course_uid:
            continue

        cs = course_stats[course_uid]

        # gains_carriere_euros
        g = _safe_float(rec.get("gains_carriere_euros"))
        if g is not None:
            cs["gains"].append(g)

        # nb_courses_carriere
        nc = _safe_int(rec.get("nb_courses_carriere"))
        if nc is not None:
            cs["courses"].append(nc)

        # nb_victoires_carriere
        nv = _safe_int(rec.get("nb_victoires_carriere"))
        if nv is not None:
            cs["wins"].append(nv)

        # handicap_valeur
        hv = _safe_float(rec.get("handicap_valeur"))
        if hv is not None:
            cs["handicaps"].append(hv)

        # taux_reclamation
        tr = _safe_float(rec.get("taux_reclamation_euros"))
        if tr is not None and tr > 0:
            cs["has_claimer"] = True

        # quinte / tierce (take the first truthy value per course)
        if rec.get("cnd_cond_is_quinte"):
            cs["is_quinte"] = 1
        if rec.get("cnd_cond_is_tierce"):
            cs["is_tierce"] = 1

        # nombre_partants (take max seen)
        np_val = _safe_int(rec.get("nombre_partants"))
        if np_val is not None and np_val > cs["nombre_partants"]:
            cs["nombre_partants"] = np_val

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        n_read, len(course_stats), time.time() - t0,
    )
    return dict(course_stats)


# ===========================================================================
# PERCENTILE HELPER
# ===========================================================================


def _compute_percentile_map(course_stats: dict[str, dict]) -> dict[str, float]:
    """Build course_uid -> percentile of avg gains in field.

    Returns values in [0, 1].
    """
    # Compute avg gains per course
    avg_gains: list[tuple[str, float]] = []
    for cuid, cs in course_stats.items():
        gl = cs["gains"]
        if gl:
            avg_gains.append((cuid, sum(gl) / len(gl)))

    if not avg_gains:
        return {}

    # Sort by avg gains, assign percentile
    avg_gains.sort(key=lambda x: x[1])
    n = len(avg_gains)
    result: dict[str, float] = {}
    for rank, (cuid, _) in enumerate(avg_gains):
        result[cuid] = round(rank / max(n - 1, 1), 4)
    return result


# ===========================================================================
# PASS 2: COMPUTE FEATURES & STREAM OUTPUT
# ===========================================================================


def _pass2_features(
    input_path: Path,
    output_path: Path,
    course_stats: dict[str, dict],
    percentile_map: dict[str, float],
    logger,
) -> int:
    """Stream partants, compute features, write directly to disk.

    Returns total records written.
    """
    logger.info("--- Pass 2: calcul features et ecriture ---")
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0

    fill_counts: dict[str, int] = {
        "rgf_estimated_prize_class": 0,
        "rgf_field_experience_avg": 0,
        "rgf_field_wins_avg": 0,
        "rgf_is_quinte_race": 0,
        "rgf_is_tierce_race": 0,
        "rgf_is_claiming": 0,
        "rgf_field_handicap_spread": 0,
        "rgf_horse_vs_field_class": 0,
        "rgf_horse_vs_field_experience": 0,
        "rgf_field_quality_composite": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Pass 2: traite %d records...", n_processed)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid")
            if not partant_uid or not course_uid:
                continue

            cs = course_stats.get(course_uid)
            if cs is None:
                continue

            features: dict[str, Any] = {"partant_uid": partant_uid}

            # --- Course-level features ---

            # 1. rgf_estimated_prize_class
            pct = percentile_map.get(course_uid)
            features["rgf_estimated_prize_class"] = pct
            if pct is not None:
                fill_counts["rgf_estimated_prize_class"] += 1

            # 2. rgf_field_experience_avg
            courses_list = cs["courses"]
            field_exp_avg: Optional[float] = None
            if courses_list:
                field_exp_avg = round(sum(courses_list) / len(courses_list), 2)
            features["rgf_field_experience_avg"] = field_exp_avg
            if field_exp_avg is not None:
                fill_counts["rgf_field_experience_avg"] += 1

            # 3. rgf_field_wins_avg
            wins_list = cs["wins"]
            field_wins_avg: Optional[float] = None
            if wins_list:
                field_wins_avg = round(sum(wins_list) / len(wins_list), 2)
            features["rgf_field_wins_avg"] = field_wins_avg
            if field_wins_avg is not None:
                fill_counts["rgf_field_wins_avg"] += 1

            # 4. rgf_is_quinte_race
            features["rgf_is_quinte_race"] = cs["is_quinte"]
            fill_counts["rgf_is_quinte_race"] += 1

            # 5. rgf_is_tierce_race
            features["rgf_is_tierce_race"] = cs["is_tierce"]
            fill_counts["rgf_is_tierce_race"] += 1

            # 6. rgf_is_claiming
            features["rgf_is_claiming"] = 1 if cs["has_claimer"] else 0
            fill_counts["rgf_is_claiming"] += 1

            # 7. rgf_field_handicap_spread
            handicaps = cs["handicaps"]
            handicap_spread: Optional[float] = None
            if len(handicaps) >= 2:
                handicap_spread = round(max(handicaps) - min(handicaps), 2)
            features["rgf_field_handicap_spread"] = handicap_spread
            if handicap_spread is not None:
                fill_counts["rgf_field_handicap_spread"] += 1

            # --- Per-horse relative features ---

            # 8. rgf_horse_vs_field_class
            horse_gains = _safe_float(rec.get("gains_carriere_euros"))
            gains_list = cs["gains"]
            field_gains_avg = sum(gains_list) / len(gains_list) if gains_list else None
            horse_vs_class: Optional[float] = None
            if horse_gains is not None and field_gains_avg is not None and field_gains_avg > 0:
                horse_vs_class = round(horse_gains / field_gains_avg, 4)
            features["rgf_horse_vs_field_class"] = horse_vs_class
            if horse_vs_class is not None:
                fill_counts["rgf_horse_vs_field_class"] += 1

            # 9. rgf_horse_vs_field_experience
            horse_courses = _safe_int(rec.get("nb_courses_carriere"))
            horse_vs_exp: Optional[float] = None
            if horse_courses is not None and field_exp_avg is not None and field_exp_avg > 0:
                horse_vs_exp = round(horse_courses / field_exp_avg, 4)
            features["rgf_horse_vs_field_experience"] = horse_vs_exp
            if horse_vs_exp is not None:
                fill_counts["rgf_horse_vs_field_experience"] += 1

            # 10. rgf_field_quality_composite
            # (field_wins_avg * log(field_gains_avg + 1)) / nombre_partants
            nb_partants = cs["nombre_partants"]
            composite: Optional[float] = None
            if field_wins_avg is not None and field_gains_avg is not None and nb_partants > 0:
                composite = round(
                    (field_wins_avg * math.log(field_gains_avg + 1)) / nb_partants, 4
                )
            features["rgf_field_quality_composite"] = composite
            if composite is not None:
                fill_counts["rgf_field_quality_composite"] += 1

            # Write to output
            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Pass 2 terminee: %d features ecrites en %.1fs",
        n_written, elapsed,
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)",
            k, v, n_written, 100 * v / n_written if n_written else 0,
        )

    return n_written


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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features race grade/quality a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/race_grade_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_grade_features_builder")
    logger.info("=" * 70)
    logger.info("race_grade_features_builder.py -- Race grade/quality features")
    logger.info("=" * 70)

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "race_grade_features.jsonl"

    # Pass 1: aggregate per-course stats
    course_stats = _pass1_aggregate(input_path, logger)

    # Compute percentile map for prize class
    percentile_map = _compute_percentile_map(course_stats)
    logger.info("Percentile map: %d courses", len(percentile_map))

    # Pass 2: compute features and stream to disk
    n_written = _pass2_features(input_path, out_path, course_stats, percentile_map, logger)

    logger.info("Termine -- %d partants traites", n_written)


if __name__ == "__main__":
    main()
