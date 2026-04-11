#!/usr/bin/env python3
"""
feature_builders.competition_analysis_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Competition dynamics features analysing the competitive landscape within
each race field.

Two-pass architecture:
  Pass 1 -- stream partants_master.jsonl, collect per course_uid a list
            of {num_pmu, nb_victoires, gains, age, cote, nb_courses}.
  Pass 2 -- iterate collected courses, compute 10 features per partant.

Temporal integrity: all inputs (nb_victoires, gains_carriere, age, cote,
nb_courses) are pre-race snapshots already available at race time -- no
future leakage.

Produces:
  - competition_analysis_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/competition_analysis/

Features per partant (10):
  - cmp_nb_strong_competitors   : count of horses with nb_victoires > 5
  - cmp_nb_debutants            : count of horses with nb_courses < 3
  - cmp_horse_rank_by_wins      : rank by nb_victoires in field (1=most)
  - cmp_horse_rank_by_gains     : rank by gains_carriere in field (1=most)
  - cmp_horse_rank_by_age       : rank by age in field (1=youngest)
  - cmp_dominance_score         : max(1/cote) / sum(1/cote) -- favourite dominance
  - cmp_field_depth             : std of gains_carriere in field
  - cmp_chalenger_count         : horses with cote within 2x of favourite cote
  - cmp_horse_is_class_leader   : 1 if horse has highest gains in field
  - cmp_separation_from_favorite: |cote - fav_cote| / fav_cote

Usage:
    python feature_builders/competition_analysis_builder.py
    python feature_builders/competition_analysis_builder.py --input path/to/partants_master.jsonl
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PATH = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/competition_analysis")

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
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _stdev(values: list[float]) -> Optional[float]:
    """Population standard deviation. Returns None if < 2 values."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _rank_descending(values: list[Optional[float]], default: float = 0.0) -> list[int]:
    """Dense rank, highest value = rank 1.  None treated as *default*."""
    cleaned = [(v if v is not None else default) for v in values]
    indexed = sorted(enumerate(cleaned), key=lambda x: x[1], reverse=True)
    ranks = [0] * len(cleaned)
    for rank, (orig_idx, _) in enumerate(indexed, start=1):
        ranks[orig_idx] = rank
    return ranks


def _rank_ascending(values: list[Optional[float]], default: float = 999.0) -> list[int]:
    """Dense rank, lowest value = rank 1.  None treated as *default*."""
    cleaned = [(v if v is not None else default) for v in values]
    indexed = sorted(enumerate(cleaned), key=lambda x: x[1])
    ranks = [0] * len(cleaned)
    for rank, (orig_idx, _) in enumerate(indexed, start=1):
        ranks[orig_idx] = rank
    return ranks


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_competition_analysis_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build competition analysis features from partants_master.jsonl."""
    logger.info("=== Competition Analysis Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1: collect per-course field data
    # ------------------------------------------------------------------
    # course_uid -> list of slim dicts
    course_fields: dict[str, list[dict]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 -- lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid", "")
        if not course_uid:
            continue

        slim = {
            "uid": rec.get("partant_uid"),
            "course_uid": course_uid,
            "date": rec.get("date_reunion_iso", ""),
            "num_pmu": _safe_int(rec.get("num_pmu")) or 0,
            "nb_victoires": _safe_int(rec.get("nb_victoires_carriere")) or 0,
            "gains": _safe_float(rec.get("gains_carriere_euros")),
            "age": _safe_int(rec.get("age")),
            "cote": _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference")),
            "nb_courses": _safe_int(rec.get("nb_courses_carriere")) or 0,
        }
        course_fields[course_uid].append(slim)

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        n_read, len(course_fields), time.time() - t0,
    )
    gc.collect()

    # ------------------------------------------------------------------
    # Pass 2: compute features per partant
    # ------------------------------------------------------------------
    t1 = time.time()
    results: list[dict[str, Any]] = []
    n_courses_done = 0

    for course_uid, runners in course_fields.items():
        field_size = len(runners)

        # -- Field-level extractions --
        nb_vics = [r["nb_victoires"] for r in runners]
        nb_courses_list = [r["nb_courses"] for r in runners]
        gains_list = [r["gains"] for r in runners]
        ages = [r["age"] for r in runners]
        cotes = [r["cote"] for r in runners]

        # cmp_nb_strong_competitors: nb_victoires > 5
        nb_strong = sum(1 for v in nb_vics if v > 5)

        # cmp_nb_debutants: nb_courses < 3
        nb_deb = sum(1 for c in nb_courses_list if c < 3)

        # Ranks
        rank_wins = _rank_descending(nb_vics, default=0)
        rank_gains = _rank_descending(gains_list, default=0.0)
        rank_age = _rank_ascending(ages, default=999.0)

        # cmp_dominance_score: max(1/cote) / sum(1/cote)
        implied_probs = []
        for c in cotes:
            if c is not None and c > 0:
                implied_probs.append(1.0 / c)
            else:
                implied_probs.append(None)

        valid_ips = [ip for ip in implied_probs if ip is not None]
        dominance: Optional[float] = None
        if valid_ips:
            total_ip = sum(valid_ips)
            max_ip = max(valid_ips)
            if total_ip > 0:
                dominance = round(max_ip / total_ip, 6)

        # cmp_field_depth: std of gains_carriere
        gains_clean = [g for g in gains_list if g is not None]
        field_depth = _stdev(gains_clean)
        if field_depth is not None:
            field_depth = round(field_depth, 2)

        # Favourite cote (lowest valid cote)
        valid_cotes = [c for c in cotes if c is not None and c > 0]
        fav_cote: Optional[float] = min(valid_cotes) if valid_cotes else None

        # cmp_chalenger_count: horses within 2x of favourite's cote
        chalenger_count: Optional[int] = None
        if fav_cote is not None and fav_cote > 0:
            threshold = fav_cote * 2.0
            chalenger_count = sum(
                1 for c in valid_cotes if c <= threshold and c != fav_cote
            )

        # Max gains for class leader detection
        max_gains: Optional[float] = max(gains_clean) if gains_clean else None

        # -- Per-runner features --
        for i, r in enumerate(runners):
            # cmp_horse_is_class_leader
            is_leader: Optional[int] = None
            if r["gains"] is not None and max_gains is not None:
                is_leader = 1 if r["gains"] >= max_gains and max_gains > 0 else 0

            # cmp_separation_from_favorite
            sep: Optional[float] = None
            if r["cote"] is not None and fav_cote is not None and fav_cote > 0:
                sep = round(abs(r["cote"] - fav_cote) / fav_cote, 6)

            feat: dict[str, Any] = {
                "partant_uid": r["uid"],
                "course_uid": r["course_uid"],
                "date_reunion_iso": r["date"],
                "cmp_nb_strong_competitors": nb_strong,
                "cmp_nb_debutants": nb_deb,
                "cmp_horse_rank_by_wins": rank_wins[i],
                "cmp_horse_rank_by_gains": rank_gains[i],
                "cmp_horse_rank_by_age": rank_age[i],
                "cmp_dominance_score": dominance,
                "cmp_field_depth": field_depth,
                "cmp_chalenger_count": chalenger_count,
                "cmp_horse_is_class_leader": is_leader,
                "cmp_separation_from_favorite": sep,
            }
            results.append(feat)

        n_courses_done += 1
        if (n_courses_done % 10_000) == 0:
            logger.info("  Pass 2 -- %d courses traitees...", n_courses_done)

        # gc every 500K results
        if len(results) % _LOG_EVERY < field_size:
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Competition analysis build termine: %d features, %d courses en %.1fs",
        len(results), len(course_fields), elapsed,
    )
    gc.collect()
    return results


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features competition analysis a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=str(INPUT_PATH),
        help="Chemin vers partants_master.jsonl",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("competition_analysis_builder")
    logger.info("=" * 70)
    logger.info("competition_analysis_builder.py -- Competition dynamics features")
    logger.info("=" * 70)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_competition_analysis_features(input_path, logger)

    # Save (save_jsonl uses .tmp + rename, newline="\n")
    out_path = output_dir / "competition_analysis_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rates
    if results:
        feature_keys = [k for k in results[0] if k not in ("partant_uid", "course_uid", "date_reunion_iso")]
        total_count = len(results)
        logger.info("=== Fill rates (%d features) ===", len(feature_keys))
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info("  %s: %d/%d (%.1f%%)", k, filled, total_count, 100 * filled / total_count)

    logger.info("Termine -- %d partants traites", len(results))


if __name__ == "__main__":
    main()
