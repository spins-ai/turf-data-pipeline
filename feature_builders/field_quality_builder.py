#!/usr/bin/env python3
"""
feature_builders.field_quality_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Field quality features based on Elo ratings of the peloton.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, computes Elo ratings (same engine as elo_rating_builder),
then derives field-level quality metrics per partant.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the Elo ratings -- no future leakage.

Requires Elo computation inline (no dependency on pre-computed Elo file)
to guarantee temporal consistency.

Produces:
  - field_quality.jsonl   in output/field_quality/

Features per partant:
  - field_elo_mean           : Elo moyen du peloton
  - field_elo_std            : ecart-type Elo du peloton
  - field_nb_outsiders       : nb chevaux avec Elo < 1400 dans le peloton
  - field_nb_class_horses    : nb chevaux avec Elo > 1600
  - horse_elo_rank_in_field  : rang du cheval par Elo dans ce peloton (1 = best)

Usage:
    python feature_builders/field_quality_builder.py
    python feature_builders/field_quality_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "field_quality"

# Elo parameters (same as elo_rating_builder for consistency)
BASE_ELO = 1500.0
K_EARLY = 32
K_MID = 24
K_LATE = 16

# Field quality thresholds
OUTSIDER_THRESHOLD = 1400.0
CLASS_THRESHOLD = 1600.0

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# ELO ENGINE (inline to guarantee temporal consistency)
# ===========================================================================


def _get_k(nb_races: int) -> float:
    """Adaptive K-factor based on experience."""
    if nb_races < 10:
        return K_EARLY
    if nb_races < 30:
        return K_MID
    return K_LATE


def _expected_score(rating: float, opponent_avg: float) -> float:
    """Standard Elo expected score."""
    return 1.0 / (1.0 + 10.0 ** ((opponent_avg - rating) / 400.0))


class _EloState:
    """Lightweight per-entity Elo tracker."""

    __slots__ = ("rating", "nb_races")

    def __init__(self) -> None:
        self.rating: float = BASE_ELO
        self.nb_races: int = 0


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
# MAIN BUILD
# ===========================================================================


def build_field_quality_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build field quality features from partants_master.jsonl.

    Two-step approach:
      1. Read all records with minimal fields, sort chronologically.
      2. Process course-by-course: snapshot pre-race Elo for field metrics,
         then update Elo after race.
    """
    logger.info("=== Field Quality Builder ===")
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
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_elo: dict[str, _EloState] = defaultdict(_EloState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

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

        # Skip courses with no valid identifier
        if not course_uid:
            for rec in course_group:
                results.append({
                    "partant_uid": rec["uid"],
                    "field_elo_mean": None,
                    "field_elo_std": None,
                    "field_nb_outsiders": None,
                    "field_nb_class_horses": None,
                    "horse_elo_rank_in_field": None,
                })
            n_processed += len(course_group)
            continue

        # -- Snapshot pre-race Elo for all partants in this course --
        horse_elos_in_field: list[tuple[str, float]] = []  # (uid, elo)
        pre_race_data: list[dict] = []

        for rec in course_group:
            cheval = rec["cheval"]
            h_elo = horse_elo[cheval].rating if cheval else BASE_ELO
            horse_elos_in_field.append((rec["uid"], h_elo))
            pre_race_data.append({
                "rec": rec,
                "h_elo": h_elo,
            })

        # -- Compute field-level statistics --
        elo_values = [e for _, e in horse_elos_in_field]
        n_field = len(elo_values)

        if n_field > 0:
            field_mean = sum(elo_values) / n_field
            if n_field > 1:
                variance = sum((e - field_mean) ** 2 for e in elo_values) / (n_field - 1)
                field_std = math.sqrt(variance)
            else:
                field_std = 0.0
            nb_outsiders = sum(1 for e in elo_values if e < OUTSIDER_THRESHOLD)
            nb_class = sum(1 for e in elo_values if e > CLASS_THRESHOLD)

            # Rank by Elo descending (rank 1 = highest Elo)
            sorted_elos = sorted(horse_elos_in_field, key=lambda x: -x[1])
            uid_to_rank = {}
            for rank_idx, (uid, _) in enumerate(sorted_elos, start=1):
                uid_to_rank[uid] = rank_idx
        else:
            field_mean = None
            field_std = None
            nb_outsiders = None
            nb_class = None
            uid_to_rank = {}

        # -- Emit features --
        for pr in pre_race_data:
            rec = pr["rec"]
            rank = uid_to_rank.get(rec["uid"])

            results.append({
                "partant_uid": rec["uid"],
                "field_elo_mean": round(field_mean, 2) if field_mean is not None else None,
                "field_elo_std": round(field_std, 2) if field_std is not None else None,
                "field_nb_outsiders": nb_outsiders,
                "field_nb_class_horses": nb_class,
                "horse_elo_rank_in_field": rank,
            })

        # -- Update Elo after race --
        n_runners = len(course_group)
        if n_runners >= 2:
            total_elo = sum(pr["h_elo"] for pr in pre_race_data)

            for pr in pre_race_data:
                rec = pr["rec"]
                cheval = rec["cheval"]
                if not cheval:
                    continue

                h_state = horse_elo[cheval]
                opp_avg = (total_elo - pr["h_elo"]) / (n_runners - 1)
                expected = _expected_score(pr["h_elo"], opp_avg)
                k = _get_k(h_state.nb_races)
                actual = 1.0 if rec["gagnant"] else 0.0
                h_state.rating += k * (actual - expected)
                h_state.nb_races += 1
        else:
            # Solo runner -- just increment race count
            for pr in pre_race_data:
                cheval = pr["rec"]["cheval"]
                if cheval:
                    horse_elo[cheval].nb_races += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Field quality build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_elo),
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
        description="Construction des field quality features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/field_quality/)",
    )
    args = parser.parse_args()

    logger = setup_logging("field_quality_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_field_quality_features(input_path, logger)

    # Save
    out_path = output_dir / "field_quality.jsonl"
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
