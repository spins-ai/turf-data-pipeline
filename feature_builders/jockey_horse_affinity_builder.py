#!/usr/bin/env python3
"""
feature_builders.jockey_horse_affinity_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Evaluates how well specific jockey-horse combinations perform together.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant jockey-horse affinity features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the affinity stats -- no future leakage.

Produces:
  - jockey_horse_affinity.jsonl   in output/jockey_horse_affinity/

Features per partant:
  - jh_combo_win_rate   : win rate of this jockey on this specific horse
  - jh_combo_nb_rides   : number of times this jockey has ridden this horse before
  - jh_combo_place_rate : place rate (top 3) of this jockey-horse combo
  - jh_is_regular       : 1 if jockey has ridden this horse 3+ times, 0 otherwise
  - jh_first_time       : 1 if this is the first time this jockey rides this horse

Usage:
    python feature_builders/jockey_horse_affinity_builder.py
    python feature_builders/jockey_horse_affinity_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "jockey_horse_affinity"

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# COMBO STATE
# ===========================================================================


class _ComboState:
    """Lightweight per jockey-horse combination tracker."""

    __slots__ = ("wins", "places", "rides")

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.rides: int = 0


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


def build_jockey_horse_affinity(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build jockey-horse affinity features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory.
      2. Sort chronologically for strict temporal ordering.
      3. Process record by record, snapshotting combo stats before updating.

    Temporal integrity: features reflect only races strictly before the
    current record's date -- no same-day leakage within a course group.
    """
    logger.info("=== Jockey-Horse Affinity Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        position = rec.get("position_arrivee")
        try:
            position_int = int(position) if position is not None else None
        except (ValueError, TypeError):
            position_int = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "jockey": rec.get("nom_jockey"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": position_int,
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

    # -- Phase 3: Process course by course --
    t2 = time.time()
    combo_state: dict[tuple[str, str], _ComboState] = defaultdict(_ComboState)

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

        # -- Snapshot pre-race stats for all partants (temporal integrity) --
        pre_race_features: list[dict[str, Any]] = []

        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]

            if cheval and jockey:
                key = (jockey, cheval)
                state = combo_state[key]
                nb_rides = state.rides
                wins = state.wins
                places = state.places

                win_rate = round(wins / nb_rides, 4) if nb_rides > 0 else None
                place_rate = round(places / nb_rides, 4) if nb_rides > 0 else None
                is_regular = 1 if nb_rides >= 3 else 0
                first_time = 1 if nb_rides == 0 else 0

                pre_race_features.append({
                    "partant_uid": rec["uid"],
                    "jh_combo_win_rate": win_rate,
                    "jh_combo_nb_rides": nb_rides,
                    "jh_combo_place_rate": place_rate,
                    "jh_is_regular": is_regular,
                    "jh_first_time": first_time,
                })
            else:
                pre_race_features.append({
                    "partant_uid": rec["uid"],
                    "jh_combo_win_rate": None,
                    "jh_combo_nb_rides": None,
                    "jh_combo_place_rate": None,
                    "jh_is_regular": None,
                    "jh_first_time": None,
                })

        # Emit features (pre-race snapshot -- no leakage)
        results.extend(pre_race_features)

        # -- Update combo stats after race --
        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]

            if not cheval or not jockey:
                continue

            key = (jockey, cheval)
            state = combo_state[key]
            state.rides += 1
            if rec["gagnant"]:
                state.wins += 1
            pos = rec["position"]
            if pos is not None and pos <= 3:
                state.places += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Affinity build termine: %d features en %.1fs (combos uniques: %d)",
        len(results), elapsed, len(combo_state),
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
        description="Construction des features affinite jockey-cheval a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/jockey_horse_affinity/)",
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_horse_affinity_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_jockey_horse_affinity(input_path, logger)

    # Save
    out_path = output_dir / "jockey_horse_affinity.jsonl"
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
