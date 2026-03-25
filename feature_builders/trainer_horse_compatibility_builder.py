#!/usr/bin/env python3
"""
feature_builders.trainer_horse_compatibility_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Evaluates how well a specific trainer-horse combination performs together,
plus whether the trainer's speciality matches the current race discipline.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant trainer-horse compatibility features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the compatibility stats -- no future leakage.

Produces:
  - trainer_horse_compatibility.jsonl   in output/trainer_horse_compatibility/

Features per partant:
  - trainer_horse_win_rate      : win rate of this trainer with this specific horse
  - trainer_horse_nb_races      : number of times trainer has trained this horse before
  - trainer_horse_roi           : ROI backing this trainer-horse combo
  - trainer_new_horse           : 1 if trainer has this horse for first time
  - trainer_speciality_match    : 1 if trainer's best discipline = this race's discipline

Usage:
    python feature_builders/trainer_horse_compatibility_builder.py
    python feature_builders/trainer_horse_compatibility_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "trainer_horse_compatibility"

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
# STATE TRACKERS
# ===========================================================================


class _ComboState:
    """Per trainer-horse combination tracker."""

    __slots__ = ("wins", "rides", "roi_sum", "roi_count")

    def __init__(self) -> None:
        self.wins: int = 0
        self.rides: int = 0
        self.roi_sum: float = 0.0
        self.roi_count: int = 0


class _TrainerDisciplineState:
    """Per trainer: tracks wins per discipline to find best discipline."""

    __slots__ = ("wins_by_disc", "total_by_disc")

    def __init__(self) -> None:
        self.wins_by_disc: dict[str, int] = defaultdict(int)
        self.total_by_disc: dict[str, int] = defaultdict(int)

    def best_discipline(self) -> Optional[str]:
        """Return discipline with highest win rate (min 3 races)."""
        best_disc = None
        best_wr = -1.0
        for disc, total in self.total_by_disc.items():
            if total < 3:
                continue
            wr = self.wins_by_disc[disc] / total
            if wr > best_wr:
                best_wr = wr
                best_disc = disc
        return best_disc


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_trainer_horse_compatibility(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build trainer-horse compatibility features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory.
      2. Sort chronologically for strict temporal ordering.
      3. Process record by record, snapshotting combo stats before updating.

    Temporal integrity: features reflect only races strictly before the
    current record's date -- no same-day leakage within a course group.
    """
    logger.info("=== Trainer-Horse Compatibility Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        entraineur = rec.get("nom_entraineur") or rec.get("entraineur")

        odds_val = rec.get("rapport_simple_gagnant")
        if odds_val is not None:
            try:
                odds_val = float(odds_val)
                if odds_val <= 0:
                    odds_val = None
            except (ValueError, TypeError):
                odds_val = None

        discipline = rec.get("discipline") or rec.get("type_course") or ""
        discipline = discipline.strip().upper()

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "entraineur": entraineur,
            "gagnant": bool(rec.get("is_gagnant")),
            "odds": odds_val,
            "discipline": discipline,
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
    trainer_disc_state: dict[str, _TrainerDisciplineState] = defaultdict(_TrainerDisciplineState)

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
            entraineur = rec["entraineur"]
            discipline = rec["discipline"]

            if cheval and entraineur:
                key = (entraineur, cheval)
                state = combo_state[key]
                nb_races = state.rides
                wins = state.wins

                win_rate = round(wins / nb_races, 4) if nb_races > 0 else None
                roi = round(state.roi_sum / state.roi_count, 4) if state.roi_count > 0 else None
                new_horse = 1 if nb_races == 0 else 0

                # Trainer speciality match
                best_disc = trainer_disc_state[entraineur].best_discipline()
                if best_disc and discipline:
                    speciality_match = 1 if best_disc == discipline else 0
                else:
                    speciality_match = None

                pre_race_features.append({
                    "partant_uid": rec["uid"],
                    "trainer_horse_win_rate": win_rate,
                    "trainer_horse_nb_races": nb_races,
                    "trainer_horse_roi": roi,
                    "trainer_new_horse": new_horse,
                    "trainer_speciality_match": speciality_match,
                })
            else:
                pre_race_features.append({
                    "partant_uid": rec["uid"],
                    "trainer_horse_win_rate": None,
                    "trainer_horse_nb_races": None,
                    "trainer_horse_roi": None,
                    "trainer_new_horse": None,
                    "trainer_speciality_match": None,
                })

        # Emit features (pre-race snapshot -- no leakage)
        results.extend(pre_race_features)

        # -- Update states after race --
        for rec in course_group:
            cheval = rec["cheval"]
            entraineur = rec["entraineur"]

            if not cheval or not entraineur:
                continue

            key = (entraineur, cheval)
            state = combo_state[key]
            state.rides += 1
            if rec["gagnant"]:
                state.wins += 1

            # ROI: track profit/loss
            odds = rec["odds"]
            if odds is not None and odds > 0:
                state.roi_count += 1
                if rec["gagnant"]:
                    state.roi_sum += odds - 1.0
                else:
                    state.roi_sum -= 1.0

            # Update trainer discipline stats
            disc = rec["discipline"]
            if disc:
                tds = trainer_disc_state[entraineur]
                tds.total_by_disc[disc] += 1
                if rec["gagnant"]:
                    tds.wins_by_disc[disc] += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Trainer-horse compatibility build termine: %d features en %.1fs (combos: %d, entraineurs: %d)",
        len(results), elapsed, len(combo_state), len(trainer_disc_state),
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
        description="Construction des features compatibilite entraineur-cheval a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/trainer_horse_compatibility/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_horse_compatibility_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_trainer_horse_compatibility(input_path, logger)

    # Save
    out_path = output_dir / "trainer_horse_compatibility.jsonl"
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
