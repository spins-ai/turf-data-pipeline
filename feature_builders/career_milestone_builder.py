#!/usr/bin/env python3
"""
feature_builders.career_milestone_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Career milestone features capturing where a horse is in its racing lifecycle.

Temporal integrity: for any partant at date D, only races with date < D
contribute to career state -- no future leakage.

Produces:
  - career_milestone_features.jsonl   in output/career_milestone/

Features per partant (5):
  - is_first_10_races       : 1 if horse has fewer than 10 career starts, else 0
  - is_maiden               : 1 if horse has never won before this race, else 0
  - days_since_first_race   : calendar days since the horse's first recorded race
  - total_prize_rank_in_field: rank of this horse's career earnings among today's
                               field (1 = highest earner). Uses race-level ranking.
  - is_career_best_class    : 1 if this race's allocation is the highest the horse
                               has ever faced, else 0

Usage:
    python feature_builders/career_milestone_builder.py
    python feature_builders/career_milestone_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "career_milestone"

_LOG_EVERY = 500_000

# ===========================================================================
# CAREER STATE TRACKER
# ===========================================================================


class _MilestoneState:
    """Per-horse career accumulator for milestone features."""

    __slots__ = ("nb_courses", "wins", "gains_total", "first_date", "max_allocation")

    def __init__(self) -> None:
        self.nb_courses: int = 0
        self.wins: int = 0
        self.gains_total: float = 0.0
        self.first_date: str = ""  # ISO date of first race
        self.max_allocation: float = 0.0  # highest allocation faced


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


def _days_between(date_a: str, date_b: str) -> Optional[int]:
    """Calendar days between two ISO date strings (YYYY-MM-DD). Returns None on error."""
    if not date_a or not date_b:
        return None
    try:
        ya, ma, da = int(date_a[:4]), int(date_a[5:7]), int(date_a[8:10])
        yb, mb, db = int(date_b[:4]), int(date_b[5:7]), int(date_b[8:10])
        from datetime import date as _date

        return (_date(yb, mb, db) - _date(ya, ma, da)).days
    except (ValueError, IndexError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_career_milestone_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build career milestone features from partants_master.jsonl."""
    logger.info("=== Career Milestone Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Parse allocation safely
        alloc_raw = rec.get("allocation")
        alloc = None
        if alloc_raw is not None:
            try:
                alloc = float(alloc_raw)
            except (ValueError, TypeError):
                alloc = None

        # Parse gains safely
        gains_raw = rec.get("gains_carriere_euros") or rec.get("gains")
        gains = 0.0
        if gains_raw is not None:
            try:
                gains = float(gains_raw)
            except (ValueError, TypeError):
                gains = 0.0

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "allocation": alloc,
            "gains_carriere": gains,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process date by date --
    t2 = time.time()
    horse_state: dict[str, _MilestoneState] = defaultdict(_MilestoneState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        current_date = slim_records[i]["date"]
        date_group: list[dict] = []

        while i < total and slim_records[i]["date"] == current_date:
            date_group.append(slim_records[i])
            i += 1

        # Group by course for field-level ranking
        courses: dict[str, list[dict]] = defaultdict(list)
        for rec in date_group:
            courses[rec["course"]].append(rec)

        # -- Emit features (pre-update snapshot) --
        for course_uid, field in courses.items():
            # Compute total_prize_rank within field
            # Collect (gains_total, index) for ranking
            gains_list: list[tuple[float, int]] = []
            for idx, rec in enumerate(field):
                cheval = rec["cheval"]
                if cheval and cheval in horse_state:
                    gains_list.append((horse_state[cheval].gains_total, idx))
                else:
                    gains_list.append((0.0, idx))

            # Rank: highest gains = rank 1
            gains_list.sort(key=lambda x: -x[0])
            rank_map: dict[int, int] = {}
            for rank, (_, idx) in enumerate(gains_list, 1):
                rank_map[idx] = rank

            for idx, rec in enumerate(field):
                cheval = rec["cheval"]

                if not cheval:
                    results.append({
                        "partant_uid": rec["uid"],
                        "is_first_10_races": None,
                        "is_maiden": None,
                        "days_since_first_race": None,
                        "total_prize_rank_in_field": None,
                        "is_career_best_class": None,
                    })
                    continue

                state = horse_state.get(cheval)

                if state is None or state.nb_courses == 0:
                    # Very first race
                    results.append({
                        "partant_uid": rec["uid"],
                        "is_first_10_races": 1,
                        "is_maiden": 1,
                        "days_since_first_race": 0,
                        "total_prize_rank_in_field": rank_map.get(idx),
                        "is_career_best_class": 1,  # first race = best class by default
                    })
                else:
                    nb = state.nb_courses
                    is_first_10 = 1 if nb < 10 else 0
                    is_maiden = 1 if state.wins == 0 else 0
                    days_since = _days_between(state.first_date, current_date)

                    alloc = rec["allocation"]
                    if alloc is not None and state.max_allocation > 0:
                        is_best_class = 1 if alloc > state.max_allocation else 0
                    elif alloc is not None:
                        is_best_class = 1
                    else:
                        is_best_class = None

                    results.append({
                        "partant_uid": rec["uid"],
                        "is_first_10_races": is_first_10,
                        "is_maiden": is_maiden,
                        "days_since_first_race": days_since,
                        "total_prize_rank_in_field": rank_map.get(idx),
                        "is_career_best_class": is_best_class,
                    })

        # -- Update state with this date's outcomes --
        for rec in date_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            state = horse_state[cheval]

            if state.nb_courses == 0:
                state.first_date = current_date

            state.nb_courses += 1

            if rec["gagnant"]:
                state.wins += 1

            state.gains_total += rec["gains_carriere"]

            alloc = rec["allocation"]
            if alloc is not None and alloc > state.max_allocation:
                state.max_allocation = alloc

        n_processed += len(date_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Career milestone build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        len(horse_state),
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
        description="Construction des features career milestone a partir de partants_master"
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
        help="Repertoire de sortie (defaut: output/career_milestone/)",
    )
    args = parser.parse_args()

    logger = setup_logging("career_milestone_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_career_milestone_features(input_path, logger)

    # Save
    out_path = output_dir / "career_milestone_features.jsonl"
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
