#!/usr/bin/env python3
"""
feature_builders.career_stats_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Career-level cumulative statistics for each horse.

Reads partants_master.jsonl in streaming mode (16 GB), processes all records
chronologically, and computes per-partant career features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the career stats — no future leakage.

Produces:
  - career_stats.jsonl   in output/career_stats/

Features per partant:
  - nb_courses_carriere     : total career race count before this race
  - gains_carriere_total    : total career earnings before this race
  - gains_par_course_moyen  : gains_carriere_total / nb_courses_carriere
  - win_rate_carriere       : career win rate (wins / races)
  - place_rate_carriere     : career place rate (top 3 / races)
  - best_allocation_won     : highest allocation in a race the horse won

Usage:
    python feature_builders/career_stats_builder.py
    python feature_builders/career_stats_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "career_stats"

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# CAREER STATE TRACKER
# ===========================================================================


class _CareerState:
    """Lightweight per-horse career accumulator."""

    __slots__ = (
        "nb_courses",
        "gains_total",
        "wins",
        "places",
        "best_allocation_won",
    )

    def __init__(self) -> None:
        self.nb_courses: int = 0
        self.gains_total: float = 0.0
        self.wins: int = 0
        self.places: int = 0  # top 3
        self.best_allocation_won: float = 0.0


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


def _sort_key(rec: dict) -> tuple:
    """Sort key: date, course_uid, num_pmu for determinism."""
    return (
        rec.get("date", ""),
        rec.get("course", ""),
        rec.get("num", 0) or 0,
    )


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_career_stats_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build career statistics features from partants_master.jsonl.

    Two-phase approach:
      1. Read minimal fields into memory, sort chronologically.
      2. Process in order, accumulating per-horse career stats.
         Features are emitted BEFORE updating state (strict temporal integrity).

    Memory budget:
      - Slim records: ~16M records * ~150 bytes = ~2.4 GB
      - Career dicts: ~390K horses * ~60 bytes = ~23 MB
    """
    logger.info("=== Career Stats Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Parse position_arrivee safely
        pos_raw = rec.get("position_arrivee")
        pos = None
        if pos_raw is not None:
            try:
                pos = int(pos_raw)
            except (ValueError, TypeError):
                pos = None

        # Parse gains safely
        gains_raw = rec.get("gains")
        gains = None
        if gains_raw is not None:
            try:
                gains = float(gains_raw)
            except (ValueError, TypeError):
                gains = None

        # Parse allocation safely
        alloc_raw = rec.get("allocation")
        alloc = None
        if alloc_raw is not None:
            try:
                alloc = float(alloc_raw)
            except (ValueError, TypeError):
                alloc = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": pos,
            "gains": gains,
            "allocation": alloc,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=_sort_key)
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process record by record ──
    t2 = time.time()
    horse_career: dict[str, _CareerState] = defaultdict(_CareerState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by date to ensure strict temporal integrity:
    # all records on date D see only stats from dates < D
    i = 0
    total = len(slim_records)

    while i < total:
        current_date = slim_records[i]["date"]
        date_group: list[dict] = []

        while i < total and slim_records[i]["date"] == current_date:
            date_group.append(slim_records[i])
            i += 1

        # ── Emit features (pre-update snapshot) ──
        for rec in date_group:
            cheval = rec["cheval"]

            if not cheval:
                results.append({
                    "partant_uid": rec["uid"],
                    "nb_courses_carriere": None,
                    "gains_carriere_total": None,
                    "gains_par_course_moyen": None,
                    "win_rate_carriere": None,
                    "place_rate_carriere": None,
                    "best_allocation_won": None,
                })
                continue

            state = horse_career[cheval]
            nb = state.nb_courses

            if nb == 0:
                # First race ever — no career history
                results.append({
                    "partant_uid": rec["uid"],
                    "nb_courses_carriere": 0,
                    "gains_carriere_total": 0.0,
                    "gains_par_course_moyen": None,
                    "win_rate_carriere": None,
                    "place_rate_carriere": None,
                    "best_allocation_won": None,
                })
            else:
                gains_avg = round(state.gains_total / nb, 2)
                win_rate = round(state.wins / nb, 6)
                place_rate = round(state.places / nb, 6)
                best_alloc = state.best_allocation_won if state.best_allocation_won > 0 else None

                results.append({
                    "partant_uid": rec["uid"],
                    "nb_courses_carriere": nb,
                    "gains_carriere_total": round(state.gains_total, 2),
                    "gains_par_course_moyen": gains_avg,
                    "win_rate_carriere": win_rate,
                    "place_rate_carriere": place_rate,
                    "best_allocation_won": best_alloc,
                })

        # ── Update career stats with this date's outcomes ──
        for rec in date_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            state = horse_career[cheval]
            state.nb_courses += 1

            if rec["gains"] is not None:
                state.gains_total += rec["gains"]

            if rec["gagnant"]:
                state.wins += 1
                # Track best allocation won
                if rec["allocation"] is not None and rec["allocation"] > state.best_allocation_won:
                    state.best_allocation_won = rec["allocation"]

            # Place = top 3
            if rec["position"] is not None and 1 <= rec["position"] <= 3:
                state.places += 1

        n_processed += len(date_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Career stats build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        len(horse_career),
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
        description="Construction des features career stats a partir de partants_master"
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
        help="Repertoire de sortie (defaut: output/career_stats/)",
    )
    args = parser.parse_args()

    logger = setup_logging("career_stats_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_career_stats_features(input_path, logger)

    # Save
    out_path = output_dir / "career_stats.jsonl"
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
