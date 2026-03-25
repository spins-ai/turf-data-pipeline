#!/usr/bin/env python3
"""
feature_builders.elo_rating_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Adaptive Elo ratings for horses, jockeys and trainers.

Reads partants_master.jsonl in streaming mode (16 GB), processes all records
chronologically, and computes per-partant Elo-based features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the Elo rating — no future leakage.

Produces:
  - elo_ratings.jsonl   in output/elo_ratings/

Features per partant:
  - elo_cheval          : horse Elo at race time
  - elo_jockey          : jockey Elo at race time
  - elo_entraineur      : trainer Elo at race time
  - elo_combined        : weighted combination (60/25/15)
  - elo_cheval_delta    : change since horse's last race
  - nb_races_elo        : number of past races for horse (experience)
  - elo_discipline      : horse Elo within discipline (trot or galop)
  - elo_surface         : horse Elo on this surface type (herbe or cendree)

Usage:
    python feature_builders/elo_rating_builder.py
    python feature_builders/elo_rating_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "elo_ratings"

# Elo parameters
BASE_ELO = 1500.0
K_EARLY = 32      # first 10 races
K_MID = 24        # races 10-30
K_LATE = 16       # after 30 races
COMBINED_WEIGHTS = (0.60, 0.25, 0.15)  # horse, jockey, trainer

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# ELO ENGINE
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

    __slots__ = ("rating", "nb_races", "prev_rating")

    def __init__(self) -> None:
        self.rating: float = BASE_ELO
        self.nb_races: int = 0
        self.prev_rating: float = BASE_ELO


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
        rec.get("date_reunion_iso", ""),
        rec.get("course_uid", ""),
        rec.get("num_pmu", 0) or 0,
    )


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_elo_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build Elo rating features from partants_master.jsonl.

    Two-pass approach to keep RAM under control:
      1. First pass: read only sort keys + identifiers into a lightweight list
         to determine chronological order.
      2. Second pass: process in chronological order, computing Elo updates.

    Actually, since we need to group by course for opponent-average calculation,
    we use a single-pass approach: read all records with only the fields we need,
    sort in memory, then process course-by-course.

    Memory budget (~3.5 GB ceiling):
      - Lightweight records: ~16M records * ~200 bytes = ~3.2 GB
      - Elo dicts: ~390K entities * ~80 bytes = ~31 MB
      - Output accumulator: written in streaming mode
    """
    logger.info("=== Elo Rating Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields into memory ──
    # We only keep what we need for Elo computation to limit RAM.
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        discipline_raw = (rec.get("discipline") or "").upper().strip()
        surface_raw = (rec.get("type_piste") or "").upper().strip()

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "jockey": rec.get("jockey_driver"),
            "entraineur": rec.get("entraineur"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": rec.get("position_arrivee"),
            "discipline": discipline_raw if discipline_raw else None,
            "surface": surface_raw if surface_raw else None,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process course by course ──
    t2 = time.time()
    horse_elo: dict[str, _EloState] = defaultdict(_EloState)
    jockey_elo: dict[str, _EloState] = defaultdict(_EloState)
    trainer_elo: dict[str, _EloState] = defaultdict(_EloState)
    # Discipline-specific Elo: key = (horse_name, discipline)
    horse_disc_elo: dict[tuple, _EloState] = defaultdict(_EloState)
    # Surface-specific Elo: key = (horse_name, surface)
    horse_surf_elo: dict[tuple, _EloState] = defaultdict(_EloState)

    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by course_uid (records are sorted by date+course so consecutive)
    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants for this course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while i < total and slim_records[i]["course"] == course_uid and slim_records[i]["date"] == course_date:
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # Skip courses with no valid identifier
        if not course_uid:
            for rec in course_group:
                results.append({
                    "partant_uid": rec["uid"],
                    "elo_cheval": None,
                    "elo_jockey": None,
                    "elo_entraineur": None,
                    "elo_combined": None,
                    "elo_cheval_delta": None,
                    "nb_races_elo": None,
                    "elo_discipline": None,
                    "elo_surface": None,
                })
            continue

        # ── Snapshot pre-race Elo for all partants ──
        pre_race: list[dict[str, Any]] = []
        horse_ratings = []
        # Determine discipline/surface for this course (all runners share same course)
        course_discipline = course_group[0].get("discipline")
        course_surface = course_group[0].get("surface")

        for rec in course_group:
            h = rec["cheval"]
            j = rec["jockey"]
            t = rec["entraineur"]

            h_elo = horse_elo[h].rating if h else BASE_ELO
            j_elo = jockey_elo[j].rating if j else BASE_ELO
            t_elo = trainer_elo[t].rating if t else BASE_ELO

            h_nb = horse_elo[h].nb_races if h else 0
            h_prev = horse_elo[h].prev_rating if h else BASE_ELO

            # Discipline-specific Elo
            disc_elo = None
            if h and course_discipline:
                disc_key = (h, course_discipline)
                disc_state = horse_disc_elo[disc_key]
                disc_elo = disc_state.rating if disc_state.nb_races > 0 else None

            # Surface-specific Elo
            surf_elo = None
            if h and course_surface:
                surf_key = (h, course_surface)
                surf_state = horse_surf_elo[surf_key]
                surf_elo = surf_state.rating if surf_state.nb_races > 0 else None

            combined = (
                COMBINED_WEIGHTS[0] * h_elo
                + COMBINED_WEIGHTS[1] * j_elo
                + COMBINED_WEIGHTS[2] * t_elo
            )
            delta = h_elo - h_prev if h_nb > 0 else None

            pre_race.append({
                "rec": rec,
                "h_elo": h_elo,
                "j_elo": j_elo,
                "t_elo": t_elo,
                "combined": round(combined, 2),
                "delta": round(delta, 2) if delta is not None else None,
                "nb_races": h_nb,
                "disc_elo": round(disc_elo, 2) if disc_elo is not None else None,
                "surf_elo": round(surf_elo, 2) if surf_elo is not None else None,
            })
            horse_ratings.append(h_elo)

        # Emit features (pre-race snapshot — no leakage)
        for pr in pre_race:
            results.append({
                "partant_uid": pr["rec"]["uid"],
                "elo_cheval": round(pr["h_elo"], 2),
                "elo_jockey": round(pr["j_elo"], 2),
                "elo_entraineur": round(pr["t_elo"], 2),
                "elo_combined": pr["combined"],
                "elo_cheval_delta": pr["delta"],
                "nb_races_elo": pr["nb_races"],
                "elo_discipline": pr["disc_elo"],
                "elo_surface": pr["surf_elo"],
            })

        # ── Update Elo ratings after race ──
        n_runners = len(course_group)
        if n_runners < 2:
            # Solo runner — just increment race count, no rating change
            for pr in pre_race:
                rec = pr["rec"]
                if rec["cheval"]:
                    horse_elo[rec["cheval"]].prev_rating = horse_elo[rec["cheval"]].rating
                    horse_elo[rec["cheval"]].nb_races += 1
                    if course_discipline:
                        horse_disc_elo[(rec["cheval"], course_discipline)].nb_races += 1
                    if course_surface:
                        horse_surf_elo[(rec["cheval"], course_surface)].nb_races += 1
                if rec["jockey"]:
                    jockey_elo[rec["jockey"]].nb_races += 1
                if rec["entraineur"]:
                    trainer_elo[rec["entraineur"]].nb_races += 1
            n_processed += n_runners
            continue

        # Compute opponent averages and update
        total_horse_elo = sum(horse_ratings)

        for pr in pre_race:
            rec = pr["rec"]
            is_winner = rec["gagnant"]

            # --- Horse Elo update ---
            if rec["cheval"]:
                h_state = horse_elo[rec["cheval"]]
                opp_avg = (total_horse_elo - pr["h_elo"]) / (n_runners - 1)
                expected = _expected_score(pr["h_elo"], opp_avg)
                k = _get_k(h_state.nb_races)
                actual = 1.0 if is_winner else 0.0
                h_state.prev_rating = h_state.rating
                h_state.rating += k * (actual - expected)
                h_state.nb_races += 1

                # --- Discipline-specific Elo update ---
                if course_discipline:
                    d_state = horse_disc_elo[(rec["cheval"], course_discipline)]
                    d_k = _get_k(d_state.nb_races)
                    d_expected = _expected_score(d_state.rating, opp_avg)
                    d_state.prev_rating = d_state.rating
                    d_state.rating += d_k * (actual - d_expected)
                    d_state.nb_races += 1

                # --- Surface-specific Elo update ---
                if course_surface:
                    s_state = horse_surf_elo[(rec["cheval"], course_surface)]
                    s_k = _get_k(s_state.nb_races)
                    s_expected = _expected_score(s_state.rating, opp_avg)
                    s_state.prev_rating = s_state.rating
                    s_state.rating += s_k * (actual - s_expected)
                    s_state.nb_races += 1

            # --- Jockey Elo update ---
            if rec["jockey"]:
                j_state = jockey_elo[rec["jockey"]]
                # Use jockey ratings of opponents
                j_ratings = [p["j_elo"] for p in pre_race if p["rec"]["uid"] != rec["uid"]]
                j_opp_avg = sum(j_ratings) / len(j_ratings) if j_ratings else BASE_ELO
                j_expected = _expected_score(pr["j_elo"], j_opp_avg)
                j_k = _get_k(j_state.nb_races)
                j_actual = 1.0 if is_winner else 0.0
                j_state.prev_rating = j_state.rating
                j_state.rating += j_k * (j_actual - j_expected)
                j_state.nb_races += 1

            # --- Trainer Elo update ---
            if rec["entraineur"]:
                t_state = trainer_elo[rec["entraineur"]]
                t_ratings = [p["t_elo"] for p in pre_race if p["rec"]["uid"] != rec["uid"]]
                t_opp_avg = sum(t_ratings) / len(t_ratings) if t_ratings else BASE_ELO
                t_expected = _expected_score(pr["t_elo"], t_opp_avg)
                t_k = _get_k(t_state.nb_races)
                t_actual = 1.0 if is_winner else 0.0
                t_state.prev_rating = t_state.rating
                t_state.rating += t_k * (t_actual - t_expected)
                t_state.nb_races += 1

        n_processed += n_runners
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Elo build termine: %d features en %.1fs (chevaux: %d, jockeys: %d, entraineurs: %d)",
        len(results), elapsed,
        len(horse_elo), len(jockey_elo), len(trainer_elo),
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
        description="Construction des features Elo a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/elo_ratings/)",
    )
    args = parser.parse_args()

    logger = setup_logging("elo_rating_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_elo_features(input_path, logger)

    # Save
    out_path = output_dir / "elo_ratings.jsonl"
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
