#!/usr/bin/env python3
"""
feature_builders.going_preference_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Evaluates how well each horse performs on different going/terrain types.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant going-preference features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the going statistics -- no future leakage.

Produces:
  - going_preference.jsonl   in output/going_preference/

Features per partant:
  - going_pref_win_rate     : horse's win rate on current terrain type
  - going_pref_place_rate   : horse's place rate on current terrain type
  - going_pref_advantage    : going_pref_win_rate / overall win rate (>1 = prefers this going)
  - going_pref_nb_runs      : number of past runs on this terrain type (confidence)
  - going_pref_best_terrain : terrain with best win rate (encoded as int)
  - going_match_score       : 1.0 if current = best, 0.5 if adjacent, 0.0 if opposite

Usage:
    python feature_builders/going_preference_builder.py
    python feature_builders/going_preference_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "going_preference"

# Terrain encoding for going_pref_best_terrain
TERRAIN_CODES: dict[str, int] = {
    "bon": 1,
    "souple": 2,
    "leger": 3,
    "lourd": 4,
    "collant": 5,
    "tres_souple": 6,
    "inconnu": 0,
}

# Adjacency map: for each terrain, list of terrains considered "adjacent"
# (similar footing). Used for going_match_score = 0.5.
TERRAIN_ADJACENCY: dict[str, set[str]] = {
    "bon": {"leger", "souple"},
    "souple": {"bon", "tres_souple"},
    "leger": {"bon"},
    "lourd": {"collant", "tres_souple"},
    "collant": {"lourd", "tres_souple"},
    "tres_souple": {"souple", "collant", "lourd"},
    "inconnu": set(),
}

# Place threshold: position_arrivee <= PLACE_THRESHOLD counts as placed
PLACE_THRESHOLD = 3

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# TERRAIN NORMALISATION
# ===========================================================================

_TERRAIN_ALIASES: dict[str, str] = {
    "bon": "bon",
    "b": "bon",
    "good": "bon",
    "ferme": "bon",
    "souple": "souple",
    "s": "souple",
    "soft": "souple",
    "assez souple": "souple",
    "leger": "leger",
    "l": "leger",
    "light": "leger",
    "lourd": "lourd",
    "heavy": "lourd",
    "tres lourd": "lourd",
    "collant": "collant",
    "sticky": "collant",
    "tres_souple": "tres_souple",
    "tres souple": "tres_souple",
    "very soft": "tres_souple",
}


def _normalise_terrain(raw: Any) -> str:
    """Normalise a raw terrain value to one of the canonical categories."""
    if not raw or not isinstance(raw, str):
        return "inconnu"
    key = raw.strip().lower().replace("_", " ").replace("-", " ")
    # Direct lookup
    normalised = _TERRAIN_ALIASES.get(key)
    if normalised:
        return normalised
    # Substring matching for compound descriptions
    for alias, canon in _TERRAIN_ALIASES.items():
        if alias in key:
            return canon
    return "inconnu"


def _extract_terrain(rec: dict) -> str:
    """Extract terrain from a partant record, trying multiple fields."""
    for field in ("cnd_cond_type_terrain", "met_terrain_predit", "terrain"):
        val = rec.get(field)
        if val:
            normalised = _normalise_terrain(val)
            if normalised != "inconnu":
                return normalised
    return "inconnu"


# ===========================================================================
# PER-HORSE GOING STATE
# ===========================================================================


class _GoingStats:
    """Tracks per-horse stats across all terrain types."""

    __slots__ = ("terrain_wins", "terrain_places", "terrain_runs",
                 "total_wins", "total_runs")

    def __init__(self) -> None:
        self.terrain_wins: dict[str, int] = defaultdict(int)
        self.terrain_places: dict[str, int] = defaultdict(int)
        self.terrain_runs: dict[str, int] = defaultdict(int)
        self.total_wins: int = 0
        self.total_runs: int = 0


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
# FEATURE COMPUTATION HELPERS
# ===========================================================================


def _compute_match_score(current_terrain: str, best_terrain: str) -> float:
    """Compute going_match_score based on current vs best terrain."""
    if current_terrain == "inconnu" or best_terrain == "inconnu":
        return 0.0
    if current_terrain == best_terrain:
        return 1.0
    adjacent = TERRAIN_ADJACENCY.get(current_terrain, set())
    if best_terrain in adjacent:
        return 0.5
    return 0.0


def _best_terrain(stats: _GoingStats) -> str:
    """Return terrain type where horse has highest win rate, or 'inconnu'."""
    best = "inconnu"
    best_rate = -1.0
    for terrain, runs in stats.terrain_runs.items():
        if terrain == "inconnu" or runs == 0:
            continue
        rate = stats.terrain_wins.get(terrain, 0) / runs
        if rate > best_rate or (rate == best_rate and runs > stats.terrain_runs.get(best, 0)):
            best_rate = rate
            best = terrain
    return best


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_going_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build going preference features from partants_master.jsonl.

    Single-pass approach with in-memory sort:
      1. Read minimal fields into memory.
      2. Sort chronologically for determinism.
      3. Process record-by-record, snapshotting pre-race stats then updating.
    """
    logger.info("=== Going Preference Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        pos = rec.get("position_arrivee")
        try:
            pos = int(pos) if pos is not None else None
        except (ValueError, TypeError):
            pos = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": pos,
            "terrain": _extract_terrain(rec),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process record by record --
    t2 = time.time()
    horse_stats: dict[str, _GoingStats] = defaultdict(_GoingStats)
    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by course (date+course) for batch snapshot then update
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (i < total
               and slim_records[i]["course"] == course_uid
               and slim_records[i]["date"] == course_date):
            course_group.append(slim_records[i])
            i += 1

        # -- Snapshot pre-race features for all partants in this course --
        pre_race_features: list[dict[str, Any]] = []

        for rec in course_group:
            cheval = rec["cheval"]
            terrain = rec["terrain"]

            if not cheval:
                pre_race_features.append({
                    "partant_uid": rec["uid"],
                    "going_pref_win_rate": None,
                    "going_pref_place_rate": None,
                    "going_pref_advantage": None,
                    "going_pref_nb_runs": None,
                    "going_pref_best_terrain": None,
                    "going_match_score": None,
                })
                continue

            stats = horse_stats[cheval]
            t_runs = stats.terrain_runs.get(terrain, 0)
            t_wins = stats.terrain_wins.get(terrain, 0)
            t_places = stats.terrain_places.get(terrain, 0)

            # Win rate on current terrain
            win_rate = (t_wins / t_runs) if t_runs > 0 else None
            place_rate = (t_places / t_runs) if t_runs > 0 else None

            # Overall win rate
            overall_wr = (stats.total_wins / stats.total_runs) if stats.total_runs > 0 else None

            # Advantage ratio
            if win_rate is not None and overall_wr is not None and overall_wr > 0:
                advantage = round(win_rate / overall_wr, 4)
            else:
                advantage = None

            # Best terrain
            best = _best_terrain(stats)
            best_code = TERRAIN_CODES.get(best, 0)

            # Match score
            if stats.total_runs > 0 and terrain != "inconnu":
                match_score = _compute_match_score(terrain, best)
            else:
                match_score = None

            pre_race_features.append({
                "partant_uid": rec["uid"],
                "going_pref_win_rate": round(win_rate, 4) if win_rate is not None else None,
                "going_pref_place_rate": round(place_rate, 4) if place_rate is not None else None,
                "going_pref_advantage": advantage,
                "going_pref_nb_runs": t_runs,
                "going_pref_best_terrain": best_code if stats.total_runs > 0 else None,
                "going_match_score": round(match_score, 4) if match_score is not None else None,
            })

        # Emit features (pre-race snapshot -- no leakage)
        results.extend(pre_race_features)

        # -- Update stats after race --
        for rec in course_group:
            cheval = rec["cheval"]
            if not cheval:
                continue
            terrain = rec["terrain"]
            is_winner = rec["gagnant"]
            pos = rec["position"]
            is_placed = pos is not None and pos <= PLACE_THRESHOLD

            stats = horse_stats[cheval]
            stats.terrain_runs[terrain] += 1
            stats.total_runs += 1

            if is_winner:
                stats.terrain_wins[terrain] += 1
                stats.total_wins += 1

            if is_placed:
                stats.terrain_places[terrain] += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Going preference build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_stats),
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
        description="Construction des features de preference terrain a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/going_preference/)",
    )
    args = parser.parse_args()

    logger = setup_logging("going_preference_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_going_features(input_path, logger)

    # Save
    out_path = output_dir / "going_preference.jsonl"
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
