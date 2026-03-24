#!/usr/bin/env python3
"""
feature_builders.distance_preference_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Evaluates how well each horse performs at different distance categories.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant distance-preference features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - distance_preference.jsonl   in output/distance_preference/

Features per partant:
  - dist_pref_win_rate      : horse win rate at current distance category
  - dist_pref_place_rate    : horse place rate at current distance category
  - dist_pref_advantage     : dist_pref_win_rate / overall win rate (>1 = prefers this distance)
  - dist_pref_nb_runs       : number of past runs at this distance category
  - dist_pref_best_category : distance category where horse has best win rate
                              (1=sprint, 2=mile, 3=intermediate, 4=staying)
  - dist_match_score        : 1.0 if current = best distance, 0.5 if adjacent, 0.0 otherwise

Distance categories:
  sprint       : <1300m
  mile         : 1300-1900m
  intermediate : 1900-2500m
  staying      : 2500m+

Usage:
    python feature_builders/distance_preference_builder.py
    python feature_builders/distance_preference_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "distance_preference"

# Distance category boundaries (in metres)
CAT_SPRINT = 1        # <1300m
CAT_MILE = 2          # 1300-1900m
CAT_INTERMEDIATE = 3  # 1900-2500m
CAT_STAYING = 4       # 2500m+

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# DISTANCE HELPERS
# ===========================================================================


def _distance_category(distance_m: float) -> int:
    """Map a distance in metres to a category code."""
    if distance_m < 1300:
        return CAT_SPRINT
    if distance_m < 1900:
        return CAT_MILE
    if distance_m < 2500:
        return CAT_INTERMEDIATE
    return CAT_STAYING


def _categories_adjacent(cat_a: int, cat_b: int) -> bool:
    """Return True if two categories are adjacent (differ by exactly 1)."""
    return abs(cat_a - cat_b) == 1


# ===========================================================================
# PER-HORSE ACCUMULATOR
# ===========================================================================


class _HorseDistStats:
    """Tracks per-horse statistics across distance categories."""

    __slots__ = ("cat_wins", "cat_places", "cat_runs", "total_wins", "total_runs")

    def __init__(self) -> None:
        # {category_int: count}
        self.cat_wins: dict[int, int] = defaultdict(int)
        self.cat_places: dict[int, int] = defaultdict(int)
        self.cat_runs: dict[int, int] = defaultdict(int)
        self.total_wins: int = 0
        self.total_runs: int = 0

    def win_rate_for(self, cat: int) -> Optional[float]:
        """Win rate at a specific category, or None if no runs."""
        runs = self.cat_runs.get(cat, 0)
        if runs == 0:
            return None
        return self.cat_wins.get(cat, 0) / runs

    def place_rate_for(self, cat: int) -> Optional[float]:
        """Place rate at a specific category, or None if no runs."""
        runs = self.cat_runs.get(cat, 0)
        if runs == 0:
            return None
        return self.cat_places.get(cat, 0) / runs

    def overall_win_rate(self) -> Optional[float]:
        """Overall win rate across all distances."""
        if self.total_runs == 0:
            return None
        return self.total_wins / self.total_runs

    def best_category(self) -> Optional[int]:
        """Category with highest win rate (minimum 1 run). Ties broken by most runs."""
        best_cat = None
        best_wr = -1.0
        best_runs = 0
        for cat in sorted(self.cat_runs.keys()):
            runs = self.cat_runs[cat]
            if runs == 0:
                continue
            wr = self.cat_wins.get(cat, 0) / runs
            if wr > best_wr or (wr == best_wr and runs > best_runs):
                best_wr = wr
                best_cat = cat
                best_runs = runs
        return best_cat

    def record_race(self, cat: int, is_winner: bool, is_placed: bool) -> None:
        """Update statistics after a race."""
        self.cat_runs[cat] += 1
        self.total_runs += 1
        if is_winner:
            self.cat_wins[cat] += 1
            self.total_wins += 1
        if is_placed:
            self.cat_places[cat] += 1


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


def build_distance_preference_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build distance preference features from partants_master.jsonl.

    Single-pass approach: read minimal fields into memory, sort
    chronologically, then process race by race with strict temporal
    integrity (features use only prior race data).
    """
    logger.info("=== Distance Preference Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0
    n_skipped_dist = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Parse distance
        raw_dist = rec.get("distance")
        try:
            dist_m = float(raw_dist) if raw_dist is not None else None
        except (ValueError, TypeError):
            dist_m = None

        if dist_m is None or dist_m <= 0:
            n_skipped_dist += 1
            # Still emit a record with null features
            slim_records.append({
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "cheval": rec.get("nom_cheval"),
                "dist_cat": None,
                "gagnant": bool(rec.get("is_gagnant")),
                "place": _is_placed(rec),
            })
            continue

        slim_records.append({
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "dist_cat": _distance_category(dist_m),
            "gagnant": bool(rec.get("is_gagnant")),
            "place": _is_placed(rec),
        })

    logger.info(
        "Phase 1 terminee: %d records en %.1fs (%d sans distance valide)",
        len(slim_records), time.time() - t0, n_skipped_dist,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process race by race --
    t2 = time.time()
    horse_stats: dict[str, _HorseDistStats] = defaultdict(_HorseDistStats)
    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by course (date+course consecutive after sort)
    i = 0
    total = len(slim_records)

    while i < total:
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

        # -- Snapshot pre-race features for all partants --
        for rec in course_group:
            cheval = rec["cheval"]
            dist_cat = rec["dist_cat"]

            if not cheval or dist_cat is None:
                results.append(_null_features(rec["uid"]))
                continue

            stats = horse_stats[cheval]
            wr = stats.win_rate_for(dist_cat)
            pr = stats.place_rate_for(dist_cat)
            overall_wr = stats.overall_win_rate()
            nb_runs = stats.cat_runs.get(dist_cat, 0)
            best_cat = stats.best_category()

            # Compute advantage
            if wr is not None and overall_wr is not None and overall_wr > 0:
                advantage = round(wr / overall_wr, 4)
            else:
                advantage = None

            # Compute match score
            if best_cat is not None and dist_cat is not None:
                if dist_cat == best_cat:
                    match_score = 1.0
                elif _categories_adjacent(dist_cat, best_cat):
                    match_score = 0.5
                else:
                    match_score = 0.0
            else:
                match_score = None

            results.append({
                "partant_uid": rec["uid"],
                "dist_pref_win_rate": round(wr, 4) if wr is not None else None,
                "dist_pref_place_rate": round(pr, 4) if pr is not None else None,
                "dist_pref_advantage": advantage,
                "dist_pref_nb_runs": nb_runs,
                "dist_pref_best_category": best_cat,
                "dist_match_score": match_score,
            })

        # -- Update stats after race (post-race, preserves temporal integrity) --
        for rec in course_group:
            cheval = rec["cheval"]
            dist_cat = rec["dist_cat"]
            if cheval and dist_cat is not None:
                horse_stats[cheval].record_race(
                    dist_cat, rec["gagnant"], rec["place"]
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Distance preference build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_stats),
    )

    return results


def _is_placed(rec: dict) -> bool:
    """Determine if a partant is placed (top 3) based on available fields."""
    if rec.get("is_place"):
        return True
    pos = rec.get("position_arrivee")
    if pos is not None:
        try:
            return int(pos) <= 3
        except (ValueError, TypeError):
            pass
    return False


def _null_features(uid) -> dict[str, Any]:
    """Return a feature dict with all null values."""
    return {
        "partant_uid": uid,
        "dist_pref_win_rate": None,
        "dist_pref_place_rate": None,
        "dist_pref_advantage": None,
        "dist_pref_nb_runs": None,
        "dist_pref_best_category": None,
        "dist_match_score": None,
    }


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
        description="Construction des features de preference de distance a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/distance_preference/)",
    )
    args = parser.parse_args()

    logger = setup_logging("distance_preference_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_distance_preference_features(input_path, logger)

    # Save
    out_path = output_dir / "distance_preference.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_r = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_r, 100 * v / total_r)


if __name__ == "__main__":
    main()
