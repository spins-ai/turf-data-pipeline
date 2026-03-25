#!/usr/bin/env python3
"""
feature_builders.combo_triple_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
5 triple/double combo win-rate features combining entity pairs/triples.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant combo features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - combo_triple_features.jsonl   in output/combo_triple_features/

Features per partant:
  - jockey_distance_terrain_wr   : jockey x distance_bucket x terrain win rate
  - trainer_hippo_discipline_wr  : trainer x hippodrome x discipline win rate
  - age_sex_distance_wr          : age x sex x distance_bucket win rate
  - horse_season_wr              : horse x season (quarter) win rate
  - jockey_corde_wr              : jockey x corde (rope/rail) win rate

Usage:
    python feature_builders/combo_triple_builder.py
    python feature_builders/combo_triple_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "combo_triple_features"

_MIN_OBS = 3
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


class _WinRunCounter:
    """Lightweight counter for wins and total runs."""

    __slots__ = ("wins", "runs")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0

    def rate(self) -> Optional[float]:
        if self.runs < _MIN_OBS:
            return None
        return round(self.wins / self.runs, 4)

    def update(self, is_winner: bool) -> None:
        self.runs += 1
        if is_winner:
            self.wins += 1


def _distance_bucket(distance: Optional[int]) -> str:
    """Bucket distance into categories."""
    if distance is None:
        return "unknown"
    if distance <= 1200:
        return "sprint"
    if distance <= 1600:
        return "mile"
    if distance <= 2200:
        return "inter"
    if distance <= 3000:
        return "long"
    return "marathon"


def _month_to_season(month: Optional[int]) -> str:
    """Convert month (1-12) to season label."""
    if month is None:
        return "unknown"
    if month in (12, 1, 2):
        return "hiver"
    if month in (3, 4, 5):
        return "printemps"
    if month in (6, 7, 8):
        return "ete"
    return "automne"


def _normalise_corde(corde_raw) -> str:
    """Normalise corde (rope/rail) to 'droite' or 'gauche' or 'unknown'."""
    if not corde_raw:
        return "unknown"
    c = str(corde_raw).lower().strip()
    if "droit" in c:
        return "droite"
    if "gauch" in c:
        return "gauche"
    return "unknown"


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_combo_triple_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build 5 combo triple/double features from partants_master.jsonl."""
    logger.info("=== Combo Triple Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        distance = rec.get("distance")
        try:
            distance = int(distance) if distance is not None else None
        except (ValueError, TypeError):
            distance = None

        date_iso = rec.get("date_reunion_iso", "")
        month = None
        if date_iso and len(date_iso) >= 7:
            try:
                month = int(date_iso[5:7])
            except (ValueError, TypeError):
                pass

        age = rec.get("age")
        try:
            age = int(age) if age is not None else None
        except (ValueError, TypeError):
            age = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": date_iso,
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "jockey": rec.get("jockey_driver"),
            "entraineur": rec.get("entraineur"),
            "gagnant": bool(rec.get("is_gagnant")),
            "discipline": (rec.get("discipline") or "").lower().strip(),
            "hippo": (rec.get("hippodrome_normalise") or "").lower().strip(),
            "distance": distance,
            "dist_bucket": _distance_bucket(distance),
            "type_piste": (rec.get("type_piste") or "").lower().strip(),
            "sexe": (rec.get("sexe") or "").lower().strip(),
            "age": age,
            "month": month,
            "season": _month_to_season(month),
            "corde": _normalise_corde(rec.get("corde")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()

    # Accumulators
    jockey_dist_terrain: dict[tuple, _WinRunCounter] = defaultdict(_WinRunCounter)
    trainer_hippo_disc: dict[tuple, _WinRunCounter] = defaultdict(_WinRunCounter)
    age_sex_dist: dict[tuple, _WinRunCounter] = defaultdict(_WinRunCounter)
    horse_season: dict[tuple, _WinRunCounter] = defaultdict(_WinRunCounter)
    jockey_corde: dict[tuple, _WinRunCounter] = defaultdict(_WinRunCounter)

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

        # -- Snapshot pre-race features --
        for rec in course_group:
            jockey = rec["jockey"]
            entraineur = rec["entraineur"]
            cheval = rec["cheval"]
            dist_bucket = rec["dist_bucket"]
            type_piste = rec["type_piste"]
            hippo = rec["hippo"]
            discipline = rec["discipline"]
            age = rec["age"]
            sexe = rec["sexe"]
            season = rec["season"]
            corde = rec["corde"]

            # 1. jockey x distance_bucket x terrain
            f1 = None
            if jockey and dist_bucket != "unknown" and type_piste:
                counter = jockey_dist_terrain.get((jockey, dist_bucket, type_piste))
                if counter:
                    f1 = counter.rate()

            # 2. trainer x hippodrome x discipline
            f2 = None
            if entraineur and hippo and discipline:
                counter = trainer_hippo_disc.get((entraineur, hippo, discipline))
                if counter:
                    f2 = counter.rate()

            # 3. age x sex x distance_bucket
            f3 = None
            if age is not None and sexe and dist_bucket != "unknown":
                counter = age_sex_dist.get((age, sexe, dist_bucket))
                if counter:
                    f3 = counter.rate()

            # 4. horse x season
            f4 = None
            if cheval and season != "unknown":
                counter = horse_season.get((cheval, season))
                if counter:
                    f4 = counter.rate()

            # 5. jockey x corde
            f5 = None
            if jockey and corde != "unknown":
                counter = jockey_corde.get((jockey, corde))
                if counter:
                    f5 = counter.rate()

            results.append({
                "partant_uid": rec["uid"],
                "jockey_distance_terrain_wr": f1,
                "trainer_hippo_discipline_wr": f2,
                "age_sex_distance_wr": f3,
                "horse_season_wr": f4,
                "jockey_corde_wr": f5,
            })

        # -- Update accumulators after race --
        for rec in course_group:
            jockey = rec["jockey"]
            entraineur = rec["entraineur"]
            cheval = rec["cheval"]
            is_winner = rec["gagnant"]
            dist_bucket = rec["dist_bucket"]
            type_piste = rec["type_piste"]
            hippo = rec["hippo"]
            discipline = rec["discipline"]
            age = rec["age"]
            sexe = rec["sexe"]
            season = rec["season"]
            corde = rec["corde"]

            if jockey and dist_bucket != "unknown" and type_piste:
                jockey_dist_terrain[(jockey, dist_bucket, type_piste)].update(is_winner)
            if entraineur and hippo and discipline:
                trainer_hippo_disc[(entraineur, hippo, discipline)].update(is_winner)
            if age is not None and sexe and dist_bucket != "unknown":
                age_sex_dist[(age, sexe, dist_bucket)].update(is_winner)
            if cheval and season != "unknown":
                horse_season[(cheval, season)].update(is_winner)
            if jockey and corde != "unknown":
                jockey_corde[(jockey, corde)].update(is_winner)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Combo triple build termine: %d features en %.1fs",
        len(results), elapsed,
    )
    return results


# ===========================================================================
# CLI
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
        description="Construction des combo triple features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/combo_triple_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("combo_triple_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_combo_triple_features(input_path, logger)

    out_path = output_dir / "combo_triple_features.jsonl"
    save_jsonl(results, out_path, logger)

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
