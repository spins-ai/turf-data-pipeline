#!/usr/bin/env python3
"""
feature_builders.trainer_distance_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer distance specialization features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant trainer-distance features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - trainer_distance.jsonl   in output/trainer_distance/

Features per partant:
  - trainer_sprint_win_rate   : win rate entraineur sur <1300m
  - trainer_mile_win_rate     : sur 1300-1900m
  - trainer_staying_win_rate  : sur >2500m
  - trainer_distance_match    : 1 si meilleure categorie = course actuelle

Usage:
    python feature_builders/trainer_distance_builder.py
    python feature_builders/trainer_distance_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "trainer_distance"

_LOG_EVERY = 500_000

# Distance categories (in metres)
_SPRINT_MAX = 1300
_MILE_MIN = 1300
_MILE_MAX = 1900
# Middle distance = 1900-2500 (not a feature but fills the gap)
_STAYING_MIN = 2500


def _distance_category(distance: float) -> Optional[str]:
    """Classify distance into sprint/mile/middle/staying."""
    if distance < _SPRINT_MAX:
        return "sprint"
    elif distance <= _MILE_MAX:
        return "mile"
    elif distance <= _STAYING_MIN:
        return "middle"
    else:
        return "staying"


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
        return v if v == v else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PER-TRAINER STATE
# ===========================================================================


class _TrainerDistState:
    """Per-trainer accumulated state for distance specialization."""

    __slots__ = ("wins", "runs")

    def __init__(self) -> None:
        # {category: count}
        self.wins: dict[str, int] = defaultdict(int)
        self.runs: dict[str, int] = defaultdict(int)

    def win_rate(self, category: str) -> Optional[float]:
        """Win rate for a distance category. None if no runs."""
        r = self.runs.get(category, 0)
        if r == 0:
            return None
        return round(self.wins.get(category, 0) / r, 4)

    def best_category(self) -> Optional[str]:
        """Category with the highest win rate (min 3 runs)."""
        best_cat = None
        best_rate = -1.0
        for cat in ("sprint", "mile", "middle", "staying"):
            r = self.runs.get(cat, 0)
            if r < 3:
                continue
            rate = self.wins.get(cat, 0) / r
            if rate > best_rate:
                best_rate = rate
                best_cat = cat
        return best_cat

    def snapshot(self, current_category: Optional[str]) -> dict[str, Any]:
        """Compute features using only past races (strict temporal)."""
        sprint_wr = self.win_rate("sprint")
        mile_wr = self.win_rate("mile")
        staying_wr = self.win_rate("staying")

        best = self.best_category()
        match = None
        if best is not None and current_category is not None:
            match = 1 if best == current_category else 0

        return {
            "trainer_sprint_win_rate": sprint_wr,
            "trainer_mile_win_rate": mile_wr,
            "trainer_staying_win_rate": staying_wr,
            "trainer_distance_match": match,
        }

    def update(self, category: Optional[str], is_winner: bool) -> None:
        """Update state with a new race result (post-race)."""
        if category is None:
            return
        self.runs[category] += 1
        if is_winner:
            self.wins[category] += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_trainer_distance_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build trainer distance features from partants_master.jsonl."""
    logger.info("=== Trainer Distance Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        distance = _safe_float(rec.get("distance"))
        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "entraineur": (rec.get("entraineur") or "").strip(),
            "distance": distance,
            "dist_cat": _distance_category(distance) if distance and distance > 0 else None,
            "is_gagnant": bool(rec.get("is_gagnant")),
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

    # -- Phase 3: Process record by record --
    t2 = time.time()
    trainer_states: dict[str, _TrainerDistState] = defaultdict(_TrainerDistState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        # -- Snapshot pre-race features --
        for rec in course_group:
            entraineur = rec["entraineur"]
            dist_cat = rec["dist_cat"]

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "trainer_sprint_win_rate": None,
                "trainer_mile_win_rate": None,
                "trainer_staying_win_rate": None,
                "trainer_distance_match": None,
            }

            if entraineur:
                state = trainer_states[entraineur]
                snap = state.snapshot(dist_cat)
                features.update(snap)

            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            entraineur = rec["entraineur"]
            if entraineur:
                trainer_states[entraineur].update(rec["dist_cat"], rec["is_gagnant"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Trainer distance build termine: %d features en %.1fs (entraineurs uniques: %d)",
        len(results), elapsed, len(trainer_states),
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
        description="Construction des features trainer distance a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/trainer_distance/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_distance_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_trainer_distance_features(input_path, logger)

    # Save
    out_path = output_dir / "trainer_distance.jsonl"
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
