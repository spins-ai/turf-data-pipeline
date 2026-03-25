#!/usr/bin/env python3
"""
feature_builders.jockey_trainer_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep jockey-trainer combo analysis plus specialist rates.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant deep jockey/trainer features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - jockey_trainer_deep.jsonl   in output/jockey_trainer_deep/

Features per partant:
  - jt_combo_roi              : ROI of betting on this jockey-trainer combo
  - jt_combo_avg_position     : average finish position of combo
  - jockey_distance_specialist: jockey win rate at this distance category
  - trainer_terrain_specialist: trainer win rate on this terrain type
  - jockey_claiming_expert    : jockey win rate in claiming races
  - trainer_2yo_specialist    : trainer win rate with 2-year-old horses

Usage:
    python feature_builders/jockey_trainer_deep_builder.py
    python feature_builders/jockey_trainer_deep_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "jockey_trainer_deep"

_LOG_EVERY = 500_000

_CLAIMER_RE = re.compile(r"r[eé]clamer|claiming|claimer", re.IGNORECASE)

# Distance categories (metres)
CAT_SPRINT = 1       # <1300m
CAT_MILE = 2         # 1300-1900m
CAT_INTERMEDIATE = 3 # 1900-2500m
CAT_STAYING = 4      # 2500m+


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


def _parse_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _distance_category(distance_m: float) -> int:
    if distance_m < 1300:
        return CAT_SPRINT
    if distance_m < 1900:
        return CAT_MILE
    if distance_m < 2500:
        return CAT_INTERMEDIATE
    return CAT_STAYING


def _is_claiming(conditions: str) -> bool:
    """Return True if conditions text indicates a claiming race."""
    if not conditions:
        return False
    return bool(_CLAIMER_RE.search(conditions))


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# ACCUMULATOR
# ===========================================================================


class _ComboStats:
    """Accumulates stats for a (jockey, trainer) pair."""

    __slots__ = ("wins", "runs", "pos_sum", "pos_count", "roi_sum", "roi_count")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0
        self.pos_sum: float = 0.0
        self.pos_count: int = 0
        self.roi_sum: float = 0.0
        self.roi_count: int = 0

    def snapshot(self) -> dict[str, Any]:
        roi = round(self.roi_sum / self.roi_count, 4) if self.roi_count > 0 else None
        avg_pos = round(self.pos_sum / self.pos_count, 4) if self.pos_count > 0 else None
        return {
            "jt_combo_roi": roi,
            "jt_combo_avg_position": avg_pos,
        }

    def update(self, won: bool, position: Optional[int], odds: Optional[float]) -> None:
        self.runs += 1
        if won:
            self.wins += 1
        if position is not None and position > 0:
            self.pos_sum += position
            self.pos_count += 1
        if odds is not None and odds > 0:
            self.roi_count += 1
            if won:
                self.roi_sum += odds - 1.0
            else:
                self.roi_sum -= 1.0


class _SpecialistStats:
    """Accumulates wins/runs for a (entity, sub-key) pair."""

    __slots__ = ("wins", "runs")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0

    def win_rate(self) -> Optional[float]:
        if self.runs == 0:
            return None
        return round(self.wins / self.runs, 4)

    def update(self, won: bool) -> None:
        self.runs += 1
        if won:
            self.wins += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_jt_deep_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build jockey-trainer deep features from partants_master.jsonl."""
    logger.info("=== Jockey-Trainer Deep Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        jockey = rec.get("nom_jockey")
        trainer = rec.get("nom_entraineur")

        # Distance
        dist_raw = _safe_float(rec.get("distance"))
        dist_cat = _distance_category(dist_raw) if dist_raw is not None and dist_raw > 0 else None

        # Terrain / type_piste
        terrain = rec.get("type_piste") or rec.get("terrain") or ""
        terrain = str(terrain).strip().lower() if terrain else ""

        # Claiming
        conditions = rec.get("cnd_conditions_texte_original") or ""
        claiming = _is_claiming(conditions)

        # Age
        age = _safe_int(rec.get("age"))

        # Position
        position = _safe_int(rec.get("position_arrivee"))

        # Odds
        odds = _safe_float(rec.get("rapport_simple_gagnant"))
        if odds is not None and odds <= 0:
            odds = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "jockey": jockey,
            "trainer": trainer,
            "gagnant": bool(rec.get("is_gagnant")),
            "position": position,
            "odds": odds,
            "dist_cat": dist_cat,
            "terrain": terrain,
            "claiming": claiming,
            "age": age,
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

    # Accumulators
    combo_stats: dict[tuple, _ComboStats] = defaultdict(_ComboStats)
    jockey_dist: dict[tuple, _SpecialistStats] = defaultdict(_SpecialistStats)
    trainer_terrain: dict[tuple, _SpecialistStats] = defaultdict(_SpecialistStats)
    jockey_claiming: dict[str, _SpecialistStats] = defaultdict(_SpecialistStats)
    trainer_2yo: dict[str, _SpecialistStats] = defaultdict(_SpecialistStats)

    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by (date, course) for temporal integrity
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

        # -- Snapshot pre-race features for all partants in this course --
        for rec in course_group:
            jockey = rec["jockey"]
            trainer = rec["trainer"]
            dist_cat = rec["dist_cat"]
            terrain = rec["terrain"]
            claiming = rec["claiming"]

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "jt_combo_roi": None,
                "jt_combo_avg_position": None,
                "jockey_distance_specialist": None,
                "trainer_terrain_specialist": None,
                "jockey_claiming_expert": None,
                "trainer_2yo_specialist": None,
            }

            # Combo features
            if jockey and trainer:
                combo_key = (jockey, trainer)
                state = combo_stats.get(combo_key)
                if state is not None and state.runs > 0:
                    snap = state.snapshot()
                    features["jt_combo_roi"] = snap["jt_combo_roi"]
                    features["jt_combo_avg_position"] = snap["jt_combo_avg_position"]

            # Jockey distance specialist
            if jockey and dist_cat is not None:
                jd_key = (jockey, dist_cat)
                state = jockey_dist.get(jd_key)
                if state is not None:
                    features["jockey_distance_specialist"] = state.win_rate()

            # Trainer terrain specialist
            if trainer and terrain:
                tt_key = (trainer, terrain)
                state = trainer_terrain.get(tt_key)
                if state is not None:
                    features["trainer_terrain_specialist"] = state.win_rate()

            # Jockey claiming expert
            if jockey and claiming:
                state = jockey_claiming.get(jockey)
                if state is not None:
                    features["jockey_claiming_expert"] = state.win_rate()

            # Trainer 2yo specialist
            if trainer:
                state = trainer_2yo.get(trainer)
                if state is not None:
                    features["trainer_2yo_specialist"] = state.win_rate()

            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            jockey = rec["jockey"]
            trainer = rec["trainer"]
            won = rec["gagnant"]
            dist_cat = rec["dist_cat"]
            terrain = rec["terrain"]
            claiming = rec["claiming"]
            age = rec["age"]

            # Update combo
            if jockey and trainer:
                combo_stats[(jockey, trainer)].update(
                    won, rec["position"], rec["odds"]
                )

            # Update jockey distance specialist
            if jockey and dist_cat is not None:
                jockey_dist[(jockey, dist_cat)].update(won)

            # Update trainer terrain specialist
            if trainer and terrain:
                trainer_terrain[(trainer, terrain)].update(won)

            # Update jockey claiming expert (only for claiming races)
            if jockey and claiming:
                jockey_claiming[jockey].update(won)

            # Update trainer 2yo specialist (only for 2-year-old horses)
            if trainer and age == 2:
                trainer_2yo[trainer].update(won)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "JT Deep build termine: %d features en %.1fs (combos: %d, jockeys: %d, trainers: %d)",
        len(results), elapsed, len(combo_stats),
        len({k[0] for k in jockey_dist}),
        len({k[0] for k in trainer_terrain}),
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
        description="Construction des features jockey-trainer deep a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/jockey_trainer_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_trainer_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_jt_deep_features(input_path, logger)

    # Save
    out_path = output_dir / "jockey_trainer_deep.jsonl"
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
