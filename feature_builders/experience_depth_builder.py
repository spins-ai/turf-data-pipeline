#!/usr/bin/env python3
"""
feature_builders.experience_depth_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse experience depth features measuring breadth and depth of racing history.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant experience features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - experience_depth.jsonl   in output/experience_depth/

Features per partant (5):
  - hippo_experience       : nb past races at this hippodrome
  - distance_experience    : nb past races at this distance category
  - terrain_experience     : nb past races on this terrain type
  - discipline_experience  : nb past races in this discipline
  - total_variety_score    : nb unique (hippo, distance, terrain, discipline) combos in career

Usage:
    python feature_builders/experience_depth_builder.py
    python feature_builders/experience_depth_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "experience_depth"

_LOG_EVERY = 500_000

# Distance categories (metres)
_DISTANCE_CATEGORIES = [
    (0, 1300, "sprint"),
    (1300, 1800, "mile"),
    (1800, 2400, "intermediaire"),
    (2400, 3200, "classique"),
    (3200, 99999, "longue"),
]


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


def _distance_category(distance: Optional[float]) -> Optional[str]:
    """Map a distance in metres to a category label."""
    if distance is None or distance <= 0:
        return None
    for lo, hi, label in _DISTANCE_CATEGORIES:
        if lo <= distance < hi:
            return label
    return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseExperienceState:
    """Per-horse accumulated state for experience depth features."""

    __slots__ = ("hippo_counts", "distance_counts", "terrain_counts",
                 "discipline_counts", "unique_combos")

    def __init__(self) -> None:
        self.hippo_counts: dict[str, int] = defaultdict(int)
        self.distance_counts: dict[str, int] = defaultdict(int)
        self.terrain_counts: dict[str, int] = defaultdict(int)
        self.discipline_counts: dict[str, int] = defaultdict(int)
        self.unique_combos: set[tuple[str, str, str, str]] = set()

    def snapshot(self, hippo: Optional[str], dist_cat: Optional[str],
                 terrain: Optional[str], discipline: Optional[str]) -> dict[str, Any]:
        """Compute features using only past races (strict temporal)."""
        return {
            "hippo_experience": self.hippo_counts.get(hippo, 0) if hippo else 0,
            "distance_experience": self.distance_counts.get(dist_cat, 0) if dist_cat else 0,
            "terrain_experience": self.terrain_counts.get(terrain, 0) if terrain else 0,
            "discipline_experience": self.discipline_counts.get(discipline, 0) if discipline else 0,
            "total_variety_score": len(self.unique_combos),
        }

    def update(self, hippo: Optional[str], dist_cat: Optional[str],
               terrain: Optional[str], discipline: Optional[str]) -> None:
        """Update state with a new race result (post-race)."""
        if hippo:
            self.hippo_counts[hippo] += 1
        if dist_cat:
            self.distance_counts[dist_cat] += 1
        if terrain:
            self.terrain_counts[terrain] += 1
        if discipline:
            self.discipline_counts[discipline] += 1
        combo = (hippo or "", dist_cat or "", terrain or "", discipline or "")
        self.unique_combos.add(combo)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_experience_depth_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build experience depth features from partants_master.jsonl."""
    logger.info("=== Experience Depth Builder ===")
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
        discipline_raw = rec.get("discipline") or rec.get("type_course") or ""
        discipline = discipline_raw.strip().lower() if discipline_raw else ""
        hippo_raw = rec.get("hippodrome") or rec.get("nom_hippodrome") or ""
        hippo = hippo_raw.strip().lower() if hippo_raw else ""
        terrain_raw = rec.get("terrain") or rec.get("type_terrain") or ""
        terrain = terrain_raw.strip().lower() if terrain_raw else ""

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "hippo": hippo,
            "distance": distance,
            "dist_cat": _distance_category(distance),
            "terrain": terrain,
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

    # -- Phase 3: Process record by record --
    t2 = time.time()
    horse_states: dict[str, _HorseExperienceState] = defaultdict(_HorseExperienceState)
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

        # -- Snapshot pre-race features --
        for rec in course_group:
            cheval = rec["cheval"]
            hippo = rec["hippo"]
            dist_cat = rec["dist_cat"]
            terrain = rec["terrain"]
            discipline = rec["discipline"]

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "hippo_experience": 0,
                "distance_experience": 0,
                "terrain_experience": 0,
                "discipline_experience": 0,
                "total_variety_score": 0,
            }

            if cheval:
                state = horse_states[cheval]
                snap = state.snapshot(hippo, dist_cat, terrain, discipline)
                features.update(snap)

            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            cheval = rec["cheval"]
            if cheval:
                horse_states[cheval].update(
                    rec["hippo"], rec["dist_cat"], rec["terrain"], rec["discipline"]
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Experience depth build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results), elapsed, len(horse_states),
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
        description="Construction des features experience depth a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/experience_depth/)",
    )
    args = parser.parse_args()

    logger = setup_logging("experience_depth_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_experience_depth_features(input_path, logger)

    # Save
    out_path = output_dir / "experience_depth.jsonl"
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
