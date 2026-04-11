#!/usr/bin/env python3
"""
feature_builders.ratio_place_victoire_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ratio place/victoire features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant ratio features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - ratio_place_victoire.jsonl   in output/ratio_place_victoire/

Features per partant:
  - place_to_win_ratio       : nb_places / nb_victoires (haut = bon place, mauvais gagnant)
  - is_chronic_placer        : 1 si ratio > 3
  - win_conversion_rate      : nb_victoires / nb_places (capacite a transformer)
  - place_streak_current     : nb courses consecutives dans le top 3

Usage:
    python feature_builders/ratio_place_victoire_builder.py
    python feature_builders/ratio_place_victoire_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "ratio_place_victoire"

_LOG_EVERY = 500_000


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


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseRatioState:
    """Per-horse accumulated state for place/win ratio features."""

    __slots__ = ("nb_victoires", "nb_places", "place_streak")

    def __init__(self) -> None:
        self.nb_victoires: int = 0
        self.nb_places: int = 0
        self.place_streak: int = 0  # consecutive top-3 finishes

    def snapshot(self) -> dict[str, Any]:
        """Compute features using only past races (strict temporal)."""
        if self.nb_victoires > 0 and self.nb_places > 0:
            place_to_win = round(self.nb_places / self.nb_victoires, 4)
            win_conv = round(self.nb_victoires / self.nb_places, 4)
        elif self.nb_places > 0:
            place_to_win = None  # infinite (0 wins)
            win_conv = 0.0
        else:
            place_to_win = None
            win_conv = None

        is_chronic = None
        if place_to_win is not None:
            is_chronic = 1 if place_to_win > 3.0 else 0

        return {
            "place_to_win_ratio": place_to_win,
            "is_chronic_placer": is_chronic,
            "win_conversion_rate": win_conv,
            "place_streak_current": self.place_streak,
        }

    def update(self, position: Optional[int], is_gagnant: bool, is_place: bool) -> None:
        """Update state with a new race result (post-race)."""
        if is_gagnant:
            self.nb_victoires += 1
        if is_place:
            self.nb_places += 1

        # Place streak: consecutive top-3 finishes
        if position is not None and 1 <= position <= 3:
            self.place_streak += 1
        else:
            self.place_streak = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_ratio_place_victoire_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build ratio place/victoire features from partants_master.jsonl."""
    logger.info("=== Ratio Place/Victoire Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval") or rec.get("horse_id"),
            "position": _safe_int(rec.get("position_arrivee")),
            "is_gagnant": bool(rec.get("is_gagnant")),
            "is_place": bool(rec.get("is_place")),
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
    horse_states: dict[str, _HorseRatioState] = defaultdict(_HorseRatioState)
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

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "place_to_win_ratio": None,
                "is_chronic_placer": None,
                "win_conversion_rate": None,
                "place_streak_current": None,
            }

            if cheval:
                state = horse_states[cheval]
                snap = state.snapshot()
                features.update(snap)

            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            cheval = rec["cheval"]
            if cheval:
                horse_states[cheval].update(
                    rec["position"], rec["is_gagnant"], rec["is_place"]
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Ratio place/victoire build termine: %d features en %.1fs (chevaux uniques: %d)",
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
        description="Construction des features ratio place/victoire a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/ratio_place_victoire/)",
    )
    args = parser.parse_args()

    logger = setup_logging("ratio_place_victoire_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_ratio_place_victoire_features(input_path, logger)

    # Save
    out_path = output_dir / "ratio_place_victoire.jsonl"
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
