#!/usr/bin/env python3
"""
feature_builders.streak_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
5 streak-based features capturing winning/losing sequences.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant streak features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - streak_features.jsonl   in output/streak_features/

Features per partant:
  - current_win_streak      : consecutive wins heading into this race (0 if last was a loss)
  - current_loss_streak     : consecutive non-wins heading into this race (0 if last was a win)
  - best_streak_career      : longest win streak in the horse's career so far
  - streak_vs_field_avg     : horse's current win streak minus avg win streak of the field
  - streak_at_hippodrome    : current win streak at this specific hippodrome

Usage:
    python feature_builders/streak_builder.py
    python feature_builders/streak_builder.py --input data_master/partants_master.jsonl
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
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/streak")

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
# PER-HORSE STATE
# ===========================================================================


class _HorseStreakState:
    """Track streak data for one horse."""

    __slots__ = ("cur_win_streak", "cur_loss_streak", "best_win_streak", "hippo_streaks")

    def __init__(self) -> None:
        self.cur_win_streak: int = 0
        self.cur_loss_streak: int = 0
        self.best_win_streak: int = 0
        # hippo -> current win streak at that hippo
        self.hippo_streaks: dict[str, int] = defaultdict(int)

    def update(self, is_winner: bool, hippo: str) -> None:
        """Update streaks after a race result is known."""
        if is_winner:
            self.cur_win_streak += 1
            self.cur_loss_streak = 0
            if self.cur_win_streak > self.best_win_streak:
                self.best_win_streak = self.cur_win_streak
            if hippo:
                self.hippo_streaks[hippo] += 1
        else:
            self.cur_loss_streak += 1
            self.cur_win_streak = 0
            if hippo:
                self.hippo_streaks[hippo] = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_streak_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build 5 streak features from partants_master.jsonl."""
    logger.info("=== Streak Builder ===")
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
            "cheval": rec.get("nom_cheval"),
            "horse_id": rec.get("horse_id"),
            "gagnant": bool(rec.get("is_gagnant")),
            "hippo": (rec.get("hippodrome_normalise") or "").lower().strip(),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_state: dict[str, _HorseStreakState] = defaultdict(_HorseStreakState)
    results: list[dict[str, Any]] = []
    n_processed = 0
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
        # First, collect all win streaks in the field for the avg calculation
        field_streaks: list[int] = []
        field_features: list[dict[str, Any]] = []

        for rec in course_group:
            cheval = rec["horse_id"] or rec["cheval"]
            hippo = rec["hippo"]

            if not cheval:
                field_features.append({
                    "partant_uid": rec["uid"],
                    "current_win_streak": None,
                    "current_loss_streak": None,
                    "best_streak_career": None,
                    "streak_vs_field_avg": None,
                    "streak_at_hippodrome": None,
                })
                continue

            state = horse_state[cheval]

            cur_ws = state.cur_win_streak
            cur_ls = state.cur_loss_streak
            best_ws = state.best_win_streak
            hippo_streak = state.hippo_streaks.get(hippo, 0) if hippo else None

            field_streaks.append(cur_ws)

            field_features.append({
                "partant_uid": rec["uid"],
                "current_win_streak": cur_ws,
                "current_loss_streak": cur_ls,
                "best_streak_career": best_ws,
                "streak_vs_field_avg": cur_ws,  # placeholder, will adjust below
                "streak_at_hippodrome": hippo_streak,
            })

        # Compute field avg and adjust streak_vs_field_avg
        if field_streaks:
            field_avg = sum(field_streaks) / len(field_streaks)
            for feat in field_features:
                if feat["streak_vs_field_avg"] is not None:
                    feat["streak_vs_field_avg"] = round(
                        feat["streak_vs_field_avg"] - field_avg, 3
                    )
        else:
            for feat in field_features:
                feat["streak_vs_field_avg"] = None

        results.extend(field_features)

        # -- Update state after race --
        for rec in course_group:
            cheval = rec["horse_id"] or rec["cheval"]
            if cheval:
                horse_state[cheval].update(rec["gagnant"], rec["hippo"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Streak build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_state),
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
        description="Construction des streak features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/streak_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("streak_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_streak_features(input_path, logger)

    out_path = output_dir / "streak_features.jsonl"
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
