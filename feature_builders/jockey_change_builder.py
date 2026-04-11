#!/usr/bin/env python3
"""
feature_builders.jockey_change_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Jockey change features: detect jockey switches and evaluate impact.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - jockey_change.jsonl  in output/jockey_change/

Features per partant:
  - is_jockey_change       : 1 if jockey is different from last race
  - jockey_upgrade_score   : new jockey win rate - old jockey win rate (positive = upgrade)
  - jockey_change_win_rate : historical win rate when this horse changes jockey
  - same_jockey_streak     : nb consecutive races with the same jockey

Usage:
    python feature_builders/jockey_change_builder.py
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "jockey_change"

_LOG_EVERY = 500_000
_MIN_JOCKEY_RIDES = 10  # minimum rides to compute reliable win rate


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
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


def _norm_jockey(name: Optional[str]) -> Optional[str]:
    if not name or not isinstance(name, str):
        return None
    return name.strip().upper()


# ===========================================================================
# TRACKERS
# ===========================================================================


class _JockeyStats:
    """Global jockey win rate tracker."""
    __slots__ = ("rides", "wins")

    def __init__(self) -> None:
        self.rides: int = 0
        self.wins: int = 0

    def win_rate(self) -> Optional[float]:
        if self.rides < _MIN_JOCKEY_RIDES:
            return None
        return self.wins / self.rides


class _HorseJockeyState:
    """Per-horse jockey change tracker."""
    __slots__ = ("last_jockey", "streak", "change_count", "change_wins")

    def __init__(self) -> None:
        self.last_jockey: Optional[str] = None
        self.streak: int = 0
        self.change_count: int = 0
        self.change_wins: int = 0

    def snapshot(
        self,
        current_jockey: Optional[str],
        jockey_stats: dict[str, _JockeyStats],
    ) -> dict[str, Any]:
        feats: dict[str, Any] = {
            "is_jockey_change": None,
            "jockey_upgrade_score": None,
            "jockey_change_win_rate": None,
            "same_jockey_streak": None,
        }

        if current_jockey is None or self.last_jockey is None:
            # First race or no jockey info
            feats["same_jockey_streak"] = self.streak
            return feats

        is_change = int(current_jockey != self.last_jockey)
        feats["is_jockey_change"] = is_change
        feats["same_jockey_streak"] = self.streak

        if is_change:
            # Upgrade score = new jockey wr - old jockey wr
            new_wr = jockey_stats[current_jockey].win_rate() if current_jockey in jockey_stats else None
            old_wr = jockey_stats[self.last_jockey].win_rate() if self.last_jockey in jockey_stats else None
            if new_wr is not None and old_wr is not None:
                feats["jockey_upgrade_score"] = round(new_wr - old_wr, 4)

            # Historical win rate when this horse changes jockey
            if self.change_count > 0:
                feats["jockey_change_win_rate"] = round(self.change_wins / self.change_count, 4)

        return feats

    def update(self, jockey: Optional[str], is_winner: bool) -> None:
        if jockey is None:
            return
        if self.last_jockey is not None and jockey != self.last_jockey:
            self.change_count += 1
            if is_winner:
                self.change_wins += 1
            self.streak = 1
        else:
            self.streak += 1
        self.last_jockey = jockey


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_jockey_change_features(input_path: Path, logger) -> list[dict[str, Any]]:
    logger.info("=== Jockey Change Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

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
            "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
            "jockey": _norm_jockey(rec.get("jockey_driver")),
            "is_gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    jockey_stats: dict[str, _JockeyStats] = defaultdict(_JockeyStats)
    horse_states: dict[str, _HorseJockeyState] = defaultdict(_HorseJockeyState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while i < total and slim_records[i]["course"] == course_uid and slim_records[i]["date"] == course_date:
            course_group.append(slim_records[i])
            i += 1

        # Snapshot pre-race
        for rec in course_group:
            hid = rec["horse_id"]
            feats: dict[str, Any] = {"partant_uid": rec["uid"]}
            if hid:
                feats.update(horse_states[hid].snapshot(rec["jockey"], jockey_stats))
            else:
                feats.update({
                    "is_jockey_change": None,
                    "jockey_upgrade_score": None,
                    "jockey_change_win_rate": None,
                    "same_jockey_streak": None,
                })
            results.append(feats)

        # Update post-race
        for rec in course_group:
            hid = rec["horse_id"]
            jockey = rec["jockey"]
            is_win = rec["is_gagnant"]
            if hid:
                horse_states[hid].update(jockey, is_win)
            if jockey:
                jockey_stats[jockey].rides += 1
                if is_win:
                    jockey_stats[jockey].wins += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Jockey change build termine: %d features en %.1fs (chevaux: %d, jockeys: %d)",
        len(results), elapsed, len(horse_states), len(jockey_stats),
    )
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
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
        description="Construction des features jockey change a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("jockey_change_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_jockey_change_features(input_path, logger)

    out_path = output_dir / "jockey_change.jsonl"
    save_jsonl(results, out_path, logger)

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
