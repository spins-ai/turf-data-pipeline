#!/usr/bin/env python3
"""
feature_builders.corde_hippodrome_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Draw/post position (corde) advantage features per hippodrome.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - corde_hippodrome.jsonl  in output/corde_hippodrome/

Features per partant:
  - corde_win_rate_hippo    : historical win rate for this corde at this hippodrome
  - corde_advantage_hippo   : corde_win_rate / avg_win_rate at the hippodrome
  - best_corde_hippo        : which corde wins the most at this hippodrome
  - is_best_corde           : 1 if this horse has the best corde

Usage:
    python feature_builders/corde_hippodrome_builder.py
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "corde_hippodrome"

_LOG_EVERY = 500_000
_MIN_RACES_FOR_STATS = 20  # minimum races to consider stats reliable


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


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATS TRACKER
# ===========================================================================


class _HippoCordeStats:
    """Track per-hippodrome corde stats."""

    __slots__ = ("corde_runs", "corde_wins", "total_runs", "total_wins")

    def __init__(self) -> None:
        self.corde_runs: dict[int, int] = defaultdict(int)
        self.corde_wins: dict[int, int] = defaultdict(int)
        self.total_runs: int = 0
        self.total_wins: int = 0

    def snapshot(self, corde: Optional[int]) -> dict[str, Any]:
        feats: dict[str, Any] = {
            "corde_win_rate_hippo": None,
            "corde_advantage_hippo": None,
            "best_corde_hippo": None,
            "is_best_corde": None,
        }
        if corde is None or self.total_runs < _MIN_RACES_FOR_STATS:
            return feats

        # Win rate for this corde
        runs_c = self.corde_runs.get(corde, 0)
        wins_c = self.corde_wins.get(corde, 0)
        if runs_c >= 5:
            wr = wins_c / runs_c
            feats["corde_win_rate_hippo"] = round(wr, 4)

            # Average win rate
            avg_wr = self.total_wins / self.total_runs if self.total_runs > 0 else 0
            if avg_wr > 0:
                feats["corde_advantage_hippo"] = round(wr / avg_wr, 4)

        # Best corde
        best_c = None
        best_wr = -1.0
        for c, runs in self.corde_runs.items():
            if runs >= 5:
                wr_c = self.corde_wins.get(c, 0) / runs
                if wr_c > best_wr:
                    best_wr = wr_c
                    best_c = c
        if best_c is not None:
            feats["best_corde_hippo"] = best_c
            feats["is_best_corde"] = int(corde == best_c)

        return feats

    def update(self, corde: Optional[int], is_winner: bool) -> None:
        if corde is None:
            return
        self.corde_runs[corde] += 1
        self.total_runs += 1
        if is_winner:
            self.corde_wins[corde] += 1
            self.total_wins += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_corde_hippodrome_features(input_path: Path, logger) -> list[dict[str, Any]]:
    logger.info("=== Corde Hippodrome Builder ===")
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
            "hippo": (rec.get("hippodrome_normalise") or "").strip().lower(),
            "corde": _safe_int(rec.get("place_corde")),
            "is_gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    hippo_stats: dict[str, _HippoCordeStats] = defaultdict(_HippoCordeStats)
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
            hippo = rec["hippo"]
            feats: dict[str, Any] = {"partant_uid": rec["uid"]}
            if hippo:
                feats.update(hippo_stats[hippo].snapshot(rec["corde"]))
            else:
                feats.update({
                    "corde_win_rate_hippo": None,
                    "corde_advantage_hippo": None,
                    "best_corde_hippo": None,
                    "is_best_corde": None,
                })
            results.append(feats)

        # Update post-race
        for rec in course_group:
            hippo = rec["hippo"]
            if hippo:
                hippo_stats[hippo].update(rec["corde"], rec["is_gagnant"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Corde hippodrome build termine: %d features en %.1fs (hippodromes: %d)",
        len(results), elapsed, len(hippo_stats),
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
        description="Construction des features corde hippodrome a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("corde_hippodrome_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_corde_hippodrome_features(input_path, logger)

    out_path = output_dir / "corde_hippodrome.jsonl"
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
