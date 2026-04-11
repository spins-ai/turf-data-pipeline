#!/usr/bin/env python3
"""
feature_builders.distance_fine_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fine-grained distance features per horse.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - distance_fine.jsonl  in output/distance_fine/

Features per partant:
  - distance_vs_preferred    : distance - preferred distance of the horse
  - distance_deviation       : abs(distance - avg career distance)
  - is_exact_distance_repeat : 1 if same distance as last race
  - distance_range_career    : max_distance - min_distance in career

Usage:
    python feature_builders/distance_fine_builder.py
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "distance_fine"

_LOG_EVERY = 500_000


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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v and v > 0 else None
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
# PER-HORSE STATE
# ===========================================================================


class _HorseDistanceState:
    __slots__ = ("distances", "wins_at_distance", "last_distance", "dist_sum",
                 "dist_min", "dist_max")

    def __init__(self) -> None:
        self.distances: list[float] = []
        self.wins_at_distance: dict[int, int] = defaultdict(int)  # rounded distance -> win count
        self.last_distance: Optional[float] = None
        self.dist_sum: float = 0.0
        self.dist_min: float = float("inf")
        self.dist_max: float = 0.0

    def snapshot(self, distance: Optional[float]) -> dict[str, Any]:
        feats: dict[str, Any] = {
            "distance_vs_preferred": None,
            "distance_deviation": None,
            "is_exact_distance_repeat": None,
            "distance_range_career": None,
        }

        n = len(self.distances)
        if distance is None or n == 0:
            return feats

        # Preferred distance = distance with most wins, fallback to most frequent
        if self.wins_at_distance:
            preferred = max(self.wins_at_distance, key=lambda d: self.wins_at_distance[d])
        else:
            # Most frequent distance
            freq: dict[int, int] = defaultdict(int)
            for d in self.distances:
                freq[round(d)] += 1
            preferred = max(freq, key=lambda d: freq[d])

        feats["distance_vs_preferred"] = round(distance - preferred, 0)

        # Deviation from career average
        avg_dist = self.dist_sum / n
        feats["distance_deviation"] = round(abs(distance - avg_dist), 0)

        # Exact distance repeat
        if self.last_distance is not None:
            feats["is_exact_distance_repeat"] = int(abs(distance - self.last_distance) < 1)

        # Career range
        if self.dist_min < float("inf"):
            feats["distance_range_career"] = round(self.dist_max - self.dist_min, 0)

        return feats

    def update(self, distance: Optional[float], is_winner: bool) -> None:
        if distance is None:
            return
        self.distances.append(distance)
        self.dist_sum += distance
        if distance < self.dist_min:
            self.dist_min = distance
        if distance > self.dist_max:
            self.dist_max = distance
        self.last_distance = distance
        if is_winner:
            self.wins_at_distance[round(distance)] += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_distance_fine_features(input_path: Path, logger) -> list[dict[str, Any]]:
    logger.info("=== Distance Fine Builder ===")
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
            "distance": _safe_float(rec.get("distance")),
            "is_gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    horse_states: dict[str, _HorseDistanceState] = defaultdict(_HorseDistanceState)
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

        for rec in course_group:
            hid = rec["horse_id"]
            feats: dict[str, Any] = {"partant_uid": rec["uid"]}
            if hid:
                feats.update(horse_states[hid].snapshot(rec["distance"]))
            else:
                feats.update({
                    "distance_vs_preferred": None,
                    "distance_deviation": None,
                    "is_exact_distance_repeat": None,
                    "distance_range_career": None,
                })
            results.append(feats)

        for rec in course_group:
            hid = rec["horse_id"]
            if hid:
                horse_states[hid].update(rec["distance"], rec["is_gagnant"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Distance fine build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results), elapsed, len(horse_states),
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
        description="Construction des features distance fine a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("distance_fine_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_distance_fine_features(input_path, logger)

    out_path = output_dir / "distance_fine.jsonl"
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
