#!/usr/bin/env python3
"""
feature_builders.allocation_relative_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Relative allocation (gains) features per horse.

Uses ``gains_carriere_euros`` as a proxy for class/allocation level.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - allocation_relative.jsonl  in output/allocation_relative/

Features per partant:
  - allocation_vs_avg          : current gains / mean gains seen so far
  - is_class_upgrade           : 1 if current gains > max previously seen
  - allocation_rank_in_career  : percentile of current gains in career history
  - allocation_per_runner      : gains_carriere / nombre_partants

Usage:
    python feature_builders/allocation_relative_builder.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from bisect import bisect_left, insort
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "allocation_relative"

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
        return v if v == v else None
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


class _HorseAllocState:
    __slots__ = ("gains_history", "gains_sorted", "gains_sum", "gains_max")

    def __init__(self) -> None:
        self.gains_history: list[float] = []
        self.gains_sorted: list[float] = []
        self.gains_sum: float = 0.0
        self.gains_max: float = 0.0

    def snapshot(self, current_gains: Optional[float], nb_partants: Optional[int]) -> dict[str, Any]:
        feats: dict[str, Any] = {
            "allocation_vs_avg": None,
            "is_class_upgrade": None,
            "allocation_rank_in_career": None,
            "allocation_per_runner": None,
        }

        if current_gains is None:
            return feats

        n = len(self.gains_history)

        # allocation_per_runner
        if nb_partants is not None and nb_partants > 0:
            feats["allocation_per_runner"] = round(current_gains / nb_partants, 2)

        if n == 0:
            return feats

        # vs avg
        avg = self.gains_sum / n
        if avg > 0:
            feats["allocation_vs_avg"] = round(current_gains / avg, 4)

        # class upgrade
        feats["is_class_upgrade"] = int(current_gains > self.gains_max)

        # percentile rank (using sorted history)
        rank = bisect_left(self.gains_sorted, current_gains)
        feats["allocation_rank_in_career"] = round(rank / n, 4)

        return feats

    def update(self, gains: Optional[float]) -> None:
        if gains is not None:
            self.gains_history.append(gains)
            insort(self.gains_sorted, gains)
            self.gains_sum += gains
            if gains > self.gains_max:
                self.gains_max = gains


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_allocation_relative_features(input_path: Path, logger) -> list[dict[str, Any]]:
    logger.info("=== Allocation Relative Builder ===")
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
            "gains": _safe_float(rec.get("gains_carriere_euros")),
            "nb_partants": _safe_int(rec.get("nombre_partants")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    horse_states: dict[str, _HorseAllocState] = defaultdict(_HorseAllocState)
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
                state = horse_states[hid]
                feats.update(state.snapshot(rec["gains"], rec["nb_partants"]))
            else:
                feats.update({
                    "allocation_vs_avg": None,
                    "is_class_upgrade": None,
                    "allocation_rank_in_career": None,
                    "allocation_per_runner": None,
                })
            results.append(feats)

        for rec in course_group:
            hid = rec["horse_id"]
            if hid:
                horse_states[hid].update(rec["gains"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Allocation relative build termine: %d features en %.1fs (chevaux uniques: %d)",
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
        description="Construction des features allocation relative a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("allocation_relative_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_allocation_relative_features(input_path, logger)

    out_path = output_dir / "allocation_relative.jsonl"
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
