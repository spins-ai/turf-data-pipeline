#!/usr/bin/env python3
"""
feature_builders.cote_tendance_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Odds trend features computed from historical cote per horse.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - cote_tendance.jsonl  in output/cote_tendance/

Features per partant:
  - cote_evolution_3      : average cote variation over last 3 races
  - cote_trend_direction  : 1=shortening, -1=drifting, 0=stable
  - cote_vs_career_avg    : current cote / career average cote
  - is_cote_shortening    : 1 if cote has been decreasing over last 3 races

Usage:
    python feature_builders/cote_tendance_builder.py
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "cote_tendance"

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


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseCoteState:
    __slots__ = ("cotes",)

    def __init__(self) -> None:
        self.cotes: list[float] = []

    def snapshot(self, current_cote: Optional[float]) -> dict[str, Any]:
        """Compute features using only past cotes (strict temporal)."""
        feats: dict[str, Any] = {
            "cote_evolution_3": None,
            "cote_trend_direction": None,
            "cote_vs_career_avg": None,
            "is_cote_shortening": None,
        }

        past = self.cotes  # all past cotes (before this race)

        if current_cote is not None and len(past) >= 1:
            # career avg
            career_avg = sum(past) / len(past)
            if career_avg > 0:
                feats["cote_vs_career_avg"] = round(current_cote / career_avg, 4)

        if len(past) >= 3:
            last3 = past[-3:]
            # average variation over last 3
            diffs = [last3[i + 1] - last3[i] for i in range(len(last3) - 1)]
            avg_diff = sum(diffs) / len(diffs)
            feats["cote_evolution_3"] = round(avg_diff, 4)

            # trend direction
            if avg_diff < -0.5:
                feats["cote_trend_direction"] = 1  # shortening
            elif avg_diff > 0.5:
                feats["cote_trend_direction"] = -1  # drifting
            else:
                feats["cote_trend_direction"] = 0

            # is_cote_shortening: all 3 consecutive cotes decreasing
            is_short = int(all(last3[i + 1] < last3[i] for i in range(len(last3) - 1)))
            feats["is_cote_shortening"] = is_short

        return feats

    def update(self, cote: Optional[float]) -> None:
        if cote is not None:
            self.cotes.append(cote)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_cote_tendance_features(input_path: Path, logger) -> list[dict[str, Any]]:
    logger.info("=== Cote Tendance Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # Phase 1: read minimal fields
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
            "cote": _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # Phase 2: sort chronologically
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # Phase 3: process
    t2 = time.time()
    horse_states: dict[str, _HorseCoteState] = defaultdict(_HorseCoteState)
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
            cote = rec["cote"]
            feats: dict[str, Any] = {"partant_uid": rec["uid"]}
            if hid:
                state = horse_states[hid]
                feats.update(state.snapshot(cote))
            else:
                feats.update({
                    "cote_evolution_3": None,
                    "cote_trend_direction": None,
                    "cote_vs_career_avg": None,
                    "is_cote_shortening": None,
                })
            results.append(feats)

        # Update post-race
        for rec in course_group:
            hid = rec["horse_id"]
            if hid:
                horse_states[hid].update(rec["cote"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Cote tendance build termine: %d features en %.1fs (chevaux uniques: %d)",
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
        description="Construction des features cote tendance a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("cote_tendance_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_cote_tendance_features(input_path, logger)

    out_path = output_dir / "cote_tendance.jsonl"
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
