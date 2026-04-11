#!/usr/bin/env python3
"""
feature_builders.recency_bias_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
5 recency-weighted features that emphasise recent performance over old results.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant recency bias features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - recency_bias_features.jsonl   in output/recency_bias_features/

Features per partant:
  - weight_recent_3x           : weighted avg position (last 3 races weighted 3x vs older)
  - weight_recent_5x           : weighted avg position (last 5 races weighted 5x vs older)
  - exponential_decay_form     : exponentially decayed avg position (lambda=0.3)
  - time_weighted_elo          : Elo rating with time-decayed K-factor
  - recency_adjusted_speed     : recent speed figure weighted higher than older ones

Usage:
    python feature_builders/recency_bias_builder.py
    python feature_builders/recency_bias_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/recency_bias")

_LOG_EVERY = 500_000

# Elo parameters
_ELO_INIT = 1500.0
_ELO_K_BASE = 32.0
_ELO_DECAY = 0.05  # K-factor decay per days since last race

# Exponential decay lambda for form
_DECAY_LAMBDA = 0.3

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
# HELPERS
# ===========================================================================


def _parse_date_to_days(date_iso: str) -> Optional[int]:
    """Convert ISO date string to days since epoch (for day diffs)."""
    if not date_iso or len(date_iso) < 10:
        return None
    try:
        y, m, d = int(date_iso[:4]), int(date_iso[5:7]), int(date_iso[8:10])
        # Simplified days-since-epoch (good enough for diffs)
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


def _weighted_avg_positions(positions: list[float], n_recent: int, weight_mult: float) -> Optional[float]:
    """Weighted average where last n_recent entries get weight_mult, others get 1.0."""
    if len(positions) < 2:
        return None
    total_w = 0.0
    total_v = 0.0
    cutoff = len(positions) - n_recent
    for i, p in enumerate(positions):
        w = weight_mult if i >= cutoff else 1.0
        total_w += w
        total_v += w * p
    if total_w == 0:
        return None
    return round(total_v / total_w, 3)


def _exponential_decay_avg(positions: list[float], lam: float) -> Optional[float]:
    """Compute exponentially decayed average: more recent = higher weight.

    Weight for position at index i (0=oldest) = exp(lambda * (i - n + 1))
    So the most recent (i = n-1) gets weight 1.0, and older ones decay.
    """
    n = len(positions)
    if n < 2:
        return None
    total_w = 0.0
    total_v = 0.0
    for i, p in enumerate(positions):
        w = math.exp(lam * (i - n + 1))
        total_w += w
        total_v += w * p
    if total_w == 0:
        return None
    return round(total_v / total_w, 3)


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseRecencyState:
    """Track per-horse recency data."""

    __slots__ = ("positions", "speeds", "elo", "last_race_day", "n_races")

    def __init__(self) -> None:
        self.positions: list[float] = []
        self.speeds: list[float] = []
        self.elo: float = _ELO_INIT
        self.last_race_day: Optional[int] = None
        self.n_races: int = 0

    def update_elo(self, actual_score: float, expected_score: float, race_day: Optional[int]) -> None:
        """Update Elo with time-decayed K-factor."""
        k = _ELO_K_BASE
        if self.last_race_day is not None and race_day is not None:
            days_gap = max(0, race_day - self.last_race_day)
            # Increase K for horses returning from long breaks (more volatile)
            k = _ELO_K_BASE * (1.0 + _ELO_DECAY * min(days_gap, 365))
        self.elo += k * (actual_score - expected_score)
        self.last_race_day = race_day
        self.n_races += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_recency_bias_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build 5 recency bias features from partants_master.jsonl."""
    logger.info("=== Recency Bias Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        pos_raw = rec.get("position_arrivee")
        pos = None
        if pos_raw is not None:
            try:
                pos = float(pos_raw)
                if pos <= 0 or pos > 50:
                    pos = None
            except (TypeError, ValueError):
                pos = None

        # Speed figure from various possible fields
        speed = None
        for field in ("speed_figure", "sf_speed_figure", "vitesse_moyenne"):
            v = rec.get(field)
            if v is not None:
                try:
                    speed = float(v)
                    if speed > 0:
                        break
                    speed = None
                except (TypeError, ValueError):
                    speed = None

        date_iso = rec.get("date_reunion_iso", "")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": date_iso,
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "horse_id": rec.get("horse_id"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": pos,
            "speed": speed,
            "race_day": _parse_date_to_days(date_iso),
            "n_partants": rec.get("nombre_partants"),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_state: dict[str, _HorseRecencyState] = defaultdict(_HorseRecencyState)
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

        # -- Snapshot pre-race features --
        for rec in course_group:
            cheval = rec["horse_id"] or rec["cheval"]

            if not cheval:
                results.append({
                    "partant_uid": rec["uid"],
                    "weight_recent_3x": None,
                    "weight_recent_5x": None,
                    "exponential_decay_form": None,
                    "time_weighted_elo": None,
                    "recency_adjusted_speed": None,
                })
                continue

            state = horse_state[cheval]

            # 1. weight_recent_3x
            f_wr3 = _weighted_avg_positions(state.positions, 3, 3.0) if len(state.positions) >= 3 else None

            # 2. weight_recent_5x
            f_wr5 = _weighted_avg_positions(state.positions, 5, 5.0) if len(state.positions) >= 3 else None

            # 3. exponential_decay_form
            f_exp = _exponential_decay_avg(state.positions, _DECAY_LAMBDA) if len(state.positions) >= 3 else None

            # 4. time_weighted_elo (only emit after 3+ races)
            f_elo = round(state.elo, 1) if state.n_races >= 3 else None

            # 5. recency_adjusted_speed
            f_speed = _exponential_decay_avg(state.speeds, _DECAY_LAMBDA) if len(state.speeds) >= 3 else None

            results.append({
                "partant_uid": rec["uid"],
                "weight_recent_3x": f_wr3,
                "weight_recent_5x": f_wr5,
                "exponential_decay_form": f_exp,
                "time_weighted_elo": f_elo,
                "recency_adjusted_speed": f_speed,
            })

        # -- Update state after race --
        # Compute Elo updates: need number of runners for expected score
        n_runners = len([r for r in course_group if (r["horse_id"] or r["cheval"])])
        if n_runners < 2:
            n_runners = 2

        for rec in course_group:
            cheval = rec["horse_id"] or rec["cheval"]
            if not cheval:
                continue

            state = horse_state[cheval]

            if rec["position"] is not None:
                state.positions.append(rec["position"])
            if rec["speed"] is not None:
                state.speeds.append(rec["speed"])

            # Elo update: actual score based on finish position
            pos = rec["position"]
            if pos is not None:
                # Normalise finish to 0-1 score (1=best, 0=worst)
                actual = max(0.0, 1.0 - (pos - 1) / max(1, n_runners - 1))
                # Expected score from current Elo (simplified: vs field average of 1500)
                expected = 1.0 / (1.0 + 10.0 ** ((_ELO_INIT - state.elo) / 400.0))
                state.update_elo(actual, expected, rec["race_day"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Recency bias build termine: %d features en %.1fs (chevaux: %d)",
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
        description="Construction des recency bias features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/recency_bias_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("recency_bias_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_recency_bias_features(input_path, logger)

    out_path = output_dir / "recency_bias_features.jsonl"
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
