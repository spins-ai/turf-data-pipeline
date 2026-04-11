#!/usr/bin/env python3
"""
feature_builders.trainer_condition_pref_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer condition preference features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant trainer condition preference features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - trainer_condition_pref.jsonl  in builder_outputs/trainer_condition_pref/

Features per partant (prefix: tcp_):
  - tcp_trainer_hippo_wr           : trainer's win rate at this specific hippodrome
  - tcp_trainer_distance_wr        : trainer's win rate at this distance bucket
  - tcp_trainer_terrain_wr         : trainer's win rate on this terrain type
  - tcp_trainer_hippo_runs         : number of runners trainer has had at this hippodrome
  - tcp_trainer_specialization     : entropy of trainer's distance distribution (low=specialist, high=generalist)
  - tcp_trainer_best_hippo_match   : 1 if this hippodrome is trainer's highest win rate venue
  - tcp_trainer_best_distance_match: 1 if this distance bucket is trainer's best
  - tcp_trainer_condition_score    : composite score averaging terrain_wr, distance_wr, hippo_wr (0-1)

Usage:
    python feature_builders/trainer_condition_pref_builder.py
    python feature_builders/trainer_condition_pref_builder.py --input /path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_condition_pref")

_LOG_EVERY = 500_000

# Distance buckets (metres)
_DIST_BUCKETS = [
    ("sprint", 0, 1200),
    ("mile", 1200, 1600),
    ("middle", 1600, 2000),
    ("staying", 2000, 2400),
    ("marathon", 2400, float("inf")),
]

# Terrain mapping: raw string -> numeric code
_TERRAIN_MAP: dict[str, int] = {
    "bon": 1,
    "bon souple": 2,
    "assez souple": 2,
    "souple": 3,
    "tres souple": 4,
    "lourd": 4,
    "collant": 4,
}

# Minimum runs threshold for best-venue / best-distance selection
_MIN_RUNS_FOR_BEST = 3


# ===========================================================================
# HELPERS
# ===========================================================================


def _distance_bucket(distance: Optional[float]) -> Optional[str]:
    """Return distance bucket name for a given distance in metres."""
    if distance is None or distance <= 0:
        return None
    for name, lo, hi in _DIST_BUCKETS:
        if lo <= distance < hi:
            return name
    return "marathon"


def _terrain_code(etat_terrain: Optional[str]) -> Optional[int]:
    """Map terrain string to numeric code. None if unknown."""
    if not etat_terrain:
        return None
    return _TERRAIN_MAP.get(etat_terrain.strip().lower())


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
        return v if v == v else None  # exclude NaN
    except (ValueError, TypeError):
        return None


def _wr(wins: int, runs: int) -> Optional[float]:
    """Win rate, None if no runs."""
    if runs == 0:
        return None
    return round(wins / runs, 4)


# ===========================================================================
# PER-TRAINER STATE
# ===========================================================================


class _TrainerCondState:
    """Accumulated state for one trainer across all past races."""

    __slots__ = ("per_hippo", "per_distance", "per_terrain")

    def __init__(self) -> None:
        # {key: {"wins": int, "total": int}}
        self.per_hippo: dict[str, dict[str, int]] = defaultdict(lambda: {"wins": 0, "total": 0})
        self.per_distance: dict[str, dict[str, int]] = defaultdict(lambda: {"wins": 0, "total": 0})
        self.per_terrain: dict[int, dict[str, int]] = defaultdict(lambda: {"wins": 0, "total": 0})

    # ------------------------------------------------------------------
    # Snapshot (read-only, called BEFORE update)
    # ------------------------------------------------------------------

    def snapshot(
        self,
        hippodrome: Optional[str],
        dist_bucket: Optional[str],
        terrain_code: Optional[int],
    ) -> dict[str, Any]:
        """Compute all 8 features using strictly past data."""

        # --- per-hippo ---
        hippo_wr: Optional[float] = None
        hippo_runs: Optional[int] = None
        if hippodrome:
            h = self.per_hippo.get(hippodrome)
            if h and h["total"] > 0:
                hippo_wr = _wr(h["wins"], h["total"])
                hippo_runs = h["total"]
            else:
                hippo_runs = 0

        # --- per-distance ---
        dist_wr: Optional[float] = None
        if dist_bucket:
            d = self.per_distance.get(dist_bucket)
            if d and d["total"] > 0:
                dist_wr = _wr(d["wins"], d["total"])

        # --- per-terrain ---
        terrain_wr: Optional[float] = None
        if terrain_code is not None:
            t = self.per_terrain.get(terrain_code)
            if t and t["total"] > 0:
                terrain_wr = _wr(t["wins"], t["total"])

        # --- trainer_specialization: entropy of distance distribution ---
        specialization: Optional[float] = None
        dist_totals = [v["total"] for v in self.per_distance.values() if v["total"] > 0]
        if dist_totals:
            n_total = sum(dist_totals)
            if n_total > 0:
                entropy = 0.0
                for cnt in dist_totals:
                    p = cnt / n_total
                    if p > 0:
                        entropy -= p * math.log2(p)
                # normalise by log2(num_buckets) so it's in [0,1]
                max_entropy = math.log2(len(_DIST_BUCKETS))
                specialization = round(entropy / max_entropy, 4) if max_entropy > 0 else 0.0

        # --- best_hippo_match ---
        best_hippo_match: Optional[int] = None
        if hippodrome and self.per_hippo:
            best_h = None
            best_h_rate = -1.0
            for h_name, h_data in self.per_hippo.items():
                if h_data["total"] < _MIN_RUNS_FOR_BEST:
                    continue
                rate = h_data["wins"] / h_data["total"]
                if rate > best_h_rate:
                    best_h_rate = rate
                    best_h = h_name
            if best_h is not None:
                best_hippo_match = 1 if best_h == hippodrome else 0

        # --- best_distance_match ---
        best_distance_match: Optional[int] = None
        if dist_bucket and self.per_distance:
            best_d = None
            best_d_rate = -1.0
            for d_name, d_data in self.per_distance.items():
                if d_data["total"] < _MIN_RUNS_FOR_BEST:
                    continue
                rate = d_data["wins"] / d_data["total"]
                if rate > best_d_rate:
                    best_d_rate = rate
                    best_d = d_name
            if best_d is not None:
                best_distance_match = 1 if best_d == dist_bucket else 0

        # --- condition_score: average of non-None component win rates ---
        components = [v for v in (terrain_wr, dist_wr, hippo_wr) if v is not None]
        condition_score: Optional[float] = round(sum(components) / len(components), 4) if components else None

        return {
            "tcp_trainer_hippo_wr": hippo_wr,
            "tcp_trainer_distance_wr": dist_wr,
            "tcp_trainer_terrain_wr": terrain_wr,
            "tcp_trainer_hippo_runs": hippo_runs,
            "tcp_trainer_specialization": specialization,
            "tcp_trainer_best_hippo_match": best_hippo_match,
            "tcp_trainer_best_distance_match": best_distance_match,
            "tcp_trainer_condition_score": condition_score,
        }

    # ------------------------------------------------------------------
    # Update (called AFTER snapshot)
    # ------------------------------------------------------------------

    def update(
        self,
        hippodrome: Optional[str],
        dist_bucket: Optional[str],
        terrain_code: Optional[int],
        is_winner: bool,
    ) -> None:
        """Record the result of a race into the running state."""
        if hippodrome:
            self.per_hippo[hippodrome]["total"] += 1
            if is_winner:
                self.per_hippo[hippodrome]["wins"] += 1

        if dist_bucket:
            self.per_distance[dist_bucket]["total"] += 1
            if is_winner:
                self.per_distance[dist_bucket]["wins"] += 1

        if terrain_code is not None:
            self.per_terrain[terrain_code]["total"] += 1
            if is_winner:
                self.per_terrain[terrain_code]["wins"] += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_trainer_condition_pref_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build trainer condition preference features from partants_master.jsonl."""
    logger.info("=== Trainer Condition Pref Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Resolve trainer name (try both possible field names)
        entraineur = (
            (rec.get("entraineur") or rec.get("nom_entraineur") or "")
            .strip()
        )

        hippodrome = (rec.get("hippodrome") or "").strip().upper() or None

        distance = _safe_float(rec.get("distance"))
        dist_bucket = _distance_bucket(distance)

        etat_terrain = rec.get("etat_terrain")
        terrain_code = _terrain_code(etat_terrain)

        # Determine winner: use is_gagnant if present, else position_arrivee == 1
        if rec.get("is_gagnant") is not None:
            is_winner = bool(rec["is_gagnant"])
        else:
            pos = _safe_float(rec.get("position_arrivee"))
            is_winner = pos == 1.0 if pos is not None else False

        slim_records.append({
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "entraineur": entraineur,
            "hippodrome": hippodrome,
            "dist_bucket": dist_bucket,
            "terrain_code": terrain_code,
            "is_winner": is_winner,
        })

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically (index+sort+seek pattern) --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, snapshot BEFORE update --
    t2 = time.time()
    trainer_states: dict[str, _TrainerCondState] = defaultdict(_TrainerCondState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)
    i = 0

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        # Collect all runners of this course
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        # Snapshot features BEFORE updating state (strict temporal)
        for rec in course_group:
            entraineur = rec["entraineur"]
            feat: dict[str, Any] = {"partant_uid": rec["uid"]}

            if entraineur:
                state = trainer_states[entraineur]
                feat.update(
                    state.snapshot(
                        rec["hippodrome"],
                        rec["dist_bucket"],
                        rec["terrain_code"],
                    )
                )
            else:
                feat.update({
                    "tcp_trainer_hippo_wr": None,
                    "tcp_trainer_distance_wr": None,
                    "tcp_trainer_terrain_wr": None,
                    "tcp_trainer_hippo_runs": None,
                    "tcp_trainer_specialization": None,
                    "tcp_trainer_best_hippo_match": None,
                    "tcp_trainer_best_distance_match": None,
                    "tcp_trainer_condition_score": None,
                })

            results.append(feat)

        # Update states AFTER all snapshots for this course
        for rec in course_group:
            entraineur = rec["entraineur"]
            if entraineur:
                trainer_states[entraineur].update(
                    rec["hippodrome"],
                    rec["dist_bucket"],
                    rec["terrain_code"],
                    rec["is_winner"],
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features en %.1fs (entraineurs uniques: %d)",
        len(results), elapsed, len(trainer_states),
    )

    gc.collect()
    return results


# ===========================================================================
# SAVE & CLI
# ===========================================================================


def _save_jsonl(records: list[dict], path: Path, logger) -> None:
    """Write records as JSONL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info(
        "Sauvegarde: %d records -> %s (%.1fs)",
        len(records), path, time.time() - t0,
    )


def _find_input(cli_path: Optional[str], logger) -> Path:
    """Resolve input file path from CLI arg or default."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    # Fallback: look relative to project root
    fallback_candidates = [
        Path(__file__).resolve().parent.parent / "data_master" / "partants_master.jsonl",
        Path(__file__).resolve().parent.parent / "data_master" / "partants_master_enrichi.jsonl",
    ]
    for c in fallback_candidates:
        if c.exists():
            logger.info("Input fallback: %s", c)
            return c
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve. Essaye: {INPUT_PARTANTS} "
        f"et {[str(c) for c in fallback_candidates]}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features trainer condition preference"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_condition_pref_builder")

    input_path = _find_input(args.input, logger)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_trainer_condition_pref_features(input_path, logger)

    out_path = output_dir / "trainer_condition_pref.jsonl"
    _save_jsonl(results, out_path, logger)

    # Fill rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info(
                "  %s: %d/%d (%.1f%%)",
                k, filled, total_count, 100 * filled / total_count,
            )


if __name__ == "__main__":
    main()
