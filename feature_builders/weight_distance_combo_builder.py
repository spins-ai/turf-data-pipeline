#!/usr/bin/env python3
"""
feature_builders.weight_distance_combo_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse performance at different weight × distance combinations.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically (index + sort + seek), and computes per-partant features
BEFORE updating state -- strict temporal integrity, no future leakage.

Produces:
  - weight_distance_combo.jsonl   in output/weight_distance_combo/

Features per partant:
  - wdc_weight_distance_wr    : horse's win rate at this weight bucket × distance bucket combo
  - wdc_weight_bucket         : weight category (light <54, medium 54-58, heavy >58 kg)
  - wdc_horse_optimal_weight  : weight bucket where horse has highest win rate (min 3 runs)
  - wdc_weight_match          : 1 if current weight bucket = horse's optimal weight bucket
  - wdc_weight_distance_runs  : nb runs horse has at this weight × distance combo
  - wdc_lighter_vs_heavier    : horse's win rate when lighter than usual vs heavier than usual
                                (positive = better when lighter, negative = better heavier)
  - wdc_weight_per_km         : poids_porte / (distance / 1000) -- effort metric
  - wdc_weight_per_km_vs_avg  : current weight_per_km / horse's average weight_per_km

Usage:
    python feature_builders/weight_distance_combo_builder.py
    python feature_builders/weight_distance_combo_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/weight_distance_combo_builder.py --output-dir /path/to/output/
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/weight_distance_combo")
OUTPUT_DIR_FALLBACK = _PROJECT_ROOT / "output" / "weight_distance_combo"

_LOG_EVERY = 500_000

# Weight buckets (kg)
_LIGHT_MAX = 54.0     # < 54 kg  → "light"
_HEAVY_MIN = 58.0     # > 58 kg  → "heavy"
# Between _LIGHT_MAX and _HEAVY_MIN → "medium"

# Distance buckets (metres)
_DIST_BUCKETS = [
    (0,    1200, "sprint"),         # < 1200 m
    (1200, 1700, "mile"),           # 1200 – 1699 m
    (1700, 2200, "middle"),         # 1700 – 2199 m
    (2200, 2800, "long"),           # 2200 – 2799 m
    (2800, 999999, "marathon"),     # >= 2800 m
]

# Minimum runs required for a weight bucket to be considered "optimal"
_MIN_RUNS_OPTIMAL = 3

# Minimum runs for lighter_vs_heavier to be non-null
_MIN_RUNS_LIGHTER_HEAVIER = 3

# Weight-per-km history window per horse
_WPK_HISTORY = 20


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file one at a time (streaming)."""
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


def _weight_bucket(poids: float) -> str:
    """Classify a weight (kg) into a bucket string."""
    if poids < _LIGHT_MAX:
        return "light"
    elif poids <= _HEAVY_MIN:
        return "medium"
    else:
        return "heavy"


def _distance_bucket(distance: float) -> Optional[str]:
    """Classify a distance (metres) into a bucket string."""
    for lo, hi, label in _DIST_BUCKETS:
        if lo <= distance < hi:
            return label
    return None


def _horse_id(rec: dict) -> Optional[str]:
    """Return a stable horse identifier from a record."""
    hid = rec.get("horse_id") or rec.get("nom_cheval")
    if hid:
        return str(hid).strip()
    return None


def _weight_from_rec(rec: dict) -> Optional[float]:
    """Extract weight (kg) from a record, trying multiple field names."""
    w = _safe_float(rec.get("poids_porte_kg"))
    if w is None:
        w = _safe_float(rec.get("poids_porte"))
    return w if (w is not None and w > 0) else None


def _is_winner(rec: dict) -> bool:
    """Return True if the horse finished 1st."""
    pos = rec.get("position_arrivee")
    if pos is None:
        return bool(rec.get("is_gagnant"))
    try:
        return int(pos) == 1
    except (ValueError, TypeError):
        return bool(rec.get("is_gagnant"))


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================

WDBucket = Tuple[str, str]  # (weight_bucket, dist_bucket)


class _HorseWDCState:
    """
    Per-horse accumulated state for weight × distance combo analysis.

    All dicts are keyed by string bucket labels for clarity and memory efficiency.
    """

    __slots__ = (
        "per_weight_dist",
        "per_weight_bucket",
        "weight_per_km_history",
        "avg_weight_sum",
        "avg_weight_count",
    )

    def __init__(self) -> None:
        # {(weight_bucket, dist_bucket): {"wins": int, "total": int}}
        self.per_weight_dist: dict[WDBucket, dict[str, int]] = defaultdict(
            lambda: {"wins": 0, "total": 0}
        )
        # {weight_bucket: {"wins": int, "total": int}}
        self.per_weight_bucket: dict[str, dict[str, int]] = defaultdict(
            lambda: {"wins": 0, "total": 0}
        )
        # Recent weight_per_km values (for average computation)
        self.weight_per_km_history: deque = deque(maxlen=_WPK_HISTORY)
        # Running average for raw weight (to classify lighter/heavier than usual)
        self.avg_weight_sum: float = 0.0
        self.avg_weight_count: int = 0

    # ------------------------------------------------------------------
    # Snapshot (read features BEFORE update)
    # ------------------------------------------------------------------

    def snapshot(
        self,
        w_bucket: Optional[str],
        d_bucket: Optional[str],
        poids: Optional[float],
        distance: Optional[float],
    ) -> dict[str, Any]:
        """
        Compute features using only past races (strict temporal integrity).
        Called BEFORE updating state with the current race result.
        """
        feats: dict[str, Any] = {}

        # ---- wdc_weight_bucket ----
        feats["wdc_weight_bucket"] = w_bucket  # derived from current race, not history

        # ---- wdc_weight_per_km ----
        if poids is not None and distance is not None and distance > 0:
            wpk = round(poids / (distance / 1000.0), 4)
        else:
            wpk = None
        feats["wdc_weight_per_km"] = wpk

        # ---- wdc_weight_per_km_vs_avg ----
        if wpk is not None and self.weight_per_km_history:
            avg_wpk = sum(self.weight_per_km_history) / len(self.weight_per_km_history)
            feats["wdc_weight_per_km_vs_avg"] = round(wpk / avg_wpk, 4) if avg_wpk > 0 else None
        else:
            feats["wdc_weight_per_km_vs_avg"] = None

        # ---- wdc_weight_distance_wr  &  wdc_weight_distance_runs ----
        if w_bucket is not None and d_bucket is not None:
            key: WDBucket = (w_bucket, d_bucket)
            combo = self.per_weight_dist.get(key)
            if combo and combo["total"] > 0:
                feats["wdc_weight_distance_wr"] = round(combo["wins"] / combo["total"], 4)
                feats["wdc_weight_distance_runs"] = combo["total"]
            else:
                feats["wdc_weight_distance_wr"] = None
                feats["wdc_weight_distance_runs"] = 0
        else:
            feats["wdc_weight_distance_wr"] = None
            feats["wdc_weight_distance_runs"] = 0

        # ---- wdc_horse_optimal_weight  &  wdc_weight_match ----
        optimal = self._optimal_weight_bucket()
        feats["wdc_horse_optimal_weight"] = optimal
        if optimal is not None and w_bucket is not None:
            feats["wdc_weight_match"] = 1 if optimal == w_bucket else 0
        else:
            feats["wdc_weight_match"] = None

        # ---- wdc_lighter_vs_heavier ----
        feats["wdc_lighter_vs_heavier"] = self._lighter_vs_heavier(poids)

        return feats

    def _optimal_weight_bucket(self) -> Optional[str]:
        """Weight bucket with the highest win rate (min _MIN_RUNS_OPTIMAL runs)."""
        best_bucket = None
        best_rate = -1.0
        for bucket, stats in self.per_weight_bucket.items():
            total = stats["total"]
            if total < _MIN_RUNS_OPTIMAL:
                continue
            rate = stats["wins"] / total
            if rate > best_rate:
                best_rate = rate
                best_bucket = bucket
        return best_bucket

    def _lighter_vs_heavier(self, poids: Optional[float]) -> Optional[float]:
        """
        Win rate delta: win_rate_when_lighter_than_avg - win_rate_when_heavier_than_avg.
        Positive => horse performs better when weight is below its historical average.
        None if not enough data.

        We approximate "lighter than usual" as weight < historical average, using the
        per_weight_bucket stats (light vs medium+heavy).

        Simpler robust approach: compare light-bucket win rate vs heavy-bucket win rate.
        Returns (light_wr - heavy_wr) if both buckets have enough runs.
        """
        light = self.per_weight_bucket.get("light")
        heavy = self.per_weight_bucket.get("heavy")

        light_ok = light is not None and light["total"] >= _MIN_RUNS_LIGHTER_HEAVIER
        heavy_ok = heavy is not None and heavy["total"] >= _MIN_RUNS_LIGHTER_HEAVIER

        if not (light_ok and heavy_ok):
            return None

        light_wr = light["wins"] / light["total"]
        heavy_wr = heavy["wins"] / heavy["total"]
        return round(light_wr - heavy_wr, 4)

    # ------------------------------------------------------------------
    # Update (called AFTER snapshot)
    # ------------------------------------------------------------------

    def update(
        self,
        w_bucket: Optional[str],
        d_bucket: Optional[str],
        poids: Optional[float],
        distance: Optional[float],
        won: bool,
    ) -> None:
        """Incorporate the result of the current race into the state."""
        # Per weight × distance combo
        if w_bucket is not None and d_bucket is not None:
            key: WDBucket = (w_bucket, d_bucket)
            cell = self.per_weight_dist[key]
            cell["total"] += 1
            if won:
                cell["wins"] += 1

        # Per weight bucket only
        if w_bucket is not None:
            wb_cell = self.per_weight_bucket[w_bucket]
            wb_cell["total"] += 1
            if won:
                wb_cell["wins"] += 1

        # Weight per km history
        if poids is not None and distance is not None and distance > 0:
            wpk = poids / (distance / 1000.0)
            self.weight_per_km_history.append(wpk)

        # Running average weight
        if poids is not None:
            self.avg_weight_sum += poids
            self.avg_weight_count += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_weight_distance_combo_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """
    Build weight × distance combo features from partants_master.jsonl.

    Algorithm (temporal: index + sort + seek):
      1. Read all records, keep only the minimal fields needed.
      2. Sort chronologically by (date, course_uid, num_pmu).
      3. Process course by course in order:
         a. Snapshot features for all runners (using past state only).
         b. Update state for all runners with the outcome.
    """
    logger.info("=== Weight × Distance Combo Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Read minimal fields
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        poids = _weight_from_rec(rec)
        distance = _safe_float(rec.get("distance"))
        horse = _horse_id(rec)

        w_bucket = _weight_bucket(poids) if poids is not None else None
        d_bucket = _distance_bucket(distance) if (distance is not None and distance > 0) else None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", "") or "",
            "course": rec.get("course_uid", "") or "",
            "num": int(rec.get("num_pmu") or 0),
            "horse": horse,
            "poids": poids,
            "distance": distance,
            "w_bucket": w_bucket,
            "d_bucket": d_bucket,
            "won": _is_winner(rec),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3: Process course by course  (snapshot THEN update)
    # ------------------------------------------------------------------
    t2 = time.time()
    horse_states: dict[str, _HorseWDCState] = defaultdict(_HorseWDCState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all runners in the same course
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

        # ---- a) Snapshot: features BEFORE update ----
        for rec in course_group:
            horse = rec["horse"]
            feats: dict[str, Any] = {"partant_uid": rec["uid"]}

            if horse:
                state = horse_states[horse]
                snap = state.snapshot(
                    w_bucket=rec["w_bucket"],
                    d_bucket=rec["d_bucket"],
                    poids=rec["poids"],
                    distance=rec["distance"],
                )
                feats.update(snap)
            else:
                # Unknown horse → all features null
                feats.update(
                    {
                        "wdc_weight_bucket": rec["w_bucket"],
                        "wdc_weight_distance_wr": None,
                        "wdc_horse_optimal_weight": None,
                        "wdc_weight_match": None,
                        "wdc_weight_distance_runs": 0,
                        "wdc_lighter_vs_heavier": None,
                        "wdc_weight_per_km": (
                            round(rec["poids"] / (rec["distance"] / 1000.0), 4)
                            if rec["poids"] is not None
                            and rec["distance"] is not None
                            and rec["distance"] > 0
                            else None
                        ),
                        "wdc_weight_per_km_vs_avg": None,
                    }
                )

            results.append(feats)

        # ---- b) Update: incorporate race outcomes ----
        for rec in course_group:
            horse = rec["horse"]
            if horse:
                horse_states[horse].update(
                    w_bucket=rec["w_bucket"],
                    d_bucket=rec["d_bucket"],
                    poids=rec["poids"],
                    distance=rec["distance"],
                    won=rec["won"],
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Weight distance combo build termine: %d features en %.1fs"
        " (chevaux uniques: %d, courses: %d)",
        len(results),
        elapsed,
        len(horse_states),
        sum(1 for _ in set(r["course"] for r in slim_records)),
    )

    # Free large intermediate structure
    del slim_records, horse_states
    gc.collect()

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


def _resolve_output_dir(cli_path: Optional[str]) -> Path:
    if cli_path:
        return Path(cli_path)
    if OUTPUT_DIR.parent.exists():
        return OUTPUT_DIR
    return OUTPUT_DIR_FALLBACK


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features poids × distance a partir de partants_master.jsonl"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: auto-detection)",
    )
    args = parser.parse_args()

    logger = setup_logging("weight_distance_combo_builder")

    input_path = _find_input(args.input)
    output_dir = _resolve_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    results = build_weight_distance_combo_features(input_path, logger)

    out_path = output_dir / "weight_distance_combo.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [
            k for k in results[0].keys()
            if k not in ("partant_uid",)
        ]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates (sur %d partants) ===", total_count)
        for k in feature_keys:
            v = filled[k]
            logger.info("  %-40s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)

    logger.info("Termine en %.1fs total", time.time() - t0)


if __name__ == "__main__":
    main()
