#!/usr/bin/env python3
"""
feature_builders.performances_croisees_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-references detailed past performance data with partants_master
to compute features based on each horse's recent race history.

Two-phase architecture (RAM-safe, max ~6 GB):
  Phase 1 - Stream performances_detaillees.jsonl, extract and aggregate
            past-performance vectors keyed by (course_uid, num_pmu).
  Phase 2 - Stream partants_master.jsonl, look up perf data per partant,
            compute 12 features, write output.

Features per partant (12):
  - prf_avg_position_5          : avg position over last 5 races
  - prf_best_position_5         : best (min) position over last 5 races
  - prf_position_trend          : linear slope of position over last 5 races
  - prf_avg_distance_diff       : avg difference (past distance - current distance)
  - prf_same_hippo_winrate      : win rate at same hippodrome
  - prf_same_distance_winrate   : win rate at same distance (+/- 200m)
  - prf_terrain_adaptability    : nb distinct terrains with a top-3 finish
  - prf_avg_allocation_level    : avg allocation of past races
  - prf_jockey_consistency      : 1 if same jockey as last race, else 0
  - prf_weight_trend            : weight change (last - previous)
  - prf_class_progression       : avg nb_partants of past races (class indicator)
  - prf_days_between_avg        : avg days between past races

Output:
  D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/
      performances_croisees/performances_croisees_features.jsonl

Usage:
    python feature_builders/performances_croisees_builder.py
    python feature_builders/performances_croisees_builder.py --perf <path> --master <path>
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_PERF_PATH = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/22_performances_detaillees/"
    "performances_detaillees.jsonl"
)
DEFAULT_MASTER_PATH = Path(
    "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"
)
DEFAULT_OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/performances_croisees"
)
OUTPUT_FILENAME = "performances_croisees_features.jsonl"

_LOG_EVERY = 500_000
_GC_EVERY = 500_000
_MAX_PERFS = 10  # extract perf_1 through perf_10


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


def _ms_to_date(ms_val) -> Optional[datetime]:
    """Convert a millisecond timestamp to a datetime object."""
    if ms_val is None:
        return None
    try:
        ts = float(ms_val) / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _linear_slope(values: list[float]) -> Optional[float]:
    """Compute the slope of a simple linear regression y = a + b*x.

    x = 0, 1, 2, ... (oldest to newest).
    Returns b (slope). Positive = positions getting worse (higher numbers).
    """
    n = len(values)
    if n < 2:
        return None
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if den == 0:
        return None
    return round(num / den, 4)


# ===========================================================================
# PHASE 1: BUILD PERF LOOKUP FROM performances_detaillees
# ===========================================================================


def _extract_perfs(rec: dict) -> dict:
    """Extract a compact performance summary from a performances_detaillees record.

    Returns a dict with lists of per-race data (up to _MAX_PERFS), keyed
    compactly to save RAM.
    """
    perfs = []
    for i in range(1, _MAX_PERFS + 1):
        prefix = f"perf_{i}_"
        pos = _safe_int(rec.get(f"{prefix}position"))
        if pos is None:
            # No more perfs available
            break
        perf = {
            "pos": pos,
            "dist": _safe_int(rec.get(f"{prefix}distance")),
            "hippo": (rec.get(f"{prefix}hippodrome") or "").strip().upper(),
            "terrain": (rec.get(f"{prefix}terrain") or "").strip().lower(),
            "alloc": _safe_int(rec.get(f"{prefix}allocation")),
            "jockey": (rec.get(f"{prefix}jockey") or "").strip().upper(),
            "poids": _safe_float(rec.get(f"{prefix}poids")),
            "nb_part": _safe_int(rec.get(f"{prefix}nb_partants")),
            "date": _ms_to_date(rec.get(f"{prefix}date")),
        }
        perfs.append(perf)

    if not perfs:
        return {}

    return {"perfs": perfs}


def _build_perf_lookup(perf_path: Path, logger) -> dict:
    """Phase 1: Stream performances_detaillees and build lookup dict.

    Key: (course_uid, num_pmu) -> compact perf dict.
    """
    logger.info("=== Phase 1: Chargement performances_detaillees ===")
    logger.info("  Fichier: %s", perf_path)
    t0 = time.time()

    lookup: dict[tuple[str, int], dict] = {}
    n_loaded = 0
    n_skipped = 0

    for rec in _iter_jsonl(perf_path, logger):
        course_uid = rec.get("course_uid", "")
        num_pmu = _safe_int(rec.get("num_pmu"))
        if not course_uid or num_pmu is None:
            n_skipped += 1
            continue

        extracted = _extract_perfs(rec)
        if not extracted:
            n_skipped += 1
            continue

        lookup[(course_uid, num_pmu)] = extracted
        n_loaded += 1

        if n_loaded % _LOG_EVERY == 0:
            logger.info("  Phase 1: %d records charges...", n_loaded)

        if n_loaded % _GC_EVERY == 0:
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Phase 1 terminee: %d entries en lookup, %d ignores, %.1fs",
        n_loaded, n_skipped, elapsed,
    )
    gc.collect()
    return lookup


# ===========================================================================
# PHASE 2: COMPUTE FEATURES
# ===========================================================================


_FEATURE_NAMES = [
    "prf_avg_position_5",
    "prf_best_position_5",
    "prf_position_trend",
    "prf_avg_distance_diff",
    "prf_same_hippo_winrate",
    "prf_same_distance_winrate",
    "prf_terrain_adaptability",
    "prf_avg_allocation_level",
    "prf_jockey_consistency",
    "prf_weight_trend",
    "prf_class_progression",
    "prf_days_between_avg",
]


def _compute_features(
    perf_data: dict,
    current_hippo: str,
    current_distance: Optional[int],
) -> dict[str, Any]:
    """Compute the 12 features from a perf_data dict.

    perf_data["perfs"] is a list ordered perf_1 (most recent) to perf_N.
    """
    features: dict[str, Any] = {k: None for k in _FEATURE_NAMES}

    perfs = perf_data.get("perfs", [])
    if not perfs:
        return features

    # -- Position features (last 5) --
    positions_5 = [p["pos"] for p in perfs[:5] if p["pos"] is not None]
    if positions_5:
        features["prf_avg_position_5"] = round(sum(positions_5) / len(positions_5), 2)
        features["prf_best_position_5"] = min(positions_5)

    # Trend: oldest to newest for regression (reverse the list)
    if len(positions_5) >= 2:
        features["prf_position_trend"] = _linear_slope(list(reversed(positions_5)))

    # -- Distance diff vs current race --
    if current_distance and current_distance > 0:
        diffs = []
        for p in perfs:
            d = p.get("dist")
            if d and d > 0:
                diffs.append(d - current_distance)
        if diffs:
            features["prf_avg_distance_diff"] = round(sum(diffs) / len(diffs), 0)

    # -- Same hippodrome win rate --
    current_hippo_upper = current_hippo.strip().upper()
    if current_hippo_upper:
        same_hippo = [p for p in perfs if p.get("hippo") == current_hippo_upper]
        if same_hippo:
            wins = sum(1 for p in same_hippo if p["pos"] == 1)
            features["prf_same_hippo_winrate"] = round(wins / len(same_hippo), 4)

    # -- Same distance win rate (+/- 200m) --
    if current_distance and current_distance > 0:
        same_dist = [
            p for p in perfs
            if p.get("dist") and abs(p["dist"] - current_distance) <= 200
        ]
        if same_dist:
            wins = sum(1 for p in same_dist if p["pos"] == 1)
            features["prf_same_distance_winrate"] = round(wins / len(same_dist), 4)

    # -- Terrain adaptability: nb distinct terrains with top-3 finish --
    terrain_placed = set()
    for p in perfs:
        terrain = p.get("terrain", "")
        if terrain and p["pos"] is not None and p["pos"] <= 3:
            terrain_placed.add(terrain)
    features["prf_terrain_adaptability"] = len(terrain_placed) if terrain_placed else 0

    # -- Avg allocation level --
    allocs = [p["alloc"] for p in perfs if p.get("alloc") and p["alloc"] > 0]
    if allocs:
        features["prf_avg_allocation_level"] = round(sum(allocs) / len(allocs), 0)

    # -- Jockey consistency: 1 if same jockey as last race --
    last_jockey = perfs[0].get("jockey", "") if perfs else ""
    if len(perfs) >= 1 and last_jockey:
        features["prf_jockey_consistency"] = 1
        # We mark 1 by default (same jockey as last race).
        # This will be overridden in phase 2 if we have the current jockey.
        # For now, store the last jockey name to compare later.
        features["_last_jockey"] = last_jockey
    else:
        features["prf_jockey_consistency"] = 0

    # -- Weight trend: last - previous --
    weights = [p["poids"] for p in perfs[:2] if p.get("poids") is not None]
    if len(weights) >= 2:
        features["prf_weight_trend"] = round(weights[0] - weights[1], 1)

    # -- Class progression: avg nb_partants of past races --
    nb_parts = [p["nb_part"] for p in perfs if p.get("nb_part") and p["nb_part"] > 0]
    if nb_parts:
        features["prf_class_progression"] = round(sum(nb_parts) / len(nb_parts), 1)

    # -- Days between races avg --
    dates = [p["date"] for p in perfs if p.get("date") is not None]
    if len(dates) >= 2:
        gaps = []
        for j in range(len(dates) - 1):
            delta = abs((dates[j] - dates[j + 1]).days)
            if delta > 0:
                gaps.append(delta)
        if gaps:
            features["prf_days_between_avg"] = round(sum(gaps) / len(gaps), 1)

    return features


def _build_features(
    master_path: Path,
    perf_lookup: dict,
    output_path: Path,
    logger,
) -> int:
    """Phase 2: Stream partants_master, compute features, write output."""
    logger.info("=== Phase 2: Calcul des features ===")
    logger.info("  Master: %s", master_path)
    logger.info("  Output: %s", output_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    n_matched = 0
    fill_counts = {k: 0 for k in _FEATURE_NAMES}

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(master_path, logger):
            n_processed += 1

            partant_uid = rec.get("partant_uid", "")
            course_uid = rec.get("course_uid", "")
            date_reunion_iso = rec.get("date_reunion_iso", "")
            num_pmu = _safe_int(rec.get("num_pmu"))

            if not partant_uid or not course_uid:
                continue

            # Lookup key
            key = (course_uid, num_pmu) if num_pmu is not None else None
            perf_data = perf_lookup.get(key) if key else None

            # Current race context
            current_hippo = (rec.get("hippodrome_normalise") or rec.get("hippodrome") or "")
            current_distance = _safe_int(rec.get("distance"))
            current_jockey = (rec.get("jockey") or "").strip().upper()

            # Compute features
            if perf_data:
                n_matched += 1
                features = _compute_features(perf_data, current_hippo, current_distance)
            else:
                features = {k: None for k in _FEATURE_NAMES}
                features["prf_terrain_adaptability"] = None
                features["prf_jockey_consistency"] = None

            # Resolve jockey consistency against current jockey
            last_jockey = features.pop("_last_jockey", "")
            if last_jockey and current_jockey:
                features["prf_jockey_consistency"] = 1 if last_jockey == current_jockey else 0
            elif not perf_data:
                features["prf_jockey_consistency"] = None

            # Build output record
            out_rec = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_reunion_iso,
            }
            for fname in _FEATURE_NAMES:
                val = features.get(fname)
                out_rec[fname] = val
                if val is not None:
                    fill_counts[fname] += 1

            fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            n_written += 1

            if n_processed % _LOG_EVERY == 0:
                logger.info("  Phase 2: %d records traites, %d ecrits...", n_processed, n_written)

            if n_processed % _GC_EVERY == 0:
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Phase 2 terminee: %d traites, %d ecrits, %d matches en %.1fs",
        n_processed, n_written, n_matched, elapsed,
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k in _FEATURE_NAMES:
        cnt = fill_counts[k]
        pct = 100.0 * cnt / n_written if n_written else 0
        logger.info("  %-30s : %d / %d (%.1f%%)", k, cnt, n_written, pct)

    return n_written


# ===========================================================================
# MAIN ORCHESTRATOR
# ===========================================================================


def build_performances_croisees(
    perf_path: Path,
    master_path: Path,
    output_dir: Path,
) -> int:
    """Run the full two-phase build."""
    logger = setup_logging("performances_croisees_builder")
    logger.info("=" * 60)
    logger.info("Performances Croisees Feature Builder")
    logger.info("=" * 60)
    t_global = time.time()

    # Phase 1: build lookup
    perf_lookup = _build_perf_lookup(perf_path, logger)

    # Phase 2: compute and write features
    output_path = output_dir / OUTPUT_FILENAME
    n_written = _build_features(master_path, perf_lookup, output_path, logger)

    # Free lookup
    del perf_lookup
    gc.collect()

    logger.info(
        "Build complet: %d features en %.1fs",
        n_written, time.time() - t_global,
    )
    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Performances croisees feature builder"
    )
    parser.add_argument(
        "--perf",
        type=str,
        default=None,
        help="Path to performances_detaillees.jsonl",
    )
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory",
    )
    args = parser.parse_args()

    perf_path = Path(args.perf) if args.perf else DEFAULT_PERF_PATH
    master_path = Path(args.master) if args.master else DEFAULT_MASTER_PATH
    output_dir = Path(args.output) if args.output else DEFAULT_OUTPUT_DIR

    if not perf_path.exists():
        print(f"ERREUR: Fichier performances introuvable: {perf_path}")
        sys.exit(1)
    if not master_path.exists():
        print(f"ERREUR: Fichier master introuvable: {master_path}")
        sys.exit(1)

    build_performances_croisees(perf_path, master_path, output_dir)


if __name__ == "__main__":
    main()
