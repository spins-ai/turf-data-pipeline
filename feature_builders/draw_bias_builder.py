#!/usr/bin/env python3
"""
feature_builders.draw_bias_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Draw (stall position) bias features per hippodrome + distance combination.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant draw bias features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the draw statistics -- no future leakage.

Produces:
  - draw_bias.jsonl   in output/draw_bias/

Features per partant:
  - draw_win_rate          : historical win rate from this draw at hippo+distance
  - draw_place_rate        : historical place rate (top 3) from this draw
  - draw_advantage         : draw_win_rate / avg_win_rate for the hippo+distance (>1 = advantaged)
  - draw_inside_bias       : win rate of draws 1-4 vs 5+ at this hippo+distance
  - draw_position_normalized : numPmu / nb_partants (0-1 scale, 0=inside, 1=outside)
  - draw_nb_samples        : number of historical races this draw stat is based on

Usage:
    python feature_builders/draw_bias_builder.py
    python feature_builders/draw_bias_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "draw_bias"

# Inside draw threshold (draws 1..INSIDE_MAX are "inside")
INSIDE_MAX = 4

# Minimum samples before emitting stats (otherwise None)
MIN_SAMPLES = 1

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# DRAW STATS ACCUMULATOR
# ===========================================================================


class _DrawStats:
    """Accumulates win/place counts for a specific (hippo, dist_cat, draw)."""

    __slots__ = ("wins", "places", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.total: int = 0


class _HippoDistStats:
    """Aggregated stats for a (hippo, dist_cat) combination across all draws."""

    __slots__ = ("total_wins", "total_runners", "inside_wins", "inside_total",
                 "outside_wins", "outside_total")

    def __init__(self) -> None:
        self.total_wins: int = 0
        self.total_runners: int = 0
        self.inside_wins: int = 0
        self.inside_total: int = 0
        self.outside_wins: int = 0
        self.outside_total: int = 0


# ===========================================================================
# HELPERS
# ===========================================================================


def _distance_category(distance) -> Optional[int]:
    """Round distance to nearest 200m bucket."""
    if distance is None:
        return None
    try:
        d = int(distance)
    except (ValueError, TypeError):
        return None
    if d <= 0:
        return None
    return round(d / 200) * 200


def _is_placed(position) -> bool:
    """Return True if position is in top 3."""
    if position is None:
        return False
    try:
        return int(position) <= 3
    except (ValueError, TypeError):
        return False


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


def _sort_key(rec: dict) -> tuple:
    """Sort key: date, course_uid, num_pmu for determinism."""
    return (
        rec.get("date_reunion_iso", ""),
        rec.get("course_uid", ""),
        rec.get("num_pmu", 0) or 0,
    )


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_draw_bias_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build draw bias features from partants_master.jsonl.

    Single-pass approach: read minimal fields, sort chronologically,
    then process course-by-course with strict temporal integrity.

    For each partant, we snapshot the accumulated draw stats BEFORE
    updating them with the current race results.
    """
    logger.info("=== Draw Bias Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
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
            "hippo": rec.get("hippodrome_normalise", ""),
            "distance": rec.get("distance"),
            "nb_partants": rec.get("nb_partants"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": rec.get("position_arrivee"),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()

    # Accumulated stats: (hippo, dist_cat, draw) -> _DrawStats
    draw_stats: dict[tuple[str, int, int], _DrawStats] = defaultdict(_DrawStats)
    # Accumulated stats: (hippo, dist_cat) -> _HippoDistStats
    hippo_dist_stats: dict[tuple[str, int], _HippoDistStats] = defaultdict(_HippoDistStats)

    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants for this course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (i < total
               and slim_records[i]["course"] == course_uid
               and slim_records[i]["date"] == course_date):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # Determine hippo + distance_category for this course
        hippo = course_group[0].get("hippo", "")
        raw_distance = course_group[0].get("distance")
        dist_cat = _distance_category(raw_distance)

        # Determine nb_partants from field or fallback to group size
        nb_partants_raw = course_group[0].get("nb_partants")
        try:
            nb_partants = int(nb_partants_raw) if nb_partants_raw is not None else len(course_group)
        except (ValueError, TypeError):
            nb_partants = len(course_group)
        if nb_partants <= 0:
            nb_partants = len(course_group)

        can_compute = bool(hippo) and dist_cat is not None
        hd_key = (hippo, dist_cat) if can_compute else None

        # -- Snapshot pre-race stats & emit features --
        for rec in course_group:
            draw = rec["num"]
            draw_pos_norm = (draw / nb_partants) if (draw and nb_partants > 0) else None

            if can_compute and draw and draw > 0:
                ds_key = (hippo, dist_cat, draw)
                ds = draw_stats.get(ds_key)
                hds = hippo_dist_stats.get(hd_key)

                if ds is not None and ds.total >= MIN_SAMPLES:
                    win_rate = ds.wins / ds.total
                    place_rate = ds.places / ds.total

                    # Average win rate across all draws at this hippo+dist
                    avg_win_rate = (
                        hds.total_wins / hds.total_runners
                        if hds is not None and hds.total_runners > 0
                        else None
                    )
                    advantage = (
                        win_rate / avg_win_rate
                        if avg_win_rate is not None and avg_win_rate > 0
                        else None
                    )

                    # Inside bias: win rate inside (1-4) vs outside (5+)
                    inside_bias = None
                    if hds is not None:
                        inside_wr = (
                            hds.inside_wins / hds.inside_total
                            if hds.inside_total > 0 else 0.0
                        )
                        outside_wr = (
                            hds.outside_wins / hds.outside_total
                            if hds.outside_total > 0 else 0.0
                        )
                        inside_bias = round(inside_wr - outside_wr, 6)

                    results.append({
                        "partant_uid": rec["uid"],
                        "draw_win_rate": round(win_rate, 6),
                        "draw_place_rate": round(place_rate, 6),
                        "draw_advantage": round(advantage, 4) if advantage is not None else None,
                        "draw_inside_bias": inside_bias,
                        "draw_position_normalized": round(draw_pos_norm, 4) if draw_pos_norm is not None else None,
                        "draw_nb_samples": ds.total,
                    })
                else:
                    # Not enough samples yet
                    results.append({
                        "partant_uid": rec["uid"],
                        "draw_win_rate": None,
                        "draw_place_rate": None,
                        "draw_advantage": None,
                        "draw_inside_bias": None,
                        "draw_position_normalized": round(draw_pos_norm, 4) if draw_pos_norm is not None else None,
                        "draw_nb_samples": 0,
                    })
            else:
                # Cannot compute (missing hippo, distance, or draw)
                results.append({
                    "partant_uid": rec["uid"],
                    "draw_win_rate": None,
                    "draw_place_rate": None,
                    "draw_advantage": None,
                    "draw_inside_bias": None,
                    "draw_position_normalized": round(draw_pos_norm, 4) if draw_pos_norm is not None else None,
                    "draw_nb_samples": None,
                })

        # -- Update stats AFTER emitting (temporal integrity) --
        if can_compute:
            for rec in course_group:
                draw = rec["num"]
                if not draw or draw <= 0:
                    continue

                ds_key = (hippo, dist_cat, draw)
                ds = draw_stats[ds_key]
                ds.total += 1
                if rec["gagnant"]:
                    ds.wins += 1
                if _is_placed(rec["position"]):
                    ds.places += 1

                hds = hippo_dist_stats[hd_key]
                hds.total_runners += 1
                if rec["gagnant"]:
                    hds.total_wins += 1
                if draw <= INSIDE_MAX:
                    hds.inside_total += 1
                    if rec["gagnant"]:
                        hds.inside_wins += 1
                else:
                    hds.outside_total += 1
                    if rec["gagnant"]:
                        hds.outside_wins += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Draw bias build termine: %d features en %.1fs "
        "(hippo+dist combos: %d, draw slots tracked: %d)",
        len(results), elapsed,
        len(hippo_dist_stats), len(draw_stats),
    )

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


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de biais de corde a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/draw_bias/)",
    )
    args = parser.parse_args()

    logger = setup_logging("draw_bias_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_draw_bias_features(input_path, logger)

    # Save
    out_path = output_dir / "draw_bias.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
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
