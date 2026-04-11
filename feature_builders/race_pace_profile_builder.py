#!/usr/bin/env python3
"""
feature_builders.race_pace_profile_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Estimates race pace profile from field composition.

For each partant, computes 8 features that characterise the competitive
context of the race based on the career statistics of all starters:

  rpp_field_avg_wins_pct       – average win percentage of all horses in the race
  rpp_field_avg_experience     – average nb_courses_carriere in the field
  rpp_field_max_wins_pct       – highest individual win percentage in the field
  rpp_horse_rank_by_wins_pct   – this horse's rank by win% (1 = best in field)
  rpp_horse_rank_by_earnings   – this horse's rank by gains_carriere_euros (1 = richest)
  rpp_quality_gap              – horse's win_pct minus field_avg_win_pct (positive = above avg)
  rpp_top_contenders           – count of horses with win_pct > 15% (strong field indicator)
  rpp_field_competitiveness    – std-dev of win percentages across the field

Temporal integrity: only career counters already accumulated at race time are
used (nb_courses_carriere, nb_victoires_carriere, gains_carriere_euros).
No future data leakage.

Two-pass streaming design keeps RAM well below 1 GB even on 500 K+ records:
  Pass 1 – read JSONL line-by-line, group slim dicts by course_uid.
  Pass 2 – compute features per race group, write output JSONL.

Produces:
  race_pace_profile.jsonl   in OUTPUT_DIR

Usage:
    python feature_builders/race_pace_profile_builder.py
    python feature_builders/race_pace_profile_builder.py \\
        --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl \\
        --output-dir D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_pace_profile
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

INPUT_PARTANTS = Path(
    "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"
)
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_pace_profile"
)

# Fields extracted from each raw line (keeps RAM low during Pass 1)
_KEEP_FIELDS = (
    "partant_uid",
    "course_uid",
    "num_pmu",
    "date_reunion_iso",
    "nb_victoires_carriere",
    "nb_courses_carriere",
    "gains_carriere_euros",
)

# Progress log every N lines read
_LOG_EVERY = 500_000

# Threshold for "top contender" classification
_TOP_CONTENDER_WIN_PCT = 0.15  # 15 %

# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float, return None on failure."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _win_pct(nb_victoires: Any, nb_courses: Any) -> Optional[float]:
    """Career win percentage. Returns None when data is missing or zero races."""
    try:
        nv = float(nb_victoires or 0)
        nc = float(nb_courses)
    except (TypeError, ValueError):
        return None
    if nc <= 0:
        return None
    return nv / nc


def _safe_mean(values: list[float]) -> Optional[float]:
    """Arithmetic mean of a non-empty list, None otherwise."""
    if not values:
        return None
    return sum(values) / len(values)


def _safe_stdev(values: list[float]) -> Optional[float]:
    """Population standard deviation, None if fewer than 2 values."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return math.sqrt(variance)


def _rank_descending(values: list[Optional[float]]) -> list[Optional[int]]:
    """
    Dense descending rank (1 = highest value).
    Positions with None values receive None as rank.
    """
    indexed = [
        (i, v) for i, v in enumerate(values) if v is not None
    ]
    # Sort highest first
    indexed_sorted = sorted(indexed, key=lambda x: x[1], reverse=True)
    ranks: list[Optional[int]] = [None] * len(values)
    for rank, (orig_idx, _) in enumerate(indexed_sorted, start=1):
        ranks[orig_idx] = rank
    return ranks


# ===========================================================================
# RACE-LEVEL FEATURE COMPUTATION
# ===========================================================================


def _build_race_features(runners: list[dict]) -> list[dict]:
    """Compute race-pace-profile features for all starters in one race.

    Parameters
    ----------
    runners : list[dict]
        Slim partant records sharing the same course_uid.

    Returns
    -------
    list[dict]
        One feature dict per runner (keyed by partant_uid + course_uid).
    """
    n = len(runners)

    # --- Compute per-runner win percentages and earnings ---
    win_pcts: list[Optional[float]] = []
    earnings: list[Optional[float]] = []
    experiences: list[Optional[float]] = []

    for r in runners:
        wp = _win_pct(r.get("nb_victoires_carriere"), r.get("nb_courses_carriere"))
        win_pcts.append(wp)

        eg = _safe_float(r.get("gains_carriere_euros"))
        earnings.append(eg)

        nc = _safe_float(r.get("nb_courses_carriere"))
        experiences.append(nc)

    # Filter out None values for field-level aggregations
    wp_known = [v for v in win_pcts if v is not None]
    exp_known = [v for v in experiences if v is not None]

    # --- Field-level features ---
    field_avg_wins_pct: Optional[float] = _safe_mean(wp_known)
    field_avg_experience: Optional[float] = _safe_mean(exp_known)
    field_max_wins_pct: Optional[float] = max(wp_known) if wp_known else None
    field_competitiveness: Optional[float] = _safe_stdev(wp_known)

    top_contenders: int = sum(
        1 for v in wp_known if v > _TOP_CONTENDER_WIN_PCT
    )

    # --- Per-runner ranks (descending: 1 = best) ---
    ranks_by_win_pct = _rank_descending(win_pcts)
    ranks_by_earnings = _rank_descending(earnings)

    # --- Assemble output records ---
    results: list[dict] = []
    for i, r in enumerate(runners):
        wp_i = win_pcts[i]

        # quality_gap: signed diff vs field average (None if either is None)
        quality_gap: Optional[float] = None
        if wp_i is not None and field_avg_wins_pct is not None:
            quality_gap = round(wp_i - field_avg_wins_pct, 6)

        feat = {
            "partant_uid": r.get("partant_uid"),
            "course_uid": r.get("course_uid"),
            "date_reunion_iso": r.get("date_reunion_iso"),
            "num_pmu": r.get("num_pmu"),
            # 8 pace-profile features
            "rpp_field_avg_wins_pct": (
                round(field_avg_wins_pct, 6)
                if field_avg_wins_pct is not None
                else None
            ),
            "rpp_field_avg_experience": (
                round(field_avg_experience, 2)
                if field_avg_experience is not None
                else None
            ),
            "rpp_field_max_wins_pct": (
                round(field_max_wins_pct, 6)
                if field_max_wins_pct is not None
                else None
            ),
            "rpp_horse_rank_by_wins_pct": ranks_by_win_pct[i],
            "rpp_horse_rank_by_earnings": ranks_by_earnings[i],
            "rpp_quality_gap": quality_gap,
            "rpp_top_contenders": top_contenders,
            "rpp_field_competitiveness": (
                round(field_competitiveness, 6)
                if field_competitiveness is not None
                else None
            ),
        }
        results.append(feat)

    return results


# ===========================================================================
# TWO-PASS STREAMING BUILDER
# ===========================================================================


def build_race_pace_profile(
    input_path: Path,
    output_dir: Path,
    logger,
) -> int:
    """Stream *input_path* (JSONL), group by course_uid, compute features,
    write output JSONL.  Returns the number of feature records written.

    Pass 1 – read every line, keep only _KEEP_FIELDS, accumulate in memory
              grouped by course_uid.
    Pass 2 – for each race group, compute features and stream to output file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "race_pace_profile.jsonl"

    # ------------------------------------------------------------------
    # Pass 1: group by course_uid
    # ------------------------------------------------------------------
    logger.info("Pass 1 — lecture de %s", input_path)
    course_groups: dict[str, list[dict]] = defaultdict(list)
    n_read = 0
    t0 = time.perf_counter()

    with open(input_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            slim = {k: row.get(k) for k in _KEEP_FIELDS}
            cuid = slim.get("course_uid") or ""
            course_groups[cuid].append(slim)

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                elapsed = time.perf_counter() - t0
                logger.info(
                    "  %d lignes lues, %d courses | %.1f s",
                    n_read, len(course_groups), elapsed,
                )

    elapsed_p1 = time.perf_counter() - t0
    logger.info(
        "Pass 1 terminee — %d partants, %d courses uniques (%.1f s)",
        n_read, len(course_groups), elapsed_p1,
    )

    # ------------------------------------------------------------------
    # Pass 2: compute features per race, stream to output
    # ------------------------------------------------------------------
    logger.info("Pass 2 — calcul des features et ecriture vers %s", out_path)
    n_written = 0
    t1 = time.perf_counter()

    with open(out_path, "w", encoding="utf-8", newline="\n") as out_fh:
        for cuid, runners in course_groups.items():
            if not cuid:
                continue
            feats = _build_race_features(runners)
            for f in feats:
                out_fh.write(json.dumps(f, ensure_ascii=False) + "\n")
                n_written += 1

    elapsed_p2 = time.perf_counter() - t1
    logger.info(
        "Pass 2 terminee — %d enregistrements ecrits (%.1f s)",
        n_written, elapsed_p2,
    )

    # Free memory
    del course_groups
    gc.collect()

    return n_written


# ===========================================================================
# STATS REPORT
# ===========================================================================


def _fill_rate_report(out_path: Path, logger) -> None:
    """Read output JSONL and log fill rate per feature."""
    feature_cols = [
        "rpp_field_avg_wins_pct",
        "rpp_field_avg_experience",
        "rpp_field_max_wins_pct",
        "rpp_horse_rank_by_wins_pct",
        "rpp_horse_rank_by_earnings",
        "rpp_quality_gap",
        "rpp_top_contenders",
        "rpp_field_competitiveness",
    ]
    counts: dict[str, int] = {k: 0 for k in feature_cols}
    total = 0

    with open(out_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1
            for col in feature_cols:
                if rec.get(col) is not None:
                    counts[col] += 1

    if total == 0:
        logger.warning("Aucun enregistrement dans le fichier de sortie.")
        return

    logger.info("Fill rate par feature (%d partants):", total)
    for col in feature_cols:
        pct = 100.0 * counts[col] / total
        logger.info("  %-38s %d/%d  (%.1f%%)", col, counts[col], total, pct)


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "race_pace_profile_builder — "
            "Estime le profil de rythme d'une course a partir de la composition du champ."
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(INPUT_PARTANTS),
        help="Chemin vers partants_master.jsonl",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help="Repertoire de sortie (fichier race_pace_profile.jsonl cree dedans)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_pace_profile_builder")
    logger.info("=" * 70)
    logger.info("race_pace_profile_builder.py — Profil de rythme par course")
    logger.info("=" * 70)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    t_start = time.perf_counter()

    n_written = build_race_pace_profile(input_path, output_dir, logger)

    total_elapsed = time.perf_counter() - t_start
    logger.info(
        "Termine — %d features ecrites en %.1f s", n_written, total_elapsed
    )

    # Fill rate report
    out_path = output_dir / "race_pace_profile.jsonl"
    if out_path.exists() and n_written > 0:
        _fill_rate_report(out_path, logger)


if __name__ == "__main__":
    main()
