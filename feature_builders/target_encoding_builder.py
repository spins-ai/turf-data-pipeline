#!/usr/bin/env python3
"""
feature_builders.target_encoding_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Smoothed target-encoded features for high-cardinality categoricals.

Temporal integrity is strictly enforced: for any partant at date D, only races
with date < D contribute to the running category statistics.  Within a race
(same date + course_uid), statistics are frozen BEFORE any updates — no
same-race leakage.

Algorithm
---------
  1. Read minimal fields into memory (streaming).
  2. Sort all records chronologically (date_reunion_iso, course_uid, num_pmu).
  3. Iterate course-by-course.  For each race group:
       a. Snapshot current global wins/total.
       b. Emit smoothed TE for every runner using the pre-race snapshot.
       c. Update global and per-category counters.

Smoothing formula (m = 20):
    te = (n * category_mean + m * global_mean) / (n + m)

where:
    n            = number of past observations for the category key
    category_mean = wins_cat / n  (historical win rate for that category)
    global_mean  = wins_global / total_global  (running global win rate)
    m            = 20  (smoothing / prior strength)

When n == 0 (category never seen before), te falls back to global_mean
(which itself starts from the first race seen).  When global_total == 0
the feature is emitted as None.

Features (8):
  - te_jockey_win_rate     : smoothed win rate for the jockey
  - te_trainer_win_rate    : smoothed win rate for the trainer
  - te_hippodrome_win_rate : smoothed win rate at the hippodrome
  - te_sire_win_rate       : smoothed win rate for the sire (nom_pere)
  - te_hippo_distance_wr   : smoothed win rate for (hippodrome, distance_bucket) pair
  - te_jockey_trainer_wr   : smoothed win rate for (jockey, trainer) pair
  - te_discipline_wr       : smoothed win rate for the discipline / specialite
  - te_terrain_wr          : smoothed win rate for the terrain type (etat_terrain)

Output
------
  D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/target_encoding/
      target_encoding_features.jsonl

Usage:
    python feature_builders/target_encoding_builder.py
    python feature_builders/target_encoding_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/target_encoding_builder.py --output /path/to/output_dir
    python feature_builders/target_encoding_builder.py --smoothing 30
"""

from __future__ import annotations

import argparse
import gc
import json
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/target_encoding")

# Smoothing prior weight — m in: te = (n*cat_mean + m*global_mean) / (n + m)
SMOOTHING_M = 20

# Progress log every N records
_LOG_EVERY = 500_000

# Distance bucket boundaries (metres) — closed on left, open on right
_DISTANCE_BUCKETS: list[tuple[int, int, str]] = [
    (0,    1100, "sprint"),
    (1100, 1600, "mile_court"),
    (1600, 2000, "mile"),
    (2000, 2500, "moyen"),
    (2500, 3200, "long"),
    (3200, 9999, "tres_long"),
]

# ===========================================================================
# HELPERS — field extraction
# ===========================================================================


def _distance_bucket(distance_raw) -> Optional[str]:
    """Map a raw distance value (int / float / str in metres) to a bucket label."""
    if distance_raw is None:
        return None
    try:
        d = int(distance_raw)
    except (ValueError, TypeError):
        return None
    for lo, hi, label in _DISTANCE_BUCKETS:
        if lo <= d < hi:
            return label
    return "tres_long"


def _is_win(rec: dict) -> bool:
    """Return True when this partant finished 1st.

    Checks is_gagnant first (boolean flag), then falls back to position_arrivee.
    """
    if rec.get("is_gagnant"):
        return True
    pos = rec.get("position_arrivee")
    if pos is None:
        return False
    try:
        return int(pos) == 1
    except (ValueError, TypeError):
        return False


def _jockey(rec: dict) -> Optional[str]:
    return rec.get("jockey") or rec.get("nom_jockey")


def _trainer(rec: dict) -> Optional[str]:
    return rec.get("entraineur") or rec.get("nom_entraineur")


def _hippodrome(rec: dict) -> Optional[str]:
    return rec.get("hippodrome")


def _sire(rec: dict) -> Optional[str]:
    return rec.get("nom_pere")


def _discipline(rec: dict) -> Optional[str]:
    return rec.get("discipline") or rec.get("specialite")


def _terrain(rec: dict) -> Optional[str]:
    return rec.get("etat_terrain")


# ===========================================================================
# HELPERS — I/O
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield parsed dicts from a JSONL file line-by-line (constant RAM)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
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
# STATE CONTAINERS
# ===========================================================================


class _CatStats:
    """Win / total accumulator for a single category key."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def smoothed_te(self, glob_wins: int, glob_total: int, m: int) -> Optional[float]:
        """Return the smoothed target-encoded rate.

        te = (n * cat_mean + m * global_mean) / (n + m)

        Falls back to global_mean when n == 0.
        Returns None when global_total == 0 (no prior available yet).
        """
        if glob_total == 0:
            return None
        global_mean = glob_wins / glob_total
        n = self.total
        if n == 0:
            return round(global_mean, 6)
        cat_mean = self.wins / n
        te = (n * cat_mean + m * global_mean) / (n + m)
        return round(te, 6)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_target_encoding_features(
    input_path: Path,
    output_dir: Path,
    logger,
    smoothing_m: int = SMOOTHING_M,
) -> None:
    """Compute and write smoothed target-encoded features.

    Parameters
    ----------
    input_path  : Path to partants_master.jsonl
    output_dir  : Directory for output files (created if absent)
    logger      : Standard Python logger
    smoothing_m : Prior weight m (default SMOOTHING_M = 20)
    """
    logger.info("=== Target Encoding Builder ===")
    logger.info("Smoothing m=%d", smoothing_m)
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1 — Read minimal fields into memory (streaming)
    # ------------------------------------------------------------------
    slim_records: list[dict[str, Any]] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        jock = _jockey(rec)
        train = _trainer(rec)
        hippo = _hippodrome(rec)
        dist_bucket = _distance_bucket(rec.get("distance"))

        # Composite keys — None if either component is missing
        hippo_dist = (
            f"{hippo}|{dist_bucket}"
            if hippo is not None and dist_bucket is not None
            else None
        )
        jock_train = (
            f"{jock}|{train}"
            if jock is not None and train is not None
            else None
        )

        slim_records.append({
            "uid":         rec.get("partant_uid"),
            "course":      rec.get("course_uid", ""),
            "date":        rec.get("date_reunion_iso", ""),
            "num":         rec.get("num_pmu", 0) or 0,
            "win":         _is_win(rec),
            # category keys (one per feature dimension)
            "k_jockey":        jock,
            "k_trainer":       train,
            "k_hippodrome":    hippo,
            "k_sire":          _sire(rec),
            "k_hippo_dist":    hippo_dist,
            "k_jock_train":    jock_train,
            "k_discipline":    _discipline(rec),
            "k_terrain":       _terrain(rec),
        })

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2 — Sort chronologically (date, course_uid, num_pmu)
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3 — Iterate course-by-course, emit features, update state
    # ------------------------------------------------------------------
    t2 = time.time()

    # Global counters
    glob_wins: int = 0
    glob_total: int = 0

    # Per-dimension category stats (key → _CatStats)
    # Dimension names map to output feature names
    _DIMS: list[tuple[str, str]] = [
        ("k_jockey",     "te_jockey_win_rate"),
        ("k_trainer",    "te_trainer_win_rate"),
        ("k_hippodrome", "te_hippodrome_win_rate"),
        ("k_sire",       "te_sire_win_rate"),
        ("k_hippo_dist", "te_hippo_distance_wr"),
        ("k_jock_train", "te_jockey_trainer_wr"),
        ("k_discipline", "te_discipline_wr"),
        ("k_terrain",    "te_terrain_wr"),
    ]
    cat_stats: dict[str, defaultdict] = {
        dim_key: defaultdict(_CatStats) for dim_key, _ in _DIMS
    }

    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    i = 0
    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]

        # --- Collect all runners of this race (same date + course_uid) ---
        course_group: list[dict] = []
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        # Snapshot global counts BEFORE updating with this race
        snap_glob_wins = glob_wins
        snap_glob_total = glob_total

        # --- Emit features for each runner using pre-race snapshot ---
        for runner in course_group:
            row: dict[str, Any] = {
                "partant_uid":      runner["uid"],
                "course_uid":       course_uid,
                "date_reunion_iso": course_date,
            }
            for dim_key, feat_name in _DIMS:
                cat_key = runner.get(dim_key)
                if cat_key is None:
                    row[feat_name] = None
                else:
                    row[feat_name] = cat_stats[dim_key][cat_key].smoothed_te(
                        snap_glob_wins, snap_glob_total, smoothing_m
                    )
            results.append(row)
            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traitement %d / %d...", n_processed, total)

        # --- Update state AFTER all runners emitted (no leakage) ---
        for runner in course_group:
            won = runner["win"]
            glob_total += 1
            if won:
                glob_wins += 1
            for dim_key, _ in _DIMS:
                cat_key = runner.get(dim_key)
                if cat_key is None:
                    continue
                s = cat_stats[dim_key][cat_key]
                s.total += 1
                if won:
                    s.wins += 1

    elapsed_phase3 = time.time() - t2
    logger.info(
        "Phase 3 terminee: %d records en %.1fs (%.0f rec/s)",
        n_processed,
        elapsed_phase3,
        n_processed / elapsed_phase3 if elapsed_phase3 > 0 else 0,
    )

    # ------------------------------------------------------------------
    # Phase 4 — Write output JSONL
    # ------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "target_encoding_features.jsonl"
    logger.info("Ecriture: %s", out_path)
    t3 = time.time()

    with open(out_path, "w", encoding="utf-8") as fout:
        for row in results:
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")

    logger.info(
        "Ecriture terminee: %d lignes en %.1fs",
        len(results),
        time.time() - t3,
    )

    # ------------------------------------------------------------------
    # Summary statistics
    # ------------------------------------------------------------------
    logger.info("--- Statistiques ---")
    global_win_rate = glob_wins / glob_total if glob_total else 0.0
    logger.info(
        "  Global: %d courses, %d victoires (win rate=%.4f)",
        glob_total, glob_wins, global_win_rate,
    )
    for dim_key, feat_name in _DIMS:
        n_cats = len(cat_stats[dim_key])
        logger.info("  %-20s -> %d categories distinctes", feat_name, n_cats)

    # Fill rate per feature
    logger.info("--- Fill rates ---")
    n_total = len(results)
    if n_total:
        for _, feat_name in _DIMS:
            n_filled = sum(1 for r in results if r.get(feat_name) is not None)
            logger.info(
                "  %-25s %d / %d (%.1f%%)",
                feat_name,
                n_filled,
                n_total,
                100.0 * n_filled / n_total,
            )

    # Free memory
    del slim_records, results, cat_stats
    gc.collect()

    logger.info("=== Done. Duree totale: %.1fs ===", time.time() - t0)


# ===========================================================================
# CLI
# ===========================================================================


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build smoothed target-encoded features for high-cardinality categoricals. "
            "Temporal integrity guaranteed: stats are updated only after each race is emitted."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=INPUT_PARTANTS,
        help=(
            "Path to partants_master.jsonl "
            f"(default: {INPUT_PARTANTS})"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--smoothing",
        type=int,
        default=SMOOTHING_M,
        help=f"Smoothing prior weight m (default: {SMOOTHING_M})",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logger = setup_logging("target_encoding_builder")

    logger.info("Input     : %s", args.input)
    logger.info("Output    : %s", args.output)
    logger.info("Smoothing : m=%d", args.smoothing)

    if not args.input.exists():
        logger.error("Fichier introuvable: %s", args.input)
        sys.exit(1)

    build_target_encoding_features(
        input_path=args.input,
        output_dir=args.output,
        logger=logger,
        smoothing_m=args.smoothing,
    )


if __name__ == "__main__":
    main()
