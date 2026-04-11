#!/usr/bin/env python3
"""
feature_builders.field_composition_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep field composition analysis -- understanding WHO is in the race.

Goes beyond simple field-size counts by profiling the age distribution,
gender balance, gains hierarchy, experience spread, and weight allocation
of each race's participants, then positioning each horse relative to its
field.

Temporal integrity: all features are computed from the race's own declared
data (no historical leakage).  Career counters (gains, victories, nb_courses)
are assumed already available at race time.

Produces:
  - field_composition_deep.jsonl  in builder_outputs/field_composition_deep/

Features per partant (10):
  - fcd_field_avg_age          : average age of all horses in the race
  - fcd_horse_age_vs_field     : horse's age minus field average age
  - fcd_field_pct_male         : percentage of males (sexe M or H) in the field
  - fcd_field_avg_gains        : average gains_carriere_euros across the field
  - fcd_horse_gains_rank       : rank of horse's gains within the field (1=highest)
  - fcd_field_avg_win_rate     : average (nb_victoires/nb_courses) across field
  - fcd_horse_wr_rank          : rank of horse's win rate within the field
  - fcd_nb_debutants           : number of horses with nb_courses_carriere < 3
  - fcd_field_weight_spread    : max - min poids_porte in the field
  - fcd_horse_weight_rank      : rank of horse's poids_porte (1=heaviest)

Two-pass approach:
  Pass 1 -- group partants by course_uid, store minimal per-horse stats.
  Pass 2 -- re-read and emit per-partant features using precomputed course data.

Usage:
    python feature_builders/field_composition_deep_builder.py
    python feature_builders/field_composition_deep_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_composition_deep")

_LOG_EVERY = 500_000

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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _win_rate(nb_victoires, nb_courses) -> Optional[float]:
    """Compute win rate from career counters. Returns None if no data."""
    v = _safe_int(nb_victoires)
    c = _safe_int(nb_courses)
    if c is None or c <= 0:
        return None
    v = v or 0
    return v / c


def _rank_descending(values: list[Optional[float]], uid_list: list[str]) -> dict[str, Optional[int]]:
    """Rank values in descending order (highest = rank 1).

    Returns {uid: rank} dict.  Horses with None values get rank None.
    """
    indexed = [(uid, val) for uid, val in zip(uid_list, values) if val is not None]
    indexed.sort(key=lambda x: x[1], reverse=True)
    ranks: dict[str, Optional[int]] = {}
    for rank, (uid, _) in enumerate(indexed, start=1):
        ranks[uid] = rank
    # None for horses without a value
    for uid, val in zip(uid_list, values):
        if val is None:
            ranks[uid] = None
    return ranks


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_field_composition_deep(input_path: Path, output_path: Path, logger) -> int:
    """Build deep field composition features.

    Two-pass approach:
      Pass 1 -- stream through JSONL, group minimal stats by course_uid.
      Pass 2 -- re-stream, compute per-partant features, write to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Field Composition Deep Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # ---------------------------------------------------------------
    # Pass 1: group per-horse stats by course_uid
    # ---------------------------------------------------------------
    # course_uid -> list of {uid, age, sexe, gains, nb_vic, nb_courses, poids}
    course_horses: dict[str, list[dict[str, Any]]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 -- lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid", "")
        if not course_uid:
            continue

        course_horses[course_uid].append({
            "uid": rec.get("partant_uid"),
            "age": _safe_int(rec.get("age")),
            "sexe": (rec.get("sexe") or "").strip().upper(),
            "gains": _safe_float(rec.get("gains_carriere_euros")),
            "nb_vic": _safe_int(rec.get("nb_victoires_carriere")),
            "nb_courses": _safe_int(rec.get("nb_courses_carriere")),
            "poids": _safe_float(rec.get("poids_porte_kg")),
        })

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        n_read, len(course_horses), time.time() - t0,
    )

    # ---------------------------------------------------------------
    # Precompute per-course aggregate stats + per-horse ranks
    # ---------------------------------------------------------------
    # course_uid -> {field_avg_age, field_pct_male, field_avg_gains,
    #                field_avg_win_rate, nb_debutants, field_weight_spread,
    #                ranks_gains: {uid: rank}, ranks_wr: {uid: rank},
    #                ranks_weight: {uid: rank}}
    course_stats: dict[str, dict[str, Any]] = {}

    for course_uid, horses in course_horses.items():
        n = len(horses)

        # -- fcd_field_avg_age --
        ages = [h["age"] for h in horses if h["age"] is not None]
        field_avg_age = (sum(ages) / len(ages)) if ages else None

        # -- fcd_field_pct_male --
        if n > 0:
            nb_male = sum(1 for h in horses if h["sexe"] in ("M", "H"))
            field_pct_male = round(nb_male / n, 4)
        else:
            field_pct_male = None

        # -- fcd_field_avg_gains --
        gains_vals = [h["gains"] for h in horses if h["gains"] is not None]
        field_avg_gains = round(sum(gains_vals) / len(gains_vals), 2) if gains_vals else None

        # -- fcd_field_avg_win_rate --
        win_rates = []
        for h in horses:
            wr = _win_rate(h["nb_vic"], h["nb_courses"])
            if wr is not None:
                win_rates.append(wr)
        field_avg_win_rate = round(sum(win_rates) / len(win_rates), 4) if win_rates else None

        # -- fcd_nb_debutants (nb_courses_carriere < 3) --
        nb_debutants = 0
        for h in horses:
            nc = h["nb_courses"]
            if nc is not None and nc < 3:
                nb_debutants += 1
            elif nc is None:
                nb_debutants += 1  # unknown experience = treat as debutant

        # -- fcd_field_weight_spread --
        poids_vals = [h["poids"] for h in horses if h["poids"] is not None]
        if len(poids_vals) >= 2:
            field_weight_spread = round(max(poids_vals) - min(poids_vals), 2)
        else:
            field_weight_spread = None

        # -- Rankings --
        uid_list = [h["uid"] for h in horses]

        # Gains rank (1 = highest gains)
        gains_for_rank = [h["gains"] for h in horses]
        ranks_gains = _rank_descending(gains_for_rank, uid_list)

        # Win rate rank (1 = highest win rate)
        wr_for_rank = [_win_rate(h["nb_vic"], h["nb_courses"]) for h in horses]
        ranks_wr = _rank_descending(wr_for_rank, uid_list)

        # Weight rank (1 = heaviest)
        poids_for_rank = [h["poids"] for h in horses]
        ranks_weight = _rank_descending(poids_for_rank, uid_list)

        course_stats[course_uid] = {
            "field_avg_age": field_avg_age,
            "field_pct_male": field_pct_male,
            "field_avg_gains": field_avg_gains,
            "field_avg_win_rate": field_avg_win_rate,
            "nb_debutants": nb_debutants,
            "field_weight_spread": field_weight_spread,
            "ranks_gains": ranks_gains,
            "ranks_wr": ranks_wr,
            "ranks_weight": ranks_weight,
        }

    logger.info("Precompute termine pour %d courses", len(course_stats))

    # ---------------------------------------------------------------
    # Pass 2: re-stream and emit per-partant features
    # ---------------------------------------------------------------
    t1 = time.time()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    fill_counts: dict[str, int] = defaultdict(int)
    feature_keys = [
        "fcd_field_avg_age",
        "fcd_horse_age_vs_field",
        "fcd_field_pct_male",
        "fcd_field_avg_gains",
        "fcd_horse_gains_rank",
        "fcd_field_avg_win_rate",
        "fcd_horse_wr_rank",
        "fcd_nb_debutants",
        "fcd_field_weight_spread",
        "fcd_horse_weight_rank",
    ]

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        n_pass2 = 0
        for rec in _iter_jsonl(input_path, logger):
            n_pass2 += 1
            if n_pass2 % _LOG_EVERY == 0:
                logger.info("  Pass 2 -- traite %d records...", n_pass2)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid", "")
            cs = course_stats.get(course_uid)

            features: dict[str, Any] = {"partant_uid": partant_uid}

            if cs is None:
                # No course stats -- emit all None
                for k in feature_keys:
                    features[k] = None
            else:
                # 1. fcd_field_avg_age
                features["fcd_field_avg_age"] = (
                    round(cs["field_avg_age"], 2) if cs["field_avg_age"] is not None else None
                )

                # 2. fcd_horse_age_vs_field
                horse_age = _safe_int(rec.get("age"))
                if horse_age is not None and cs["field_avg_age"] is not None:
                    features["fcd_horse_age_vs_field"] = round(horse_age - cs["field_avg_age"], 2)
                else:
                    features["fcd_horse_age_vs_field"] = None

                # 3. fcd_field_pct_male
                features["fcd_field_pct_male"] = cs["field_pct_male"]

                # 4. fcd_field_avg_gains
                features["fcd_field_avg_gains"] = cs["field_avg_gains"]

                # 5. fcd_horse_gains_rank
                features["fcd_horse_gains_rank"] = cs["ranks_gains"].get(partant_uid)

                # 6. fcd_field_avg_win_rate
                features["fcd_field_avg_win_rate"] = cs["field_avg_win_rate"]

                # 7. fcd_horse_wr_rank
                features["fcd_horse_wr_rank"] = cs["ranks_wr"].get(partant_uid)

                # 8. fcd_nb_debutants
                features["fcd_nb_debutants"] = cs["nb_debutants"]

                # 9. fcd_field_weight_spread
                features["fcd_field_weight_spread"] = cs["field_weight_spread"]

                # 10. fcd_horse_weight_rank
                features["fcd_horse_weight_rank"] = cs["ranks_weight"].get(partant_uid)

            # Track fill rates
            for k in feature_keys:
                if features.get(k) is not None:
                    fill_counts[k] += 1

            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Field Composition Deep termine: %d features en %.1fs (%d courses)",
        n_written, elapsed, len(course_stats),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k in feature_keys:
        v = fill_counts.get(k, 0)
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de composition profonde du champ"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("field_composition_deep_builder")
    logger.info("=" * 70)
    logger.info("field_composition_deep_builder.py — Deep field composition features")
    logger.info("=" * 70)

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "field_composition_deep.jsonl"
    build_field_composition_deep(input_path, out_path, logger)


if __name__ == "__main__":
    main()
