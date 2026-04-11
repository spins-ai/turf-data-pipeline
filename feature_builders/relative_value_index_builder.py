#!/usr/bin/env python3
"""
feature_builders.relative_value_index_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Relative value index features -- comprehensive value assessment combining
multiple dimensions (power index, field comparison, odds mismatch).

Reads partants_master.jsonl in streaming mode.
Two-pass approach:
  Pass 1 -- per-course aggregation: build lists of {num_pmu, cote, power_index}
  Pass 2 -- per-partant: compute relative value features using field context

Temporal integrity: all inputs (career stats, cote_finale) are pre-race
data already available before the race -- no future leakage.

Produces:
  - relative_value_index.jsonl  in builder_outputs/relative_value_index/

Features per partant (10):
  - rvi_horse_power_index     : (vic*5 + p2*3 + p3*2 + places) / max(courses,1)
  - rvi_field_power_index_avg : average power index of the field
  - rvi_horse_power_vs_field  : horse power index / field average
  - rvi_horse_power_rank      : rank by power index in field (1=best)
  - rvi_odds_power_mismatch   : power_rank - odds_rank (positive = undervalued)
  - rvi_value_index           : horse_power_vs_field * cote_finale
  - rvi_is_strong_value       : 1 if power_rank <= 3 AND odds_rank > 5
  - rvi_is_weak_favorite      : 1 if odds_rank <= 3 AND power_rank > 5
  - rvi_field_power_spread    : max - min power index in field
  - rvi_horse_vs_top_rival    : horse power index / highest power in field

Usage:
    python feature_builders/relative_value_index_builder.py
    python feature_builders/relative_value_index_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/relative_value_index")

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
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _compute_power_index(rec: dict) -> Optional[float]:
    """Compute horse power index from career stats.

    Formula: (vic*5 + p2*3 + p3*2 + places) / max(courses, 1)
    """
    nb_courses = _safe_int(rec.get("nb_courses"))
    nb_vic = _safe_int(rec.get("nb_victoires"))
    nb_p2 = _safe_int(rec.get("nb_places_2eme"))
    nb_p3 = _safe_int(rec.get("nb_places_3eme"))
    nb_places = _safe_int(rec.get("nb_places_carriere"))

    if nb_courses is None:
        return None

    vic = nb_vic or 0
    p2 = nb_p2 or 0
    p3 = nb_p3 or 0
    places = nb_places or 0

    numerator = vic * 5 + p2 * 3 + p3 * 2 + places
    denominator = max(nb_courses, 1)
    return round(numerator / denominator, 4)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_relative_value_index_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build relative value index features from partants_master.jsonl."""
    logger.info("=== Relative Value Index Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -----------------------------------------------------------------------
    # Pass 1: Read minimal fields, compute per-horse power index
    # -----------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference"))

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cote": cote,
            "power_index": _compute_power_index(rec),
        }
        slim_records.append(slim)

    logger.info(
        "Pass 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # Sort chronologically
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -----------------------------------------------------------------------
    # Pass 2: Group by course, compute field-level and per-partant features
    # -----------------------------------------------------------------------
    t2 = time.time()
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

        # -- Collect field-level power indices and cotes --
        power_indices: list[Optional[float]] = [r["power_index"] for r in course_group]
        cotes: list[Optional[float]] = [r["cote"] for r in course_group]

        valid_powers = [p for p in power_indices if p is not None]
        valid_cotes = [c for c in cotes if c is not None and c > 0]

        # Field average power index
        field_avg: Optional[float] = None
        if valid_powers:
            field_avg = round(sum(valid_powers) / len(valid_powers), 4)

        # Field power spread (max - min)
        field_spread: Optional[float] = None
        max_power: Optional[float] = None
        if len(valid_powers) >= 2:
            max_power = max(valid_powers)
            field_spread = round(max_power - min(valid_powers), 4)
        elif len(valid_powers) == 1:
            max_power = valid_powers[0]

        # -- Rank by power index (descending: highest power = rank 1) --
        power_with_idx = []
        for idx, p in enumerate(power_indices):
            power_with_idx.append((idx, p if p is not None else -999.0))
        power_with_idx.sort(key=lambda x: x[1], reverse=True)
        power_ranks = [0] * len(course_group)
        for rank, (orig_idx, _) in enumerate(power_with_idx, start=1):
            power_ranks[orig_idx] = rank

        # -- Rank by cote (ascending: lowest cote = rank 1 = favorite) --
        cote_with_idx = []
        for idx, c in enumerate(cotes):
            cote_with_idx.append((idx, c if c is not None and c > 0 else 9999.0))
        cote_with_idx.sort(key=lambda x: x[1])
        odds_ranks = [0] * len(course_group)
        for rank, (orig_idx, _) in enumerate(cote_with_idx, start=1):
            odds_ranks[orig_idx] = rank

        # -- Emit features per partant --
        for idx, rec in enumerate(course_group):
            pi = power_indices[idx]
            cote = cotes[idx]
            p_rank = power_ranks[idx]
            o_rank = odds_ranks[idx]

            # rvi_horse_power_vs_field
            power_vs_field: Optional[float] = None
            if pi is not None and field_avg is not None and field_avg > 0:
                power_vs_field = round(pi / field_avg, 4)

            # rvi_odds_power_mismatch: power_rank - odds_rank
            mismatch: Optional[int] = None
            if pi is not None and cote is not None and cote > 0:
                mismatch = p_rank - o_rank

            # rvi_value_index: horse_power_vs_field * cote_finale
            value_index: Optional[float] = None
            if power_vs_field is not None and cote is not None and cote > 0:
                value_index = round(power_vs_field * cote, 4)

            # rvi_is_strong_value: power_rank <= 3 AND odds_rank > 5
            is_strong_value: Optional[int] = None
            if pi is not None and cote is not None and cote > 0:
                is_strong_value = int(p_rank <= 3 and o_rank > 5)

            # rvi_is_weak_favorite: odds_rank <= 3 AND power_rank > 5
            is_weak_fav: Optional[int] = None
            if pi is not None and cote is not None and cote > 0:
                is_weak_fav = int(o_rank <= 3 and p_rank > 5)

            # rvi_horse_vs_top_rival: power / max power
            vs_top: Optional[float] = None
            if pi is not None and max_power is not None and max_power > 0:
                vs_top = round(pi / max_power, 4)

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "rvi_horse_power_index": pi,
                "rvi_field_power_index_avg": field_avg,
                "rvi_horse_power_vs_field": power_vs_field,
                "rvi_horse_power_rank": p_rank if pi is not None else None,
                "rvi_odds_power_mismatch": mismatch,
                "rvi_value_index": value_index,
                "rvi_is_strong_value": is_strong_value,
                "rvi_is_weak_favorite": is_weak_fav,
                "rvi_field_power_spread": field_spread,
                "rvi_horse_vs_top_rival": vs_top,
            }
            results.append(features)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Relative value index build termine: %d features en %.1fs",
        len(results), elapsed,
    )

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features relative value index a partir de partants_master"
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

    logger = setup_logging("relative_value_index_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_relative_value_index_features(input_path, logger)

    # Save (save_jsonl uses .tmp then rename, open with newline="\n")
    out_path = output_dir / "relative_value_index.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rates
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
