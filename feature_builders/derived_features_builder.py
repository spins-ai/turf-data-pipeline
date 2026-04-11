#!/usr/bin/env python3
"""
feature_builders.derived_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
9 derived features from field interactions and ratios discovered
via correlation analysis on partants_master.

These features combine existing raw fields that are individually weak
predictors but become informative when paired.

Temporal integrity: all source fields are point-in-time safe (career stats
and sequence fields already respect temporal ordering in the pipeline).

Produces:
  - derived_features.jsonl  in output/derived_features/

Features per partant (9):
  - class_drop_x_gains     : spd_is_class_drop * gains_carriere_euros.
                              Horses dropping in class with high career earnings
                              are strong contenders (corr=0.158 with is_winner).
  - cote_vs_form            : cote_finale / seq_position_moy_5.
                              Odds-to-form ratio: low values signal "backed
                              horse with good recent form" (corr=0.158).
  - inedit_x_experience     : is_inedit * nb_courses_carriere.
                              Interaction capturing experienced horses marked
                              as "new" at a venue/distance (corr=0.116).
  - places_2_3_rate         : (nb_places_2eme + nb_places_3eme)
                              / nb_courses_carriere.
                              Consistency measure: fraction of minor placings
                              (2nd/3rd) across career (corr=0.123).
  - gains_per_race_rank     : rank of (gains_carriere_euros
                              / nb_courses_carriere) within the same course.
                              Percentile 0-1 where 0 = highest earnings
                              per race in the field (relative class measure).
  - gains_par_victoire      : gains_carriere_euros / nb_victoires_carriere.
                              Average earnings per win -- quality measure.
  - cote_ratio              : cote_finale / cote_reference.
                              Odds drift: >1 means drifted (less backed),
                              <1 means shortened (more backed).
  - poids_par_distance      : poids_porte_kg / (distance / 1000).
                              Weight burden per km -- higher = harder task.
  - gains_momentum          : gains_annee_euros / gains_carriere_euros.
                              Recent earnings share -- high = current form.

Usage:
    python feature_builders/derived_features_builder.py
    python feature_builders/derived_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"  # os.path.join(
OUTPUT_DIR_DEFAULT = "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/derived_features"

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# HELPERS
# ===========================================================================


def _get_float(row: dict, key: str) -> Optional[float]:
    """Safely extract a numeric value from a row."""
    val = row.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# BUILDER
# ===========================================================================


def build_derived_features(
    partants: list[dict],
    logger: logging.Logger | None = None,
) -> list[dict]:
    """Build 9 derived features from raw partants_master fields.

    Parameters
    ----------
    partants : list[dict]
        Records from partants_master (or enriched variant).
    logger : logging.Logger, optional
        Logger instance.

    Returns
    -------
    list[dict]
        Input records augmented with 9 new feature columns.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Phase 1: group runners by course for rank computation
    # ------------------------------------------------------------------
    course_runners: dict[str, list[int]] = defaultdict(list)
    for idx, row in enumerate(partants):
        cuid = row.get("course_uid")
        if cuid:
            course_runners[cuid].append(idx)

    # Pre-compute gains_per_race for every runner
    gains_per_race: dict[int, Optional[float]] = {}
    for idx, row in enumerate(partants):
        g = _get_float(row, "gains_carriere_euros")
        nc = _get_float(row, "nb_courses_carriere")
        if g is not None and nc is not None and nc > 0:
            gains_per_race[idx] = g / nc
        else:
            gains_per_race[idx] = None

    # Compute rank (percentile) of gains_per_race within each course
    gains_rank: dict[int, Optional[float]] = {}
    for cuid, indices in course_runners.items():
        scored = [
            (i, gains_per_race[i])
            for i in indices
            if gains_per_race[i] is not None
        ]
        if len(scored) < 2:
            for i in indices:
                gains_rank[i] = None
            continue
        # Sort descending (highest earnings/race = rank 0.0)
        scored.sort(key=lambda x: -(x[1] or 0))  # type: ignore[arg-type]
        n_scored = len(scored)
        for rank_pos, (i, _) in enumerate(scored):
            # Percentile: 0.0 = best, 1.0 = worst
            gains_rank[i] = rank_pos / (n_scored - 1) if n_scored > 1 else 0.5
        # Runners without a score get None
        scored_set = {i for i, _ in scored}
        for i in indices:
            if i not in scored_set:
                gains_rank[i] = None

    # ------------------------------------------------------------------
    # Phase 2: compute per-runner features
    # ------------------------------------------------------------------
    enriched = 0
    results: list[dict] = []

    for idx, row in enumerate(partants):
        feat: dict[str, Optional[float]] = {}

        # --- 1. class_drop_x_gains ---
        cd = _get_float(row, "spd_is_class_drop")
        g = _get_float(row, "gains_carriere_euros")
        if cd is not None and g is not None:
            feat["class_drop_x_gains"] = round(cd * g, 2)
        else:
            feat["class_drop_x_gains"] = None

        # --- 2. cote_vs_form ---
        cf = _get_float(row, "cote_finale")
        pm5 = _get_float(row, "seq_position_moy_5")
        if cf is not None and pm5 is not None and pm5 > 0:
            feat["cote_vs_form"] = round(cf / pm5, 4)
        else:
            feat["cote_vs_form"] = None

        # --- 3. inedit_x_experience ---
        ie = _get_float(row, "is_inedit")
        nc = _get_float(row, "nb_courses_carriere")
        if ie is not None and nc is not None:
            feat["inedit_x_experience"] = round(ie * nc, 1)
        else:
            feat["inedit_x_experience"] = None

        # --- 4. places_2_3_rate ---
        p2 = _get_float(row, "nb_places_2eme")
        p3 = _get_float(row, "nb_places_3eme")
        if p2 is not None and p3 is not None and nc is not None and nc > 0:
            feat["places_2_3_rate"] = round((p2 + p3) / nc, 4)
        else:
            feat["places_2_3_rate"] = None

        # --- 5. gains_per_race_rank ---
        feat["gains_per_race_rank"] = (
            round(gains_rank[idx], 4)
            if idx in gains_rank and gains_rank[idx] is not None
            else None
        )

        # --- 6. gains_par_victoire ---
        nv = _get_float(row, "nb_victoires_carriere")
        if g is not None and nv is not None and nv > 0:
            feat["gains_par_victoire"] = round(g / nv, 2)
        else:
            feat["gains_par_victoire"] = None

        # --- 7. cote_ratio ---
        cr = _get_float(row, "cote_reference")
        if cf is not None and cr is not None and cr > 0:
            feat["cote_ratio"] = round(cf / cr, 4)
        else:
            feat["cote_ratio"] = None

        # --- 8. poids_par_distance ---
        ppk = _get_float(row, "poids_porte_kg")
        dist = _get_float(row, "distance")
        if ppk is not None and dist is not None and dist > 0:
            feat["poids_par_distance"] = round(ppk / (dist / 1000), 4)
        else:
            feat["poids_par_distance"] = None

        # --- 9. gains_momentum ---
        ga = _get_float(row, "gains_annee_euros")
        if ga is not None and g is not None and g > 0:
            feat["gains_momentum"] = round(ga / g, 4)
        else:
            feat["gains_momentum"] = None

        if any(v is not None for v in feat.values()):
            enriched += 1

        row.update(feat)
        results.append(row)

        if (idx + 1) % _LOG_EVERY == 0:
            logger.info(
                "  %d/%d traites, %d enrichis",
                idx + 1,
                len(partants),
                enriched,
            )

    logger.info(
        "Derived features: %d/%d enrichis (%.1f%%)",
        enriched,
        len(results),
        100 * enriched / max(len(results), 1),
    )
    return results


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="9 derived features from field interactions and ratios"
    )
    parser.add_argument(
        "--input",
        default=PARTANTS_DEFAULT,
        help="Partants JSONL/JSON file",
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR_DEFAULT,
        help="Output directory",
    )
    args = parser.parse_args()

    logger = setup_logging("derived_features")
    logger.info("=" * 70)
    logger.info("derived_features_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_derived_features(partants, logger)

    out_path = os.path.join(args.output_dir, "derived_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine -- %d partants traites", len(results))


if __name__ == "__main__":
    main()
