#!/usr/bin/env python3
"""
feature_builders.interaction_pairs_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
20 high-predictive multiplicative interaction pairs for horse racing ML.

Reads partants_master.jsonl in single-pass streaming mode and computes
pure product (A * B) interaction features.  No temporal state is needed
-- every input field is already a pre-race snapshot.

Produces:
  - interaction_pairs_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/interaction_pairs/

Features per partant (20):
  1.  int_class_x_cote           : spd_class_rating * cote_finale
  2.  int_speed_x_partants       : spd_speed_figure * nombre_partants
  3.  int_wins_x_jt_synergy      : nb_victoires_carriere * gnn_duo_jockey_entraineur_win_rate
  4.  int_form_x_class_edge      : seq_serie_places * spd_class_vs_field
  5.  int_meteo_x_speed          : met_impact_meteo_score * spd_speed_figure
  6.  int_stamina_x_distance     : ped_stamina_index * distance
  7.  int_draw_x_bias            : num_pmu * spd_bias_interieur
  8.  int_weight_x_distance      : poids_porte_kg * distance
  9.  int_age_x_distance         : age * distance
  10. int_cote_x_partants        : cote_finale * nombre_partants
  11. int_elo_x_field            : elo_combined (or spd_class_rating) * spd_field_strength_avg
  12. int_gains_year_x_career    : gains_annee_euros * nb_courses_carriere
  13. int_jockey_rides_x_wins    : gnn_jockey_nb_chevaux * nb_victoires_carriere
  14. int_cote_x_concentration   : cote_finale * rap_market_concentration
  15. int_repos_x_age            : seq_jours_depuis_derniere * age
  16. int_speed_x_cote           : spd_speed_figure * cote_finale
  17. int_form_x_cote            : seq_nb_victoires_recent_5 * cote_finale
  18. int_distance_x_partants    : distance * nombre_partants
  19. int_class_x_experience     : spd_class_rating * nb_courses_carriere
  20. int_weight_x_cote          : poids_porte_kg * cote_finale

Usage:
    python feature_builders/interaction_pairs_builder.py
    python feature_builders/interaction_pairs_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/interaction_pairs")
OUTPUT_FILE = "interaction_pairs_features.jsonl"

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    """Convert value to float or return None."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _safe_mul(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Multiply two optional floats; None if either is missing."""
    if a is None or b is None:
        return None
    result = a * b
    return round(result, 6) if math.isfinite(result) else None


# ===========================================================================
# INTERACTION PAIR DEFINITIONS
# ===========================================================================

# Each tuple: (output_name, field_a, field_b, fallback_a, fallback_b)
# fallback fields are tried when primary field is absent from the record.
PAIR_DEFS: list[tuple[str, str, str, Optional[str], Optional[str]]] = [
    # 1. class at odds
    ("int_class_x_cote", "spd_class_rating", "cote_finale", None, None),
    # 2. speed quality adjusted for field
    ("int_speed_x_partants", "spd_speed_figure", "nombre_partants", None, None),
    # 3. win ability x jt synergy
    ("int_wins_x_jt_synergy", "nb_victoires_carriere", "gnn_duo_jockey_entraineur_win_rate", None, None),
    # 4. recent form x class advantage
    ("int_form_x_class_edge", "seq_serie_places", "spd_class_vs_field", None, None),
    # 5. weather adjusted speed
    ("int_meteo_x_speed", "met_impact_meteo_score", "spd_speed_figure", None, None),
    # 6. stamina genes x distance
    ("int_stamina_x_distance", "ped_stamina_index", "distance", None, None),
    # 7. draw position x actual bias
    ("int_draw_x_bias", "num_pmu", "spd_bias_interieur", None, None),
    # 8. endurance load (galop)
    ("int_weight_x_distance", "poids_porte_kg", "distance", None, None),
    # 9. maturity x distance aptitude
    ("int_age_x_distance", "age", "distance", None, None),
    # 10. odds in context
    ("int_cote_x_partants", "cote_finale", "nombre_partants", None, None),
    # 11. elo x field strength (fallback: spd_class_rating if elo_combined absent)
    ("int_elo_x_field", "elo_combined", "spd_field_strength_avg", "spd_class_rating", None),
    # 12. current productivity
    ("int_gains_year_x_career", "gains_annee_euros", "nb_courses_carriere", None, None),
    # 13. jockey volume x horse quality
    ("int_jockey_rides_x_wins", "gnn_jockey_nb_chevaux", "nb_victoires_carriere", None, None),
    # 14. odds x market concentration
    ("int_cote_x_concentration", "cote_finale", "rap_market_concentration", None, None),
    # 15. rest relative to age (fallback: jours_depuis_derniere if seq_ version absent)
    ("int_repos_x_age", "seq_jours_depuis_derniere", "age", "jours_depuis_derniere", None),
    # 16. speed at odds (value signal)
    ("int_speed_x_cote", "spd_speed_figure", "cote_finale", None, None),
    # 17. recent wins at odds
    ("int_form_x_cote", "seq_nb_victoires_recent_5", "cote_finale", None, None),
    # 18. race difficulty index
    ("int_distance_x_partants", "distance", "nombre_partants", None, None),
    # 19. class x experience
    ("int_class_x_experience", "spd_class_rating", "nb_courses_carriere", None, None),
    # 20. weight burden at odds
    ("int_weight_x_cote", "poids_porte_kg", "cote_finale", None, None),
]

FEATURE_NAMES = [p[0] for p in PAIR_DEFS]


def _get_val(rec: dict, primary: str, fallback: Optional[str]) -> Optional[float]:
    """Get float value from record, trying fallback field if primary is None."""
    val = _safe_float(rec.get(primary))
    if val is None and fallback:
        val = _safe_float(rec.get(fallback))
    return val


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_interaction_pairs(input_path: Path, output_path: Path, logger) -> int:
    """Stream partants_master.jsonl and compute 20 interaction pairs.

    Single-pass: read one record, compute products, write output line.
    Returns the number of records written.
    """
    logger.info("=== Interaction Pairs Builder ===")
    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", output_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_read = 0
    n_written = 0
    n_errors = 0
    fill_counts = {name: 0 for name in FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
                continue

            n_read += 1

            # Build output record with identifiers
            out = {
                "partant_uid": rec.get("partant_uid"),
                "course_uid": rec.get("course_uid"),
                "date_reunion_iso": rec.get("date_reunion_iso"),
            }

            # Compute all 20 interaction pairs
            for feat_name, field_a, field_b, fb_a, fb_b in PAIR_DEFS:
                a = _get_val(rec, field_a, fb_a)
                b = _get_val(rec, field_b, fb_b)
                val = _safe_mul(a, b)
                out[feat_name] = val
                if val is not None:
                    fill_counts[feat_name] += 1

            fout.write(json.dumps(out, ensure_ascii=False))
            fout.write("\n")
            n_written += 1

            # Progress + GC
            if n_read % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_read)
            if n_read % _GC_EVERY == 0:
                gc.collect()

    # Atomic replace
    if os.path.exists(output_path):
        os.replace(tmp_out, output_path)
    else:
        os.rename(tmp_out, output_path)

    elapsed = time.time() - t0
    logger.info("Lecture terminee: %d records, %d erreurs JSON", n_read, n_errors)
    logger.info(
        "Interaction pairs build termine: %d features ecrites en %.1fs",
        n_written, elapsed,
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for name in FEATURE_NAMES:
        cnt = fill_counts[name]
        pct = 100.0 * cnt / n_written if n_written else 0.0
        logger.info("  %-30s: %7d / %d  (%.1f%%)", name, cnt, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="20 interaction pairs multiplicatives pour modeles ML turf"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_DEFAULT,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("interaction_pairs_builder")

    input_path = Path(args.input) if args.input else INPUT_DEFAULT
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    out_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_path = out_dir / OUTPUT_FILE

    n = build_interaction_pairs(input_path, output_path, logger)
    logger.info("Termine. %d records ecrits dans %s", n, output_path)


if __name__ == "__main__":
    main()
