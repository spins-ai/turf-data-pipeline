#!/usr/bin/env python3
"""
feature_builders.final_composite_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Final composite / meta features that combine multiple raw columns into
powerful aggregate signals for ML models.

Single-pass streaming: reads partants_master.jsonl once, computes 12 composite
features per partant, writes output as it goes.

Temporal integrity: all features are derived from the partant's own record
(career stats + race-day data). No future leakage.

Produces:
  - final_composite.jsonl  in builder_outputs/final_composite/

Features per partant (12):
  - fcm_power_rating         : (vic*3 + p2*2 + p3) / courses
  - fcm_career_roi_estimate  : gains / (courses * 1000 + 1)
  - fcm_implied_vs_actual_wr : proba_implicite - (vic/courses)
  - fcm_class_x_form         : log(gains+1) * (vic / courses)
  - fcm_weight_x_distance    : poids / distance * 1000
  - fcm_age_efficiency       : gains / (age * 1000 + 1)
  - fcm_handicap_x_weight    : handicap * poids / 100
  - fcm_field_adjusted_wr    : (vic/courses) * log(field+1)
  - fcm_versatility_score    : places_carriere / courses
  - fcm_strike_rate_log      : log(vic+1) / log(courses+2)
  - fcm_risk_reward          : cote * proba_implicite
  - fcm_completeness_score   : count(non-null key fields) / 10

Usage:
    python feature_builders/final_composite_builder.py
    python feature_builders/final_composite_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/final_composite")
_LOG_EVERY = 500_000

# Feature names for fill-rate tracking
_FEAT_NAMES = [
    "fcm_power_rating",
    "fcm_career_roi_estimate",
    "fcm_implied_vs_actual_wr",
    "fcm_class_x_form",
    "fcm_weight_x_distance",
    "fcm_age_efficiency",
    "fcm_handicap_x_weight",
    "fcm_field_adjusted_wr",
    "fcm_versatility_score",
    "fcm_strike_rate_log",
    "fcm_risk_reward",
    "fcm_completeness_score",
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _sf(val) -> Optional[float]:
    """Safe float conversion."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if f != f else f  # NaN check
    except (TypeError, ValueError):
        return None


def _si(val) -> Optional[int]:
    """Safe int conversion."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


# ===========================================================================
# BUILD
# ===========================================================================


def build(logger, input_path: Optional[Path] = None) -> None:
    t0 = time.time()
    src = input_path or INPUT_PARTANTS
    logger.info("=== Final Composite Builder (12 features) ===")
    logger.info("Input : %s", src)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "final_composite.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    fill = {k: 0 for k in _FEAT_NAMES}

    with open(src, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_written += 1
            if n_written % _LOG_EVERY == 0:
                logger.info("  %d records traites...", n_written)
                gc.collect()

            # --- Extract raw values ---
            partant_uid = rec.get("partant_uid", "")
            course_uid = rec.get("course_uid", "")
            date_str = rec.get("date_reunion_iso", "")

            nb_vic = _si(rec.get("nb_victoires_carriere"))
            nb_courses = _si(rec.get("nb_courses_carriere"))
            nb_p2 = _si(rec.get("nb_places_2eme"))
            nb_p3 = _si(rec.get("nb_places_3eme"))
            nb_places = _si(rec.get("nb_places_carriere"))
            gains = _sf(rec.get("gains_carriere_euros"))
            age = _si(rec.get("age"))
            distance = _si(rec.get("distance"))
            nombre_partants = _si(rec.get("nombre_partants"))
            poids = _sf(rec.get("poids_porte_kg"))
            handicap = _sf(rec.get("handicap_valeur"))
            cote = _sf(rec.get("cote_finale"))
            proba_impl = _sf(rec.get("proba_implicite"))
            discipline = rec.get("discipline")
            sexe = rec.get("sexe")

            out: dict = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }

            # 1. fcm_power_rating
            #    (vic*3 + p2*2 + p3) / courses  if courses > 0
            if nb_courses is not None and nb_courses > 0 and nb_vic is not None:
                p2 = nb_p2 if nb_p2 is not None else 0
                p3 = nb_p3 if nb_p3 is not None else 0
                val = (nb_vic * 3 + p2 * 2 + p3) / nb_courses
                out["fcm_power_rating"] = round(val, 4)
                fill["fcm_power_rating"] += 1
            else:
                out["fcm_power_rating"] = None

            # 2. fcm_career_roi_estimate
            #    gains / (courses * 1000 + 1)
            if gains is not None and nb_courses is not None:
                denom = nb_courses * 1000 + 1
                out["fcm_career_roi_estimate"] = round(gains / denom, 4)
                fill["fcm_career_roi_estimate"] += 1
            else:
                out["fcm_career_roi_estimate"] = None

            # 3. fcm_implied_vs_actual_wr
            #    proba_implicite - (vic / courses)  if courses > 2
            if (proba_impl is not None and nb_vic is not None
                    and nb_courses is not None and nb_courses > 2):
                actual_wr = nb_vic / nb_courses
                out["fcm_implied_vs_actual_wr"] = round(proba_impl - actual_wr, 4)
                fill["fcm_implied_vs_actual_wr"] += 1
            else:
                out["fcm_implied_vs_actual_wr"] = None

            # 4. fcm_class_x_form
            #    log(gains+1) * (vic / max(courses, 1))
            if gains is not None and nb_vic is not None and nb_courses is not None:
                courses_safe = max(nb_courses, 1)
                lg = math.log(gains + 1) if gains >= 0 else None
                if lg is not None:
                    out["fcm_class_x_form"] = round(lg * (nb_vic / courses_safe), 4)
                    fill["fcm_class_x_form"] += 1
                else:
                    out["fcm_class_x_form"] = None
            else:
                out["fcm_class_x_form"] = None

            # 5. fcm_weight_x_distance
            #    poids / distance * 1000  (weight per km)
            if poids is not None and distance is not None and distance > 0:
                out["fcm_weight_x_distance"] = round(poids / distance * 1000, 4)
                fill["fcm_weight_x_distance"] += 1
            else:
                out["fcm_weight_x_distance"] = None

            # 6. fcm_age_efficiency
            #    gains / (age * 1000 + 1)
            if gains is not None and age is not None:
                denom = age * 1000 + 1
                out["fcm_age_efficiency"] = round(gains / denom, 4)
                fill["fcm_age_efficiency"] += 1
            else:
                out["fcm_age_efficiency"] = None

            # 7. fcm_handicap_x_weight
            #    handicap * poids / 100  if both available
            if handicap is not None and poids is not None:
                out["fcm_handicap_x_weight"] = round(handicap * poids / 100, 4)
                fill["fcm_handicap_x_weight"] += 1
            else:
                out["fcm_handicap_x_weight"] = None

            # 8. fcm_field_adjusted_wr
            #    (vic / courses) * log(field_size + 1)
            if (nb_vic is not None and nb_courses is not None
                    and nb_courses > 0 and nombre_partants is not None):
                wr = nb_vic / nb_courses
                out["fcm_field_adjusted_wr"] = round(
                    wr * math.log(nombre_partants + 1), 4
                )
                fill["fcm_field_adjusted_wr"] += 1
            else:
                out["fcm_field_adjusted_wr"] = None

            # 9. fcm_versatility_score
            #    places_carriere / max(courses, 1)
            if nb_places is not None and nb_courses is not None:
                courses_safe = max(nb_courses, 1)
                out["fcm_versatility_score"] = round(nb_places / courses_safe, 4)
                fill["fcm_versatility_score"] += 1
            else:
                out["fcm_versatility_score"] = None

            # 10. fcm_strike_rate_log
            #     log(vic + 1) / log(courses + 2)
            if nb_vic is not None and nb_courses is not None:
                out["fcm_strike_rate_log"] = round(
                    math.log(nb_vic + 1) / math.log(nb_courses + 2), 4
                )
                fill["fcm_strike_rate_log"] += 1
            else:
                out["fcm_strike_rate_log"] = None

            # 11. fcm_risk_reward
            #     cote * proba_implicite  (should be ~1, deviation = edge)
            if cote is not None and proba_impl is not None:
                out["fcm_risk_reward"] = round(cote * proba_impl, 4)
                fill["fcm_risk_reward"] += 1
            else:
                out["fcm_risk_reward"] = None

            # 12. fcm_completeness_score
            #     count of non-null among 10 key fields / 10
            key_fields = [
                cote, gains, age, distance, poids,
                handicap, nb_vic, nb_courses, discipline, sexe,
            ]
            non_null = sum(1 for v in key_fields if v is not None)
            out["fcm_completeness_score"] = round(non_null / 10, 1)
            fill["fcm_completeness_score"] += 1

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

    # Rename tmp to final
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d records en %.1fs", n_written, elapsed)
    logger.info("=== Fill rates ===")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %-30s: %7d / %d (%.1f%%)", k, v, n_written, pct)


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Final composite meta-features builder"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: INPUT_PARTANTS)",
    )
    args = parser.parse_args()

    logger = setup_logging("final_composite_builder")

    input_path = Path(args.input) if args.input else None
    if input_path and not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    build(logger, input_path=input_path)


if __name__ == "__main__":
    main()
