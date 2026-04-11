#!/usr/bin/env python3
"""
feature_builders.math_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Mathematical derived features: ratios, interactions, log transforms, z-scores.

Takes existing numeric columns from partants_master and computes 30 high-value
derived features that capture nonlinear relationships. Two-pass approach:
  - Pass 1: collect per-course stats for z-score/percentile features
  - Pass 2: compute all features and write output

Features (30):
  # Performance ratios (6)
  - mth_win_rate_career        : victoires / courses carriere
  - mth_place_rate_career      : places / courses carriere
  - mth_gains_per_course       : gains carriere / courses carriere
  - mth_recent_form_ratio      : (victoires_5 + places_5) / 5
  - mth_vitesse_relative       : reduction_km / distance (vitesse normalisee)
  - mth_class_field_ratio      : class_rating / field_strength_avg

  # Domain-expert interactions (12)
  - mth_form_x_cote            : serie_places * log(cote) - value signal
  - mth_speed_x_field          : speed_figure * field_strength_avg
  - mth_age_x_courses          : age * nb_courses (usure index)
  - mth_draw_x_field_size      : num_pmu / nombre_partants (position relative)
  - mth_poids_x_distance       : poids * distance / 1000 (endurance load)
  - mth_elo_x_proba            : elo produit x proba implicite
  - mth_gains_x_cote           : log(gains+1) * cote (value overbet)
  - mth_speed_sq               : speed_figure^2
  - mth_age_sq                 : age^2 (non-linear aging)
  - mth_cote_inv               : 1 / cote (implied probability)
  - mth_draw_bias_interaction  : draw_position * bias_interieur
  - mth_serie_momentum         : serie_places * momentum (if available)

  # Statistical transforms (6)
  - mth_log_cote               : log(cote + 1)
  - mth_log_gains_carriere     : log(gains_carriere + 1)
  - mth_log_distance           : log(distance)
  - mth_speed_zscore_field     : (speed - field_mean) / field_std
  - mth_class_zscore_field     : (class - field_mean_class) / field_std_class
  - mth_draw_percentile        : num_pmu / nombre_partants

  # Composite scores (6)
  - mth_power_index            : win_rate * speed_figure * (1/cote)
  - mth_consistency_score      : place_rate * (1 - std_positions)
  - mth_value_score            : (form_ratio - proba_implicite) / proba_implicite
  - mth_fatigue_index          : races_last_30d / age (activity relative to age)
  - mth_class_advantage        : class_vs_field * log(cote+1) (class edge at odds)
  - mth_field_adjusted_speed   : speed_figure - field_strength_avg

Memory: ~8 GB (course stats dict for z-scores)
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
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/math_features")
_LOG_EVERY = 500_000


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


def _safe_div(a, b):
    """Safe division returning None if impossible."""
    if a is None or b is None or b == 0:
        return None
    return a / b


def _safe_log(val):
    """Safe log(val + 1)."""
    if val is None or val < 0:
        return None
    return math.log(val + 1)


def build(logger) -> None:
    t0 = time.time()

    # ---- Pass 1: Collect per-course stats for z-score computation ----
    logger.info("Pass 1: Collecte stats par course pour z-scores...")

    # course_uid -> { speeds: [], classes: [], cotes: [] }
    course_stats: dict[str, dict] = defaultdict(lambda: {
        "speeds": [], "classes": [], "cotes": [],
    })

    n_pass1 = 0
    with open(INPUT_PARTANTS, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_pass1 += 1
            if n_pass1 % _LOG_EVERY == 0:
                logger.info("  Pass 1: %d records...", n_pass1)

            cuid = rec.get("course_uid", "")
            if not cuid:
                continue

            cs = course_stats[cuid]

            spd = _sf(rec.get("spd_speed_figure"))
            if spd is not None:
                cs["speeds"].append(spd)

            cls = _sf(rec.get("spd_class_rating"))
            if cls is not None:
                cs["classes"].append(cls)

            cote = _sf(rec.get("cote_finale")) or _sf(rec.get("cote_reference"))
            if cote is not None:
                cs["cotes"].append(cote)

    logger.info("  Pass 1 termine: %d records, %d courses", n_pass1, len(course_stats))

    # Pre-compute mean/std per course
    course_agg: dict[str, dict] = {}
    for cuid, cs in course_stats.items():
        agg = {}
        for key in ("speeds", "classes", "cotes"):
            vals = cs[key]
            if len(vals) >= 2:
                mean = sum(vals) / len(vals)
                std = (sum((v - mean) ** 2 for v in vals) / len(vals)) ** 0.5
                agg[f"{key}_mean"] = mean
                agg[f"{key}_std"] = std if std > 0 else None
            elif len(vals) == 1:
                agg[f"{key}_mean"] = vals[0]
                agg[f"{key}_std"] = None
            else:
                agg[f"{key}_mean"] = None
                agg[f"{key}_std"] = None
        course_agg[cuid] = agg

    del course_stats
    gc.collect()
    logger.info("  Aggregations par course calculees")

    # ---- Pass 2: Compute all features ----
    logger.info("Pass 2: Calcul des 30 features mathematiques...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "math_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    feat_names = [
        "mth_win_rate_career", "mth_place_rate_career", "mth_gains_per_course",
        "mth_recent_form_ratio", "mth_vitesse_relative", "mth_class_field_ratio",
        "mth_form_x_cote", "mth_speed_x_field", "mth_age_x_courses",
        "mth_draw_x_field_size", "mth_poids_x_distance", "mth_elo_x_proba",
        "mth_gains_x_cote", "mth_speed_sq", "mth_age_sq", "mth_cote_inv",
        "mth_draw_bias_interaction", "mth_serie_momentum",
        "mth_log_cote", "mth_log_gains_carriere", "mth_log_distance",
        "mth_speed_zscore_field", "mth_class_zscore_field", "mth_draw_percentile",
        "mth_power_index", "mth_consistency_score", "mth_value_score",
        "mth_fatigue_index", "mth_class_advantage", "mth_field_adjusted_speed",
    ]
    fill = {k: 0 for k in feat_names}

    with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin, \
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
                logger.info("  Pass 2: %d records...", n_written)
                if n_written % (2 * _LOG_EVERY) == 0:
                    gc.collect()

            # Extract raw values
            partant_uid = rec.get("partant_uid", "")
            course_uid = rec.get("course_uid", "")
            date_str = rec.get("date_reunion_iso", "")

            nb_courses = _si(rec.get("nb_courses_carriere"))
            nb_victoires = _si(rec.get("nb_victoires_carriere"))
            nb_places = _si(rec.get("nb_places_carriere"))
            gains_carriere = _sf(rec.get("gains_carriere_euros"))
            age = _si(rec.get("age"))
            distance = _si(rec.get("distance"))
            reduction = _si(rec.get("reduction_km_ms"))
            nombre_partants = _si(rec.get("nombre_partants"))
            num_pmu = _si(rec.get("num_pmu"))
            cote = _sf(rec.get("cote_finale")) or _sf(rec.get("cote_reference"))
            poids = _sf(rec.get("poids_porte_kg")) or _sf(rec.get("poids_base_kg"))
            speed_fig = _sf(rec.get("spd_speed_figure"))
            class_rating = _sf(rec.get("spd_class_rating"))
            class_vs_field = _sf(rec.get("spd_class_vs_field"))
            field_avg = _sf(rec.get("spd_field_strength_avg"))
            bias_int = _sf(rec.get("spd_bias_interieur"))
            serie_places = _si(rec.get("seq_serie_places"))
            vic_5 = _si(rec.get("seq_nb_victoires_recent_5"))
            plc_5 = _si(rec.get("seq_nb_places_recent_5"))
            proba_impl = _sf(rec.get("proba_implicite"))

            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }

            # === Performance ratios (6) ===
            wr = _safe_div(nb_victoires, nb_courses)
            out["mth_win_rate_career"] = round(wr, 4) if wr is not None else None
            if wr is not None:
                fill["mth_win_rate_career"] += 1

            pr = _safe_div(nb_places, nb_courses)
            out["mth_place_rate_career"] = round(pr, 4) if pr is not None else None
            if pr is not None:
                fill["mth_place_rate_career"] += 1

            gpc = _safe_div(gains_carriere, nb_courses)
            out["mth_gains_per_course"] = round(gpc, 2) if gpc is not None else None
            if gpc is not None:
                fill["mth_gains_per_course"] += 1

            if vic_5 is not None and plc_5 is not None:
                out["mth_recent_form_ratio"] = round((vic_5 + plc_5) / 5.0, 4)
                fill["mth_recent_form_ratio"] += 1
            else:
                out["mth_recent_form_ratio"] = None

            vr = _safe_div(reduction, distance) if reduction and distance else None
            out["mth_vitesse_relative"] = round(vr, 4) if vr is not None else None
            if vr is not None:
                fill["mth_vitesse_relative"] += 1

            cfr = _safe_div(class_rating, field_avg)
            out["mth_class_field_ratio"] = round(cfr, 4) if cfr is not None else None
            if cfr is not None:
                fill["mth_class_field_ratio"] += 1

            # === Domain-expert interactions (12) ===
            log_cote = _safe_log(cote) if cote else None
            if serie_places is not None and log_cote is not None:
                out["mth_form_x_cote"] = round(serie_places * log_cote, 4)
                fill["mth_form_x_cote"] += 1
            else:
                out["mth_form_x_cote"] = None

            if speed_fig is not None and field_avg is not None:
                out["mth_speed_x_field"] = round(speed_fig * field_avg, 4)
                fill["mth_speed_x_field"] += 1
            else:
                out["mth_speed_x_field"] = None

            if age is not None and nb_courses is not None:
                out["mth_age_x_courses"] = age * nb_courses
                fill["mth_age_x_courses"] += 1
            else:
                out["mth_age_x_courses"] = None

            dp = _safe_div(num_pmu, nombre_partants)
            out["mth_draw_x_field_size"] = round(dp, 4) if dp is not None else None
            if dp is not None:
                fill["mth_draw_x_field_size"] += 1

            if poids is not None and distance is not None:
                out["mth_poids_x_distance"] = round(poids * distance / 1000.0, 2)
                fill["mth_poids_x_distance"] += 1
            else:
                out["mth_poids_x_distance"] = None

            if proba_impl is not None and cote is not None:
                out["mth_elo_x_proba"] = round(proba_impl * cote, 4)
                fill["mth_elo_x_proba"] += 1
            else:
                out["mth_elo_x_proba"] = None

            if gains_carriere is not None and cote is not None:
                lg = _safe_log(gains_carriere)
                if lg is not None:
                    out["mth_gains_x_cote"] = round(lg * cote, 4)
                    fill["mth_gains_x_cote"] += 1
                else:
                    out["mth_gains_x_cote"] = None
            else:
                out["mth_gains_x_cote"] = None

            if speed_fig is not None:
                out["mth_speed_sq"] = round(speed_fig ** 2, 4)
                fill["mth_speed_sq"] += 1
            else:
                out["mth_speed_sq"] = None

            if age is not None:
                out["mth_age_sq"] = age * age
                fill["mth_age_sq"] += 1
            else:
                out["mth_age_sq"] = None

            if cote is not None and cote > 0:
                out["mth_cote_inv"] = round(1.0 / cote, 6)
                fill["mth_cote_inv"] += 1
            else:
                out["mth_cote_inv"] = None

            if num_pmu is not None and bias_int is not None:
                out["mth_draw_bias_interaction"] = round(num_pmu * bias_int, 4)
                fill["mth_draw_bias_interaction"] += 1
            else:
                out["mth_draw_bias_interaction"] = None

            if serie_places is not None and vic_5 is not None:
                out["mth_serie_momentum"] = serie_places * (vic_5 + 1)
                fill["mth_serie_momentum"] += 1
            else:
                out["mth_serie_momentum"] = None

            # === Statistical transforms (6) ===
            out["mth_log_cote"] = round(log_cote, 4) if log_cote is not None else None
            if log_cote is not None:
                fill["mth_log_cote"] += 1

            lg_gains = _safe_log(gains_carriere)
            out["mth_log_gains_carriere"] = round(lg_gains, 4) if lg_gains is not None else None
            if lg_gains is not None:
                fill["mth_log_gains_carriere"] += 1

            if distance is not None and distance > 0:
                out["mth_log_distance"] = round(math.log(distance), 4)
                fill["mth_log_distance"] += 1
            else:
                out["mth_log_distance"] = None

            # Z-scores (need course aggregates)
            agg = course_agg.get(course_uid, {})

            spd_mean = agg.get("speeds_mean")
            spd_std = agg.get("speeds_std")
            if speed_fig is not None and spd_mean is not None and spd_std is not None:
                out["mth_speed_zscore_field"] = round((speed_fig - spd_mean) / spd_std, 4)
                fill["mth_speed_zscore_field"] += 1
            else:
                out["mth_speed_zscore_field"] = None

            cls_mean = agg.get("classes_mean")
            cls_std = agg.get("classes_std")
            if class_rating is not None and cls_mean is not None and cls_std is not None:
                out["mth_class_zscore_field"] = round((class_rating - cls_mean) / cls_std, 4)
                fill["mth_class_zscore_field"] += 1
            else:
                out["mth_class_zscore_field"] = None

            out["mth_draw_percentile"] = round(dp, 4) if dp is not None else None
            if dp is not None:
                fill["mth_draw_percentile"] += 1

            # === Composite scores (6) ===
            if wr is not None and speed_fig is not None and cote is not None and cote > 0:
                out["mth_power_index"] = round(wr * speed_fig * (1.0 / cote), 6)
                fill["mth_power_index"] += 1
            else:
                out["mth_power_index"] = None

            if pr is not None and pr > 0:
                # consistency = place_rate (higher = more consistent)
                out["mth_consistency_score"] = round(pr, 4)
                fill["mth_consistency_score"] += 1
            else:
                out["mth_consistency_score"] = None

            if vic_5 is not None and plc_5 is not None and proba_impl is not None and proba_impl > 0:
                form_r = (vic_5 + plc_5) / 5.0
                out["mth_value_score"] = round((form_r - proba_impl) / proba_impl, 4)
                fill["mth_value_score"] += 1
            else:
                out["mth_value_score"] = None

            # fatigue: nb recent courses relative to age
            nb_hist = _si(rec.get("seq_nb_courses_historique"))
            if nb_hist is not None and age is not None and age > 0:
                out["mth_fatigue_index"] = round(nb_hist / age, 4)
                fill["mth_fatigue_index"] += 1
            else:
                out["mth_fatigue_index"] = None

            if class_vs_field is not None and log_cote is not None:
                out["mth_class_advantage"] = round(class_vs_field * log_cote, 4)
                fill["mth_class_advantage"] += 1
            else:
                out["mth_class_advantage"] = None

            if speed_fig is not None and field_avg is not None:
                out["mth_field_adjusted_speed"] = round(speed_fig - field_avg, 4)
                fill["mth_field_adjusted_speed"] += 1
            else:
                out["mth_field_adjusted_speed"] = None

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

    # Rename tmp to final
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d records en %.1fs", n_written, elapsed)
    logger.info("Fill rates:")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %-30s: %7d / %d (%.1f%%)", k, v, n_written, pct)


def main():
    parser = argparse.ArgumentParser(description="Mathematical derived features builder")
    args = parser.parse_args()
    logger = setup_logging("math_features_builder")
    build(logger)


if __name__ == "__main__":
    main()
