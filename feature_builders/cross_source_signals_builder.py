#!/usr/bin/env python3
"""
feature_builders.cross_source_signals_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-source signal features: combine information already present in
partants_master from different sources (rapports, combinaisons, sequences,
speed, pedigree) into signal features that detect consensus or divergence.

Single-pass streaming -- no external data needed.

Features (10):
  - xsrc_nb_sources             : nombre de sources de donnees pour ce partant
  - xsrc_market_vs_speed        : divergence cote_finale vs spd_speed_figure
  - xsrc_market_vs_form         : divergence cote vs forme recente
  - xsrc_speed_vs_class         : ratio speed_figure / class_rating
  - xsrc_pedigree_vs_performance: compare ped_stamina_index with distance prefs
  - xsrc_odds_consensus         : 1 if cote_finale and proba_implicite agree
  - xsrc_form_momentum_signal   : seq_serie_places * seq_nb_victoires_recent_5
  - xsrc_field_value_signal     : spd_class_vs_field * (1/cote)
  - xsrc_data_richness          : proportion of non-null key predictive fields
  - xsrc_signal_agreement       : count of positive signals (form/speed/class/odds)

Usage:
    python feature_builders/cross_source_signals_builder.py
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

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/cross_source_signals")
_LOG_EVERY = 500_000

# Prefixes used to count data sources
_SOURCE_PREFIXES = (
    "rap_",      # rapports
    "cmb_",      # combinaisons
    "seq_",      # sequences
    "spd_",      # speed figures
    "ped_",      # pedigree
    "meteo_",    # meteo
    "elo_",      # elo ratings
    "odds_",     # odds-derived
)

# Key predictive fields to measure data richness (~20 fields)
_KEY_FIELDS = [
    "cote_finale", "cote_reference", "proba_implicite",
    "spd_speed_figure", "spd_class_rating", "spd_class_vs_field",
    "spd_field_strength_avg",
    "seq_serie_places", "seq_nb_victoires_recent_5", "seq_nb_places_recent_5",
    "seq_nb_courses_historique",
    "ped_stamina_index",
    "elo_rating",
    "nb_courses_carriere", "nb_victoires_carriere", "nb_places_carriere",
    "gains_carriere_euros",
    "age", "distance", "poids_porte_kg",
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


def _safe_div(a, b):
    """Safe division returning None if impossible."""
    if a is None or b is None or b == 0:
        return None
    return a / b


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _count_sources(rec: dict) -> int:
    """Count how many distinct source prefixes have at least one non-null field."""
    found = 0
    for prefix in _SOURCE_PREFIXES:
        for key, val in rec.items():
            if key.startswith(prefix) and val is not None:
                found += 1
                break
    return found


def _data_richness(rec: dict) -> float:
    """Proportion of non-null values among key predictive fields."""
    filled = sum(1 for k in _KEY_FIELDS if rec.get(k) is not None)
    return round(filled / len(_KEY_FIELDS), 4)


def _market_vs_speed(cote: Optional[float], speed: Optional[float]) -> Optional[float]:
    """Z-scored ratio: higher cote (longshot) + high speed = divergence signal.

    Returns log(cote) / log(speed+1) as a rough divergence measure.
    Positive when odds suggest longshot but speed says otherwise.
    """
    if cote is None or speed is None or cote <= 0 or speed <= 0:
        return None
    log_cote = math.log(cote + 1)
    log_speed = math.log(speed + 1)
    if log_speed == 0:
        return None
    return round(log_cote / log_speed, 4)


def _market_vs_form(cote: Optional[float], serie_places: Optional[int]) -> Optional[float]:
    """Divergence between odds and recent form.

    High cote (longshot) + high serie_places (good form) = positive divergence.
    """
    if cote is None or serie_places is None or cote <= 0:
        return None
    implied_prob = 1.0 / cote
    # serie_places normalized roughly to [0,1] range (assuming max ~10)
    form_score = min(serie_places / 10.0, 1.0)
    # Divergence: form says good, market says bad
    return round(form_score - implied_prob, 4)


def _speed_vs_class(speed: Optional[float], class_rating: Optional[float]) -> Optional[float]:
    """Ratio of speed_figure / class_rating -- consistency check."""
    return round(speed / class_rating, 4) if speed is not None and class_rating is not None and class_rating > 0 else None


def _pedigree_vs_performance(
    stamina_index: Optional[float], distance: Optional[int], speed: Optional[float]
) -> Optional[float]:
    """Compare pedigree stamina with actual distance/speed.

    If stamina_index is high but horse runs short distances with good speed,
    there is a divergence (negative). If stamina matches distance, positive.
    """
    if stamina_index is None or distance is None:
        return None
    # Normalize distance to a [0,1] scale (1000m=0, 4000m=1)
    dist_norm = max(0.0, min(1.0, (distance - 1000) / 3000.0))
    # Stamina should correlate with distance preference
    return round(stamina_index - dist_norm, 4)


def _odds_consensus(cote: Optional[float], proba_impl: Optional[float]) -> Optional[int]:
    """1 if cote_finale and proba_implicite agree within 20%, else 0.

    proba_implicite should be ~1/cote. If they diverge > 20%, return 0.
    """
    if cote is None or proba_impl is None or cote <= 0:
        return None
    market_prob = 1.0 / cote
    if market_prob == 0:
        return None
    ratio = abs(proba_impl - market_prob) / market_prob
    return 1 if ratio <= 0.20 else 0


def _form_momentum(serie_places: Optional[int], vic_5: Optional[int]) -> Optional[int]:
    """Combined form signal: serie_places * nb_victoires_recent_5."""
    if serie_places is None or vic_5 is None:
        return None
    return serie_places * vic_5


def _field_value_signal(class_vs_field: Optional[float], cote: Optional[float]) -> Optional[float]:
    """Class edge at good odds: class_vs_field * (1/cote)."""
    if class_vs_field is None or cote is None or cote <= 0:
        return None
    return round(class_vs_field * (1.0 / cote), 6)


def _signal_agreement(
    serie_places: Optional[int],
    vic_5: Optional[int],
    speed: Optional[float],
    field_avg: Optional[float],
    class_vs_field: Optional[float],
    cote: Optional[float],
    proba_impl: Optional[float],
) -> Optional[int]:
    """Count how many signals are positive (form, speed, class, odds all pointing same way).

    Signals:
      - form positive: serie_places >= 2 or vic_5 >= 1
      - speed positive: speed > field_avg
      - class positive: class_vs_field > 0
      - odds positive: implied prob > 0.10 (roughly top-5 favourite)
    """
    count = 0
    checked = 0

    # Form signal
    if serie_places is not None or vic_5 is not None:
        checked += 1
        sp = serie_places or 0
        v5 = vic_5 or 0
        if sp >= 2 or v5 >= 1:
            count += 1

    # Speed signal
    if speed is not None and field_avg is not None:
        checked += 1
        if speed > field_avg:
            count += 1

    # Class signal
    if class_vs_field is not None:
        checked += 1
        if class_vs_field > 0:
            count += 1

    # Odds signal
    if cote is not None and cote > 0:
        checked += 1
        if (1.0 / cote) > 0.10:
            count += 1

    if checked == 0:
        return None
    return count


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build(logger) -> None:
    t0 = time.time()
    logger.info("=== Cross-Source Signals Builder ===")
    logger.info("Input: %s", INPUT_PARTANTS)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "cross_source_signals_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    feat_names = [
        "xsrc_nb_sources",
        "xsrc_market_vs_speed",
        "xsrc_market_vs_form",
        "xsrc_speed_vs_class",
        "xsrc_pedigree_vs_performance",
        "xsrc_odds_consensus",
        "xsrc_form_momentum_signal",
        "xsrc_field_value_signal",
        "xsrc_data_richness",
        "xsrc_signal_agreement",
    ]
    fill = {k: 0 for k in feat_names}
    n_written = 0

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
                logger.info("  %d records traites...", n_written)
                gc.collect()

            # --- Extract raw values ---
            partant_uid = rec.get("partant_uid", "")
            course_uid = rec.get("course_uid", "")
            date_str = rec.get("date_reunion_iso", "")

            cote = _sf(rec.get("cote_finale")) or _sf(rec.get("cote_reference"))
            proba_impl = _sf(rec.get("proba_implicite"))
            speed_fig = _sf(rec.get("spd_speed_figure"))
            class_rating = _sf(rec.get("spd_class_rating"))
            class_vs_field = _sf(rec.get("spd_class_vs_field"))
            field_avg = _sf(rec.get("spd_field_strength_avg"))
            serie_places = _si(rec.get("seq_serie_places"))
            vic_5 = _si(rec.get("seq_nb_victoires_recent_5"))
            stamina_index = _sf(rec.get("ped_stamina_index"))
            distance = _si(rec.get("distance"))

            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }

            # --- Feature 1: nb_sources ---
            val = _count_sources(rec)
            out["xsrc_nb_sources"] = val
            if val > 0:
                fill["xsrc_nb_sources"] += 1

            # --- Feature 2: market_vs_speed ---
            val = _market_vs_speed(cote, speed_fig)
            out["xsrc_market_vs_speed"] = val
            if val is not None:
                fill["xsrc_market_vs_speed"] += 1

            # --- Feature 3: market_vs_form ---
            val = _market_vs_form(cote, serie_places)
            out["xsrc_market_vs_form"] = val
            if val is not None:
                fill["xsrc_market_vs_form"] += 1

            # --- Feature 4: speed_vs_class ---
            val = _speed_vs_class(speed_fig, class_rating)
            out["xsrc_speed_vs_class"] = val
            if val is not None:
                fill["xsrc_speed_vs_class"] += 1

            # --- Feature 5: pedigree_vs_performance ---
            val = _pedigree_vs_performance(stamina_index, distance, speed_fig)
            out["xsrc_pedigree_vs_performance"] = val
            if val is not None:
                fill["xsrc_pedigree_vs_performance"] += 1

            # --- Feature 6: odds_consensus ---
            val = _odds_consensus(cote, proba_impl)
            out["xsrc_odds_consensus"] = val
            if val is not None:
                fill["xsrc_odds_consensus"] += 1

            # --- Feature 7: form_momentum_signal ---
            val = _form_momentum(serie_places, vic_5)
            out["xsrc_form_momentum_signal"] = val
            if val is not None:
                fill["xsrc_form_momentum_signal"] += 1

            # --- Feature 8: field_value_signal ---
            val = _field_value_signal(class_vs_field, cote)
            out["xsrc_field_value_signal"] = val
            if val is not None:
                fill["xsrc_field_value_signal"] += 1

            # --- Feature 9: data_richness ---
            val = _data_richness(rec)
            out["xsrc_data_richness"] = val
            if val > 0:
                fill["xsrc_data_richness"] += 1

            # --- Feature 10: signal_agreement ---
            val = _signal_agreement(
                serie_places, vic_5, speed_fig, field_avg, class_vs_field, cote, proba_impl
            )
            out["xsrc_signal_agreement"] = val
            if val is not None:
                fill["xsrc_signal_agreement"] += 1

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
        logger.info("  %-35s: %7d / %d (%.1f%%)", k, v, n_written, pct)


def main():
    parser = argparse.ArgumentParser(description="Cross-source signals feature builder")
    parser.parse_args()
    logger = setup_logging("cross_source_signals_builder")
    build(logger)


if __name__ == "__main__":
    main()
