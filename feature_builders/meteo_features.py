#!/usr/bin/env python3
"""
feature_builders.meteo_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
15 features from weather data (temperature, wind, rain, humidity).

Joins meteo data on course_uid to attach weather conditions to each partant.

Usage:
    python feature_builders/meteo_features.py
    python feature_builders/meteo_features.py --input output/02_liste_courses/partants_normalises.jsonl
    python feature_builders/meteo_features.py --meteo output/13_meteo_historique/meteo_historique.json
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
METEO_DEFAULT = os.path.join("output", "13_meteo_historique", "meteo_historique.json")
OUTPUT_DIR_DEFAULT = os.path.join("output", "meteo_features")

# ===========================================================================
# LOAD
# ===========================================================================

def _load_meteo_index(meteo_path: str, logger: logging.Logger) -> dict:
    """Load meteo data and index by course_uid."""
    data = load_json_or_jsonl(meteo_path, logger)
    index = {}
    for rec in data:
        uid = rec.get("course_uid")
        if uid:
            index[uid] = rec
    logger.info("Index meteo: %d courses", len(index))
    return index

# ===========================================================================
# BUILDER
# ===========================================================================

def build_meteo_features(partants: list, meteo_index: dict = None, logger: logging.Logger = None) -> list:
    """Build 15 weather impact features."""
    if logger is None:
        logger = logging.getLogger(__name__)
    if meteo_index is None:
        meteo_index = {}

    enriched = 0
    results = []

    for idx, p in enumerate(partants):
        course_uid = p.get("course_uid")
        meteo = meteo_index.get(course_uid, {}) if course_uid else {}

        feat = {}

        if meteo:
            enriched += 1
            temp = meteo.get("temperature_c")
            temp_min = meteo.get("temp_min_c")
            temp_max = meteo.get("temp_max_c")
            humidity = meteo.get("humidity_pct")
            precip = meteo.get("precipitation_mm")
            precip_total = meteo.get("precip_total_mm")
            wind = meteo.get("wind_speed_kmh")
            gusts = meteo.get("wind_gusts_kmh")

            feat["meteo_temperature_c"] = temp
            feat["meteo_temp_range"] = (temp_max - temp_min) if temp_max is not None and temp_min is not None else None
            feat["meteo_humidity_pct"] = humidity
            feat["meteo_precipitation_mm"] = precip
            feat["meteo_precip_total_mm"] = precip_total
            feat["meteo_wind_speed_kmh"] = wind
            feat["meteo_wind_gusts_kmh"] = gusts
            feat["meteo_is_rainy"] = meteo.get("is_rainy")
            feat["meteo_is_windy"] = meteo.get("is_windy")
            feat["meteo_is_hot"] = meteo.get("is_hot")
            feat["meteo_is_cold"] = meteo.get("is_cold")
            feat["meteo_weather_code"] = meteo.get("weather_code")

            # Comfort index: ideal temp ~15C, low wind, no rain
            if temp is not None and wind is not None:
                temp_penalty = abs(temp - 15) / 10
                wind_penalty = (wind or 0) / 40
                rain_penalty = 1.0 if meteo.get("is_rainy") else 0.0
                feat["meteo_comfort_index"] = round(
                    max(0, 1.0 - temp_penalty - wind_penalty * 0.3 - rain_penalty * 0.3), 3
                )
            else:
                feat["meteo_comfort_index"] = None

            # Wind impact (higher = more disruptive)
            if wind is not None:
                feat["meteo_wind_impact"] = round(((wind or 0) + (gusts or 0) * 0.5) / 30, 3)
            else:
                feat["meteo_wind_impact"] = None

            # Ground moisture proxy
            if precip_total is not None and humidity is not None:
                feat["meteo_ground_moisture"] = round(precip_total + humidity / 100 * 2, 3)
            else:
                feat["meteo_ground_moisture"] = None
        else:
            for k in ("meteo_temperature_c", "meteo_temp_range", "meteo_humidity_pct",
                       "meteo_precipitation_mm", "meteo_precip_total_mm",
                       "meteo_wind_speed_kmh", "meteo_wind_gusts_kmh",
                       "meteo_is_rainy", "meteo_is_windy", "meteo_is_hot", "meteo_is_cold",
                       "meteo_weather_code", "meteo_comfort_index", "meteo_wind_impact",
                       "meteo_ground_moisture"):
                feat[k] = None

        p.update(feat)
        results.append(p)

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(partants), enriched)

    logger.info("Features meteo: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================



# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="15 weather impact features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--meteo", default=METEO_DEFAULT, help="Meteo data JSON/JSONL")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("meteo_features")
    logger.info("=" * 70)
    logger.info("meteo_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    meteo_index = _load_meteo_index(args.meteo, logger)
    results = build_meteo_features(partants, meteo_index, logger)

    out_path = os.path.join(args.output_dir, "meteo_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
