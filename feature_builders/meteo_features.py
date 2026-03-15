"""
feature_builders.meteo_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Builds weather-derived features from the météo historique dataset.
Joins on course_uid to attach weather conditions to each partant.
"""

from __future__ import annotations

import json
import os
from typing import Any


def _load_meteo(meteo_path: str | None = None) -> dict[str, dict]:
    """Load météo data and index by course_uid."""
    if meteo_path is None:
        meteo_path = os.path.join(
            os.path.dirname(__file__), "..", "output", "13_meteo_historique", "meteo_historique.json"
        )
    if not os.path.exists(meteo_path):
        print(f"  [meteo] File not found: {meteo_path}")
        return {}

    with open(meteo_path, encoding="utf-8") as f:
        data = json.load(f)

    index: dict[str, dict] = {}
    for rec in data:
        uid = rec.get("course_uid")
        if uid:
            index[uid] = rec
    print(f"  [meteo] Loaded {len(index)} course weather records")
    return index


def build_meteo_features(
    partants: list[dict],
    meteo_path: str | None = None,
) -> list[dict]:
    """Build weather features for each partant.

    Features produced (15):
    - meteo_temperature_c: temperature at race time
    - meteo_temp_range: max - min temperature of the day
    - meteo_humidity_pct: relative humidity
    - meteo_precipitation_mm: hourly precipitation
    - meteo_precip_total_mm: daily total precipitation
    - meteo_wind_speed_kmh: wind speed
    - meteo_wind_gusts_kmh: wind gusts
    - meteo_is_rainy: boolean rain flag
    - meteo_is_windy: boolean wind flag (>30 kmh)
    - meteo_is_hot: boolean hot flag (>30°C)
    - meteo_is_cold: boolean cold flag (<5°C)
    - meteo_weather_code: WMO weather code
    - meteo_comfort_index: combined comfort metric (ideal ~15°C, low wind, no rain)
    - meteo_wind_impact: wind impact score (higher = more disruptive)
    - meteo_ground_moisture: estimated ground moisture (precip + humidity proxy)
    """
    meteo_index = _load_meteo(meteo_path)

    results = []
    matched = 0

    for p in partants:
        uid = p.get("partant_uid")
        course_uid = p.get("course_uid")
        row: dict[str, Any] = {"partant_uid": uid}

        meteo = meteo_index.get(course_uid, {}) if course_uid else {}

        if meteo:
            matched += 1
            temp = meteo.get("temperature_c")
            temp_min = meteo.get("temp_min_c")
            temp_max = meteo.get("temp_max_c")
            humidity = meteo.get("humidity_pct")
            precip = meteo.get("precipitation_mm")
            precip_total = meteo.get("precip_total_mm")
            wind = meteo.get("wind_speed_kmh")
            gusts = meteo.get("wind_gusts_kmh")

            row["meteo_temperature_c"] = temp
            row["meteo_temp_range"] = (temp_max - temp_min) if temp_max is not None and temp_min is not None else None
            row["meteo_humidity_pct"] = humidity
            row["meteo_precipitation_mm"] = precip
            row["meteo_precip_total_mm"] = precip_total
            row["meteo_wind_speed_kmh"] = wind
            row["meteo_wind_gusts_kmh"] = gusts
            row["meteo_is_rainy"] = meteo.get("is_rainy")
            row["meteo_is_windy"] = meteo.get("is_windy")
            row["meteo_is_hot"] = meteo.get("is_hot")
            row["meteo_is_cold"] = meteo.get("is_cold")
            row["meteo_weather_code"] = meteo.get("weather_code")

            # Comfort index: ideal temp ~15°C, low wind, no rain
            if temp is not None and wind is not None:
                temp_penalty = abs(temp - 15) / 10  # 0 at 15°C, 1 at 25°C or 5°C
                wind_penalty = (wind or 0) / 40  # 0 at calm, 1 at 40 km/h
                rain_penalty = 1.0 if meteo.get("is_rainy") else 0.0
                row["meteo_comfort_index"] = round(max(0, 1.0 - temp_penalty - wind_penalty * 0.3 - rain_penalty * 0.3), 3)
            else:
                row["meteo_comfort_index"] = None

            # Wind impact (higher = more disruptive for race)
            if wind is not None:
                row["meteo_wind_impact"] = round(((wind or 0) + (gusts or 0) * 0.5) / 30, 3)
            else:
                row["meteo_wind_impact"] = None

            # Ground moisture proxy
            if precip_total is not None and humidity is not None:
                row["meteo_ground_moisture"] = round(precip_total + humidity / 100 * 2, 3)
            else:
                row["meteo_ground_moisture"] = None
        else:
            for k in ("meteo_temperature_c", "meteo_temp_range", "meteo_humidity_pct",
                       "meteo_precipitation_mm", "meteo_precip_total_mm",
                       "meteo_wind_speed_kmh", "meteo_wind_gusts_kmh",
                       "meteo_is_rainy", "meteo_is_windy", "meteo_is_hot", "meteo_is_cold",
                       "meteo_weather_code", "meteo_comfort_index", "meteo_wind_impact",
                       "meteo_ground_moisture"):
                row[k] = None

        results.append(row)

    print(f"  [meteo] Matched {matched}/{len(partants)} partants with weather data")
    return results
