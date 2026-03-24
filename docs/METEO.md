# Weather (Meteo) Data Documentation

## Overview

Weather data is collected from multiple sources, merged into `data_master/meteo_master.json`, and joined to race entries by hippodrome + date. Weather conditions are critical for predicting going/track state, which strongly influences race outcomes.

## Data Sources

| Source | Script | API | Cost | Coverage |
|--------|--------|-----|------|----------|
| Open-Meteo | `13_meteo_historique`, `00_enrichissement_meteo`, `fetch_openmeteo_missing` | open-meteo.com Historical Weather API | Free, no API key | Since 1940, global |
| NASA POWER | `enrichissement_meteo_nasa` | power.larc.nasa.gov | Free, no limit | Satellite/model data since 1981 |
| Visual Crossing | `112_visual_crossing_scraper` | weather.visualcrossing.com Timeline API | Free tier: 1000 req/day | Historical, global |
| NOAA CDO | `96_noaa_weather_scraper` | ncdc.noaa.gov CDO API v2 | Free with API key | US/global station data |
| Meteostat | `97_meteostat_scraper` | meteostat.p.rapidapi.com | Free via RapidAPI (rate-limited) | Global station data |
| Meteo France | `35_meteo_france_api` | Meteo France stations | Paid | French stations only |

### Additional Context Sources

| Source | Script | Description |
|--------|--------|-------------|
| Reunions enrichies | `39_reunions_enrichies` | Weather/incidents/betting per meeting (2 GB) |
| Course listings | `02_liste_courses` | Penetrometer readings, track type per race |

## Merge Pipeline

1. **Individual scrapers** collect raw weather data into `output/<script_name>/`.
2. **`merge_meteo.py`** performs an intermediate merge.
3. **`merge_meteo_master.py`** produces the final `data_master/meteo_master.json` (+ `.parquet`, `.csv`), keyed by `course_uid` or `date|hippodrome|R<num>|C<num>`.
4. **`postprocess_meteo.py`** adds derived fields: `terrain_category`, `penetrometre_numeric`, `meteo_score`.

Source priority for conflicting values: non-null values from any source overwrite nulls; later sources in merge order can fill gaps but do not overwrite existing values.

## Fields Available

### Temperature

| Field | Type | Unit | Description |
|-------|------|------|-------------|
| `temperature` | float | C | Air temperature at race time |
| `reu_temperature` | float | C | Temperature from reunion record |
| `meteo_temperature` | float | C | Temperature from meteo source |
| `delta_temperature` | float | C | Deviation from seasonal average |

### Precipitation

| Field | Type | Unit | Description |
|-------|------|------|-------------|
| `precipitation` / `meteo_exacte_pluie_mm` | float | mm | Rainfall amount |
| `pluie_48h_avant` | float | mm | Cumulative rain 48h before race |

### Wind

| Field | Type | Unit | Description |
|-------|------|------|-------------|
| `vent_kmh` / `meteo_exacte_vent_kmh` | float | km/h | Wind speed |
| `meteo_exacte_vent_direction` | string | degrees/cardinal | Wind direction |

### Humidity & Pressure

| Field | Type | Unit | Description |
|-------|------|------|-------------|
| `humidite` / `meteo_humidite` | float | % | Relative humidity |
| `pression` | float | hPa | Atmospheric pressure |

### Track State

| Field | Type | Description |
|-------|------|-------------|
| `penetrometre` | float | Penetrometer reading (official ground measurement) |
| `penetrometre_numeric` | float | Normalized penetrometer (from postprocess) |
| `etat_terrain` | string | Official going description |
| `terrain_category` | string | Normalized going (sec/bon/souple/lourd) |
| `terrain_predit` | string | Predicted going from weather model |
| `type_piste` | string | Track surface type |

### Composite Scores

| Field | Type | Description |
|-------|------|-------------|
| `meteo_score` | float | Composite weather impact score (from postprocess) |
| `impact_meteo_score` | float | Weather impact score (from croisement) |

## Integration with Race Data

Weather data is joined to race entries by **`43_croisement_meteo_courses.py`**, which:

1. Builds an index of weather records keyed by `(date, hippodrome)`.
2. Matches each race to its weather record.
3. Computes derived features per runner.

### Cross-Reference Features (from script 43)

- `meteo_exacte_temperature` -- exact temperature at race hour
- `meteo_exacte_pluie_mm` -- exact precipitation
- `meteo_exacte_vent_kmh` -- exact wind speed
- `meteo_exacte_vent_direction` -- wind direction
- `meteo_humidite` -- humidity
- `pluie_48h_avant` -- cumulative rain 48h before
- `delta_temperature` -- temperature vs seasonal average
- `terrain_predit` -- predicted going from weather
- `impact_meteo_score` -- composite impact score
- `cheval_perf_terrain` -- horse's win rate on this going
- `cheval_perf_pluie` -- horse's win rate in rain
- `cheval_specialist_lourd` -- heavy-ground specialist flag
- `cheval_specialist_sec` -- firm-ground specialist flag

## Weather x Terrain Interaction Features

Computed by **`feat_meteo_terrain_interaction.py`** (8 features):

| Feature | Type | Description |
|---------|------|-------------|
| `mti_rain_x_souple` | bool | Rain + soft ground (compounding effect) |
| `mti_rain_intensity` | int | 0=dry, 1=light, 2=moderate, 3=heavy |
| `mti_terrain_degradation` | bool | Going deteriorating (recent rain) |
| `mti_frozen_risk` | bool | Frost risk (temperature < 3 C) |
| `mti_headwind` | bool | Strong headwind (wind > 30 km/h) |
| `mti_wind_impact` | int | 0=calm, 1=breezy, 2=windy, 3=storm |
| `mti_heat_stress` | bool | Heat stress (temperature > 30 C) |
| `mti_ideal_conditions` | bool | Good going + no rain + light wind |

## Data Volumes

- `13_meteo_historique`: 71 MB (weather per race via Open-Meteo)
- `39_reunions_enrichies`: 2 GB (weather/incidents/betting per meeting)
- `35_meteo_france`: 11 MB (Meteo France stations)
- `meteo_complete/`: 155 MB (intermediate merge)
- `fetch_openmeteo_missing`: 12,754 cached lookups (in progress)
