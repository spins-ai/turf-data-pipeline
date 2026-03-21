#!/usr/bin/env python3
"""
13_meteo_historique.py
=====================
Collecte des donnees meteo historiques pour chaque course hippique.

Source :
  - Meteostat Python library (v2.x) — donnees horaires par station
    Pas de cle API, pas de rate-limit.

Strategie :
  1. Pour chaque hippodrome, trouver la station Meteostat la plus proche.
  2. Regrouper les courses par station, puis fetcher le range complet de dates
     en un seul appel par station (Meteostat telecharge des CSV bulk).
  3. Extraire la meteo a l'heure de chaque course.

Produit :
  - output/13_meteo_historique/cache/*.json — cache brut par date+hippodrome
  - output/13_meteo_historique/meteo_historique.json
  - output/13_meteo_historique/meteo_historique.parquet
  - output/13_meteo_historique/meteo_historique.csv
  - .checkpoint_13.json

Usage :
    python3 13_meteo_historique.py
    python3 13_meteo_historique.py --batch 200
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from meteostat import Point
import meteostat

from hippodromes_db import HIPPODROMES_DB

# Imports optionnels
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False


# ===========================================================================
# CONFIG
# ===========================================================================

COURSES_PATH = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "courses_normalisees.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "13_meteo_historique"
CACHE_DIR = OUTPUT_DIR / "cache"

from utils.logging_setup import setup_logging


# ===========================================================================
# WMO / METEOSTAT CONDITION CODE MAPPING
# ===========================================================================
# Meteostat 'coco' (condition code) uses the same WMO-like scheme:
#  1=Clear, 2=Fair, 3=Cloudy, 4=Overcast, 5=Fog, 6=Freezing Fog,
#  7=Light Rain, 8=Rain, 9=Heavy Rain, 10=Freezing Rain, 11=Heavy Freezing Rain,
#  12=Sleet, 13=Heavy Sleet, 14=Light Snow, 15=Snow, 16=Heavy Snow,
#  17=Rain Shower, 18=Heavy Rain Shower, 19=Sleet Shower, 20=Heavy Sleet Shower,
#  21=Snow Shower, 22=Heavy Snow Shower, 23=Lightning, 24=Hail, 25=Thunderstorm,
#  26=Heavy Thunderstorm, 27=Storm

COCO_TO_WMO: dict[int, int] = {
    1: 0,    # Clear -> clair
    2: 1,    # Fair -> principalement clair
    3: 2,    # Cloudy -> partiellement nuageux
    4: 3,    # Overcast -> couvert
    5: 45,   # Fog -> brouillard
    6: 48,   # Freezing Fog -> brouillard givrant
    7: 61,   # Light Rain -> pluie legere
    8: 63,   # Rain -> pluie moderee
    9: 65,   # Heavy Rain -> pluie forte
    10: 66,  # Freezing Rain -> pluie verglacante legere
    11: 67,  # Heavy Freezing Rain -> pluie verglacante forte
    12: 51,  # Sleet -> bruine legere (approximation)
    13: 55,  # Heavy Sleet -> bruine forte
    14: 71,  # Light Snow -> neige legere
    15: 73,  # Snow -> neige moderee
    16: 75,  # Heavy Snow -> neige forte
    17: 80,  # Rain Shower -> averses legeres
    18: 82,  # Heavy Rain Shower -> averses violentes
    19: 85,  # Sleet Shower -> averses de neige legeres
    20: 86,  # Heavy Sleet Shower -> averses de neige fortes
    21: 85,  # Snow Shower -> averses de neige legeres
    22: 86,  # Heavy Snow Shower -> averses de neige fortes
    23: 95,  # Lightning -> orage
    24: 99,  # Hail -> orage avec grele forte
    25: 95,  # Thunderstorm -> orage
    26: 96,  # Heavy Thunderstorm -> orage avec grele legere
    27: 99,  # Storm -> orage avec grele forte
}

WMO_CODES: dict[int, str] = {
    0: "clair",
    1: "principalement clair",
    2: "partiellement nuageux",
    3: "couvert",
    45: "brouillard",
    48: "brouillard givrant",
    51: "bruine legere",
    53: "bruine moderee",
    55: "bruine forte",
    61: "pluie legere",
    63: "pluie moderee",
    65: "pluie forte",
    66: "pluie verglacante legere",
    67: "pluie verglacante forte",
    71: "neige legere",
    73: "neige moderee",
    75: "neige forte",
    80: "averses legeres",
    81: "averses moderees",
    82: "averses violentes",
    85: "averses de neige legeres",
    86: "averses de neige fortes",
    95: "orage",
    96: "orage avec grele legere",
    99: "orage avec grele forte",
}


# ===========================================================================
# DATACLASS
# ===========================================================================

@dataclass
class MeteoNormalisee:
    """Meteo historique normalisee pour une course."""
    course_uid: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    heure_depart: str = ""
    # Valeurs a l'heure de la course
    temperature_c: Optional[float] = None       # degC at race hour
    humidity_pct: Optional[float] = None         # % at race hour
    precipitation_mm: Optional[float] = None     # mm at race hour
    wind_speed_kmh: Optional[float] = None       # km/h at race hour
    wind_gusts_kmh: Optional[float] = None       # km/h at race hour
    weather_code: Optional[int] = None           # WMO code (0=clear, 61=rain, etc.)
    weather_description: str = ""                # human readable from WMO code
    # Agregats journaliers
    temp_min_c: Optional[float] = None
    temp_max_c: Optional[float] = None
    precip_total_mm: Optional[float] = None      # total for the day
    wind_max_kmh: Optional[float] = None
    # Derives
    is_rainy: bool = False                       # precipitation > 0.5mm
    is_windy: bool = False                       # wind > 30 km/h
    is_hot: bool = False                         # temp > 28degC
    is_cold: bool = False                        # temp < 5degC


# ===========================================================================
# UTILITAIRES
# ===========================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_hour(heure_depart: str) -> int:
    """Parse 'HH:MM' et retourne l'heure entiere la plus proche (0-23)."""
    if not heure_depart or ":" not in heure_depart:
        return 14  # defaut milieu d'apres-midi
    try:
        parts = heure_depart.strip().split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        if m >= 30:
            h = min(h + 1, 23)
        return max(0, min(h, 23))
    except (ValueError, IndexError):
        return 14


def cache_key(date_iso: str, hippodrome: str) -> str:
    """Cle de cache pour un couple date+hippodrome."""
    safe_hippo = hippodrome.replace(" ", "_").replace("/", "_")
    return f"{date_iso}_{safe_hippo}"


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else round(f, 2)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# CHECKPOINT
# ===========================================================================

class CheckpointManager:
    def __init__(self, path: Path):
        self.path = path
        self._data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        return {"completed_keys": []}

    def is_done(self, key: str) -> bool:
        return key in self._data.get("completed_keys", [])

    def mark_done(self, key: str):
        self._data.setdefault("completed_keys", []).append(key)

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False))
        tmp.replace(self.path)

    @property
    def count_done(self) -> int:
        return len(self._data.get("completed_keys", []))


# ===========================================================================
# CACHE METEO (un fichier JSON par date+hippodrome)
# ===========================================================================

def load_cached_meteo(date_iso: str, hippodrome: str) -> Optional[dict]:
    """Charge le cache meteo pour un couple date+hippodrome."""
    path = CACHE_DIR / f"{cache_key(date_iso, hippodrome)}.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return None


def save_cached_meteo(date_iso: str, hippodrome: str, data: dict):
    """Sauvegarde le cache meteo pour un couple date+hippodrome."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{cache_key(date_iso, hippodrome)}.json"
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


# ===========================================================================
# METEOSTAT : STATION MAPPING
# ===========================================================================

def build_station_mapping(
    logger: logging.Logger,
) -> dict[str, str]:
    """
    Pour chaque hippodrome dans HIPPODROMES_DB, trouve la station Meteostat
    la plus proche. Retourne hippo_name -> station_id.
    """
    hippo_to_station: dict[str, str] = {}

    for hippo_name, info in HIPPODROMES_DB.items():
        lat, lon = info["lat"], info["lon"]
        try:
            df = meteostat.stations.nearby(Point(lat, lon))
            df = df.head(5)  # top 5 plus proches
            if df.empty:
                logger.warning("Aucune station trouvee pour %s (%.3f, %.3f)", hippo_name, lat, lon)
                continue
            station_id = df.index[0]
            station_name = df.iloc[0].get("name", "?")
            hippo_to_station[hippo_name] = station_id
            logger.debug("  %s -> station %s (%s)", hippo_name, station_id, station_name)
        except Exception as e:
            logger.warning("Erreur station pour %s : %s", hippo_name, e)

    logger.info("Mapping hippodrome->station : %d / %d hippodromes mappes",
                len(hippo_to_station), len(HIPPODROMES_DB))
    return hippo_to_station


# ===========================================================================
# METEOSTAT : FETCH HOURLY DATA
# ===========================================================================

def fetch_station_hourly(
    station_id: str,
    start_date: datetime,
    end_date: datetime,
    logger: logging.Logger,
) -> Optional[pd.DataFrame]:
    """
    Fetch hourly data from Meteostat for a station over a date range.
    Returns a DataFrame indexed by datetime, or None on failure.
    Columns: temp, rhum, prcp, snwd, wdir, wspd, wpgt, pres, tsun, cldc, coco
    """
    try:
        ts = meteostat.hourly(station_id, start_date, end_date)
        df = ts.fetch()
        if df is None or df.empty:
            logger.warning("Pas de donnees Meteostat pour station %s (%s -> %s)",
                           station_id, start_date.date(), end_date.date())
            return None
        return df
    except Exception as e:
        logger.warning("Erreur Meteostat pour station %s : %s", station_id, e)
        return None


def meteostat_row_to_cache_format(row: pd.Series) -> dict:
    """
    Convertit une ligne horaire Meteostat en format compatible avec le cache
    existant (structure Open-Meteo-like pour extract_hourly / extract_daily_aggregates).
    """
    coco = _safe_float(row.get("coco"))
    wmo_code = None
    if coco is not None:
        wmo_code = COCO_TO_WMO.get(int(coco))

    return {
        "temperature_c": _safe_float(row.get("temp")),
        "humidity_pct": _safe_float(row.get("rhum")),
        "precipitation_mm": _safe_float(row.get("prcp")),
        "wind_speed_kmh": _safe_float(row.get("wspd")),
        "wind_gusts_kmh": _safe_float(row.get("wpgt")),
        "weather_code": wmo_code,
    }


def extract_day_from_df(df: pd.DataFrame, date_iso: str) -> Optional[dict]:
    """
    Extrait les 24 heures d'une journee depuis le DataFrame Meteostat
    et retourne un dict au format cache (compatible avec l'ancien format Open-Meteo).

    Format retourne:
    {
        "hourly": {
            "temperature_2m": [24 valeurs],
            "relative_humidity_2m": [24 valeurs],
            "precipitation": [24 valeurs],
            "wind_speed_10m": [24 valeurs],
            "wind_gusts_10m": [24 valeurs],
            "weather_code": [24 valeurs],
        }
    }
    """
    try:
        target_date = pd.Timestamp(date_iso)
    except Exception as e:
        log.debug(f"  Erreur parsing date meteo: {e}")
        return None

    # Filtrer les lignes du jour
    mask = (df.index.date == target_date.date())
    day_df = df.loc[mask]

    if day_df.empty:
        return None

    # Construire les tableaux horaires (0-23), avec None pour les heures manquantes
    temps = [None] * 24
    rhums = [None] * 24
    prcps = [None] * 24
    wspds = [None] * 24
    wpgts = [None] * 24
    wcodes = [None] * 24

    for ts, row in day_df.iterrows():
        h = ts.hour
        if 0 <= h <= 23:
            temps[h] = _safe_float(row.get("temp"))
            rhums[h] = _safe_float(row.get("rhum"))
            prcps[h] = _safe_float(row.get("prcp"))
            wspds[h] = _safe_float(row.get("wspd"))
            wpgts[h] = _safe_float(row.get("wpgt"))
            coco = _safe_float(row.get("coco"))
            if coco is not None:
                wcodes[h] = COCO_TO_WMO.get(int(coco))

    return {
        "hourly": {
            "temperature_2m": temps,
            "relative_humidity_2m": rhums,
            "precipitation": prcps,
            "wind_speed_10m": wspds,
            "wind_gusts_10m": wpgts,
            "weather_code": wcodes,
        }
    }


# ===========================================================================
# EXTRACTION DES DONNEES HORAIRES (compatible ancien format)
# ===========================================================================

def extract_hourly(data: dict, hour_index: int) -> dict:
    """
    Extrait les valeurs meteo pour une heure donnee (index 0-23)
    a partir du format cache.
    """
    hourly = data.get("hourly", {})
    result = {}

    def _safe_get(key: str, idx: int):
        arr = hourly.get(key, [])
        if arr and 0 <= idx < len(arr):
            return arr[idx]
        return None

    result["temperature_c"] = _safe_get("temperature_2m", hour_index)
    result["humidity_pct"] = _safe_get("relative_humidity_2m", hour_index)
    result["precipitation_mm"] = _safe_get("precipitation", hour_index)
    result["wind_speed_kmh"] = _safe_get("wind_speed_10m", hour_index)
    result["wind_gusts_kmh"] = _safe_get("wind_gusts_10m", hour_index)
    result["weather_code"] = _safe_get("weather_code", hour_index)

    return result


def extract_daily_aggregates(data: dict) -> dict:
    """Calcule les agregats journaliers a partir des donnees horaires."""
    hourly = data.get("hourly", {})
    temps = [v for v in hourly.get("temperature_2m", []) if v is not None]
    precips = [v for v in hourly.get("precipitation", []) if v is not None]
    winds = [v for v in hourly.get("wind_speed_10m", []) if v is not None]

    return {
        "temp_min_c": round(min(temps), 2) if temps else None,
        "temp_max_c": round(max(temps), 2) if temps else None,
        "precip_total_mm": round(sum(precips), 2) if precips else None,
        "wind_max_kmh": round(max(winds), 2) if winds else None,
    }


def build_meteo_normalisee(
    course: dict,
    api_data: dict,
) -> MeteoNormalisee:
    """Construit un MeteoNormalisee a partir d'une course et des donnees cache."""
    hour_idx = parse_hour(course.get("heure_depart", ""))
    hourly_vals = extract_hourly(api_data, hour_idx)
    daily_agg = extract_daily_aggregates(api_data)

    wcode = hourly_vals.get("weather_code")
    weather_desc = WMO_CODES.get(wcode, "inconnu") if wcode is not None else "inconnu"

    temp = hourly_vals.get("temperature_c")
    precip = hourly_vals.get("precipitation_mm")
    wind = hourly_vals.get("wind_speed_kmh")

    return MeteoNormalisee(
        course_uid=course.get("course_uid", ""),
        date_reunion_iso=course.get("date_reunion_iso", ""),
        hippodrome_normalise=course.get("hippodrome_normalise", ""),
        heure_depart=course.get("heure_depart", ""),
        # Horaire
        temperature_c=hourly_vals.get("temperature_c"),
        humidity_pct=hourly_vals.get("humidity_pct"),
        precipitation_mm=precip,
        wind_speed_kmh=wind,
        wind_gusts_kmh=hourly_vals.get("wind_gusts_kmh"),
        weather_code=wcode,
        weather_description=weather_desc,
        # Journalier
        temp_min_c=daily_agg.get("temp_min_c"),
        temp_max_c=daily_agg.get("temp_max_c"),
        precip_total_mm=daily_agg.get("precip_total_mm"),
        wind_max_kmh=daily_agg.get("wind_max_kmh"),
        # Derives
        is_rainy=precip is not None and precip > 0.5,
        is_windy=wind is not None and wind > 30,
        is_hot=temp is not None and temp > 28,
        is_cold=temp is not None and temp < 5,
    )


# ===========================================================================
# SAUVEGARDE
# ===========================================================================

def sauver_json(data: list[dict], path: Path, logger: logging.Logger):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)
    logger.info("Sauve: %s (%d entrees)", path.name, len(data))


def sauver_parquet(data: list[dict], path: Path, logger: logging.Logger):
    if not HAS_PARQUET or not data:
        return
    try:
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path)
        logger.info("Sauve: %s", path.name)
    except Exception as e:
        logger.warning("Parquet ignore: %s", e)


def sauver_csv(data: list[dict], path: Path, logger: logging.Logger):
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(data[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    logger.info("Sauve: %s", path.name)


def sauver_jsonl(data: list[dict], path: Path, logger: logging.Logger):
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    tmp.replace(path)
    logger.info("Sauve: %s (%d entrees)", path.name, len(data))


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Collecte meteo historique via Meteostat")
    parser.add_argument("--batch", type=int, default=200,
                        help="Sauvegarde checkpoint tous les N groupes traites")
    args = parser.parse_args()

    logger = setup_logging("13_meteo_historique")
    logger.info("=" * 70)
    logger.info("13_meteo_historique -- Collecte meteo historique via Meteostat")
    logger.info("=" * 70)

    # -- Charger les courses ---------------------------------------------------
    if not COURSES_PATH.exists():
        logger.error("Fichier introuvable : %s", COURSES_PATH)
        sys.exit(1)

    with open(COURSES_PATH, encoding="utf-8") as f:
        courses = json.load(f)
    logger.info("Courses chargees : %d", len(courses))

    # -- Checkpoint ------------------------------------------------------------
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint = CheckpointManager(OUTPUT_DIR / ".checkpoint_13.json")
    logger.info("Checkpoint : %d cles deja traitees", checkpoint.count_done)

    # -- Mapping hippodrome -> station Meteostat --------------------------------
    logger.info("Construction du mapping hippodrome -> station Meteostat...")
    hippo_to_station = build_station_mapping(logger)

    # -- Regrouper par (date, hippodrome) --------------------------------------
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    hippo_missing = set()
    hippo_no_station = set()

    for c in courses:
        date_iso = c.get("date_reunion_iso", "")
        hippo = c.get("hippodrome_normalise", "")
        if not date_iso or not hippo:
            continue
        if hippo not in HIPPODROMES_DB:
            hippo_missing.add(hippo)
            continue
        if hippo not in hippo_to_station:
            hippo_no_station.add(hippo)
            continue
        groups[(date_iso, hippo)].append(c)

    if hippo_missing:
        logger.warning(
            "Hippodromes absents de HIPPODROMES_DB (%d) : %s",
            len(hippo_missing),
            ", ".join(sorted(hippo_missing)[:20]),
        )
    if hippo_no_station:
        logger.warning(
            "Hippodromes sans station Meteostat (%d) : %s",
            len(hippo_no_station),
            ", ".join(sorted(hippo_no_station)[:20]),
        )

    total_groups = len(groups)
    total_courses = sum(len(v) for v in groups.values())
    logger.info(
        "Groupes (date, hippodrome) : %d -- Courses couvertes : %d",
        total_groups, total_courses,
    )

    # -- Regrouper par station pour fetch batch --------------------------------
    # station_id -> { "hippodromes": set, "dates": set, "groups": list of (date, hippo) }
    station_batches: dict[str, dict] = defaultdict(lambda: {"hippodromes": set(), "dates": set(), "groups": []})

    groups_needing_fetch: list[tuple[str, str]] = []  # (date, hippo) that need API fetch

    for (date_iso, hippo) in sorted(groups.keys()):
        ck = cache_key(date_iso, hippo)
        # Skip if already fully processed (checkpoint + cache exist)
        if checkpoint.is_done(ck):
            continue
        # Skip if cache file already exists on disk
        if load_cached_meteo(date_iso, hippo) is not None:
            continue
        # Need to fetch from Meteostat
        station_id = hippo_to_station[hippo]
        station_batches[station_id]["hippodromes"].add(hippo)
        station_batches[station_id]["dates"].add(date_iso)
        station_batches[station_id]["groups"].append((date_iso, hippo))
        groups_needing_fetch.append((date_iso, hippo))

    logger.info("Groupes necessitant un fetch Meteostat : %d (sur %d stations)",
                len(groups_needing_fetch), len(station_batches))

    # -- Fetch par station (batch) ---------------------------------------------
    # For each station, fetch the full date range at once, then extract per-day
    station_dataframes: dict[str, pd.DataFrame] = {}
    fetch_count = 0
    fetch_errors = 0

    for station_id, batch_info in station_batches.items():
        dates = sorted(batch_info["dates"])
        if not dates:
            continue
        start_dt = datetime.strptime(dates[0], "%Y-%m-%d")
        end_dt = datetime.strptime(dates[-1], "%Y-%m-%d") + timedelta(hours=23)

        hippo_names = ", ".join(sorted(batch_info["hippodromes"])[:3])
        logger.info(
            "Fetch Meteostat station %s (%s...) : %s -> %s (%d jours)",
            station_id, hippo_names, dates[0], dates[-1], len(dates),
        )

        df = fetch_station_hourly(station_id, start_dt, end_dt, logger)
        fetch_count += 1

        if df is not None:
            station_dataframes[station_id] = df
            logger.info("  -> %d lignes horaires recues", len(df))
        else:
            fetch_errors += 1
            logger.warning("  -> Echec pour station %s", station_id)

    logger.info("Fetches Meteostat : %d stations, %d erreurs", fetch_count, fetch_errors)

    # -- Extraire et cacher les donnees par jour+hippodrome --------------------
    extracted = 0
    extract_errors = 0

    for station_id, batch_info in station_batches.items():
        df = station_dataframes.get(station_id)
        if df is None:
            # Mark all groups for this station as done (no data)
            for (date_iso, hippo) in batch_info["groups"]:
                ck = cache_key(date_iso, hippo)
                checkpoint.mark_done(ck)
            extract_errors += len(batch_info["groups"])
            continue

        for (date_iso, hippo) in batch_info["groups"]:
            day_data = extract_day_from_df(df, date_iso)
            if day_data is not None:
                save_cached_meteo(date_iso, hippo, day_data)
                extracted += 1
            else:
                extract_errors += 1

    logger.info("Extraction : %d jours caches, %d sans donnees", extracted, extract_errors)

    # -- Construire les MeteoNormalisee pour TOUS les groupes ------------------
    all_meteo: list[MeteoNormalisee] = []
    cache_hits = 0
    errors = 0
    groups_processed = 0
    t_start = time.time()

    sorted_keys = sorted(groups.keys())

    for idx, (date_iso, hippo) in enumerate(sorted_keys, 1):
        ck = cache_key(date_iso, hippo)

        # Charger depuis le cache
        cached = load_cached_meteo(date_iso, hippo)

        if cached is not None:
            for c in groups[(date_iso, hippo)]:
                mn = build_meteo_normalisee(c, cached)
                all_meteo.append(mn)
            cache_hits += 1
        else:
            errors += 1
            if not checkpoint.is_done(ck):
                logger.debug("Pas de donnees pour %s @ %s — %d courses ignorees",
                             hippo, date_iso, len(groups[(date_iso, hippo)]))

        # Marquer dans le checkpoint
        if not checkpoint.is_done(ck):
            checkpoint.mark_done(ck)

        groups_processed += 1

        # Sauvegarde intermediaire
        if groups_processed % args.batch == 0:
            checkpoint.save()

        # Progression
        if idx % 500 == 0 or idx == total_groups:
            elapsed = time.time() - t_start
            rate = idx / elapsed if elapsed > 0 else 0
            logger.info(
                "Progression : %d/%d groupes (%.1f/s) -- %d cache hits, %d erreurs -- %d meteos",
                idx, total_groups, rate, cache_hits, errors, len(all_meteo),
            )

    # -- Checkpoint final ------------------------------------------------------
    checkpoint.save()

    elapsed = time.time() - t_start
    logger.info("=" * 70)
    logger.info("Collecte terminee en %.1fs", elapsed)
    logger.info("  Groupes traites  : %d", total_groups)
    logger.info("  Stations fetchees: %d", fetch_count)
    logger.info("  Cache hits       : %d", cache_hits)
    logger.info("  Erreurs          : %d", errors)
    logger.info("  Meteos produites : %d", len(all_meteo))

    # -- Export ----------------------------------------------------------------
    if not all_meteo:
        logger.warning("Aucune donnee meteo collectee -- pas d'export.")
        return

    meteo_dicts = [asdict(m) for m in all_meteo]

    sauver_json(meteo_dicts, OUTPUT_DIR / "meteo_historique.json", logger)
    sauver_jsonl(meteo_dicts, OUTPUT_DIR / "meteo_historique.jsonl", logger)
    sauver_parquet(meteo_dicts, OUTPUT_DIR / "meteo_historique.parquet", logger)
    sauver_csv(meteo_dicts, OUTPUT_DIR / "meteo_historique.csv", logger)

    logger.info("=" * 70)
    logger.info("13_meteo_historique termine avec succes.")


if __name__ == "__main__":
    main()
