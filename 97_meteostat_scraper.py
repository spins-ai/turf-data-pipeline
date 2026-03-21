#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 97 -- Meteostat API Scraper
Source : Meteostat API (meteostat.net) - Precise hippodrome weather
Collecte : temperature, precipitation, vent, humidite, pression par hippodrome
CRITIQUE pour : Weather Model, Going Prediction, Track State Analysis
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests

SCRIPT_NAME = "97_meteostat"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Meteostat JSON API (free, rate-limited)
METEOSTAT_API = "https://meteostat.p.rapidapi.com"

# Hippodromes with coordinates
HIPPODROMES = {
    # France - Galop
    "Longchamp": (48.857, 2.237, "FR"),
    "Chantilly": (49.188, 2.469, "FR"),
    "Deauville": (49.355, 0.077, "FR"),
    "Saint-Cloud": (48.846, 2.199, "FR"),
    "Maisons-Laffitte": (48.952, 2.157, "FR"),
    "Auteuil": (48.848, 2.254, "FR"),
    "Compiegne": (49.417, 2.826, "FR"),
    "Fontainebleau": (48.398, 2.700, "FR"),
    "Lyon-Parilly": (45.722, 4.890, "FR"),
    "Cagnes-sur-Mer": (43.664, 7.149, "FR"),
    "Toulouse": (43.600, 1.433, "FR"),
    "Bordeaux-Le-Bouscat": (44.866, -0.597, "FR"),
    "Marseille-Borely": (43.261, 5.377, "FR"),
    "Strasbourg": (48.573, 7.752, "FR"),
    "Pau": (43.306, -0.360, "FR"),
    # France - Trot
    "Vincennes": (48.832, 2.439, "FR"),
    "Enghien": (48.975, 2.302, "FR"),
    "Cabourg": (49.283, -0.120, "FR"),
    "Caen": (49.177, -0.370, "FR"),
    "Laval": (48.073, -0.773, "FR"),
    # UK
    "Ascot": (51.410, -0.674, "GB"),
    "Cheltenham": (51.917, -2.066, "GB"),
    "Epsom": (51.328, -0.265, "GB"),
    "Newmarket": (52.248, 0.391, "GB"),
    "York": (53.946, -1.090, "GB"),
    "Aintree": (53.477, -2.951, "GB"),
    "Goodwood": (50.880, -0.753, "GB"),
    "Kempton": (51.414, -0.410, "GB"),
    "Sandown": (51.374, -0.354, "GB"),
    "Doncaster": (53.527, -1.133, "GB"),
    "Newbury": (51.401, -1.300, "GB"),
    "Haydock": (53.486, -2.625, "GB"),
    "Lingfield": (51.176, -0.017, "GB"),
    "Wolverhampton": (52.600, -2.105, "GB"),
    # Ireland
    "Leopardstown": (53.273, -6.195, "IE"),
    "Curragh": (53.152, -6.793, "IE"),
    "Fairyhouse": (53.500, -6.538, "IE"),
    "Punchestown": (53.191, -6.671, "IE"),
    # USA
    "Churchill_Downs": (38.204, -85.771, "US"),
    "Belmont_Park": (40.717, -73.716, "US"),
    "Santa_Anita": (34.139, -118.042, "US"),
    "Saratoga": (43.081, -73.777, "US"),
    "Del_Mar": (32.959, -117.264, "US"),
    "Keeneland": (38.044, -84.590, "US"),
    "Gulfstream": (25.938, -80.148, "US"),
    # Australia
    "Flemington": (-37.789, 144.913, "AU"),
    "Randwick": (-33.903, 151.237, "AU"),
    "Moonee_Valley": (-37.767, 144.932, "AU"),
    "Rosehill": (-33.828, 151.023, "AU"),
    # Japan
    "Tokyo_RC": (35.666, 139.488, "JP"),
    "Nakayama": (35.740, 139.955, "JP"),
    # Hong Kong
    "Sha_Tin": (22.401, 114.197, "HK"),
    "Happy_Valley": (22.273, 114.184, "HK"),
}


def new_session(api_key=""):
    s = requests.Session()
    headers = {
        "Accept": "application/json",
    }
    if api_key:
        headers["X-RapidAPI-Key"] = api_key
        headers["X-RapidAPI-Host"] = "meteostat.p.rapidapi.com"
    s.headers.update(headers)
    return s


def smart_pause(base=1.5, jitter=0.5):
    """Meteostat rate limit: depends on plan."""
    pause = base + random.uniform(-jitter, jitter)
    time.sleep(max(0.5, pause))


def fetch_with_retry(session, url, params=None, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Rate limit, pause {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                log.warning(f"  403 Forbidden, pause 60s...")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} (essai {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Erreur reseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Echec apres {max_retries} essais: {url}")
    return None


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8", errors="replace", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8", errors="replace") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def fetch_daily_weather(session, lat, lon, start_date, end_date, use_rapidapi=True):
    """Fetch daily weather data from Meteostat for a location."""
    cache_key = f"daily_{lat}_{lon}_{start_date}_{end_date}"
    cache_key = re.sub(r'[^a-zA-Z0-9_]', '_', cache_key)
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    if use_rapidapi:
        url = f"{METEOSTAT_API}/point/daily"
    else:
        # Fallback: Meteostat open JSON endpoint
        url = "https://bulk.meteostat.net/v2/daily"

    params = {
        "lat": lat,
        "lon": lon,
        "start": start_date,
        "end": end_date,
        "units": "metric",
    }

    resp = fetch_with_retry(session, url, params=params)
    if not resp:
        return []

    try:
        data = resp.json()
        results = data.get("data", [])
    except (json.JSONDecodeError, KeyError):
        results = []

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(results, f, ensure_ascii=True, indent=2)

    return results


def fetch_hourly_weather(session, lat, lon, start_date, end_date, use_rapidapi=True):
    """Fetch hourly weather data from Meteostat."""
    cache_key = f"hourly_{lat}_{lon}_{start_date}_{end_date}"
    cache_key = re.sub(r'[^a-zA-Z0-9_]', '_', cache_key)
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    if use_rapidapi:
        url = f"{METEOSTAT_API}/point/hourly"
    else:
        url = "https://bulk.meteostat.net/v2/hourly"

    params = {
        "lat": lat,
        "lon": lon,
        "start": start_date,
        "end": end_date,
        "units": "metric",
    }

    resp = fetch_with_retry(session, url, params=params)
    if not resp:
        return []

    try:
        data = resp.json()
        results = data.get("data", [])
    except (json.JSONDecodeError, KeyError):
        results = []

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(results, f, ensure_ascii=True, indent=2)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Script 97 -- Meteostat Weather Scraper for hippodromes")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD)")
    parser.add_argument("--api-key", type=str, default="",
                        help="RapidAPI key for Meteostat")
    parser.add_argument("--api-key-file", type=str, default="",
                        help="File containing RapidAPI key")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--hourly", action="store_true", default=False,
                        help="Aussi collecter les donnees horaires")
    parser.add_argument("--chunk-days", type=int, default=365,
                        help="Nombre de jours par requete")
    args = parser.parse_args()

    # Load API key
    api_key = args.api_key
    if not api_key and args.api_key_file:
        if os.path.exists(args.api_key_file):
            with open(args.api_key_file, "r", encoding="utf-8", errors="replace") as f:
                api_key = f.read().strip()
    if not api_key:
        api_key = os.environ.get("METEOSTAT_API_KEY", "")
        if not api_key:
            api_key = os.environ.get("RAPIDAPI_KEY", "")
    use_rapidapi = bool(api_key)

    if not api_key:
        log.warning("  Pas de cle API Meteostat/RapidAPI.")
        log.warning("  Usage: --api-key YOUR_KEY ou METEOSTAT_API_KEY env var")

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 97 -- Meteostat Weather Scraper")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Hippodromes : {len(HIPPODROMES)}")
    log.info(f"  API key : {'OUI' if api_key else 'NON'}")
    log.info(f"  Donnees horaires : {'OUI' if args.hourly else 'NON'}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    done_hippos = set(checkpoint.get("done_hippos", []))
    if args.resume and done_hippos:
        log.info(f"  Reprise checkpoint: {len(done_hippos)} hippodromes deja traites")

    session = new_session(api_key)
    output_file = os.path.join(OUTPUT_DIR, "meteostat_data.jsonl")

    total_records = 0
    hippo_count = 0

    for hippo_name, (lat, lon, country) in HIPPODROMES.items():
        if hippo_name in done_hippos:
            continue

        log.info(f"  Hippodrome: {hippo_name} ({lat}, {lon}) [{country}]")

        # Fetch in chunks
        chunk_days = args.chunk_days
        current = start_date
        while current <= end_date:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
            s_str = current.strftime("%Y-%m-%d")
            e_str = chunk_end.strftime("%Y-%m-%d")

            # Daily data
            daily_data = fetch_daily_weather(session, lat, lon, s_str, e_str, use_rapidapi)
            if daily_data:
                for obs in daily_data:
                    record = {
                        "source": "meteostat",
                        "type": "daily",
                        "hippodrome": hippo_name,
                        "lat": lat,
                        "lon": lon,
                        "country": country,
                        "date": obs.get("date", ""),
                        "tavg": obs.get("tavg"),
                        "tmin": obs.get("tmin"),
                        "tmax": obs.get("tmax"),
                        "prcp": obs.get("prcp"),
                        "snow": obs.get("snow"),
                        "wdir": obs.get("wdir"),
                        "wspd": obs.get("wspd"),
                        "wpgt": obs.get("wpgt"),
                        "pres": obs.get("pres"),
                        "tsun": obs.get("tsun"),
                        "scraped_at": datetime.now().isoformat(),
                    }
                    append_jsonl(output_file, record)
                    total_records += 1

            smart_pause(1.0, 0.3)

            # Hourly data (optional)
            if args.hourly:
                hourly_data = fetch_hourly_weather(session, lat, lon, s_str, e_str, use_rapidapi)
                if hourly_data:
                    for obs in hourly_data:
                        record = {
                            "source": "meteostat",
                            "type": "hourly",
                            "hippodrome": hippo_name,
                            "lat": lat,
                            "lon": lon,
                            "country": country,
                            "date": obs.get("date", ""),
                            "hour": obs.get("hour", obs.get("time", "")),
                            "temp": obs.get("temp"),
                            "dwpt": obs.get("dwpt"),
                            "rhum": obs.get("rhum"),
                            "prcp": obs.get("prcp"),
                            "snow": obs.get("snow"),
                            "wdir": obs.get("wdir"),
                            "wspd": obs.get("wspd"),
                            "wpgt": obs.get("wpgt"),
                            "pres": obs.get("pres"),
                            "tsun": obs.get("tsun"),
                            "coco": obs.get("coco"),
                            "scraped_at": datetime.now().isoformat(),
                        }
                        append_jsonl(output_file, record)
                        total_records += 1

                smart_pause(1.0, 0.3)

            current = chunk_end + timedelta(days=1)

        done_hippos.add(hippo_name)
        hippo_count += 1

        log.info(f"    {hippo_name} termine: {total_records} records total")
        save_checkpoint({"done_hippos": list(done_hippos),
                         "total_records": total_records})

        if hippo_count % 10 == 0:
            session.close()
            session = new_session(api_key)
            time.sleep(random.uniform(3, 8))

    save_checkpoint({"done_hippos": list(done_hippos),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {hippo_count} hippodromes, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
