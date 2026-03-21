#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 96 -- NOAA Historical Weather API Scraper
Source : NOAA Climate Data Online (CDO) API - ncdc.noaa.gov
Collecte : temperature, precipitation, wind, humidity pour tous les hippodromes
CRITIQUE pour : Weather Model, Track Condition Prediction, Going Analysis
"""

import argparse
import json
import logging
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

import requests

SCRIPT_NAME = "96_noaa_weather"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("96_noaa_weather")

# NOAA CDO API base
NOAA_API_BASE = "https://www.ncdc.noaa.gov/cdo-web/api/v2"

# Major racecourse locations (lat, lon) for NOAA station lookup
# Format: name -> (lat, lon, country, noaa_station_id_if_known)
HIPPODROMES = {
    # France
    "Longchamp": (48.857, 2.237, "FR", ""),
    "Chantilly": (49.188, 2.469, "FR", ""),
    "Deauville": (49.355, 0.077, "FR", ""),
    "Saint-Cloud": (48.846, 2.199, "FR", ""),
    "Maisons-Laffitte": (48.952, 2.157, "FR", ""),
    "Vincennes": (48.832, 2.439, "FR", ""),
    "Auteuil": (48.848, 2.254, "FR", ""),
    "Enghien": (48.975, 2.302, "FR", ""),
    "Cagnes-sur-Mer": (43.664, 7.149, "FR", ""),
    "Lyon-Parilly": (45.722, 4.890, "FR", ""),
    "Toulouse": (43.600, 1.433, "FR", ""),
    "Bordeaux": (44.837, -0.579, "FR", ""),
    "Marseille-Borely": (43.261, 5.377, "FR", ""),
    "Strasbourg": (48.573, 7.752, "FR", ""),
    # UK
    "Ascot": (51.410, -0.674, "GB", ""),
    "Cheltenham": (51.917, -2.066, "GB", ""),
    "Epsom": (51.328, -0.265, "GB", ""),
    "Newmarket": (52.248, 0.391, "GB", ""),
    "York": (53.946, -1.090, "GB", ""),
    "Aintree": (53.477, -2.951, "GB", ""),
    "Goodwood": (50.880, -0.753, "GB", ""),
    "Kempton": (51.414, -0.410, "GB", ""),
    "Sandown": (51.374, -0.354, "GB", ""),
    "Doncaster": (53.527, -1.133, "GB", ""),
    # Ireland
    "Leopardstown": (53.273, -6.195, "IE", ""),
    "Curragh": (53.152, -6.793, "IE", ""),
    # USA
    "Churchill_Downs": (38.204, -85.771, "US", ""),
    "Belmont_Park": (40.717, -73.716, "US", ""),
    "Santa_Anita": (34.139, -118.042, "US", ""),
    "Saratoga": (43.081, -73.777, "US", ""),
    "Del_Mar": (32.959, -117.264, "US", ""),
    "Keeneland": (38.044, -84.590, "US", ""),
    "Gulfstream": (25.938, -80.148, "US", ""),
    # Australia
    "Flemington": (-37.789, 144.913, "AU", ""),
    "Randwick": (-33.903, 151.237, "AU", ""),
    "Moonee_Valley": (-37.767, 144.932, "AU", ""),
    # Japan
    "Tokyo_Racecourse": (35.666, 139.488, "JP", ""),
    # Hong Kong
    "Sha_Tin": (22.401, 114.197, "HK", ""),
    "Happy_Valley": (22.273, 114.184, "HK", ""),
}

# NOAA datasets
DATASETS = ["GHCND", "GSOM"]
# Data types of interest
DATATYPES = ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "AWND", "WSF2", "WSF5",
             "RHAV", "RHMN", "RHMX", "EVAP"]


def new_session(api_token=""):
    s = requests.Session()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if api_token:
        headers["token"] = api_token
    s.headers.update(headers)
    return s


def smart_pause(base=1.0, jitter=0.5):
    """NOAA API rate limit: 5 requests/sec, 10000/day."""
    pause = base + random.uniform(-jitter, jitter)
    time.sleep(max(0.3, pause))


def fetch_with_retry(session, url, params=None, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Rate limit, pause {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 503:
                log.warning(f"  503 Service unavailable, pause 30s...")
                time.sleep(30)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} sur {url} (essai {attempt}/{max_retries})")
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


def find_nearest_station(session, lat, lon, dataset_id="GHCND"):
    """Find the nearest NOAA station to given coordinates."""
    cache_key = f"station_{lat}_{lon}_{dataset_id}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    # Search within ~50km bounding box
    delta = 0.5  # approx 50km
    params = {
        "datasetid": dataset_id,
        "extent": f"{lat - delta},{lon - delta},{lat + delta},{lon + delta}",
        "limit": 10,
        "sortfield": "name",
    }

    resp = fetch_with_retry(session, f"{NOAA_API_BASE}/stations", params=params)
    if not resp:
        return None

    try:
        data = resp.json()
        results = data.get("results", [])
        if results:
            # Return the first station (closest)
            station = results[0]
            with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
                json.dump(station, f, ensure_ascii=True, indent=2)
            return station
    except (json.JSONDecodeError, KeyError):
        pass

    return None


def fetch_weather_data(session, station_id, start_date, end_date, dataset_id="GHCND"):
    """Fetch weather data for a station and date range."""
    cache_key = f"wx_{station_id}_{start_date}_{end_date}"
    cache_key = re.sub(r'[^a-zA-Z0-9_]', '_', cache_key)
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    all_results = []
    offset = 1
    limit = 1000

    while True:
        params = {
            "datasetid": dataset_id,
            "stationid": station_id,
            "startdate": start_date,
            "enddate": end_date,
            "datatypeid": ",".join(DATATYPES),
            "units": "metric",
            "limit": limit,
            "offset": offset,
        }

        resp = fetch_with_retry(session, f"{NOAA_API_BASE}/data", params=params)
        if not resp:
            break

        try:
            data = resp.json()
            results = data.get("results", [])
            if not results:
                break
            all_results.extend(results)
            metadata = data.get("metadata", {}).get("resultset", {})
            total_count = metadata.get("count", 0)
            if offset + limit > total_count:
                break
            offset += limit
            smart_pause(0.5, 0.2)
        except (json.JSONDecodeError, KeyError):
            break

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(all_results, f, ensure_ascii=True, indent=2)

    return all_results


def main():
    parser = argparse.ArgumentParser(
        description="Script 96 -- NOAA Weather Scraper for hippodromes")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD)")
    parser.add_argument("--token", type=str, default="",
                        help="NOAA CDO API token (get from ncdc.noaa.gov)")
    parser.add_argument("--token-file", type=str, default="",
                        help="File containing NOAA API token")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--chunk-days", type=int, default=365,
                        help="Nombre de jours par requete API (max 1 an)")
    args = parser.parse_args()

    # Load token
    api_token = args.token
    if not api_token and args.token_file:
        if os.path.exists(args.token_file):
            with open(args.token_file, "r", encoding="utf-8", errors="replace") as f:
                api_token = f.read().strip()
    if not api_token:
        # Try env var
        api_token = os.environ.get("NOAA_API_TOKEN", "")
    if not api_token:
        log.warning("  Pas de token NOAA API. Obtenez-en un sur ncdc.noaa.gov/cdo-web/token")
        log.warning("  Usage: --token YOUR_TOKEN ou NOAA_API_TOKEN env var")

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 96 -- NOAA Weather Scraper")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Hippodromes : {len(HIPPODROMES)}")
    log.info(f"  Token : {'OUI' if api_token else 'NON (requis)'}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    done_hippos = set(checkpoint.get("done_hippos", []))
    if args.resume and done_hippos:
        log.info(f"  Reprise checkpoint: {len(done_hippos)} hippodromes deja traites")

    session = new_session(api_token)
    output_file = os.path.join(OUTPUT_DIR, "noaa_weather_data.jsonl")

    total_records = 0
    hippo_count = 0

    for hippo_name, (lat, lon, country, station_hint) in HIPPODROMES.items():
        if hippo_name in done_hippos:
            continue

        log.info(f"  Hippodrome: {hippo_name} ({lat}, {lon}) [{country}]")

        # Find nearest station
        station = None
        if station_hint:
            station = {"id": station_hint}
        else:
            for dataset in DATASETS:
                station = find_nearest_station(session, lat, lon, dataset)
                if station:
                    break
                smart_pause(0.5, 0.2)

        if not station:
            log.warning(f"    Pas de station trouvee pour {hippo_name}")
            # Still record the attempt
            append_jsonl(output_file, {
                "source": "noaa",
                "type": "no_station",
                "hippodrome": hippo_name,
                "lat": lat,
                "lon": lon,
                "country": country,
                "scraped_at": datetime.now().isoformat(),
            })
            done_hippos.add(hippo_name)
            continue

        station_id = station.get("id", "")
        station_name = station.get("name", "")
        log.info(f"    Station: {station_id} - {station_name}")

        # Fetch weather in chunks (NOAA max 1 year per request)
        chunk_days = min(args.chunk_days, 365)
        current = start_date
        while current <= end_date:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
            start_str = current.strftime("%Y-%m-%d")
            end_str = chunk_end.strftime("%Y-%m-%d")

            for dataset in DATASETS:
                weather_data = fetch_weather_data(
                    session, station_id, start_str, end_str, dataset)

                if weather_data:
                    for obs in weather_data:
                        record = {
                            "source": "noaa",
                            "type": "weather_obs",
                            "dataset": dataset,
                            "hippodrome": hippo_name,
                            "lat": lat,
                            "lon": lon,
                            "country": country,
                            "station_id": station_id,
                            "station_name": station_name,
                            "date": obs.get("date", "")[:10],
                            "datatype": obs.get("datatype", ""),
                            "value": obs.get("value"),
                            "attributes": obs.get("attributes", ""),
                            "scraped_at": datetime.now().isoformat(),
                        }
                        append_jsonl(output_file, record)
                        total_records += 1

                smart_pause(0.5, 0.2)

            current = chunk_end + timedelta(days=1)

        done_hippos.add(hippo_name)
        hippo_count += 1

        log.info(f"    {hippo_name} termine: {total_records} records total")
        save_checkpoint({"done_hippos": list(done_hippos),
                         "total_records": total_records})

        if hippo_count % 10 == 0:
            session.close()
            session = new_session(api_token)
            time.sleep(random.uniform(3, 8))

    save_checkpoint({"done_hippos": list(done_hippos),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {hippo_count} hippodromes, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
