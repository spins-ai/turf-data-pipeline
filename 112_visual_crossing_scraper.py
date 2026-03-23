#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 112 -- Visual Crossing Weather API Scraper
Source : Visual Crossing Timeline API (weather.visualcrossing.com)
Collecte : temperature, precipitation, vent, humidite, pression, UV, conditions
CRITIQUE pour : Weather Model, Going Prediction, Track State Analysis

Free tier: 1000 requests/day, historical data included.
API docs: https://www.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

import requests

SCRIPT_NAME = "112_visual_crossing"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import (
    smart_pause, fetch_with_retry, load_checkpoint, save_checkpoint,
    append_jsonl, create_session,
)
from hippodromes_db import HIPPODROMES_DB

log = setup_logging("112_visual_crossing")

# Visual Crossing Timeline API base
VC_API_BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

# Free tier: max 1000 requests/day
MAX_REQUESTS_PER_DAY = 1000


def _make_session():
    """Create an HTTP session with appropriate headers."""
    s = create_session()
    s.headers.update({
        "Accept": "application/json",
    })
    return s


def _build_hippodromes_list():
    """
    Build a deduplicated list of hippodromes from HIPPODROMES_DB.
    Each entry: (name, lat, lon, country).
    Only includes entries that have valid lat/lon.
    """
    hippos = []
    seen_coords = set()
    for name, info in sorted(HIPPODROMES_DB.items()):
        lat = info.get("lat")
        lon = info.get("lon")
        pays = info.get("pays", "")
        if lat is None or lon is None:
            continue
        # Deduplicate by rounding coords to 2 decimals
        coord_key = (round(lat, 2), round(lon, 2))
        if coord_key in seen_coords:
            continue
        seen_coords.add(coord_key)
        hippos.append((name, lat, lon, pays))
    return hippos


def fetch_weather_period(session, lat, lon, start_date, end_date, api_key):
    """
    Fetch weather data from Visual Crossing Timeline API for a location and date range.
    The API accepts ranges up to ~1 year per call.
    Returns a list of daily observation dicts, or empty list on failure.
    """
    cache_key = f"vc_{lat}_{lon}_{start_date}_{end_date}"
    cache_key = re.sub(r'[^a-zA-Z0-9_]', '_', cache_key)
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                pass

    # Timeline API: /timeline/{location}/{start}/{end}
    location = f"{lat},{lon}"
    url = f"{VC_API_BASE}/{location}/{start_date}/{end_date}"

    params = {
        "unitGroup": "metric",
        "key": api_key,
        "contentType": "json",
        "include": "days",
        "elements": ",".join([
            "datetime", "tempmax", "tempmin", "temp", "feelslikemax", "feelslikemin",
            "feelslike", "dew", "humidity", "precip", "precipprob", "precipcover",
            "preciptype", "snow", "snowdepth", "windgust", "windspeed", "winddir",
            "pressure", "cloudcover", "visibility", "solarradiation", "solarenergy",
            "uvindex", "conditions", "description", "icon",
        ]),
    }

    resp = fetch_with_retry(session, url, params=params)
    if not resp:
        return []

    try:
        data = resp.json()
        # Check for API error
        if "errorCode" in data:
            log.warning(f"    API error: {data.get('message', data.get('errorCode', ''))}")
            return []
        days = data.get("days", [])
    except (json.JSONDecodeError, KeyError, AttributeError):
        days = []

    # Cache results
    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(days, f, ensure_ascii=True, indent=2)

    return days


def main():
    parser = argparse.ArgumentParser(
        description="Script 112 -- Visual Crossing Weather Scraper for hippodromes")
    parser.add_argument("--start", type=str, default="2023-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD)")
    parser.add_argument("--api-key", type=str, default="",
                        help="Visual Crossing API key")
    parser.add_argument("--api-key-file", type=str, default="",
                        help="File containing Visual Crossing API key")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--chunk-days", type=int, default=30,
                        help="Nombre de jours par requete (max 365, defaut 30 pour free tier)")
    parser.add_argument("--max-requests", type=int, default=MAX_REQUESTS_PER_DAY,
                        help="Nombre max de requetes par execution (free tier: 1000/jour)")
    parser.add_argument("--country", type=str, default="",
                        help="Filtrer par pays (ex: france, royaume-uni)")
    args = parser.parse_args()

    # Load API key
    api_key = args.api_key
    if not api_key and args.api_key_file:
        if os.path.exists(args.api_key_file):
            with open(args.api_key_file, "r", encoding="utf-8", errors="replace") as f:
                api_key = f.read().strip()
    if not api_key:
        api_key = os.environ.get("VISUAL_CROSSING_API_KEY", "")
    if not api_key:
        log.error("  Pas de cle API Visual Crossing.")
        log.error("  Obtenez-en une gratuitement sur https://www.visualcrossing.com/sign-up")
        log.error("  Usage: --api-key YOUR_KEY ou VISUAL_CROSSING_API_KEY env var")
        sys.exit(1)

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    # Build hippodromes list from DB
    all_hippos = _build_hippodromes_list()
    if args.country:
        country_filter = args.country.lower().strip()
        all_hippos = [(n, la, lo, p) for n, la, lo, p in all_hippos
                      if country_filter in p.lower()]

    log.info("=" * 60)
    log.info("SCRIPT 112 -- Visual Crossing Weather Scraper")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Hippodromes : {len(all_hippos)}")
    log.info(f"  Chunk : {args.chunk_days} jours par requete")
    log.info(f"  Max requetes : {args.max_requests}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    done_hippos = set(checkpoint.get("done_hippos", []))
    request_count = checkpoint.get("request_count", 0)
    if args.resume and done_hippos:
        log.info(f"  Reprise checkpoint: {len(done_hippos)} hippodromes deja traites")

    session = _make_session()
    output_file = os.path.join(OUTPUT_DIR, "visual_crossing_data.jsonl")

    total_records = 0
    hippo_count = 0

    for hippo_name, lat, lon, pays in all_hippos:
        if hippo_name in done_hippos:
            continue

        if request_count >= args.max_requests:
            log.warning(f"  Limite de {args.max_requests} requetes atteinte. "
                        f"Relancez demain pour continuer.")
            break

        log.info(f"  Hippodrome: {hippo_name} ({lat}, {lon}) [{pays}]")

        # Fetch in chunks
        chunk_days = min(args.chunk_days, 365)
        current = start_date
        hippo_records = 0

        while current <= end_date:
            if request_count >= args.max_requests:
                log.warning(f"  Limite de requetes atteinte pendant {hippo_name}")
                break

            chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
            s_str = current.strftime("%Y-%m-%d")
            e_str = chunk_end.strftime("%Y-%m-%d")

            days_data = fetch_weather_period(session, lat, lon, s_str, e_str, api_key)
            request_count += 1

            if days_data:
                for obs in days_data:
                    record = {
                        "source": "visual_crossing",
                        "type": "daily",
                        "hippodrome": hippo_name,
                        "lat": lat,
                        "lon": lon,
                        "pays": pays,
                        "date": obs.get("datetime", ""),
                        "tempmax": obs.get("tempmax"),
                        "tempmin": obs.get("tempmin"),
                        "temp": obs.get("temp"),
                        "feelslikemax": obs.get("feelslikemax"),
                        "feelslikemin": obs.get("feelslikemin"),
                        "feelslike": obs.get("feelslike"),
                        "dew": obs.get("dew"),
                        "humidity": obs.get("humidity"),
                        "precip": obs.get("precip"),
                        "precipprob": obs.get("precipprob"),
                        "precipcover": obs.get("precipcover"),
                        "preciptype": obs.get("preciptype"),
                        "snow": obs.get("snow"),
                        "snowdepth": obs.get("snowdepth"),
                        "windgust": obs.get("windgust"),
                        "windspeed": obs.get("windspeed"),
                        "winddir": obs.get("winddir"),
                        "pressure": obs.get("pressure"),
                        "cloudcover": obs.get("cloudcover"),
                        "visibility": obs.get("visibility"),
                        "solarradiation": obs.get("solarradiation"),
                        "solarenergy": obs.get("solarenergy"),
                        "uvindex": obs.get("uvindex"),
                        "conditions": obs.get("conditions"),
                        "description": obs.get("description"),
                        "icon": obs.get("icon"),
                        "scraped_at": datetime.now().isoformat(),
                    }
                    append_jsonl(output_file, record)
                    total_records += 1
                    hippo_records += 1

            # Respectful rate limiting for free tier
            smart_pause(1.5, 0.5)

            current = chunk_end + timedelta(days=1)

        done_hippos.add(hippo_name)
        hippo_count += 1

        log.info(f"    {hippo_name} termine: {hippo_records} jours, "
                 f"{total_records} records total, {request_count} requetes")
        save_checkpoint(CHECKPOINT_FILE, {
            "done_hippos": list(done_hippos),
            "total_records": total_records,
            "request_count": request_count,
        })

        # Rotate session periodically
        if hippo_count % 20 == 0:
            session.close()
            session = _make_session()
            time.sleep(random.uniform(2, 5))

    save_checkpoint(CHECKPOINT_FILE, {
        "done_hippos": list(done_hippos),
        "total_records": total_records,
        "request_count": request_count,
        "status": "done" if request_count < args.max_requests else "quota_reached",
    })

    log.info("=" * 60)
    log.info(f"TERMINE: {hippo_count} hippodromes, {total_records} records")
    log.info(f"  Requetes utilisees: {request_count}/{args.max_requests}")
    log.info(f"  Output: {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
