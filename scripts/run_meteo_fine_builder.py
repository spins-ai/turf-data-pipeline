#!/usr/bin/env python3
"""
run_meteo_fine_builder.py - D1: Meteorologie fine par hippodrome
=================================================================
Collecte meteo horaire via API OpenMeteo Archive (gratuite, sans cle).
8 features par course: precip 3h/12h/24h/48h, temp, humidity, vent, rafales.

Usage:
    python scripts/run_meteo_fine_builder.py
    python scripts/run_meteo_fine_builder.py --max-calls 500
"""

from __future__ import annotations

import gc
import json
import sys
import time
import hashlib
import argparse
from pathlib import Path
from collections import defaultdict

import duckdb
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from utils.hippodromes_db import HIPPODROMES_DB

METEO_MASTER = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/meteo_master.parquet")
PARTANTS_MASTER = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
CACHE_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/meteo_fine_cache")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/meteofine_x")

OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"
PAUSE = 0.35


def get_hippo_coords(name: str) -> tuple[float, float] | None:
    key = name.strip().lower()
    if key in HIPPODROMES_DB:
        h = HIPPODROMES_DB[key]
        return h.get("lat"), h.get("lon")
    for k, v in HIPPODROMES_DB.items():
        if key in k or k in key:
            return v.get("lat"), v.get("lon")
    return None


def fetch_hourly(lat: float, lon: float, date_start: str, date_end: str) -> dict | None:
    cache_key = hashlib.md5(f"{lat:.3f}_{lon:.3f}_{date_start}_{date_end}".encode()).hexdigest()
    cache_file = CACHE_DIR / f"{cache_key}.json"
    if cache_file.exists():
        with open(cache_file, "r") as f:
            return json.load(f)
    try:
        resp = requests.get(OPENMETEO_URL, params={
            "latitude": lat, "longitude": lon,
            "start_date": date_start, "end_date": date_end,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_gusts_10m",
            "timezone": "Europe/Paris",
        }, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(cache_file, "w") as f:
                json.dump(data, f)
            return data
        return None
    except Exception:
        return None


def extract_features(hourly_data: dict, target_date: str, target_hour: int = 14) -> dict:
    hr = hourly_data.get("hourly", {})
    times = hr.get("time", [])
    temps = hr.get("temperature_2m", [])
    humidity = hr.get("relative_humidity_2m", [])
    precip = hr.get("precipitation", [])
    wind = hr.get("wind_speed_10m", [])
    gusts = hr.get("wind_gusts_10m", [])

    target_str = f"{target_date}T{target_hour:02d}:00"
    try:
        idx = times.index(target_str)
    except ValueError:
        idx = None
        for i, t in enumerate(times):
            if t.startswith(target_date):
                idx = i + target_hour
                break
        if idx is None or idx >= len(times):
            return {}

    def safe(arr, i):
        return arr[i] if i < len(arr) and arr[i] is not None else None

    def sum_precip(hours_back):
        s = max(0, idx - hours_back)
        vals = [p for p in precip[s:idx + 1] if p is not None]
        return round(sum(vals), 2) if vals else None

    return {
        "meteofine_x__temp_depart": safe(temps, idx),
        "meteofine_x__humidity_depart": safe(humidity, idx),
        "meteofine_x__wind_speed_depart": safe(wind, idx),
        "meteofine_x__wind_gusts_depart": safe(gusts, idx),
        "meteofine_x__precip_3h_avant": sum_precip(3),
        "meteofine_x__precip_12h_avant": sum_precip(12),
        "meteofine_x__precip_24h_avant": sum_precip(24),
        "meteofine_x__precip_cumul_48h": sum_precip(48),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-calls", type=int, default=10000)
    args = parser.parse_args()

    start = time.time()
    print("=" * 70, flush=True)
    print("  D1: METEOROLOGIE FINE PAR HIPPODROME (OpenMeteo Archive)", flush=True)
    print("=" * 70, flush=True)

    # Phase 1: Get unique (hippodrome, month) pairs using DuckDB
    print("\nPhase 1: Index des courses (DuckDB)...", flush=True)
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT course_uid,
               LOWER(TRIM(hippodrome_normalise)) as hippo,
               CAST(date_reunion_iso AS VARCHAR) as date_str,
               CAST(heure_depart AS VARCHAR) as heure
        FROM read_parquet('{METEO_MASTER}')
        WHERE hippodrome_normalise IS NOT NULL
    """).fetchall()
    print(f"  {len(rows):,} courses lues", flush=True)

    # Build index: (hippo, month) -> [(uid, date, heure)]
    hippo_months = defaultdict(list)
    for uid, hippo, date_str, heure in rows:
        date = str(date_str)[:10]
        if len(date) == 10:
            month = date[:7]
            h = str(heure)[:2] if heure else "14"
            hippo_months[(hippo, month)].append((uid, date, h))
    del rows
    print(f"  {len(hippo_months):,} couples (hippodrome, mois)", flush=True)

    # Phase 2: GPS resolution
    print("\nPhase 2: Resolution GPS...", flush=True)
    all_hippos = set(h for h, _ in hippo_months.keys())
    gps = {}
    missing = []
    for h in all_hippos:
        c = get_hippo_coords(h)
        if c and c[0] and c[1]:
            gps[h] = c
        else:
            missing.append(h)
    print(f"  {len(gps)} avec GPS, {len(missing)} sans", flush=True)
    if missing:
        print(f"  Sans GPS: {missing[:8]}...", flush=True)

    # Phase 3: Fetch meteo
    keys_with_gps = [(h, m) for h, m in hippo_months.keys() if h in gps]
    print(f"\nPhase 3: Collecte meteo ({len(keys_with_gps)} appels max)...", flush=True)

    api_calls = 0
    cache_hits = 0
    course_features = {}  # uid -> features

    for i, (hippo, month) in enumerate(sorted(keys_with_gps)):
        if api_calls >= args.max_calls:
            print(f"  Max calls ({args.max_calls}) atteint!", flush=True)
            break

        lat, lon = gps[hippo]
        year, m = month.split("-")
        date_start = f"{month}-01"
        if int(m) == 12:
            date_end = f"{int(year)+1}-01-02"
        else:
            date_end = f"{year}-{int(m)+1:02d}-02"

        # Check cache
        ck = hashlib.md5(f"{lat:.3f}_{lon:.3f}_{date_start}_{date_end}".encode()).hexdigest()
        is_cached = (CACHE_DIR / f"{ck}.json").exists()

        data = fetch_hourly(lat, lon, date_start, date_end)

        if is_cached:
            cache_hits += 1
        else:
            api_calls += 1
            time.sleep(PAUSE)

        if data and "hourly" in data:
            for uid, date, heure in hippo_months[(hippo, month)]:
                try:
                    hour = int(heure) if heure and heure.isdigit() else 14
                except ValueError:
                    hour = 14
                feats = extract_features(data, date, hour)
                if feats:
                    course_features[uid] = feats

        if (i + 1) % 100 == 0:
            elapsed = time.time() - start
            print(f"  [{i+1}/{len(keys_with_gps)}] api={api_calls} cache={cache_hits} courses={len(course_features)} | {elapsed:.0f}s", flush=True)

    elapsed = time.time() - start
    print(f"\n  API: {api_calls} calls, {cache_hits} cache hits", flush=True)
    print(f"  Courses avec meteo fine: {len(course_features):,}", flush=True)

    # Phase 4: Map to partants via DuckDB
    print(f"\nPhase 4: Assignation aux partants (DuckDB)...", flush=True)
    partant_courses = con.execute(f"""
        SELECT partant_uid, course_uid
        FROM read_parquet('{PARTANTS_MASTER}', columns=['partant_uid', 'course_uid'])
    """).fetchall()
    print(f"  {len(partant_courses):,} partants lus", flush=True)
    con.close()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "meteofine_x_features.jsonl"
    matched = 0

    with open(out_path, "w", encoding="utf-8", newline="\n") as fout:
        for j, (puid, cuid) in enumerate(partant_courses):
            rec = {"partant_uid": puid}
            feats = course_features.get(cuid, {})
            if feats:
                matched += 1
                rec.update(feats)
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if (j + 1) % 500000 == 0:
                print(f"  {j+1:,} partants ecrits, {matched:,} matched", flush=True)

    elapsed = time.time() - start
    print(f"\n{'='*70}", flush=True)
    print(f"  TERMINE en {elapsed:.0f}s", flush=True)
    print(f"  API calls: {api_calls} | Cache: {cache_hits}", flush=True)
    print(f"  Courses avec meteo fine: {len(course_features):,}", flush=True)
    print(f"  Partants: {matched:,} / {len(partant_courses):,} ({matched*100/max(len(partant_courses),1):.1f}%)", flush=True)
    if out_path.exists():
        print(f"  Output: {out_path} ({out_path.stat().st_size/1024/1024:.0f} Mo)", flush=True)
    print(f"{'='*70}", flush=True)


if __name__ == "__main__":
    main()
