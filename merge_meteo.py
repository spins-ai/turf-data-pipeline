#!/usr/bin/env python3
"""
merge_meteo.py — Consolidate all météo sources into one comprehensive file.

Sources (priority order):
1. Météo France API (meteo_france_hippodromes.json) — paid data, highest quality
2. Reunions enrichies (reunions_enrichies.json) — PMU official weather from race day
3. Meteo historique (meteo_historique.json) — Open-Meteo processed, keyed by course_uid
4. Open-Meteo cache (8,589 hourly files) — raw Open-Meteo archive
5. NASA POWER (reunions_normalisees_meteo.parquet) — satellite data, global coverage
6. NASA POWER cache (nasa_meteo_cache.json) — raw NASA cache

Output: output/meteo_complete/meteo_complete.json
        output/meteo_complete/meteo_complete.parquet
        output/meteo_complete/meteo_complete.csv
"""

import json
import os
import sys
import glob
from collections import defaultdict
from pathlib import Path

import pandas as pd
import numpy as np

OUTPUT_DIR = os.path.join("output", "meteo_complete")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_hippo(name):
    """Normalize hippodrome name for matching."""
    if not name:
        return ""
    name = str(name).lower().strip()
    # Common replacements
    replacements = {
        "le lion d'angers": "le lion dangers",
        "le lion-d'angers": "le lion dangers",
        "le croise-laroche": "le croise laroche",
        "aix-les-bains": "aix les bains",
        "saint-cloud": "saint cloud",
        "cagnes-sur-mer": "cagnes sur mer",
        "bordeaux-le bouscat": "bordeaux le bouscat",
        "la roche-posay": "la roche posay",
        "pont-de-vivaux": "pont de vivaux",
        "salon-de-provence": "salon de provence",
        "le mont-saint-michel": "le mont saint michel",
    }
    for old, new in replacements.items():
        if name == old:
            return new
    # General normalization
    name = name.replace("-", " ").replace("_", " ").replace("'", " ").replace("  ", " ")
    return name.strip()


def load_courses():
    """Load the master list of 257,806 courses."""
    print("[1/7] Loading master courses list...")
    path = os.path.join("output", "02_liste_courses", "courses_normalisees.json")
    with open(path) as f:
        courses = json.load(f)
    print(f"  -> {len(courses)} courses loaded")
    return courses


def load_meteo_france():
    """Load Météo France data — keyed by (hippodrome_normalized, date)."""
    print("[2/7] Loading Météo France data...")
    path = os.path.join("output", "35_meteo_france", "meteo_france_hippodromes.json")
    with open(path) as f:
        data = json.load(f)

    meteo_france = {}
    total_entries = 0
    for rec in data:
        hippo = normalize_hippo(rec["hippodrome"])
        daily = rec.get("daily", {})
        for date_str, vals in daily.items():
            # Only keep entries with actual data
            if any(v is not None for k, v in vals.items() if k != "precip_hours"):
                meteo_france[(hippo, date_str)] = {
                    "temperature_c": vals.get("temp_mean"),
                    "temp_min_c": vals.get("temp_min"),
                    "temp_max_c": vals.get("temp_max"),
                    "precipitation_mm": vals.get("precipitation"),
                    "precip_total_mm": vals.get("rain"),
                    "wind_speed_kmh": vals.get("wind_max"),
                    "wind_gusts_kmh": vals.get("wind_gusts"),
                    "humidity_pct": None,
                    "weather_code": None,
                    "weather_description": None,
                    "source": "meteo_france",
                }
                total_entries += 1

    print(f"  -> {total_entries} date-hippodrome entries with data")
    return meteo_france


def load_reunions_enrichies():
    """Load reunions enrichies — keyed by alt_key (date_R_C) with mapping to course_uid."""
    print("[3/7] Loading reunions enrichies (PMU official weather)...")
    path = os.path.join("output", "39_reunions_enrichies", "reunions_enrichies.json")
    with open(path) as f:
        data = json.load(f)

    # Build alt_key -> meteo mapping
    reunions_meteo = {}
    has_data = 0
    for rec in data:
        alt_key = rec["course_uid"]  # format: 2013-08-26_R1_C1
        temp = rec.get("meteo_temperature")
        vent = rec.get("meteo_force_vent")
        if temp is not None or vent is not None:
            reunions_meteo[alt_key] = {
                "temperature_c": temp,
                "wind_speed_kmh": vent,
                "meteo_direction_vent": rec.get("meteo_direction_vent"),
                "meteo_nebulosite": rec.get("meteo_nebulosite"),
                "source": "reunions_enrichies_pmu",
            }
            has_data += 1

    print(f"  -> {has_data} courses with PMU official weather")
    return reunions_meteo


def load_meteo_historique():
    """Load existing meteo_historique.json — keyed by course_uid (hex hash)."""
    print("[4/7] Loading meteo_historique.json...")
    path = os.path.join("output", "13_meteo_historique", "meteo_historique.json")
    with open(path) as f:
        data = json.load(f)

    meteo_hist = {}
    for rec in data:
        meteo_hist[rec["course_uid"]] = {
            "temperature_c": rec.get("temperature_c"),
            "humidity_pct": rec.get("humidity_pct"),
            "precipitation_mm": rec.get("precipitation_mm"),
            "wind_speed_kmh": rec.get("wind_speed_kmh"),
            "wind_gusts_kmh": rec.get("wind_gusts_kmh"),
            "weather_code": rec.get("weather_code"),
            "weather_description": rec.get("weather_description"),
            "temp_min_c": rec.get("temp_min_c"),
            "temp_max_c": rec.get("temp_max_c"),
            "precip_total_mm": rec.get("precip_total_mm"),
            "wind_max_kmh": rec.get("wind_max_kmh"),
            "is_rainy": rec.get("is_rainy"),
            "is_windy": rec.get("is_windy"),
            "is_hot": rec.get("is_hot"),
            "is_cold": rec.get("is_cold"),
            "source": "open_meteo_historique",
        }

    print(f"  -> {len(meteo_hist)} courses with meteo_historique data")
    return meteo_hist


def load_open_meteo_cache():
    """Load all Open-Meteo cache files — keyed by (hippodrome, date)."""
    print("[5/7] Loading Open-Meteo cache files...")
    cache_dir = os.path.join("output", "13_meteo_historique", "cache")
    files = glob.glob(os.path.join(cache_dir, "*.json"))
    print(f"  -> Found {len(files)} cache files")

    cache = {}
    errors = 0
    for fpath in files:
        fname = os.path.basename(fpath).replace(".json", "")
        # Format: 2016-05-01_alencon
        parts = fname.split("_", 1)
        if len(parts) != 2:
            # Try splitting on date pattern
            if len(fname) >= 10 and fname[4] == "-" and fname[7] == "-":
                date_str = fname[:10]
                hippo = fname[11:] if len(fname) > 11 else ""
            else:
                errors += 1
                continue
        else:
            date_str = parts[0]
            hippo = parts[1]

        hippo = normalize_hippo(hippo)

        try:
            with open(fpath) as f:
                data = json.load(f)
        except Exception:
            errors += 1
            continue

        hourly = data.get("hourly", {})
        temps = hourly.get("temperature_2m", [])
        humids = hourly.get("relative_humidity_2m", [])
        precips = hourly.get("precipitation", [])
        winds = hourly.get("wind_speed_10m", [])
        gusts = hourly.get("wind_gusts_10m", [])
        codes = hourly.get("weather_code", [])

        def safe_mean(lst):
            vals = [v for v in lst if v is not None]
            return round(sum(vals) / len(vals), 1) if vals else None

        def safe_max(lst):
            vals = [v for v in lst if v is not None]
            return max(vals) if vals else None

        def safe_min(lst):
            vals = [v for v in lst if v is not None]
            return min(vals) if vals else None

        def safe_sum(lst):
            vals = [v for v in lst if v is not None]
            return round(sum(vals), 1) if vals else None

        cache[(hippo, date_str)] = {
            "temperature_c": safe_mean(temps),
            "temp_min_c": safe_min(temps),
            "temp_max_c": safe_max(temps),
            "humidity_pct": safe_mean(humids),
            "precipitation_mm": safe_sum(precips),
            "wind_speed_kmh": safe_mean(winds),
            "wind_gusts_kmh": safe_max(gusts),
            "weather_code": None,  # Most are null
            "source": "open_meteo_cache",
        }

    if errors:
        print(f"  -> {errors} files with errors (skipped)")
    print(f"  -> {len(cache)} date-hippodrome entries loaded from cache")
    return cache


def load_nasa_data():
    """Load NASA POWER data from reunions_normalisees_meteo.parquet — keyed by reunion_uid."""
    print("[6/7] Loading NASA POWER data (from reunions_normalisees_meteo)...")
    path = os.path.join("output", "01_calendrier_reunions", "reunions_normalisees_meteo.parquet")
    df = pd.read_parquet(path)

    # Build reunion_uid -> meteo mapping
    nasa = {}
    for _, row in df.iterrows():
        if pd.isna(row.get("meteo_source")) or row["meteo_source"] != "nasa-power":
            continue
        nasa[row["reunion_uid"]] = {
            "temperature_c": row.get("meteo_temperature_moyenne") if pd.notna(row.get("meteo_temperature_moyenne")) else None,
            "temp_min_c": row.get("meteo_temperature_min") if pd.notna(row.get("meteo_temperature_min")) else None,
            "temp_max_c": row.get("meteo_temperature_max") if pd.notna(row.get("meteo_temperature_max")) else None,
            "precipitation_mm": row.get("meteo_precipitation_mm") if pd.notna(row.get("meteo_precipitation_mm")) else None,
            "precip_total_mm": row.get("meteo_pluie_mm") if pd.notna(row.get("meteo_pluie_mm")) else None,
            "wind_speed_kmh": row.get("meteo_vent_max_kmh") if pd.notna(row.get("meteo_vent_max_kmh")) else None,
            "wind_gusts_kmh": row.get("meteo_rafales_max_kmh") if pd.notna(row.get("meteo_rafales_max_kmh")) else None,
            "humidity_pct": row.get("meteo_humidite_pct") if pd.notna(row.get("meteo_humidite_pct")) else None,
            "weather_code": row.get("meteo_code_wmo") if pd.notna(row.get("meteo_code_wmo")) else None,
            "weather_description": row.get("meteo_description") if pd.notna(row.get("meteo_description")) else None,
            "source": "nasa_power",
        }
        # Also index by (hippo_normalise, date) for fallback
        hippo = normalize_hippo(row.get("hippodrome_normalise", ""))
        date_str = str(row.get("date_reunion_iso", ""))[:10]
        if hippo and date_str:
            nasa_key = ("nasa_geo", hippo, date_str)
            nasa[nasa_key] = nasa[row["reunion_uid"]]

    print(f"  -> {len([k for k in nasa if not isinstance(k, tuple)])} reunions with NASA data")
    return nasa


def compute_flags(rec):
    """Compute boolean weather flags from numeric data."""
    temp = rec.get("temperature_c")
    wind = rec.get("wind_speed_kmh")
    precip = rec.get("precipitation_mm") or rec.get("precip_total_mm")

    rec["is_rainy"] = bool(precip and precip > 0.5) if precip is not None else None
    rec["is_windy"] = bool(wind and wind > 30) if wind is not None else None
    rec["is_hot"] = bool(temp and temp > 30) if temp is not None else None
    rec["is_cold"] = bool(temp and temp < 5) if temp is not None else None
    return rec


def merge_all():
    """Main merge logic."""
    courses = load_courses()
    meteo_france = load_meteo_france()
    reunions_meteo = load_reunions_enrichies()
    meteo_hist = load_meteo_historique()
    open_meteo_cache = load_open_meteo_cache()
    nasa_data = load_nasa_data()

    print("\n[7/7] Merging all sources...")

    # Build alt_key mapping: course_uid -> alt_key (date_R_C)
    uid_to_alt = {}
    for c in courses:
        alt_key = f"{c['date_reunion_iso']}_R{c['numero_reunion']}_C{c['numero_course']}"
        uid_to_alt[c["course_uid"]] = alt_key

    results = []
    source_counts = defaultdict(int)
    no_meteo = 0

    for c in courses:
        course_uid = c["course_uid"]
        reunion_uid = c.get("reunion_uid", "")
        date_str = str(c.get("date_reunion_iso", ""))[:10]
        hippo = normalize_hippo(c.get("hippodrome_normalise", ""))
        alt_key = uid_to_alt.get(course_uid, "")

        meteo = None

        # Priority 1: Météo France (highest quality paid data)
        key_mf = (hippo, date_str)
        if key_mf in meteo_france:
            meteo = meteo_france[key_mf].copy()
        else:
            # Try with original hippodrome name
            hippo_orig = normalize_hippo(c.get("hippodrome", ""))
            if (hippo_orig, date_str) in meteo_france:
                meteo = meteo_france[(hippo_orig, date_str)].copy()

        # Priority 2: Reunions enrichies (PMU official, has temperature + wind)
        if meteo is None and alt_key in reunions_meteo:
            re = reunions_meteo[alt_key]
            meteo = {
                "temperature_c": re.get("temperature_c"),
                "temp_min_c": None,
                "temp_max_c": None,
                "humidity_pct": None,
                "precipitation_mm": None,
                "wind_speed_kmh": re.get("wind_speed_kmh"),
                "wind_gusts_kmh": None,
                "weather_code": None,
                "weather_description": re.get("meteo_nebulosite"),
                "source": "reunions_enrichies_pmu",
            }

        # Priority 3: Existing meteo_historique (Open-Meteo processed)
        if meteo is None and course_uid in meteo_hist:
            meteo = meteo_hist[course_uid].copy()

        # Priority 4: Open-Meteo cache (raw hourly aggregated)
        if meteo is None and (hippo, date_str) in open_meteo_cache:
            meteo = open_meteo_cache[(hippo, date_str)].copy()

        # Priority 5: NASA POWER (satellite, lower resolution)
        if meteo is None:
            if reunion_uid in nasa_data:
                meteo = nasa_data[reunion_uid].copy()
            elif ("nasa_geo", hippo, date_str) in nasa_data:
                meteo = nasa_data[("nasa_geo", hippo, date_str)].copy()

        # Enrichment: if we have meteo but missing fields, try to fill from lower-priority sources
        if meteo is not None:
            src = meteo["source"]

            # Fill gaps from lower priority sources
            fill_sources = []
            if src != "reunions_enrichies_pmu" and alt_key in reunions_meteo:
                fill_sources.append(reunions_meteo[alt_key])
            if src != "open_meteo_historique" and course_uid in meteo_hist:
                fill_sources.append(meteo_hist[course_uid])
            if src != "open_meteo_cache" and (hippo, date_str) in open_meteo_cache:
                fill_sources.append(open_meteo_cache[(hippo, date_str)])
            if src != "nasa_power":
                if reunion_uid in nasa_data:
                    fill_sources.append(nasa_data[reunion_uid])

            for fill in fill_sources:
                for field in ["temperature_c", "temp_min_c", "temp_max_c", "humidity_pct",
                              "precipitation_mm", "precip_total_mm", "wind_speed_kmh",
                              "wind_gusts_kmh", "weather_code", "weather_description"]:
                    if meteo.get(field) is None and fill.get(field) is not None:
                        meteo[field] = fill[field]

        # Build output record
        if meteo is not None:
            source_counts[meteo["source"]] += 1
            meteo = compute_flags(meteo)
        else:
            no_meteo += 1
            meteo = {
                "temperature_c": None,
                "temp_min_c": None,
                "temp_max_c": None,
                "humidity_pct": None,
                "precipitation_mm": None,
                "precip_total_mm": None,
                "wind_speed_kmh": None,
                "wind_gusts_kmh": None,
                "weather_code": None,
                "weather_description": None,
                "is_rainy": None,
                "is_windy": None,
                "is_hot": None,
                "is_cold": None,
                "source": None,
            }

        record = {
            "course_uid": course_uid,
            "reunion_uid": reunion_uid,
            "date_reunion_iso": date_str,
            "hippodrome_normalise": c.get("hippodrome_normalise", ""),
        }
        record.update(meteo)
        results.append(record)

    # === Output ===
    total = len(results)
    with_meteo = total - no_meteo
    pct = 100 * with_meteo / total if total > 0 else 0

    print("\n" + "=" * 60)
    print("METEO MERGE RESULTS")
    print("=" * 60)
    print(f"Total courses:          {total:,}")
    print(f"With météo data:        {with_meteo:,} ({pct:.1f}%)")
    print(f"Without météo data:     {no_meteo:,} ({100 - pct:.1f}%)")
    print()
    print("Source breakdown (primary source):")
    for src, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"  {src:30s}: {count:>8,} ({100 * count / total:.1f}%)")
    print("=" * 60)

    # Save JSON
    print("\nSaving JSON...")
    json_path = os.path.join(OUTPUT_DIR, "meteo_complete.json")
    with open(json_path, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=None)
    print(f"  -> {json_path} ({os.path.getsize(json_path) / 1e6:.1f} MB)")

    # Save Parquet
    print("Saving Parquet...")
    df = pd.DataFrame(results)
    parquet_path = os.path.join(OUTPUT_DIR, "meteo_complete.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"  -> {parquet_path} ({os.path.getsize(parquet_path) / 1e6:.1f} MB)")

    # Save CSV
    print("Saving CSV...")
    csv_path = os.path.join(OUTPUT_DIR, "meteo_complete.csv")
    df.to_csv(csv_path, index=False)
    print(f"  -> {csv_path} ({os.path.getsize(csv_path) / 1e6:.1f} MB)")

    print("\nDone!")
    return results


if __name__ == "__main__":
    merge_all()
