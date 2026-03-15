"""Fetch missing meteo data from Open-Meteo Archive API for all hippodromes."""
import json
import os
import time
import requests
import importlib.util

OUTPUT_DIR = "output/13_meteo_historique/cache"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load hippodromes DB
spec = importlib.util.spec_from_file_location('hippo', 'hippodromes_db.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
db = mod.HIPPODROMES_DB

# Load courses to know which hippodromes need meteo
print("Loading courses...")
hippo_dates = {}
with open('output/02_liste_courses/courses_normalisees.json', 'rb') as f:
    f.read(1)
    buf = []
    depth = 0
    while True:
        chunk = f.read(131072)
        if not chunk:
            break
        for c in chunk.decode('utf-8', errors='replace'):
            if c == '{':
                depth += 1
            if depth > 0:
                buf.append(c)
            if c == '}':
                depth -= 1
                if depth == 0:
                    try:
                        obj = json.loads(''.join(buf))
                        h = obj.get('hippodrome_normalise', '')
                        d = obj.get('date_reunion_iso', '')
                        if h and d:
                            if h not in hippo_dates:
                                hippo_dates[h] = set()
                            hippo_dates[h].add(d)
                    except:
                        pass
                    buf = []

print(f"Hippodromes with courses: {len(hippo_dates)}")

# Check existing cache
existing = set()
for f in os.listdir(OUTPUT_DIR):
    if f.endswith('.json'):
        existing.add(f.replace('.json', ''))

print(f"Existing cache files: {len(existing)}")

# Find missing
to_fetch = {}  # hippo -> {lat, lon, dates}
for h, dates in hippo_dates.items():
    key = h.lower().replace('-', ' ').replace('_', ' ')
    # Find coords
    coords = None
    if key in db and 'lat' in db[key]:
        coords = (db[key]['lat'], db[key]['lon'])
    else:
        # Try partial match
        for dbkey, dbval in db.items():
            if dbkey in key or key in dbkey:
                if 'lat' in dbval:
                    coords = (dbval['lat'], dbval['lon'])
                    break

    if not coords:
        continue

    # Check which dates are missing from cache
    missing_dates = set()
    for d in dates:
        cache_key = f"{h}_{d}"
        if cache_key not in existing:
            missing_dates.add(d)

    if missing_dates:
        to_fetch[h] = {
            'lat': coords[0],
            'lon': coords[1],
            'dates': sorted(missing_dates)
        }

total_missing = sum(len(v['dates']) for v in to_fetch.values())
print(f"Hippodromes to fetch: {len(to_fetch)}")
print(f"Total missing date-hippo pairs: {total_missing}")

# Fetch by hippodrome, yearly chunks
fetched = 0
errors = 0

for i, (h, info) in enumerate(sorted(to_fetch.items())):
    lat, lon = info['lat'], info['lon']
    dates = info['dates']

    # Group dates by year
    years = {}
    for d in dates:
        y = d[:4]
        if y not in years:
            years[y] = []
        years[y].append(d)

    for year, year_dates in sorted(years.items()):
        start = f"{year}-01-01"
        end = f"{year}-12-31"
        if int(year) >= 2026:
            end = "2026-03-12"

        try:
            url = (
                f"https://archive-api.open-meteo.com/v1/archive?"
                f"latitude={lat}&longitude={lon}"
                f"&start_date={start}&end_date={end}"
                f"&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
                f"precipitation_sum,precipitation_hours,rain_sum,"
                f"windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,"
                f"shortwave_radiation_sum,et0_fao_evapotranspiration"
                f"&timezone=auto"
            )
            resp = requests.get(url, timeout=30)
            data = resp.json()

            if 'daily' not in data:
                print(f"  [{i+1}/{len(to_fetch)}] {h} {year}: no daily data")
                errors += 1
                time.sleep(1.2)
                continue

            daily = data['daily']
            time_list = daily.get('time', [])

            # Save one cache file per date that we need
            for j, date_str in enumerate(time_list):
                if date_str in year_dates:
                    cache_entry = {
                        'temperature_c': daily.get('temperature_2m_mean', [None])[j] if j < len(daily.get('temperature_2m_mean', [])) else None,
                        'temp_max_c': daily.get('temperature_2m_max', [None])[j] if j < len(daily.get('temperature_2m_max', [])) else None,
                        'temp_min_c': daily.get('temperature_2m_min', [None])[j] if j < len(daily.get('temperature_2m_min', [])) else None,
                        'precipitation_mm': daily.get('precipitation_sum', [None])[j] if j < len(daily.get('precipitation_sum', [])) else None,
                        'precip_hours': daily.get('precipitation_hours', [None])[j] if j < len(daily.get('precipitation_hours', [])) else None,
                        'rain_mm': daily.get('rain_sum', [None])[j] if j < len(daily.get('rain_sum', [])) else None,
                        'wind_max_kmh': daily.get('windspeed_10m_max', [None])[j] if j < len(daily.get('windspeed_10m_max', [])) else None,
                        'wind_gusts_kmh': daily.get('windgusts_10m_max', [None])[j] if j < len(daily.get('windgusts_10m_max', [])) else None,
                        'wind_dir': daily.get('winddirection_10m_dominant', [None])[j] if j < len(daily.get('winddirection_10m_dominant', [])) else None,
                        'radiation': daily.get('shortwave_radiation_sum', [None])[j] if j < len(daily.get('shortwave_radiation_sum', [])) else None,
                        'evapotranspiration': daily.get('et0_fao_evapotranspiration', [None])[j] if j < len(daily.get('et0_fao_evapotranspiration', [])) else None,
                        'hippodrome': h,
                        'date': date_str,
                        'lat': lat,
                        'lon': lon,
                        'source': 'open_meteo_archive'
                    }

                    cache_file = os.path.join(OUTPUT_DIR, f"{h}_{date_str}.json")
                    with open(cache_file, 'w') as cf:
                        json.dump(cache_entry, cf, ensure_ascii=False)
                    fetched += 1

            print(f"  [{i+1}/{len(to_fetch)}] {h} {year}: {len(year_dates)} dates OK")

        except Exception as e:
            print(f"  [{i+1}/{len(to_fetch)}] {h} {year}: ERROR {e}")
            errors += 1

        time.sleep(1.2)

print(f"\n=== DONE ===")
print(f"Fetched: {fetched} cache files")
print(f"Errors: {errors}")
