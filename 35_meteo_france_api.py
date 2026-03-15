#!/usr/bin/env python3
"""
Script 35 — Météo-France API via Open-Meteo (AROME 1.5km)
Source : open-meteo.com/en/docs/meteofrance-api + archive-api.open-meteo.com
CRITIQUE pour : Terrain précis par hippodrome, Conditions piste
Remplace/complète le script 13 avec données plus précises
"""

import requests
import json
import time
import os
import logging
from datetime import datetime, timedelta

OUTPUT_DIR = "output/35_meteo_france"
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

# Coordonnées GPS des principaux hippodromes français
HIPPODROMES = {
    "LONGCHAMP": {"lat": 48.8569, "lon": 2.2347},
    "CHANTILLY": {"lat": 49.1938, "lon": 2.4710},
    "DEAUVILLE": {"lat": 49.3583, "lon": 0.0606},
    "SAINT-CLOUD": {"lat": 48.8431, "lon": 2.2067},
    "AUTEUIL": {"lat": 48.8518, "lon": 2.2530},
    "VINCENNES": {"lat": 48.8340, "lon": 2.4350},
    "ENGHIEN": {"lat": 48.9760, "lon": 2.3040},
    "MAISONS-LAFFITTE": {"lat": 48.9475, "lon": 2.1492},
    "COMPIEGNE": {"lat": 49.4181, "lon": 2.8097},
    "FONTAINEBLEAU": {"lat": 48.4247, "lon": 2.7022},
    "LYON-PARILLY": {"lat": 45.7256, "lon": 4.8967},
    "LYON-LA SOIE": {"lat": 45.7597, "lon": 4.9017},
    "MARSEILLE-BORELY": {"lat": 43.2597, "lon": 5.3750},
    "BORDEAUX-LE BOUSCAT": {"lat": 44.8700, "lon": -0.6050},
    "TOULOUSE": {"lat": 43.5783, "lon": 1.4300},
    "STRASBOURG": {"lat": 48.5500, "lon": 7.7500},
    "CAGNES-SUR-MER": {"lat": 43.6683, "lon": 7.1364},
    "NANTES": {"lat": 47.2589, "lon": -1.5833},
    "PAU": {"lat": 43.3100, "lon": -0.3700},
    "CRAON": {"lat": 47.8469, "lon": -0.9508},
    "VICHY": {"lat": 46.1192, "lon": 3.4100},
    "CLAIREFONTAINE": {"lat": 49.3117, "lon": 0.0617},
    "CABOURG": {"lat": 49.2922, "lon": -0.1169},
    "DIEPPE": {"lat": 49.9267, "lon": 1.0700},
    "CHATEAUBRIANT": {"lat": 47.7178, "lon": -1.3819},
    "PORNICHET": {"lat": 47.2567, "lon": -2.3450},
    "MONT-DE-MARSAN": {"lat": 43.8933, "lon": -0.4950},
    "DAX": {"lat": 43.7100, "lon": -1.0533},
    "AIX-LES-BAINS": {"lat": 45.6917, "lon": 5.8983},
    "LE MANS": {"lat": 47.9922, "lon": 0.2100},
    "ANGERS": {"lat": 47.4706, "lon": -0.5678},
    "LE LION-D'ANGERS": {"lat": 47.6297, "lon": -0.7117},
    "ROYAN": {"lat": 45.6208, "lon": -1.0228},
    "LA ROCHE-POSAY": {"lat": 46.7886, "lon": 0.8122},
    "VITTEL": {"lat": 48.2022, "lon": 5.9406},
    "LE CROISE-LAROCHE": {"lat": 50.6567, "lon": 3.0872},
    "MOULINS": {"lat": 46.5653, "lon": 3.3319},
    "ARGENTAN": {"lat": 48.7422, "lon": 0.0178},
    "POMPADOUR": {"lat": 45.4058, "lon": 1.3711},
    "TARBES": {"lat": 43.2297, "lon": 0.0692},
}

def fetch_meteo_period(lat, lon, date_from, date_to):
    """Récupérer météo historique via Open-Meteo (inclut Météo-France AROME)"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_from,
        "end_date": date_to,
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
                 "precipitation_sum,precipitation_hours,rain_sum,"
                 "wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,"
                 "shortwave_radiation_sum,et0_fao_evapotranspiration",
        "hourly": "temperature_2m,precipitation,rain,wind_speed_10m,wind_gusts_10m,relative_humidity_2m,soil_moisture_0_to_10cm",
        "timezone": "Europe/Paris",
        "models": "meteofrance_seamless",
    }
    
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                if "error" in data:
                    log.warning(f"  API error: {data.get('reason', data.get('error'))}")
                    return None
                return data
            elif resp.status_code == 429:
                wait = 10 * (attempt + 1)
                log.warning(f"  Rate limit, attente {wait}s...")
                time.sleep(wait)
                continue
            else:
                log.warning(f"  HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.error(f"  Erreur API (tentative {attempt+1}): {e}")
        if attempt < 2:
            time.sleep(3)
    return None

def main():
    log.info("=" * 60)
    log.info("SCRIPT 35 — Météo-France via Open-Meteo (AROME)")
    log.info("=" * 60)
    
    all_records = []
    output_file = os.path.join(OUTPUT_DIR, "meteo_france_hippodromes.json")
    
    # Charger les dates de courses depuis nos données
    courses_file = "output/02_liste_courses/courses_normalisees.json"
    if os.path.exists(courses_file):
        with open(courses_file) as f:
            courses = json.load(f)
        
        # Extraire les dates et hippodromes uniques
        date_hippo = set()
        for c in courses:
            date = c.get("date_reunion_iso", "")[:10]
            hippo = c.get("hippodrome_normalise", "").upper()
            if date and hippo:
                date_hippo.add((date, hippo))
        
        log.info(f"  {len(date_hippo)} combinaisons date/hippodrome uniques")
    else:
        log.warning("  Pas de fichier courses, collecte pour tous les hippodromes 2020-2026")
        date_hippo = set()
    
    # Collecter par hippodrome et par mois pour éviter trop de requêtes
    collected = 0
    
    for hippo_name, coords in HIPPODROMES.items():
        cache_file = os.path.join(CACHE_DIR, f"{hippo_name}.json")
        
        if os.path.exists(cache_file):
            with open(cache_file) as f:
                hippo_data = json.load(f)
            daily_count = len(hippo_data.get("daily", {}))
            if daily_count >= 1000:  # Au moins ~3 ans de données
                all_records.append(hippo_data)
                collected += 1
                log.info(f"  Cache OK {hippo_name}: {daily_count} jours")
                continue
            else:
                log.warning(f"  Cache incomplet {hippo_name}: {daily_count} jours, re-collecte...")
                os.remove(cache_file)
        
        log.info(f"  Collecte météo {hippo_name} ({coords['lat']}, {coords['lon']})...")
        
        # Collecter par année pour rester dans les limites API
        hippo_meteo = {
            "hippodrome": hippo_name,
            "lat": coords["lat"],
            "lon": coords["lon"],
            "daily": {},
            "source": "open_meteo_meteofrance",
        }
        
        for year in range(2016, 2027):
            date_from = f"{year}-01-01"
            date_to = f"{year}-12-31" if year < 2026 else datetime.now().strftime("%Y-%m-%d")
            
            data = fetch_meteo_period(coords["lat"], coords["lon"], date_from, date_to)
            
            if data and "daily" in data:
                daily = data["daily"]
                dates = daily.get("time", [])
                n = len(dates)

                def safe_get(key, idx):
                    arr = daily.get(key, [])
                    return arr[idx] if idx < len(arr) else None

                for i, d in enumerate(dates):
                    hippo_meteo["daily"][d] = {
                        "temp_max": safe_get("temperature_2m_max", i),
                        "temp_min": safe_get("temperature_2m_min", i),
                        "temp_mean": safe_get("temperature_2m_mean", i),
                        "precipitation": safe_get("precipitation_sum", i),
                        "precip_hours": safe_get("precipitation_hours", i),
                        "rain": safe_get("rain_sum", i),
                        "wind_max": safe_get("wind_speed_10m_max", i),
                        "wind_gusts": safe_get("wind_gusts_10m_max", i),
                        "wind_dir": safe_get("wind_direction_10m_dominant", i),
                        "radiation": safe_get("shortwave_radiation_sum", i),
                        "evapotranspiration": safe_get("et0_fao_evapotranspiration", i),
                    }
                log.info(f"    {year}: {n} jours")
            
            time.sleep(3)  # Rate limit Open-Meteo
        
        # Sauvegarder cache hippodrome
        with open(cache_file, "w") as f:
            json.dump(hippo_meteo, f, ensure_ascii=False)
        
        all_records.append(hippo_meteo)
        collected += 1
        log.info(f"  ✅ {hippo_name}: {len(hippo_meteo['daily'])} jours de météo")
    
    with open(output_file, "w") as f:
        json.dump(all_records, f, ensure_ascii=False)
    
    log.info("=" * 60)
    log.info(f"TERMINÉ: {collected} hippodromes, météo complète 2016-2026")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
