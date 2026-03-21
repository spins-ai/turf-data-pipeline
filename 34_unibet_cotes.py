#!/usr/bin/env python3
"""
Script 34 — Unibet FR : Cotes bookmaker français
Source : unibet.fr (API interne reverse-engineered)
CRITIQUE pour : Value Detection (PMU vs bookmaker), Market Comparison
"""

import requests
import json
import time
import random
import os
import logging
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "34_unibet_cotes")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

# Unibet FR API endpoints (reverse-engineered from frontend)
BASE_URL = "https://www.unibet.fr"
API_URL = "https://eu-offering-api.kambicdn.com/offering/v2018/ubfr"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

session = requests.Session()
session.headers.update({
    "User-Agent": random.choice(USER_AGENTS),
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
})

def smart_pause(base=1.0, jitter=0.5):
    time.sleep(base + random.uniform(-jitter, jitter))

def get_horse_racing_events():
    """Récupérer les événements hippiques depuis l'API Kambi (Unibet)"""
    events = []
    
    # L'API Kambi utilisée par Unibet
    url = f"{API_URL}/listView/horse_racing.json"
    params = {
        "lang": "fr_FR",
        "market": "FR",
        "client_id": 2,
        "channel_id": 1,
        "ncid": int(time.time() * 1000),
        "useCombined": "true",
    }
    
    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            events_raw = data.get("layout", {}).get("sections", [])
            for section in events_raw:
                for event in section.get("events", []):
                    events.append(event)
            log.info(f"  {len(events)} événements hippiques Unibet")
        else:
            log.warning(f"  HTTP {resp.status_code}")
    except Exception as e:
        log.error(f"  Erreur: {e}")
    
    return events

def get_event_odds(event_path):
    """Récupérer les cotes pour un événement"""
    url = f"{API_URL}/betoffer/event/{event_path}.json"
    params = {
        "lang": "fr_FR",
        "market": "FR",
        "client_id": 2,
        "channel_id": 1,
        "ncid": int(time.time() * 1000),
    }
    
    try:
        resp = session.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None

def scrape_unibet_daily():
    """Scraper les cotes Unibet du jour et à venir"""
    all_records = []
    
    # Essayer l'API Kambi
    events = get_horse_racing_events()
    
    for event in events:
        event_id = event.get("id", "")
        event_name = event.get("name", "")
        event_path = event.get("path", "")
        event_start = event.get("start", "")
        
        record_base = {
            "event_id": event_id,
            "event_name": event_name,
            "event_start": event_start,
            "source": "unibet_fr",
            "collected_at": datetime.now().isoformat(),
        }
        
        # Récupérer les cotes
        if event_path:
            smart_pause(0.5, 0.2)
            odds_data = get_event_odds(event_path)
            
            if odds_data:
                for offer in odds_data.get("betOffers", []):
                    market_type = offer.get("criterion", {}).get("label", "")
                    for outcome in offer.get("outcomes", []):
                        record = {**record_base}
                        record["market_type"] = market_type
                        record["runner_name"] = outcome.get("label", "")
                        record["runner_id"] = outcome.get("id", "")
                        
                        odds_am = outcome.get("odds", 0)
                        if odds_am:
                            record["cote_decimale"] = odds_am / 1000.0
                        
                        record["status"] = outcome.get("status", "")
                        all_records.append(record)
        
        all_records.append(record_base)
    
    # Aussi essayer le scraping HTML direct
    if not events:
        log.info("  API Kambi vide, essai scraping HTML...")
        from bs4 import BeautifulSoup
        
        url = f"{BASE_URL}/sport/courses-hippiques"
        try:
            resp = session.get(url, timeout=30, headers={"Accept": "text/html"})
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                # Chercher les données JSON embedded
                scripts = soup.find_all("script")
                for script in scripts:
                    text = script.string or ""
                    if "horse_racing" in text or "hippique" in text.lower():
                        # Essayer d'extraire le JSON
                        json_matches = __import__('re').findall(r'\{[^{}]{100,}\}', text)
                        for m in json_matches[:5]:
                            try:
                                data = json.loads(m)
                                all_records.append({"source": "unibet_html", "data": data})
                            except (json.JSONDecodeError, ValueError):
                                pass
        except Exception as e:
            log.debug(f"  Erreur HTML: {e}")
    
    return all_records

def main():
    log.info("=" * 60)
    log.info("SCRIPT 34 — Unibet FR Cotes Hippiques")
    log.info("=" * 60)
    
    output_file = os.path.join(OUTPUT_DIR, "unibet_cotes.json")
    all_records = []
    if os.path.exists(output_file):
        with open(output_file, encoding="utf-8") as f:
            all_records = json.load(f)
    
    # Collecter les cotes actuelles
    new_records = scrape_unibet_daily()
    all_records.extend(new_records)
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
    
    log.info(f"TERMINÉ: {len(new_records)} nouveaux records, {len(all_records)} total")

if __name__ == "__main__":
    main()
