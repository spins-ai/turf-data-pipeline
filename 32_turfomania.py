#!/usr/bin/env python3
"""
Script 32 — Turfomania : Indices de confiance, Turf Machine IA, fiches techniques
Source : turfomania.fr
CRITIQUE pour : Features uniques (indice confiance, forme), Alternative predictions
"""

import requests
import json
import random
import os
import re
import sys
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "32_turfomania")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

log = setup_logging("32_turfomania")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

session = requests.Session()
req_count = 0

def rotate_session():
    global session, req_count
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "DNT": "1",
    })
    req_count = 0

def scrape_day(date_str):
    global req_count
    cache_file = os.path.join(CACHE_DIR, f"{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)
    
    records = []
    # turfomania.fr/pronostics/YYYY-MM-DD
    url = f"https://www.turfomania.fr/pronostics/{date_str}/"
    
    try:
        resp = session.get(url, timeout=20)
        req_count += 1
        if req_count >= random.randint(25, 40):
            rotate_session()
        
        if resp.status_code == 200 and len(resp.text) > 2000:
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Chercher les courses/réunions
            reunions = soup.find_all(["div", "section"], class_=re.compile(r"reunion|course|race|event", re.I))
            
            # Chercher aussi les tableaux
            tables = soup.find_all("table")
            
            for table in tables:
                header_row = table.find("tr")
                headers = [th.get_text(strip=True) for th in (header_row.find_all(["th", "td"]) if header_row else [])]
                
                for row in table.find_all("tr")[1:]:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 3:
                        record = {"date": date_str, "source": "turfomania"}
                        for j, cell in enumerate(cells):
                            text = cell.get_text(strip=True)
                            col_name = headers[j] if j < len(headers) else f"col_{j}"
                            col_name = re.sub(r'[^\w]', '_', col_name.lower()).strip('_')
                            record[col_name] = text
                        records.append(record)
            
            # Chercher les indices de confiance
            confiance_divs = soup.find_all(attrs={"class": re.compile(r"confiance|indice|score|rating", re.I)})
            for div in confiance_divs:
                text = div.get_text(" ", strip=True)
                if text:
                    records.append({
                        "date": date_str,
                        "type": "indice_confiance",
                        "source": "turfomania",
                        "raw_text": text[:300],
                    })
            
            # Chercher les sélections/pronostics
            selections = soup.find_all(attrs={"class": re.compile(r"selection|prono|pick|turf.machine", re.I)})
            for sel in selections:
                text = sel.get_text(" | ", strip=True)
                if text and len(text) > 5:
                    records.append({
                        "date": date_str,
                        "type": "pronostic",
                        "source": "turfomania",
                        "raw_text": text[:500],
                    })
    
    except Exception as e:
        log.debug(f"Erreur {date_str}: {e}")
    
    if records:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
    
    return records

def main():
    log.info("=" * 60)
    log.info("SCRIPT 32 — Turfomania Indices & Pronostics")
    log.info("=" * 60)
    
    rotate_session()
    all_records = []
    output_file = os.path.join(OUTPUT_DIR, "turfomania_data.json")
    
    start = datetime(2020, 1, 1)
    end = datetime.now()
    current = start
    collected = 0
    
    while current < end:
        date_str = current.strftime("%Y-%m-%d")
        records = scrape_day(date_str)
        if records:
            all_records.extend(records)
            collected += 1
        
        if collected % 30 == 0 and collected > 0:
            log.info(f"  {collected} jours, {len(all_records)} records")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_records, f, ensure_ascii=False)
        
        current += timedelta(days=1)
        smart_pause(2.5, 1.0)
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False)
    
    log.info(f"TERMINÉ: {collected} jours, {len(all_records)} records")

if __name__ == "__main__":
    main()
