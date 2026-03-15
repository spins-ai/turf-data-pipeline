#!/usr/bin/env python3
"""
Script 33 — Turf-FR : Pronostics presse, % adversaires battus, stats
Source : turf-fr.com
CRITIQUE pour : Consensus experts, Stats adversaires, Features presse
"""

import requests
import json
import time
import random
import os
import re
import logging
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

OUTPUT_DIR = "output/33_turf_fr"
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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

def smart_pause(base=3.0, jitter=1.5):
    time.sleep(base + random.uniform(-jitter, jitter))
    if random.random() < 0.08:
        time.sleep(random.uniform(5, 20))

def scrape_day(date_str):
    global req_count
    cache_file = os.path.join(CACHE_DIR, f"{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            return json.load(f)
    
    records = []
    dd, mm, yyyy = date_str[8:10], date_str[5:7], date_str[:4]
    
    urls = [
        f"https://www.turf-fr.com/pronostic-quinte-{dd}-{mm}-{yyyy}.php",
        f"https://www.turf-fr.com/pronostic-{dd}-{mm}-{yyyy}.php",
        f"https://www.turf-fr.com/resultats-{dd}-{mm}-{yyyy}.php",
        f"https://www.turf-fr.com/programme-{dd}-{mm}-{yyyy}.php",
    ]
    
    for url in urls:
        try:
            resp = session.get(url, timeout=20)
            req_count += 1
            if req_count >= random.randint(25, 40):
                rotate_session()
            
            if resp.status_code == 200 and len(resp.text) > 1500:
                soup = BeautifulSoup(resp.text, "html.parser")
                
                tables = soup.find_all("table")
                for table in tables:
                    header_row = table.find("tr")
                    headers = [th.get_text(strip=True) for th in (header_row.find_all(["th", "td"]) if header_row else [])]
                    
                    for row in table.find_all("tr")[1:]:
                        cells = row.find_all(["td", "th"])
                        if len(cells) >= 2:
                            record = {"date": date_str, "source": "turf_fr", "url_type": url.split("/")[-1][:10]}
                            for j, cell in enumerate(cells):
                                text = cell.get_text(strip=True)
                                col_name = headers[j] if j < len(headers) else f"col_{j}"
                                col_name = re.sub(r'[^\w]', '_', col_name.lower()).strip('_')
                                record[col_name] = text
                            records.append(record)
                
                # Stats spécifiques
                for div in soup.find_all(attrs={"class": re.compile(r"adversaire|battus|pourcent|stat", re.I)}):
                    text = div.get_text(" ", strip=True)
                    if text:
                        records.append({"date": date_str, "type": "stats", "source": "turf_fr", "raw_text": text[:300]})
                
                if records:
                    break
                    
        except Exception as e:
            log.debug(f"Erreur {url}: {e}")
        
        smart_pause(1.5, 0.5)
    
    if records:
        with open(cache_file, "w") as f:
            json.dump(records, f, ensure_ascii=False)
    return records

def main():
    log.info("=" * 60)
    log.info("SCRIPT 33 — Turf-FR Pronostics & Stats")
    log.info("=" * 60)
    
    rotate_session()
    all_records = []
    output_file = os.path.join(OUTPUT_DIR, "turf_fr_data.json")
    
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
            with open(output_file, "w") as f:
                json.dump(all_records, f, ensure_ascii=False)
        current += timedelta(days=1)
        smart_pause(2.5, 1.0)
    
    with open(output_file, "w") as f:
        json.dump(all_records, f, ensure_ascii=False)
    log.info(f"TERMINÉ: {collected} jours, {len(all_records)} records")

if __name__ == "__main__":
    main()
