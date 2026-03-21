#!/usr/bin/env python3
"""
Script 31 — Zone-Turf : Pronostics communautaires + stats chevaux
Source : zone-turf.fr
CRITIQUE pour : Crowd Wisdom, Consensus Pronostiqueurs, Features communautaires
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "31_zone_turf")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

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
        "Referer": "https://www.zone-turf.fr/",
    })
    req_count = 0

def smart_pause(base=3.0, jitter=1.5):
    time.sleep(base + random.uniform(-jitter, jitter))
    if random.random() < 0.08:
        time.sleep(random.uniform(5, 20))

def scrape_day(date_str):
    """Scraper une journée de pronostics/résultats Zone-Turf"""
    global req_count
    
    cache_file = os.path.join(CACHE_DIR, f"{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            return json.load(f)
    
    records = []
    # Format date pour URL: zone-turf.fr/pronostic/YYYY-MM-DD ou /resultats/
    urls_to_try = [
        f"https://www.zone-turf.fr/pronostic/{date_str}/",
        f"https://www.zone-turf.fr/resultats/{date_str}/",
        f"https://www.zone-turf.fr/programme-courses/{date_str}/",
    ]
    
    for url in urls_to_try:
        try:
            resp = session.get(url, timeout=20)
            req_count += 1
            if req_count >= random.randint(30, 45):
                rotate_session()
            
            if resp.status_code == 200 and len(resp.text) > 2000:
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # Chercher les courses
                course_blocks = soup.find_all(["div", "section", "article"], 
                    class_=re.compile(r"course|race|reunion|prono", re.I))
                
                if not course_blocks:
                    course_blocks = soup.find_all("table")
                
                for block in course_blocks:
                    # Extraire les données de chaque course
                    rows = block.find_all("tr") if block.name == "table" else block.find_all(["div", "li"])
                    
                    for row in rows:
                        text = row.get_text(" | ", strip=True)
                        if len(text) > 5:
                            record = {
                                "date": date_str,
                                "source_url": url.split("/")[3],  # pronostic ou resultats
                                "source": "zone_turf",
                            }
                            
                            # Extraire numéro cheval
                            num_match = re.search(r'(?:^|\s)(\d{1,2})(?:\s|\.|-)', text)
                            if num_match:
                                record["num"] = int(num_match.group(1))
                            
                            # Extraire cote
                            cote_match = re.search(r'(\d+[.,]\d+)\s*/\s*1|cote\s*:\s*(\d+[.,]\d+)', text, re.I)
                            if cote_match:
                                record["cote"] = cote_match.group(1) or cote_match.group(2)
                            
                            record["raw_text"] = text[:300]
                            records.append(record)
                
                # Aussi chercher les pronos structurés
                prono_divs = soup.find_all(attrs={"class": re.compile(r"prono|selection|pick|tip", re.I)})
                for div in prono_divs:
                    text = div.get_text(" ", strip=True)
                    if text and len(text) > 3:
                        records.append({
                            "date": date_str,
                            "type": "pronostic",
                            "source": "zone_turf",
                            "raw_text": text[:300],
                        })
                
                if records:
                    break  # On a trouvé des données, pas besoin des autres URLs
                    
        except Exception as e:
            log.debug(f"  Erreur {url}: {e}")
        
        smart_pause(1.5, 0.5)
    
    if records:
        with open(cache_file, "w") as f:
            json.dump(records, f, ensure_ascii=False)
    
    return records

def main():
    log.info("=" * 60)
    log.info("SCRIPT 31 — Zone-Turf Pronostics & Stats")
    log.info("=" * 60)
    
    rotate_session()
    
    all_records = []
    output_file = os.path.join(OUTPUT_DIR, "zone_turf_data.json")
    
    # Parcourir 2020-2026
    start = datetime(2020, 1, 1)
    end = datetime.now()
    current = start
    
    collected_days = 0
    
    while current < end:
        date_str = current.strftime("%Y-%m-%d")
        records = scrape_day(date_str)
        
        if records:
            all_records.extend(records)
            collected_days += 1
        
        if collected_days % 30 == 0 and collected_days > 0:
            log.info(f"  {collected_days} jours collectés, {len(all_records)} records")
            with open(output_file, "w") as f:
                json.dump(all_records, f, ensure_ascii=False)
        
        current += timedelta(days=1)
        smart_pause(2.0, 1.0)
    
    with open(output_file, "w") as f:
        json.dump(all_records, f, ensure_ascii=False)
    
    log.info("=" * 60)
    log.info(f"TERMINÉ: {collected_days} jours, {len(all_records)} records")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
