#!/usr/bin/env python3
"""
Script 26 — Scraping Geny.com (PMU Group)
Source : geny.com/partants-pmu/{date}
Collecte : pronostics Geny, stats jockeys détaillées, commentaires experts
CRITIQUE pour : Anomaly Detector, Jockey Synergy, Meta Model, Commentaires NLP
"""

import requests
import json
import time
import random
import os
import re
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

SCRIPT_NAME = "26_geny"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s

def smart_pause(base=3.0, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.1:
        pause += random.uniform(5, 20)
    time.sleep(max(1.5, pause))

def scrape_day(session, date_str):
    """Scraper la page partants d'un jour"""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    url = f"https://www.geny.com/partants-pmu/{date_str}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return None
    except Exception as e:
        log.debug(f"  Erreur réseau geny: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    result = {"date": date_str, "reunions": [], "courses": []}

    # Extraire les réunions
    for div in soup.find_all(["div", "section", "article"]):
        classes = " ".join(div.get("class", []))

        # Réunions
        if "reunion" in classes.lower() or "meeting" in classes.lower():
            reunion = {"classes": classes, "text": div.get_text(strip=True)[:200]}
            links = div.find_all("a", href=True)
            for link in links:
                if "course" in link["href"].lower() or "partants" in link["href"].lower():
                    reunion["course_url"] = link["href"]
            result["reunions"].append(reunion)

    # Extraire les tables de partants
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 3:
                partant = {"date": date_str, "source": "geny"}
                for j, cell in enumerate(cells):
                    if j < len(headers) and headers[j]:
                        partant[headers[j]] = cell
                    else:
                        partant[f"col_{j}"] = cell
                result["courses"].append(partant)

    # Extraire les pronostics
    pronostics = []
    for div in soup.find_all(["div", "span", "p"]):
        text = div.get_text(strip=True)
        if re.search(r'pronostic|favori|outsider|base|chance', text, re.I):
            if len(text) < 300:
                pronostics.append(text)
    if pronostics:
        result["pronostics_raw"] = pronostics[:20]

    # Commentaires experts
    comments = []
    for div in soup.find_all(["div", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        if "comment" in classes.lower() or "avis" in classes.lower() or "analyse" in classes.lower():
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 1000:
                comments.append(text)
    if comments:
        result["commentaires_experts"] = comments[:10]

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result

def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Script 26 — Geny.com Scraper")
    parser.add_argument("--start", type=str, default="2020-01-01",
                        help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), défaut=hier")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Nombre max de jours (0=illimité)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else (datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 26 — Geny.com Scraper")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "geny_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        if args.max_days and day_count >= args.max_days:
            log.info(f"  Max {args.max_days} jours atteint, arrêt.")
            break

        date_str = current.strftime("%Y-%m-%d")
        data = scrape_day(session, date_str)

        if data:
            append_jsonl(output_file, data)
            total_records += 1

        day_count += 1
        if day_count % 30 == 0:
            log.info(f"  {date_str} | {total_records} jours collectés")
            save_checkpoint({"last_date": date_str, "total_records": total_records})

        if day_count % 50 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(10, 30))

        current += timedelta(days=1)
        smart_pause(2.0, 1.0)

    save_checkpoint({"last_date": (current - timedelta(days=1)).strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {total_records} jours collectés → {output_file}")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
