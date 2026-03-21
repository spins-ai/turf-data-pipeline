#!/usr/bin/env python3
"""
Script 37 — Racing Post via rpscrape : Form, ratings, résultats internationaux
Source : racingpost.com (via technique rpscrape)
CRITIQUE pour : Ratings internationaux, form UK/FR, speed figures

PATCH JSONL : append mode, ~15 MB RAM au lieu de 1.6 GB
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
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "37_racing_post")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

session = requests.Session()
req_count = 0

def rotate_session():
    global session, req_count
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-GB,en;q=0.9",
        "DNT": "1",
    })
    req_count = 0

def smart_pause(base=4.0, jitter=2.0):
    time.sleep(base + random.uniform(-jitter, jitter))
    if random.random() < 0.1:
        time.sleep(random.uniform(10, 30))

# Codes des hippodromes français sur Racing Post
FR_COURSES = {
    "longchamp": 211, "chantilly": 207, "deauville": 209,
    "saint-cloud": 219, "auteuil": 204, "compiegne": 208,
    "fontainebleau": 210, "lyon-parilly": 213, "marseille": 214,
    "bordeaux": 206, "toulouse": 223, "cagnes-sur-mer": 1047,
    "pau": 216, "strasbourg": 220, "vichy": 224,
    "clairefontaine": 1048, "maisons-laffitte": 1180,
}

def scrape_results_page(course_id, date_str):
    """Scraper une page de résultats Racing Post"""
    global req_count

    cache_key = f"{course_id}_{date_str}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    # Format: racingpost.com/results/COURSE_ID/YYYY-MM-DD
    url = f"https://www.racingpost.com/results/{course_id}/{date_str}"

    try:
        resp = session.get(url, timeout=20)
        req_count += 1
        if req_count >= random.randint(15, 25):
            rotate_session()

        if resp.status_code == 200 and len(resp.text) > 2000:
            soup = BeautifulSoup(resp.text, "html.parser")
            records = []

            # Chercher les résultats de courses
            race_cards = soup.find_all(["div", "section"], class_=re.compile(r"result|race|card", re.I))

            for race in race_cards:
                # Titre de la course
                title = race.find(["h2", "h3", "span"], class_=re.compile(r"title|name|header", re.I))
                race_name = title.get_text(strip=True) if title else ""

                # Conditions
                info = race.find(attrs={"class": re.compile(r"info|condition|detail", re.I)})
                race_info = info.get_text(" ", strip=True) if info else ""

                # Résultats/runners
                runners = race.find_all("tr")
                if not runners:
                    runners = race.find_all(["div", "li"], class_=re.compile(r"runner|horse|result", re.I))

                for runner in runners:
                    text = runner.get_text(" | ", strip=True)

                    if len(text) > 10:
                        record = {
                            "date": date_str,
                            "course_id": course_id,
                            "race_name": race_name,
                            "race_info": race_info[:200],
                            "source": "racing_post",
                        }

                        # Extraire position
                        pos_match = re.search(r'^(\d{1,2})(st|nd|rd|th)?', text)
                        if pos_match:
                            record["position"] = int(pos_match.group(1))

                        # Extraire cote
                        odds_match = re.search(r'(\d+/\d+|evens|\d+\.\d+)', text)
                        if odds_match:
                            record["odds"] = odds_match.group(1)

                        # Extraire RPR/TS
                        rpr_match = re.search(r'RPR[:\s]*(\d+)', text, re.I)
                        if rpr_match:
                            record["rpr"] = int(rpr_match.group(1))

                        ts_match = re.search(r'TS[:\s]*(\d+)', text, re.I)
                        if ts_match:
                            record["topspeed"] = int(ts_match.group(1))

                        record["raw_text"] = text[:400]
                        records.append(record)

            if records:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(records, f, ensure_ascii=False)
                return records

    except Exception as e:
        log.debug(f"  Erreur {url}: {e}")

    return []

def main():
    log.info("=" * 60)
    log.info("SCRIPT 37 — Racing Post Results (FR courses) — MODE JSONL")
    log.info("=" * 60)

    rotate_session()
    output_file = os.path.join(OUTPUT_DIR, "racing_post_fr.jsonl")
    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_37.json")

    # Checkpoint
    total_records = 0
    last_course_idx = 0
    last_date_str = ""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r", encoding="utf-8", errors="replace") as f:
            cp = json.load(f)
        total_records = cp.get("total_records", 0)
        last_course_idx = cp.get("last_course_idx", 0)
        last_date_str = cp.get("last_date_str", "")
        log.info(f"Reprise checkpoint: {total_records} records, course_idx={last_course_idx}, dernière date={last_date_str}")

    # Parcourir les courses françaises 2020-2026
    start = datetime(2020, 1, 1)
    end = datetime.now()
    collected_pages = 0

    course_list = list(FR_COURSES.items())

    for course_idx, (course_name, course_id) in enumerate(course_list):
        # Skip courses déjà traitées
        if course_idx < last_course_idx:
            continue

        log.info(f"  Course: {course_name} (ID: {course_id})")

        current = start
        # Si on reprend cette course, skip les dates déjà faites
        if course_idx == last_course_idx and last_date_str:
            try:
                resume_date = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
                if resume_date > current:
                    current = resume_date
                    log.info(f"    Reprise à la date {current.strftime('%Y-%m-%d')}")
            except ValueError:
                pass

        while current < end:
            date_str = current.strftime("%Y-%m-%d")

            records = scrape_results_page(course_id, date_str)
            if records:
                # Append JSONL — pas d'accumulation en mémoire
                with open(output_file, "a", encoding="utf-8") as f:
                    for r in records:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")
                total_records += len(records)
                collected_pages += 1

            current += timedelta(days=1)
            smart_pause(3.0, 1.5)

            if collected_pages % 20 == 0 and collected_pages > 0:
                log.info(f"    {collected_pages} pages, {total_records} records total")
                # Checkpoint
                with open(checkpoint_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "last_course_idx": course_idx,
                        "last_date_str": date_str,
                        "total_records": total_records,
                    }, f)

        # Checkpoint fin de course
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump({
                "last_course_idx": course_idx + 1,
                "last_date_str": "",
                "total_records": total_records,
            }, f)

    log.info(f"TERMINÉ: {collected_pages} pages, {total_records} records Racing Post")

if __name__ == "__main__":
    main()
