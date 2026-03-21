#!/usr/bin/env python3
"""
Script 76 — Scraping USTrotting.com (trotting US)
Source : ustrotting.com/publicsite/
Collecte : resultats courses trot US, statistiques chevaux, drivers, trainers,
           records, pedigree standardbred
CRITIQUE pour : Trot Analysis, Driver Stats, Standardbred Pedigree
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "76_usta_trot"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry

log = setup_logging("76_usta_trot")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.ustrotting.com"

# Hippodromes majeurs trot US
MAJOR_TRACKS = [
    "meadowlands", "yonkers", "pocono", "mohawk", "woodbine",
    "hoosier", "northfield", "scioto", "plainridge", "harrahs",
    "dover", "freehold", "tioga", "saratoga-harness",
]


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


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


def scrape_race_results_day(session, date_str):
    """Scraper les resultats de courses trot US pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # Essayer plusieurs formats d'URL
    date_fmt = date_str.replace("-", "")
    urls_to_try = [
        f"{BASE_URL}/publicsite/racing/results.html?date={date_str}",
        f"{BASE_URL}/racing/results/{date_str}",
        f"{BASE_URL}/publicsite/racing/raceresults.cfm?date={date_fmt}",
    ]

    soup = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    records = []

    # Extraire les resultats de chaque course
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "source": "usta_trotting",
                "date": date_str,
                "type": "race_result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extraire temps (format X:XX.X)
            for cell in cells:
                time_match = re.search(r'(\d:\d{2}\.\d)', cell)
                if time_match:
                    record["time_brut"] = time_match.group(1)
                    parts = time_match.group(1).split(":")
                    record["time_seconds"] = float(parts[0]) * 60 + float(parts[1])
                    break

            records.append(record)

    # Extraire les sections de course (race headers)
    for header in soup.find_all(["h2", "h3", "h4", "div"], class_=True):
        classes = " ".join(header.get("class", []))
        text = header.get_text(strip=True)
        if any(kw in classes.lower() or kw in text.lower()
               for kw in ["race", "course", "trot", "pace"]):
            if text and 5 < len(text) < 300:
                record = {
                    "source": "usta_trotting",
                    "date": date_str,
                    "type": "race_header",
                    "contenu": text,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Extraire distance
                dist_match = re.search(r'(\d+)\s*(mile|furlong|meter|m)', text, re.I)
                if dist_match:
                    record["distance_brut"] = dist_match.group(0)

                # Extraire purse
                purse_match = re.search(r'\$[\d,]+', text)
                if purse_match:
                    record["purse_brut"] = purse_match.group(0)
                    record["purse_usd"] = int(purse_match.group(0).replace("$", "").replace(",", ""))

                # Trot vs Pace
                if "trot" in text.lower():
                    record["gait"] = "trot"
                elif "pace" in text.lower():
                    record["gait"] = "pace"

                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_horse_profile(session, horse_name):
    """Scraper le profil d'un cheval standardbred."""
    slug = re.sub(r'[^a-zA-Z0-9]', '_', horse_name.lower())
    cache_file = os.path.join(CACHE_DIR, f"horse_{slug}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/publicsite/horse/search.cfm?name={requests.utils.quote(horse_name)}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    profile = {
        "source": "usta_trotting",
        "type": "horse_profile",
        "horse_name": horse_name,
        "scraped_at": datetime.now().isoformat(),
    }

    # Extraire les infos de base
    for dt in soup.find_all(["dt", "th", "label", "strong"]):
        dd = dt.find_next_sibling(["dd", "td", "span"])
        if dd:
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val:
                profile[key] = val

    # Extraire le pedigree
    for el in soup.find_all(string=re.compile(r'(sire|dam|broodmare)', re.I)):
        parent = el.find_parent()
        if parent:
            key = "sire" if "sire" in el.lower() else "dam" if "dam" in el.lower() else "broodmare_sire"
            profile[key] = parent.get_text(strip=True)

    # Records (mark)
    for el in soup.find_all(string=re.compile(r'\d:\d{2}\.\d')):
        parent = el.find_parent()
        if parent:
            profile["best_time_brut"] = parent.get_text(strip=True)
            time_match = re.search(r'(\d:\d{2}\.\d)', el)
            if time_match:
                parts = time_match.group(1).split(":")
                profile["best_time_seconds"] = float(parts[0]) * 60 + float(parts[1])
            break

    # Earnings
    for el in soup.find_all(string=re.compile(r'\$[\d,]+')):
        earnings_match = re.search(r'\$([\d,]+)', el)
        if earnings_match and int(earnings_match.group(1).replace(",", "")) > 100:
            profile["earnings_brut"] = el.strip()
            profile["earnings_usd"] = int(earnings_match.group(1).replace(",", ""))
            break

    # Starts / Wins / Seconds / Thirds
    for el in soup.find_all(string=re.compile(r'\d+-\d+-\d+-\d+')):
        record_match = re.search(r'(\d+)-(\d+)-(\d+)-(\d+)', el)
        if record_match:
            profile["starts"] = int(record_match.group(1))
            profile["wins"] = int(record_match.group(2))
            profile["seconds"] = int(record_match.group(3))
            profile["thirds"] = int(record_match.group(4))
            break

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)

    return profile


def scrape_driver_stats(session, year):
    """Scraper les statistiques des drivers pour une annee."""
    cache_file = os.path.join(CACHE_DIR, f"drivers_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/publicsite/racing/driverstandings.cfm?year={year}",
        f"{BASE_URL}/racing/standings/drivers/{year}",
    ]

    soup = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "source": "usta_trotting",
                "year": year,
                "type": "driver_stats",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_trainer_stats(session, year):
    """Scraper les statistiques des trainers pour une annee."""
    cache_file = os.path.join(CACHE_DIR, f"trainers_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/publicsite/racing/trainerstandings.cfm?year={year}",
        f"{BASE_URL}/racing/standings/trainers/{year}",
    ]

    soup = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "source": "usta_trotting",
                "year": year,
                "type": "trainer_stats",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 76 — USTA Trotting Scraper (trot US)")
    parser.add_argument("--start", type=str, default="2020-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--mode", choices=["results", "stats", "all"], default="all",
                        help="Mode: results (courses), stats (drivers/trainers), all")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 76 — USTA Trotting Scraper (trot US)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Mode : {args.mode}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "usta_trot_data.jsonl")

    total_records = checkpoint.get("total_records", 0)

    # --- Mode RESULTS ---
    if args.mode in ("results", "all"):
        log.info("--- Phase 1: Resultats courses ---")
        last_date = checkpoint.get("last_date")
        current = start_date
        if args.resume and last_date:
            resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            if resume_date > current:
                current = resume_date
                log.info(f"  Reprise au checkpoint : {current.date()}")

        day_count = 0
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            records = scrape_race_results_day(session, date_str)

            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | jours={day_count} records={total_records}")
                save_checkpoint({"last_date": date_str, "total_records": total_records})

            if day_count % 80 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

    # --- Mode STATS ---
    if args.mode in ("stats", "all"):
        log.info("--- Phase 2: Statistiques drivers/trainers ---")
        year_start = start_date.year
        year_end = end_date.year

        for year in range(year_start, year_end + 1):
            log.info(f"  Drivers {year}...")
            drivers = scrape_driver_stats(session, year)
            for rec in drivers:
                append_jsonl(output_file, rec)
                total_records += 1
            smart_pause(2.0, 1.0)

            log.info(f"  Trainers {year}...")
            trainers = scrape_trainer_stats(session, year)
            for rec in trainers:
                append_jsonl(output_file, rec)
                total_records += 1
            smart_pause(2.0, 1.0)

    save_checkpoint({"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
