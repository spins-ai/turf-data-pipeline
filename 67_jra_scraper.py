#!/usr/bin/env python3
"""
Script 67 — Scraping jra.go.jp (Japan Racing Association)
Source : jra.go.jp
Collecte : race results, stats, horse profiles, jockey/trainer data
CRITIQUE pour : Japanese Racing Data, Results Analysis, Stakes Races
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "67_jra"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, append_jsonl, load_checkpoint, save_checkpoint, create_session

log = setup_logging("67_jra")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.jra.go.jp"
# JRA English access page
ENGLISH_URL = f"{BASE_URL}/en"



def scrape_race_calendar(session, date_str):
    """Scrape JRA race calendar / schedule for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"calendar_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    # JRA schedule URL pattern
    url = f"{BASE_URL}/datafile/seiseki/replay/{dt.strftime('%Y')}/{dt.strftime('%Y%m%d')}.html"
    resp = fetch_with_retry(session, url)
    if not resp:
        # Try English version
        url = f"{ENGLISH_URL}/racing/schedule/{date_str}"
        resp = fetch_with_retry(session, url)
    if not resp:
        return None

    # Handle Japanese encoding
    if resp.encoding and "shift" in resp.encoding.lower():
        resp.encoding = "shift_jis"
    elif resp.encoding and "euc" in resp.encoding.lower():
        resp.encoding = "euc-jp"

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract race links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if ("race" in href.lower() or "result" in href.lower() or "seiseki" in href.lower()) and text and len(text) > 1:
            records.append({
                "date": date_str,
                "source": "jra",
                "type": "race_link",
                "text": text,
                "url": href if href.startswith("http") else BASE_URL + href,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(session, date_str):
    """Scrape JRA race results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    # Try multiple URL patterns (JRA changes formats)
    urls_to_try = [
        f"{BASE_URL}/datafile/seiseki/replay/{dt.strftime('%Y')}/{dt.strftime('%Y%m%d')}.html",
        f"{BASE_URL}/JRADB/accessS.html?CESSION={dt.strftime('%Y%m%d')}",
        f"{ENGLISH_URL}/racing/results/{date_str}",
    ]

    soup = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            if resp.encoding and "shift" in resp.encoding.lower():
                resp.encoding = "shift_jis"
            elif resp.encoding and "euc" in resp.encoding.lower():
                resp.encoding = "euc-jp"
            soup = BeautifulSoup(resp.text, "html.parser")
            break

    if not soup:
        return None

    records = []

    # Extract result tables
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
                "date": date_str,
                "source": "jra",
                "type": "result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Parse time
                time_match = re.search(r'(\d+:\d+\.\d+)', cell)
                if time_match:
                    record["time_parsed"] = time_match.group(1)

                # Parse weight (e.g., "58.0" or "54")
                if any(kw in key for kw in ["weight", "kinryo", "futan"]):
                    w_match = re.search(r'(\d+\.?\d*)', cell)
                    if w_match:
                        record["weight_parsed"] = float(w_match.group(1))

            records.append(record)

    # Extract race info blocks (race name, distance, surface, condition)
    for div in soup.find_all(["div", "h2", "h3", "caption"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["race-name", "race-info", "race-header"]):
            if text and len(text) > 2:
                # Try to extract distance
                dist_match = re.search(r'(\d{3,4})\s*m', text)
                surface = ""
                if any(kw in text.lower() for kw in ["turf", "shiba"]):
                    surface = "turf"
                elif any(kw in text.lower() for kw in ["dirt", "daat"]):
                    surface = "dirt"

                records.append({
                    "date": date_str,
                    "source": "jra",
                    "type": "race_info",
                    "content": text[:500],
                    "distance": dist_match.group(1) if dist_match else "",
                    "surface": surface,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_horse_stats(session, date_str):
    """Scrape JRA horse/jockey/trainer statistics page."""
    cache_file = os.path.join(CACHE_DIR, f"stats_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    url = f"{BASE_URL}/datafile/seiseki/{dt.strftime('%Y')}/jra_ranking.html"
    resp = fetch_with_retry(session, url)
    if not resp:
        url = f"{ENGLISH_URL}/racing/statistics/"
        resp = fetch_with_retry(session, url)
    if not resp:
        return None

    if resp.encoding and "shift" in resp.encoding.lower():
        resp.encoding = "shift_jis"

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract ranking/stats tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        # Detect table type
        table_type = "stats"
        header_text = " ".join(headers).lower()
        if any(kw in header_text for kw in ["jockey", "kishu"]):
            table_type = "jockey_ranking"
        elif any(kw in header_text for kw in ["trainer", "chokyoshi"]):
            table_type = "trainer_ranking"
        elif any(kw in header_text for kw in ["horse", "uma"]):
            table_type = "horse_ranking"
        elif any(kw in header_text for kw in ["sire", "seed"]):
            table_type = "sire_ranking"

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "date": date_str,
                "source": "jra",
                "type": table_type,
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
    parser = argparse.ArgumentParser(description="Script 67 — JRA Scraper (Japanese racing results, stats)")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=today")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 67 — JRA Scraper (Japanese Racing)")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "jra_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # JRA races mostly Sat/Sun — scrape all days but expect fewer results on weekdays
        # Scrape race calendar
        cal_records = scrape_race_calendar(session, date_str)
        if cal_records:
            for rec in cal_records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(2.5, 1.0)

        # Scrape results
        result_records = scrape_results(session, date_str)
        if result_records:
            for rec in result_records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(2.5, 1.0)

        # Scrape stats (weekly)
        if current.weekday() == 0:
            stats_records = scrape_horse_stats(session, date_str)
            if stats_records:
                for rec in stats_records:
                    append_jsonl(output_file, rec)
                    total_records += 1
            smart_pause(2.5, 1.0)

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | days={day_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

        if day_count % 80 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(1.0, 0.5)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
