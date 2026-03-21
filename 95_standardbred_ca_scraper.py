#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 95 -- Scraping Standardbred Canada
Source : standardbredcanada.ca - Standardbred Canada trot data
Collecte : results trot CA, pedigrees, stakes, horse records, driver stats
CRITIQUE pour : Trot Model International, North America Harness Data
"""

import argparse
import json
import logging
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "95_standardbred_ca"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry

log = setup_logging("95_standardbred_ca")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.standardbredcanada.ca"


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8", errors="replace", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8", errors="replace") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def scrape_sc_day(session, date_str):
    """Scrape standardbred results for a given day."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/racing/results?date={date_str}",
        f"{BASE_URL}/racing/results/{date_str}",
        f"{BASE_URL}/results/{date_str}",
        f"{BASE_URL}/racing/entries?date={date_str}",
    ]

    records = []
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # -- Results tables --
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            headers = []
            if rows:
                headers = [th.get_text(strip=True).lower().replace(" ", "_")
                           for th in rows[0].find_all(["th", "td"])]
            if len(headers) < 2:
                continue

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if not cells or len(cells) < 2:
                    continue
                record = {
                    "date": date_str,
                    "source": "standardbred_ca",
                    "type": "result",
                    "discipline": "trot",
                    "country": "CA",
                    "url": url,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    record[key] = cell
                # Extract position
                if cells:
                    pos_match = re.match(r'^(\d+)', cells[0])
                    if pos_match:
                        record["position"] = int(pos_match.group(1))
                # Extract time
                for cell in cells:
                    time_match = re.search(r'(\d:[0-5]\d\.\d)', cell)
                    if time_match:
                        record["time"] = time_match.group(1)
                        break
                records.append(record)

        # -- Meeting info --
        for div in soup.find_all(["div", "section", "header", "h2", "h3"]):
            text = div.get_text(strip=True)
            classes = " ".join(div.get("class", []) if div.get("class") else [])
            if any(kw in classes.lower() for kw in ["meeting", "venue", "track",
                                                      "race-header", "event-header"]):
                if text and 3 < len(text) < 500:
                    record = {
                        "date": date_str,
                        "source": "standardbred_ca",
                        "type": "meeting_info",
                        "contenu": text[:400],
                        "scraped_at": datetime.now().isoformat(),
                    }
                    records.append(record)

        # -- Horse/driver links --
        for a in soup.find_all("a", href=True):
            href = a["href"]
            name = a.get_text(strip=True)
            if any(kw in href.lower() for kw in ["/horse/", "/driver/", "/trainer/",
                                                   "/stallion/", "/broodmare/"]):
                if name and len(name) > 2:
                    link_type = "horse_link"
                    if "/driver/" in href.lower():
                        link_type = "driver_link"
                    elif "/trainer/" in href.lower():
                        link_type = "trainer_link"
                    elif "/stallion/" in href.lower():
                        link_type = "stallion_link"
                    elif "/broodmare/" in href.lower():
                        link_type = "broodmare_link"
                    records.append({
                        "date": date_str,
                        "source": "standardbred_ca",
                        "type": link_type,
                        "name": name,
                        "profile_url": href if href.startswith("http") else BASE_URL + href,
                        "scraped_at": datetime.now().isoformat(),
                    })

        # -- Pedigree info --
        for div in soup.find_all(["div", "section", "table"], class_=True):
            classes = " ".join(div.get("class", []))
            if any(kw in classes.lower() for kw in ["pedigree", "breeding", "sire",
                                                      "dam", "bloodline"]):
                text = div.get_text(strip=True)
                if text and 5 < len(text) < 2000:
                    records.append({
                        "date": date_str,
                        "source": "standardbred_ca",
                        "type": "pedigree_info",
                        "contenu": text[:1500],
                        "scraped_at": datetime.now().isoformat(),
                    })

        # -- Stakes info --
        for div in soup.find_all(["div", "span", "p"], class_=True):
            classes = " ".join(div.get("class", []))
            text = div.get_text(strip=True)
            if any(kw in classes.lower() for kw in ["stake", "purse", "prize",
                                                      "conditions", "class"]):
                if text and 5 < len(text) < 500:
                    record = {
                        "date": date_str,
                        "source": "standardbred_ca",
                        "type": "stakes_info",
                        "contenu": text[:400],
                        "scraped_at": datetime.now().isoformat(),
                    }
                    purse_match = re.search(r'\$\s*([\d,]+)', text)
                    if purse_match:
                        record["purse_cad"] = purse_match.group(1).replace(",", "")
                    records.append(record)

        # -- Embedded JSON --
        for script in soup.find_all("script"):
            script_text = script.string or ""
            for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
                try:
                    data = json.loads(m.group(2))
                    records.append({
                        "date": date_str,
                        "source": "standardbred_ca",
                        "type": "embedded_data",
                        "var_name": m.group(1),
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                except json.JSONDecodeError:
                    pass

        for script in soup.find_all("script", {"type": "application/json"}):
            try:
                data = json.loads(script.string or "")
                records.append({
                    "date": date_str,
                    "source": "standardbred_ca",
                    "type": "script_json",
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

        smart_pause(1.0, 0.5)
        break  # Stop after first successful URL

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Script 95 -- Standardbred Canada Scraper")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 95 -- Standardbred Canada Scraper")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "standardbred_ca_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        records = scrape_sc_day(session, date_str)

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

    save_checkpoint({"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {day_count} jours, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
