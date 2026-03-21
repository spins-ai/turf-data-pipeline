#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 93 -- Scraping Raceform.co.uk
Source : raceform.co.uk - UK form database
Collecte : form guides, race cards, results, horse profiles, going data
CRITIQUE pour : UK Form Analysis, Cross-Market Validation, Going Impact
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "93_raceform"
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.raceform.co.uk"


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def smart_pause(base=2.5, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
    time.sleep(max(1.0, pause))


def fetch_with_retry(session, url, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Too Many Requests, pause {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                log.warning(f"  403 Forbidden sur {url}, pause 60s...")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} sur {url} (essai {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Erreur reseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Echec apres {max_retries} essais: {url}")
    return None


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


def scrape_raceform_day(session, date_str):
    """Scrape race cards and results for a given day."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    # Try multiple URL patterns
    urls_to_try = [
        f"{BASE_URL}/racecards/{date_str}",
        f"{BASE_URL}/results/{date_str}",
        f"{BASE_URL}/racing/{date_str}",
        f"{BASE_URL}/cards/{date_str}",
    ]

    records = []
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # -- Race cards / results tables --
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
                    "source": "raceform",
                    "type": "race_entry",
                    "url": url,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    record[key] = cell
                records.append(record)

        # -- Race headers (meeting, going, distance) --
        for header_div in soup.find_all(["div", "section", "header"], class_=True):
            classes = " ".join(header_div.get("class", []))
            if any(kw in classes.lower() for kw in ["race-header", "meeting", "card-header",
                                                      "race-info", "conditions"]):
                text = header_div.get_text(strip=True)
                if text and 5 < len(text) < 1000:
                    record = {
                        "date": date_str,
                        "source": "raceform",
                        "type": "race_header",
                        "contenu": text[:800],
                        "scraped_at": datetime.now().isoformat(),
                    }
                    # Extract going
                    going_match = re.search(r'(?:going|ground)[:\s]*([A-Za-z\s\-/]+)',
                                            text, re.IGNORECASE)
                    if going_match:
                        record["going"] = going_match.group(1).strip()
                    # Extract distance
                    dist_match = re.search(r'(\d+[mf]\s*\d*[yf]?|\d+\s*(?:miles?|furlongs?))',
                                           text, re.IGNORECASE)
                    if dist_match:
                        record["distance"] = dist_match.group(1).strip()
                    records.append(record)

        # -- Form guides / comments --
        for div in soup.find_all(["div", "p", "span"], class_=True):
            classes = " ".join(div.get("class", []))
            text = div.get_text(strip=True)
            if any(kw in classes.lower() for kw in ["form", "comment", "analysis",
                                                      "spotlight", "verdict", "tip"]):
                if text and 10 < len(text) < 3000:
                    records.append({
                        "date": date_str,
                        "source": "raceform",
                        "type": "form_comment",
                        "contenu": text[:2500],
                        "scraped_at": datetime.now().isoformat(),
                    })

        # -- Horse profile links --
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(kw in href.lower() for kw in ["/horse/", "/profile/", "/runner/"]):
                records.append({
                    "date": date_str,
                    "source": "raceform",
                    "type": "horse_link",
                    "horse_name": a.get_text(strip=True),
                    "horse_url": href if href.startswith("http") else BASE_URL + href,
                    "scraped_at": datetime.now().isoformat(),
                })

        # -- Embedded JSON --
        for script in soup.find_all("script"):
            script_text = script.string or ""
            for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
                try:
                    data = json.loads(m.group(2))
                    records.append({
                        "date": date_str,
                        "source": "raceform",
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
                    "source": "raceform",
                    "type": "script_json",
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

        # -- Data attributes --
        for el in soup.find_all(attrs=lambda attrs: attrs and any(
                k.startswith("data-") and any(kw in k for kw in
                ["horse", "race", "runner", "form", "going", "odds"])
                for k in attrs)):
            data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
            if data_attrs:
                records.append({
                    "date": date_str,
                    "source": "raceform",
                    "type": "data_attributes",
                    "tag": el.name,
                    "text": el.get_text(strip=True)[:200],
                    "attributes": data_attrs,
                    "scraped_at": datetime.now().isoformat(),
                })

        smart_pause(1.0, 0.5)

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Script 93 -- Raceform UK Scraper (form database)")
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
    log.info("SCRIPT 93 -- Raceform UK Scraper")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date > start_date:
            start_date = resume_date
            log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "raceform_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        records = scrape_raceform_day(session, date_str)

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
