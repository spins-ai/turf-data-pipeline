#!/usr/bin/env python3
"""
Script 66 — Scraping racing.hkjc.com (Hong Kong Jockey Club)
Source : racing.hkjc.com
Collecte : sectional times, GPS tracking, results, race cards, dividends
CRITIQUE pour : HK Sectionals, GPS Data, Race Analysis, Pace Model
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

SCRIPT_NAME = "66_hkjc"
OUTPUT_DIR = os.path.join("output", SCRIPT_NAME)
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

BASE_URL = "https://racing.hkjc.com"
RESULTS_URL = f"{BASE_URL}/racing/information/English/Racing/LocalResults.aspx"
ENTRIES_URL = f"{BASE_URL}/racing/information/English/Racing/RaceCard.aspx"
SECTIONALS_URL = f"{BASE_URL}/racing/information/English/Racing/SectionalTime.aspx"


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-HK,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Referer": BASE_URL,
    })
    return s


def smart_pause(base=3.0, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
    time.sleep(max(1.5, pause))


def fetch_with_retry(session, url, max_retries=3, timeout=30, params=None):
    """GET with automatic retry (3 attempts then skip)."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout, params=params)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Too Many Requests, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                log.warning(f"  403 Forbidden on {url}, waiting 60s...")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} on {url} (attempt {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Network error: {e} (attempt {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Failed after {max_retries} attempts: {url}")
    return None


def append_jsonl(filepath, record):
    """Append a JSONL record (append mode)."""
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    """Load resume checkpoint."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    """Save checkpoint."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def scrape_race_card(session, date_str):
    """Scrape HKJC race card for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecard_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # HKJC uses DD/MM/YYYY format
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hkjc_date = dt.strftime("%d/%m/%Y")

    params = {"RaceDate": hkjc_date}
    resp = fetch_with_retry(session, ENTRIES_URL, params=params)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract race links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if "RaceCard" in href and text and len(text) > 1:
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "race_link",
                "text": text,
                "url": href if href.startswith("http") else BASE_URL + href,
                "scraped_at": datetime.utcnow().isoformat(),
            })

    # Extract race card tables
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
                "source": "hkjc",
                "type": "race_card_entry",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(session, date_str):
    """Scrape HKJC race results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hkjc_date = dt.strftime("%d/%m/%Y")

    params = {"RaceDate": hkjc_date}
    resp = fetch_with_retry(session, RESULTS_URL, params=params)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract results tables
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
                "source": "hkjc",
                "type": "result",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Parse finish time
                time_match = re.search(r'(\d+:\d+\.\d+)', cell)
                if time_match:
                    record["finish_time_parsed"] = time_match.group(1)

            records.append(record)

    # Extract dividend/payout info
    for div in soup.find_all(["div", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["dividend", "payout", "pool"]):
            text = div.get_text(strip=True)
            if text and len(text) > 2:
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "dividend",
                    "content": text[:1000],
                    "scraped_at": datetime.utcnow().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_sectionals(session, date_str):
    """Scrape HKJC sectional times and GPS data."""
    cache_file = os.path.join(CACHE_DIR, f"sectionals_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hkjc_date = dt.strftime("%d/%m/%Y")

    params = {"RaceDate": hkjc_date}
    resp = fetch_with_retry(session, SECTIONALS_URL, params=params)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract sectional time tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        # Detect if this is a sectional table
        header_text = " ".join(headers).lower()
        is_sectional = any(kw in header_text for kw in ["sectional", "section", "200m", "400m", "furlong"])

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "date": date_str,
                "source": "hkjc",
                "type": "sectional_time" if is_sectional else "timing_data",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Parse sectional time values
                sec_match = re.search(r'(\d+\.\d{1,2})', cell)
                if sec_match and j > 0:
                    record[f"sec_{j}_parsed"] = float(sec_match.group(1))

            records.append(record)

    # Extract GPS/tracking data elements
    for el in soup.find_all(["div", "span", "td"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["gps", "tracking", "position", "sectional"]):
            text = el.get_text(strip=True)
            if text and re.search(r'\d', text):
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "gps_data",
                    "value": text,
                    "classes": classes,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

    # Extract race replay / running position data
    for script in soup.find_all("script"):
        script_text = script.string or ""
        if any(kw in script_text.lower() for kw in ["runposition", "gps", "sectiontime", "trackingdata"]):
            # Try to extract JSON data from script
            json_matches = re.findall(r'\{[^{}]{20,}\}', script_text)
            for jm in json_matches[:10]:
                try:
                    data = json.loads(jm)
                    records.append({
                        "date": date_str,
                        "source": "hkjc",
                        "type": "embedded_data",
                        "data": data,
                        "scraped_at": datetime.utcnow().isoformat(),
                    })
                except json.JSONDecodeError:
                    pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 66 — HKJC Scraper (HK sectionals, GPS, results)")
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
    log.info("SCRIPT 66 — HKJC Scraper (Hong Kong Racing)")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date > start_date:
            start_date = resume_date
            log.info(f"  Resuming from checkpoint: {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "hkjc_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    # HKJC races typically on Wed & Sun — but scrape all days in case
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Scrape race card
        card_records = scrape_race_card(session, date_str)
        if card_records:
            for rec in card_records:
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

        # Scrape sectional times
        sect_records = scrape_sectionals(session, date_str)
        if sect_records:
            for rec in sect_records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | days={day_count} records={total_records}")
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
    log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
