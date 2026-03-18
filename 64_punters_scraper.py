#!/usr/bin/env python3
"""
Script 64 — Scraping Punters.com.au (Australian Racing)
Source : punters.com.au
Collecte : race data, form guides, tips, results, track conditions
CRITIQUE pour : Australian Racing Data, Form Analysis, Track Bias
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

SCRIPT_NAME = "64_punters"
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

BASE_URL = "https://www.punters.com.au"


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
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
    """GET with automatic retry (3 attempts then skip)."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
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


def scrape_form_guide(session, date_str):
    """Scrape Punters.com.au form guide for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"form_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/form-guide/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract meeting/track sections
    for section in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "race-card", "form-guide", "track"]):
            track_name = ""
            h_tag = section.find(["h2", "h3", "h4"])
            if h_tag:
                track_name = h_tag.get_text(strip=True)

            # Extract race links
            for link in section.find_all("a", href=True):
                href = link["href"]
                text = link.get_text(strip=True)
                if ("race" in href.lower() or "form" in href.lower()) and text and len(text) > 2:
                    records.append({
                        "date": date_str,
                        "source": "punters_au",
                        "type": "race_link",
                        "track": track_name,
                        "text": text,
                        "url": href if href.startswith("http") else BASE_URL + href,
                        "scraped_at": datetime.utcnow().isoformat(),
                    })

    # Extract form guide tables
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
                "source": "punters_au",
                "type": "form_entry",
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
    """Scrape Punters.com.au results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/results/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
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
                "source": "punters_au",
                "type": "result",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract track condition info
    for div in soup.find_all(["div", "span"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["track-condition", "rail", "weather", "going"]):
            text = div.get_text(strip=True)
            if text and len(text) > 1:
                records.append({
                    "date": date_str,
                    "source": "punters_au",
                    "type": "track_condition",
                    "value": text,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_tips(session, date_str):
    """Scrape Punters.com.au tips — expert picks and consensus."""
    cache_file = os.path.join(CACHE_DIR, f"tips_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/tips/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract tip sections
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["tip", "pick", "selection", "best-bet"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 1000:
                # Try to extract horse name and race
                horse = ""
                race = ""
                h_tag = div.find(["h3", "h4", "h5", "strong"])
                if h_tag:
                    horse = h_tag.get_text(strip=True)
                race_tag = div.find(["span", "small"], class_=lambda c: c and "race" in " ".join(c).lower() if c else False)
                if race_tag:
                    race = race_tag.get_text(strip=True)

                records.append({
                    "date": date_str,
                    "source": "punters_au",
                    "type": "tip",
                    "horse": horse,
                    "race": race,
                    "content": text[:500],
                    "scraped_at": datetime.utcnow().isoformat(),
                })

    # Extract tips from tables
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
                "source": "punters_au",
                "type": "tip_table",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 64 — Punters.com.au Scraper (AU race data, form, tips)")
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
    log.info("SCRIPT 64 — Punters.com.au Scraper (Australian Racing)")
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
    output_file = os.path.join(OUTPUT_DIR, "punters_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Scrape form guide
        form_records = scrape_form_guide(session, date_str)
        if form_records:
            for rec in form_records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(2.0, 1.0)

        # Scrape results
        result_records = scrape_results(session, date_str)
        if result_records:
            for rec in result_records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(2.0, 1.0)

        # Scrape tips
        tip_records = scrape_tips(session, date_str)
        if tip_records:
            for rec in tip_records:
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
