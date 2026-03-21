#!/usr/bin/env python3
"""
Script 59 — Scraping Racing TV
Source : racingtv.com
Collecte : replays metadata, race data, meeting schedules, race cards
CRITIQUE pour : Replay Analysis, Race Metadata, Meeting Coverage
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

SCRIPT_NAME = "59_racing_tv"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("59_racing_tv")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.racingtv.com"


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






def scrape_racecards(session, date_str):
    """Scrape Racing TV race cards and meeting schedule for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racecards/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract meeting / race links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if any(kw in href.lower() for kw in ["racecard", "race-card", "meeting", "/racecards/"]):
            text = link.get_text(strip=True)
            if text and len(text) > 2:
                records.append({
                    "date": date_str,
                    "source": "racing_tv",
                    "type": "race_link",
                    "text": text,
                    "url": href if href.startswith("http") else BASE_URL + href,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract race card tables
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
                "source": "racing_tv",
                "type": "racecard_entry",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract meeting schedule sections
    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "schedule", "fixture", "programme"]):
            text = section.get_text(strip=True)
            if text and 5 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "racing_tv",
                    "type": "meeting_schedule",
                    "content": text[:400],
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract course name
                course_el = section.find(["h2", "h3", "h4", "a"])
                if course_el:
                    record["course"] = course_el.get_text(strip=True)
                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(session, date_str):
    """Scrape Racing TV results for a given date."""
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

        race_name = ""
        prev = table.find_previous(["h2", "h3", "h4"])
        if prev:
            race_name = prev.get_text(strip=True)

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "date": date_str,
                "source": "racing_tv",
                "type": "result",
                "race_name": race_name,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Parse position
            if cells:
                pos_match = re.match(r'^(\d+)(st|nd|rd|th)?$', cells[0].strip(), re.IGNORECASE)
                if pos_match:
                    record["position_parsed"] = int(pos_match.group(1))

            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_replays(session, date_str):
    """Scrape Racing TV replays metadata for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"replays_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/replays/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract replay entries (video metadata, not the video itself)
    for section in soup.find_all(["div", "article", "li"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["replay", "video", "race-replay", "media"]):
            record = {
                "date": date_str,
                "source": "racing_tv",
                "type": "replay_metadata",
                "scraped_at": datetime.now().isoformat(),
            }

            # Race title
            title_el = section.find(["h2", "h3", "h4", "a"])
            if title_el:
                record["race_title"] = title_el.get_text(strip=True)
                href = title_el.get("href")
                if href:
                    record["replay_url"] = href if href.startswith("http") else BASE_URL + href

            # Time
            time_el = section.find(["span", "time", "div"],
                                   class_=lambda c: c and any(k in " ".join(c).lower()
                                                              for k in ["time", "clock", "schedule"]))
            if time_el:
                record["race_time"] = time_el.get_text(strip=True)

            # Course
            course_el = section.find(["span", "a", "div"],
                                     class_=lambda c: c and any(k in " ".join(c).lower()
                                                                for k in ["course", "venue", "track"]))
            if course_el:
                record["course"] = course_el.get_text(strip=True)

            # Duration
            duration_el = section.find(["span", "div"],
                                       class_=lambda c: c and "duration" in " ".join(c).lower())
            if duration_el:
                record["duration"] = duration_el.get_text(strip=True)

            # Data attributes (video IDs, etc.)
            for attr in ["data-video-id", "data-race-id", "data-meeting-id", "data-replay-id"]:
                val = section.get(attr)
                if val:
                    record[attr.replace("data-", "")] = val

            if record.get("race_title") or record.get("replay_url"):
                records.append(record)

    # Extract replay links from the page
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "replay" in href.lower() and href not in [r.get("replay_url") for r in records]:
            text = link.get_text(strip=True)
            if text and len(text) > 3:
                records.append({
                    "date": date_str,
                    "source": "racing_tv",
                    "type": "replay_link",
                    "text": text,
                    "url": href if href.startswith("http") else BASE_URL + href,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_race_detail(session, race_url, date_str):
    """Scrape individual race detail page for full metadata."""
    if not race_url.startswith("http"):
        race_url = BASE_URL + race_url

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', race_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, race_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Race title
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Race conditions
    conditions = {}
    for el in soup.find_all(["span", "div", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if not text:
            continue
        if "distance" in classes.lower():
            conditions["distance"] = text
        elif "class" in classes.lower():
            conditions["race_class"] = text
        elif "going" in classes.lower():
            conditions["going"] = text
        elif "prize" in classes.lower():
            conditions["prize"] = text
        elif "runners" in classes.lower():
            conditions["num_runners"] = text

    # Runner tables
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
                "source": "racing_tv",
                "type": "race_detail",
                "race_name": race_name,
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            record.update(conditions)
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 59 — Racing TV Scraper (replays metadata, race data)")
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
    log.info("SCRIPT 59 — Racing TV Scraper")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "racing_tv_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Scrape race cards
        racecard_records = scrape_racecards(session, date_str)
        if racecard_records:
            # Scrape detail pages
            race_urls = [r.get("url") for r in racecard_records
                         if r.get("type") == "race_link" and r.get("url")]
            for rurl in list(set(race_urls))[:15]:
                detail = scrape_race_detail(session, rurl, date_str)
                if detail:
                    racecard_records.extend(detail)
                smart_pause(1.5, 0.8)

            for rec in racecard_records:
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

        # Scrape replays metadata
        replay_records = scrape_replays(session, date_str)
        if replay_records:
            for rec in replay_records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | days={day_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

        if day_count % 80 == 0:
            session.close()
            session = new_session()
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
