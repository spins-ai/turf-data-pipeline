#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 98 -- Scraping TurfTrax
Source : turftrax.com - Going/track data UK
Collecte : GoingStick readings, track conditions, rail movements, watering
CRITIQUE pour : Going Model, Track Bias, Surface Condition Prediction
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

SCRIPT_NAME = "98_turftrax"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("98_turftrax")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.turftrax.com"

# UK racecourses for TurfTrax data
UK_COURSES = [
    "ascot", "aintree", "bath", "beverley", "brighton", "carlisle",
    "catterick", "chelmsford", "cheltenham", "chester", "chepstow",
    "doncaster", "epsom", "exeter", "fakenham", "fontwell", "goodwood",
    "haydock", "hereford", "hexham", "huntingdon", "kempton", "leicester",
    "lingfield", "ludlow", "market-rasen", "musselburgh", "newbury",
    "newcastle", "newmarket", "newton-abbot", "nottingham", "plumpton",
    "pontefract", "redcar", "ripon", "salisbury", "sandown", "sedgefield",
    "southwell", "stratford", "taunton", "thirsk", "towcester",
    "uttoxeter", "warwick", "wetherby", "wincanton", "windsor",
    "wolverhampton", "worcester", "yarmouth", "york",
]


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


def scrape_going_page(session, page_url, course_name, date_str=None):
    """Scrape going/track data from a TurfTrax page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', page_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"going_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    resp = fetch_with_retry(session, page_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # -- GoingStick readings tables --
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
                "source": "turftrax",
                "type": "going_reading",
                "course": course_name,
                "url": page_url,
                "scraped_at": datetime.now().isoformat(),
            }
            if date_str:
                record["date"] = date_str
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            # Extract GoingStick value
            for cell in cells:
                gs_match = re.search(r'(\d+\.?\d*)', cell)
                if gs_match:
                    try:
                        val = float(gs_match.group(1))
                        if 0 < val <= 20:  # GoingStick range
                            record["goingstick_value"] = val
                            break
                    except ValueError:
                        pass
            records.append(record)

    # -- Going descriptions --
    for div in soup.find_all(["div", "p", "span", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["going", "ground", "track",
                                                  "surface", "condition"]):
            if text and 3 < len(text) < 1000:
                record = {
                    "source": "turftrax",
                    "type": "going_description",
                    "course": course_name,
                    "contenu": text[:800],
                    "scraped_at": datetime.now().isoformat(),
                }
                if date_str:
                    record["date"] = date_str
                # Parse going description
                for going_term in ["Heavy", "Soft", "Good to Soft", "Good",
                                   "Good to Firm", "Firm", "Hard", "Standard",
                                   "Standard to Slow", "Slow"]:
                    if going_term.lower() in text.lower():
                        record["going_official"] = going_term
                        break
                records.append(record)

    # -- Rail movements --
    for div in soup.find_all(["div", "p", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["rail", "dolling", "realignment"]):
            if text and 5 < len(text) < 500:
                record = {
                    "source": "turftrax",
                    "type": "rail_movement",
                    "course": course_name,
                    "contenu": text[:400],
                    "scraped_at": datetime.now().isoformat(),
                }
                if date_str:
                    record["date"] = date_str
                rail_match = re.search(r'(\d+)\s*(?:yards?|metres?|m)\s*(out|in)',
                                       text, re.IGNORECASE)
                if rail_match:
                    record["rail_distance"] = rail_match.group(1)
                    record["rail_direction"] = rail_match.group(2)
                records.append(record)

    # -- Watering info --
    for div in soup.find_all(["div", "p", "span"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["water", "irrigation"]):
            if text and 5 < len(text) < 500:
                record = {
                    "source": "turftrax",
                    "type": "watering",
                    "course": course_name,
                    "contenu": text[:400],
                    "scraped_at": datetime.now().isoformat(),
                }
                if date_str:
                    record["date"] = date_str
                mm_match = re.search(r'(\d+)\s*mm', text)
                if mm_match:
                    record["watering_mm"] = int(mm_match.group(1))
                records.append(record)

    # -- Stalls positions --
    for div in soup.find_all(["div", "section", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["stalls", "draw", "start"]):
            if text and 5 < len(text) < 500:
                records.append({
                    "source": "turftrax",
                    "type": "stalls_position",
                    "course": course_name,
                    "contenu": text[:400],
                    "scraped_at": datetime.now().isoformat(),
                })

    # -- Embedded JSON --
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\{[\s\S]{50,}?\});', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "turftrax",
                    "type": "embedded_data",
                    "course": course_name,
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "turftrax",
                    "type": "embedded_array",
                    "course": course_name,
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
                "source": "turftrax",
                "type": "script_json",
                "course": course_name,
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # -- Data attributes --
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["going", "stick", "ground", "rail", "track", "surface"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": "turftrax",
                "type": "data_attributes",
                "course": course_name,
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Script 98 -- TurfTrax Going/Track Data Scraper")
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
    log.info("SCRIPT 98 -- TurfTrax Going/Track Scraper")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Courses UK : {len(UK_COURSES)}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    done_keys = set(checkpoint.get("done_keys", []))
    last_date = checkpoint.get("last_date")
    if args.resume and done_keys:
        log.info(f"  Reprise checkpoint: {len(done_keys)} pages deja traitees")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise date: {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "turftrax_data.jsonl")

    total_records = 0
    page_count = 0

    # Scrape course pages (static going info)
    for course in UK_COURSES:
        course_key = f"course_{course}"
        if course_key in done_keys:
            continue

        urls = [
            f"{BASE_URL}/courses/{course}",
            f"{BASE_URL}/{course}",
            f"{BASE_URL}/going/{course}",
        ]
        for url in urls:
            records = scrape_going_page(session, url, course)
            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1
                break
            smart_pause(1.0, 0.5)

        done_keys.add(course_key)
        page_count += 1

        if page_count % 10 == 0:
            log.info(f"  courses={page_count} records={total_records}")
            save_checkpoint({"done_keys": list(done_keys),
                             "total_records": total_records})

        if page_count % 50 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        smart_pause()

    # Scrape date-based pages
    current = start_date
    day_count = 0
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        date_key = f"date_{date_str}"

        if date_key not in done_keys:
            urls = [
                f"{BASE_URL}/going/{date_str}",
                f"{BASE_URL}/results/{date_str}",
                f"{BASE_URL}/going-reports/{date_str}",
            ]
            for url in urls:
                records = scrape_going_page(session, url, "all", date_str)
                if records:
                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1
                    break
                smart_pause(0.8, 0.3)

            done_keys.add(date_key)
            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | jours={day_count} records={total_records}")
                save_checkpoint({"done_keys": list(done_keys),
                                 "total_records": total_records,
                                 "last_date": date_str})

            if day_count % 80 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

            smart_pause(1.0, 0.5)

        current += timedelta(days=1)

    save_checkpoint({"done_keys": list(done_keys),
                     "total_records": total_records, "status": "done",
                     "last_date": end_date.strftime("%Y-%m-%d")})

    log.info("=" * 60)
    log.info(f"TERMINE: {page_count} courses + {day_count} jours, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
