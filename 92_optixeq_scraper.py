#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 92 -- Scraping OptixEQ
Source : optixeq.com - Speed figures avances, pace analysis
Collecte : speed figures, pace scenarios, contention lines, track variants
CRITIQUE pour : Speed Model, Pace Handicapping, Track Bias Analysis
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

SCRIPT_NAME = "92_optixeq"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("92_optixeq")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.optixeq.com"


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


def scrape_speed_figures_page(session, page_url, date_str=None):
    """Scrape speed figures and pace data from an OptixEQ page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', page_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"optix_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    resp = fetch_with_retry(session, page_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # -- Speed figure tables --
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
                "source": "optixeq",
                "type": "speed_figure",
                "url": page_url,
                "scraped_at": datetime.now().isoformat(),
            }
            if date_str:
                record["date"] = date_str
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            # Extract numeric speed fig
            for cell in cells:
                m = re.search(r'(\d{2,3}\.?\d*)', cell)
                if m:
                    try:
                        val = float(m.group(1))
                        if 30 <= val <= 150:
                            record["speed_value"] = val
                            break
                    except ValueError:
                        pass
            records.append(record)

    # -- Pace analysis sections --
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["pace", "contention", "speed",
                                                  "figure", "variant", "track-bias"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "source": "optixeq",
                    "type": "pace_analysis",
                    "url": page_url,
                    "contenu": text[:2500],
                    "css_classes": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                if date_str:
                    record["date"] = date_str
                records.append(record)

    # -- Embedded JSON data --
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\{[\s\S]{50,}?\});', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "optixeq",
                    "type": "embedded_data",
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
                    "source": "optixeq",
                    "type": "embedded_array",
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
                "source": "optixeq",
                "type": "script_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # -- Data attributes for figures --
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["speed", "figure", "pace", "variant", "rating", "horse"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": "optixeq",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # -- Track variant / bias grids --
    for div in soup.find_all(["div", "table", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["variant", "bias", "track-profile",
                                                  "surface", "rail"]):
            sub_table = div.find("table")
            if sub_table:
                rows = sub_table.find_all("tr")
                sub_headers = []
                if rows:
                    sub_headers = [th.get_text(strip=True).lower().replace(" ", "_")
                                   for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        entry = {
                            "source": "optixeq",
                            "type": "track_variant",
                            "url": page_url,
                            "scraped_at": datetime.now().isoformat(),
                        }
                        if date_str:
                            entry["date"] = date_str
                        for j, cell in enumerate(cells):
                            key = sub_headers[j] if j < len(sub_headers) and sub_headers[j] else f"col_{j}"
                            entry[key] = cell
                        records.append(entry)

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def discover_pages(session):
    """Discover speed figure / pace pages from main site."""
    resp = fetch_with_retry(session, BASE_URL)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    pages = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(kw in href.lower() for kw in ["speed", "pace", "figure", "result",
                                               "race", "track", "analysis", "horse",
                                               "contention", "variant"]):
            if href.startswith("/"):
                href = BASE_URL + href
            if href.startswith("http"):
                pages.add(href)

    known_paths = [
        "/speed-figures", "/pace-analysis", "/results",
        "/track-variants", "/contention-lines", "/races",
        "/horses", "/tracks", "/free-reports",
    ]
    for path in known_paths:
        pages.add(BASE_URL + path)

    return list(pages)


def main():
    parser = argparse.ArgumentParser(
        description="Script 92 -- OptixEQ Scraper (speed figures, pace analysis)")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-pages", type=int, default=500,
                        help="Nombre max de pages a scraper")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 92 -- OptixEQ Scraper")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    done_urls = set(checkpoint.get("done_urls", []))
    last_date = checkpoint.get("last_date")
    if args.resume and done_urls:
        log.info(f"  Reprise checkpoint: {len(done_urls)} pages deja traitees")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise date: {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "optixeq_data.jsonl")

    # Discover static pages
    pages = discover_pages(session)
    log.info(f"  Pages decouvertes: {len(pages)}")
    smart_pause()

    total_records = 0
    page_count = 0

    # Scrape discovered pages
    for page_url in pages:
        if page_url in done_urls:
            continue
        if page_count >= args.max_pages:
            break

        records = scrape_speed_figures_page(session, page_url)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        done_urls.add(page_url)
        page_count += 1

        if page_count % 10 == 0:
            log.info(f"  pages={page_count} records={total_records}")
            save_checkpoint({"done_urls": list(done_urls),
                             "total_records": total_records,
                             "last_date": start_date.strftime("%Y-%m-%d")})

        if page_count % 50 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        smart_pause()

    # Scrape date-based pages
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        date_url = f"{BASE_URL}/results/{date_str}"

        if date_url not in done_urls and page_count < args.max_pages:
            records = scrape_speed_figures_page(session, date_url, date_str)
            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            done_urls.add(date_url)
            page_count += 1

            if page_count % 30 == 0:
                log.info(f"  {date_str} | pages={page_count} records={total_records}")
                save_checkpoint({"done_urls": list(done_urls),
                                 "total_records": total_records,
                                 "last_date": date_str})

            if page_count % 80 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

            smart_pause(1.0, 0.5)

        current += timedelta(days=1)

    save_checkpoint({"done_urls": list(done_urls),
                     "total_records": total_records, "status": "done",
                     "last_date": end_date.strftime("%Y-%m-%d")})

    log.info("=" * 60)
    log.info(f"TERMINE: {page_count} pages, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
