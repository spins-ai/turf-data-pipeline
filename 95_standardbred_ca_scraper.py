#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 95 -- Scraping Standardbred Canada (Playwright version)
Source : standardbredcanada.ca - Standardbred Canada trot data
Collecte : results trot CA, pedigrees, stakes, horse records, driver stats
CRITIQUE pour : Trot Model International, North America Harness Data

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
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

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "95_standardbred_ca"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, load_checkpoint, save_checkpoint, append_jsonl
from utils.html_parsing import extract_embedded_json, extract_data_attributes

log = setup_logging("95_standardbred_ca")

MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

BASE_URL = "https://www.standardbredcanada.ca"


# NOTE: Local version kept because it returns HTML string (page.content()) instead of bool
def navigate_with_retry(page, url, retries=MAX_RETRIES):
    """Navigate to url with retry logic. Returns HTML string or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT_MS)
            if resp and resp.status >= 400:
                log.warning("  HTTP %d on %s (attempt %d/%d)",
                            resp.status, url, attempt, retries)
                if resp.status == 429:
                    time.sleep(60 * attempt)
                elif resp.status == 403:
                    time.sleep(30 * attempt)
                else:
                    time.sleep(5 * attempt)
                continue
            page.wait_for_load_state("domcontentloaded")
            time.sleep(1.5)
            return page.content()
        except PlaywrightTimeout:
            log.warning("  Timeout on %s (attempt %d/%d)", url, attempt, retries)
            time.sleep(10 * attempt)
        except Exception as exc:
            log.warning("  Navigation error: %s (attempt %d/%d)",
                        str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


def scrape_sc_day(page, date_str):
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
        html = navigate_with_retry(page, url)
        if not html:
            continue

        # Save raw HTML to cache
        url_hash = re.sub(r'[^a-zA-Z0-9]', '_', url[-60:])
        html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}_{url_hash}.html")
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html)

        soup = BeautifulSoup(html, "html.parser")

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
        records.extend(extract_embedded_json(soup, date_str, "standardbred_ca"))
        records.extend(extract_data_attributes(soup, date_str, "standardbred_ca"))

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
    log.info("SCRIPT 95 -- Standardbred Canada Scraper (Playwright)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "standardbred_ca_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(pw)
        log.info("Browser launched (headless Chromium)")

        # Accept cookies on first navigation
        first_nav = True

        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            records = scrape_sc_day(page, date_str)

            if first_nav and records is not None:
                accept_cookies(page)
                first_nav = False

            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | jours={day_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"TERMINE: {day_count} jours, {total_records} records -> {output_file}")
        log.info("=" * 60)

    finally:
        # Graceful cleanup
        try:
            if page and not page.is_closed():
                page.close()
        except Exception:
            pass
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass
        log.info("Browser closed")


if __name__ == "__main__":
    main()
