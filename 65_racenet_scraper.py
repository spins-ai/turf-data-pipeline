#!/usr/bin/env python3
"""
Script 65 — Scraping Racenet.com.au (Australian Racing)
Source : racenet.com.au
Collecte : race cards, results, stats, horse profiles, track data
CRITIQUE pour : Australian Race Cards, Results, Horse Statistics
Backend : Playwright (headless Chromium) — bypasses Cloudflare
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser

SCRIPT_NAME = "65_racenet"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.html_parsing import extract_embedded_json, extract_data_attributes

log = setup_logging("65_racenet")

BASE_URL = "https://www.racenet.com.au"




# NOTE: Local version kept because it returns HTML string (page.content()) instead of bool
def navigate_with_retry(page, url, retries=3):
    """Navigate to a URL with retry logic. Returns page HTML or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=60_000)
            if resp and resp.status >= 400:
                log.warning("  HTTP %d on %s (attempt %d/%d)", resp.status, url, attempt, retries)
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
            log.warning("  Navigation error: %s (attempt %d/%d)", str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


def extract_comments(soup, date_str, source="racenet_au"):
    """Extract comments, tips consensus and analysis divs."""
    records = []
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "preview", "analysis",
                                                   "verdict", "expert", "consensus",
                                                   "race-comment", "tip-text"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": source,
                    "type": "comment",
                    "content": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_form_detailed(soup, date_str, source="racenet_au"):
    """Extract detailed form data from Racenet."""
    records = []
    for el in soup.find_all(["div", "span", "td", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "history", "past-run",
                                                   "recent-form", "career",
                                                   "performance", "record"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "form_detailed",
                    "content": text,
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                form_match = re.search(r'([0-9xX]{3,})', text)
                if form_match:
                    record["form_string"] = form_match.group(1)
                records.append(record)
    return records


def extract_track_distance_stats(soup, date_str, source="racenet_au"):
    """Extract stats by track and distance from Racenet."""
    records = []
    for el in soup.find_all(["div", "table", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["track-stats", "distance-stats",
                                                   "course-stats", "stat-by-track",
                                                   "stat-by-distance", "track-record"]):
            if el.name == "table":
                rows = el.find_all("tr")
                headers = []
                if rows:
                    headers = [th.get_text(strip=True).lower().replace(" ", "_")
                               for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        record = {
                            "date": date_str,
                            "source": source,
                            "type": "track_distance_stats",
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                            record[key] = cell
                        records.append(record)
            else:
                text = el.get_text(strip=True)
                if text and 5 < len(text) < 1000:
                    record = {
                        "date": date_str,
                        "source": source,
                        "type": "track_distance_stats_text",
                        "content": text[:500],
                        "classes_css": classes,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    pcts = re.findall(r'(\d{1,3})\s*%', text)
                    if pcts:
                        record["percentages"] = pcts[:10]
                    records.append(record)
    return records


def extract_tips_consensus(soup, date_str, source="racenet_au"):
    """Extract tips consensus data from Racenet."""
    records = []
    for el in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["consensus", "tip-count",
                                                   "tipster-count", "tips-summary",
                                                   "expert-picks", "selections"]):
            if el.name == "table":
                rows = el.find_all("tr")
                headers = []
                if rows:
                    headers = [th.get_text(strip=True).lower().replace(" ", "_")
                               for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        record = {
                            "date": date_str,
                            "source": source,
                            "type": "tips_consensus",
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                            record[key] = cell
                        records.append(record)
            else:
                text = el.get_text(strip=True)
                if text and 5 < len(text) < 1000:
                    records.append({
                        "date": date_str,
                        "source": source,
                        "type": "tips_consensus_text",
                        "content": text[:500],
                        "classes_css": classes,
                        "scraped_at": datetime.now().isoformat(),
                    })
    return records


def scrape_race_cards(page, date_str):
    """Scrape Racenet race cards for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/fields/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, "racenet_au"))
    records.extend(extract_data_attributes(soup, date_str, "racenet_au"))
    records.extend(extract_comments(soup, date_str, "racenet_au"))
    records.extend(extract_form_detailed(soup, date_str, "racenet_au"))
    records.extend(extract_track_distance_stats(soup, date_str, "racenet_au"))
    records.extend(extract_tips_consensus(soup, date_str, "racenet_au"))

    # Extract meeting links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if ("/racing/" in href or "/fields/" in href) and text and len(text) > 2:
            records.append({
                "date": date_str,
                "source": "racenet_au",
                "type": "meeting_link",
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
                "source": "racenet_au",
                "type": "race_card",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract runner cards from divs
    for div in soup.find_all(["div", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["runner", "horse", "entry", "starter"]):
            horse_name = ""
            jockey = ""
            trainer = ""
            barrier = ""
            weight = ""

            name_el = div.find(["a", "span", "strong"], class_=lambda c: c and any(
                kw in " ".join(c).lower() for kw in ["name", "horse", "runner"]
            ) if c else False)
            if name_el:
                horse_name = name_el.get_text(strip=True)

            for span in div.find_all(["span", "div"], class_=True):
                sc = " ".join(span.get("class", []))
                txt = span.get_text(strip=True)
                if "jockey" in sc.lower():
                    jockey = txt
                elif "trainer" in sc.lower():
                    trainer = txt
                elif "barrier" in sc.lower():
                    barrier = txt
                elif "weight" in sc.lower():
                    weight = txt

            if horse_name:
                records.append({
                    "date": date_str,
                    "source": "racenet_au",
                    "type": "runner",
                    "horse": horse_name,
                    "jockey": jockey,
                    "trainer": trainer,
                    "barrier": barrier,
                    "weight": weight,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(page, date_str):
    """Scrape Racenet results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/results/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, "racenet_au"))
    records.extend(extract_data_attributes(soup, date_str, "racenet_au"))
    records.extend(extract_comments(soup, date_str, "racenet_au"))

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
                "source": "racenet_au",
                "type": "result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract track condition and race info
    for div in soup.find_all(["div", "span"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["track-condition", "going", "rail", "weather"]):
            text = div.get_text(strip=True)
            if text:
                records.append({
                    "date": date_str,
                    "source": "racenet_au",
                    "type": "track_info",
                    "value": text,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_stats(page, date_str):
    """Scrape Racenet stats — jockey/trainer stats, track stats."""
    cache_file = os.path.join(CACHE_DIR, f"stats_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/stats"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, "racenet_au"))
    records.extend(extract_data_attributes(soup, date_str, "racenet_au"))
    records.extend(extract_track_distance_stats(soup, date_str, "racenet_au"))

    # Extract stats tables (jockey/trainer leaderboards)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        table_type = "stats"
        header_text = " ".join(headers).lower()
        if "jockey" in header_text:
            table_type = "jockey_stats"
        elif "trainer" in header_text:
            table_type = "trainer_stats"
        elif "track" in header_text:
            table_type = "track_stats"

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "date": date_str,
                "source": "racenet_au",
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
    parser = argparse.ArgumentParser(description="Script 65 — Racenet Scraper (AU race cards, results, stats) [Playwright]")
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
    log.info("SCRIPT 65 — Racenet Scraper (Australian Racing) [Playwright]")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "racenet_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = launch_browser(pw, locale="en-AU", timezone="Australia/Sydney")
    try:
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Scrape race cards
            card_records = scrape_race_cards(page, date_str)
            if card_records:
                for rec in card_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.0, 1.0)

            # Scrape results
            result_records = scrape_results(page, date_str)
            if result_records:
                for rec in result_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.0, 1.0)

            # Scrape stats (weekly to avoid redundancy)
            if current.weekday() == 0:
                stats_records = scrape_stats(page, date_str)
                if stats_records:
                    for rec in stats_records:
                        append_jsonl(output_file, rec)
                        total_records += 1
                smart_pause(2.0, 1.0)

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | days={day_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

            if day_count % 80 == 0:
                # Rotate browser context to avoid detection
                context.close()
                browser.close()
                browser, context, page = launch_browser(pw, locale="en-AU", timezone="Australia/Sydney")
                time.sleep(random.uniform(5, 15))

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
        log.info("=" * 60)
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
