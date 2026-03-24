#!/usr/bin/env python3
"""
Script 156 — At The Races Free Racecards Scraper (Playwright)
Source : attheraces.com (free section)
Collecte : Free racecards with commentary, going data, runner info
URL patterns :
  /racecard/{date}/{venue}/{time}  -> carte de course
  /racecards/{date}                -> liste des courses du jour
  /results/{date}                  -> resultats du jour
CRITIQUE pour : Free Racecards, Going Data, Commentary, Runner Info

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "156_attheraces_free"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.playwright import launch_browser, accept_cookies
from utils.html_parsing import extract_embedded_json_data
from utils.html_parsing import extract_scraper_data_attributes
from utils.html_parsing import extract_runners_table
from utils.html_parsing import extract_race_links

log = setup_logging("156_attheraces_free")

BASE_URL = "https://www.attheraces.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000


# ------------------------------------------------------------------
# Navigation helper
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Extraction helpers
# ------------------------------------------------------------------

def extract_going_data(soup, date_str):
    """Extract going/ground condition data."""
    records = []
    for el in soup.find_all(["div", "span", "p", "td", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["going", "ground", "terrain",
                                                   "surface", "track-condition",
                                                   "course-info", "conditions"]):
            if text and 2 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "attheraces_free",
                    "type": "going_data",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                going_match = re.search(
                    r'(firm|good to firm|good|good to soft|soft|heavy|'
                    r'yielding|standard|slow|fast)',
                    text, re.I
                )
                if going_match:
                    record["going"] = going_match.group(1).strip()
                records.append(record)
    return records


def extract_commentary(soup, date_str):
    """Extract race commentary and analysis from free sections."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["commentary", "comment", "analysis",
                                                   "preview", "verdict", "tip",
                                                   "selection", "spotlight",
                                                   "race-preview", "race-comment"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "attheraces_free",
                    "type": "commentary",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_race_conditions(soup, date_str):
    """Extract race conditions (distance, class, prize, etc.)."""
    records = []
    for el in soup.find_all(["div", "span", "p", "header"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["race-info", "race-header",
                                                   "race-conditions", "race-details",
                                                   "race-meta", "distance",
                                                   "prize", "class"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": "attheraces_free",
                    "type": "race_conditions",
                    "contenu": text[:500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                dist_match = re.search(r'(\d+)f|(\d+)\s*furlongs?|(\d+)m',
                                       text, re.I)
                if dist_match:
                    record["distance"] = (dist_match.group(1) or
                                          dist_match.group(2) or
                                          dist_match.group(3))
                records.append(record)
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_day_racecards(page, date_str):
    """Scrape At The Races free racecards for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racecard/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"racecards_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "attheraces_free", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "attheraces_free", date_str=date_str))
    records.extend(extract_going_data(soup, date_str))
    records.extend(extract_commentary(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str))
    records.extend(extract_runners_table(soup, "attheraces_free", date_str=date_str, race_url=url))

    race_links = extract_race_links(soup, base_url=BASE_URL)

    result = {"records": records, "race_links": race_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape individual racecard page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', race_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"race_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, race_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(r'(\d+)f\b|(\d+)\s*furlongs?|(\d+)m\b',
                           page_text, re.I)
    if dist_match:
        conditions["distance"] = (dist_match.group(1) or dist_match.group(2)
                                  or dist_match.group(3))

    going_match = re.search(
        r'going\s*:?\s*(firm|good to firm|good|good to soft|soft|heavy|'
        r'yielding|standard|slow|fast)',
        page_text, re.I
    )
    if going_match:
        conditions["going"] = going_match.group(1).strip()

    records.extend(extract_embedded_json_data(soup, "attheraces_free", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "attheraces_free", date_str=date_str))
    records.extend(extract_going_data(soup, date_str))
    records.extend(extract_commentary(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str))

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "date": date_str,
                "source": "attheraces_free",
                "type": "runner_detail",
                "race_name": race_name,
                "conditions": conditions,
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_day_results(page, date_str):
    """Scrape At The Races results page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/results/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "attheraces_free", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "attheraces_free", date_str=date_str))
    records.extend(extract_going_data(soup, date_str))
    records.extend(extract_runners_table(soup, "attheraces_free", date_str=date_str, race_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 156 — At The Races Free Racecards Scraper (commentary, going)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=yesterday")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
                else datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 156 — At The Races Free Racecards Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "attheraces_free_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-GB", timezone="Europe/London"
        )
        log.info("Browser launched (headless Chromium, locale=en-GB)")

        first_nav = True
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")

            # Scrape racecards
            rc_result = scrape_day_racecards(page, date_str)
            if first_nav and rc_result is not None:
                accept_cookies(page)
                first_nav = False

            if rc_result:
                records = rc_result.get("records", [])
                for race_url in rc_result.get("race_links", [])[:15]:
                    detail = scrape_race_detail(page, race_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Scrape results
            res_result = scrape_day_results(page, date_str)
            if res_result:
                for rec in res_result:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 10 == 0:
                log.info("  %s | days=%d records=%d", date_str, day_count, total_records)
                save_checkpoint(CHECKPOINT_FILE, {
                    "last_date": date_str,
                    "total_records": total_records,
                })

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {
            "last_date": end_date.strftime("%Y-%m-%d"),
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: %d days, %d records -> %s", day_count, total_records, output_file)
        log.info("=" * 60)

    finally:
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
