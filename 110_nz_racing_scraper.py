#!/usr/bin/env python3
"""
Script 110 — New Zealand Racing Scraper (Playwright)
Source : loveracing.nz
Collecte : NZ racing data, race fields, results, form guides, dividends
URL patterns :
  /RaceInfo/MeetingsCalendar         -> calendrier reunions
  /RaceInfo/Meeting?meetingId={id}   -> reunion avec courses
  /RaceInfo/Race?raceId={id}         -> course individuelle
  /RaceInfo/Results                  -> resultats du jour
CRITIQUE pour : NZ Racing Model, ANZAC Cross-Validation, Thoroughbred International

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
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "110_nz_racing"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.html_parsing import extract_embedded_json_data
from utils.html_parsing import extract_scraper_data_attributes
from utils.html_parsing import extract_runners_table
from utils.html_parsing import extract_race_links

log = setup_logging("110_nz_racing")

BASE_URL = "https://loveracing.nz"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000


# ------------------------------------------------------------------
# Navigation helper (returns HTML string for BS4 parsing)
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


def extract_race_conditions(soup, date_str):
    """Extract race conditions (distance, track, class, prize)."""
    records = []
    for el in soup.find_all(["div", "span", "td", "p", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["race-info", "condition", "details",
                                                   "distance", "prize", "class",
                                                   "track-condition", "rail",
                                                   "race-detail", "race-header"]):
            text = el.get_text(strip=True)
            if text and 2 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "nz_racing",
                    "type": "race_condition",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse distance (metres)
                dist_match = re.search(r'(\d{3,5})\s*m(?:etres?)?', text, re.I)
                if dist_match:
                    record["distance_m"] = dist_match.group(1)
                # Parse track condition (NZ uses similar to AU)
                track_match = re.search(
                    r'(firm|good|dead|slow|heavy|synthetic)\s*(\d)?',
                    text, re.I
                )
                if track_match:
                    record["track_condition"] = track_match.group(0).strip()
                records.append(record)
    return records


def extract_results_data(soup, date_str, race_url=""):
    """Extract finishing positions, margins, and dividends from results pages."""
    records = []
    for el in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "finish", "placing",
                                                   "winner", "dividend",
                                                   "returns", "payout",
                                                   "tote", "fixed-odds"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "nz_racing",
                    "type": "result_block",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_form_data(soup, date_str):
    """Extract form/history data for horses."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "history", "career",
                                                   "record", "stats",
                                                   "performance", "last-starts",
                                                   "comment", "gear"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "nz_racing",
                    "type": "form_data",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_day_races(page, date_str):
    """Scrape loveracing.nz races page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/RaceInfo/Results?date={date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"day_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "nz_racing", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "nz_racing", date_str=date_str))
    records.extend(extract_race_conditions(soup, date_str))
    records.extend(extract_runners_table(soup, "nz_racing", date_str=date_str))
    records.extend(extract_form_data(soup, date_str))
    records.extend(extract_results_data(soup, date_str))

    race_links = extract_race_links(soup, base_url=BASE_URL)

    # Extract venue/meeting blocks
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "venue", "card",
                                                   "race-list", "raceday"]):
            record = {
                "date": date_str,
                "source": "nz_racing",
                "type": "meeting",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong", "a"])
            if title:
                record["venue"] = title.get_text(strip=True)
            # Code type (T=Thoroughbred, H=Harness, G=Greyhound)
            code_match = re.search(r'\b(Thoroughbred|Harness|Greyhound)\b',
                                   div.get_text(strip=True), re.I)
            if code_match:
                record["code"] = code_match.group(1)
            records.append(record)

    result = {"records": records, "race_links": race_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape individual race page for detailed runner data."""
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

    # Race title
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Race conditions
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(r'(\d{3,5})\s*m(?:etres?)?', page_text, re.I)
    if dist_match:
        conditions["distance_m"] = dist_match.group(1)

    track_match = re.search(
        r'track\s*(?:condition|rating)?\s*:?\s*(firm|good|dead|slow|heavy|synthetic)\s*(\d)?',
        page_text, re.I
    )
    if track_match:
        conditions["track_condition"] = track_match.group(0).strip()

    class_match = re.search(r'(group\s*[123]|listed|rating\s*\d+|maiden|open)',
                            page_text, re.I)
    if class_match:
        conditions["race_class"] = class_match.group(1)

    prize_match = re.search(r'\$\s*([\d,]+)', page_text)
    if prize_match:
        conditions["prize_money"] = prize_match.group(1)

    records.extend(extract_embedded_json_data(soup, "nz_racing", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "nz_racing", date_str=date_str))
    records.extend(extract_race_conditions(soup, date_str))
    records.extend(extract_results_data(soup, date_str, race_url=race_url))
    records.extend(extract_form_data(soup, date_str))

    # Runners table
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
                "source": "nz_racing",
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

    # Dividends section
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["dividend", "tote", "payout",
                                                   "returns", "fixed-odds"]):
            text = div.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "nz_racing",
                    "type": "dividend",
                    "race_name": race_name,
                    "contenu": text[:1500],
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 110 — NZ Racing Scraper (loveracing.nz)"
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
    log.info("SCRIPT 110 — NZ Racing Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "nz_racing_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-NZ", timezone="Pacific/Auckland"
        )
        log.info("Browser launched (headless Chromium, locale=en-NZ)")

        first_nav = True
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")

            # Scrape day races
            result = scrape_day_races(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual race pages (limit to avoid overload)
                for race_url in result.get("race_links", [])[:15]:
                    detail = scrape_race_detail(page, race_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
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
