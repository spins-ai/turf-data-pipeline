#!/usr/bin/env python3
"""
Script 105 — GeeGeez Gold Scraper (Playwright)
Source : geegeez.co.uk
Collecte : UK racing data, form guides, speed ratings, race cards, results
URL patterns :
  /racecards/{date}     -> liste des courses du jour
  /racecards/{venue}/   -> carte de course par hippodrome
  /results/{date}       -> resultats du jour
  /form/{horse-name}/   -> fiche forme cheval
CRITIQUE pour : UK Form Guides, Speed Ratings, Going Analysis

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

SCRIPT_NAME = "105_geegeez_gold"
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

log = setup_logging("105_geegeez_gold")

BASE_URL = "https://www.geegeez.co.uk"
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


def extract_speed_ratings(soup, date_str):
    """Extract speed/performance ratings from page content."""
    records = []
    for el in soup.find_all(["div", "span", "td", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["speed", "rating", "score",
                                                   "performance", "rpr",
                                                   "topspeed", "ts-rating"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "geegeez",
                    "type": "speed_rating",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract numeric rating
                rating_match = re.search(r'(\d{1,3}(?:\.\d)?)', text)
                if rating_match:
                    record["rating_value"] = rating_match.group(1)
                records.append(record)
    return records


def extract_form_guide(soup, date_str):
    """Extract form guide data (recent runs, comments, trainer/jockey stats)."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "guide", "comment",
                                                   "analysis", "verdict",
                                                   "spotlight", "tip",
                                                   "selection", "overview"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "geegeez",
                    "type": "form_guide",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_going_data(soup, date_str):
    """Extract going/ground condition data."""
    records = []
    for el in soup.find_all(["div", "span", "p", "td"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["going", "ground", "terrain",
                                                   "surface", "track-condition"]):
            if text and 2 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "geegeez",
                    "type": "going_data",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse going description
                going_match = re.search(
                    r'(firm|good to firm|good|good to soft|soft|heavy|'
                    r'yielding|standard|slow|fast)',
                    text, re.I
                )
                if going_match:
                    record["going"] = going_match.group(1).strip()
                records.append(record)
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_day_index(page, date_str):
    """Scrape the GeeGeez day index page for race links and overview data."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racecards/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract structured data
    records.extend(extract_embedded_json_data(soup, "geegeez", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "geegeez", date_str=date_str))
    records.extend(extract_speed_ratings(soup, date_str))
    records.extend(extract_form_guide(soup, date_str))
    records.extend(extract_going_data(soup, date_str))
    records.extend(extract_runners_table(soup, "geegeez", date_str=date_str))

    # Extract venue/meeting blocks
    race_links = extract_race_links(soup, base_url=BASE_URL)
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "venue", "card",
                                                   "race-list", "racecard"]):
            record = {
                "date": date_str,
                "source": "geegeez",
                "type": "meeting",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong", "a"])
            if title:
                record["venue"] = title.get_text(strip=True)
            # Going info within meeting block
            for span in div.find_all(["span", "small", "em", "p"]):
                text = span.get_text(strip=True)
                going_match = re.search(
                    r'(going|ground)\s*:?\s*(firm|good to firm|good|good to soft|'
                    r'soft|heavy|yielding|standard|slow|fast)',
                    text, re.I
                )
                if going_match:
                    record["going"] = going_match.group(2).strip()
            records.append(record)

    result = {"records": records, "race_links": race_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape individual race card/result page for detailed runner data."""
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

    # Race title / prize name
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Race conditions
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(r'(\d+)f\b|(\d+)\s*furlongs?|(\d+)m\b|(\d[\d,]*)\s*m(?:etres?)?',
                           page_text, re.I)
    if dist_match:
        conditions["distance"] = (dist_match.group(1) or dist_match.group(2)
                                  or dist_match.group(3) or dist_match.group(4))

    going_match = re.search(
        r'going\s*:?\s*(firm|good to firm|good|good to soft|soft|heavy|'
        r'yielding|standard|slow|fast)',
        page_text, re.I
    )
    if going_match:
        conditions["going"] = going_match.group(1).strip()

    class_match = re.search(r'class\s*(\d)', page_text, re.I)
    if class_match:
        conditions["race_class"] = class_match.group(1)

    type_match = re.search(r'(flat|hurdle|chase|national hunt|nh flat|bumper)',
                           page_text, re.I)
    if type_match:
        conditions["race_type"] = type_match.group(1)

    # Structured data extraction
    records.extend(extract_embedded_json_data(soup, "geegeez", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "geegeez", date_str=date_str))
    records.extend(extract_speed_ratings(soup, date_str))
    records.extend(extract_form_guide(soup, date_str))
    records.extend(extract_going_data(soup, date_str))

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
                "source": "geegeez",
                "type": "runner_detail",
                "race_name": race_name,
                "conditions": conditions,
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            # Data attributes on row
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val
            records.append(record)

    # Verdict / tips sections
    for div in soup.find_all(["div", "p", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["verdict", "tip", "selection",
                                                   "analysis", "comment",
                                                   "spotlight", "form-summary"]):
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "geegeez",
                    "type": "verdict",
                    "race_name": race_name,
                    "conditions": conditions,
                    "contenu": text[:2000],
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Also try results pages
# ------------------------------------------------------------------

def scrape_results_day(page, date_str):
    """Scrape results page for a given date."""
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

    records.extend(extract_embedded_json_data(soup, "geegeez", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "geegeez", date_str=date_str))
    records.extend(extract_speed_ratings(soup, date_str))
    records.extend(extract_runners_table(soup, "geegeez", date_str=date_str, race_url=url))

    # Result-specific extraction
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "finishing", "placed",
                                                   "winner", "returns"]):
            text = div.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "geegeez",
                    "type": "result_block",
                    "contenu": text[:1500],
                    "classes_css": classes,
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
        description="Script 105 — GeeGeez Gold Scraper (UK form guides, speed ratings)"
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
    log.info("SCRIPT 105 — GeeGeez Gold Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "geegeez_data.jsonl")

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

            # Scrape racecards index
            result = scrape_day_index(page, date_str)

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

            # Also scrape results page
            results_data = scrape_results_day(page, date_str)
            if results_data:
                for rec in results_data:
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
