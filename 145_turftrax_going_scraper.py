#!/usr/bin/env python3
"""
Script 145 — TurfTrax Going Scraper (Playwright)
Source : turftrax.com
Collecte : GoingStick readings, going data, track conditions, moisture levels
URL patterns :
  /going-reports/           -> daily going reports by course
  /going-reports/{venue}/   -> venue-specific going history
  /goingstick/              -> GoingStick real-time readings
  /results/{date}           -> results with going conditions
CRITIQUE pour : GoingStick Data, Going Reports, Track Condition Analysis

Section TODO : 7Q

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

SCRIPT_NAME = "145_turftrax_going"
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

log = setup_logging("145_turftrax_going")

BASE_URL = "https://www.turftrax.com"
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

def extract_goingstick_readings(soup, date_str):
    """Extract GoingStick numeric readings from page content."""
    records = []
    for el in soup.find_all(["div", "span", "td", "section", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["goingstick", "going-stick",
                                                   "stick-reading", "gs-value",
                                                   "reading", "measurement",
                                                   "going-value"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "turftrax",
                    "type": "goingstick_reading",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract numeric GoingStick value (typically 2.0 - 14.0)
                gs_match = re.search(r'(\d{1,2}\.\d{1,2})', text)
                if gs_match:
                    record["goingstick_value"] = gs_match.group(1)
                records.append(record)
    return records


def extract_going_reports(soup, date_str):
    """Extract going description reports (firm, good, soft, etc.)."""
    records = []
    for el in soup.find_all(["div", "span", "p", "td", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["going", "ground", "terrain",
                                                   "surface", "condition",
                                                   "track-condition", "report",
                                                   "official-going"]):
            text = el.get_text(strip=True)
            if text and 2 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": "turftrax",
                    "type": "going_report",
                    "contenu": text[:500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse going description
                going_match = re.search(
                    r'(firm|good to firm|good|good to soft|soft|heavy|'
                    r'yielding|yielding to soft|standard|standard to slow|'
                    r'slow|fast)',
                    text, re.I
                )
                if going_match:
                    record["going"] = going_match.group(1).strip()
                # Extract venue name
                venue_match = re.search(
                    r'(Ascot|Cheltenham|Epsom|Goodwood|Newmarket|Sandown|'
                    r'York|Doncaster|Haydock|Kempton|Lingfield|Newbury|'
                    r'Chester|Aintree|Wetherby|Catterick|Musselburgh|'
                    r'Hamilton|Perth|Ayr|Carlisle|Newcastle|Wolverhampton|'
                    r'Nottingham|Leicester|Windsor|Brighton|Bath|Salisbury|'
                    r'Ffos Las|Chepstow|Pontefract|Thirsk|Ripon|Redcar|'
                    r'Beverley|Warwick|Fontwell|Plumpton|Wincanton|Exeter|'
                    r'Taunton|Uttoxeter|Sedgefield|Ludlow|Fakenham|'
                    r'Market Rasen|Hexham|Bangor|Southwell|Towcester|'
                    r'Stratford|Worcester|Huntingdon|Hereford)',
                    text, re.I
                )
                if venue_match:
                    record["venue"] = venue_match.group(1).strip()
                records.append(record)
    return records


def extract_moisture_data(soup, date_str):
    """Extract moisture levels and rail positions."""
    records = []
    for el in soup.find_all(["div", "span", "p", "td"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["moisture", "penetrometer",
                                                   "rail", "stalls",
                                                   "watering", "rainfall",
                                                   "drainage"]):
            text = el.get_text(strip=True)
            if text and 2 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "turftrax",
                    "type": "moisture_data",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract moisture percentage
                moisture_match = re.search(r'(\d{1,3}(?:\.\d{1,2})?)\s*%', text)
                if moisture_match:
                    record["moisture_pct"] = moisture_match.group(1)
                records.append(record)
    return records


def extract_venue_links(soup):
    """Extract links to venue-specific going reports."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(going-reports?|goingstick|venue|course|track)/', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_going_table(soup, date_str, page_url=""):
    """Extract going data from tables (course-by-course readings)."""
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "date": date_str,
                "source": "turftrax",
                "type": "going_table_row",
                "url": page_url,
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
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_going_reports_page(page, date_str):
    """Scrape the daily going reports page."""
    cache_file = os.path.join(CACHE_DIR, f"going_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/going-reports/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"going_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "turftrax", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "turftrax", date_str=date_str))
    records.extend(extract_goingstick_readings(soup, date_str))
    records.extend(extract_going_reports(soup, date_str))
    records.extend(extract_moisture_data(soup, date_str))
    records.extend(extract_going_table(soup, date_str, page_url=url))

    venue_links = extract_venue_links(soup)

    result = {"records": records, "venue_links": venue_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_venue_going(page, venue_url, date_str):
    """Scrape venue-specific going report page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', venue_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"venue_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, venue_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Venue name
    venue_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            venue_name = text
            break

    records.extend(extract_embedded_json_data(soup, "turftrax", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "turftrax", date_str=date_str))
    records.extend(extract_goingstick_readings(soup, date_str))
    records.extend(extract_going_reports(soup, date_str))
    records.extend(extract_moisture_data(soup, date_str))
    records.extend(extract_going_table(soup, date_str, page_url=venue_url))

    # Add venue context to records
    for rec in records:
        if venue_name and "venue" not in rec:
            rec["venue"] = venue_name

    # History / trend sections
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["history", "trend", "archive",
                                                   "previous", "timeline"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "turftrax",
                    "type": "going_history",
                    "venue": venue_name,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": venue_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_goingstick_page(page, date_str):
    """Scrape GoingStick real-time readings page."""
    cache_file = os.path.join(CACHE_DIR, f"goingstick_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/goingstick/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "turftrax", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "turftrax", date_str=date_str))
    records.extend(extract_goingstick_readings(soup, date_str))
    records.extend(extract_going_reports(soup, date_str))
    records.extend(extract_going_table(soup, date_str, page_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 145 — TurfTrax Going Scraper (GoingStick readings, going data)"
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
    log.info("SCRIPT 145 — TurfTrax Going Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "turftrax_going_data.jsonl")

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

            # Scrape going reports page
            result = scrape_going_reports_page(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual venue pages
                for venue_url in result.get("venue_links", [])[:20]:
                    detail = scrape_venue_going(page, venue_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Also scrape GoingStick page
            gs_data = scrape_goingstick_page(page, date_str)
            if gs_data:
                for rec in gs_data:
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
