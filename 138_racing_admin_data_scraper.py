#!/usr/bin/env python3
"""
Script 138 — Racing Administration / BHA Data Scraper (Playwright)
Source : britishhorseracing.com (BHA — British Horseracing Authority)
Collecte : fixture lists, suspensions, disciplinary inquiries, going reports,
           race planning, regulatory data
URL patterns :
  /regulation/                      -> regulation overview
  /regulation/disciplinary/         -> disciplinary results, suspensions
  /regulation/going-reports/        -> going reports by course
  /racing/fixtures/                 -> fixture lists
  /racing/racecourses/              -> racecourse directory
  /regulation/whip-use/             -> whip use data
  /regulation/equine-welfare/       -> equine welfare reports
CRITIQUE pour : BHA Regulatory Data, Fixture Lists, Going Reports (TODO 7I/7M)

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

SCRIPT_NAME = "138_racing_admin_bha"
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

log = setup_logging("138_racing_admin_bha")

BASE_URL = "https://www.britishhorseracing.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Sections to scrape (path, type label)
SECTION_PATHS = [
    ("/regulation/disciplinary", "disciplinary"),
    ("/regulation/going-reports", "going_reports"),
    ("/racing/fixtures", "fixtures"),
    ("/racing/racecourses", "racecourses"),
    ("/regulation", "regulation"),
    ("/regulation/whip-use", "whip_use"),
    ("/regulation/equine-welfare", "equine_welfare"),
]


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

def extract_tables(soup, section_type):
    """Extract tabular data from any page."""
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
                "source": "bha",
                "type": section_type,
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


def extract_disciplinary(soup):
    """Extract disciplinary results and suspensions."""
    records = []
    for el in soup.find_all(["div", "article", "section", "li", "tr"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["disciplinary", "suspension",
                                                   "inquiry", "enquiry", "penalty",
                                                   "ban", "fine", "hearing",
                                                   "result", "case"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "source": "bha",
                    "type": "disciplinary",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract person name
                name_el = el.find(["h3", "h4", "strong", "a", "span"])
                if name_el:
                    record["person"] = name_el.get_text(strip=True)
                # Try to extract date
                date_match = re.search(
                    r'(\d{1,2}\s+\w+\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})',
                    text
                )
                if date_match:
                    record["decision_date"] = date_match.group(1)
                # Try to extract penalty type
                penalty_match = re.search(
                    r'(suspended?|ban(?:ned)?|fine[d]?|caution(?:ed)?|'
                    r'warn(?:ed|ing)?|disqualif)',
                    text, re.I
                )
                if penalty_match:
                    record["penalty_type"] = penalty_match.group(1).lower()
                # Try to extract suspension days
                days_match = re.search(r'(\d+)\s*(?:day|jour)', text, re.I)
                if days_match:
                    record["suspension_days"] = days_match.group(1)
                records.append(record)
    return records


def extract_going_reports(soup):
    """Extract going reports (ground condition data by course)."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p", "li", "tr"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["going", "ground", "report",
                                                   "course", "condition",
                                                   "terrain", "surface",
                                                   "stick", "penetrometer"]):
            if text and 5 < len(text) < 2000:
                record = {
                    "source": "bha",
                    "type": "going_report",
                    "contenu": text[:1500],
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
                # Extract course name
                course_el = el.find(["h3", "h4", "strong", "a"])
                if course_el:
                    record["course"] = course_el.get_text(strip=True)
                # Extract date
                date_match = re.search(
                    r'(\d{1,2}\s+\w+\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})',
                    text
                )
                if date_match:
                    record["report_date"] = date_match.group(1)
                # Extract GoingStick / penetrometer reading
                stick_match = re.search(r'(?:going\s*stick|penetrometer)\s*:?\s*(\d+\.?\d*)',
                                        text, re.I)
                if stick_match:
                    record["going_stick"] = stick_match.group(1)
                records.append(record)
    return records


def extract_fixtures(soup):
    """Extract fixture list data (race meetings scheduled)."""
    records = []
    for el in soup.find_all(["div", "article", "section", "li", "tr"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["fixture", "meeting", "event",
                                                   "card", "schedule", "race-day"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "source": "bha",
                    "type": "fixture",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract venue
                venue_el = el.find(["h3", "h4", "strong", "a"])
                if venue_el:
                    record["venue"] = venue_el.get_text(strip=True)
                # Extract date
                date_match = re.search(
                    r'(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})',
                    text
                )
                if date_match:
                    record["fixture_date"] = date_match.group(1)
                # Extract race type (flat / national hunt)
                type_match = re.search(r'(flat|national hunt|nh|jumps?|all-weather|aw)',
                                       text, re.I)
                if type_match:
                    record["race_type"] = type_match.group(1)
                records.append(record)
    return records


def extract_racecourses(soup):
    """Extract racecourse directory information."""
    records = []
    for el in soup.find_all(["div", "article", "section", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["racecourse", "venue", "track",
                                                   "course", "location"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "source": "bha",
                    "type": "racecourse",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                name_el = el.find(["h3", "h4", "strong", "a"])
                if name_el:
                    record["course_name"] = name_el.get_text(strip=True)
                # Extract surface type
                surface_match = re.search(r'(turf|all-weather|aw|polytrack|tapeta|fibresand)',
                                          text, re.I)
                if surface_match:
                    record["surface"] = surface_match.group(1)
                # Extract direction
                dir_match = re.search(r'(left-handed|right-handed|left|right)',
                                      text, re.I)
                if dir_match:
                    record["direction"] = dir_match.group(1)
                records.append(record)
    return records


def extract_subpage_links(soup, section_path):
    """Extract links to sub-pages within a section."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if section_path in href or re.search(
                r'/(regulation|disciplinary|going|fixture|racecourse|welfare|whip)/',
                href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            if full_url.startswith(BASE_URL):
                links.add(full_url)
    return sorted(links)


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_section(page, section_path, section_type):
    """Scrape a given section of the BHA website."""
    safe_name = section_path.strip("/").replace("/", "_")
    cache_file = os.path.join(CACHE_DIR, f"{safe_name}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}{section_path}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"{safe_name}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Common extractors
    records.extend(extract_embedded_json_data(soup, "bha", date_str=section_type))
    records.extend(extract_scraper_data_attributes(soup, "bha", date_str=section_type))
    records.extend(extract_tables(soup, section_type))

    # Section-specific extractors
    if "disciplinary" in section_type:
        records.extend(extract_disciplinary(soup))
    elif "going" in section_type:
        records.extend(extract_going_reports(soup))
    elif "fixture" in section_type:
        records.extend(extract_fixtures(soup))
    elif "racecourse" in section_type:
        records.extend(extract_racecourses(soup))

    # Get sub-page links
    subpage_links = extract_subpage_links(soup, section_path)

    result = {"records": records, "subpage_links": subpage_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_subpage(page, sub_url, section_type):
    """Scrape a sub-page within a section for additional data."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', sub_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"sub_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, sub_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Page title
    page_title = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            page_title = text
            break

    records.extend(extract_embedded_json_data(soup, "bha", date_str=section_type))
    records.extend(extract_scraper_data_attributes(soup, "bha", date_str=section_type))
    records.extend(extract_tables(soup, section_type))

    if "disciplinary" in section_type:
        records.extend(extract_disciplinary(soup))
    elif "going" in section_type:
        records.extend(extract_going_reports(soup))
    elif "fixture" in section_type:
        records.extend(extract_fixtures(soup))
    elif "racecourse" in section_type:
        records.extend(extract_racecourses(soup))

    # Tag all records with page title and URL
    for rec in records:
        rec["page_title"] = page_title
        rec["url"] = sub_url

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Date-based scraping for going reports and fixtures
# ------------------------------------------------------------------

def scrape_going_reports_by_date(page, date_str):
    """Scrape going reports for a specific date."""
    cache_file = os.path.join(CACHE_DIR, f"going_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/regulation/going-reports/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "bha", date_str="going_report"))
    records.extend(extract_tables(soup, "going_report"))
    records.extend(extract_going_reports(soup))

    for rec in records:
        rec["date"] = date_str

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_fixtures_by_date(page, date_str):
    """Scrape fixtures for a specific date."""
    cache_file = os.path.join(CACHE_DIR, f"fixtures_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/fixtures/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "bha", date_str="fixture"))
    records.extend(extract_tables(soup, "fixture"))
    records.extend(extract_fixtures(soup))

    for rec in records:
        rec["date"] = date_str

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 138 — BHA Racing Admin Data Scraper (fixtures, suspensions, going)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Start date (YYYY-MM-DD) for going/fixtures")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=yesterday")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--max-subpages", type=int, default=20,
                        help="Max sub-pages per section (0=unlimited)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
                else datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 138 — BHA Racing Admin Data Scraper (Playwright)")
    log.info("  Sections: %d", len(SECTION_PATHS))
    log.info("  Date range: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    completed_sections = set(checkpoint.get("completed_sections", []))
    last_date = checkpoint.get("last_date")

    output_file = os.path.join(OUTPUT_DIR, "bha_racing_admin_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-GB", timezone="Europe/London"
        )
        log.info("Browser launched (headless Chromium, locale=en-GB)")

        first_nav = True
        total_records = 0

        # Phase 1: Scrape all section pages
        for section_path, section_type in SECTION_PATHS:
            section_key = f"{section_path}_{section_type}"
            if args.resume and section_key in completed_sections:
                log.info("  Skipping (already done): %s", section_path)
                continue

            log.info("  Scraping section: %s (%s)", section_path, section_type)
            result = scrape_section(page, section_path, section_type)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape sub-pages
                subpage_links = result.get("subpage_links", [])
                max_sub = args.max_subpages if args.max_subpages else len(subpage_links)
                for sub_url in subpage_links[:max_sub]:
                    if sub_url == f"{BASE_URL}{section_path}":
                        continue
                    sub_records = scrape_subpage(page, sub_url, section_type)
                    if sub_records:
                        records.extend(sub_records)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            completed_sections.add(section_key)
            save_checkpoint(CHECKPOINT_FILE, {
                "completed_sections": list(completed_sections),
                "last_date": last_date,
                "total_records": total_records,
            })
            smart_pause(2.0, 1.0)

        # Phase 2: Scrape going reports and fixtures by date
        if args.resume and last_date:
            resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            current = resume_date
            log.info("  Resuming date scraping from: %s", current.date())
        else:
            current = start_date

        day_count = 0
        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")

            # Going reports for this date
            going_data = scrape_going_reports_by_date(page, date_str)
            if going_data:
                for rec in going_data:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(1.0, 0.5)

            # Fixtures for this date
            fixture_data = scrape_fixtures_by_date(page, date_str)
            if fixture_data:
                for rec in fixture_data:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 10 == 0:
                log.info("  %s | days=%d records=%d", date_str, day_count, total_records)
                save_checkpoint(CHECKPOINT_FILE, {
                    "completed_sections": list(completed_sections),
                    "last_date": date_str,
                    "total_records": total_records,
                })

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {
            "completed_sections": list(completed_sections),
            "last_date": end_date.strftime("%Y-%m-%d"),
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: %d sections + %d days, %d records -> %s",
                 len(SECTION_PATHS), day_count, total_records, output_file)
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
