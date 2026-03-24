#!/usr/bin/env python3
"""
Script 137 — The Jockey Club Database Scraper (Playwright)
Source : thejockeyclub.com
Collecte : official US racing statistics, stakes results, records, hall of fame
URL patterns :
  /racing/statistics/             -> racing statistics overview
  /racing/thoroughbred/stakes/    -> stakes race results
  /racing/records/                -> records and milestones
  /racing/hall-of-fame/           -> hall of fame jockeys/trainers
  /racing/statistics/leading/     -> leading jockey/trainer stats
CRITIQUE pour : Jockey/Trainer Stats, US Stakes Results (TODO 7L)

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
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "137_jockey_club_db"
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

log = setup_logging("137_jockey_club_db")

BASE_URL = "https://www.thejockeyclub.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Sections to scrape (path, type label)
SECTION_PATHS = [
    ("/racing/statistics", "statistics"),
    ("/racing/thoroughbred/stakes", "stakes_results"),
    ("/racing/records", "records"),
    ("/racing/hall-of-fame", "hall_of_fame"),
    ("/racing/statistics/leading-jockeys", "leading_jockeys"),
    ("/racing/statistics/leading-trainers", "leading_trainers"),
    ("/racing/statistics/leading-owners", "leading_owners"),
    ("/racing/statistics/leading-breeders", "leading_breeders"),
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

def extract_statistics_tables(soup, section_type):
    """Extract tabular statistics data from any page."""
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
                "source": "jockey_club",
                "type": section_type,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extract data-attributes from row
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)
    return records


def extract_stakes_results(soup):
    """Extract stakes race results (name, date, winner, purse, etc.)."""
    records = []
    for div in soup.find_all(["div", "article", "section", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["stake", "race", "result",
                                                   "event", "card", "entry"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "source": "jockey_club",
                    "type": "stakes_result",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract race name
                title = div.find(["h2", "h3", "h4", "strong", "a"])
                if title:
                    record["race_name"] = title.get_text(strip=True)
                # Try to extract purse
                purse_match = re.search(r'\$[\d,]+', text)
                if purse_match:
                    record["purse"] = purse_match.group(0)
                # Try to extract date
                date_match = re.search(
                    r'(\w+\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})',
                    text
                )
                if date_match:
                    record["race_date"] = date_match.group(1)
                records.append(record)
    return records


def extract_leader_stats(soup, leader_type):
    """Extract leading jockey/trainer/owner/breeder statistics."""
    records = []
    for el in soup.find_all(["div", "section", "article", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["leader", "ranking", "stat",
                                                   "jockey", "trainer", "owner",
                                                   "breeder", "top", "standing"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "source": "jockey_club",
                    "type": leader_type,
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract name
                name_el = el.find(["h3", "h4", "strong", "a", "span"])
                if name_el:
                    record["name"] = name_el.get_text(strip=True)
                # Try to extract wins
                wins_match = re.search(r'(\d+)\s*(?:wins?|victories?|W)', text, re.I)
                if wins_match:
                    record["wins"] = wins_match.group(1)
                # Try to extract earnings
                earn_match = re.search(r'\$[\d,]+', text)
                if earn_match:
                    record["earnings"] = earn_match.group(0)
                records.append(record)
    return records


def extract_records_data(soup):
    """Extract racing records and milestones."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["record", "milestone", "history",
                                                   "achievement", "fact",
                                                   "fastest", "most"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                records.append({
                    "source": "jockey_club",
                    "type": "record",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_hall_of_fame(soup):
    """Extract hall of fame entries."""
    records = []
    for el in soup.find_all(["div", "article", "section", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["hall", "fame", "inductee",
                                                   "honoree", "legend",
                                                   "profile", "bio"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "source": "jockey_club",
                    "type": "hall_of_fame",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                name_el = el.find(["h2", "h3", "h4", "strong", "a"])
                if name_el:
                    record["name"] = name_el.get_text(strip=True)
                records.append(record)
    return records


def extract_subpage_links(soup, section_path):
    """Extract links to sub-pages within a section."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if section_path in href or re.search(r'/(statistics|stakes|records|hall)', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            if full_url.startswith(BASE_URL):
                links.add(full_url)
    return sorted(links)


def extract_embedded_json_data(soup, section_type):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "source": "jockey_club",
                    "type": f"{section_type}_json",
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    for script in soup.find_all("script", {"id": "__NEXT_DATA__"}):
        try:
            data = json.loads(script.string or "")
            page_props = data.get("props", {}).get("pageProps", {})
            if page_props:
                records.append({
                    "source": "jockey_club",
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_data_attributes(soup, section_type):
    """Extract data-* attributes related to racing stats."""
    records = []
    keywords = ["horse", "jockey", "trainer", "owner", "breeder", "race",
                "result", "wins", "earnings", "rank", "stat"]
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in keywords)
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": "jockey_club",
                "type": f"{section_type}_attrs",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_section(page, section_path, section_type):
    """Scrape a given section of The Jockey Club website."""
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
    records.extend(extract_embedded_json_data(soup, section_type))
    records.extend(extract_data_attributes(soup, section_type))
    records.extend(extract_statistics_tables(soup, section_type))

    # Section-specific extractors
    if "stakes" in section_type:
        records.extend(extract_stakes_results(soup))
    elif "leading" in section_type or "jockey" in section_type or "trainer" in section_type:
        records.extend(extract_leader_stats(soup, section_type))
    elif "records" in section_type:
        records.extend(extract_records_data(soup))
    elif "hall" in section_type:
        records.extend(extract_hall_of_fame(soup))

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

    records.extend(extract_embedded_json_data(soup, section_type))
    records.extend(extract_data_attributes(soup, section_type))
    records.extend(extract_statistics_tables(soup, section_type))

    if "stakes" in section_type:
        records.extend(extract_stakes_results(soup))
    elif "leading" in section_type:
        records.extend(extract_leader_stats(soup, section_type))
    elif "records" in section_type:
        records.extend(extract_records_data(soup))
    elif "hall" in section_type:
        records.extend(extract_hall_of_fame(soup))

    # Tag all records with page title and URL
    for rec in records:
        rec["page_title"] = page_title
        rec["url"] = sub_url

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Year-based scraping for stakes results
# ------------------------------------------------------------------

def scrape_stakes_by_year(page, year):
    """Scrape stakes results for a specific year."""
    cache_file = os.path.join(CACHE_DIR, f"stakes_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/thoroughbred/stakes/{year}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "stakes_year"))
    records.extend(extract_statistics_tables(soup, "stakes_year"))
    records.extend(extract_stakes_results(soup))

    for rec in records:
        rec["year"] = year

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 137 — The Jockey Club Scraper (US racing stats, stakes results)"
    )
    parser.add_argument("--start-year", type=int, default=2020,
                        help="Start year for historical stakes scraping")
    parser.add_argument("--end-year", type=int, default=None,
                        help="End year (default=current year)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-subpages", type=int, default=20,
                        help="Max sub-pages per section (0=unlimited)")
    args = parser.parse_args()

    end_year = args.end_year or datetime.now().year

    log.info("=" * 60)
    log.info("SCRIPT 137 — The Jockey Club Database Scraper (Playwright)")
    log.info("  Sections: %d", len(SECTION_PATHS))
    log.info("  Stakes years: %d -> %d", args.start_year, end_year)
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    completed_sections = set(checkpoint.get("completed_sections", []))
    last_year = checkpoint.get("last_year", args.start_year - 1)

    output_file = os.path.join(OUTPUT_DIR, "jockey_club_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US)")

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
                "last_year": last_year,
                "total_records": total_records,
            })
            smart_pause(2.0, 1.0)

        # Phase 2: Scrape stakes results by year
        for year in range(args.start_year, end_year + 1):
            if args.resume and year <= last_year:
                log.info("  Skipping stakes year %d (already done)", year)
                continue

            log.info("  Scraping stakes year: %d", year)
            stakes = scrape_stakes_by_year(page, year)
            if stakes:
                for rec in stakes:
                    append_jsonl(output_file, rec)
                    total_records += 1

            last_year = year
            save_checkpoint(CHECKPOINT_FILE, {
                "completed_sections": list(completed_sections),
                "last_year": last_year,
                "total_records": total_records,
            })
            smart_pause(2.0, 1.0)

        save_checkpoint(CHECKPOINT_FILE, {
            "completed_sections": list(completed_sections),
            "last_year": end_year,
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: %d sections + years, %d records -> %s",
                 len(SECTION_PATHS), total_records, output_file)
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
