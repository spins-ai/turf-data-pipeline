#!/usr/bin/env python3
"""
Script 119 — Racing Administrative Bodies Scraper (Playwright)
Source : britishhorseracing.com (BHA UK), ifhaonline.org (IFHA international)
Collecte : Regulatory data, licensing info, race conditions, rule updates,
           fixture lists, administrative announcements
URL patterns :
  britishhorseracing.com/regulation/    -> regulatory info
  britishhorseracing.com/racing/        -> fixture lists, race programming
  ifhaonline.org/page/RulesOfRacing     -> international rules
  ifhaonline.org/page/Members           -> member authorities directory
CRITIQUE pour : Regulatory Compliance, Race Conditions, Fixture Programming,
                Licensing Data, Administrative Updates

NOTE: This is a placeholder scraper. The target sites may require
      specific access or authentication. Adjust URLs as needed.

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
from utils.html_parsing import extract_embedded_json_data, extract_scraper_data_attributes

SCRIPT_NAME = "119_racing_admin"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("119_racing_admin")

# Administrative bodies to scrape
ADMIN_SOURCES = {
    "bha": {
        "base_url": "https://www.britishhorseracing.com",
        "label": "British Horseracing Authority (BHA)",
        "pages": [
            "/regulation/",
            "/racing/fixtures/",
            "/racing/programme/",
            "/regulation/licensing/",
            "/about/",
        ],
    },
    "ifha": {
        "base_url": "https://www.ifhaonline.org",
        "label": "International Federation of Horseracing Authorities (IFHA)",
        "pages": [
            "/page/RulesOfRacing",
            "/page/Members",
            "/page/About",
            "/resources/",
        ],
    },
}

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

def extract_internal_links(soup, base_url):
    """Extract internal links for further crawling."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(regulation|racing|fixture|licence|rule|member|resource)', href, re.I):
            full_url = href if href.startswith("http") else f"{base_url}{href}"
            links.add(full_url)
    return sorted(links)


def extract_regulatory_data(soup, date_str, source_key):
    """Extract regulatory and administrative content blocks."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["regulation", "rule", "policy",
                                                   "licence", "licensing",
                                                   "compliance", "notice",
                                                   "announcement", "update",
                                                   "news", "content"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": source_key,
                    "type": "regulatory_content",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_fixture_data(soup, date_str, source_key):
    """Extract fixture list and race programming data."""
    records = []
    for el in soup.find_all(["div", "section", "table", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["fixture", "programme", "schedule",
                                                   "calendar", "meeting",
                                                   "event", "raceday"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": source_key,
                    "type": "fixture_data",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to parse venue name
                venue_el = el.find(["h2", "h3", "h4", "strong", "a"])
                if venue_el:
                    record["venue"] = venue_el.get_text(strip=True)
                # Try to parse date
                date_match = re.search(
                    r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*'
                    r'\s+(\d{4})',
                    text, re.I
                )
                if date_match:
                    record["fixture_date"] = date_match.group(0)
                records.append(record)
    return records


def extract_member_directory(soup, date_str, source_key):
    """Extract member authority directory data."""
    records = []
    for el in soup.find_all(["div", "li", "article", "section", "tr"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["member", "authority", "country",
                                                   "organisation", "directory",
                                                   "listing", "entry"]):
            if text and 3 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": source_key,
                    "type": "member_authority",
                    "contenu": text[:800],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_tables(soup, date_str, source_key):
    """Extract table data from admin pages."""
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
                "source": source_key,
                "type": "admin_table",
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

def scrape_admin_page(page, url, date_str, source_key):
    """Scrape a single administrative page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"{source_key}_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"{source_key}_{url_hash}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Page title
    page_title = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            page_title = text
            break

    # Extract all data types
    records.extend(extract_embedded_json_data(soup, source_key, date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, source_key, date_str=date_str))
    records.extend(extract_regulatory_data(soup, date_str, source_key))
    records.extend(extract_fixture_data(soup, date_str, source_key))
    records.extend(extract_member_directory(soup, date_str, source_key))
    records.extend(extract_tables(soup, date_str, source_key))

    # Tag all records with page info
    for rec in records:
        rec["page_title"] = page_title
        rec["url"] = url

    # Internal links for further crawling
    base_url = ADMIN_SOURCES.get(source_key, {}).get("base_url", "")
    internal_links = extract_internal_links(soup, base_url)

    result = {"records": records, "internal_links": internal_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_admin_source(page, source_key, source_config, date_str, output_file):
    """Scrape all pages for a given administrative source."""
    base_url = source_config["base_url"]
    label = source_config["label"]
    pages = source_config["pages"]

    log.info("  Scraping: %s (%s)", label, base_url)
    total = 0
    all_internal_links = set()

    for page_path in pages:
        url = f"{base_url}{page_path}"
        result = scrape_admin_page(page, url, date_str, source_key)

        if result:
            records = result.get("records", [])
            for rec in records:
                append_jsonl(output_file, rec)
                total += 1

            all_internal_links.update(result.get("internal_links", []))

        smart_pause(2.0, 1.0)

    # Follow a limited number of discovered internal links
    visited = {f"{base_url}{p}" for p in pages}
    for link in sorted(all_internal_links)[:10]:
        if link in visited:
            continue
        visited.add(link)

        result = scrape_admin_page(page, link, date_str, source_key)
        if result:
            records = result.get("records", [])
            for rec in records:
                append_jsonl(output_file, rec)
                total += 1

        smart_pause(2.0, 1.0)

    log.info("  %s: %d records", label, total)
    return total


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 119 — Racing Administrative Bodies Scraper (BHA, IFHA)"
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
    log.info("SCRIPT 119 — Racing Administrative Bodies Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("  Sources: %s", ", ".join(
        cfg["label"] for cfg in ADMIN_SOURCES.values()
    ))
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "racing_admin_data.jsonl")

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

            for source_key, source_config in ADMIN_SOURCES.items():
                count = scrape_admin_source(
                    page, source_key, source_config, date_str, output_file
                )

                if first_nav and count > 0:
                    accept_cookies(page)
                    first_nav = False

                total_records += count
                smart_pause(3.0, 1.5)

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
