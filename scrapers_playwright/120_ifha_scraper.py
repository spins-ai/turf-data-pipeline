#!/usr/bin/env python3
"""
Script 120 (Playwright) — IFHA International Federation Scraper
Source : ifhaonline.org
Collecte : World thoroughbred rankings, international grading/group stakes,
           racing authority data, cross-country race validation
URL patterns :
  /page/Rankings/{year}        -> world rankings by year
  /page/GradedStakes/{year}    -> international graded stakes catalogue
  /page/IRR/{year}             -> international race results
  /page/Members                -> member authorities directory
  /resources/                  -> publications, rules, standards
CRITIQUE pour : World Rankings, International Grading, Cross-Country Validation,
                Graded Stakes Data, Authority Directory

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

# Resolve paths for imports (scrapers_playwright/ is one level down)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from utils.playwright import launch_browser, accept_cookies
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

SCRIPT_NAME = "120_ifha"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

log = setup_logging("120_ifha")

BASE_URL = "https://www.ifhaonline.org"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# IFHA ranking categories
RANKING_CATEGORIES = [
    "flat", "turf", "dirt", "sprint", "mile",
    "intermediate", "long", "extended",
    "steeplechase", "hurdle", "jumps",
]

# Grade/Group levels
GRADE_LEVELS = ["G1", "G2", "G3", "Gr.1", "Gr.2", "Gr.3",
                "Group 1", "Group 2", "Group 3",
                "Grade 1", "Grade 2", "Grade 3",
                "Listed", "LR"]


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
# Parsing helpers
# ------------------------------------------------------------------

def parse_rating(text):
    """Extract a numeric rating from text like '126' or '126p'."""
    m = re.match(r"(\d+)\s*[a-zA-Z]?$", text.strip())
    return int(m.group(1)) if m else None


def parse_grade(text):
    """Extract grade/group level from text."""
    text_u = text.upper().strip()
    for gl in GRADE_LEVELS:
        if gl.upper() in text_u:
            return gl
    return None


# ------------------------------------------------------------------
# Extraction helpers
# ------------------------------------------------------------------

def extract_ranking_links(soup):
    """Extract links to ranking detail pages."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(ranking|rank|horse|detail)', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_rankings_table(soup, date_str, category=""):
    """Extract world ranking data from tables."""
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
                "source": "ifha",
                "type": "world_ranking",
                "scraped_at": datetime.now().isoformat(),
            }
            if category:
                record["category"] = category

            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
                # Try to extract rating value
                rating = parse_rating(cell)
                if rating is not None and rating > 50 and "rating" not in record:
                    record["rating"] = rating

            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)
    return records


def extract_graded_stakes(soup, date_str):
    """Extract graded/group stakes data from tables and listings."""
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
                "source": "ifha",
                "type": "graded_stake",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
                grade = parse_grade(cell)
                if grade and "grade" not in record:
                    record["grade"] = grade

            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)

    # Also extract card/list elements
    for el in soup.find_all(["div", "article", "li", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["race", "stake", "event",
                                                   "result", "listing",
                                                   "group", "grade"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "ifha",
                    "type": "graded_stake",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                grade = parse_grade(text)
                if grade:
                    record["grade"] = grade
                # Distance
                dist_m = re.search(r'(\d[\d,]*)\s*m\b', text, re.I)
                if dist_m:
                    record["distance_m"] = dist_m.group(1).replace(",", "")
                # Prize money
                prize_m = re.search(
                    r'(?:USD|EUR|GBP|\$|\u00a3|\u20ac)\s*([\d,]+)', text
                )
                if prize_m:
                    record["prize_money"] = prize_m.group(1).replace(",", "")
                records.append(record)
    return records


def extract_race_results(soup, date_str):
    """Extract international race result data."""
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
                "source": "ifha",
                "type": "intl_race_result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
                grade = parse_grade(cell)
                if grade and "grade" not in record:
                    record["grade"] = grade

            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)

    # Result blocks
    for el in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "race", "winner",
                                                   "finish", "placed"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "ifha",
                    "type": "intl_race_result",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                grade = parse_grade(text)
                if grade:
                    record["grade"] = grade
                records.append(record)
    return records


def extract_member_authorities(soup, date_str):
    """Extract IFHA member authority directory data."""
    records = []
    for el in soup.find_all(["div", "li", "article", "section", "tr"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["member", "authority", "country",
                                                   "organisation", "directory",
                                                   "listing", "entry"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": "ifha",
                    "type": "member_authority",
                    "contenu": text[:800],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_embedded_json_data(soup, date_str):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "date": date_str,
                    "source": "ifha",
                    "type": "embedded_json",
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
                    "date": date_str,
                    "source": "ifha",
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_data_attributes(soup, date_str):
    """Extract data-* attributes related to rankings/racing."""
    records = []
    keywords = ["horse", "rank", "rating", "country", "trainer",
                "jockey", "category", "race", "grade", "stake"]
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in keywords)
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "ifha",
                "type": "data_attrs",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_world_rankings(page, year, date_str):
    """Scrape IFHA world thoroughbred rankings for a given year."""
    cache_file = os.path.join(CACHE_DIR, f"rankings_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/page/Rankings/{year}",
        f"{BASE_URL}/rankings?year={year}",
        f"{BASE_URL}/page/Rankings",
    ]

    html = None
    for url in urls_to_try:
        html = navigate_with_retry(page, url)
        if html:
            break
        smart_pause(3.0, 1.5)

    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"rankings_{year}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_rankings_table(soup, date_str))
    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))

    # Tag all with year
    for rec in records:
        rec["ranking_year"] = year

    # Try category-specific pages
    for category in RANKING_CATEGORIES:
        cat_url = f"{BASE_URL}/page/Rankings/{year}/{category}"
        cat_html = navigate_with_retry(page, cat_url, retries=1)
        if cat_html:
            cat_soup = BeautifulSoup(cat_html, "html.parser")
            cat_records = extract_rankings_table(cat_soup, date_str, category=category)
            for rec in cat_records:
                rec["ranking_year"] = year
            if cat_records:
                records.extend(cat_records)
                log.info("  Rankings %d/%s: %d records", year, category, len(cat_records))
        smart_pause(4.0, 2.0)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_graded_stakes_page(page, year, date_str):
    """Scrape IFHA international graded/group stakes catalogue."""
    cache_file = os.path.join(CACHE_DIR, f"graded_stakes_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/page/GradedStakes/{year}",
        f"{BASE_URL}/resources/international-cataloguing-standards",
        f"{BASE_URL}/page/Races",
    ]

    html = None
    for url in urls_to_try:
        html = navigate_with_retry(page, url)
        if html:
            break
        smart_pause(3.0, 1.5)

    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_graded_stakes(soup, date_str))
    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))

    for rec in records:
        rec["year"] = year

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_international_results(page, year, date_str):
    """Scrape IFHA international race results."""
    cache_file = os.path.join(CACHE_DIR, f"results_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/page/IRR/{year}",
        f"{BASE_URL}/page/Results/{year}",
        f"{BASE_URL}/page/IRR",
    ]

    html = None
    for url in urls_to_try:
        html = navigate_with_retry(page, url)
        if html:
            break
        smart_pause(3.0, 1.5)

    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_race_results(soup, date_str))
    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))

    for rec in records:
        rec["year"] = year

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_member_directory(page, date_str):
    """Scrape IFHA member authorities directory."""
    cache_file = os.path.join(CACHE_DIR, f"members_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/page/Members"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_member_authorities(soup, date_str))
    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 120 (Playwright) — IFHA International Federation Scraper "
                    "(world rankings, international grading)"
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

    start_year = start_date.year
    end_year = end_date.year

    log.info("=" * 60)
    log.info("SCRIPT 120 (Playwright) — IFHA International Federation Scraper")
    log.info("  Period: %s -> %s (years %d-%d)",
             start_date.date(), end_date.date(), start_year, end_year)
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_year = checkpoint.get("last_year")
    if args.resume and last_year:
        start_year = last_year + 1
        log.info("  Resuming from checkpoint year: %d", start_year)

    output_file = os.path.join(OUTPUT_DIR, "ifha_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US)")

        first_nav = True
        total_records = 0

        for year in range(start_year, end_year + 1):
            date_str = f"{year}-01-01"
            log.info("--- Year %d ---", year)

            # World rankings
            rankings = scrape_world_rankings(page, year, date_str)
            if first_nav and rankings is not None:
                accept_cookies(page)
                first_nav = False
            if rankings:
                for rec in rankings:
                    append_jsonl(output_file, rec)
                    total_records += 1
            smart_pause(5.0, 2.5)

            # Graded stakes
            stakes = scrape_graded_stakes_page(page, year, date_str)
            if stakes:
                for rec in stakes:
                    append_jsonl(output_file, rec)
                    total_records += 1
            smart_pause(5.0, 2.5)

            # International results
            results = scrape_international_results(page, year, date_str)
            if results:
                for rec in results:
                    append_jsonl(output_file, rec)
                    total_records += 1
            smart_pause(5.0, 2.5)

            # Member directory (once per run, not per year)
            if year == start_year:
                members = scrape_member_directory(page, date_str)
                if members:
                    for rec in members:
                        append_jsonl(output_file, rec)
                        total_records += 1
                smart_pause(3.0, 1.5)

            save_checkpoint(CHECKPOINT_FILE, {
                "last_year": year,
                "total_records": total_records,
            })
            log.info("  Year %d done: %d cumulative records", year, total_records)

        save_checkpoint(CHECKPOINT_FILE, {
            "last_year": end_year,
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: years %d-%d, %d records -> %s",
                 start_year, end_year, total_records, output_file)
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
