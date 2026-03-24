#!/usr/bin/env python3
"""
Script 126 (Playwright) -- Racing Index scraper.
Source : racingindex.com
Collecte : UK ratings, speed figures, race analysis, performance data
CRITIQUE pour : Speed figures, race analysis, performance ratings UK

Usage:
    pip install playwright beautifulsoup4
    playwright install chromium
    python 126_racing_index_scraper.py --start 2024-01-01 --end 2026-03-24
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.playwright import launch_browser, accept_cookies
from utils.html_parsing import extract_embedded_json_data
from utils.html_parsing import extract_scraper_data_attributes

log = setup_logging("126_racing_index_scraper")

SCRIPT_NAME = "126_racing_index"
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "output", SCRIPT_NAME
)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

BASE_URL = "https://www.racingindex.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Major UK courses
UK_COURSES = [
    "ascot", "cheltenham", "aintree", "epsom", "goodwood", "newmarket",
    "york", "doncaster", "sandown", "kempton", "newbury", "haydock",
    "chester", "windsor", "lingfield", "wolverhampton", "catterick",
    "thirsk", "ripon", "nottingham", "leicester", "warwick",
    "bangor-on-dee", "market-rasen", "wincanton", "exeter", "fontwell",
    "plumpton", "sedgefield", "wetherby", "uttoxeter", "carlisle",
    "musselburgh", "ayr", "hamilton", "perth", "kelso",
]

RACE_TYPES = ["flat", "hurdle", "chase", "nhf", "bumper"]


# ------------------------------------------------------------------
# Navigation helper
# ------------------------------------------------------------------

def navigate_with_retry(page, url, retries=MAX_RETRIES):
    """Navigate to url with retry logic. Returns HTML string or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT_MS)
            if resp and resp.status >= 400:
                log.warning(
                    "  HTTP %d on %s (attempt %d/%d)",
                    resp.status, url, attempt, retries,
                )
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
            log.warning(
                "  Navigation error: %s (attempt %d/%d)",
                str(exc)[:200], attempt, retries,
            )
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


# ------------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------------

def find_course(text):
    """Match a known UK course name in text."""
    text_lower = text.lower()
    for c in UK_COURSES:
        if c.replace("-", " ") in text_lower:
            return c
    return ""


def parse_position(text):
    """Extract finishing position from text like '1st', '3rd'."""
    m = re.match(r"^(\d+)(?:st|nd|rd|th)?$", text.strip(), re.I)
    return int(m.group(1)) if m else None


def parse_time(text):
    """Extract race time in seconds from text like '1m 32.40s'."""
    m = re.search(r"(\d+)\s*m\s*(\d+(?:\.\d+)?)\s*s?", text, re.I)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    m = re.search(r"(\d+):(\d+(?:\.\d+)?)", text)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    return None


# ------------------------------------------------------------------
# Extraction: ratings and speed figures
# ------------------------------------------------------------------

def extract_ratings(soup, date_str):
    """Extract performance ratings and speed figures from page content."""
    records = []
    for el in soup.find_all(
        ["div", "span", "td", "section", "article"], class_=True
    ):
        classes = " ".join(el.get("class", []))
        if not any(
            kw in classes.lower()
            for kw in [
                "rating", "speed", "figure", "index", "score",
                "performance", "rpr", "topspeed", "ts-rating",
                "metric", "benchmark", "par",
            ]
        ):
            continue

        text = el.get_text(strip=True)
        if not text or len(text) < 1 or len(text) > 1000:
            continue

        record = {
            "date": date_str,
            "source": "racingindex",
            "type": "rating",
            "contenu": text[:500],
            "classes_css": classes,
            "scraped_at": datetime.now().isoformat(),
        }

        # Extract numeric rating
        rating_m = re.search(r"(\d{1,3}(?:\.\d{1,2})?)", text)
        if rating_m:
            record["rating_value"] = float(rating_m.group(1))

        # Extract horse name if present nearby
        parent = el.parent
        if parent:
            name_el = parent.find(["a", "strong", "h3", "h4"])
            if name_el:
                name_text = name_el.get_text(strip=True)
                if name_text and len(name_text) < 100:
                    record["horse_name"] = name_text

        records.append(record)

    return records


# ------------------------------------------------------------------
# Extraction: race analysis
# ------------------------------------------------------------------

def extract_race_analysis(soup, date_str):
    """Extract race analysis and commentary data."""
    records = []
    for el in soup.find_all(
        ["div", "section", "article", "p"], class_=True
    ):
        classes = " ".join(el.get("class", []))
        if not any(
            kw in classes.lower()
            for kw in [
                "analysis", "review", "report", "comment", "verdict",
                "insight", "assessment", "summary", "recap",
                "race-report", "performance-review",
            ]
        ):
            continue

        text = el.get_text(strip=True)
        if not text or len(text) < 30 or len(text) > 5000:
            continue

        record = {
            "date": date_str,
            "source": "racingindex",
            "type": "race_analysis",
            "contenu": text[:3000],
            "classes_css": classes,
            "scraped_at": datetime.now().isoformat(),
        }

        # Try to get associated race name
        h_el = el.find(["h2", "h3", "h4", "strong"])
        if h_el:
            record["subject"] = h_el.get_text(strip=True)

        # Course
        course = find_course(text)
        if course:
            record["course"] = course

        records.append(record)

    return records


# ------------------------------------------------------------------
# Extraction: results tables with speed data
# ------------------------------------------------------------------

def extract_results_table(soup, date_str, race_url=""):
    """Extract runner data with speed figures from result tables."""
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [
                th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                for th in rows[0].find_all(["th", "td"])
            ]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue

            record = {
                "date": date_str,
                "source": "racingindex",
                "type": "result_runner",
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Parse specific fields
                pos = parse_position(cell)
                if pos is not None and "position" not in record:
                    record["position"] = pos
                t = parse_time(cell)
                if t is not None and "time_seconds" not in record:
                    record["time_raw"] = cell
                    record["time_seconds"] = t

            # Speed figure columns
            for h_idx, h_name in enumerate(headers):
                if any(
                    kw in h_name
                    for kw in ["speed", "rating", "index", "figure", "rpr", "ts"]
                ):
                    if h_idx < len(cells):
                        try:
                            record[f"speed_{h_name}"] = float(
                                cells[h_idx].replace(",", "")
                            )
                        except ValueError:
                            record[f"speed_{h_name}_raw"] = cells[h_idx]

            # Data attributes on row
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)

    return records


# ------------------------------------------------------------------
# Extraction: embedded JSON and data-attributes
# ------------------------------------------------------------------


# ------------------------------------------------------------------
# Scrape: ratings / speed figures page
# ------------------------------------------------------------------

def scrape_ratings_page(page, date_str):
    """Scrape the Racing Index ratings page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"ratings_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/ratings/{date_str}",
        f"{BASE_URL}/ratings/?date={date_str}",
        f"{BASE_URL}/speed-figures/{date_str}",
        f"{BASE_URL}/index/{date_str}",
    ]

    html = None
    for url in urls_to_try:
        html = navigate_with_retry(page, url)
        if html:
            break
        smart_pause(2.0, 1.0)

    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"ratings_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "racingindex", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "racingindex", date_str=date_str))
    records.extend(extract_ratings(soup, date_str))
    records.extend(extract_results_table(soup, date_str))

    # Extract race links for detail pages
    race_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/(race|result|analysis|review)/", href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            race_links.add(full_url)

    result = {"records": records, "race_links": sorted(race_links)}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


# ------------------------------------------------------------------
# Scrape: results page
# ------------------------------------------------------------------

def scrape_results_day(page, date_str):
    """Scrape results page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/results/{date_str}",
        f"{BASE_URL}/results/?date={date_str}",
        f"{BASE_URL}/racing/results/{date_str}",
    ]

    html = None
    for url in urls_to_try:
        html = navigate_with_retry(page, url)
        if html:
            break
        smart_pause(2.0, 1.0)

    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "racingindex", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "racingindex", date_str=date_str))
    records.extend(extract_ratings(soup, date_str))
    records.extend(extract_results_table(soup, date_str))
    records.extend(extract_race_analysis(soup, date_str))

    # Meeting blocks
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(
            kw in classes.lower()
            for kw in ["meeting", "venue", "fixture", "card"]
        ):
            record = {
                "date": date_str,
                "source": "racingindex",
                "type": "meeting",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong", "a"])
            if title:
                record["venue"] = title.get_text(strip=True)
                course = find_course(record["venue"])
                if course:
                    record["course"] = course
            for span in div.find_all(["span", "small", "em", "p"]):
                text = span.get_text(strip=True)
                going_m = re.search(
                    r"(going|ground)\s*:?\s*(firm|good to firm|good|"
                    r"good to soft|soft|heavy|yielding|standard|slow|fast)",
                    text, re.I,
                )
                if going_m:
                    record["going"] = going_m.group(2).strip()
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Scrape: race detail / analysis page
# ------------------------------------------------------------------

def scrape_race_detail(page, race_url, date_str):
    """Scrape a single race detail/analysis page."""
    url_hash = re.sub(r"[^a-zA-Z0-9]", "_", race_url[-80:])
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

    # Conditions
    page_text = soup.get_text()
    conditions = {}

    course = find_course(page_text)
    if course:
        conditions["course"] = course

    dist_m = re.search(r"(\d+)\s*(?:f|furlongs?)\b", page_text, re.I)
    if dist_m:
        conditions["distance_furlongs"] = int(dist_m.group(1))

    going_m = re.search(
        r"going\s*:?\s*(firm|good to firm|good|good to soft|soft|heavy|"
        r"yielding|standard|slow|fast)",
        page_text, re.I,
    )
    if going_m:
        conditions["going"] = going_m.group(1).strip()

    class_m = re.search(r"class\s*(\d)", page_text, re.I)
    if class_m:
        conditions["race_class"] = int(class_m.group(1))

    for rt in RACE_TYPES:
        if rt in page_text.lower():
            conditions["race_type"] = rt
            break

    # Structured data
    records.extend(extract_embedded_json_data(soup, "racingindex", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "racingindex", date_str=date_str))
    records.extend(extract_ratings(soup, date_str))
    records.extend(extract_race_analysis(soup, date_str))

    # Results table with speed data
    for rec in extract_results_table(soup, date_str, race_url):
        rec["race_name"] = race_name
        rec["conditions"] = conditions
        records.append(rec)

    # Speed figure summary sections
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(
            kw in classes.lower()
            for kw in [
                "speed-figure", "performance-index", "par-time",
                "standard-time", "race-index", "sectional",
            ]
        ):
            text = div.get_text(strip=True)
            if text and 3 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "racingindex",
                    "type": "speed_summary",
                    "race_name": race_name,
                    "conditions": conditions,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Scrape: top-rated horses / leaderboards
# ------------------------------------------------------------------

def scrape_leaderboard(page, date_str):
    """Scrape top-rated horses and leaderboard pages."""
    cache_file = os.path.join(CACHE_DIR, f"leaderboard_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/leaderboard",
        f"{BASE_URL}/top-rated",
        f"{BASE_URL}/rankings",
        f"{BASE_URL}/best-rated",
    ]

    html = None
    for url in urls_to_try:
        html = navigate_with_retry(page, url)
        if html:
            break
        smart_pause(2.0, 1.0)

    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "racingindex", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "racingindex", date_str=date_str))
    records.extend(extract_ratings(soup, date_str))
    records.extend(extract_results_table(soup, date_str))

    # Leaderboard cards
    for el in soup.find_all(
        ["div", "li", "tr", "article"], class_=True
    ):
        classes = " ".join(el.get("class", []))
        if any(
            kw in classes.lower()
            for kw in [
                "leader", "top", "ranked", "best", "champion",
                "highest", "elite",
            ]
        ):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": "racingindex",
                    "type": "leaderboard_entry",
                    "contenu": text[:500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract rating
                rating_m = re.search(r"(\d{2,3}(?:\.\d{1,2})?)", text)
                if rating_m:
                    record["rating_value"] = float(rating_m.group(1))
                # Horse name
                name_el = el.find(["a", "strong", "h3", "h4"])
                if name_el:
                    record["horse_name"] = name_el.get_text(strip=True)
                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 126 (Playwright) -- Racing Index Scraper "
        "(UK ratings, speed figures, race analysis)"
    )
    parser.add_argument(
        "--start", type=str, default="2024-01-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="End date (YYYY-MM-DD), default=yesterday",
    )
    parser.add_argument(
        "--resume", action="store_true", default=True,
        help="Resume from last checkpoint",
    )
    parser.add_argument(
        "--max-days", type=int, default=0,
        help="Max days to scrape (0=unlimited)",
    )
    parser.add_argument(
        "--max-detail-pages", type=int, default=15,
        help="Max detail pages per day",
    )
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (
        datetime.strptime(args.end, "%Y-%m-%d")
        if args.end
        else datetime.now() - timedelta(days=1)
    )

    log.info("=" * 60)
    log.info("SCRIPT 126 (Playwright) -- Racing Index Scraper")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date > start_date:
            start_date = resume_date
            log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "racingindex_data.jsonl")

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
        total_records = checkpoint.get("total_records", 0)

        # --- Phase 1: Leaderboard (once) ---
        if not checkpoint.get("leaderboard_done"):
            log.info("--- Phase 1: Leaderboard / Top Rated ---")
            lb_data = scrape_leaderboard(page, datetime.now().strftime("%Y-%m-%d"))
            if first_nav and lb_data is not None:
                accept_cookies(page)
                first_nav = False
            if lb_data:
                for rec in lb_data:
                    append_jsonl(output_file, rec)
                    total_records += 1
                log.info("  Leaderboard: %d records", len(lb_data))
            save_checkpoint(CHECKPOINT_FILE, {
                "leaderboard_done": True,
                "total_records": total_records,
            })
            smart_pause(3.0, 1.5)

        # --- Phase 2: Daily ratings + results ---
        log.info("--- Phase 2: Daily ratings + results ---")

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")

            # Ratings page
            result = scrape_ratings_page(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Detail pages (limited)
                for race_url in result.get("race_links", [])[:args.max_detail_pages]:
                    detail = scrape_race_detail(page, race_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Results page
            results_data = scrape_results_day(page, date_str)
            if results_data:
                for rec in results_data:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 10 == 0:
                log.info(
                    "  %s | days=%d records=%d",
                    date_str, day_count, total_records,
                )
                save_checkpoint(CHECKPOINT_FILE, {
                    "leaderboard_done": True,
                    "last_date": date_str,
                    "total_records": total_records,
                })

            # Rotate browser every 80 days
            if day_count % 80 == 0:
                log.info("  Rotating browser context...")
                try:
                    page.close()
                    context.close()
                    browser.close()
                except Exception:
                    pass
                smart_pause(5.0, 2.0)
                browser, context, page = launch_browser(
                    pw, locale="en-GB", timezone="Europe/London"
                )
                first_nav = True

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {
            "leaderboard_done": True,
            "last_date": end_date.strftime("%Y-%m-%d"),
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info(
            "DONE: %d days, %d records -> %s",
            day_count, total_records, output_file,
        )
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
