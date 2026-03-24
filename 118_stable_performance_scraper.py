#!/usr/bin/env python3
"""
Script 118 — Stable Performance Scraper (Playwright)
Source : stableperformance.com (and similar trainer stats portals)
Collecte : Trainer/stable statistics, strike rates, yard form, seasonal trends,
           course/going preferences, trainer-jockey combinations
URL patterns :
  /trainers/               -> trainer directory
  /trainer/{name}/         -> trainer profile
  /trainer/{name}/stats/   -> detailed statistics
  /stable/{name}/          -> stable overview and recent results
CRITIQUE pour : Trainer Strike Rates, Yard Form, Stable Stars,
                Course/Going Preferences, Trainer-Jockey Combos

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

SCRIPT_NAME = "118_stable_performance"
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

log = setup_logging("118_stable_performance")

BASE_URL = "https://www.stableperformance.com"
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

def extract_trainer_links(soup):
    """Extract links to individual trainer/stable profile pages."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(trainer|stable)/', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_trainer_stats(soup, date_str):
    """Extract trainer/stable statistics (strike rates, win counts, etc.)."""
    records = []
    for el in soup.find_all(["div", "span", "td", "section", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stat", "strike", "rate", "win",
                                                   "record", "performance",
                                                   "percentage", "summary",
                                                   "yard", "form"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "stableperformance",
                    "type": "trainer_stat",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract strike rate percentage
                pct_match = re.search(r'(\d{1,3}(?:\.\d{1,2})?)\s*%', text)
                if pct_match:
                    record["strike_rate_pct"] = pct_match.group(1)
                # Try to extract win/run ratio
                ratio_match = re.search(r'(\d+)\s*/\s*(\d+)', text)
                if ratio_match:
                    record["wins"] = ratio_match.group(1)
                    record["runs"] = ratio_match.group(2)
                records.append(record)
    return records


def extract_stable_form(soup, date_str):
    """Extract recent stable/yard form data."""
    records = []
    for el in soup.find_all(["div", "section", "article", "ul", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "recent", "result",
                                                   "winner", "runner",
                                                   "history", "stable",
                                                   "yard", "trend"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "stableperformance",
                    "type": "stable_form",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_course_going_prefs(soup, date_str):
    """Extract trainer course and going preference data."""
    records = []
    for el in soup.find_all(["div", "table", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["course", "going", "ground",
                                                   "preference", "track",
                                                   "venue", "surface"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "stableperformance",
                    "type": "course_going_pref",
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
                records.append(record)
    return records


def extract_trainers_table(soup, date_str, page_url=""):
    """Extract trainer data from ranking/league tables."""
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
                "source": "stableperformance",
                "type": "trainer_table",
                "url": page_url,
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


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_trainer_directory(page, date_str):
    """Scrape trainer directory page for profile links and overview data."""
    cache_file = os.path.join(CACHE_DIR, f"directory_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/trainers/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"directory_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract structured data
    records.extend(extract_embedded_json_data(soup, "stableperformance", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "stableperformance", date_str=date_str))
    records.extend(extract_trainers_table(soup, date_str, page_url=url))
    records.extend(extract_trainer_stats(soup, date_str))

    # Extract trainer links
    trainer_links = extract_trainer_links(soup)

    result = {"records": records, "trainer_links": trainer_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_trainer_profile(page, profile_url, date_str):
    """Scrape individual trainer profile page for detailed statistics."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', profile_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"profile_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, profile_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Trainer name
    trainer_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 2:
            trainer_name = text
            break

    # Extract all data types
    records.extend(extract_embedded_json_data(soup, "stableperformance", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "stableperformance", date_str=date_str))
    records.extend(extract_trainer_stats(soup, date_str))
    records.extend(extract_stable_form(soup, date_str))
    records.extend(extract_course_going_prefs(soup, date_str))
    records.extend(extract_trainers_table(soup, date_str, page_url=profile_url))

    # Tag all records with trainer name and URL
    for rec in records:
        rec["trainer_name"] = trainer_name
        rec["url"] = profile_url

    # Trainer-jockey combo sections
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["jockey", "combo", "combination",
                                                   "partnership", "pairing"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "stableperformance",
                    "type": "trainer_jockey_combo",
                    "trainer_name": trainer_name,
                    "contenu": text[:2000],
                    "url": profile_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_seasonal_trends(page, date_str):
    """Scrape seasonal/monthly trainer trend pages."""
    cache_file = os.path.join(CACHE_DIR, f"trends_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/trends/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "stableperformance", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "stableperformance", date_str=date_str))
    records.extend(extract_trainers_table(soup, date_str, page_url=url))
    records.extend(extract_trainer_stats(soup, date_str))

    # Trend-specific extraction
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["trend", "season", "month",
                                                   "pattern", "hot", "cold"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "stableperformance",
                    "type": "seasonal_trend",
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
        description="Script 118 — Stable Performance Scraper (trainer/stable statistics)"
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
    log.info("SCRIPT 118 — Stable Performance Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "stable_performance_data.jsonl")

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

            # Scrape trainer directory
            result = scrape_trainer_directory(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual trainer profiles (limit to avoid overload)
                for profile_url in result.get("trainer_links", [])[:15]:
                    detail = scrape_trainer_profile(page, profile_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Also scrape seasonal trends
            trends_data = scrape_seasonal_trends(page, date_str)
            if trends_data:
                for rec in trends_data:
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
