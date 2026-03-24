#!/usr/bin/env python3
"""
Script 134 — William Hill Horse Racing Scraper (Playwright)
Source : williamhill.com
Collecte : horse racing odds, each-way terms, specials, enhanced odds
URL patterns :
  /betting/horse-racing             -> horse racing hub
  /betting/horse-racing/meetings    -> today's meetings
  /betting/horse-racing/specials    -> specials / enhanced offers
CRITIQUE pour : Odds, Each-Way Terms, Specials Markets

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "134_william_hill"
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
from utils.html_parsing import extract_runners_table
from utils.html_parsing import extract_race_links

log = setup_logging("134_william_hill")

BASE_URL = "https://sports.williamhill.com"
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
            time.sleep(random.uniform(1.5, 3.0))
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

def extract_meeting_links(soup):
    """Extract links to individual meetings / race cards."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/betting/horse-racing/[a-z]', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_each_way_terms(soup, date_str):
    """Extract each-way terms (e.g., 1/4 odds, 3 places)."""
    records = []
    page_text = soup.get_text()

    # Pattern: "Each-Way: 1/4 odds, 3 places" or similar
    ew_patterns = [
        r'each[\s-]*way\s*:?\s*(\d/\d)\s*(?:odds?)?[,\s]*(\d+)\s*place',
        r'E/W\s*:?\s*(\d/\d)\s*[,\s]*(\d+)\s*place',
        r'(\d/\d)\s*(?:the\s)?odds?\s*[,\s]*(\d+)\s*place',
    ]

    for pattern in ew_patterns:
        for match in re.finditer(pattern, page_text, re.I):
            records.append({
                "date": date_str,
                "source": "william_hill",
                "type": "each_way_terms",
                "fraction": match.group(1),
                "places": match.group(2),
                "raw_text": match.group(0)[:200],
                "scraped_at": datetime.now().isoformat(),
            })

    # Also look for EW terms in specific elements
    for el in soup.find_all(["div", "span", "p", "small"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in [
            "each-way", "eachway", "ew-terms", "ew_terms",
            "market-terms", "terms", "place-terms",
        ]):
            if text and 3 < len(text) < 300:
                records.append({
                    "date": date_str,
                    "source": "william_hill",
                    "type": "each_way_terms",
                    "contenu": text[:200],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_odds_data(soup, date_str, race_url=""):
    """Extract runner names and odds from the page."""
    records = []
    # William Hill uses structured market containers
    for row in soup.find_all(["div", "tr", "li", "article"], class_=True):
        classes = " ".join(row.get("class", []))
        if any(kw in classes.lower() for kw in [
            "runner", "participant", "selection", "bet-item",
            "btmarket__runner", "btmarket__selection",
            "sp-o-market__selection", "market-content",
        ]):
            runner_name = ""
            odds_text = ""
            jockey = ""
            trainer = ""

            # Runner name
            for name_el in row.find_all(["span", "div", "a", "strong"], class_=True):
                name_classes = " ".join(name_el.get("class", []))
                if any(k in name_classes.lower() for k in [
                    "name", "runner-name", "selection-name", "participant",
                    "btmarket__name", "sp-o-market__name",
                ]):
                    runner_name = name_el.get_text(strip=True)
                    break

            # Odds
            for odds_el in row.find_all(["span", "button", "div", "a"], class_=True):
                odds_classes = " ".join(odds_el.get("class", []))
                if any(k in odds_classes.lower() for k in [
                    "odds", "price", "betbutton", "sp-o-market__price",
                    "btmarket__odds", "btn--bet",
                ]):
                    odds_text = odds_el.get_text(strip=True)
                    break

            # Jockey / Trainer
            for meta_el in row.find_all(["span", "small", "div"], class_=True):
                meta_classes = " ".join(meta_el.get("class", []))
                meta_text = meta_el.get_text(strip=True)
                if "jockey" in meta_classes.lower():
                    jockey = meta_text
                elif "trainer" in meta_classes.lower():
                    trainer = meta_text

            if runner_name or odds_text:
                record = {
                    "date": date_str,
                    "source": "william_hill",
                    "type": "market_runner",
                    "runner_name": runner_name[:200],
                    "odds_text": odds_text,
                    "jockey": jockey[:100],
                    "trainer": trainer[:100],
                    "url": race_url,
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Data attributes
                for attr_name, attr_val in row.attrs.items():
                    if attr_name.startswith("data-"):
                        clean = attr_name.replace("data-", "").replace("-", "_")
                        record[clean] = attr_val
                records.append(record)
    return records


def extract_specials(soup, date_str):
    """Extract specials / enhanced odds / boosted markets."""
    records = []
    for el in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in [
            "special", "enhanced", "boost", "promo", "featured",
            "price-boost", "offer", "acca",
        ]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                heading = ""
                h_el = el.find(["h2", "h3", "h4", "strong"])
                if h_el:
                    heading = h_el.get_text(strip=True)

                records.append({
                    "date": date_str,
                    "source": "william_hill",
                    "type": "special",
                    "heading": heading[:300],
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_embedded_json(soup, date_str):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, (dict, list)):
                records.append({
                    "date": date_str,
                    "source": "william_hill",
                    "type": "embedded_json",
                    "data_id": script.get("id", ""),
                    "data": data if isinstance(data, dict) else {"items": data},
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    # __NEXT_DATA__ or similar SSR payloads
    for script in soup.find_all("script", {"id": "__NEXT_DATA__"}):
        try:
            data = json.loads(script.string or "")
            page_props = data.get("props", {}).get("pageProps", {})
            if page_props:
                records.append({
                    "date": date_str,
                    "source": "william_hill",
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_racing_hub(page, date_str):
    """Scrape the WH horse racing hub for meetings and overview data."""
    cache_file = os.path.join(CACHE_DIR, f"hub_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/betting/horse-racing"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"hub_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str))
    records.extend(extract_odds_data(soup, date_str))
    records.extend(extract_each_way_terms(soup, date_str))
    records.extend(extract_specials(soup, date_str))

    meeting_links = extract_meeting_links(soup)

    result = {"records": records, "meeting_links": meeting_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_meeting(page, meeting_url, date_str):
    """Scrape an individual meeting page for race cards and odds."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', meeting_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"meeting_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, meeting_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Venue name
    venue_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 2:
            venue_name = text
            break

    records.extend(extract_embedded_json(soup, date_str))
    records.extend(extract_odds_data(soup, date_str, race_url=meeting_url))
    records.extend(extract_each_way_terms(soup, date_str))
    records.extend(extract_runners_table(soup, "william_hill", date_str=date_str, race_url=meeting_url))

    # Tag records with venue
    for rec in records:
        rec["venue"] = venue_name[:200]

    # Get individual race links
    race_links = extract_race_links(soup, base_url=BASE_URL)

    result = {"records": records, "race_links": race_links, "venue": venue_name}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape an individual race page for detailed runner/odds data."""
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

    # Race name
    race_name = ""
    for h in soup.find_all(["h1", "h2", "h3"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Race conditions
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(
        r'(\d+)f\b|(\d+)\s*furlongs?|(\d+)m\b|(\d[\d,]*)\s*m(?:etres?)?',
        page_text, re.I
    )
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

    records.extend(extract_embedded_json(soup, date_str))
    records.extend(extract_odds_data(soup, date_str, race_url=race_url))
    records.extend(extract_each_way_terms(soup, date_str))
    records.extend(extract_runners_table(soup, "william_hill", date_str=date_str, race_url=race_url))

    # Tag records
    for rec in records:
        rec["race_name"] = race_name[:300]
        rec["conditions"] = conditions

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_specials_page(page, date_str):
    """Scrape the specials / enhanced odds page."""
    cache_file = os.path.join(CACHE_DIR, f"specials_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/betting/horse-racing/specials"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_specials(soup, date_str))
    records.extend(extract_embedded_json(soup, date_str))
    records.extend(extract_odds_data(soup, date_str, race_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 134 — William Hill Horse Racing Scraper (odds, each-way, specials)"
    )
    parser.add_argument("--start", type=str, default=None,
                        help="Start date (YYYY-MM-DD), default=today")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=today")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--specials", action="store_true", default=True,
                        help="Also scrape specials / enhanced odds page")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    start_date = datetime.strptime(args.start or today, "%Y-%m-%d")
    end_date = datetime.strptime(args.end or today, "%Y-%m-%d")

    log.info("=" * 60)
    log.info("SCRIPT 134 — William Hill Horse Racing Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date <= end_date:
            start_date = resume_date
            log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "william_hill_data.jsonl")

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
            log.info("Scraping %s ...", date_str)

            # Hub page
            result = scrape_racing_hub(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual meetings
                for meeting_url in result.get("meeting_links", [])[:12]:
                    meeting_data = scrape_meeting(page, meeting_url, date_str)
                    if meeting_data:
                        records.extend(meeting_data.get("records", []))
                        # Scrape individual races within meeting
                        for race_url in meeting_data.get("race_links", [])[:10]:
                            detail = scrape_race_detail(page, race_url, date_str)
                            if detail:
                                records.extend(detail)
                            smart_pause(1.5, 0.8)
                    smart_pause(2.0, 1.0)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Specials page
            if args.specials:
                specials_data = scrape_specials_page(page, date_str)
                if specials_data:
                    for rec in (specials_data if isinstance(specials_data, list) else []):
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
            smart_pause(2.0, 1.0)

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
