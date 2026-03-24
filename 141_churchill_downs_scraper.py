#!/usr/bin/env python3
"""
Script 141 — Churchill Downs Scraper (Playwright)
Source : churchilldowns.com
Collecte : Kentucky Derby data, results, entries, stakes races,
           Oaks, Breeders' Cup prep races, Churchill Downs meet results
URL patterns :
  /racing/results/           -> race results
  /racing/entries/            -> daily entries
  /racing/stakes-schedule/   -> stakes schedule
  /derby/                    -> Kentucky Derby section
CRITIQUE pour : US Triple Crown data, Kentucky Derby, Churchill Downs stakes

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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.playwright import launch_browser, accept_cookies
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

SCRIPT_NAME = "141_churchill_downs"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

log = setup_logging("141_churchill_downs")

BASE_URL = "https://www.churchilldowns.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Key event / section paths
EVENT_PATHS = [
    "/derby/",
    "/derby/history/",
    "/derby/results/",
    "/derby/entries/",
    "/racing/stakes-schedule/",
    "/oaks/",
    "/oaks/history/",
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

def extract_race_links(soup):
    """Extract links to individual race entries / results from a listing page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(race|result|entr|stakes|derby)/', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_race_conditions(soup, date_str, race_url=""):
    """Extract race conditions: distance, surface, purse, class."""
    records = []
    for el in soup.find_all(["div", "section", "article", "dl"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["race-info", "race-detail",
                                                   "race-condition", "race-header",
                                                   "card-header", "entry-info",
                                                   "stakes-info"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "churchill_downs",
                    "type": "race_conditions",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Distance (furlongs or miles)
                dist_match = re.search(
                    r'(\d+(?:\s*\d/\d)?)\s*furlongs?|(\d+(?:\s*\d/\d)?)\s*miles?|'
                    r'(\d+)f\b|(\d+)\s*yds?\b',
                    text, re.I
                )
                if dist_match:
                    record["distance_raw"] = dist_match.group(0).strip()

                # Surface
                surface_match = re.search(
                    r'(dirt|turf|synthetic|polytrack|all[- ]weather|tapeta)',
                    text, re.I
                )
                if surface_match:
                    record["surface"] = surface_match.group(1).strip()

                # Track condition
                track_match = re.search(
                    r'(fast|firm|good|yielding|soft|sloppy|muddy|heavy|wet fast|sealed)',
                    text, re.I
                )
                if track_match:
                    record["track_condition"] = track_match.group(1).strip()

                # Purse
                purse_match = re.search(r'\$\s*([\d,]+)', text)
                if purse_match:
                    record["purse"] = purse_match.group(0).strip()

                # Race type / class
                type_match = re.search(
                    r'(grade?\s*[iI1]{1,3}|graded stakes|stakes|allowance|'
                    r'maiden|claiming|handicap|optional claiming|starter)',
                    text, re.I
                )
                if type_match:
                    record["race_type"] = type_match.group(1).strip()

                # Age/sex restrictions
                restrict_match = re.search(
                    r'(\d+)\s*(?:year[- ]olds?|yo)\s*(?:and up|&\s*up|\+)?|'
                    r'(fillies|colts|geldings|mares)',
                    text, re.I
                )
                if restrict_match:
                    record["restrictions"] = restrict_match.group(0).strip()

                records.append(record)

    return records


def extract_runners_table(soup, date_str, race_url="", race_name=""):
    """Extract runner data from entry or result tables."""
    records = []
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
                "source": "churchill_downs",
                "type": "runner",
                "race_name": race_name,
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
    return records


def extract_runner_cards(soup, date_str, race_url="", race_name=""):
    """Extract runner data from card-based layouts (non-table)."""
    records = []
    for el in soup.find_all(["div", "article", "li", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["runner", "horse", "entry",
                                                   "participant", "contender",
                                                   "selection", "starter"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "churchill_downs",
                    "type": "runner_card",
                    "race_name": race_name,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                heading = el.find(["h2", "h3", "h4", "strong", "a"])
                if heading:
                    record["horse_name"] = heading.get_text(strip=True)

                for span in el.find_all(["span", "small", "p", "div"], class_=True):
                    sc = " ".join(span.get("class", []))
                    st = span.get_text(strip=True)
                    if any(k in sc.lower() for k in ["jockey", "rider"]):
                        record["jockey"] = st
                    elif any(k in sc.lower() for k in ["trainer"]):
                        record["trainer"] = st
                    elif any(k in sc.lower() for k in ["owner"]):
                        record["owner"] = st
                    elif any(k in sc.lower() for k in ["weight", "lbs"]):
                        record["weight"] = st
                    elif any(k in sc.lower() for k in ["odds", "ml", "morning-line"]):
                        record["morning_line"] = st
                    elif any(k in sc.lower() for k in ["post", "pp", "gate"]):
                        record["post_position"] = st
                    elif any(k in sc.lower() for k in ["sire", "dam", "pedigree"]):
                        record["pedigree_info"] = st

                for attr_name, attr_val in el.attrs.items():
                    if attr_name.startswith("data-"):
                        clean = attr_name.replace("data-", "").replace("-", "_")
                        record[clean] = attr_val

                records.append(record)
    return records


def extract_results_data(soup, date_str, race_url=""):
    """Extract finishing positions and result data."""
    records = []
    for el in soup.find_all(["div", "section", "article", "ol", "ul"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "finish", "placed",
                                                   "winner", "payout", "exacta",
                                                   "trifecta", "superfecta",
                                                   "race-result"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "churchill_downs",
                    "type": "result_block",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Extract payout/odds
                payout_matches = re.findall(r'\$\s*([\d,.]+)', text)
                if payout_matches:
                    record["payouts_found"] = payout_matches[:10]

                # Fractional time
                time_matches = re.findall(r'(\d+:\d{2}(?:\.\d{1,2})?)', text)
                if time_matches:
                    record["times_found"] = time_matches[:5]

                records.append(record)
    return records


def extract_derby_data(soup, date_str, derby_url=""):
    """Extract Kentucky Derby specific data (history, entries, points)."""
    records = []
    for el in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["derby", "triple-crown", "points",
                                                   "leaderboard", "contender",
                                                   "history", "past-winner",
                                                   "stakes", "oaks"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 5000:
                record = {
                    "date": date_str,
                    "source": "churchill_downs",
                    "type": "derby_data",
                    "contenu": text[:3000],
                    "classes_css": classes,
                    "url": derby_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                headings = el.find_all(["h2", "h3", "h4"])
                if headings:
                    record["section_headings"] = [
                        h.get_text(strip=True) for h in headings
                        if h.get_text(strip=True)
                    ]

                records.append(record)
    return records


def extract_stakes_schedule(soup, date_str, schedule_url=""):
    """Extract stakes schedule/calendar data."""
    records = []
    for el in soup.find_all(["div", "section", "table", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stakes", "schedule", "calendar",
                                                   "fixture", "event"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 5000:
                record = {
                    "date": date_str,
                    "source": "churchill_downs",
                    "type": "stakes_schedule",
                    "contenu": text[:3000],
                    "classes_css": classes,
                    "url": schedule_url,
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
                    "source": "churchill_downs",
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
                    "source": "churchill_downs",
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

def scrape_entries_day(page, date_str):
    """Scrape the Churchill Downs entries page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"entries_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/entries/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"entries_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str, race_url=url))
    records.extend(extract_runners_table(soup, date_str, race_url=url))
    records.extend(extract_runner_cards(soup, date_str, race_url=url))

    race_links = extract_race_links(soup)

    result = {"records": records, "race_links": race_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape an individual race entry/result page."""
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

    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str, race_url=race_url))
    records.extend(extract_runners_table(soup, date_str, race_url=race_url, race_name=race_name))
    records.extend(extract_runner_cards(soup, date_str, race_url=race_url, race_name=race_name))
    records.extend(extract_results_data(soup, date_str, race_url=race_url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results_day(page, date_str):
    """Scrape results page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/results/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str, race_url=url))
    records.extend(extract_runners_table(soup, date_str, race_url=url))
    records.extend(extract_results_data(soup, date_str, race_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_derby_events(page, date_str):
    """Scrape Kentucky Derby, Oaks, and stakes schedule pages."""
    all_records = []
    for path in EVENT_PATHS:
        cache_key = re.sub(r'[^a-zA-Z0-9]', '_', path)
        cache_file = os.path.join(CACHE_DIR, f"event_{cache_key}.json")
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                all_records.extend(json.load(f))
            continue

        url = f"{BASE_URL}{path}"
        html = navigate_with_retry(page, url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        records = []

        records.extend(extract_embedded_json_data(soup, date_str))
        records.extend(extract_derby_data(soup, date_str, derby_url=url))
        records.extend(extract_stakes_schedule(soup, date_str, schedule_url=url))
        records.extend(extract_race_conditions(soup, date_str, race_url=url))

        # Follow race links from event page
        race_links = extract_race_links(soup)
        for race_url_link in race_links[:20]:
            detail = scrape_race_detail(page, race_url_link, date_str)
            if detail:
                records.extend(detail)
            smart_pause(1.5, 0.8)

        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        all_records.extend(records)
        smart_pause(2.0, 1.0)

    return all_records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 141 — Churchill Downs Scraper (Kentucky Derby, results, entries)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=yesterday")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--derby-only", action="store_true",
                        help="Only scrape Derby/Oaks/stakes pages, skip daily results")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
                else datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 141 — Churchill Downs Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "churchill_downs_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US)")

        first_nav = True
        total_records = 0

        # Scrape Derby/Oaks/stakes pages once
        derby_records = scrape_derby_events(page, start_date.strftime("%Y-%m-%d"))
        if first_nav and derby_records:
            accept_cookies(page)
            first_nav = False
        for rec in derby_records:
            append_jsonl(output_file, rec)
            total_records += 1
        log.info("  Derby/Stakes events: %d records", len(derby_records))

        if args.derby_only:
            log.info("  --derby-only: skipping daily scraping")
        else:
            current = start_date
            day_count = 0

            while current <= end_date:
                if args.max_days and day_count >= args.max_days:
                    break

                date_str = current.strftime("%Y-%m-%d")

                # Scrape entries
                result = scrape_entries_day(page, date_str)

                if first_nav and result is not None:
                    accept_cookies(page)
                    first_nav = False

                if result:
                    records = result.get("records", [])

                    for race_url in result.get("race_links", [])[:15]:
                        detail = scrape_race_detail(page, race_url, date_str)
                        if detail:
                            records.extend(detail)
                        smart_pause(1.5, 0.8)

                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1

                # Also scrape results
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
        log.info("DONE: %d total records -> %s", total_records, output_file)
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
