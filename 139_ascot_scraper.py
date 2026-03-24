#!/usr/bin/env python3
"""
Script 139 — Ascot Racecourse Scraper (Playwright)
Source : ascot.com
Collecte : UK flat/jump results, race cards, festival info (Royal Ascot, Champions Day)
URL patterns :
  /racing/results/                -> resultats par date
  /racing/race-cards/             -> cartes de course
  /racing/festivals/royal-ascot/  -> festival Royal Ascot
  /racing/festivals/champions-day/ -> Champions Day
CRITIQUE pour : UK prestige flat/jump data, Royal Ascot, festival schedules

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

SCRIPT_NAME = "139_ascot"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

log = setup_logging("139_ascot")

BASE_URL = "https://www.ascot.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Key festivals / fixture dates to scrape
FESTIVAL_PATHS = [
    "/racing/festivals/royal-ascot/",
    "/racing/festivals/champions-day/",
    "/racing/festivals/king-george/",
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
    """Extract links to individual race cards / results from a listing page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(race-card|result|race|runner)/', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_race_conditions(soup, date_str, race_url=""):
    """Extract race conditions: distance, going, class, type, prize."""
    records = []
    page_text = soup.get_text()

    for el in soup.find_all(["div", "section", "article", "dl"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["race-info", "race-detail",
                                                   "race-condition", "race-header",
                                                   "card-header", "racecard-header"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "ascot",
                    "type": "race_conditions",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Distance
                dist_match = re.search(
                    r'(\d+)f\b|(\d+)\s*furlongs?|(\d+)m\s*(\d+)f|(\d[\d,]*)\s*m(?:etres?)?',
                    text, re.I
                )
                if dist_match:
                    record["distance_raw"] = dist_match.group(0)

                # Going
                going_match = re.search(
                    r'(firm|good to firm|good|good to soft|soft|heavy|'
                    r'yielding|standard|slow|fast)',
                    text, re.I
                )
                if going_match:
                    record["going"] = going_match.group(1).strip()

                # Race class
                class_match = re.search(r'class\s*(\d)', text, re.I)
                if class_match:
                    record["race_class"] = class_match.group(1)

                # Race type (flat vs jump)
                type_match = re.search(
                    r'(flat|hurdle|chase|national hunt|nh flat|bumper|stakes|handicap|'
                    r'listed|group\s*[123])',
                    text, re.I
                )
                if type_match:
                    record["race_type"] = type_match.group(1).strip()

                # Prize money
                prize_match = re.search(r'[£$]\s*([\d,]+)', text)
                if prize_match:
                    record["prize_money"] = prize_match.group(0).strip()

                records.append(record)

    return records


def extract_runners_table(soup, date_str, race_url="", race_name=""):
    """Extract runner data from race card or result tables."""
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
                "source": "ascot",
                "type": "runner",
                "race_name": race_name,
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
    return records


def extract_runner_cards(soup, date_str, race_url="", race_name=""):
    """Extract runner data from card-based layouts (non-table)."""
    records = []
    for el in soup.find_all(["div", "article", "li", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["runner", "horse-card", "entry",
                                                   "participant", "selection",
                                                   "runner-card", "racecard-runner"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "ascot",
                    "type": "runner_card",
                    "race_name": race_name,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Try to extract horse name from heading
                heading = el.find(["h2", "h3", "h4", "strong", "a"])
                if heading:
                    record["horse_name"] = heading.get_text(strip=True)

                # Jockey / trainer
                for span in el.find_all(["span", "small", "p", "div"], class_=True):
                    sc = " ".join(span.get("class", []))
                    st = span.get_text(strip=True)
                    if any(k in sc.lower() for k in ["jockey", "rider"]):
                        record["jockey"] = st
                    elif any(k in sc.lower() for k in ["trainer"]):
                        record["trainer"] = st
                    elif any(k in sc.lower() for k in ["silk", "colour"]):
                        record["silks"] = st

                # Data attributes
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
        if any(kw in classes.lower() for kw in ["result", "finishing", "placed",
                                                   "winner", "returns", "dividend",
                                                   "race-result"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "ascot",
                    "type": "result_block",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Extract SP odds
                sp_matches = re.findall(r'(\d{1,3})/(\d{1,3})', text)
                if sp_matches:
                    record["sp_odds_found"] = [f"{n}/{d}" for n, d in sp_matches[:10]]

                records.append(record)
    return records


def extract_festival_info(soup, date_str, festival_url=""):
    """Extract festival-specific information (schedule, feature races)."""
    records = []
    for el in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["festival", "fixture", "event",
                                                   "schedule", "programme",
                                                   "feature", "meeting"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 5000:
                record = {
                    "date": date_str,
                    "source": "ascot",
                    "type": "festival_info",
                    "contenu": text[:3000],
                    "classes_css": classes,
                    "url": festival_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Try to extract race names from headings
                headings = el.find_all(["h2", "h3", "h4"])
                if headings:
                    record["feature_races"] = [
                        h.get_text(strip=True) for h in headings
                        if h.get_text(strip=True)
                    ]

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
                    "source": "ascot",
                    "type": "embedded_json",
                    "data_id": script.get("id", ""),
                    "data": data,
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
                    "source": "ascot",
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_going_data(soup, date_str):
    """Extract going/ground condition data."""
    records = []
    for el in soup.find_all(["div", "span", "p", "td", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["going", "ground", "terrain",
                                                   "surface", "track-condition",
                                                   "course-info"]):
            if text and 2 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "ascot",
                    "type": "going_data",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
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

def scrape_racecards_day(page, date_str):
    """Scrape the Ascot race cards page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/race-cards/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"racecards_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_going_data(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str, race_url=url))
    records.extend(extract_runners_table(soup, date_str, race_url=url))
    records.extend(extract_runner_cards(soup, date_str, race_url=url))

    race_links = extract_race_links(soup)

    result = {"records": records, "race_links": race_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape an individual race card/result page for detailed runner data."""
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

    # Race title
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str, race_url=race_url))
    records.extend(extract_going_data(soup, date_str))
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
    records.extend(extract_going_data(soup, date_str))
    records.extend(extract_runners_table(soup, date_str, race_url=url))
    records.extend(extract_results_data(soup, date_str, race_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_festivals(page, date_str):
    """Scrape festival pages for Royal Ascot, Champions Day, etc."""
    all_records = []
    for path in FESTIVAL_PATHS:
        cache_key = re.sub(r'[^a-zA-Z0-9]', '_', path)
        cache_file = os.path.join(CACHE_DIR, f"festival_{cache_key}.json")
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
        records.extend(extract_festival_info(soup, date_str, festival_url=url))
        records.extend(extract_race_conditions(soup, date_str, race_url=url))

        # Follow race links from festival page
        race_links = extract_race_links(soup)
        for race_url in race_links[:20]:
            detail = scrape_race_detail(page, race_url, date_str)
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
        description="Script 139 — Ascot Racecourse Scraper (UK flat/jump results, festival info)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=yesterday")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--festivals-only", action="store_true",
                        help="Only scrape festival pages, skip daily results")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
                else datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 139 — Ascot Racecourse Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "ascot_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-GB", timezone="Europe/London"
        )
        log.info("Browser launched (headless Chromium, locale=en-GB)")

        first_nav = True
        total_records = 0

        # Scrape festival pages once
        festival_records = scrape_festivals(page, start_date.strftime("%Y-%m-%d"))
        if first_nav and festival_records:
            accept_cookies(page)
            first_nav = False
        for rec in festival_records:
            append_jsonl(output_file, rec)
            total_records += 1
        log.info("  Festivals: %d records", len(festival_records))

        if args.festivals_only:
            log.info("  --festivals-only: skipping daily scraping")
        else:
            current = start_date
            day_count = 0

            while current <= end_date:
                if args.max_days and day_count >= args.max_days:
                    break

                date_str = current.strftime("%Y-%m-%d")

                # Scrape racecards
                result = scrape_racecards_day(page, date_str)

                if first_nav and result is not None:
                    accept_cookies(page)
                    first_nav = False

                if result:
                    records = result.get("records", [])

                    # Scrape individual race pages
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
