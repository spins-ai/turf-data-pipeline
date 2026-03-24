#!/usr/bin/env python3
"""
Script 108 — Matchbook Exchange Scraper (Playwright)
Source : matchbook.com
Collecte : Betting exchange data, horse racing odds, back/lay prices, liquidity
URL patterns :
  /events/horse-racing           -> liste des courses
  /events/horse-racing/{event}   -> marche d'une course
  /edge/rest/events              -> API endpoint (JSON)
CRITIQUE pour : Exchange Odds, Back/Lay Spreads, Market Liquidity, True Odds

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

SCRIPT_NAME = "108_matchbook"
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

log = setup_logging("108_matchbook")

BASE_URL = "https://www.matchbook.com"
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

def extract_event_links(soup):
    """Extract links to individual event/market pages."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'(horse-racing|racing)/[^?#]+', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            # Skip generic category pages
            if not re.search(r'horse-racing/?$', full_url):
                links.add(full_url)
    return sorted(links)


def extract_back_lay_odds(soup, date_str, event_url=""):
    """Extract back and lay odds from exchange market display."""
    records = []
    for el in soup.find_all(["div", "span", "td", "button"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["back", "lay", "odds", "price",
                                                   "best-price", "available",
                                                   "offer", "bid"]):
            text = el.get_text(strip=True)
            if text and re.search(r'\d+\.?\d*', text):
                record = {
                    "date": date_str,
                    "source": "matchbook",
                    "type": "exchange_odds",
                    "contenu": text[:200],
                    "classes_css": classes,
                    "url": event_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse back/lay side
                if "back" in classes.lower():
                    record["side"] = "back"
                elif "lay" in classes.lower():
                    record["side"] = "lay"
                # Parse decimal odds
                odds_match = re.search(r'(\d+\.?\d*)', text)
                if odds_match:
                    record["odds_decimal"] = odds_match.group(1)
                # Parse stake/liquidity amount
                amount_match = re.search(r'[\$\xA3\u20AC]?\s*([\d,]+(?:\.\d{2})?)', text)
                if amount_match:
                    record["amount_available"] = amount_match.group(1).replace(",", "")
                records.append(record)
    return records


def extract_market_depth(soup, date_str, event_url=""):
    """Extract market depth / liquidity info from exchange."""
    records = []
    for el in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["market", "depth", "liquidity",
                                                   "matched", "volume",
                                                   "traded", "ladder"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "matchbook",
                    "type": "market_depth",
                    "contenu": text[:1000],
                    "classes_css": classes,
                    "url": event_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse total matched amount
                matched_match = re.search(
                    r'matched\s*:?\s*[\$\xA3\u20AC]?\s*([\d,]+(?:\.\d{2})?)',
                    text, re.I
                )
                if matched_match:
                    record["total_matched"] = matched_match.group(1).replace(",", "")
                records.append(record)
    return records


def extract_runner_odds_table(soup, date_str, event_url=""):
    """Extract runner data with back/lay odds from market tables."""
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
                "source": "matchbook",
                "type": "runner_odds",
                "url": event_url,
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

def scrape_racing_index(page, date_str):
    """Scrape the Matchbook horse racing index page."""
    cache_file = os.path.join(CACHE_DIR, f"index_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/events/horse-racing"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"index_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "matchbook", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "matchbook", date_str=date_str))
    records.extend(extract_back_lay_odds(soup, date_str, event_url=url))
    records.extend(extract_market_depth(soup, date_str, event_url=url))
    records.extend(extract_runner_odds_table(soup, date_str, event_url=url))

    event_links = extract_event_links(soup)

    # Extract meeting/event blocks
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["event", "meeting", "venue",
                                                   "market", "coupon",
                                                   "race-card", "fixture"]):
            record = {
                "date": date_str,
                "source": "matchbook",
                "type": "meeting",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong", "a"])
            if title:
                record["venue"] = title.get_text(strip=True)
            # Time extraction
            time_el = div.find(["time", "span"], class_=lambda c: c and
                               any(kw in c.lower() for kw in ["time", "clock", "start"]))
            if time_el:
                record["race_time"] = time_el.get_text(strip=True)
            records.append(record)

    result = {"records": records, "event_links": event_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_event_detail(page, event_url, date_str):
    """Scrape individual event/market page for detailed odds data."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', event_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"event_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, event_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Event title
    event_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            event_name = text
            break

    # Event metadata
    metadata = {}
    page_text = soup.get_text()

    time_match = re.search(r'(\d{1,2}:\d{2})', page_text)
    if time_match:
        metadata["race_time"] = time_match.group(1)

    venue_match = re.search(
        r'(Ascot|Cheltenham|Newmarket|York|Epsom|Aintree|Goodwood|'
        r'Kempton|Sandown|Doncaster|Haydock|Lingfield|Wolverhampton|'
        r'Leopardstown|Curragh|Galway|Fairyhouse|Punchestown|Dundalk)',
        page_text, re.I
    )
    if venue_match:
        metadata["venue"] = venue_match.group(1).strip()

    # Structured data extraction
    records.extend(extract_embedded_json_data(soup, "matchbook", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "matchbook", date_str=date_str))
    records.extend(extract_back_lay_odds(soup, date_str, event_url=event_url))
    records.extend(extract_market_depth(soup, date_str, event_url=event_url))

    # Runner odds from tables and structured divs
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
                "source": "matchbook",
                "type": "runner_detail",
                "event_name": event_name,
                "metadata": metadata,
                "url": event_url,
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

    # Selection/runner cards (exchange-style layout, not always table-based)
    for div in soup.find_all(["div", "li", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["selection", "runner", "participant",
                                                   "outcome", "competitor"]):
            text = div.get_text(strip=True)
            if text and 3 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": "matchbook",
                    "type": "selection",
                    "event_name": event_name,
                    "metadata": metadata,
                    "contenu": text[:500],
                    "classes_css": classes,
                    "url": event_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse odds from selection
                odds_match = re.search(r'(\d+\.?\d*)', text)
                if odds_match:
                    record["odds_decimal"] = odds_match.group(1)
                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 108 — Matchbook Exchange Scraper (exchange odds, back/lay)"
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
    log.info("SCRIPT 108 — Matchbook Exchange Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "matchbook_data.jsonl")

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

            # Scrape racing index
            result = scrape_racing_index(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual event pages (limit to avoid overload)
                for event_url in result.get("event_links", [])[:15]:
                    detail = scrape_event_detail(page, event_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
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
