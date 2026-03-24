#!/usr/bin/env python3
"""
Script 109 — Racing Australia Scraper (Playwright)
Source : racingaustralia.horse
Collecte : Australian racing data, race results, fields, form guides, black-type results
URL patterns :
  /FreeFields/Results.aspx?Key={date}  -> resultats du jour
  /FreeFields/Field.aspx?Key={date}    -> champs du jour
  /FreeFields/Calendar.aspx             -> calendrier reunions
  /FreeFields/Results.aspx?Key={date}&MeetingCode={code} -> resultats reunion
CRITIQUE pour : AU Racing Model, International Coverage, Thoroughbred Cross-Validation

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

SCRIPT_NAME = "109_racing_australia"
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
from utils.html_parsing import extract_runners_table

log = setup_logging("109_racing_australia")

BASE_URL = "https://www.racingaustralia.horse"
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

def extract_meeting_links(soup, date_str):
    """Extract links to individual meetings from a day page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'(Results|Field|RaceDay)\.aspx', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_race_conditions(soup, date_str):
    """Extract race conditions (distance, going, class, prize)."""
    records = []
    page_text = soup.get_text()

    for el in soup.find_all(["div", "span", "td", "p", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["race-info", "condition", "details",
                                                   "distance", "prize", "class",
                                                   "track-condition", "rail"]):
            text = el.get_text(strip=True)
            if text and 2 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "racing_australia",
                    "type": "race_condition",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse distance (metres)
                dist_match = re.search(r'(\d{3,5})\s*m(?:etres?)?', text, re.I)
                if dist_match:
                    record["distance_m"] = dist_match.group(1)
                # Parse track condition
                track_match = re.search(
                    r'(firm|good|soft|heavy|synthetic|dead)\s*(\d)?',
                    text, re.I
                )
                if track_match:
                    record["track_condition"] = track_match.group(0).strip()
                records.append(record)
    return records


def extract_results_data(soup, date_str, race_url=""):
    """Extract finishing positions and margins from results pages."""
    records = []
    for el in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "finish", "placing",
                                                   "winner", "dividend",
                                                   "returns", "payout"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "racing_australia",
                    "type": "result_block",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_form_data(soup, date_str):
    """Extract form/history data for horses."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "history", "career",
                                                   "record", "stats",
                                                   "performance", "last-starts"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "racing_australia",
                    "type": "form_data",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_day_fields(page, date_str):
    """Scrape Racing Australia fields page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"fields_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/FreeFields/Field.aspx?Key={date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"fields_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "racing_australia", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "racing_australia", date_str=date_str))
    records.extend(extract_race_conditions(soup, date_str))
    records.extend(extract_runners_table(soup, "racing_australia", date_str=date_str))
    records.extend(extract_form_data(soup, date_str))

    meeting_links = extract_meeting_links(soup, date_str)

    # Extract venue/meeting blocks
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "venue", "card",
                                                   "race-list", "raceday"]):
            record = {
                "date": date_str,
                "source": "racing_australia",
                "type": "meeting",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong", "a"])
            if title:
                record["venue"] = title.get_text(strip=True)
            # State info
            state_match = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b',
                                    div.get_text(strip=True))
            if state_match:
                record["state"] = state_match.group(1)
            records.append(record)

    result = {"records": records, "meeting_links": meeting_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_meeting_detail(page, meeting_url, date_str):
    """Scrape individual meeting/race page for detailed runner data."""
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

    # Race title
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Race conditions
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(r'(\d{3,5})\s*m(?:etres?)?', page_text, re.I)
    if dist_match:
        conditions["distance_m"] = dist_match.group(1)

    track_match = re.search(
        r'track\s*(?:condition|rating)?\s*:?\s*(firm|good|soft|heavy|synthetic|dead)\s*(\d)?',
        page_text, re.I
    )
    if track_match:
        conditions["track_condition"] = track_match.group(0).strip()

    class_match = re.search(r'(group\s*[123]|listed|benchmark\s*\d+|maiden|class\s*\d)',
                            page_text, re.I)
    if class_match:
        conditions["race_class"] = class_match.group(1)

    prize_match = re.search(r'\$\s*([\d,]+)', page_text)
    if prize_match:
        conditions["prize_money"] = prize_match.group(1)

    records.extend(extract_embedded_json_data(soup, "racing_australia", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "racing_australia", date_str=date_str))
    records.extend(extract_race_conditions(soup, date_str))
    records.extend(extract_results_data(soup, date_str, race_url=meeting_url))

    # Runners table
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
                "source": "racing_australia",
                "type": "runner_detail",
                "race_name": race_name,
                "conditions": conditions,
                "url": meeting_url,
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

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results_day(page, date_str):
    """Scrape results page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/FreeFields/Results.aspx?Key={date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "racing_australia", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "racing_australia", date_str=date_str))
    records.extend(extract_runners_table(soup, "racing_australia", date_str=date_str, race_url=url))
    records.extend(extract_results_data(soup, date_str, race_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 109 — Racing Australia Scraper (AU thoroughbred racing)"
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
    log.info("SCRIPT 109 — Racing Australia Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "racing_australia_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-AU", timezone="Australia/Sydney"
        )
        log.info("Browser launched (headless Chromium, locale=en-AU)")

        first_nav = True
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")

            # Scrape fields index
            result = scrape_day_fields(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual meeting pages (limit to avoid overload)
                for meeting_url in result.get("meeting_links", [])[:15]:
                    detail = scrape_meeting_detail(page, meeting_url, date_str)
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
