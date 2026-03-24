#!/usr/bin/env python3
"""
Script 135 — International Trot Scraper (Playwright)
Sources :
  - tfrenchtrotter.com  -> French trotter pedigrees, results, stats
  - trotting.com         -> International trot results, statistics
Collecte : international trot results, statistics, mile rates, pedigree data
URL patterns :
  tfrenchtrotter.com/en/horse/{name}       -> horse profile & stats
  tfrenchtrotter.com/en/results/{date}     -> daily results
  trotting.com/results/                    -> results by date/track
  trotting.com/statistics/                 -> driver/trainer stats
CRITIQUE pour : International Trot Results, Mile Rates, Trot Statistics

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

SCRIPT_NAME = "135_trot_international"
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

log = setup_logging("135_trot_international")

# Source URLs
TFRENCH_BASE = "https://www.tfrenchtrotter.com"
TROTTING_BASE = "https://www.trotting.com"

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
# Extraction helpers — tfrenchtrotter.com
# ------------------------------------------------------------------

def extract_tfrench_results(soup, date_str, source_url=""):
    """Extract race results from tfrenchtrotter.com pages."""
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
                "source": "tfrenchtrotter",
                "type": "result",
                "url": source_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            # Extract mile rate from cells
            for cell in cells:
                mile_match = re.search(r"(\d)[:\'](\d{2})[\".](\d)", cell)
                if mile_match:
                    record["mile_rate_raw"] = cell.strip()
                    mins = int(mile_match.group(1))
                    secs = int(mile_match.group(2))
                    tenths = int(mile_match.group(3))
                    record["mile_rate_seconds"] = mins * 60 + secs + tenths / 10.0

            # Data attributes
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val
            records.append(record)
    return records


def extract_tfrench_horse_profile(soup, date_str, horse_url=""):
    """Extract horse profile data from tfrenchtrotter horse pages."""
    records = []
    # Horse name from heading
    horse_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 1:
            horse_name = text
            break

    # Profile info blocks
    for el in soup.find_all(["div", "section", "dl", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in [
            "profile", "info", "detail", "pedigree", "stats",
            "horse-info", "horse-detail", "career", "record",
        ]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 5000:
                record = {
                    "date": date_str,
                    "source": "tfrenchtrotter",
                    "type": "horse_profile",
                    "horse_name": horse_name[:200],
                    "contenu": text[:3000],
                    "classes_css": classes,
                    "url": horse_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse pedigree elements
                sire_match = re.search(r'(?:sire|pere|father)\s*:?\s*([A-Za-z\s\'\-]+)', text, re.I)
                if sire_match:
                    record["sire"] = sire_match.group(1).strip()[:100]
                dam_match = re.search(r'(?:dam|mere|mother)\s*:?\s*([A-Za-z\s\'\-]+)', text, re.I)
                if dam_match:
                    record["dam"] = dam_match.group(1).strip()[:100]

                records.append(record)

    # Career statistics
    for el in soup.find_all(["div", "span", "p", "td"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in [
            "earnings", "gains", "prize", "record", "stat",
        ]):
            if text and 2 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "tfrenchtrotter",
                    "type": "career_stat",
                    "horse_name": horse_name[:200],
                    "contenu": text[:300],
                    "classes_css": classes,
                    "url": horse_url,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_tfrench_race_links(soup):
    """Extract links to individual races from tfrenchtrotter results page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(race|result|course)/', href, re.I):
            full_url = href if href.startswith("http") else f"{TFRENCH_BASE}{href}"
            links.add(full_url)
    return sorted(links)


def extract_tfrench_horse_links(soup):
    """Extract horse profile links from a results page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/horse/', href, re.I):
            full_url = href if href.startswith("http") else f"{TFRENCH_BASE}{href}"
            links.add(full_url)
    return sorted(links)


# ------------------------------------------------------------------
# Extraction helpers — trotting.com
# ------------------------------------------------------------------

def extract_trotting_results(soup, date_str, source_url=""):
    """Extract race results from trotting.com pages."""
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
                "source": "trotting_com",
                "type": "result",
                "url": source_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            # Extract mile rate
            for cell in cells:
                mile_match = re.search(r"(\d)[:\'](\d{2})[\".](\d)", cell)
                if mile_match:
                    record["mile_rate_raw"] = cell.strip()
                    mins = int(mile_match.group(1))
                    secs = int(mile_match.group(2))
                    tenths = int(mile_match.group(3))
                    record["mile_rate_seconds"] = mins * 60 + secs + tenths / 10.0

            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val
            records.append(record)
    return records


def extract_trotting_statistics(soup, date_str, source_url=""):
    """Extract driver/trainer statistics from trotting.com."""
    records = []
    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in [
            "stats", "statistic", "ranking", "leaderboard",
            "driver", "trainer", "top", "leader",
        ]):
            # Try table extraction first
            for table in section.find_all("table"):
                rows = table.find_all("tr")
                headers = []
                if rows:
                    headers = [th.get_text(strip=True).lower().replace(" ", "_")
                               for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if not cells or len(cells) < 2:
                        continue
                    record = {
                        "date": date_str,
                        "source": "trotting_com",
                        "type": "statistic",
                        "url": source_url,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    for j, cell in enumerate(cells):
                        key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                        record[key] = cell
                    records.append(record)

            # Fallback: text extraction
            if not records:
                text = section.get_text(strip=True)
                if text and 10 < len(text) < 5000:
                    records.append({
                        "date": date_str,
                        "source": "trotting_com",
                        "type": "statistic_block",
                        "contenu": text[:3000],
                        "classes_css": classes,
                        "url": source_url,
                        "scraped_at": datetime.now().isoformat(),
                    })
    return records


def extract_mile_rates(soup, date_str, source_url=""):
    """Extract mile rate / time data from any page."""
    records = []
    page_text = soup.get_text()

    # Look for mile rate patterns: 1:55.3, 1'56"2, 1.57.4, etc.
    mile_patterns = [
        r"(\d)[:\'](\d{2})[\".](\d)",   # 1:55.3 or 1'55"3
        r"(\d)\.(\d{2})\.(\d)",           # 1.55.3
    ]
    for pattern in mile_patterns:
        for match in re.finditer(pattern, page_text):
            mins = int(match.group(1))
            secs = int(match.group(2))
            tenths = int(match.group(3))
            total_secs = mins * 60 + secs + tenths / 10.0
            # Only include plausible trot mile rates (1:50-2:20 range)
            if 110.0 <= total_secs <= 140.0:
                records.append({
                    "date": date_str,
                    "source": "trot_international",
                    "type": "mile_rate",
                    "mile_rate_raw": match.group(0),
                    "mile_rate_seconds": total_secs,
                    "url": source_url,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_embedded_json(soup, date_str, source="trot_international"):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, (dict, list)):
                records.append({
                    "date": date_str,
                    "source": source,
                    "type": "embedded_json",
                    "data_id": script.get("id", ""),
                    "data": data if isinstance(data, dict) else {"items": data},
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
                    "source": source,
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


# ------------------------------------------------------------------
# Main scraping functions — tfrenchtrotter.com
# ------------------------------------------------------------------

def scrape_tfrench_results_day(page, date_str):
    """Scrape tfrenchtrotter results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"tfrench_results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{TFRENCH_BASE}/en/results/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"tfrench_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, source="tfrenchtrotter"))
    records.extend(extract_tfrench_results(soup, date_str, source_url=url))
    records.extend(extract_mile_rates(soup, date_str, source_url=url))

    race_links = extract_tfrench_race_links(soup)
    horse_links = extract_tfrench_horse_links(soup)

    result = {
        "records": records,
        "race_links": race_links,
        "horse_links": horse_links,
    }
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_tfrench_horse(page, horse_url, date_str):
    """Scrape a horse profile page on tfrenchtrotter."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', horse_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"horse_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, horse_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, source="tfrenchtrotter"))
    records.extend(extract_tfrench_horse_profile(soup, date_str, horse_url=horse_url))
    records.extend(extract_mile_rates(soup, date_str, source_url=horse_url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main scraping functions — trotting.com
# ------------------------------------------------------------------

def scrape_trotting_results_day(page, date_str):
    """Scrape trotting.com results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"trotting_results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{TROTTING_BASE}/results/?date={date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"trotting_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, source="trotting_com"))
    records.extend(extract_trotting_results(soup, date_str, source_url=url))
    records.extend(extract_mile_rates(soup, date_str, source_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_trotting_statistics_page(page, date_str):
    """Scrape trotting.com statistics page."""
    cache_file = os.path.join(CACHE_DIR, f"trotting_stats_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{TROTTING_BASE}/statistics/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, source="trotting_com"))
    records.extend(extract_trotting_statistics(soup, date_str, source_url=url))
    records.extend(extract_mile_rates(soup, date_str, source_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 135 — International Trot Scraper (tfrenchtrotter + trotting.com)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=yesterday")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--max-horses", type=int, default=20,
                        help="Max horse profiles to scrape per day (default=20)")
    parser.add_argument("--skip-trotting", action="store_true", default=False,
                        help="Skip trotting.com (only scrape tfrenchtrotter)")
    parser.add_argument("--skip-tfrench", action="store_true", default=False,
                        help="Skip tfrenchtrotter (only scrape trotting.com)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
                else datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 135 — International Trot Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("  Sources: tfrenchtrotter=%s, trotting.com=%s",
             not args.skip_tfrench, not args.skip_trotting)
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date <= end_date:
            start_date = resume_date
            log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "trot_international_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US)")

        first_nav = True
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")
            log.info("Scraping %s ...", date_str)

            # ---- tfrenchtrotter.com ----
            if not args.skip_tfrench:
                tfrench_result = scrape_tfrench_results_day(page, date_str)

                if first_nav and tfrench_result is not None:
                    accept_cookies(page)
                    first_nav = False

                if tfrench_result:
                    records = tfrench_result.get("records", [])

                    # Scrape horse profiles (limited)
                    horse_links = tfrench_result.get("horse_links", [])[:args.max_horses]
                    for horse_url in horse_links:
                        horse_data = scrape_tfrench_horse(page, horse_url, date_str)
                        if horse_data:
                            records.extend(
                                horse_data if isinstance(horse_data, list) else []
                            )
                        smart_pause(1.5, 0.8)

                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1

                smart_pause(2.0, 1.0)

            # ---- trotting.com ----
            if not args.skip_trotting:
                if first_nav:
                    # Accept cookies on first trotting.com nav
                    trotting_result = scrape_trotting_results_day(page, date_str)
                    if trotting_result is not None:
                        accept_cookies(page)
                        first_nav = False
                else:
                    trotting_result = scrape_trotting_results_day(page, date_str)

                if trotting_result:
                    for rec in (trotting_result if isinstance(trotting_result, list)
                                else trotting_result.get("records", [])
                                if isinstance(trotting_result, dict) else []):
                        append_jsonl(output_file, rec)
                        total_records += 1

                # Statistics (once per session, not per day)
                if day_count == 0:
                    stats_data = scrape_trotting_statistics_page(page, date_str)
                    if stats_data:
                        for rec in (stats_data if isinstance(stats_data, list) else []):
                            append_jsonl(output_file, rec)
                            total_records += 1

                smart_pause(2.0, 1.0)

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
