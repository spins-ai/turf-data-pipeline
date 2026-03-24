#!/usr/bin/env python3
"""
Script 117 — Jockey Planet Scraper (Playwright)
Source : jockeyplanet.com
Collecte : Jockey statistics, win rates, recent form, career records
URL patterns :
  /jockeys/              -> jockey directory
  /jockey/{name}/        -> jockey profile page
  /jockey/{name}/stats/  -> detailed statistics
  /rankings/             -> jockey rankings
CRITIQUE pour : Jockey Win Rates, Strike Rates, Recent Form, Course Preferences

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

SCRIPT_NAME = "117_jockey_planet"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("117_jockey_planet")

BASE_URL = "https://www.jockeyplanet.com"
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

def extract_jockey_links(soup):
    """Extract links to individual jockey profile pages."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/jockey/', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_jockey_stats(soup, date_str):
    """Extract jockey statistics (win rate, strike rate, career records)."""
    records = []
    for el in soup.find_all(["div", "span", "td", "section", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stat", "win", "rate", "record",
                                                   "performance", "strike",
                                                   "percentage", "career"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "jockeyplanet",
                    "type": "jockey_stat",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract percentage
                pct_match = re.search(r'(\d{1,3}(?:\.\d{1,2})?)\s*%', text)
                if pct_match:
                    record["win_rate_pct"] = pct_match.group(1)
                # Try to extract win count
                win_match = re.search(r'(\d+)\s*(?:wins?|victories)', text, re.I)
                if win_match:
                    record["wins"] = win_match.group(1)
                records.append(record)
    return records


def extract_recent_form(soup, date_str):
    """Extract jockey recent form data (last rides, results)."""
    records = []
    for el in soup.find_all(["div", "section", "article", "ul", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "recent", "result",
                                                   "ride", "last", "history",
                                                   "run", "performance"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "jockeyplanet",
                    "type": "recent_form",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_rankings_table(soup, date_str):
    """Extract jockey rankings from tables."""
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
                "source": "jockeyplanet",
                "type": "jockey_ranking",
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


def extract_embedded_json_data(soup, date_str):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "date": date_str,
                    "source": "jockeyplanet",
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
                    "source": "jockeyplanet",
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_data_attributes(soup, date_str):
    """Extract data-* attributes related to jockeys/racing."""
    records = []
    keywords = ["jockey", "rider", "win", "stat", "rank", "rate",
                "form", "result", "horse", "race"]
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in keywords)
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "jockeyplanet",
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

def scrape_jockey_directory(page, date_str):
    """Scrape jockey directory page for profile links and overview data."""
    cache_file = os.path.join(CACHE_DIR, f"directory_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/jockeys/"
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
    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))
    records.extend(extract_rankings_table(soup, date_str))
    records.extend(extract_jockey_stats(soup, date_str))

    # Extract jockey links
    jockey_links = extract_jockey_links(soup)

    result = {"records": records, "jockey_links": jockey_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_jockey_profile(page, profile_url, date_str):
    """Scrape individual jockey profile page for detailed stats."""
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

    # Jockey name
    jockey_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 2:
            jockey_name = text
            break

    # Extract all data types
    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))
    records.extend(extract_jockey_stats(soup, date_str))
    records.extend(extract_recent_form(soup, date_str))
    records.extend(extract_rankings_table(soup, date_str))

    # Tag all records with jockey name and URL
    for rec in records:
        rec["jockey_name"] = jockey_name
        rec["url"] = profile_url

    # Bio / profile sections
    for div in soup.find_all(["div", "p", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["bio", "profile", "about",
                                                   "summary", "overview",
                                                   "info", "detail"]):
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "jockeyplanet",
                    "type": "jockey_bio",
                    "jockey_name": jockey_name,
                    "contenu": text[:2000],
                    "url": profile_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_rankings_page(page, date_str):
    """Scrape the jockey rankings page."""
    cache_file = os.path.join(CACHE_DIR, f"rankings_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/rankings/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))
    records.extend(extract_rankings_table(soup, date_str))
    records.extend(extract_jockey_stats(soup, date_str))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 117 — Jockey Planet Scraper (jockey statistics, win rates, form)"
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
    log.info("SCRIPT 117 — Jockey Planet Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "jockey_planet_data.jsonl")

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

            # Scrape jockey directory
            result = scrape_jockey_directory(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual jockey profiles (limit to avoid overload)
                for profile_url in result.get("jockey_links", [])[:15]:
                    detail = scrape_jockey_profile(page, profile_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Also scrape rankings page
            rankings_data = scrape_rankings_page(page, date_str)
            if rankings_data:
                for rec in rankings_data:
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
