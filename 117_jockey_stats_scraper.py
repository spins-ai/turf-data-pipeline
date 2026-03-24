#!/usr/bin/env python3
"""
Script 117 — Jockey Stats Scraper (Playwright)
Sources : attheraces.com, sportinglife.com
Collecte : Detailed jockey statistics — win rate by course type, distance,
           going, trainer combos, seasonal trends, prize money
URL patterns :
  attheraces.com/jockeys/{name}                -> fiche jockey
  attheraces.com/jockeys                       -> liste jockeys
  sportinglife.com/racing/jockey/{id}/{name}   -> fiche jockey
  sportinglife.com/racing/jockeys              -> liste jockeys
CRITIQUE pour : Jockey Win Rates, Course/Distance/Going Analysis,
                Jockey-Trainer Combos, Seasonal Form

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
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "117_jockey_stats"
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

log = setup_logging("117_jockey_stats")

ATR_BASE = "https://www.attheraces.com"
SL_BASE = "https://www.sportinglife.com"
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
# Extraction helpers — At The Races
# ------------------------------------------------------------------

def extract_atr_jockey_links(soup):
    """Extract jockey profile links from ATR jockeys index page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/jockey[s]?/[^/?#]+', href, re.I):
            full_url = href if href.startswith("http") else f"{ATR_BASE}{href}"
            # Skip the bare /jockeys index
            if not re.search(r'/jockeys?/?$', full_url):
                links.add(full_url)
    return sorted(links)


def extract_atr_jockey_stats(soup, jockey_url=""):
    """Extract detailed jockey stats from an ATR jockey profile page."""
    records = []

    # Extract jockey name from page heading
    jockey_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and 2 < len(text) < 100:
            jockey_name = text
            break

    # Stats tables (win rates, course stats, distance stats, going stats)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        # Detect table type from headers
        table_type = "general_stats"
        header_text = " ".join(headers)
        if any(kw in header_text for kw in ["course", "track", "racecourse"]):
            table_type = "course_stats"
        elif any(kw in header_text for kw in ["distance", "furlong", "mile"]):
            table_type = "distance_stats"
        elif any(kw in header_text for kw in ["going", "ground", "surface"]):
            table_type = "going_stats"
        elif any(kw in header_text for kw in ["trainer", "partnership"]):
            table_type = "trainer_combo_stats"
        elif any(kw in header_text for kw in ["month", "season", "year"]):
            table_type = "seasonal_stats"

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "source": "attheraces",
                "type": table_type,
                "jockey_name": jockey_name,
                "url": jockey_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Try to extract win rate
            for cell in cells:
                rate_match = re.search(r'(\d{1,3}(?:\.\d{1,2})?)%', cell)
                if rate_match:
                    record["win_rate_pct"] = rate_match.group(1)
                    break

            # Extract data-* attributes from row
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)

    # Stats blocks (non-table structured content)
    for el in soup.find_all(["div", "section", "dl", "ul"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stat", "record", "performance",
                                                   "career", "form", "summary",
                                                   "profile-stat", "jockey-stat"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 3000:
                record = {
                    "source": "attheraces",
                    "type": "stats_block",
                    "jockey_name": jockey_name,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": jockey_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)

    return records


# ------------------------------------------------------------------
# Extraction helpers — Sporting Life
# ------------------------------------------------------------------

def extract_sl_jockey_links(soup):
    """Extract jockey profile links from Sporting Life jockeys page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/racing/jockey/\d+/', href, re.I):
            full_url = href if href.startswith("http") else f"{SL_BASE}{href}"
            links.add(full_url)
    return sorted(links)


def extract_sl_jockey_stats(soup, jockey_url=""):
    """Extract jockey stats from a Sporting Life jockey profile page."""
    records = []

    # Jockey name
    jockey_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and 2 < len(text) < 100:
            jockey_name = text
            break

    # Stats tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        # Detect table type
        table_type = "general_stats"
        header_text = " ".join(headers)
        if any(kw in header_text for kw in ["course", "track"]):
            table_type = "course_stats"
        elif any(kw in header_text for kw in ["distance", "furlong"]):
            table_type = "distance_stats"
        elif any(kw in header_text for kw in ["going", "ground"]):
            table_type = "going_stats"
        elif any(kw in header_text for kw in ["trainer"]):
            table_type = "trainer_combo_stats"
        elif any(kw in header_text for kw in ["class", "grade"]):
            table_type = "class_stats"

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "source": "sportinglife",
                "type": table_type,
                "jockey_name": jockey_name,
                "url": jockey_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extract win rate
            for cell in cells:
                rate_match = re.search(r'(\d{1,3}(?:\.\d{1,2})?)%', cell)
                if rate_match:
                    record["win_rate_pct"] = rate_match.group(1)
                    break

            # Runs-wins pattern e.g. "12-3" or "3/15"
            for cell in cells:
                rw_match = re.search(r'(\d+)\s*[-/]\s*(\d+)', cell)
                if rw_match:
                    record["wins"] = rw_match.group(1)
                    record["runs"] = rw_match.group(2)
                    break

            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)

    # Stats cards / summary panels
    for el in soup.find_all(["div", "section", "dl", "ul", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stat", "record", "performance",
                                                   "career", "summary", "profile",
                                                   "info-block", "key-stat"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 3000:
                record = {
                    "source": "sportinglife",
                    "type": "stats_block",
                    "jockey_name": jockey_name,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": jockey_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)

    return records


# ------------------------------------------------------------------
# Embedded JSON / data-attribute extractors
# ------------------------------------------------------------------

def extract_embedded_json_data(soup, source):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "source": source,
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
                    "source": source,
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_data_attributes(soup, source):
    """Extract data-* attributes related to jockeys/racing."""
    records = []
    keywords = ["jockey", "trainer", "horse", "runner", "stat", "win",
                "rate", "record", "form", "result", "ride", "mount"]
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in keywords)
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": source,
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

def scrape_atr_jockey_index(page):
    """Scrape ATR jockeys index page for jockey profile links."""
    cache_file = os.path.join(CACHE_DIR, "atr_jockey_index.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{ATR_BASE}/jockeys"
    html = navigate_with_retry(page, url)
    if not html:
        # Try alternative URL pattern
        url = f"{ATR_BASE}/racing/jockeys"
        html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, "atr_jockey_index.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    jockey_links = extract_atr_jockey_links(soup)

    records = []
    records.extend(extract_embedded_json_data(soup, "attheraces"))
    records.extend(extract_data_attributes(soup, "attheraces"))

    # Extract jockey listing items
    for el in soup.find_all(["div", "li", "tr", "article", "a"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["jockey", "rider", "person",
                                                   "athlete", "listing",
                                                   "card", "item"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 1000:
                record = {
                    "source": "attheraces",
                    "type": "jockey_listing",
                    "contenu": text[:500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)

    result = {"records": records, "jockey_links": jockey_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_atr_jockey_profile(page, jockey_url):
    """Scrape an individual ATR jockey profile for detailed stats."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', jockey_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"atr_jockey_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, jockey_url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"atr_jockey_{url_hash}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_atr_jockey_stats(soup, jockey_url))
    records.extend(extract_embedded_json_data(soup, "attheraces"))
    records.extend(extract_data_attributes(soup, "attheraces"))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_sl_jockey_index(page):
    """Scrape Sporting Life jockeys index for profile links."""
    cache_file = os.path.join(CACHE_DIR, "sl_jockey_index.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{SL_BASE}/racing/jockeys"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, "sl_jockey_index.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    jockey_links = extract_sl_jockey_links(soup)

    records = []
    records.extend(extract_embedded_json_data(soup, "sportinglife"))
    records.extend(extract_data_attributes(soup, "sportinglife"))

    result = {"records": records, "jockey_links": jockey_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_sl_jockey_profile(page, jockey_url):
    """Scrape an individual Sporting Life jockey profile page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', jockey_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"sl_jockey_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, jockey_url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"sl_jockey_{url_hash}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_sl_jockey_stats(soup, jockey_url))
    records.extend(extract_embedded_json_data(soup, "sportinglife"))
    records.extend(extract_data_attributes(soup, "sportinglife"))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 117 — Jockey Stats Scraper (win rates by course, distance, going, trainer combos)"
    )
    parser.add_argument("--source", type=str, default="all",
                        choices=["all", "atr", "sportinglife"],
                        help="Which source to scrape (default=all)")
    parser.add_argument("--max-jockeys", type=int, default=0,
                        help="Max jockeys to scrape per source (0=unlimited)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 117 — Jockey Stats Scraper (Playwright)")
    log.info("  Sources: %s", args.source)
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    output_file = os.path.join(OUTPUT_DIR, "jockey_stats.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-GB", timezone="Europe/London"
        )
        log.info("Browser launched (headless Chromium, locale=en-GB)")

        total_records = 0
        first_nav = True

        # ----------------------------------------------------------
        # At The Races
        # ----------------------------------------------------------
        if args.source in ("all", "atr"):
            log.info("-" * 40)
            log.info("Source: At The Races")
            log.info("-" * 40)

            atr_done = set(checkpoint.get("atr_done", []))

            index_result = scrape_atr_jockey_index(page)
            if first_nav and index_result is not None:
                accept_cookies(page)
                first_nav = False

            if index_result:
                jockey_links = index_result.get("jockey_links", [])
                log.info("  Found %d jockey profiles on ATR", len(jockey_links))

                for rec in index_result.get("records", []):
                    append_jsonl(output_file, rec)
                    total_records += 1

                scraped_count = 0
                for jurl in jockey_links:
                    if jurl in atr_done:
                        continue
                    if args.max_jockeys and scraped_count >= args.max_jockeys:
                        break

                    detail = scrape_atr_jockey_profile(page, jurl)
                    if detail:
                        for rec in detail:
                            append_jsonl(output_file, rec)
                            total_records += 1

                    atr_done.add(jurl)
                    scraped_count += 1

                    if scraped_count % 10 == 0:
                        log.info("  ATR jockeys=%d records=%d", scraped_count, total_records)
                        save_checkpoint(CHECKPOINT_FILE, {
                            "atr_done": list(atr_done),
                            "total_records": total_records,
                        })

                    smart_pause(2.0, 1.0)

                log.info("  ATR done: %d jockeys scraped", scraped_count)

        # ----------------------------------------------------------
        # Sporting Life
        # ----------------------------------------------------------
        if args.source in ("all", "sportinglife"):
            log.info("-" * 40)
            log.info("Source: Sporting Life")
            log.info("-" * 40)

            sl_done = set(checkpoint.get("sl_done", []))

            index_result = scrape_sl_jockey_index(page)
            if first_nav and index_result is not None:
                accept_cookies(page)
                first_nav = False

            if index_result:
                jockey_links = index_result.get("jockey_links", [])
                log.info("  Found %d jockey profiles on Sporting Life", len(jockey_links))

                for rec in index_result.get("records", []):
                    append_jsonl(output_file, rec)
                    total_records += 1

                scraped_count = 0
                for jurl in jockey_links:
                    if jurl in sl_done:
                        continue
                    if args.max_jockeys and scraped_count >= args.max_jockeys:
                        break

                    detail = scrape_sl_jockey_profile(page, jurl)
                    if detail:
                        for rec in detail:
                            append_jsonl(output_file, rec)
                            total_records += 1

                    sl_done.add(jurl)
                    scraped_count += 1

                    if scraped_count % 10 == 0:
                        log.info("  SL jockeys=%d records=%d", scraped_count, total_records)
                        save_checkpoint(CHECKPOINT_FILE, {
                            "sl_done": list(sl_done),
                            "total_records": total_records,
                        })

                    smart_pause(2.0, 1.0)

                log.info("  Sporting Life done: %d jockeys scraped", scraped_count)

        save_checkpoint(CHECKPOINT_FILE, {
            "atr_done": list(checkpoint.get("atr_done", [])),
            "sl_done": list(checkpoint.get("sl_done", [])),
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
