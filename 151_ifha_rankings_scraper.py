#!/usr/bin/env python3
"""
Script 151 — IFHA World Rankings Scraper (Playwright)
Source : ifhaonline.org/racing/WorldRankings
Collecte : World horse rankings (free/public data)
URL patterns :
  /racing/WorldRankings                     -> rankings overview
  /racing/WorldRankings/Rankings?...        -> filtered rankings by year/category
  /racing/WorldRankings/RankingDetail/...   -> horse detail page
CRITIQUE pour : International Rankings, Performance Benchmarks

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

SCRIPT_NAME = "151_ifha_rankings"
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

log = setup_logging("151_ifha_rankings")

BASE_URL = "https://www.ifhaonline.org"
RANKINGS_URL = f"{BASE_URL}/racing/WorldRankings"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Categories to scrape
RANKING_CATEGORIES = [
    "Flat",
    "Turf",
    "Dirt",
    "Sprint",
    "Mile",
    "Intermediate",
    "Long",
    "Extended",
]

# Year range to scrape
DEFAULT_START_YEAR = 2018


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
            time.sleep(2.0)  # IFHA can be slow to render
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

def extract_ranking_table(soup, year, category):
    """Extract ranking data from a table on the rankings page."""
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
                "source": "ifha",
                "type": "world_ranking",
                "year": year,
                "category": category,
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

            # Try to extract horse name, ranking, rating from cells
            if len(cells) >= 3:
                # Common patterns: Rank, Horse, Country, Rating, ...
                if cells[0].isdigit():
                    record["rank"] = cells[0]
                if len(cells) > 1 and not cells[1].isdigit():
                    record["horse_name"] = cells[1]

            records.append(record)
    return records


def extract_ranking_cards(soup, year, category):
    """Extract ranking data from card/div-based layouts."""
    records = []
    for card in soup.find_all(["div", "article", "li"], class_=True):
        classes = " ".join(card.get("class", []))
        if not any(kw in classes.lower() for kw in ["rank", "horse", "entry",
                                                       "result", "item",
                                                       "listing", "row"]):
            continue

        text = card.get_text(strip=True)
        if len(text) < 5 or len(text) > 2000:
            continue

        record = {
            "source": "ifha",
            "type": "world_ranking_card",
            "year": year,
            "category": category,
            "contenu": text[:1500],
            "classes_css": classes,
            "scraped_at": datetime.now().isoformat(),
        }

        # Extract specific elements
        name_el = card.find(["h3", "h4", "a", "strong"], class_=lambda c: c and
                            any(kw in (c if isinstance(c, str) else " ".join(c)).lower()
                                for kw in ["name", "horse", "title"]))
        if name_el:
            record["horse_name"] = name_el.get_text(strip=True)

        # Rating
        rating_el = card.find(["span", "div", "td"], class_=lambda c: c and
                              any(kw in (c if isinstance(c, str) else " ".join(c)).lower()
                                  for kw in ["rating", "score", "points"]))
        if rating_el:
            record["rating"] = rating_el.get_text(strip=True)

        # Country
        country_el = card.find(["span", "img", "abbr"], class_=lambda c: c and
                               any(kw in (c if isinstance(c, str) else " ".join(c)).lower()
                                   for kw in ["country", "flag", "nation"]))
        if country_el:
            record["country"] = country_el.get("title", "") or country_el.get_text(strip=True)

        # Rank number
        rank_match = re.search(r'^\s*(\d{1,4})\s', text)
        if rank_match:
            record["rank"] = rank_match.group(1)

        if record.get("horse_name") or record.get("rank"):
            records.append(record)

    return records


def extract_detail_links(soup):
    """Extract links to horse detail pages."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(RankingDetail|HorseDetail|horse)', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_embedded_json(soup, year, category):
    """Extract embedded JSON data."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, (dict, list)):
                records.append({
                    "source": "ifha",
                    "type": "embedded_json",
                    "year": year,
                    "category": category,
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_rankings_page(page, year, category):
    """Scrape a specific rankings page for a year/category combination."""
    cache_key = f"rankings_{year}_{category.lower()}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # Build URL with parameters
    url = f"{RANKINGS_URL}?year={year}&category={category}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"{cache_key}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, year, category))
    records.extend(extract_ranking_table(soup, year, category))
    records.extend(extract_ranking_cards(soup, year, category))

    detail_links = extract_detail_links(soup)

    result = {"records": records, "detail_links": detail_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def scrape_horse_detail(page, detail_url, year, category):
    """Scrape a horse detail page from IFHA."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', detail_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"horse_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, detail_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Horse title
    horse_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 2:
            horse_name = text
            break

    # Basic info sections
    for section in soup.find_all(["div", "section", "dl"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["detail", "info", "profile",
                                                   "bio", "stats", "career",
                                                   "performance", "pedigree"]):
            text = section.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "source": "ifha",
                    "type": "horse_detail",
                    "horse_name": horse_name,
                    "year": year,
                    "category": category,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": detail_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Tables on detail page (race history, etc.)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "source": "ifha",
                "type": "horse_race_history",
                "horse_name": horse_name,
                "year": year,
                "url": detail_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    records.extend(extract_embedded_json(soup, year, category))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 151 — IFHA World Rankings Scraper (international horse rankings)"
    )
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR,
                        help="Start year (default=%d)" % DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=None,
                        help="End year (default=current year)")
    parser.add_argument("--categories", type=str, nargs="*", default=None,
                        help="Categories to scrape (default=all)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-details", type=int, default=10,
                        help="Max horse detail pages per category (default=10)")
    args = parser.parse_args()

    start_year = args.start_year
    end_year = args.end_year or datetime.now().year
    categories = args.categories or RANKING_CATEGORIES

    log.info("=" * 60)
    log.info("SCRIPT 151 — IFHA World Rankings Scraper (Playwright)")
    log.info("  Years: %d -> %d", start_year, end_year)
    log.info("  Categories: %s", categories)
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    processed_keys = set(checkpoint.get("processed_keys", []))
    if args.resume:
        log.info("  Already processed: %d year/category combos", len(processed_keys))

    output_file = os.path.join(OUTPUT_DIR, "ifha_rankings.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US)")

        first_nav = True
        total_records = 0
        combo_count = 0

        for year in range(start_year, end_year + 1):
            for category in categories:
                combo_key = f"{year}_{category}"
                if combo_key in processed_keys:
                    log.info("  Skipping already processed: %s", combo_key)
                    continue

                log.info("  Scraping: year=%d category=%s", year, category)
                result = scrape_rankings_page(page, year, category)

                if first_nav and result is not None:
                    accept_cookies(page)
                    first_nav = False

                if result:
                    records = result.get("records", [])

                    # Scrape horse detail pages (limited)
                    for detail_url in result.get("detail_links", [])[:args.max_details]:
                        detail = scrape_horse_detail(page, detail_url, year, category)
                        if detail:
                            records.extend(detail)
                        smart_pause(2.0, 1.0)

                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1

                processed_keys.add(combo_key)
                combo_count += 1

                if combo_count % 5 == 0:
                    log.info("  Processed %d combos, %d records total",
                             combo_count, total_records)
                    save_checkpoint(CHECKPOINT_FILE, {
                        "processed_keys": sorted(processed_keys),
                        "total_records": total_records,
                    })

                smart_pause(2.0, 1.0)

        save_checkpoint(CHECKPOINT_FILE, {
            "processed_keys": sorted(processed_keys),
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: %d year/category combos, %d records -> %s",
                 combo_count, total_records, output_file)
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
