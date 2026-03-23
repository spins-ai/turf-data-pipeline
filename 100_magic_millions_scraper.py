#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 100 -- Scraping Magic Millions (Playwright version)
Source : magicmillions.com.au - Magic Millions AU horse sales
Collecte : lots vendus, prix, pedigrees, acheteurs, vendeurs, catalogues
CRITIQUE pour : Sales Model, Pedigree Valuation, Investment Analysis

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import os
import random
import sys
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "100_magic_millions"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.html_parsing import extract_embedded_json, extract_data_attributes

log = setup_logging("100_magic_millions")

MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

BASE_URL = "https://www.magicmillions.com.au"

# Known sale categories
SALE_TYPES = [
    "gold-coast-yearling-sale",
    "gold-coast-march-yearling-sale",
    "national-yearling-sale",
    "national-broodmare-sale",
    "national-weanling-sale",
    "gold-coast-broodmare-sale",
    "adelaide-yearling-sale",
    "perth-yearling-sale",
    "tasmanian-yearling-sale",
    "national-racehorse-sale",
    "2yo-classic-sale",
]

# Year range for historical sales
SALE_YEARS = list(range(2015, 2027))


# NOTE: Local version kept because it returns HTML string (page.content()) instead of bool
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


def scrape_sale_results_page(page, page_url, sale_name, year):
    """Scrape sale results page (catalogue/results listing)."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', page_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"sale_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    html = navigate_with_retry(page, page_url)
    if not html:
        return []

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"sale_{url_hash}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # -- Results tables --
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
                "source": "magic_millions",
                "type": "sale_result",
                "sale_name": sale_name,
                "year": year,
                "url": page_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extract price
            combined = " ".join(cells)
            price_match = re.search(r'\$\s*([\d,]+)', combined)
            if price_match:
                record["price_aud"] = int(price_match.group(1).replace(",", ""))

            # Extract lot number
            lot_match = re.search(r'(?:Lot|#)\s*(\d+)', combined, re.IGNORECASE)
            if lot_match:
                record["lot_number"] = int(lot_match.group(1))

            records.append(record)

    # -- Lot cards / catalogue entries --
    for card in soup.find_all(["div", "article", "li", "section"], class_=True):
        classes = " ".join(card.get("class", []))
        if any(kw in classes.lower() for kw in ["lot", "catalogue", "horse",
                                                  "entry", "result", "listing"]):
            text = card.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "source": "magic_millions",
                    "type": "lot_card",
                    "sale_name": sale_name,
                    "year": year,
                    "text": text[:2500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Extract horse name
                heading = card.find(["h2", "h3", "h4", "h5"])
                if heading:
                    record["horse_name"] = heading.get_text(strip=True)

                # Extract price
                price_match = re.search(r'\$\s*([\d,]+)', text)
                if price_match:
                    record["price_aud"] = int(price_match.group(1).replace(",", ""))

                # Extract lot number
                lot_match = re.search(r'(?:Lot|#)\s*(\d+)', text, re.IGNORECASE)
                if lot_match:
                    record["lot_number"] = int(lot_match.group(1))

                # Extract sire/dam
                sire_match = re.search(r'(?:by|sire)[:\s]+([A-Z][A-Za-z\s\'-]+)', text)
                if sire_match:
                    record["sire"] = sire_match.group(1).strip()
                dam_match = re.search(r'(?:out of|dam|from)[:\s]+([A-Z][A-Za-z\s\'-]+)', text)
                if dam_match:
                    record["dam"] = dam_match.group(1).strip()

                # Extract buyer/vendor
                buyer_match = re.search(r'(?:buyer|purchaser|sold to)[:\s]+(.+?)(?:\n|\||$)',
                                        text, re.IGNORECASE)
                if buyer_match:
                    record["buyer"] = buyer_match.group(1).strip()[:200]
                vendor_match = re.search(r'(?:vendor|consignor|offered by)[:\s]+(.+?)(?:\n|\||$)',
                                         text, re.IGNORECASE)
                if vendor_match:
                    record["vendor"] = vendor_match.group(1).strip()[:200]

                # Detail link
                link = card.find("a", href=True)
                if link:
                    href = link["href"]
                    if href.startswith("/"):
                        href = BASE_URL + href
                    record["detail_url"] = href

                records.append(record)

    # -- Pedigree sections --
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["pedigree", "breeding", "bloodline",
                                                  "family", "lineage"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                records.append({
                    "source": "magic_millions",
                    "type": "pedigree",
                    "sale_name": sale_name,
                    "year": year,
                    "contenu": text[:1500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # -- Sale statistics / summaries --
    for div in soup.find_all(["div", "section", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["summary", "statistics", "stats",
                                                  "aggregate", "total", "average",
                                                  "median", "clearance"]):
            if text and 10 < len(text) < 2000:
                record = {
                    "source": "magic_millions",
                    "type": "sale_summary",
                    "sale_name": sale_name,
                    "year": year,
                    "contenu": text[:1500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract summary stats
                avg_match = re.search(r'(?:average|avg)[:\s]*\$?\s*([\d,]+)', text, re.IGNORECASE)
                if avg_match:
                    record["average_aud"] = int(avg_match.group(1).replace(",", ""))
                median_match = re.search(r'(?:median)[:\s]*\$?\s*([\d,]+)', text, re.IGNORECASE)
                if median_match:
                    record["median_aud"] = int(median_match.group(1).replace(",", ""))
                gross_match = re.search(r'(?:gross|total|turnover|aggregate)[:\s]*\$?\s*([\d,]+)',
                                        text, re.IGNORECASE)
                if gross_match:
                    record["gross_aud"] = int(gross_match.group(1).replace(",", ""))
                lots_match = re.search(r'(\d+)\s*(?:lots?\s*(?:sold|offered))', text, re.IGNORECASE)
                if lots_match:
                    record["lots_count"] = int(lots_match.group(1))
                clearance_match = re.search(r'(?:clearance)[:\s]*(\d+\.?\d*)\s*%', text, re.IGNORECASE)
                if clearance_match:
                    record["clearance_pct"] = float(clearance_match.group(1))
                records.append(record)

    # -- Embedded JSON and data attributes --
    records.extend(extract_embedded_json(soup, str(year), "magic_millions"))
    records.extend(extract_data_attributes(soup, str(year), "magic_millions"))

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def scrape_lot_detail(page, lot_url, sale_name, year):
    """Scrape individual lot detail page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', lot_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"lot_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    html = navigate_with_retry(page, lot_url)
    if not html:
        return []

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"lot_{url_hash}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract all text content with structure
    record = {
        "source": "magic_millions",
        "type": "lot_detail",
        "sale_name": sale_name,
        "year": year,
        "url": lot_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Horse name
    h1 = soup.find("h1")
    if h1:
        record["horse_name"] = h1.get_text(strip=True)

    # All tables (pedigree, results, etc.)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        table_data = []
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells:
                row_dict = {}
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    row_dict[key] = cell
                table_data.append(row_dict)
        if table_data:
            records.append({
                "source": "magic_millions",
                "type": "lot_detail_table",
                "sale_name": sale_name,
                "year": year,
                "url": lot_url,
                "horse_name": record.get("horse_name", ""),
                "table_data": table_data,
                "scraped_at": datetime.now().isoformat(),
            })

    # Key info fields
    for div in soup.find_all(["div", "span", "p", "dl", "dt", "dd"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["detail", "info", "property",
                                                  "field", "specification",
                                                  "attribute"]):
            if text and 3 < len(text) < 500:
                # Try to parse key-value
                kv_match = re.match(r'^(.+?)[:\s]+(.+)$', text)
                if kv_match:
                    key = kv_match.group(1).strip().lower().replace(" ", "_")
                    val = kv_match.group(2).strip()
                    record[key] = val

    if len(record) > 5:  # Only add if we got meaningful data
        records.insert(0, record)

    # Embedded JSON and data attributes
    records.extend(extract_embedded_json(soup, str(year), "magic_millions"))
    records.extend(extract_data_attributes(soup, str(year), "magic_millions"))

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Script 100 -- Magic Millions AU Horse Sales Scraper")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-pages", type=int, default=2000,
                        help="Nombre max de pages a scraper")
    parser.add_argument("--scrape-details", action="store_true", default=False,
                        help="Scraper aussi les pages de detail de chaque lot")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 100 -- Magic Millions AU Sales Scraper (Playwright)")
    log.info(f"  Sale types : {len(SALE_TYPES)}")
    log.info(f"  Years : {SALE_YEARS[0]}-{SALE_YEARS[-1]}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    done_urls = set(checkpoint.get("done_urls", []))
    if args.resume and done_urls:
        log.info(f"  Reprise checkpoint: {len(done_urls)} pages deja traitees")

    output_file = os.path.join(OUTPUT_DIR, "magic_millions_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(pw)
        log.info("Browser launched (headless Chromium)")

        # Accept cookies on first navigation
        first_nav = True

        total_records = 0
        page_count = 0
        all_lot_urls = []

        # Phase 1: Scrape sale results pages
        log.info("  Phase 1: Pages de resultats de ventes")
        for year in SALE_YEARS:
            for sale_type in SALE_TYPES:
                if page_count >= args.max_pages:
                    break

                # Try multiple URL patterns
                urls_to_try = [
                    f"{BASE_URL}/sales/{year}/{sale_type}/results",
                    f"{BASE_URL}/sales/{sale_type}/{year}/results",
                    f"{BASE_URL}/sales-results/{year}/{sale_type}",
                    f"{BASE_URL}/{year}-{sale_type}/results",
                    f"{BASE_URL}/sales/{year}/{sale_type}",
                ]

                for url in urls_to_try:
                    if url in done_urls:
                        continue

                    records = scrape_sale_results_page(page, url, sale_type, year)

                    if first_nav:
                        accept_cookies(page)
                        first_nav = False

                    if records:
                        for rec in records:
                            append_jsonl(output_file, rec)
                            total_records += 1
                            # Collect lot detail URLs
                            detail_url = rec.get("detail_url")
                            if detail_url and args.scrape_details:
                                all_lot_urls.append((sale_type, year, detail_url))

                    done_urls.add(url)
                    page_count += 1

                    if records:
                        log.info(f"    {year} {sale_type}: {len(records)} records")
                        break  # Found data, stop trying URL patterns

                    smart_pause(1.0, 0.5)

                if page_count % 20 == 0:
                    save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                                     "total_records": total_records})

        # Phase 2: Scrape lot detail pages (if enabled)
        if args.scrape_details and all_lot_urls:
            log.info(f"  Phase 2: Detail des lots ({len(all_lot_urls)} lots)")
            for sale_type, year, lot_url in all_lot_urls:
                if lot_url in done_urls:
                    continue
                if page_count >= args.max_pages:
                    break

                records = scrape_lot_detail(page, lot_url, sale_type, year)
                if records:
                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1

                done_urls.add(lot_url)
                page_count += 1

                if page_count % 20 == 0:
                    log.info(f"    lots: pages={page_count} records={total_records}")
                    save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                                     "total_records": total_records})

                smart_pause(1.5, 0.8)

        # Phase 3: Discover additional pages from main site
        log.info("  Phase 3: Decouverte de pages additionnelles")
        main_html = navigate_with_retry(page, BASE_URL)
        if main_html:
            main_soup = BeautifulSoup(main_html, "html.parser")
            for a in main_soup.find_all("a", href=True):
                href = a["href"]
                if any(kw in href.lower() for kw in ["sale", "result", "catalogue",
                                                       "lot", "statistics"]):
                    if href.startswith("/"):
                        href = BASE_URL + href
                    if href.startswith("http") and href not in done_urls and page_count < args.max_pages:
                        records = scrape_sale_results_page(page, href, "discovered", 0)
                        if records:
                            for rec in records:
                                append_jsonl(output_file, rec)
                                total_records += 1
                        done_urls.add(href)
                        page_count += 1
                        smart_pause()

        save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"TERMINE: {page_count} pages, {total_records} records -> {output_file}")
        log.info("=" * 60)

    finally:
        # Graceful cleanup
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
