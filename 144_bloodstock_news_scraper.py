#!/usr/bin/env python3
"""
Script 144 — Bloodstock / TDN Scraper (Playwright)
Source : thoroughbreddailynews.com
Collecte : Stallion stats, sales data, breeding news, sire rankings
URL patterns :
  /stallion-stats/          -> stallion performance statistics
  /sales/                   -> sales results and catalogues
  /sire-list/               -> sire rankings by progeny earnings
  /news/breeding/           -> breeding news and analysis
  /results/                 -> race results with bloodlines
CRITIQUE pour : Stallion Stats, Sales Data, Sire Rankings, Breeding Analysis

Section TODO : 7Q

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

SCRIPT_NAME = "144_bloodstock_news"
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

log = setup_logging("144_bloodstock_news")

BASE_URL = "https://www.thoroughbreddailynews.com"
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

def extract_stallion_stats(soup, date_str):
    """Extract stallion performance statistics from page content."""
    records = []
    for el in soup.find_all(["div", "section", "table", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stallion", "sire", "stud",
                                                   "stats", "ranking",
                                                   "progeny", "earnings",
                                                   "covering", "fee"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "tdn_bloodstock",
                    "type": "stallion_stats",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract stallion name
                name_match = re.search(r'(?:stallion|sire)\s*:?\s*([A-Z][a-zA-Z\s\'-]+)', text)
                if name_match:
                    record["stallion_name"] = name_match.group(1).strip()
                # Try to extract fee
                fee_match = re.search(r'\$\s*([\d,]+)', text)
                if fee_match:
                    record["stud_fee"] = fee_match.group(1).replace(",", "")
                records.append(record)
    return records


def extract_sales_data(soup, date_str):
    """Extract sales results and catalogue data."""
    records = []
    for el in soup.find_all(["div", "section", "table", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["sale", "auction", "lot",
                                                   "catalogue", "consign",
                                                   "buyer", "vendor",
                                                   "hammer", "price"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "tdn_bloodstock",
                    "type": "sales_data",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract price
                price_match = re.search(r'[\$\u00a3\u20ac]\s*([\d,]+)', text)
                if price_match:
                    record["price"] = price_match.group(1).replace(",", "")
                # Try to extract sale name
                sale_match = re.search(r'(Keeneland|Tattersalls|Goffs|Arqana|Fasig-Tipton'
                                       r'|Inglis|Magic Millions)', text, re.I)
                if sale_match:
                    record["sale_house"] = sale_match.group(1).strip()
                records.append(record)
    return records


def extract_breeding_news(soup, date_str):
    """Extract breeding news and analysis articles."""
    records = []
    for el in soup.find_all(["article", "div", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["article", "news", "story",
                                                   "post", "entry", "content",
                                                   "breeding", "bloodstock"]):
            text = el.get_text(strip=True)
            if text and 50 < len(text) < 5000:
                record = {
                    "date": date_str,
                    "source": "tdn_bloodstock",
                    "type": "breeding_news",
                    "contenu": text[:3000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract title
                title_el = el.find(["h1", "h2", "h3"])
                if title_el:
                    record["title"] = title_el.get_text(strip=True)[:200]
                records.append(record)
    return records


def extract_sire_rankings(soup, date_str):
    """Extract sire ranking tables."""
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        # Check if this looks like a sire/stallion table
        header_text = " ".join(headers)
        if not any(kw in header_text for kw in ["sire", "stallion", "stud",
                                                  "progeny", "runner",
                                                  "winner", "earnings"]):
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "date": date_str,
                "source": "tdn_bloodstock",
                "type": "sire_ranking",
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


def extract_article_links(soup):
    """Extract links to individual articles and stallion pages."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(stallion|sire|sale|breeding|bloodstock|news)/', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_embedded_json_data(soup, date_str):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "date": date_str,
                    "source": "tdn_bloodstock",
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
                    "source": "tdn_bloodstock",
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_data_attributes(soup, date_str):
    """Extract data-* attributes related to bloodstock/breeding."""
    records = []
    keywords = ["horse", "stallion", "sire", "dam", "sale", "lot",
                "breeding", "bloodstock", "pedigree", "progeny"]
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in keywords)
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "tdn_bloodstock",
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

def scrape_stallion_page(page, date_str):
    """Scrape stallion stats listing page."""
    cache_file = os.path.join(CACHE_DIR, f"stallion_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/stallion-stats/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"stallion_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))
    records.extend(extract_stallion_stats(soup, date_str))
    records.extend(extract_sire_rankings(soup, date_str))

    article_links = extract_article_links(soup)

    result = {"records": records, "article_links": article_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_sales_page(page, date_str):
    """Scrape sales results page."""
    cache_file = os.path.join(CACHE_DIR, f"sales_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/sales/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))
    records.extend(extract_sales_data(soup, date_str))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_article_detail(page, article_url, date_str):
    """Scrape individual article for breeding/bloodstock content."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', article_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"article_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, article_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))
    records.extend(extract_stallion_stats(soup, date_str))
    records.extend(extract_sales_data(soup, date_str))
    records.extend(extract_breeding_news(soup, date_str))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_sire_list_page(page, date_str):
    """Scrape sire rankings page."""
    cache_file = os.path.join(CACHE_DIR, f"sire_list_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/sire-list/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_data_attributes(soup, date_str))
    records.extend(extract_sire_rankings(soup, date_str))
    records.extend(extract_stallion_stats(soup, date_str))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 144 — Bloodstock / TDN Scraper (stallion stats, sales data)"
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
    log.info("SCRIPT 144 — Bloodstock / TDN Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "bloodstock_news_data.jsonl")

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

            # Scrape stallion stats page
            result = scrape_stallion_page(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual article pages
                for article_url in result.get("article_links", [])[:10]:
                    detail = scrape_article_detail(page, article_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Also scrape sales and sire list pages
            sales_data = scrape_sales_page(page, date_str)
            if sales_data:
                for rec in sales_data:
                    append_jsonl(output_file, rec)
                    total_records += 1

            sire_data = scrape_sire_list_page(page, date_str)
            if sire_data:
                for rec in sire_data:
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
