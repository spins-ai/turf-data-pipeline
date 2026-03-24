#!/usr/bin/env python3
"""
Script 132 — Inglis Scraper (Playwright)
Source : inglis.com.au (Inglis Bloodstock — Australia)
Collecte : Australian thoroughbred auction results, sale catalogs, lot data
URL patterns :
  /sales/                          -> liste des ventes
  /sales/{sale-slug}/              -> page d'une vente
  /sales/{sale-slug}/lots/         -> lots d'une vente
  /sales/{sale-slug}/lots/{lot}/   -> fiche lot individuel
  /sales/{sale-slug}/results/      -> resultats d'une vente
CRITIQUE pour : AU Thoroughbred Auction Data, Yearling / Weanling / Easter Sales

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

SCRIPT_NAME = "132_inglis"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("132_inglis")

BASE_URL = "https://www.inglis.com.au"
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

def extract_sale_links(soup):
    """Extract links to individual sales from the sales index page."""
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/sales/[a-z0-9\-]+', href, re.I) and "/lots" not in href:
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            if full_url not in seen:
                seen.add(full_url)
                sale_name = a.get_text(strip=True)
                links.append({"url": full_url, "name": sale_name})
    return links


def extract_lot_links(soup, sale_url):
    """Extract links to individual lot pages from a sale page."""
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/lots?/\d+', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            lot_match = re.search(r'/lots?/(\d+)', href)
            lot_num = lot_match.group(1) if lot_match else ""
            if full_url not in seen:
                seen.add(full_url)
                links.append({"url": full_url, "lot": lot_num})
    return links


def extract_results_table(soup, sale_name, sale_url):
    """Extract results table from a sale results page."""
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
                "source": "inglis",
                "type": "sale_result",
                "sale_name": sale_name,
                "sale_url": sale_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Data attributes
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            # Try to extract price (AUD)
            for cell in cells:
                price_match = re.search(r'\$\s*([\d,]+)', cell)
                if price_match:
                    record["price_aud"] = price_match.group(1).replace(",", "")
                    break

            records.append(record)
    return records


def extract_lot_detail(soup, lot_url, sale_name):
    """Extract detailed lot information from a lot detail page."""
    records = []
    page_text = soup.get_text()

    lot_record = {
        "source": "inglis",
        "type": "lot_detail",
        "sale_name": sale_name,
        "url": lot_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Lot number
    lot_match = re.search(r'lot\s*#?\s*(\d+)', page_text, re.I)
    if lot_match:
        lot_record["lot_number"] = lot_match.group(1)

    # Horse name from title
    for h in soup.find_all(["h1", "h2", "h3"]):
        text = h.get_text(strip=True)
        if text and len(text) > 2:
            lot_record["horse_name"] = text
            break

    # Key-value pairs from detail tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower().replace(" ", "_").replace(":", "")
                val = cells[1].get_text(strip=True)
                if key and val and len(key) < 60:
                    lot_record[key] = val

    # Definition-list data
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val and len(key) < 60:
                lot_record[key] = val

    # Labelled divs (horse info, vendor, buyer, sire, dam)
    for el in soup.find_all(["div", "span", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["sire", "dam", "vendor", "buyer",
                                                   "price", "lot", "horse", "property",
                                                   "pedigree", "catalog", "detail",
                                                   "breeding", "consignor"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 500:
                lot_record[f"detail_{classes.replace(' ', '_')[:40]}"] = text[:300]

    # Parse common fields from page text
    sire_match = re.search(r'(?:sire|by)\s*:?\s*([A-Z][A-Za-z\s\'\-\.]+)', page_text)
    if sire_match:
        lot_record["sire"] = sire_match.group(1).strip()[:100]

    dam_match = re.search(r'dam\s*:?\s*([A-Z][A-Za-z\s\'\-\.]+)', page_text)
    if dam_match:
        lot_record["dam"] = dam_match.group(1).strip()[:100]

    vendor_match = re.search(r'(?:vendor|consigned by|property of)\s*:?\s*(.+?)(?:\n|$)',
                             page_text, re.I)
    if vendor_match:
        lot_record["vendor"] = vendor_match.group(1).strip()[:200]

    buyer_match = re.search(r'(?:buyer|purchased by|sold to)\s*:?\s*(.+?)(?:\n|$)',
                            page_text, re.I)
    if buyer_match:
        lot_record["buyer"] = buyer_match.group(1).strip()[:200]

    price_match = re.search(r'\$\s*([\d,]+)', page_text)
    if price_match:
        lot_record["price_aud"] = price_match.group(1).replace(",", "")

    sex_match = re.search(r'\b(colt|filly|gelding|horse|ridgling|mare|stallion)\b',
                          page_text, re.I)
    if sex_match:
        lot_record["sex"] = sex_match.group(1)

    color_match = re.search(
        r'\b(grey|gray|bay|chestnut|black|roan|dark bay|brown)\b',
        page_text, re.I
    )
    if color_match:
        lot_record["color"] = color_match.group(1)

    year_match = re.search(r'\b(foaled|born|year)\s*:?\s*(\d{4})\b', page_text, re.I)
    if year_match:
        lot_record["foaled"] = year_match.group(2)

    records.append(lot_record)

    # Pedigree extraction
    pedigree = extract_pedigree(soup, lot_url)
    if pedigree:
        records.append(pedigree)

    # Embedded JSON
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "source": "inglis",
                    "type": "embedded_json",
                    "sale_name": sale_name,
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    # __NEXT_DATA__ or SSR payloads
    for script in soup.find_all("script", {"id": "__NEXT_DATA__"}):
        try:
            data = json.loads(script.string or "")
            page_props = data.get("props", {}).get("pageProps", {})
            if page_props:
                records.append({
                    "source": "inglis",
                    "type": "next_data",
                    "sale_name": sale_name,
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_pedigree(soup, lot_url):
    """Extract pedigree tree from lot page."""
    pedigree_record = {
        "source": "inglis",
        "type": "pedigree",
        "url": lot_url,
        "scraped_at": datetime.now().isoformat(),
    }

    ancestors = []

    # Look for pedigree table
    for table in soup.find_all("table"):
        classes = " ".join(table.get("class", []))
        table_text = table.get_text(strip=True)[:500]
        if (any(kw in classes.lower() for kw in ["pedigree", "ped", "lineage", "tree"])
                or "sire" in table_text.lower()
                or "dam" in table_text.lower()):
            for cell in table.find_all(["td", "th"]):
                text = cell.get_text(strip=True)
                if text and 2 < len(text) < 200:
                    link = cell.find("a", href=True)
                    entry = {"name": text}
                    if link:
                        entry["url"] = link["href"]
                    ancestors.append(entry)

    # Also look for structured pedigree divs
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["pedigree", "lineage", "tree",
                                                   "sire", "dam", "breeding"]):
            for a in div.find_all("a", href=True):
                text = a.get_text(strip=True)
                if text and 2 < len(text) < 200:
                    ancestors.append({"name": text, "url": a["href"]})

    if not ancestors:
        return None

    # Deduplicate
    seen = set()
    unique_ancestors = []
    for a in ancestors:
        key = a.get("name", "")
        if key and key not in seen:
            seen.add(key)
            unique_ancestors.append(a)

    pedigree_record["ancestors"] = unique_ancestors
    return pedigree_record


def extract_sale_summary(soup, sale_url, sale_name):
    """Extract sale summary statistics."""
    records = []
    page_text = soup.get_text()

    for el in soup.find_all(["div", "section", "article", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["summary", "statistics", "stats",
                                                   "overview", "totals", "recap",
                                                   "aggregate", "average"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                records.append({
                    "source": "inglis",
                    "type": "sale_summary",
                    "sale_name": sale_name,
                    "url": sale_url,
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Aggregate stats
    total_match = re.search(r'(?:gross|total)\s*:?\s*\$\s*([\d,]+)', page_text, re.I)
    avg_match = re.search(r'average\s*:?\s*\$\s*([\d,]+)', page_text, re.I)
    median_match = re.search(r'median\s*:?\s*\$\s*([\d,]+)', page_text, re.I)
    sold_match = re.search(r'(\d+)\s*(?:sold|lots?\s*sold)', page_text, re.I)
    offered_match = re.search(r'(\d+)\s*(?:offered|catalogu?ed|lots?\s*offered)',
                              page_text, re.I)
    top_match = re.search(r'(?:top\s*(?:lot|price))\s*:?\s*\$\s*([\d,]+)',
                          page_text, re.I)
    clearance_match = re.search(r'clearance\s*(?:rate)?\s*:?\s*(\d+(?:\.\d+)?)\s*%',
                                page_text, re.I)

    if any([total_match, avg_match, sold_match]):
        summary = {
            "source": "inglis",
            "type": "sale_aggregate",
            "sale_name": sale_name,
            "url": sale_url,
            "scraped_at": datetime.now().isoformat(),
        }
        if total_match:
            summary["gross_aud"] = total_match.group(1).replace(",", "")
        if avg_match:
            summary["average_aud"] = avg_match.group(1).replace(",", "")
        if median_match:
            summary["median_aud"] = median_match.group(1).replace(",", "")
        if sold_match:
            summary["lots_sold"] = sold_match.group(1)
        if offered_match:
            summary["lots_offered"] = offered_match.group(1)
        if top_match:
            summary["top_price_aud"] = top_match.group(1).replace(",", "")
        if clearance_match:
            summary["clearance_rate_pct"] = clearance_match.group(1)
        records.append(summary)

    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_sales_index(page):
    """Scrape the Inglis sales index to find all available sales."""
    cache_file = os.path.join(CACHE_DIR, "sales_index.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    all_sales = []

    # Try multiple index pages
    for path in ["/sales", "/sales/", "/sales/past", "/sales/past/",
                 "/sales/results", "/sales/upcoming"]:
        url = f"{BASE_URL}{path}"
        html = navigate_with_retry(page, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        links = extract_sale_links(soup)
        all_sales.extend(links)
        smart_pause(1.0, 0.5)

    # Try paginated results
    for page_num in range(2, 20):
        url = f"{BASE_URL}/sales?page={page_num}"
        html = navigate_with_retry(page, url)
        if not html:
            break
        soup = BeautifulSoup(html, "html.parser")
        links = extract_sale_links(soup)
        if not links:
            break
        all_sales.extend(links)
        smart_pause(1.0, 0.5)

    # Deduplicate
    seen = set()
    unique_sales = []
    for s in all_sales:
        if s["url"] not in seen:
            seen.add(s["url"])
            unique_sales.append(s)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(unique_sales, f, ensure_ascii=False, indent=2)

    return unique_sales


def scrape_sale(page, sale_info, output_file, max_lots=200):
    """Scrape a single sale: results + individual lot pages."""
    sale_url = sale_info["url"]
    sale_name = sale_info.get("name", "")
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', sale_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"sale_{url_hash}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
        return cached.get("count", 0)

    html = navigate_with_retry(page, sale_url)
    if not html:
        return 0

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"sale_{url_hash}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Sale summary
    records.extend(extract_sale_summary(soup, sale_url, sale_name))

    # Try results page
    for results_suffix in ["/results", "/results/"]:
        results_url = sale_url.rstrip("/") + results_suffix
        results_html = navigate_with_retry(page, results_url)
        if results_html:
            results_soup = BeautifulSoup(results_html, "html.parser")
            table_records = extract_results_table(results_soup, sale_name, results_url)
            records.extend(table_records)
            records.extend(extract_sale_summary(results_soup, results_url, sale_name))
            break
        smart_pause(0.5, 0.3)

    # Try lots page
    lot_links = []
    for lots_suffix in ["/lots", "/lots/"]:
        lots_url = sale_url.rstrip("/") + lots_suffix
        lots_html = navigate_with_retry(page, lots_url)
        if lots_html:
            lots_soup = BeautifulSoup(lots_html, "html.parser")
            lot_links = extract_lot_links(lots_soup, lots_url)

            # Check for pagination on lots page
            for pg in range(2, 50):
                next_url = f"{lots_url}?page={pg}"
                next_html = navigate_with_retry(page, next_url)
                if not next_html:
                    break
                next_soup = BeautifulSoup(next_html, "html.parser")
                more = extract_lot_links(next_soup, next_url)
                if not more:
                    break
                lot_links.extend(more)
                smart_pause(0.8, 0.4)
            break
        smart_pause(0.5, 0.3)

    # Also check main sale page for lot links
    lot_links.extend(extract_lot_links(soup, sale_url))

    # Deduplicate lot links
    seen_lots = set()
    unique_lots = []
    for lot in lot_links:
        if lot["url"] not in seen_lots:
            seen_lots.add(lot["url"])
            unique_lots.append(lot)

    # Scrape individual lots
    for lot_info in unique_lots[:max_lots]:
        lot_url = lot_info["url"]
        lot_html = navigate_with_retry(page, lot_url)
        if not lot_html:
            smart_pause(1.0, 0.5)
            continue

        lot_soup = BeautifulSoup(lot_html, "html.parser")
        lot_records = extract_lot_detail(lot_soup, lot_url, sale_name)
        records.extend(lot_records)
        smart_pause(1.0, 0.5)

    # Embedded JSON from main sale page
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "source": "inglis",
                    "type": "embedded_json",
                    "sale_name": sale_name,
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    # Write all records
    for rec in records:
        append_jsonl(output_file, rec)

    with open(cache_file, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"sale_url": sale_url, "count": len(records)},
                  f, ensure_ascii=False, indent=2)

    return len(records)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 132 — Inglis Scraper (Australian thoroughbred auction results)"
    )
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-sales", type=int, default=0,
                        help="Max sales to scrape (0=unlimited)")
    parser.add_argument("--max-lots", type=int, default=200,
                        help="Max lots per sale (default=200)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 132 — Inglis Scraper (Playwright)")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_sale_url = checkpoint.get("last_sale_url", "")

    output_file = os.path.join(OUTPUT_DIR, "inglis_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-AU", timezone="Australia/Sydney"
        )
        log.info("Browser launched (headless Chromium, locale=en-AU)")

        # Accept cookies on first visit
        html = navigate_with_retry(page, BASE_URL)
        if html:
            accept_cookies(page)

        # Get list of all sales
        sales = scrape_sales_index(page)
        log.info("  Found %d sales to scrape", len(sales))

        # Resume logic
        if args.resume and last_sale_url:
            skip = True
            filtered = []
            for s in sales:
                if s["url"] == last_sale_url:
                    skip = False
                    continue
                if not skip:
                    filtered.append(s)
            if filtered:
                sales = filtered
                log.info("  Resuming after: %s (%d remaining)", last_sale_url, len(sales))

        total_records = checkpoint.get("total_records", 0)
        sale_count = 0

        for sale_info in sales:
            if args.max_sales and sale_count >= args.max_sales:
                break

            log.info("  Scraping sale: %s", sale_info.get("name", sale_info["url"]))
            count = scrape_sale(page, sale_info, output_file, max_lots=args.max_lots)
            total_records += count
            sale_count += 1

            log.info("  Sale done: %d records (total: %d)", count, total_records)

            save_checkpoint(CHECKPOINT_FILE, {
                "last_sale_url": sale_info["url"],
                "total_records": total_records,
                "sales_done": sale_count,
            })

            smart_pause(2.0, 1.0)

        save_checkpoint(CHECKPOINT_FILE, {
            "last_sale_url": sales[-1]["url"] if sales else "",
            "total_records": total_records,
            "sales_done": sale_count,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: %d sales, %d records -> %s",
                 sale_count, total_records, output_file)
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
