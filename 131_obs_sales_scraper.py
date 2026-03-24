#!/usr/bin/env python3
"""
Script 131 — OBS Sales Scraper (Playwright)
Source : obssales.com (Ocala Breeders' Sales Company)
Collecte : US thoroughbred auction results, sale catalogs, hip data
URL patterns :
  /results/                   -> liste des ventes passees
  /results/{sale-id}/         -> resultats d'une vente
  /catalog/{sale-id}/         -> catalogue d'une vente
  /catalog/{sale-id}/hip/{n}  -> fiche d'un lot (hip)
CRITIQUE pour : US Thoroughbred Auction Data, Yearling / 2YO Sales

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
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "131_obs_sales"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("131_obs_sales")

BASE_URL = "https://www.obssales.com"
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
    """Extract links to individual sales from the results/catalog index."""
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(results|catalog|sale)/[^/]+', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            if full_url not in seen:
                seen.add(full_url)
                sale_name = a.get_text(strip=True)
                links.append({"url": full_url, "name": sale_name})
    return links


def extract_hip_links(soup):
    """Extract links to individual hip (lot) pages from a sale page."""
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/hip/\d+', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            hip_match = re.search(r'/hip/(\d+)', href)
            hip_num = hip_match.group(1) if hip_match else ""
            if full_url not in seen:
                seen.add(full_url)
                links.append({"url": full_url, "hip": hip_num})
    return links


def extract_sale_results_table(soup, sale_name, sale_url):
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
                "source": "obs_sales",
                "type": "sale_result",
                "sale_name": sale_name,
                "sale_url": sale_url,
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

            # Try to extract price
            for cell in cells:
                price_match = re.search(r'\$\s*([\d,]+)', cell)
                if price_match:
                    record["price_usd"] = price_match.group(1).replace(",", "")
                    break

            records.append(record)
    return records


def extract_hip_detail(soup, hip_url, sale_name):
    """Extract detailed lot (hip) information from a hip detail page."""
    records = []
    page_text = soup.get_text()

    hip_record = {
        "source": "obs_sales",
        "type": "hip_detail",
        "sale_name": sale_name,
        "url": hip_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Hip number
    hip_match = re.search(r'hip\s*#?\s*(\d+)', page_text, re.I)
    if hip_match:
        hip_record["hip_number"] = hip_match.group(1)

    # Horse name from title
    for h in soup.find_all(["h1", "h2", "h3"]):
        text = h.get_text(strip=True)
        if text and len(text) > 2:
            hip_record["horse_name"] = text
            break

    # Extract key-value pairs from detail tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower().replace(" ", "_").replace(":", "")
                val = cells[1].get_text(strip=True)
                if key and val and len(key) < 60:
                    hip_record[key] = val

    # Extract definition-list data
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val and len(key) < 60:
                hip_record[key] = val

    # Extract labelled divs (horse info, consignor, buyer, sire, dam)
    for el in soup.find_all(["div", "span", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["sire", "dam", "consignor", "buyer",
                                                   "price", "lot", "hip", "horse",
                                                   "pedigree", "catalog", "detail"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 500:
                hip_record[f"detail_{classes.replace(' ', '_')[:40]}"] = text[:300]

    # Parse common fields from page text
    sire_match = re.search(r'sire\s*:?\s*([A-Z][A-Za-z\s\'\-\.]+)', page_text)
    if sire_match:
        hip_record["sire"] = sire_match.group(1).strip()[:100]

    dam_match = re.search(r'dam\s*:?\s*([A-Z][A-Za-z\s\'\-\.]+)', page_text)
    if dam_match:
        hip_record["dam"] = dam_match.group(1).strip()[:100]

    consignor_match = re.search(r'consign(?:or|ed by)\s*:?\s*(.+?)(?:\n|$)',
                                page_text, re.I)
    if consignor_match:
        hip_record["consignor"] = consignor_match.group(1).strip()[:200]

    buyer_match = re.search(r'(?:buyer|purchased by|sold to)\s*:?\s*(.+?)(?:\n|$)',
                            page_text, re.I)
    if buyer_match:
        hip_record["buyer"] = buyer_match.group(1).strip()[:200]

    price_match = re.search(r'\$\s*([\d,]+)', page_text)
    if price_match:
        hip_record["price_usd"] = price_match.group(1).replace(",", "")

    sex_match = re.search(r'\b(colt|filly|gelding|horse|ridgling)\b', page_text, re.I)
    if sex_match:
        hip_record["sex"] = sex_match.group(1)

    color_match = re.search(
        r'\b(grey|gray|bay|chestnut|black|roan|dark bay|brown)\b',
        page_text, re.I
    )
    if color_match:
        hip_record["color"] = color_match.group(1)

    year_match = re.search(r'\b(foaled|born|year)\s*:?\s*(\d{4})\b', page_text, re.I)
    if year_match:
        hip_record["foaled"] = year_match.group(2)

    records.append(hip_record)

    # Extract embedded JSON
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "source": "obs_sales",
                    "type": "embedded_json",
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_sale_summary(soup, sale_url):
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
                    "source": "obs_sales",
                    "type": "sale_summary",
                    "url": sale_url,
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Try to extract aggregate stats
    total_match = re.search(r'total\s*:?\s*\$\s*([\d,]+)', page_text, re.I)
    avg_match = re.search(r'average\s*:?\s*\$\s*([\d,]+)', page_text, re.I)
    median_match = re.search(r'median\s*:?\s*\$\s*([\d,]+)', page_text, re.I)
    sold_match = re.search(r'(\d+)\s*(?:sold|lots?\s*sold)', page_text, re.I)
    offered_match = re.search(r'(\d+)\s*(?:offered|catalogu?ed|lots?\s*offered)',
                              page_text, re.I)

    if any([total_match, avg_match, sold_match]):
        summary = {
            "source": "obs_sales",
            "type": "sale_aggregate",
            "url": sale_url,
            "scraped_at": datetime.now().isoformat(),
        }
        if total_match:
            summary["total_usd"] = total_match.group(1).replace(",", "")
        if avg_match:
            summary["average_usd"] = avg_match.group(1).replace(",", "")
        if median_match:
            summary["median_usd"] = median_match.group(1).replace(",", "")
        if sold_match:
            summary["lots_sold"] = sold_match.group(1)
        if offered_match:
            summary["lots_offered"] = offered_match.group(1)
        records.append(summary)

    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_sales_index(page):
    """Scrape the OBS sales/results index to find all available sales."""
    cache_file = os.path.join(CACHE_DIR, "sales_index.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    all_sales = []

    # Try results page
    for path in ["/results", "/results/", "/catalog", "/catalog/",
                 "/sales", "/sales/", "/past-sales"]:
        url = f"{BASE_URL}{path}"
        html = navigate_with_retry(page, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        links = extract_sale_links(soup)
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


def scrape_sale(page, sale_info, output_file):
    """Scrape a single sale: results table + individual hip pages."""
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

    # Extract results table
    table_records = extract_sale_results_table(soup, sale_name, sale_url)
    records.extend(table_records)

    # Extract sale summary
    records.extend(extract_sale_summary(soup, sale_url))

    # Extract embedded JSON
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "source": "obs_sales",
                    "type": "embedded_json",
                    "sale_name": sale_name,
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    # Extract hip links and scrape individual hips (limit to avoid overload)
    hip_links = extract_hip_links(soup)
    for hip_info in hip_links[:200]:
        hip_url = hip_info["url"]
        hip_html = navigate_with_retry(page, hip_url)
        if not hip_html:
            smart_pause(1.0, 0.5)
            continue

        hip_soup = BeautifulSoup(hip_html, "html.parser")
        hip_records = extract_hip_detail(hip_soup, hip_url, sale_name)
        records.extend(hip_records)
        smart_pause(1.0, 0.5)

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
        description="Script 131 — OBS Sales Scraper (US thoroughbred auction results)"
    )
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-sales", type=int, default=0,
                        help="Max sales to scrape (0=unlimited)")
    parser.add_argument("--max-hips", type=int, default=200,
                        help="Max hips per sale (default=200)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 131 — OBS Sales Scraper (Playwright)")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_sale_url = checkpoint.get("last_sale_url", "")

    output_file = os.path.join(OUTPUT_DIR, "obs_sales_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US)")

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
            count = scrape_sale(page, sale_info, output_file)
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
