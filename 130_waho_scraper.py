#!/usr/bin/env python3
"""
Script 130 — WAHO Scraper (Playwright)
Source : waho.org (World Arabian Horse Organization)
Collecte : Arabian pedigree data, studbook entries, horse details
URL patterns :
  /Home/HorseSearch           -> recherche de chevaux
  /Home/HorseDetails/{id}     -> fiche cheval (pedigree, performances)
CRITIQUE pour : Arabian Pedigree, International Studbook

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

SCRIPT_NAME = "130_waho"
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

log = setup_logging("130_waho")

BASE_URL = "https://www.waho.org"
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

def extract_horse_search_results(soup):
    """Extract horse links from a search results page."""
    results = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/Home/HorseDetails/\d+', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            horse_id_match = re.search(r'/HorseDetails/(\d+)', href)
            horse_id = horse_id_match.group(1) if horse_id_match else ""
            name = a.get_text(strip=True)
            results.append({
                "horse_id": horse_id,
                "name": name,
                "url": full_url,
            })
    # Deduplicate by horse_id
    seen = set()
    unique = []
    for r in results:
        if r["horse_id"] and r["horse_id"] not in seen:
            seen.add(r["horse_id"])
            unique.append(r)
    return unique


def extract_horse_details(soup, horse_url, horse_id):
    """Extract pedigree and details from a horse detail page."""
    records = []
    page_text = soup.get_text()

    # Basic horse info
    horse_record = {
        "source": "waho",
        "type": "horse_detail",
        "horse_id": horse_id,
        "url": horse_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Horse name from title or h1
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 2:
            horse_record["name"] = text
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
                    horse_record[key] = val

    # Extract definition-list style data (dt/dd pairs)
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val and len(key) < 60:
                horse_record[key] = val

    # Extract labelled spans/divs
    for el in soup.find_all(["div", "span", "p", "label"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["detail", "field", "info", "label",
                                                   "value", "horse-name", "breed",
                                                   "color", "sex", "country"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 500:
                horse_record[f"detail_{classes.replace(' ', '_')[:40]}"] = text[:300]

    # Parse common fields from page text
    sex_match = re.search(r'\b(stallion|mare|gelding|colt|filly)\b', page_text, re.I)
    if sex_match:
        horse_record["sex"] = sex_match.group(1)

    color_match = re.search(
        r'\b(grey|gray|bay|chestnut|black|roan|palomino|dun|buckskin|cremello|perlino)\b',
        page_text, re.I
    )
    if color_match:
        horse_record["color"] = color_match.group(1)

    year_match = re.search(r'\b(foaled|born|year)\s*:?\s*(\d{4})\b', page_text, re.I)
    if year_match:
        horse_record["foaled"] = year_match.group(2)

    country_match = re.search(r'\b(country|bred in|origin)\s*:?\s*([A-Z]{2,3})\b',
                              page_text, re.I)
    if country_match:
        horse_record["country"] = country_match.group(2)

    records.append(horse_record)

    # Pedigree extraction
    pedigree = extract_pedigree(soup, horse_id)
    if pedigree:
        records.append(pedigree)

    return records


def extract_pedigree(soup, horse_id):
    """Extract pedigree tree from tables or structured divs."""
    pedigree_record = {
        "source": "waho",
        "type": "pedigree",
        "horse_id": horse_id,
        "scraped_at": datetime.now().isoformat(),
    }

    # Look for pedigree table (typically 4-generation, tree layout)
    pedigree_tables = []
    for table in soup.find_all("table"):
        classes = " ".join(table.get("class", []))
        table_text = table.get_text(strip=True)[:500]
        if (any(kw in classes.lower() for kw in ["pedigree", "ped", "lineage", "tree"])
                or "sire" in table_text.lower()
                or "dam" in table_text.lower()):
            pedigree_tables.append(table)

    ancestors = []
    for table in pedigree_tables:
        for cell in table.find_all(["td", "th"]):
            text = cell.get_text(strip=True)
            if text and 2 < len(text) < 200:
                link = cell.find("a", href=True)
                entry = {"name": text}
                if link:
                    href = link["href"]
                    anc_id = re.search(r'/HorseDetails/(\d+)', href)
                    if anc_id:
                        entry["ancestor_id"] = anc_id.group(1)
                ancestors.append(entry)

    # Also look for structured pedigree divs
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["pedigree", "lineage", "tree",
                                                   "sire", "dam", "ancestor"]):
            for a in div.find_all("a", href=True):
                text = a.get_text(strip=True)
                if text and 2 < len(text) < 200:
                    entry = {"name": text}
                    href = a["href"]
                    anc_id = re.search(r'/HorseDetails/(\d+)', href)
                    if anc_id:
                        entry["ancestor_id"] = anc_id.group(1)
                    ancestors.append(entry)

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

    # Try to assign sire/dam from first entries
    page_text = soup.get_text()
    sire_match = re.search(r'sire\s*:?\s*([A-Z][A-Za-z\s\'\-\.]+)', page_text)
    if sire_match:
        pedigree_record["sire"] = sire_match.group(1).strip()
    dam_match = re.search(r'dam\s*:?\s*([A-Z][A-Za-z\s\'\-\.]+)', page_text)
    if dam_match:
        pedigree_record["dam"] = dam_match.group(1).strip()

    return pedigree_record


# ------------------------------------------------------------------
# Search and pagination
# ------------------------------------------------------------------

def search_horses(page, query, max_pages=50):
    """Search WAHO database and return list of horse entries."""
    all_results = []
    search_url = f"{BASE_URL}/Home/HorseSearch"

    html = navigate_with_retry(page, search_url)
    if not html:
        return all_results

    # Fill search form and submit
    try:
        # Try to fill the search input
        search_input = page.locator("input[name*='search'], input[name*='Search'], "
                                    "input[name*='horse'], input[name*='Horse'], "
                                    "input[type='text']").first
        if search_input.is_visible(timeout=5000):
            search_input.fill(query)
            time.sleep(0.5)

            # Submit form
            submit_btn = page.locator("button[type='submit'], input[type='submit'], "
                                      "button:has-text('Search'), "
                                      "button:has-text('Find')").first
            if submit_btn.is_visible(timeout=3000):
                submit_btn.click()
                page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT_MS)
                time.sleep(2)
    except Exception as exc:
        log.warning("  Search form interaction failed: %s", str(exc)[:200])

    # Parse results pages
    for page_num in range(1, max_pages + 1):
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        results = extract_horse_search_results(soup)

        if not results:
            break

        all_results.extend(results)
        log.info("  Search page %d: %d results (total: %d)",
                 page_num, len(results), len(all_results))

        # Try to go to next page
        next_link = page.locator("a:has-text('Next'), a:has-text('>>'), "
                                 "a.next, [class*='next']").first
        try:
            if next_link.is_visible(timeout=3000):
                next_link.click()
                page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT_MS)
                time.sleep(2)
            else:
                break
        except Exception:
            break

    return all_results


# ------------------------------------------------------------------
# Alphabet-based crawl
# ------------------------------------------------------------------

SEARCH_LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def scrape_by_letter(page, letter, output_file, checkpoint):
    """Scrape all horses whose names start with a given letter."""
    cache_file = os.path.join(CACHE_DIR, f"letter_{letter}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
        return cached.get("count", 0)

    log.info("  Searching letter: %s", letter)
    results = search_horses(page, letter)

    total = 0
    for entry in results:
        horse_id = entry.get("horse_id", "")
        if not horse_id:
            continue

        # Check if already scraped
        detail_cache = os.path.join(CACHE_DIR, f"horse_{horse_id}.json")
        if os.path.exists(detail_cache):
            with open(detail_cache, "r", encoding="utf-8") as f:
                cached_records = json.load(f)
            for rec in cached_records:
                append_jsonl(output_file, rec)
                total += 1
            continue

        horse_url = entry.get("url", f"{BASE_URL}/Home/HorseDetails/{horse_id}")
        html = navigate_with_retry(page, horse_url)
        if not html:
            smart_pause(1.0, 0.5)
            continue

        # Save raw HTML
        html_file = os.path.join(HTML_CACHE_DIR, f"horse_{horse_id}.html")
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html)

        soup = BeautifulSoup(html, "html.parser")
        records = extract_horse_details(soup, horse_url, horse_id)
        records.extend(extract_embedded_json_data(soup, "waho"))

        # Cache detail records
        with open(detail_cache, "w", encoding="utf-8", newline="\n") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        for rec in records:
            append_jsonl(output_file, rec)
            total += 1

        smart_pause(1.5, 0.8)

    # Cache letter summary
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump({"letter": letter, "count": total, "results_found": len(results)},
                  f, ensure_ascii=False, indent=2)

    return total


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 130 — WAHO Scraper (Arabian pedigree data)"
    )
    parser.add_argument("--letters", type=str, default="",
                        help="Comma-separated letters to scrape (default=A-Z)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-per-letter", type=int, default=0,
                        help="Max horses per letter (0=unlimited)")
    args = parser.parse_args()

    letters = [l.strip().upper() for l in args.letters.split(",") if l.strip()] \
        if args.letters else SEARCH_LETTERS

    log.info("=" * 60)
    log.info("SCRIPT 130 — WAHO Scraper (Playwright)")
    log.info("  Letters: %s", ", ".join(letters))
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_letter = checkpoint.get("last_letter", "")
    if args.resume and last_letter:
        try:
            idx = letters.index(last_letter)
            letters = letters[idx + 1:]
            log.info("  Resuming after letter: %s", last_letter)
        except ValueError:
            pass

    output_file = os.path.join(OUTPUT_DIR, "waho_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US)")

        first_nav = True
        total_records = 0

        for letter in letters:
            if first_nav:
                html = navigate_with_retry(page, BASE_URL)
                if html:
                    accept_cookies(page)
                first_nav = False

            count = scrape_by_letter(page, letter, output_file, checkpoint)
            total_records += count

            log.info("  Letter %s: %d records (total: %d)",
                     letter, count, total_records)

            save_checkpoint(CHECKPOINT_FILE, {
                "last_letter": letter,
                "total_records": total_records,
            })

            smart_pause(2.0, 1.0)

        save_checkpoint(CHECKPOINT_FILE, {
            "last_letter": letters[-1] if letters else "Z",
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: %d letters, %d records -> %s",
                 len(SEARCH_LETTERS), total_records, output_file)
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
