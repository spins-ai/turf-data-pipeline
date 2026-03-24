#!/usr/bin/env python3
"""
Script 152 — Zone-Turf Stats Scraper (Playwright)
Source : zone-turf.fr/statistiques
Collecte : Stats par hippodrome, jockey, entraineur
URL patterns :
  /statistiques/                         -> page principale stats
  /statistiques/jockeys/                 -> classement jockeys
  /statistiques/entraineurs/             -> classement entraineurs
  /statistiques/hippodromes/             -> stats par hippodrome
  /statistiques/jockeys/{slug}/          -> fiche jockey
  /statistiques/entraineurs/{slug}/      -> fiche entraineur
  /statistiques/hippodromes/{slug}/      -> fiche hippodrome
CRITIQUE pour : FR Jockey/Trainer/Venue Stats, Performance Analysis

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

SCRIPT_NAME = "152_zone_turf_stats"
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

log = setup_logging("152_zone_turf_stats")

BASE_URL = "https://www.zone-turf.fr"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Stat sections to scrape
STAT_SECTIONS = [
    {"name": "jockeys", "url": "/statistiques/jockeys/", "type": "jockey"},
    {"name": "entraineurs", "url": "/statistiques/entraineurs/", "type": "entraineur"},
    {"name": "hippodromes", "url": "/statistiques/hippodromes/", "type": "hippodrome"},
]


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

def extract_stats_table(soup, section_type):
    """Extract statistics from tables."""
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
                "source": "zone_turf",
                "type": f"stats_{section_type}",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extract data-attributes
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            # Extract links in row (for detail pages)
            link = row.find("a", href=True)
            if link:
                record["detail_url"] = link["href"] if link["href"].startswith("http") \
                    else f"{BASE_URL}{link['href']}"
                record["name"] = link.get_text(strip=True)

            records.append(record)
    return records


def extract_stats_cards(soup, section_type):
    """Extract statistics from card/div-based layouts."""
    records = []
    for card in soup.find_all(["div", "article", "li"], class_=True):
        classes = " ".join(card.get("class", []))
        if not any(kw in classes.lower() for kw in ["stat", "classement", "ranking",
                                                       "palmares", "fiche", "item",
                                                       "entry", "row", "card"]):
            continue

        text = card.get_text(strip=True)
        if len(text) < 5 or len(text) > 2000:
            continue

        record = {
            "source": "zone_turf",
            "type": f"stats_{section_type}_card",
            "contenu": text[:1500],
            "classes_css": classes,
            "scraped_at": datetime.now().isoformat(),
        }

        # Name
        name_el = card.find(["h3", "h4", "a", "strong"])
        if name_el:
            record["name"] = name_el.get_text(strip=True)
            if name_el.name == "a" and name_el.get("href"):
                href = name_el["href"]
                record["detail_url"] = href if href.startswith("http") \
                    else f"{BASE_URL}{href}"

        # Stats numbers
        for span in card.find_all(["span", "div", "td"], class_=True):
            span_classes = " ".join(span.get("class", []))
            span_text = span.get_text(strip=True)
            if any(kw in span_classes.lower() for kw in ["victoire", "win", "gain",
                                                            "course", "run",
                                                            "place", "taux",
                                                            "pct", "percent"]):
                record[span_classes.split()[-1] if span_classes.split() else "stat"] = span_text

        if record.get("name"):
            records.append(record)

    return records


def extract_detail_links(soup, section_type):
    """Extract links to detail pages for a given section."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(rf'/statistiques/{section_type}s?/', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            # Exclude the section index page itself
            if full_url.rstrip("/") != f"{BASE_URL}/statistiques/{section_type}s".rstrip("/"):
                links.add(full_url)
    return sorted(links)


def extract_pagination_links(soup, section_url):
    """Extract pagination links for multi-page listings."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'page=\d+|/page/\d+', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_detail_stats(soup, section_type, detail_url):
    """Extract detailed stats from a jockey/trainer/venue detail page."""
    records = []

    # Title/name
    name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 2:
            name = text
            break

    # Stats summary sections
    for section in soup.find_all(["div", "section", "dl"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["stat", "resume", "summary",
                                                   "palmares", "bilan",
                                                   "performance", "carriere",
                                                   "career", "bio", "info"]):
            text = section.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "source": "zone_turf",
                    "type": f"detail_{section_type}",
                    "name": name,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": detail_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Tables on detail page (recent results, stats breakdown)
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
                "source": "zone_turf",
                "type": f"detail_{section_type}_table",
                "name": name,
                "url": detail_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Embedded JSON
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, (dict, list)):
                records.append({
                    "source": "zone_turf",
                    "type": "embedded_json",
                    "section": section_type,
                    "name": name,
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

def scrape_section_index(page, section):
    """Scrape a section index page (jockeys, entraineurs, hippodromes)."""
    section_name = section["name"]
    section_type = section["type"]
    cache_file = os.path.join(CACHE_DIR, f"index_{section_name}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}{section['url']}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"index_{section_name}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_stats_table(soup, section_type))
    records.extend(extract_stats_cards(soup, section_type))

    detail_links = extract_detail_links(soup, section_type)
    pagination_links = extract_pagination_links(soup, url)

    result = {
        "records": records,
        "detail_links": detail_links,
        "pagination_links": pagination_links,
    }
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def scrape_section_page(page, page_url, section_type):
    """Scrape a paginated page of a section."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', page_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"page_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, page_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_stats_table(soup, section_type))
    records.extend(extract_stats_cards(soup, section_type))

    detail_links = extract_detail_links(soup, section_type)

    result = {"records": records, "detail_links": detail_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def scrape_detail_page(page, detail_url, section_type):
    """Scrape a detail page (jockey/trainer/venue)."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', detail_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, detail_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = extract_detail_stats(soup, section_type, detail_url)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 152 — Zone-Turf Stats Scraper (jockeys, entraineurs, hippodromes)"
    )
    parser.add_argument("--sections", type=str, nargs="*", default=None,
                        help="Sections to scrape: jockeys, entraineurs, hippodromes (default=all)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-details", type=int, default=30,
                        help="Max detail pages per section (default=30)")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="Max pagination pages per section (default=10)")
    args = parser.parse_args()

    sections = STAT_SECTIONS
    if args.sections:
        sections = [s for s in STAT_SECTIONS if s["name"] in args.sections]

    log.info("=" * 60)
    log.info("SCRIPT 152 — Zone-Turf Stats Scraper (Playwright)")
    log.info("  Sections: %s", [s["name"] for s in sections])
    log.info("  Max details/section: %d, Max pages/section: %d",
             args.max_details, args.max_pages)
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    processed_urls = set(checkpoint.get("processed_urls", []))
    if args.resume:
        log.info("  Already processed: %d URLs", len(processed_urls))

    output_file = os.path.join(OUTPUT_DIR, "zone_turf_stats.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="fr-FR", timezone="Europe/Paris"
        )
        log.info("Browser launched (headless Chromium, locale=fr-FR)")

        first_nav = True
        total_records = 0
        section_count = 0

        for section in sections:
            section_name = section["name"]
            section_type = section["type"]
            section_url = f"{BASE_URL}{section['url']}"

            log.info("  === Section: %s ===", section_name)

            # Scrape index page
            result = scrape_section_index(page, section)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            all_detail_links = set()
            if result:
                records = result.get("records", [])
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

                all_detail_links.update(result.get("detail_links", []))

                # Scrape pagination pages
                for i, page_url in enumerate(result.get("pagination_links", [])[:args.max_pages]):
                    if page_url in processed_urls:
                        continue
                    log.info("    Page %d: %s", i + 1, page_url[:80])
                    page_result = scrape_section_page(page, page_url, section_type)
                    if page_result:
                        for rec in page_result.get("records", []):
                            append_jsonl(output_file, rec)
                            total_records += 1
                        all_detail_links.update(page_result.get("detail_links", []))
                    processed_urls.add(page_url)
                    smart_pause(2.0, 1.0)

            # Scrape detail pages
            detail_links = sorted(all_detail_links)[:args.max_details]
            for i, detail_url in enumerate(detail_links):
                if detail_url in processed_urls:
                    continue
                log.info("    Detail %d/%d: %s", i + 1, len(detail_links), detail_url[-60:])
                detail_records = scrape_detail_page(page, detail_url, section_type)
                if detail_records:
                    for rec in detail_records:
                        append_jsonl(output_file, rec)
                        total_records += 1
                processed_urls.add(detail_url)
                smart_pause(2.0, 1.0)

                if (i + 1) % 10 == 0:
                    save_checkpoint(CHECKPOINT_FILE, {
                        "processed_urls": sorted(processed_urls),
                        "total_records": total_records,
                    })

            section_count += 1
            log.info("  Section %s done: %d records so far", section_name, total_records)

        save_checkpoint(CHECKPOINT_FILE, {
            "processed_urls": sorted(processed_urls),
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: %d sections, %d records -> %s",
                 section_count, total_records, output_file)
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
