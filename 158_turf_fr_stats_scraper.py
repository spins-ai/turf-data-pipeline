#!/usr/bin/env python3
"""
Script 158 — Turf-FR Stats Scraper (Playwright)
Source : turf-fr.com/stats
Collecte : Stats detaillees par hippodrome, jockey, entraineur
URL patterns :
  /stats/                          -> page stats principale
  /stats/hippodromes/              -> stats par hippodrome
  /stats/hippodromes/{nom}/        -> detail hippodrome
  /stats/jockeys/                  -> classement jockeys
  /stats/jockeys/{nom}/            -> detail jockey
  /stats/entraineurs/              -> classement entraineurs
  /stats/entraineurs/{nom}/        -> detail entraineur
  /stats/proprietaires/            -> classement proprietaires
CRITIQUE pour : Stats FR detaillees, analyse par hippodrome/jockey/entraineur

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

SCRIPT_NAME = "158_turf_fr_stats"
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
from utils.html_parsing import extract_embedded_json_data
from utils.html_parsing import extract_scraper_data_attributes

log = setup_logging("158_turf_fr_stats")

BASE_URL = "https://www.turf-fr.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Stats sections to scrape
STATS_SECTIONS = [
    {"path": "/stats/", "type": "stats_index", "label": "Index stats"},
    {"path": "/stats/hippodromes/", "type": "stats_hippodromes", "label": "Stats hippodromes"},
    {"path": "/stats/jockeys/", "type": "stats_jockeys", "label": "Classement jockeys"},
    {"path": "/stats/entraineurs/", "type": "stats_entraineurs", "label": "Classement entraineurs"},
    {"path": "/stats/proprietaires/", "type": "stats_proprietaires", "label": "Classement proprietaires"},
    {"path": "/stats/chevaux/", "type": "stats_chevaux", "label": "Stats chevaux"},
    {"path": "/stats/plat/", "type": "stats_plat", "label": "Stats courses plat"},
    {"path": "/stats/obstacles/", "type": "stats_obstacles", "label": "Stats courses obstacles"},
    {"path": "/stats/trot/", "type": "stats_trot", "label": "Stats courses trot"},
    {"path": "/stats/records/", "type": "stats_records", "label": "Records"},
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

def extract_stats_tables(soup, section_type, section_label):
    """Extract statistics tables."""
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        table_title = ""
        prev = table.find_previous(["h2", "h3", "h4", "caption", "strong"])
        if prev:
            table_title = prev.get_text(strip=True)

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "source": "turf_fr_stats",
                "type": section_type,
                "section_label": section_label,
                "table_title": table_title,
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


def extract_ranking_blocks(soup, section_type, section_label):
    """Extract ranking/classement blocks (non-table layouts)."""
    records = []
    for el in soup.find_all(["div", "section", "article", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["classement", "ranking", "rank",
                                                   "stats-row", "stats-item",
                                                   "jockey", "entraineur",
                                                   "hippodrome", "top-",
                                                   "palmares", "bilan"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "source": "turf_fr_stats",
                    "type": section_type,
                    "section_label": section_label,
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                rank_match = re.search(r'^(\d{1,4})[.\s)\-]', text)
                if rank_match:
                    record["rang"] = rank_match.group(1)
                wins_match = re.search(r'(\d+)\s*(?:victoires?|wins?|V)', text, re.I)
                if wins_match:
                    record["victoires"] = wins_match.group(1)
                pct_match = re.search(r'(\d{1,3}[.,]\d{1,2})\s*%', text)
                if pct_match:
                    record["pourcentage"] = pct_match.group(1)
                records.append(record)
    return records


def extract_hippodrome_stats(soup, section_type, section_label):
    """Extract hippodrome-specific statistics."""
    records = []
    for el in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["hippodrome", "course", "piste",
                                                   "track", "venue", "circuit",
                                                   "fiche-hippodrome"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "source": "turf_fr_stats",
                    "type": f"{section_type}_hippodrome",
                    "section_label": section_label,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract hippodrome name
                name_el = el.find(["h2", "h3", "h4", "strong", "a"])
                if name_el:
                    record["hippodrome_nom"] = name_el.get_text(strip=True)
                # Extract location
                loc_match = re.search(r'(?:ville|lieu|departement)\s*:?\s*([^,\n]{3,50})',
                                      text, re.I)
                if loc_match:
                    record["localisation"] = loc_match.group(1).strip()
                records.append(record)
    return records


def extract_detail_links(soup, section_path):
    """Extract detail page links within a stats section."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/stats/" in href and href != section_path:
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            # Avoid going back up to parent sections
            if len(href.rstrip("/").split("/")) > len(section_path.rstrip("/").split("/")):
                links.add(full_url)
    return sorted(links)


def extract_stats_summary(soup, section_type, section_label):
    """Extract summary statistics blocks."""
    records = []
    for el in soup.find_all(["div", "section", "span"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stats", "stat-", "summary",
                                                   "kpi", "metric", "count",
                                                   "total", "average", "moyenne",
                                                   "bilan", "resultat"]):
            text = el.get_text(strip=True)
            if text and 2 < len(text) < 500:
                record = {
                    "source": "turf_fr_stats",
                    "type": f"{section_type}_summary",
                    "section_label": section_label,
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                num_match = re.search(r'(\d[\d\s.,]*\d|\d+)', text)
                if num_match:
                    record["valeur"] = num_match.group(1).strip()
                records.append(record)
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_stats_section(page, section):
    """Scrape a single stats section."""
    path = section["path"]
    section_type = section["type"]
    section_label = section["label"]

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', path)
    cache_file = os.path.join(CACHE_DIR, f"section_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}{path}"
    log.info("Scraping stats section: %s (%s)", section_label, url)

    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"{url_hash}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "turf_fr_stats", date_str=section_type))
    records.extend(extract_scraper_data_attributes(soup, "turf_fr_stats", date_str=section_type))
    records.extend(extract_stats_tables(soup, section_type, section_label))
    records.extend(extract_ranking_blocks(soup, section_type, section_label))
    records.extend(extract_hippodrome_stats(soup, section_type, section_label))
    records.extend(extract_stats_summary(soup, section_type, section_label))

    detail_links = extract_detail_links(soup, path)

    result = {"records": records, "detail_links": detail_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_detail_page(page, detail_url, parent_type, parent_label):
    """Scrape a detail page (e.g., individual hippodrome, jockey, entraineur)."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', detail_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, detail_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    page_title = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            page_title = text
            break

    detail_type = f"{parent_type}_detail"
    detail_label = f"{parent_label} > {page_title}"

    records.extend(extract_embedded_json_data(soup, "turf_fr_stats", date_str=detail_type))
    records.extend(extract_scraper_data_attributes(soup, "turf_fr_stats", date_str=detail_type))
    records.extend(extract_stats_tables(soup, detail_type, detail_label))
    records.extend(extract_ranking_blocks(soup, detail_type, detail_label))
    records.extend(extract_hippodrome_stats(soup, detail_type, detail_label))
    records.extend(extract_stats_summary(soup, detail_type, detail_label))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 158 — Turf-FR Stats Scraper (hippodromes, jockeys, entraineurs)"
    )
    parser.add_argument("--max-detail-pages", type=int, default=30,
                        help="Max detail pages per section (default=30)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 158 — Turf-FR Stats Scraper (Playwright)")
    log.info("  Sections: %d", len(STATS_SECTIONS))
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    processed_sections = set(checkpoint.get("processed_sections", []))
    total_records = checkpoint.get("total_records", 0)

    output_file = os.path.join(OUTPUT_DIR, "turf_fr_stats_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="fr-FR", timezone="Europe/Paris"
        )
        log.info("Browser launched (headless Chromium, locale=fr-FR)")

        first_nav = True
        section_count = 0

        for section in STATS_SECTIONS:
            section_key = section["path"]
            if args.resume and section_key in processed_sections:
                log.info("  Skipping already processed: %s", section["label"])
                continue

            result = scrape_stats_section(page, section)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape detail pages
                detail_links = result.get("detail_links", [])
                for detail_url in detail_links[:args.max_detail_pages]:
                    detail_records = scrape_detail_page(
                        page, detail_url, section["type"], section["label"]
                    )
                    if detail_records:
                        records.extend(detail_records)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

                log.info("  %s: %d records", section["label"], len(records))

            processed_sections.add(section_key)
            section_count += 1

            save_checkpoint(CHECKPOINT_FILE, {
                "processed_sections": sorted(processed_sections),
                "total_records": total_records,
            })

            smart_pause(2.0, 1.0)

        save_checkpoint(CHECKPOINT_FILE, {
            "processed_sections": sorted(processed_sections),
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
