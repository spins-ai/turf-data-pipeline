#!/usr/bin/env python3
"""
Script 157 — CanalTurf Statistiques Scraper (Playwright)
Source : canalturf.com/statistiques
Collecte : Stats avancees (pas les fiches chevaux) — classements jockeys,
           entraineurs, hippodromes, tendances, stats par type de course
URL patterns :
  /statistiques/                          -> page stats principale
  /statistiques/jockeys/                  -> classement jockeys
  /statistiques/entraineurs/              -> classement entraineurs
  /statistiques/hippodromes/              -> stats par hippodrome
  /statistiques/proprietaires/            -> classement proprietaires
  /statistiques/chevaux/classement/       -> classement chevaux
CRITIQUE pour : Stats FR avancees, classements, tendances

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

SCRIPT_NAME = "157_canalturf_stats"
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

log = setup_logging("157_canalturf_stats")

BASE_URL = "https://www.canalturf.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Stats sections to scrape
STATS_SECTIONS = [
    {"path": "/statistiques/", "type": "stats_index", "label": "Index stats"},
    {"path": "/statistiques/jockeys/", "type": "stats_jockeys", "label": "Classement jockeys"},
    {"path": "/statistiques/entraineurs/", "type": "stats_entraineurs", "label": "Classement entraineurs"},
    {"path": "/statistiques/hippodromes/", "type": "stats_hippodromes", "label": "Stats hippodromes"},
    {"path": "/statistiques/proprietaires/", "type": "stats_proprietaires", "label": "Classement proprietaires"},
    {"path": "/statistiques/chevaux/classement/", "type": "stats_chevaux_classement", "label": "Classement chevaux"},
    {"path": "/statistiques/tendances/", "type": "stats_tendances", "label": "Tendances"},
    {"path": "/statistiques/plat/", "type": "stats_plat", "label": "Stats plat"},
    {"path": "/statistiques/obstacles/", "type": "stats_obstacles", "label": "Stats obstacles"},
    {"path": "/statistiques/trot/", "type": "stats_trot", "label": "Stats trot"},
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
    """Extract statistics tables from CanalTurf stats pages."""
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        # Try to get table title
        table_title = ""
        prev = table.find_previous(["h2", "h3", "h4", "caption", "strong"])
        if prev:
            table_title = prev.get_text(strip=True)

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "source": "canalturf_stats",
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
                                                   "player", "jockey-row",
                                                   "trainer-row", "top-"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "source": "canalturf_stats",
                    "type": section_type,
                    "section_label": section_label,
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract rank number
                rank_match = re.search(r'^(\d{1,4})[.\s)\-]', text)
                if rank_match:
                    record["rang"] = rank_match.group(1)
                # Extract win stats
                wins_match = re.search(r'(\d+)\s*(?:victoires?|wins?|V)', text, re.I)
                if wins_match:
                    record["victoires"] = wins_match.group(1)
                # Extract percentage
                pct_match = re.search(r'(\d{1,3}[.,]\d{1,2})\s*%', text)
                if pct_match:
                    record["pourcentage"] = pct_match.group(1)
                records.append(record)
    return records


def extract_stats_summary(soup, section_type, section_label):
    """Extract summary statistics blocks."""
    records = []
    for el in soup.find_all(["div", "section", "span"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stats", "stat-", "summary",
                                                   "kpi", "metric", "count",
                                                   "total", "average", "moyenne"]):
            text = el.get_text(strip=True)
            if text and 2 < len(text) < 500:
                record = {
                    "source": "canalturf_stats",
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


def extract_sub_links(soup, section_path):
    """Extract sub-navigation links within stats section."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/statistiques/" in href and href != section_path:
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_embedded_json_data(soup, section_type):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "source": "canalturf_stats",
                    "type": f"{section_type}_json",
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass
    return records


def extract_data_attributes(soup, section_type):
    """Extract data-* attributes from stats elements."""
    records = []
    keywords = ["jockey", "entraineur", "trainer", "cheval", "horse",
                "hippodrome", "stat", "rank", "score", "win", "course"]
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in keywords)
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": "canalturf_stats",
                "type": f"{section_type}_data_attrs",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_stats_section(page, section, output_file):
    """Scrape a single stats section and its sub-pages."""
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

    records.extend(extract_embedded_json_data(soup, section_type))
    records.extend(extract_data_attributes(soup, section_type))
    records.extend(extract_stats_tables(soup, section_type, section_label))
    records.extend(extract_ranking_blocks(soup, section_type, section_label))
    records.extend(extract_stats_summary(soup, section_type, section_label))

    sub_links = extract_sub_links(soup, path)

    result = {"records": records, "sub_links": sub_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_sub_page(page, sub_url, parent_type, parent_label):
    """Scrape a sub-page within a stats section."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', sub_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"sub_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, sub_url)
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

    sub_type = f"{parent_type}_detail"
    records.extend(extract_embedded_json_data(soup, sub_type))
    records.extend(extract_data_attributes(soup, sub_type))
    records.extend(extract_stats_tables(soup, sub_type, f"{parent_label} > {page_title}"))
    records.extend(extract_ranking_blocks(soup, sub_type, f"{parent_label} > {page_title}"))
    records.extend(extract_stats_summary(soup, sub_type, f"{parent_label} > {page_title}"))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 157 — CanalTurf Statistiques Scraper (classements, tendances)"
    )
    parser.add_argument("--max-sub-pages", type=int, default=20,
                        help="Max sub-pages per section (default=20)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 157 — CanalTurf Statistiques Scraper (Playwright)")
    log.info("  Sections: %d", len(STATS_SECTIONS))
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    processed_sections = set(checkpoint.get("processed_sections", []))
    total_records = checkpoint.get("total_records", 0)

    output_file = os.path.join(OUTPUT_DIR, "canalturf_stats_data.jsonl")

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

            result = scrape_stats_section(page, section, output_file)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape sub-pages
                sub_links = result.get("sub_links", [])
                for sub_url in sub_links[:args.max_sub_pages]:
                    sub_records = scrape_sub_page(
                        page, sub_url, section["type"], section["label"]
                    )
                    if sub_records:
                        records.extend(sub_records)
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
