#!/usr/bin/env python3
"""
Script 103 — Scraping Tiercé Magazine (Playwright version)
Source : tierce-magazine.com
Collecte : pronostics, prédictions, analyses courses hippiques
CRITIQUE pour : Predictions, Expert Tips, Pronostics

URLs réelles :
  /pronostics/ -> liste des pronostics du jour
  /pronostics/{date}/ -> pronostics par date
  /pronostics-courses-hippiques/ -> index pronostics

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import os
import sys
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "103_tierce_magazine"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("103_tierce_magazine")

MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

BASE_URL = "https://www.tierce-magazine.com"


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


# ------------------------------------------------------------------
# Extraction helpers (BeautifulSoup-based)
# ------------------------------------------------------------------

def extract_pronostics(soup, date_str, source="tierce_magazine"):
    """Extract pronostic/prediction blocks from a page."""
    records = []
    # Look for pronostic containers (articles, cards, divs with relevant classes)
    for el in soup.find_all(["article", "div", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["pronostic", "prediction", "prono",
                                                   "tip", "selection", "pick",
                                                   "card", "course-item", "race-item"]):
            text = el.get_text(strip=True)
            if not text or len(text) < 10:
                continue

            record = {
                "date": date_str,
                "source": source,
                "type": "pronostic",
                "contenu": text[:3000],
                "classes_css": classes,
                "scraped_at": datetime.now().isoformat(),
            }

            # Extract race name / title
            title_el = el.find(["h2", "h3", "h4", "h5", "strong"])
            if title_el:
                record["titre"] = title_el.get_text(strip=True)

            # Extract horse names (numbered selections)
            selections = []
            for li in el.find_all("li"):
                li_text = li.get_text(strip=True)
                if li_text and len(li_text) < 200:
                    selections.append(li_text)
            if selections:
                record["selections"] = selections

            # Extract links to detailed pages
            for a in el.find_all("a", href=True):
                href = a["href"]
                if any(kw in href.lower() for kw in ["pronostic", "course", "reunion"]):
                    record["detail_url"] = href if href.startswith("http") else f"{BASE_URL}{href}"
                    break

            records.append(record)
    return records


def extract_reunions(soup, date_str, source="tierce_magazine"):
    """Extract reunion/meeting blocks with hippodrome info."""
    records = []
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["reunion", "meeting", "hippodrome",
                                                   "programme", "race-card"]):
            record = {
                "date": date_str,
                "source": source,
                "type": "reunion",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong"])
            if title:
                record["hippodrome"] = title.get_text(strip=True)

            # Discipline detection
            text = div.get_text(strip=True)
            disc_match = re.search(r'(trot attelé|trot monté|plat|haies|steeple|cross)',
                                   text, re.I)
            if disc_match:
                record["discipline"] = disc_match.group(1)

            record["contenu"] = text[:1000]
            if len(text) > 10:
                records.append(record)
    return records


def extract_expert_analyses(soup, date_str, source="tierce_magazine"):
    """Extract expert commentary and analysis sections."""
    records = []
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "analyse", "editorial",
                                                   "expert", "avis", "conseil",
                                                   "resume", "description"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "analyse_expert",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_table_data(soup, date_str, source="tierce_magazine"):
    """Extract tabular data (partants, résultats, cotes)."""
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "table_data",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    record[key] = cell
                records.append(record)
    return records


def extract_page_links(soup):
    """Extract links to pronostic detail pages and course pages."""
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(kw in href.lower() for kw in ["/pronostic", "/course", "/reunion",
                                               "/programme", "/partants"]):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.append(full_url)
    return list(set(links))


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_pronostics_page(page, url, date_str):
    """Scrape a pronostics listing page."""
    cache_key = re.sub(r'[^a-zA-Z0-9]', '_', url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"prono_{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"prono_{date_str}_{cache_key[:30]}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract all data types
    records.extend(extract_pronostics(soup, date_str))
    records.extend(extract_reunions(soup, date_str))
    records.extend(extract_expert_analyses(soup, date_str))
    records.extend(extract_table_data(soup, date_str))

    # Get detail page links
    detail_links = extract_page_links(soup)

    result = {"records": records, "detail_links": detail_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_detail_page(page, detail_url, date_str):
    """Scrape a pronostic detail page for deeper data."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', detail_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, detail_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Title / race name
    nom_course = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            nom_course = text
            break

    # Conditions from page text
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(r'(\d[\d\s]*)\s*m(?:ètre)?', page_text)
    if dist_match:
        conditions["distance_m"] = dist_match.group(1).replace(" ", "")

    terrain_match = re.search(
        r'(terrain|piste|sol)\s*:?\s*(bon|souple|très souple|collant|lourd|léger|sec|'
        r'pénétrant|très léger)',
        page_text, re.I
    )
    if terrain_match:
        conditions["etat_terrain"] = terrain_match.group(2).strip()

    disc_match = re.search(r'(trot attelé|trot monté|plat|haies|steeple|cross)',
                           page_text, re.I)
    if disc_match:
        conditions["discipline"] = disc_match.group(1)

    # Extract pronostics + analyses from detail page
    records.extend(extract_pronostics(soup, date_str))
    records.extend(extract_expert_analyses(soup, date_str))
    records.extend(extract_table_data(soup, date_str))

    # Enrich records with course context
    for rec in records:
        if nom_course:
            rec["nom_course"] = nom_course
        if conditions:
            rec["conditions"] = conditions
        rec["url_source"] = detail_url

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Script 103 — Tiercé Magazine Scraper (pronostics, prédictions, analyses)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), défaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 103 — Tiercé Magazine Scraper (Playwright)")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "tierce_magazine.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(pw)
        log.info("Browser launched (headless Chromium)")

        # Accept cookies on first navigation
        first_nav = True

        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Try multiple URL patterns for the pronostics page
            urls_to_try = [
                f"{BASE_URL}/pronostics/{date_str}/",
                f"{BASE_URL}/pronostics-courses-hippiques/{date_str}/",
                f"{BASE_URL}/programme/{date_str}/",
            ]

            result = None
            for url in urls_to_try:
                result = scrape_pronostics_page(page, url, date_str)
                if result and result.get("records"):
                    break

                if first_nav:
                    accept_cookies(page)
                    first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape detail pages (limit to 15 per day)
                for detail_url in result.get("detail_links", [])[:15]:
                    detail_records = scrape_detail_page(page, detail_url, date_str)
                    if detail_records:
                        records.extend(detail_records)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | jours={day_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"TERMINÉ: {day_count} jours, {total_records} records → {output_file}")
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
