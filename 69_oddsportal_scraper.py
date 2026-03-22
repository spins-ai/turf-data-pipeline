#!/usr/bin/env python3
"""
Script 69 — Scraping OddsPortal.com (Playwright version)
Source : oddsportal.com/horse-racing/
Collecte : cotes historiques multi-bookmakers, mouvements de cotes, odds comparison
CRITIQUE pour : Odds Model, Market Efficiency, Value Detection (etape 7E)

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "69_oddsportal"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("69_oddsportal")

MAX_RETRIES = 3

BASE_URL = "https://www.oddsportal.com"








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
# Scraping functions (BeautifulSoup-based, fed from page.content())
# ------------------------------------------------------------------

def scrape_daily_results(page, date_str):
    """Scraper les resultats et cotes OddsPortal pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # OddsPortal utilise le format YYYYMMDD dans les URLs de resultats
    d = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = d.strftime("%Y%m%d")
    url = f"{BASE_URL}/matches/horse-racing/{url_date}/"

    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # --- Extraire les evenements de course ---
    for row in soup.find_all("div", class_=True):
        classes = " ".join(row.get("class", []))
        if any(kw in classes.lower() for kw in ["event", "match", "deactivate", "table-participant"]):
            record = {
                "date": date_str,
                "source": "oddsportal",
                "type": "event",
                "scraped_at": datetime.now().isoformat(),
            }

            # Nom de l'evenement / course
            name_el = row.find(["a", "span", "p"], class_=lambda c: c and any(
                kw in c for kw in ["name", "event", "participant"]))
            if name_el:
                record["nom_event"] = name_el.get_text(strip=True)
                if name_el.name == "a" and name_el.get("href"):
                    record["url_detail"] = name_el["href"]

            # Pays / Hippodrome
            country_el = row.find(["span", "a"], class_=lambda c: c and "country" in c) if row else None
            if country_el:
                record["pays"] = country_el.get_text(strip=True)

            records.append(record)

    # --- Extraire les tables de cotes ---
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                rec = {
                    "date": date_str,
                    "source": "oddsportal",
                    "type": "odds_row",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    rec[key] = cell

                # Extraire les cotes numeriques
                for cell in cells:
                    odds_match = re.search(r'(\d+\.\d+)', cell)
                    if odds_match:
                        rec["best_odds"] = odds_match.group(1)
                        break

                records.append(rec)

    # --- Extraire les bookmakers et cotes depuis data-attributes ---
    for el in soup.find_all(attrs={"data-odd": True}):
        records.append({
            "date": date_str,
            "source": "oddsportal",
            "type": "bookmaker_odd",
            "odd_value": el.get("data-odd"),
            "bookmaker": el.get("data-bk", ""),
            "text": el.get_text(strip=True),
            "scraped_at": datetime.now().isoformat(),
        })

    # --- Extraire les odds history (mouvements) ---
    for div in soup.find_all("div", class_=lambda c: c and any(
            kw in c for kw in ["odds-movement", "history", "graph", "closing"])):
        text = div.get_text(strip=True)
        if text and len(text) > 3:
            records.append({
                "date": date_str,
                "source": "oddsportal",
                "type": "odds_movement",
                "contenu": text[:500],
                "scraped_at": datetime.now().isoformat(),
            })

    # Sauvegarder cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_event_detail(page, event_url, date_str):
    """Scraper le detail d'un evenement pour les cotes comparees multi-bookmakers."""
    if not event_url.startswith("http"):
        event_url = f"{BASE_URL}{event_url}"

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', event_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, event_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    details = []

    # Nom de la course
    nom_course = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            nom_course = text
            break

    # Extraire comparaison de cotes par bookmaker
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            detail = {
                "date": date_str,
                "source": "oddsportal",
                "type": "odds_comparison",
                "nom_course": nom_course,
                "url_event": event_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                detail[key] = cell
            details.append(detail)

    # Cotes d'ouverture / cloture
    for span in soup.find_all(["span", "div"], class_=lambda c: c and any(
            kw in c for kw in ["opening", "closing", "average", "highest"])):
        text = span.get_text(strip=True)
        odds_match = re.search(r'(\d+\.\d+)', text)
        if odds_match:
            details.append({
                "date": date_str,
                "source": "oddsportal",
                "type": "odds_summary",
                "nom_course": nom_course,
                "label": " ".join(span.get("class", [])),
                "odds_value": odds_match.group(1),
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False, indent=2)

    return details


def main():
    parser = argparse.ArgumentParser(
        description="Script 69 — OddsPortal Scraper (Playwright, cotes historiques multi-bookmakers)")
    parser.add_argument("--start", type=str, default="2020-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--detail", action="store_true", default=False,
                        help="Scraper aussi le detail de chaque evenement")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 69 — OddsPortal Scraper (Playwright)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Detail events : {args.detail}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "oddsportal_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(pw, locale="en-US", timezone="America/New_York")
        log.info("Browser launched (headless Chromium)")

        # Accept cookies on first navigation
        first_nav = True

        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            records = scrape_daily_results(page, date_str)

            if first_nav and records is not None:
                accept_cookies(page)
                first_nav = False

            if records:
                # Optionnel : scraper les details de chaque evenement
                if args.detail:
                    event_urls = [r.get("url_detail") for r in records
                                  if r.get("url_detail")]
                    for eurl in set(filter(None, event_urls)):
                        detail = scrape_event_detail(page, eurl, date_str)
                        if detail:
                            records.extend(detail)
                        smart_pause(2.0, 1.0)

                # Ecrire en JSONL
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | jours={day_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

            current += timedelta(days=1)
            smart_pause(1.5, 0.8)

        save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"TERMINE: {day_count} jours, {total_records} records -> {output_file}")
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
