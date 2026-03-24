#!/usr/bin/env python3
"""
Script 148 — LeTurf Consensus Scraper (Playwright)
Source : leturf.fr
Collecte : Pronostics communautaires, consensus picks, tendances
URL patterns :
  /pronostics/                  -> page pronostics du jour
  /pronostics/{date}/           -> pronostics par date
  /tendances/                   -> tendances communautaires
  /consensus/{reunion}/         -> consensus par reunion
CRITIQUE pour : Community Consensus, Tendances, Pronostics FR

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

SCRIPT_NAME = "148_leturf_consensus"
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

log = setup_logging("148_leturf_consensus")

BASE_URL = "https://www.leturf.fr"
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

def extract_consensus_picks(soup, date_str):
    """Extract consensus/community picks from pronostics page."""
    records = []
    for el in soup.find_all(["div", "section", "article", "tr"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["prono", "consensus", "pick",
                                                   "selection", "cheval",
                                                   "favori", "vote",
                                                   "communautaire", "tip"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "leturf",
                    "type": "consensus_pick",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to extract horse name (often in bold/strong)
                name_el = el.find(["strong", "b", "a", "h3", "h4"])
                if name_el:
                    record["cheval"] = name_el.get_text(strip=True)

                # Try to extract percentage/vote count
                pct_match = re.search(r'(\d{1,3})\s*%', text)
                if pct_match:
                    record["pourcentage"] = pct_match.group(1)

                vote_match = re.search(r'(\d+)\s*(?:votes?|avis)', text, re.I)
                if vote_match:
                    record["votes"] = vote_match.group(1)

                records.append(record)
    return records


def extract_tendances(soup, date_str):
    """Extract tendance (trend) data from community pages."""
    records = []
    for el in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["tendance", "trend", "evolution",
                                                   "hausse", "baisse",
                                                   "mouvement", "cote"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "leturf",
                    "type": "tendance",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Direction
                if any(kw in text.lower() for kw in ["hausse", "monte", "up", "+"]):
                    record["direction"] = "hausse"
                elif any(kw in text.lower() for kw in ["baisse", "descend", "down", "-"]):
                    record["direction"] = "baisse"
                records.append(record)
    return records


def extract_reunion_links(soup):
    """Extract links to reunion/race pages."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(reunion|course|pronostic|r\d+)', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_pronostic_table(soup, date_str, page_url=""):
    """Extract structured pronostic data from tables."""
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
                "date": date_str,
                "source": "leturf",
                "type": "pronostic_table",
                "url": page_url,
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


def extract_embedded_json(soup, date_str):
    """Extract JSON-LD and embedded JSON data."""
    records = []
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string or "")
            if data:
                records.append({
                    "date": date_str,
                    "source": "leturf",
                    "type": "json_ld",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "date": date_str,
                    "source": "leturf",
                    "type": "embedded_json",
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

def scrape_pronostics_day(page, date_str):
    """Scrape the LeTurf pronostics page for a date."""
    cache_file = os.path.join(CACHE_DIR, f"pronostics_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # LeTurf uses DD-MM-YYYY format in URLs
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = date_obj.strftime("%d-%m-%Y")
    url = f"{BASE_URL}/pronostics/{url_date}/"
    html = navigate_with_retry(page, url)
    if not html:
        # Try alternate URL format
        url = f"{BASE_URL}/pronostics/{date_str}/"
        html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"pronostics_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str))
    records.extend(extract_consensus_picks(soup, date_str))
    records.extend(extract_tendances(soup, date_str))
    records.extend(extract_pronostic_table(soup, date_str, page_url=url))

    reunion_links = extract_reunion_links(soup)

    result = {"records": records, "reunion_links": reunion_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def scrape_reunion_detail(page, reunion_url, date_str):
    """Scrape a specific reunion/course page for consensus data."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', reunion_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"reunion_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, reunion_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Course title
    course_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            course_name = text
            break

    records.extend(extract_embedded_json(soup, date_str))
    records.extend(extract_consensus_picks(soup, date_str))
    records.extend(extract_tendances(soup, date_str))
    records.extend(extract_pronostic_table(soup, date_str, page_url=reunion_url))

    # Attach course name
    for rec in records:
        rec["course_name"] = course_name
        rec["reunion_url"] = reunion_url

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 148 — LeTurf Consensus Scraper (pronostics communautaires)"
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
    log.info("SCRIPT 148 — LeTurf Consensus Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "leturf_consensus.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="fr-FR", timezone="Europe/Paris"
        )
        log.info("Browser launched (headless Chromium, locale=fr-FR)")

        first_nav = True
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")
            result = scrape_pronostics_day(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape reunion detail pages
                for reunion_url in result.get("reunion_links", [])[:15]:
                    detail = scrape_reunion_detail(page, reunion_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(2.0, 1.0)

                for rec in records:
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
            smart_pause(1.5, 0.8)

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
