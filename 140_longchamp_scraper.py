#!/usr/bin/env python3
"""
Script 140 — Paris Longchamp Scraper (Playwright)
Source : parislongchamp.com
Collecte : FR prestige events (Prix de l'Arc de Triomphe, Moulin, Opéra, etc.),
           race conditions, results, programme des réunions
URL patterns :
  /courses/resultats/           -> résultats
  /courses/programme/           -> programme / race cards
  /evenements/qatar-prix-arc/   -> Arc de Triomphe
  /evenements/                  -> événements prestige
CRITIQUE pour : FR Group 1 data, Arc de Triomphe, Moulin, Opéra, Saint-Cloud

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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.playwright import launch_browser, accept_cookies
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

SCRIPT_NAME = "140_longchamp"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

log = setup_logging("140_longchamp")

BASE_URL = "https://www.parislongchamp.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Key prestige event paths
EVENT_PATHS = [
    "/evenements/qatar-prix-arc-de-triomphe/",
    "/evenements/prix-du-moulin-de-longchamp/",
    "/evenements/prix-de-l-opera/",
    "/evenements/prix-du-jockey-club/",
    "/evenements/prix-de-diane/",
    "/evenements/prix-saint-alary/",
    "/evenements/grand-prix-de-paris/",
    "/evenements/",
]


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

def extract_race_links(soup):
    """Extract links to individual race cards / results from a listing page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(course|resultat|programme|race|reunion)/', href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            links.add(full_url)
    return sorted(links)


def extract_race_conditions(soup, date_str, race_url=""):
    """Extract race conditions: distance, terrain, catégorie, dotation."""
    records = []
    for el in soup.find_all(["div", "section", "article", "dl"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["course-info", "race-info",
                                                   "conditions", "detail-course",
                                                   "info-course", "race-detail",
                                                   "programme-detail"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "longchamp",
                    "type": "race_conditions",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Distance (metres)
                dist_match = re.search(
                    r'(\d[\d\s]*)\s*m(?:[eè]tres?)?|(\d+)\s*m\b',
                    text, re.I
                )
                if dist_match:
                    record["distance_raw"] = dist_match.group(0).strip()

                # Terrain (going equivalent)
                terrain_match = re.search(
                    r'(très? l[ée]ger|l[ée]ger|bon|bon souple|souple|'
                    r'tr[eè]s souple|lourd|coll[ae]nt|'
                    r'firm|good|soft|heavy)',
                    text, re.I
                )
                if terrain_match:
                    record["terrain"] = terrain_match.group(1).strip()

                # Catégorie (Group, Listed, etc.)
                cat_match = re.search(
                    r'(groupe?\s*[iI1]{1,3}|group\s*[123]|listed?|handicap|'
                    r'conditions?|claimer|r[ée]clamer)',
                    text, re.I
                )
                if cat_match:
                    record["categorie"] = cat_match.group(1).strip()

                # Dotation (prize money)
                prize_match = re.search(r'(\d[\d\s,.]*)\s*[€$]|[€$]\s*([\d\s,.]+)', text)
                if prize_match:
                    record["dotation"] = prize_match.group(0).strip()

                # Discipline
                disc_match = re.search(
                    r'(plat|haies|steeplechase|cross[- ]country|trot)',
                    text, re.I
                )
                if disc_match:
                    record["discipline"] = disc_match.group(1).strip()

                records.append(record)

    return records


def extract_runners_table(soup, date_str, race_url="", race_name=""):
    """Extract runner data from race card or result tables."""
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
                "date": date_str,
                "source": "longchamp",
                "type": "runner",
                "race_name": race_name,
                "url": race_url,
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


def extract_runner_cards(soup, date_str, race_url="", race_name=""):
    """Extract runner data from card-based layouts (non-table)."""
    records = []
    for el in soup.find_all(["div", "article", "li", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["partant", "cheval", "runner",
                                                   "entry", "participant",
                                                   "engag", "selection"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "longchamp",
                    "type": "runner_card",
                    "race_name": race_name,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                heading = el.find(["h2", "h3", "h4", "strong", "a"])
                if heading:
                    record["cheval_nom"] = heading.get_text(strip=True)

                for span in el.find_all(["span", "small", "p", "div"], class_=True):
                    sc = " ".join(span.get("class", []))
                    st = span.get_text(strip=True)
                    if any(k in sc.lower() for k in ["jockey", "cavalier"]):
                        record["jockey"] = st
                    elif any(k in sc.lower() for k in ["entraineur", "trainer"]):
                        record["entraineur"] = st
                    elif any(k in sc.lower() for k in ["propri", "owner"]):
                        record["proprietaire"] = st
                    elif any(k in sc.lower() for k in ["poids", "weight"]):
                        record["poids"] = st

                for attr_name, attr_val in el.attrs.items():
                    if attr_name.startswith("data-"):
                        clean = attr_name.replace("data-", "").replace("-", "_")
                        record[clean] = attr_val

                records.append(record)
    return records


def extract_results_data(soup, date_str, race_url=""):
    """Extract finishing positions and result data."""
    records = []
    for el in soup.find_all(["div", "section", "article", "ol", "ul"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["resultat", "result", "classement",
                                                   "arrivee", "rapport",
                                                   "dividende", "winner"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "longchamp",
                    "type": "result_block",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Extract rapports PMU (odds)
                rapport_matches = re.findall(r'(\d+[.,]\d+)\s*[€]?', text)
                if rapport_matches:
                    record["rapports_found"] = rapport_matches[:10]

                records.append(record)
    return records


def extract_event_info(soup, date_str, event_url=""):
    """Extract prestige event information (Arc, Moulin, etc.)."""
    records = []
    for el in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["evenement", "event", "festival",
                                                   "programme", "edition",
                                                   "presentation", "highlight"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 5000:
                record = {
                    "date": date_str,
                    "source": "longchamp",
                    "type": "event_info",
                    "contenu": text[:3000],
                    "classes_css": classes,
                    "url": event_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                headings = el.find_all(["h2", "h3", "h4"])
                if headings:
                    record["courses_vedettes"] = [
                        h.get_text(strip=True) for h in headings
                        if h.get_text(strip=True)
                    ]

                records.append(record)
    return records


def extract_embedded_json_data(soup, date_str):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                records.append({
                    "date": date_str,
                    "source": "longchamp",
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
                    "source": "longchamp",
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    return records


def extract_terrain_data(soup, date_str):
    """Extract terrain (going/ground) condition data."""
    records = []
    for el in soup.find_all(["div", "span", "p", "td", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["terrain", "going", "piste",
                                                   "surface", "etat-terrain",
                                                   "course-info"]):
            if text and 2 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "longchamp",
                    "type": "terrain_data",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                terrain_match = re.search(
                    r'(très? l[ée]ger|l[ée]ger|bon|bon souple|souple|'
                    r'très? souple|lourd|collant)',
                    text, re.I
                )
                if terrain_match:
                    record["terrain"] = terrain_match.group(1).strip()
                records.append(record)
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_programme_day(page, date_str):
    """Scrape the Longchamp programme page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"programme_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/courses/programme/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"programme_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_terrain_data(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str, race_url=url))
    records.extend(extract_runners_table(soup, date_str, race_url=url))
    records.extend(extract_runner_cards(soup, date_str, race_url=url))

    race_links = extract_race_links(soup)

    result = {"records": records, "race_links": race_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape an individual race card/result page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', race_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"race_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, race_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str, race_url=race_url))
    records.extend(extract_terrain_data(soup, date_str))
    records.extend(extract_runners_table(soup, date_str, race_url=race_url, race_name=race_name))
    records.extend(extract_runner_cards(soup, date_str, race_url=race_url, race_name=race_name))
    records.extend(extract_results_data(soup, date_str, race_url=race_url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results_day(page, date_str):
    """Scrape results page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"resultats_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/courses/resultats/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, date_str))
    records.extend(extract_race_conditions(soup, date_str, race_url=url))
    records.extend(extract_terrain_data(soup, date_str))
    records.extend(extract_runners_table(soup, date_str, race_url=url))
    records.extend(extract_results_data(soup, date_str, race_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_events(page, date_str):
    """Scrape prestige event pages (Arc, Moulin, Opera, etc.)."""
    all_records = []
    for path in EVENT_PATHS:
        cache_key = re.sub(r'[^a-zA-Z0-9]', '_', path)
        cache_file = os.path.join(CACHE_DIR, f"event_{cache_key}.json")
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                all_records.extend(json.load(f))
            continue

        url = f"{BASE_URL}{path}"
        html = navigate_with_retry(page, url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        records = []

        records.extend(extract_embedded_json_data(soup, date_str))
        records.extend(extract_event_info(soup, date_str, event_url=url))
        records.extend(extract_race_conditions(soup, date_str, race_url=url))

        # Follow race links from event page
        race_links = extract_race_links(soup)
        for race_url_link in race_links[:20]:
            detail = scrape_race_detail(page, race_url_link, date_str)
            if detail:
                records.extend(detail)
            smart_pause(1.5, 0.8)

        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        all_records.extend(records)
        smart_pause(2.0, 1.0)

    return all_records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 140 — Paris Longchamp Scraper (FR prestige events, Arc, Moulin)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=yesterday")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--events-only", action="store_true",
                        help="Only scrape event pages, skip daily programme/results")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
                else datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 140 — Paris Longchamp Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "longchamp_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="fr-FR", timezone="Europe/Paris"
        )
        log.info("Browser launched (headless Chromium, locale=fr-FR)")

        first_nav = True
        total_records = 0

        # Scrape prestige event pages once
        event_records = scrape_events(page, start_date.strftime("%Y-%m-%d"))
        if first_nav and event_records:
            accept_cookies(page)
            first_nav = False
        for rec in event_records:
            append_jsonl(output_file, rec)
            total_records += 1
        log.info("  Events: %d records", len(event_records))

        if args.events_only:
            log.info("  --events-only: skipping daily scraping")
        else:
            current = start_date
            day_count = 0

            while current <= end_date:
                if args.max_days and day_count >= args.max_days:
                    break

                date_str = current.strftime("%Y-%m-%d")

                # Scrape programme
                result = scrape_programme_day(page, date_str)

                if first_nav and result is not None:
                    accept_cookies(page)
                    first_nav = False

                if result:
                    records = result.get("records", [])

                    for race_url in result.get("race_links", [])[:15]:
                        detail = scrape_race_detail(page, race_url, date_str)
                        if detail:
                            records.extend(detail)
                        smart_pause(1.5, 0.8)

                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1

                # Also scrape results
                results_data = scrape_results_day(page, date_str)
                if results_data:
                    for rec in results_data:
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
        log.info("DONE: %d total records -> %s", total_records, output_file)
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
