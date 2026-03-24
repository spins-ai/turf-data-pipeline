#!/usr/bin/env python3
"""
Script 128 — TrackMaster Speed Figures Scraper (Playwright)
Source : trackmaster.com
Collecte : US speed figures, pace projections, race analysis, track profiles
URL patterns :
  /entries/{date}          -> entries du jour
  /entries/{track}/{date}  -> entries par piste
  /results/{date}          -> resultats du jour
  /results/{track}/{date}  -> resultats par piste
  /speed-figures/          -> speed figures / past performances
  /pace-projections/       -> pace projections
  /track-profiles/         -> track bias / profil piste
CRITIQUE pour : US Speed Figures, Pace Analysis, Track Bias

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
from utils.html_parsing import extract_embedded_json_data
from utils.html_parsing import extract_scraper_data_attributes
from utils.html_parsing import extract_runners_table
from utils.html_parsing import extract_race_links

SCRIPT_NAME = "128_trackmaster"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

log = setup_logging("128_trackmaster")

BASE_URL = "https://www.trackmaster.com"
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


def extract_speed_figures(soup, date_str):
    """Extract speed figures and performance ratings from page content."""
    records = []
    for el in soup.find_all(["div", "span", "td", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["speed", "figure", "rating",
                                                   "score", "performance",
                                                   "beyer", "bris", "tm-fig",
                                                   "power-rating"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": "trackmaster",
                    "type": "speed_figure",
                    "contenu": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                rating_match = re.search(r'(\d{1,3}(?:\.\d)?)', text)
                if rating_match:
                    record["figure_value"] = rating_match.group(1)
                records.append(record)
    return records


def extract_pace_projections(soup, date_str):
    """Extract pace projection data (early speed, pace scenario, run style)."""
    records = []
    for el in soup.find_all(["div", "section", "td", "span"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["pace", "projection", "early-speed",
                                                   "run-style", "speed-point",
                                                   "pace-scenario", "ep-rating",
                                                   "running-style"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": "trackmaster",
                    "type": "pace_projection",
                    "contenu": text[:500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse pace rating
                pace_match = re.search(r'(\d{1,3}(?:\.\d)?)', text)
                if pace_match:
                    record["pace_value"] = pace_match.group(1)
                # Parse run style
                style_match = re.search(
                    r'(E|EP|P|PS|S|closer|presser|stalker|front.?runner|'
                    r'early|mid.?pack|off.?the.?pace)',
                    text, re.I
                )
                if style_match:
                    record["run_style"] = style_match.group(1).strip()
                records.append(record)
    return records


def extract_race_analysis(soup, date_str):
    """Extract race analysis, picks, and commentary blocks."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["analysis", "comment", "pick",
                                                   "selection", "verdict",
                                                   "handicapping", "insight",
                                                   "expert", "tip", "outlook"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "trackmaster",
                    "type": "race_analysis",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_track_profile(soup, date_str):
    """Extract track bias and profile data."""
    records = []
    for el in soup.find_all(["div", "section", "table", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["track", "bias", "profile",
                                                   "surface", "condition",
                                                   "rail", "weather",
                                                   "course-info"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "trackmaster",
                    "type": "track_profile",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse surface type
                surface_match = re.search(
                    r'(dirt|turf|synthetic|polytrack|tapeta|all.?weather)',
                    text, re.I
                )
                if surface_match:
                    record["surface"] = surface_match.group(1).strip()
                # Parse condition
                cond_match = re.search(
                    r'(fast|firm|good|yielding|sloppy|muddy|wet.?fast|sealed)',
                    text, re.I
                )
                if cond_match:
                    record["condition"] = cond_match.group(1).strip()
                records.append(record)
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_day_entries(page, date_str):
    """Scrape the TrackMaster entries page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"entries_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/entries/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"entries_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "trackmaster", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "trackmaster", date_str=date_str))
    records.extend(extract_speed_figures(soup, date_str))
    records.extend(extract_pace_projections(soup, date_str))
    records.extend(extract_race_analysis(soup, date_str))
    records.extend(extract_track_profile(soup, date_str))
    records.extend(extract_runners_table(soup, "trackmaster", date_str=date_str))

    race_links = extract_race_links(soup, base_url=BASE_URL)

    # Extract track / meeting blocks
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "track", "card",
                                                   "race-list", "entries"]):
            record = {
                "date": date_str,
                "source": "trackmaster",
                "type": "meeting",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong", "a"])
            if title:
                record["track_name"] = title.get_text(strip=True)
            for span in div.find_all(["span", "small", "em", "p"]):
                text = span.get_text(strip=True)
                surface_match = re.search(
                    r'(dirt|turf|synthetic|polytrack|tapeta|all.?weather)',
                    text, re.I
                )
                if surface_match:
                    record["surface"] = surface_match.group(1).strip()
                cond_match = re.search(
                    r'(fast|firm|good|sloppy|muddy|wet.?fast|sealed)',
                    text, re.I
                )
                if cond_match:
                    record["condition"] = cond_match.group(1).strip()
            records.append(record)

    result = {"records": records, "race_links": race_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape individual race page for detailed speed figures and pace data."""
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

    # Race title
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Race conditions
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(
        r'(\d+(?:\s*1/[24])?)\s*(?:furlongs?|f\b)|'
        r'(\d+(?:\s*1/[248])?)\s*(?:miles?|mi?\b)',
        page_text, re.I
    )
    if dist_match:
        conditions["distance"] = (dist_match.group(1) or dist_match.group(2)).strip()

    surface_match = re.search(
        r'(dirt|turf|synthetic|polytrack|tapeta|all.?weather)',
        page_text, re.I
    )
    if surface_match:
        conditions["surface"] = surface_match.group(1).strip()

    cond_match = re.search(
        r'(?:track|condition)\s*:?\s*(fast|firm|good|yielding|sloppy|muddy|wet.?fast|sealed)',
        page_text, re.I
    )
    if cond_match:
        conditions["condition"] = cond_match.group(1).strip()

    purse_match = re.search(r'\$[\d,]+', page_text)
    if purse_match:
        conditions["purse"] = purse_match.group(0)

    class_match = re.search(
        r'(maiden|claiming|allowance|stakes|graded|grade\s*[123I]+|G[123]|'
        r'optional\s+claiming|starter|handicap)',
        page_text, re.I
    )
    if class_match:
        conditions["race_class"] = class_match.group(1).strip()

    # Structured data extraction
    records.extend(extract_embedded_json_data(soup, "trackmaster", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "trackmaster", date_str=date_str))
    records.extend(extract_speed_figures(soup, date_str))
    records.extend(extract_pace_projections(soup, date_str))
    records.extend(extract_race_analysis(soup, date_str))
    records.extend(extract_track_profile(soup, date_str))

    # Runners table
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
                "source": "trackmaster",
                "type": "runner_detail",
                "race_name": race_name,
                "conditions": conditions,
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

    # Analysis / verdict sections
    for div in soup.find_all(["div", "p", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["verdict", "pick", "selection",
                                                   "analysis", "comment",
                                                   "handicapping", "insight"]):
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "trackmaster",
                    "type": "verdict",
                    "race_name": race_name,
                    "conditions": conditions,
                    "contenu": text[:2000],
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results_day(page, date_str):
    """Scrape results page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/results/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "trackmaster", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "trackmaster", date_str=date_str))
    records.extend(extract_speed_figures(soup, date_str))
    records.extend(extract_pace_projections(soup, date_str))
    records.extend(extract_runners_table(soup, "trackmaster", date_str=date_str, race_url=url))

    # Result-specific extraction
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "finish", "placed",
                                                   "winner", "returns",
                                                   "payoff", "exacta",
                                                   "trifecta", "payout"]):
            text = div.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "trackmaster",
                    "type": "result_block",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_speed_figures_page(page, date_str):
    """Scrape dedicated speed figures page if available."""
    cache_file = os.path.join(CACHE_DIR, f"figures_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/speed-figures/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "trackmaster", date_str=date_str))
    records.extend(extract_speed_figures(soup, date_str))
    records.extend(extract_runners_table(soup, "trackmaster", date_str=date_str, race_url=url))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 128 — TrackMaster Speed Figures Scraper (US speed figures, pace projections)"
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
    log.info("SCRIPT 128 — TrackMaster Speed Figures Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "trackmaster_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US, tz=America/New_York)")

        first_nav = True
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")

            # Scrape entries index
            result = scrape_day_entries(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual race pages (limit to avoid overload)
                for race_url in result.get("race_links", [])[:15]:
                    detail = scrape_race_detail(page, race_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Scrape results page
            results_data = scrape_results_day(page, date_str)
            if results_data:
                for rec in results_data:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Scrape dedicated speed figures page
            figures_data = scrape_speed_figures_page(page, date_str)
            if figures_data:
                for rec in figures_data:
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
