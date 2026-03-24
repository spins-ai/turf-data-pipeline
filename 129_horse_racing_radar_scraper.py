#!/usr/bin/env python3
"""
Script 129 — Horse Racing Radar Scraper (Playwright)
Source : horseracingradar.com
Collecte : US form analysis, statistics, predictions, trainer/jockey stats
URL patterns :
  /racecards/{date}        -> cartes du jour
  /racecards/{track}/      -> carte par piste
  /results/{date}          -> resultats du jour
  /statistics/             -> stats trainers, jockeys, tracks
  /predictions/{date}      -> predictions / picks
  /form/{horse}/           -> fiche forme cheval
CRITIQUE pour : US Form Analysis, Statistics, Predictions

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

SCRIPT_NAME = "129_horse_racing_radar"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

log = setup_logging("129_horse_racing_radar")

BASE_URL = "https://www.horseracingradar.com"
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


def extract_form_analysis(soup, date_str):
    """Extract form analysis data (recent runs, form figures, trends)."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "analysis", "guide",
                                                   "comment", "overview",
                                                   "spotlight", "profile",
                                                   "recent-runs", "history"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "horseracingradar",
                    "type": "form_analysis",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract form figures (e.g. 1-2-3-0 or 12341)
                form_match = re.search(r'\b([0-9PFU/-]{3,15})\b', text)
                if form_match:
                    record["form_figures"] = form_match.group(1)
                records.append(record)
    return records


def extract_statistics(soup, date_str):
    """Extract statistical data (win rates, ROI, trainer/jockey stats)."""
    records = []
    for el in soup.find_all(["div", "section", "td", "span", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["stat", "percentage", "win-rate",
                                                   "roi", "strike-rate",
                                                   "record", "performance",
                                                   "trainer-stat", "jockey-stat"]):
            text = el.get_text(strip=True)
            if text and 1 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": "horseracingradar",
                    "type": "statistic",
                    "contenu": text[:500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse percentage
                pct_match = re.search(r'(\d{1,3}(?:\.\d{1,2})?)%', text)
                if pct_match:
                    record["percentage"] = pct_match.group(1)
                # Parse win/run ratio
                ratio_match = re.search(r'(\d+)\s*/\s*(\d+)', text)
                if ratio_match:
                    record["wins"] = ratio_match.group(1)
                    record["runs"] = ratio_match.group(2)
                records.append(record)
    return records


def extract_predictions(soup, date_str):
    """Extract predictions, picks, and probability data."""
    records = []
    for el in soup.find_all(["div", "section", "article", "p", "span"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["prediction", "pick", "tip",
                                                   "selection", "probability",
                                                   "forecast", "best-bet",
                                                   "confidence", "recommended"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": "horseracingradar",
                    "type": "prediction",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse probability or confidence
                prob_match = re.search(r'(\d{1,3}(?:\.\d{1,2})?)%', text)
                if prob_match:
                    record["probability_pct"] = prob_match.group(1)
                # Parse odds
                odds_match = re.search(r'(\d+/\d+|\d+\.\d{2})', text)
                if odds_match:
                    record["odds"] = odds_match.group(1)
                records.append(record)
    return records


def extract_trainer_jockey_stats(soup, date_str):
    """Extract trainer and jockey statistics blocks."""
    records = []
    for el in soup.find_all(["div", "section", "table", "article"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["trainer", "jockey", "sire",
                                                   "owner", "breeder",
                                                   "connections"]):
            text = el.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                record = {
                    "date": date_str,
                    "source": "horseracingradar",
                    "type": "connections_stat",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Determine sub-type
                if "trainer" in classes.lower():
                    record["stat_category"] = "trainer"
                elif "jockey" in classes.lower():
                    record["stat_category"] = "jockey"
                elif "sire" in classes.lower():
                    record["stat_category"] = "sire"
                records.append(record)
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_day_racecards(page, date_str):
    """Scrape the Horse Racing Radar racecards page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racecards/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    html_file = os.path.join(HTML_CACHE_DIR, f"racecards_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "horseracingradar", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "horseracingradar", date_str=date_str))
    records.extend(extract_form_analysis(soup, date_str))
    records.extend(extract_statistics(soup, date_str))
    records.extend(extract_predictions(soup, date_str))
    records.extend(extract_trainer_jockey_stats(soup, date_str))
    records.extend(extract_runners_table(soup, "horseracingradar", date_str=date_str))

    race_links = extract_race_links(soup, base_url=BASE_URL)

    # Extract track / meeting blocks
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "track", "card",
                                                   "race-list", "racecard"]):
            record = {
                "date": date_str,
                "source": "horseracingradar",
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
            records.append(record)

    result = {"records": records, "race_links": race_links}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_race_detail(page, race_url, date_str):
    """Scrape individual race page for detailed form analysis and predictions."""
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
    records.extend(extract_embedded_json_data(soup, "horseracingradar", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "horseracingradar", date_str=date_str))
    records.extend(extract_form_analysis(soup, date_str))
    records.extend(extract_statistics(soup, date_str))
    records.extend(extract_predictions(soup, date_str))
    records.extend(extract_trainer_jockey_stats(soup, date_str))

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
                "source": "horseracingradar",
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

    # Verdict / prediction sections
    for div in soup.find_all(["div", "p", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["verdict", "pick", "selection",
                                                   "analysis", "comment",
                                                   "prediction", "forecast"]):
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "horseracingradar",
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

    records.extend(extract_embedded_json_data(soup, "horseracingradar", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "horseracingradar", date_str=date_str))
    records.extend(extract_form_analysis(soup, date_str))
    records.extend(extract_statistics(soup, date_str))
    records.extend(extract_runners_table(soup, "horseracingradar", date_str=date_str, race_url=url))

    # Result-specific extraction
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "finish", "placed",
                                                   "winner", "returns",
                                                   "payoff", "payout"]):
            text = div.get_text(strip=True)
            if text and 3 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "horseracingradar",
                    "type": "result_block",
                    "contenu": text[:1500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_predictions_page(page, date_str):
    """Scrape dedicated predictions page if available."""
    cache_file = os.path.join(CACHE_DIR, f"predictions_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/predictions/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "horseracingradar", date_str=date_str))
    records.extend(extract_predictions(soup, date_str))
    records.extend(extract_statistics(soup, date_str))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_statistics_page(page, date_str):
    """Scrape the statistics hub for trainer/jockey/track stats."""
    cache_file = os.path.join(CACHE_DIR, f"statistics_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/statistics/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "horseracingradar", date_str=date_str))
    records.extend(extract_statistics(soup, date_str))
    records.extend(extract_trainer_jockey_stats(soup, date_str))

    # Statistics tables
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
                "source": "horseracingradar",
                "type": "stat_table_row",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 129 — Horse Racing Radar Scraper (US form analysis, statistics, predictions)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=yesterday")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--skip-statistics", action="store_true", default=False,
                        help="Skip the statistics hub page")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
                else datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 129 — Horse Racing Radar Scraper (Playwright)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "horse_racing_radar_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-US", timezone="America/New_York"
        )
        log.info("Browser launched (headless Chromium, locale=en-US, tz=America/New_York)")

        # Scrape statistics hub once (not date-dependent)
        if not args.skip_statistics:
            today_str = datetime.now().strftime("%Y-%m-%d")
            stats_data = scrape_statistics_page(page, today_str)
            if stats_data:
                for rec in stats_data:
                    append_jsonl(output_file, rec)
                log.info("  Statistics hub: %d records", len(stats_data))
            accept_cookies(page)

        first_nav = not args.skip_statistics
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")

            # Scrape racecards index
            result = scrape_day_racecards(page, date_str)

            if not first_nav and result is not None:
                accept_cookies(page)
                first_nav = True

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

            # Scrape predictions page
            predictions_data = scrape_predictions_page(page, date_str)
            if predictions_data:
                for rec in predictions_data:
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
