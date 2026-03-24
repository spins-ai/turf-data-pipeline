#!/usr/bin/env python3
"""
Script 125 (Playwright) -- HorseRaceBase scraper.
Source : horseracebase.com
Collecte : UK horse profiles, form guides, race results, tips, race cards
CRITIQUE pour : UK form data, detailed horse profiles, race analysis

Usage:
    pip install playwright beautifulsoup4
    playwright install chromium
    python 125_horseracebase_scraper.py --start 2024-01-01 --end 2026-03-24
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
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.playwright import launch_browser, accept_cookies
from utils.html_parsing import extract_embedded_json_data
from utils.html_parsing import extract_scraper_data_attributes

log = setup_logging("125_horseracebase_scraper")

SCRIPT_NAME = "125_horseracebase"
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "output", SCRIPT_NAME
)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

BASE_URL = "https://www.horseracebase.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

# Major UK racecourses for cross-reference
UK_COURSES = [
    "ascot", "cheltenham", "aintree", "epsom", "goodwood", "newmarket",
    "york", "doncaster", "sandown", "kempton", "newbury", "haydock",
    "chester", "windsor", "lingfield", "wolverhampton", "catterick",
    "thirsk", "ripon", "nottingham", "leicester", "warwick",
    "bangor-on-dee", "market-rasen", "wincanton", "exeter", "fontwell",
    "plumpton", "sedgefield", "wetherby", "uttoxeter", "carlisle",
    "musselburgh", "ayr", "hamilton", "perth", "kelso",
]

# UK race types
RACE_TYPES = ["flat", "hurdle", "chase", "nhf", "bumper"]


# ------------------------------------------------------------------
# Navigation helper
# ------------------------------------------------------------------

def navigate_with_retry(page, url, retries=MAX_RETRIES):
    """Navigate to url with retry logic. Returns HTML string or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT_MS)
            if resp and resp.status >= 400:
                log.warning(
                    "  HTTP %d on %s (attempt %d/%d)",
                    resp.status, url, attempt, retries,
                )
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
            log.warning(
                "  Navigation error: %s (attempt %d/%d)",
                str(exc)[:200], attempt, retries,
            )
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


# ------------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------------

def find_course(text):
    """Match a known UK course name in text."""
    text_lower = text.lower()
    for c in UK_COURSES:
        if c.replace("-", " ") in text_lower:
            return c
    return ""


def parse_position(text):
    """Extract finishing position from text like '1st', '3rd'."""
    m = re.match(r"^(\d+)(?:st|nd|rd|th)?$", text.strip(), re.I)
    return int(m.group(1)) if m else None


def parse_prize(text):
    """Extract GBP prize money from text."""
    m = re.search(r"[^\d]?([\d,]+(?:\.\d{2})?)\s*(?:GBP|\u00a3|pounds?)?", text)
    if m:
        val = m.group(1).replace(",", "")
        try:
            return float(val)
        except ValueError:
            pass
    return None


def parse_time(text):
    """Extract race time in seconds from text like '1m 32.40s'."""
    m = re.search(r"(\d+)\s*m\s*(\d+(?:\.\d+)?)\s*s?", text, re.I)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    m = re.search(r"(\d+):(\d+(?:\.\d+)?)", text)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    return None


# ------------------------------------------------------------------
# Extraction: horse profiles
# ------------------------------------------------------------------

def extract_horse_profiles(soup, date_str):
    """Extract horse profile data from page content."""
    records = []
    for el in soup.find_all(
        ["div", "section", "article", "li"], class_=True
    ):
        classes = " ".join(el.get("class", []))
        if not any(
            kw in classes.lower()
            for kw in [
                "horse", "profile", "runner", "card", "entry",
                "performer", "animal",
            ]
        ):
            continue

        text = el.get_text(strip=True)
        if not text or len(text) < 5:
            continue

        record = {
            "date": date_str,
            "source": "horseracebase",
            "type": "horse_profile",
            "scraped_at": datetime.now().isoformat(),
        }

        # Horse name
        name_el = el.find(["h2", "h3", "h4", "strong", "a"], class_=True)
        if name_el:
            record["horse_name"] = name_el.get_text(strip=True)
        elif el.find("a", href=True):
            link = el.find("a", href=True)
            if "/horse/" in (link.get("href", "") or ""):
                record["horse_name"] = link.get_text(strip=True)

        # Trainer
        trainer_el = el.find(
            ["span", "div", "a"],
            class_=lambda c: c and any(
                kw in " ".join(c).lower() for kw in ["trainer", "yard"]
            ),
        )
        if trainer_el:
            record["trainer"] = trainer_el.get_text(strip=True)
        else:
            m = re.search(r"trainer\s*:?\s*([A-Z][a-zA-Z\s\-']+)", text)
            if m:
                record["trainer"] = m.group(1).strip()

        # Jockey
        jockey_el = el.find(
            ["span", "div", "a"],
            class_=lambda c: c and "jockey" in " ".join(c).lower(),
        )
        if jockey_el:
            record["jockey"] = jockey_el.get_text(strip=True)
        else:
            m = re.search(r"jockey\s*:?\s*([A-Z][a-zA-Z\s\-']+)", text)
            if m:
                record["jockey"] = m.group(1).strip()

        # Age / weight
        age_m = re.search(r"\b(\d)\s*yo\b|\bage\s*:?\s*(\d)\b", text, re.I)
        if age_m:
            record["age"] = int(age_m.group(1) or age_m.group(2))

        weight_m = re.search(
            r"(\d{1,2})\s*(?:st|stone)\s*(\d{1,2})?\s*(?:lb|lbs)?", text, re.I
        )
        if weight_m:
            st = int(weight_m.group(1))
            lb = int(weight_m.group(2) or 0)
            record["weight_lbs"] = st * 14 + lb

        # Official rating
        or_m = re.search(r"(?:OR|official\s*rating)\s*:?\s*(\d{1,3})", text, re.I)
        if or_m:
            record["official_rating"] = int(or_m.group(1))

        # Form figures
        form_m = re.search(r"form\s*:?\s*([0-9PFU/\-]{2,})", text, re.I)
        if form_m:
            record["form"] = form_m.group(1).strip()

        # Profile link
        link_el = el.find("a", href=True)
        if link_el:
            href = link_el.get("href", "")
            if href and "/horse/" in href:
                record["url_profile"] = (
                    href if href.startswith("http") else f"{BASE_URL}{href}"
                )

        if record.get("horse_name"):
            records.append(record)

    return records


# ------------------------------------------------------------------
# Extraction: form guides
# ------------------------------------------------------------------

def extract_form_guides(soup, date_str):
    """Extract form guide data (recent runs, analysis, verdicts)."""
    records = []
    for el in soup.find_all(
        ["div", "section", "article", "p"], class_=True
    ):
        classes = " ".join(el.get("class", []))
        if not any(
            kw in classes.lower()
            for kw in [
                "form", "guide", "comment", "analysis", "verdict",
                "spotlight", "tip", "selection", "overview", "preview",
                "nap", "assessment",
            ]
        ):
            continue

        text = el.get_text(strip=True)
        if not text or len(text) < 20 or len(text) > 5000:
            continue

        record = {
            "date": date_str,
            "source": "horseracebase",
            "type": "form_guide",
            "contenu": text[:3000],
            "classes_css": classes,
            "scraped_at": datetime.now().isoformat(),
        }

        # Extract horse name if mentioned
        h_el = el.find(["h3", "h4", "strong"])
        if h_el:
            record["subject"] = h_el.get_text(strip=True)

        records.append(record)

    return records


# ------------------------------------------------------------------
# Extraction: race results tables
# ------------------------------------------------------------------

def extract_results_table(soup, date_str, race_url=""):
    """Extract runner data from result tables."""
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [
                th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                for th in rows[0].find_all(["th", "td"])
            ]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue

            record = {
                "date": date_str,
                "source": "horseracebase",
                "type": "result_runner",
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Parse specific fields
                pos = parse_position(cell)
                if pos is not None and "position" not in record:
                    record["position"] = pos
                prize = parse_prize(cell)
                if prize is not None and "prize_gbp" not in record:
                    record["prize_gbp"] = prize
                t = parse_time(cell)
                if t is not None and "time_seconds" not in record:
                    record["time_raw"] = cell
                    record["time_seconds"] = t

            # Data attributes on row
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)

    return records


# ------------------------------------------------------------------
# Extraction: embedded JSON and data-attributes
# ------------------------------------------------------------------


# ------------------------------------------------------------------
# Scrape: racecards day index
# ------------------------------------------------------------------

def scrape_racecards_day(page, date_str):
    """Scrape the HorseRaceBase racecards page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/racecards/{date_str}",
        f"{BASE_URL}/racecards/?date={date_str}",
        f"{BASE_URL}/racing/racecards/{date_str}",
    ]

    html = None
    for url in urls_to_try:
        html = navigate_with_retry(page, url)
        if html:
            break
        smart_pause(2.0, 1.0)

    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"racecards_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Structured data
    records.extend(extract_embedded_json_data(soup, "horseracebase", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "horseracebase", date_str=date_str))
    records.extend(extract_horse_profiles(soup, date_str))
    records.extend(extract_form_guides(soup, date_str))
    records.extend(extract_results_table(soup, date_str))

    # Race links for detail scraping
    race_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/(racecard|race|card|tip)/", href, re.I):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            race_links.add(full_url)

    # Meeting/venue blocks
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(
            kw in classes.lower()
            for kw in ["meeting", "venue", "card", "race-list", "racecard"]
        ):
            record = {
                "date": date_str,
                "source": "horseracebase",
                "type": "meeting",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong", "a"])
            if title:
                record["venue"] = title.get_text(strip=True)
                course = find_course(record["venue"])
                if course:
                    record["course"] = course
            # Going
            for span in div.find_all(["span", "small", "em", "p"]):
                text = span.get_text(strip=True)
                going_m = re.search(
                    r"(going|ground)\s*:?\s*(firm|good to firm|good|"
                    r"good to soft|soft|heavy|yielding|standard|slow|fast)",
                    text, re.I,
                )
                if going_m:
                    record["going"] = going_m.group(2).strip()
            records.append(record)

    result = {"records": records, "race_links": sorted(race_links)}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


# ------------------------------------------------------------------
# Scrape: race detail page
# ------------------------------------------------------------------

def scrape_race_detail(page, race_url, date_str):
    """Scrape a single race card or result detail page."""
    url_hash = re.sub(r"[^a-zA-Z0-9]", "_", race_url[-80:])
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
    page_text = soup.get_text()
    conditions = {}

    course = find_course(page_text)
    if course:
        conditions["course"] = course

    dist_m = re.search(r"(\d+)\s*(?:f|furlongs?)\b", page_text, re.I)
    if dist_m:
        conditions["distance_furlongs"] = int(dist_m.group(1))

    going_m = re.search(
        r"going\s*:?\s*(firm|good to firm|good|good to soft|soft|heavy|"
        r"yielding|standard|slow|fast)",
        page_text, re.I,
    )
    if going_m:
        conditions["going"] = going_m.group(1).strip()

    class_m = re.search(r"class\s*(\d)", page_text, re.I)
    if class_m:
        conditions["race_class"] = int(class_m.group(1))

    for rt in RACE_TYPES:
        if rt in page_text.lower():
            conditions["race_type"] = rt
            break

    prize_m = re.search(
        r"prize\s*(?:money|fund)?\s*:?\s*\u00a3?\s*([\d,]+)", page_text, re.I,
    )
    if prize_m:
        val = prize_m.group(1).replace(",", "")
        try:
            conditions["total_prize_gbp"] = float(val)
        except ValueError:
            pass

    # Structured data
    records.extend(extract_embedded_json_data(soup, "horseracebase", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "horseracebase", date_str=date_str))
    records.extend(extract_horse_profiles(soup, date_str))
    records.extend(extract_form_guides(soup, date_str))

    # Results table
    for rec in extract_results_table(soup, date_str, race_url):
        rec["race_name"] = race_name
        rec["conditions"] = conditions
        records.append(rec)

    # Tips / verdict sections
    for div in soup.find_all(["div", "p", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(
            kw in classes.lower()
            for kw in [
                "verdict", "tip", "selection", "analysis", "comment",
                "spotlight", "nap", "each-way",
            ]
        ):
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 5000:
                records.append({
                    "date": date_str,
                    "source": "horseracebase",
                    "type": "verdict",
                    "race_name": race_name,
                    "conditions": conditions,
                    "contenu": text[:3000],
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Scrape: results page for a date
# ------------------------------------------------------------------

def scrape_results_day(page, date_str):
    """Scrape results page for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/results/{date_str}",
        f"{BASE_URL}/results/?date={date_str}",
        f"{BASE_URL}/racing/results/{date_str}",
    ]

    html = None
    for url in urls_to_try:
        html = navigate_with_retry(page, url)
        if html:
            break
        smart_pause(2.0, 1.0)

    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json_data(soup, "horseracebase", date_str=date_str))
    records.extend(extract_scraper_data_attributes(soup, "horseracebase", date_str=date_str))
    records.extend(extract_results_table(soup, date_str))
    records.extend(extract_horse_profiles(soup, date_str))

    # Result-specific blocks
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(
            kw in classes.lower()
            for kw in ["result", "finishing", "placed", "winner", "returns"]
        ):
            text = div.get_text(strip=True)
            if text and 3 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "horseracebase",
                    "type": "result_block",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Scrape: horse profile detail page
# ------------------------------------------------------------------

def scrape_horse_profile(page, horse_url, date_str):
    """Scrape an individual horse profile page."""
    url_hash = re.sub(r"[^a-zA-Z0-9]", "_", horse_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"horse_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, horse_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    record = {
        "source": "horseracebase",
        "type": "horse_profile_detail",
        "url": horse_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Horse name from h1
    h1 = soup.find("h1")
    if h1:
        record["horse_name"] = h1.get_text(strip=True)

    page_text = soup.get_text()

    # Sire / Dam
    sire_m = re.search(r"sire\s*:?\s*([A-Z][a-zA-Z\s\-']+)", page_text, re.I)
    if sire_m:
        record["sire"] = sire_m.group(1).strip()

    dam_m = re.search(r"dam\s*:?\s*([A-Z][a-zA-Z\s\-']+)", page_text, re.I)
    if dam_m:
        record["dam"] = dam_m.group(1).strip()

    # Age, sex, colour
    age_m = re.search(r"\b(\d)\s*yo\b|\bage\s*:?\s*(\d)\b", page_text, re.I)
    if age_m:
        record["age"] = int(age_m.group(1) or age_m.group(2))

    sex_m = re.search(
        r"\b(colt|filly|gelding|mare|horse|ridgling)\b", page_text, re.I,
    )
    if sex_m:
        record["sex"] = sex_m.group(1).lower()

    colour_m = re.search(
        r"\b(bay|brown|black|chestnut|grey|roan|dun)\b", page_text, re.I,
    )
    if colour_m:
        record["colour"] = colour_m.group(1).lower()

    # OR
    or_m = re.search(r"(?:OR|official\s*rating)\s*:?\s*(\d{1,3})", page_text, re.I)
    if or_m:
        record["official_rating"] = int(or_m.group(1))

    # Form
    form_m = re.search(r"form\s*:?\s*([0-9PFU/\-]{2,})", page_text, re.I)
    if form_m:
        record["form"] = form_m.group(1).strip()

    # Career stats
    stats_m = re.search(
        r"(\d+)\s*(?:runs?|starts?)\s*[,;]?\s*(\d+)\s*wins?", page_text, re.I,
    )
    if stats_m:
        record["career_runs"] = int(stats_m.group(1))
        record["career_wins"] = int(stats_m.group(2))

    # Past performances from tables
    past_perf = extract_results_table(soup, date_str, horse_url)
    record["past_performances"] = past_perf

    # Embedded JSON
    json_data = extract_embedded_json_data(soup, "horseracebase", date_str=date_str)
    if json_data:
        record["embedded_data"] = json_data

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    return record


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 125 (Playwright) -- HorseRaceBase Scraper "
        "(UK horse profiles, form guides, race results)"
    )
    parser.add_argument(
        "--start", type=str, default="2024-01-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="End date (YYYY-MM-DD), default=yesterday",
    )
    parser.add_argument(
        "--resume", action="store_true", default=True,
        help="Resume from last checkpoint",
    )
    parser.add_argument(
        "--max-days", type=int, default=0,
        help="Max days to scrape (0=unlimited)",
    )
    parser.add_argument(
        "--max-detail-pages", type=int, default=15,
        help="Max detail pages per day",
    )
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (
        datetime.strptime(args.end, "%Y-%m-%d")
        if args.end
        else datetime.now() - timedelta(days=1)
    )

    log.info("=" * 60)
    log.info("SCRIPT 125 (Playwright) -- HorseRaceBase Scraper")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date > start_date:
            start_date = resume_date
            log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "horseracebase_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(
            pw, locale="en-GB", timezone="Europe/London"
        )
        log.info("Browser launched (headless Chromium, locale=en-GB)")

        first_nav = True
        current = start_date
        day_count = 0
        total_records = checkpoint.get("total_records", 0)

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")

            # --- Racecards ---
            result = scrape_racecards_day(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Detail pages (limited)
                for race_url in result.get("race_links", [])[:args.max_detail_pages]:
                    detail = scrape_race_detail(page, race_url, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                # Horse profile links from details
                horse_urls = set()
                for rec in records:
                    url_prof = rec.get("url_profile", "")
                    if url_prof and "/horse/" in url_prof:
                        horse_urls.add(url_prof)

                for horse_url in sorted(horse_urls)[:10]:
                    profile = scrape_horse_profile(page, horse_url, date_str)
                    if profile:
                        records.append(profile)
                    smart_pause(2.0, 1.0)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # --- Results ---
            results_data = scrape_results_day(page, date_str)
            if results_data:
                for rec in results_data:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 10 == 0:
                log.info(
                    "  %s | days=%d records=%d",
                    date_str, day_count, total_records,
                )
                save_checkpoint(CHECKPOINT_FILE, {
                    "last_date": date_str,
                    "total_records": total_records,
                })

            # Rotate browser every 80 days to avoid memory leaks
            if day_count % 80 == 0:
                log.info("  Rotating browser context...")
                try:
                    page.close()
                    context.close()
                    browser.close()
                except Exception:
                    pass
                smart_pause(5.0, 2.0)
                browser, context, page = launch_browser(
                    pw, locale="en-GB", timezone="Europe/London"
                )
                first_nav = True

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {
            "last_date": end_date.strftime("%Y-%m-%d"),
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info(
            "DONE: %d days, %d records -> %s",
            day_count, total_records, output_file,
        )
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
