#!/usr/bin/env python3
"""
Script 37 — Racing Post via Playwright : Form, ratings, resultats internationaux
Source : racingpost.com (SPA — JS-rendered content)
CRITIQUE pour : Ratings internationaux, form UK/FR, speed figures

Uses Playwright to render the JS-heavy SPA and extract:
  RPR, Topspeed, OR, jockey, trainer, weight, draw, form figures

PATCH JSONL : append mode, ~15 MB RAM au lieu de 1.6 GB

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, accept_cookies

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "../../output", "37_racing_post")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("37_rpscrape_racing_post")

MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 30_000

# Codes des hippodromes francais sur Racing Post
FR_COURSES = {
    "longchamp": 211, "chantilly": 207, "deauville": 209,
    "saint-cloud": 219, "auteuil": 204, "compiegne": 208,
    "fontainebleau": 210, "lyon-parilly": 213, "marseille": 214,
    "bordeaux": 206, "toulouse": 223, "cagnes-sur-mer": 1047,
    "pau": 216, "strasbourg": 220, "vichy": 224,
    "clairefontaine": 1048, "maisons-laffitte": 1180,
}


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
            time.sleep(2)  # extra wait for Racing Post JS rendering
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


def _safe_int(text):
    """Extract integer from text, return None if not possible."""
    if not text:
        return None
    m = re.search(r'(\d+)', text.strip())
    return int(m.group(1)) if m else None


def _extract_text(el):
    """Get stripped text from a BS element, or empty string."""
    return el.get_text(strip=True) if el else ""


def parse_race_results(soup, date_str, course_id, course_name):
    """Parse fully-rendered Racing Post results page.

    Extracts structured runner data including RPR, Topspeed, OR,
    jockey, trainer, weight, draw and form from the JS-rendered HTML.
    """
    records = []

    # Racing Post wraps each race in a section/div with various class patterns
    race_sections = soup.find_all(
        ["div", "section"],
        class_=re.compile(r"rp-raceResult|raceResult|race-result|card-result|result-card", re.I)
    )
    if not race_sections:
        # Fallback: look for any container with result-like classes
        race_sections = soup.find_all(
            ["div", "section"],
            class_=re.compile(r"result|race|card", re.I)
        )

    for race in race_sections:
        # Race title
        title_el = race.find(
            ["h2", "h3", "span", "a"],
            class_=re.compile(r"rp-raceTimeCourseName|title|name|header|raceTitle", re.I)
        )
        race_name = _extract_text(title_el)

        # Race conditions / info line
        info_el = race.find(
            attrs={"class": re.compile(r"rp-raceInfo|info|condition|detail|raceInfo", re.I)}
        )
        race_info = _extract_text(info_el)[:300]

        # Distance
        dist_el = race.find(attrs={"class": re.compile(r"distance|dist", re.I)})
        distance = _extract_text(dist_el)

        # Going
        going_el = race.find(attrs={"class": re.compile(r"going", re.I)})
        going = _extract_text(going_el)

        # Runner rows — Racing Post uses <tr> in rp-horseTable or similar
        runner_rows = race.find_all("tr", class_=re.compile(
            r"rp-horseTable|horse-table|runner|result-row", re.I
        ))
        if not runner_rows:
            runner_rows = race.find_all("tr")

        for row in runner_rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            text = row.get_text(" | ", strip=True)
            if len(text) < 10:
                continue

            record = {
                "date": date_str,
                "course_id": course_id,
                "course_name": course_name,
                "race_name": race_name,
                "race_info": race_info,
                "distance": distance,
                "going": going,
                "source": "racing_post",
                "scraped_at": datetime.now().isoformat(),
            }

            # Position — first cell or data-ending attribute
            pos_el = row.find(attrs={"data-ending": True})
            if pos_el:
                record["position"] = _safe_int(pos_el.get("data-ending", ""))
            else:
                pos_match = re.search(r'^(\d{1,2})', _extract_text(cells[0]))
                if pos_match:
                    record["position"] = int(pos_match.group(1))

            # Draw
            draw_el = row.find(attrs={"class": re.compile(r"draw|stall", re.I)})
            if draw_el:
                record["draw"] = _safe_int(_extract_text(draw_el))
            else:
                draw_el = row.find(attrs={"data-draw": True})
                if draw_el:
                    record["draw"] = _safe_int(draw_el.get("data-draw", ""))

            # Horse name
            horse_el = row.find(
                ["a", "span", "td"],
                class_=re.compile(r"horse|runner-name|rp-horseName", re.I)
            )
            if horse_el:
                record["horse"] = _extract_text(horse_el)

            # Jockey
            jockey_el = row.find(
                ["a", "span", "td"],
                class_=re.compile(r"jockey|rp-jockey", re.I)
            )
            if jockey_el:
                record["jockey"] = _extract_text(jockey_el)

            # Trainer
            trainer_el = row.find(
                ["a", "span", "td"],
                class_=re.compile(r"trainer|rp-trainer", re.I)
            )
            if trainer_el:
                record["trainer"] = _extract_text(trainer_el)

            # Weight
            weight_el = row.find(
                ["span", "td"],
                class_=re.compile(r"weight|wgt|rp-weight", re.I)
            )
            if weight_el:
                record["weight"] = _extract_text(weight_el)

            # Age
            age_el = row.find(attrs={"class": re.compile(r"age", re.I)})
            if age_el:
                record["age"] = _safe_int(_extract_text(age_el))

            # Odds / SP
            odds_el = row.find(attrs={"class": re.compile(r"odds|sp|price|rp-odds", re.I)})
            if odds_el:
                record["odds"] = _extract_text(odds_el)
            else:
                odds_match = re.search(r'(\d+/\d+|evens|\d+\.\d+)', text)
                if odds_match:
                    record["odds"] = odds_match.group(1)

            # RPR — Racing Post Rating
            rpr_el = row.find(
                attrs={"class": re.compile(r"rpr|rpRating|rp-horseTable__rpRow|rating-rpr", re.I)}
            )
            if rpr_el:
                record["rpr"] = _safe_int(_extract_text(rpr_el))
            else:
                rpr_match = re.search(r'RPR[:\s]*(\d+)', text, re.I)
                if rpr_match:
                    record["rpr"] = int(rpr_match.group(1))

            # Topspeed (TS)
            ts_el = row.find(
                attrs={"class": re.compile(r"topspeed|ts|rp-horseTable__tsRow|rating-ts", re.I)}
            )
            if ts_el:
                record["topspeed"] = _safe_int(_extract_text(ts_el))
            else:
                ts_match = re.search(r'TS[:\s]*(\d+)', text, re.I)
                if ts_match:
                    record["topspeed"] = int(ts_match.group(1))

            # Official Rating (OR)
            or_el = row.find(
                attrs={"class": re.compile(r'\bor\b|official.?rating|rp-horseTable__orRow', re.I)}
            )
            if or_el:
                record["official_rating"] = _safe_int(_extract_text(or_el))
            else:
                or_match = re.search(r'OR[:\s]*(\d+)', text)
                if or_match:
                    record["official_rating"] = int(or_match.group(1))

            # Form figures
            form_el = row.find(
                attrs={"class": re.compile(r"form|formFig|rp-form|recent-form", re.I)}
            )
            if form_el:
                record["form"] = _extract_text(form_el)
            else:
                form_match = re.search(r'\b([0-9PFU/-]{2,12})\b', text)
                if form_match:
                    record["form"] = form_match.group(1)

            # Comment / in-running
            comment_el = row.find(
                attrs={"class": re.compile(r"comment|in-running|race-comment", re.I)}
            )
            if comment_el:
                record["comment"] = _extract_text(comment_el)[:400]

            record["raw_text"] = text[:500]
            records.append(record)

    return records


def scrape_results_page(page, course_id, course_name, date_str):
    """Scrape a Racing Post results page using Playwright-rendered HTML."""
    cache_file = os.path.join(CACHE_DIR, f"{course_id}_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    url = f"https://www.racingpost.com/results/{course_id}/{date_str}"
    html = navigate_with_retry(page, url)
    if not html or len(html) < 2000:
        return []

    soup = BeautifulSoup(html, "html.parser")
    records = parse_race_results(soup, date_str, course_id, course_name)

    if records:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)

    return records


def export_cache_to_jsonl():
    """Export cache to JSONL without scraping."""
    jsonl_file = os.path.join(OUTPUT_DIR, "racing_post_cache.jsonl")
    log.info(f"Export cache -> {jsonl_file}")
    count = 0
    with open(jsonl_file, "w", encoding="utf-8", newline="\n") as fout:
        for fname in sorted(os.listdir(CACHE_DIR)):
            if not fname.endswith(".json"):
                continue
            cache_path = os.path.join(CACHE_DIR, fname)
            try:
                with open(cache_path, encoding="utf-8") as fin:
                    data = json.load(fin)
                if isinstance(data, list):
                    for entry in data:
                        fout.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        count += 1
                elif data:
                    fout.write(json.dumps(data, ensure_ascii=False) + "\n")
                    count += 1
            except Exception as e:
                log.debug(f"  Erreur lecture cache {fname}: {e}")
    log.info(f"  JSONL: {count} entrees -> {jsonl_file}")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Script 37 -- Racing Post Results (FR courses, Playwright)"
    )
    parser.add_argument("--export", action="store_true",
                        help="Export cache to JSONL without scraping")
    parser.add_argument("--start", type=str, default="2020-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=today")
    args = parser.parse_args()

    if args.export:
        log.info("=" * 60)
        log.info("SCRIPT 37 -- Export cache -> JSONL (--export)")
        log.info("=" * 60)
        export_cache_to_jsonl()
        return

    log.info("=" * 60)
    log.info("SCRIPT 37 -- Racing Post Results (FR courses) -- PLAYWRIGHT")
    log.info("=" * 60)

    output_file = os.path.join(OUTPUT_DIR, "racing_post_fr.jsonl")
    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_37.json")

    # Checkpoint / resume
    total_records = 0
    last_course_idx = 0
    last_date_str = ""
    if os.path.exists(checkpoint_file):
        cp = load_checkpoint(checkpoint_file)
        total_records = cp.get("total_records", 0)
        last_course_idx = cp.get("last_course_idx", 0)
        last_date_str = cp.get("last_date_str", "")
        log.info(f"Reprise checkpoint: {total_records} records, "
                 f"course_idx={last_course_idx}, derniere date={last_date_str}")

    start = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    course_list = list(FR_COURSES.items())
    collected_pages = 0

    with sync_playwright() as pw:
        browser, context, page = launch_browser(
            pw,
            locale="en-GB",
            timezone="Europe/London",
        )
        log.info("Playwright browser launched (en-GB, Europe/London)")

        try:
            # Accept cookies on first navigation
            first_nav = True

            for course_idx, (course_name, course_id) in enumerate(course_list):
                if course_idx < last_course_idx:
                    continue

                log.info(f"  Course: {course_name} (ID: {course_id})")

                current = start
                if course_idx == last_course_idx and last_date_str:
                    try:
                        resume_date = (datetime.strptime(last_date_str, "%Y-%m-%d")
                                       + timedelta(days=1))
                        if resume_date > current:
                            current = resume_date
                            log.info(f"    Reprise a la date {current.strftime('%Y-%m-%d')}")
                    except ValueError:
                        pass

                while current < end:
                    date_str = current.strftime("%Y-%m-%d")

                    records = scrape_results_page(page, course_id, course_name, date_str)

                    if first_nav:
                        accept_cookies(page)
                        first_nav = False

                    if records:
                        for r in records:
                            append_jsonl(output_file, r)
                        total_records += len(records)
                        collected_pages += 1

                    current += timedelta(days=1)
                    smart_pause(3.0, 1.5)

                    if collected_pages % 20 == 0 and collected_pages > 0:
                        log.info(f"    {collected_pages} pages, {total_records} records total")
                        save_checkpoint(checkpoint_file, {
                            "last_course_idx": course_idx,
                            "last_date_str": date_str,
                            "total_records": total_records,
                        })

                # Checkpoint fin de course
                save_checkpoint(checkpoint_file, {
                    "last_course_idx": course_idx + 1,
                    "last_date_str": "",
                    "total_records": total_records,
                })

        finally:
            browser.close()
            log.info("Browser closed")

    log.info(f"TERMINE: {collected_pages} pages, {total_records} records Racing Post")


if __name__ == "__main__":
    main()
