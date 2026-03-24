#!/usr/bin/env python3
"""
Script 53 — Scraping Paris-Turf.com (Playwright version)
Source : paris-turf.com -- donnees riches via __NEXT_DATA__ JSON
URLs reelles :
  /programme-courses/hier -> liste meetings + courses du jour
  /programme-courses/aujourdhui
  /course/{hippo}-{prix}-idc-{id} -> details course avec runners complets
Donnees : runners avec records, stats, ferrage, musique, cotes, etc.

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

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

SCRIPT_NAME = "53_paris_turf"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl

log = setup_logging("53_paris_turf")

MAX_RETRIES = 3

BASE_URL = "https://www.paris-turf.com"








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
# Extraction helpers
# ------------------------------------------------------------------

def extract_next_data(html_text):
    """Extraire __NEXT_DATA__ JSON depuis le HTML."""
    soup = BeautifulSoup(html_text, "html.parser")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if script and script.string:
        try:
            return json.loads(script.string)
        except json.JSONDecodeError:
            pass
    return None


# ------------------------------------------------------------------
# Scraping functions (BeautifulSoup-based, fed from page.content())
# ------------------------------------------------------------------

def get_day_courses(page, page_slug):
    """Recuperer les courses d'un jour via la page programme."""
    url = f"{BASE_URL}/programme-courses/{page_slug}"
    html = navigate_with_retry(page, url)
    if not html:
        return [], []

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"programme_{page_slug}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    # Extraire les liens de courses
    soup = BeautifulSoup(html, "html.parser")
    course_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/course/" in href and "idc-" in href:
            if not href.startswith("http"):
                href = BASE_URL + href
            course_links.add(href)

    # Extraire __NEXT_DATA__ pour les meetings et races
    next_data = extract_next_data(html)
    meetings = []
    if next_data:
        state = next_data.get("props", {}).get("pageProps", {}).get("initialState", {})
        rcs = state.get("raceCardsState", {})
        meetings_data = rcs.get("meetings", {})
        for date_key, mlist in meetings_data.items():
            if isinstance(mlist, list):
                for m in mlist:
                    meetings.append({
                        "date": date_key,
                        "id": m.get("id"),
                        "name": m.get("name", ""),
                        "country": m.get("country", ""),
                        "pmuNumber": m.get("pmuNumber"),
                        "time": m.get("time", ""),
                    })

    return sorted(course_links), meetings


def scrape_course(page, course_url, date_iso, output_runners, output_races):
    """Scraper une course individuelle via __NEXT_DATA__."""
    # Extraire l'ID de la course pour le cache
    idc_match = re.search(r'idc-([a-f0-9]+)', course_url)
    idc = idc_match.group(1) if idc_match else course_url.split("/")[-1]
    cache_file = os.path.join(CACHE_DIR, f"course_{idc[:40]}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
            return cached.get("nb_runners", 0)

    html = navigate_with_retry(page, course_url)
    if not html:
        return 0

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"course_{idc[:40]}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    next_data = extract_next_data(html)
    if not next_data:
        return 0

    state = next_data.get("props", {}).get("pageProps", {}).get("initialState", {})
    rcs = state.get("raceCardsState", {})

    # Races
    races = rcs.get("races", {})
    nb_runners = 0

    for date_key, race_list in races.items():
        if not isinstance(race_list, list):
            continue
        for race in race_list:
            if not isinstance(race, dict):
                continue
            race_record = {
                "date": date_iso,
                "source": "paris_turf",
                "type": "race",
                "idc": idc,
                "url": course_url,
                "scraped_at": datetime.now().isoformat(),
            }
            # Copier les champs cles
            for k in ["id", "distance", "specialty", "totalPrize", "going", "class",
                       "minAge", "maxAge", "winnerTimeKm", "penetrometer", "time",
                       "prizeCurrency", "name", "raceNumber", "meetingName",
                       "surface", "autostart", "direction"]:
                if k in race:
                    race_record[k] = race[k]
            # Prize breakdown
            pb = race.get("prizeBreakdown")
            if pb:
                race_record["prizeBreakdown"] = pb
            append_jsonl(output_races, race_record)

    # Runners
    runners = rcs.get("runners", {})
    for race_key, runner_list in runners.items():
        if not isinstance(runner_list, list):
            continue
        for runner in runner_list:
            if not isinstance(runner, dict):
                continue
            runner_record = {
                "date": date_iso,
                "source": "paris_turf",
                "type": "runner",
                "idc": idc,
                "scraped_at": datetime.now().isoformat(),
            }
            # Copier tous les champs importants
            important_fields = [
                "id", "horseName", "horseId", "horseUUID", "horseSir", "horseDam",
                "sex", "age", "draw", "weightKg", "liveWeightKg",
                "jockeyName", "jockeyId", "jockeyUUID", "jockeyAllowance", "jockeyChanged",
                "trainerName", "trainerId", "trainerUUID",
                "ownerName", "ownerId", "ownerUUID",
                "breederName", "breederId",
                "numberOfRuns", "numberOfWins", "numberOfPlaces",
                "totalPrize", "totalWinningPrize",
                "formFigs", "redkm", "chrono",
                "blinkers", "blinkersFirstTime", "hood", "tongueTie",
                "shoeing", "shoeingFront", "shoeingBack", "noShoes", "noShoesFirstTime",
                "protectionFirstTime",
                "isRunning", "isSupplemented", "isEngaged",
                "ranking", "margin", "incident", "liveIncident",
                "comment", "emoji", "bestImpression",
                "handicapRatingKg",
                "raceId", "raceNumber", "raceDate", "raceName",
                "raceType", "raceSpeciality", "raceSurface",
                "raceTrackCode", "raceTotalPrize", "raceAutostart", "raceDirection",
                "meetingId", "meetingName",
                "distance", "daysSincePreviousRace", "previousRaceDate", "previousRaceId",
                "saddle",
            ]
            for k in important_fields:
                if k in runner and runner[k] is not None:
                    runner_record[k] = runner[k]
            # Records (historique de performance)
            records = runner.get("records")
            if records:
                runner_record["records"] = records
            # External IDs
            ext = runner.get("externalId")
            if ext:
                runner_record["externalId"] = ext

            append_jsonl(output_runners, runner_record)
            nb_runners += 1

    # Sauvegarder cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump({"idc": idc, "nb_runners": nb_runners, "date": date_iso}, f)

    return nb_runners


def main():
    parser = argparse.ArgumentParser(description="Script 53 — Paris-Turf Scraper (Playwright, Next.js JSON)")
    parser.add_argument("--pages", type=str, nargs="+",
                        default=["hier", "aujourdhui"],
                        help="Pages a scraper (hier, aujourdhui, demain)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 53 — Paris-Turf Scraper (Playwright)")
    log.info(f"  Pages : {args.pages}")
    log.info("=" * 60)

    output_runners = os.path.join(OUTPUT_DIR, "paris_turf_runners.jsonl")
    output_races = os.path.join(OUTPUT_DIR, "paris_turf_races.jsonl")

    total_courses = 0
    total_runners = 0

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(pw)
        log.info("Browser launched (headless Chromium)")

        # Accept cookies on first navigation
        first_nav = True

        for page_slug in args.pages:
            log.info(f"  Page : /programme-courses/{page_slug}")
            course_links, meetings = get_day_courses(page, page_slug)

            if first_nav:
                accept_cookies(page)
                first_nav = False

            log.info(f"    {len(course_links)} courses, {len(meetings)} meetings")

            for i, curl in enumerate(course_links):
                nb = scrape_course(page, curl, datetime.now().strftime("%Y-%m-%d"),
                                   output_runners, output_races)
                total_runners += nb
                total_courses += 1
                if (i + 1) % 10 == 0:
                    log.info(f"    {i+1}/{len(course_links)} courses, {total_runners} runners")
                smart_pause(1.5, 0.8)

        log.info("=" * 60)
        log.info(f"TERMINE: {total_courses} courses, {total_runners} runners")
        log.info(f"  -> {output_runners}")
        log.info(f"  -> {output_races}")
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
