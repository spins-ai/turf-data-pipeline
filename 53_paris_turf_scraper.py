#!/usr/bin/env python3
"""
Script 53 — Scraping Paris-Turf.com (corrigé)
Source : paris-turf.com — données riches via __NEXT_DATA__ JSON
URLs réelles :
  /programme-courses/hier → liste meetings + courses du jour
  /programme-courses/aujourdhui
  /course/{hippo}-{prix}-idc-{id} → détails course avec runners complets
Données : runners avec records, stats, ferrage, musique, cotes, etc.
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "53_paris_turf"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.paris-turf.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
}


def new_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def smart_pause(base=2.0, jitter=1.0):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.05:
        pause += random.uniform(5, 12)
    time.sleep(max(1.0, pause))


def fetch_with_retry(session, url, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(60 * attempt)
                continue
            if resp.status_code == 403:
                time.sleep(30)
                continue
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} sur {url} (essai {attempt})")
                time.sleep(5 * attempt)
                continue
            return resp
        except Exception as e:
            log.warning(f"  Erreur: {e} (essai {attempt})")
            time.sleep(5 * attempt)
    return None


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


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_day_courses(session, page_slug):
    """Récupérer les courses d'un jour via la page programme."""
    resp = fetch_with_retry(session, f"{BASE_URL}/programme-courses/{page_slug}")
    if not resp:
        return [], []

    # Extraire les liens de courses
    soup = BeautifulSoup(resp.text, "html.parser")
    course_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/course/" in href and "idc-" in href:
            if not href.startswith("http"):
                href = BASE_URL + href
            course_links.add(href)

    # Extraire __NEXT_DATA__ pour les meetings et races
    next_data = extract_next_data(resp.text)
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


def scrape_course(session, course_url, date_iso, output_runners, output_races):
    """Scraper une course individuelle via __NEXT_DATA__."""
    # Extraire l'ID de la course pour le cache
    idc_match = re.search(r'idc-([a-f0-9]+)', course_url)
    idc = idc_match.group(1) if idc_match else course_url.split("/")[-1]
    cache_file = os.path.join(CACHE_DIR, f"course_{idc[:40]}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
            return cached.get("nb_runners", 0)

    resp = fetch_with_retry(session, course_url)
    if not resp:
        return 0

    next_data = extract_next_data(resp.text)
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
            # Copier les champs clés
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
    parser = argparse.ArgumentParser(description="Script 53 — Paris-Turf Scraper (Next.js JSON)")
    parser.add_argument("--pages", type=str, nargs="+",
                        default=["hier", "aujourdhui"],
                        help="Pages à scraper (hier, aujourdhui, demain)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 53 — Paris-Turf Scraper (Next.js __NEXT_DATA__)")
    log.info(f"  Pages : {args.pages}")
    log.info("=" * 60)

    session = new_session()
    output_runners = os.path.join(OUTPUT_DIR, "paris_turf_runners.jsonl")
    output_races = os.path.join(OUTPUT_DIR, "paris_turf_races.jsonl")

    total_courses = 0
    total_runners = 0

    for page_slug in args.pages:
        log.info(f"  Page : /programme-courses/{page_slug}")
        course_links, meetings = get_day_courses(session, page_slug)
        log.info(f"    {len(course_links)} courses, {len(meetings)} meetings")

        for i, curl in enumerate(course_links):
            nb = scrape_course(session, curl, datetime.now().strftime("%Y-%m-%d"),
                               output_runners, output_races)
            total_runners += nb
            total_courses += 1
            if (i + 1) % 10 == 0:
                log.info(f"    {i+1}/{len(course_links)} courses, {total_runners} runners")
            smart_pause(1.5, 0.8)

    log.info("=" * 60)
    log.info(f"TERMINÉ: {total_courses} courses, {total_runners} runners")
    log.info(f"  → {output_runners}")
    log.info(f"  → {output_races}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
