#!/usr/bin/env python3
"""
Script 54 — Scraping turf-fr.com (ex-TurfInfo)
Retargets from defunct turfinfo.fr to turf-fr.com which provides the same data:
courses, partants, cotes, resultats, musique.

Source : turf-fr.com/programmes-courses/{YYYYMMDD}
         turf-fr.com/courses-pmu/arrivees-rapports/{slug}
Collecte : informations detaillees de courses, partants, cotes, resultats
CRITIQUE pour : Race Detail Features, Partant History, Form Analysis

Usage:
    python 54_turfinfo_scraper.py --start 2024-01-01 --end 2024-03-31
    python 54_turfinfo_scraper.py --start 2025-03-15 --end 2025-03-15  # single day test
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

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

SCRIPT_NAME = "54_turfinfo"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("54_turfinfo")

# =========================================================================
# HTTP Session (with retry)
# =========================================================================

BASE_URL = "https://www.turf-fr.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
}


def create_session():
    """Create a requests session with retry adapter."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=2,
                  status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# =========================================================================
# Programme scraping — list of courses for a day
# =========================================================================

def scrape_programme_day(session, date_str):
    """Scrape the turf-fr.com programme page for a given date (YYYY-MM-DD).

    Returns dict with 'records' and 'course_links' or None on failure.
    """
    cache_file = os.path.join(CACHE_DIR, f"programme_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # turf-fr.com uses YYYYMMDD format
    date_compact = date_str.replace("-", "")
    url = f"{BASE_URL}/programmes-courses/{date_compact}"

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            log.debug(f"  No programme for {date_str} (404)")
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"  Failed to fetch programme for {date_str}: {e}")
        return None

    html = resp.text

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []
    course_links = set()

    # --- Extract reunions from the mega-menu or main content ---
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["reunion", "meeting", "hippodrome",
                                                   "itemmenucourse", "head-race"]):
            record = {
                "date": date_str,
                "source": "turf-fr",
                "type": "reunion",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong", "a"])
            if title:
                text = title.get_text(strip=True)
                # Extract reunion number and hippodrome: "R1 - Vincennes"
                m = re.match(r'(R\d+)\s*[-–]\s*(.*)', text)
                if m:
                    record["reunion_num"] = m.group(1)
                    record["hippodrome"] = m.group(2).strip()
                else:
                    record["hippodrome"] = text

            text_content = div.get_text(strip=True)
            if text_content and len(text_content) < 500:
                record["resume"] = text_content[:300]
            records.append(record)

    # --- Collect course links ---
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Partants pages (for upcoming races)
        if "/courses-pmu/partants/" in href:
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            course_links.add(full_url)
        # Arrivees-rapports pages (for past races with results)
        elif "/courses-pmu/arrivees-rapports/" in href and href != "/courses-pmu/arrivees-rapports/":
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            course_links.add(full_url)

    # Also check the arrivees page for this date (may have more result links)
    arrivees_url = f"{BASE_URL}/arrivees-rapports-pmu/{date_compact}"
    try:
        resp_arr = session.get(arrivees_url, timeout=30)
        if resp_arr.status_code == 200:
            soup_arr = BeautifulSoup(resp_arr.text, "html.parser")
            for a in soup_arr.find_all("a", href=True):
                href = a["href"]
                if "/courses-pmu/arrivees-rapports/" in href and href != "/courses-pmu/arrivees-rapports/":
                    full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                    course_links.add(full_url)
    except requests.RequestException:
        pass

    result = {"records": records, "course_links": sorted(course_links)}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


# =========================================================================
# Course detail scraping — partants, cotes, resultats
# =========================================================================

def scrape_course_detail(session, course_url, date_str):
    """Scrape detailed race info: partants, cotes, results."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', course_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    try:
        resp = session.get(course_url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"  Failed to fetch detail {course_url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Race title and conditions ---
    nom_prix = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            nom_prix = text
            break

    # Extract conditions from page text
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(r'(\d{3,5})\s*m(?:[eè]tre)?', page_text)
    if dist_match:
        conditions["distance_m"] = dist_match.group(1).replace(" ", "")

    dotation_match = re.search(r'(\d[\d\s,.]*)\s*\u20ac', page_text)
    if dotation_match:
        conditions["dotation"] = dotation_match.group(0)

    disc_match = re.search(r'(trot attel[eé]|trot mont[eé]|plat|haies|steeple(?:-chase)?|cross)',
                           page_text, re.I)
    if disc_match:
        conditions["discipline"] = disc_match.group(1)

    terrain_match = re.search(r'terrain\s*:?\s*([\w\s]+)', page_text, re.I)
    if terrain_match:
        conditions["terrain"] = terrain_match.group(1).strip()[:50]

    is_results_page = "/arrivees-rapports/" in course_url

    # --- Extract partants from tables ---
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            # Skip empty separator rows
            if all(c in ("", "-") for c in cells):
                continue

            record_type = "resultat_partant" if is_results_page else "partant_detail"
            record = {
                "date": date_str,
                "source": "turf-fr",
                "type": record_type,
                "nom_prix": nom_prix,
                "conditions": conditions,
                "url_course": course_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extract musique (form sequence)
            for cell in cells:
                musique_match = re.search(r'([0-9DATapRrHh]{5,})', cell)
                if musique_match:
                    record["musique"] = musique_match.group(1)
                    break

            # Extract cote (odds)
            for cell in cells:
                cote_match = re.search(r'(\d+\.?\d*)\s*/\s*1', cell)
                if cote_match:
                    record["cote"] = float(cote_match.group(1))
                    break

            # Extract poids
            for cell in cells:
                poids_match = re.search(r'(\d{2}[.,]?\d?)\s*kg', cell)
                if poids_match:
                    record["poids_kg"] = poids_match.group(1).replace(",", ".")
                    break

            records.append(record)

    # --- Comments / analyses ---
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "analyse", "expert", "avis",
                                                   "resume", "verdict", "recap",
                                                   "race-comment", "description", "editorial"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "commentaire_course",
                    "nom_prix": nom_prix,
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# =========================================================================
# Main
# =========================================================================

def main():
    parser = argparse.ArgumentParser(description="Script 54 — TurfInfo/Turf-FR Scraper")
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--no-resume", action="store_true", default=False,
                        help="Ignorer le checkpoint et recommencer")
    parser.add_argument("--max-courses-per-day", type=int, default=20,
                        help="Max course details to scrape per day")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 54 — Turf-FR Scraper (ex-TurfInfo)")
    log.info(f"  Source : {BASE_URL}")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    if not args.no_resume:
        checkpoint = load_checkpoint(CHECKPOINT_FILE)
        last_date = checkpoint.get("last_date")
        if args.resume and last_date:
            resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            if resume_date > start_date:
                start_date = resume_date
                log.info(f"  Reprise au checkpoint : {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "turfinfo_data.jsonl")
    session = create_session()

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        result = scrape_programme_day(session, date_str)

        if result:
            records = result.get("records", [])
            course_links = result.get("course_links", [])

            log.debug(f"  {date_str} | {len(course_links)} course links found")

            # Scrape details of each course (capped)
            for curl in course_links[:args.max_courses_per_day]:
                detail = scrape_course_detail(session, curl, date_str)
                if detail:
                    records.extend(detail)
                smart_pause(0.8, 0.4)

            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 10 == 0:
            log.info(f"  {date_str} | jours={day_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

        current += timedelta(days=1)
        smart_pause(0.5, 0.3)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {day_count} jours, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
