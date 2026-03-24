#!/usr/bin/env python3
"""
Script 25 — Scraping Turfostats stats galop
Source : turfostats.com
Collecte les stats détaillées : Keyrace index, style de course, affinité distance
CRITIQUE pour : Track Bias, Pace Profile, Field Strength
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json
import time
import random
import os
import re
import sys
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, create_session

SCRIPT_NAME = "25_turfostats"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

log = setup_logging("25_turfostats_scraper")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


def scrape_programme_day(session, date_str):
    """Scraper le programme d'un jour"""
    cache_file = os.path.join(CACHE_DIR, f"programme_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    url = f"https://www.turfostats.com/programme.php?date={date_str}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return None
    except Exception as e:
        log.debug(f"  Erreur réseau turfostats programme: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    courses = []

    # Extraire les liens vers les courses individuelles
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "id_course=" in href:
            course_id = re.search(r'id_course=(\d+)', href)
            if course_id:
                name = link.get_text(strip=True)
                courses.append({
                    "id_course": course_id.group(1),
                    "nom_prix": name,
                    "date": date_str,
                    "url": href if href.startswith("http") else f"https://www.turfostats.com/{href}",
                })

    # Extraire les tables de programme
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 3:
                record = {"date": date_str, "source": "turfostats"}
                for j, cell in enumerate(cells):
                    if j < len(headers) and headers[j]:
                        record[headers[j]] = cell
                    else:
                        record[f"col_{j}"] = cell

                # Chercher un lien course dans la row
                link = row.find("a", href=True)
                if link and "id_course" in link.get("href", ""):
                    course_id = re.search(r'id_course=(\d+)', link["href"])
                    if course_id:
                        record["id_course_turfostats"] = course_id.group(1)

                courses.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(courses, f, ensure_ascii=False, indent=2)

    return courses

def scrape_course_detail(session, course_url, course_id):
    """Scraper les détails d'une course (partants + stats)"""
    cache_file = os.path.join(CACHE_DIR, f"course_{course_id}.json")
    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    try:
        resp = session.get(course_url, timeout=30)
        if resp.status_code != 200:
            return None
    except Exception as e:
        log.debug(f"  Erreur réseau turfostats course: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    result = {"id_course": course_id, "partants": []}

    # Extraire les infos course
    for h in soup.find_all(["h1", "h2", "h3"]):
        text = h.get_text(strip=True)
        if text:
            result["titre"] = text
            break

    # Extraire les tables de partants avec stats
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]

        if len(headers) >= 5:  # Table assez large = probablement les partants
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if cells:
                    partant = {}
                    for j, cell in enumerate(cells):
                        if j < len(headers) and headers[j]:
                            partant[headers[j]] = cell
                        else:
                            partant[f"col_{j}"] = cell
                    result["partants"].append(partant)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result

def export_cache_to_jsonl():
    """Export cache to JSONL without scraping."""
    jsonl_programmes = os.path.join(OUTPUT_DIR, "turfostats_programmes.jsonl")
    jsonl_courses = os.path.join(OUTPUT_DIR, "turfostats_courses.jsonl")
    log.info(f"Export cache → JSONL")
    prog_count = 0
    course_count = 0
    with open(jsonl_programmes, "w", encoding="utf-8") as fp, \
         open(jsonl_courses, "w", encoding="utf-8") as fc:
        for fname in sorted(os.listdir(CACHE_DIR)):
            if not fname.endswith(".json"):
                continue
            cache_path = os.path.join(CACHE_DIR, fname)
            try:
                with open(cache_path, encoding="utf-8") as fin:
                    data = json.load(fin)
            except Exception as e:
                log.debug(f"  Erreur lecture cache {fname}: {e}")
                continue

            if fname.startswith("programme_"):
                if isinstance(data, list):
                    for entry in data:
                        fp.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        prog_count += 1
                else:
                    fp.write(json.dumps(data, ensure_ascii=False) + "\n")
                    prog_count += 1
            elif fname.startswith("course_"):
                fc.write(json.dumps(data, ensure_ascii=False) + "\n")
                course_count += 1
    log.info(f"  JSONL programmes: {prog_count} entrées → {jsonl_programmes}")
    log.info(f"  JSONL courses: {course_count} entrées → {jsonl_courses}")
    return prog_count, course_count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Script 25 — Turfostats Stats Galop")
    parser.add_argument("--export", action="store_true",
                        help="Export cache to JSONL without scraping")
    args = parser.parse_args()

    if args.export:
        log.info("=" * 60)
        log.info("SCRIPT 25 — Export cache → JSONL (--export)")
        log.info("=" * 60)
        export_cache_to_jsonl()
        return

    log.info("=" * 60)
    log.info("SCRIPT 25 — Turfostats Stats Galop")
    log.info("=" * 60)

    session = create_session(USER_AGENTS)

    # Collecter les programmes sur une large plage de dates
    # Turfostats couvre le galop plat français
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2026, 3, 14)
    current = start_date

    all_courses = []
    all_details = []
    day_count = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        courses = scrape_programme_day(session, date_str)

        if courses:
            all_courses.extend(courses)
            # Scraper les détails de chaque course
            for c in courses:
                if c.get("id_course") or c.get("id_course_turfostats"):
                    cid = c.get("id_course") or c.get("id_course_turfostats")
                    curl = c.get("url", f"https://www.turfostats.com/programme.php?id_course={cid}")
                    detail = scrape_course_detail(session, curl, cid)
                    if detail:
                        all_details.append(detail)
                    smart_pause(1.5, 0.8)

        day_count += 1
        if day_count % 30 == 0:
            log.info(f"  {date_str} | {len(all_courses)} courses, {len(all_details)} détails")
            # Sauvegarde intermédiaire
            with open(os.path.join(OUTPUT_DIR, "turfostats_courses.json"), "w", encoding="utf-8") as f:
                json.dump(all_courses, f, ensure_ascii=False)
            with open(os.path.join(OUTPUT_DIR, "turfostats_details.json"), "w", encoding="utf-8") as f:
                json.dump(all_details, f, ensure_ascii=False)

        if day_count % 100 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(0.5, 0.3)

    # Sauvegarde finale
    log.info("Sauvegarde finale...")
    with open(os.path.join(OUTPUT_DIR, "turfostats_courses.json"), "w", encoding="utf-8") as f:
        json.dump(all_courses, f, ensure_ascii=False)
    with open(os.path.join(OUTPUT_DIR, "turfostats_details.json"), "w", encoding="utf-8") as f:
        json.dump(all_details, f, ensure_ascii=False)

    # Agrégation cache → JSONL
    jsonl_programmes = os.path.join(OUTPUT_DIR, "turfostats_programmes.jsonl")
    jsonl_courses = os.path.join(OUTPUT_DIR, "turfostats_courses.jsonl")
    log.info(f"Agrégation cache → JSONL")
    prog_count = 0
    course_count = 0
    with open(jsonl_programmes, "w", encoding="utf-8") as fp, \
         open(jsonl_courses, "w", encoding="utf-8") as fc:
        for fname in sorted(os.listdir(CACHE_DIR)):
            if not fname.endswith(".json"):
                continue
            cache_path = os.path.join(CACHE_DIR, fname)
            try:
                with open(cache_path, encoding="utf-8") as fin:
                    data = json.load(fin)
            except Exception as e:
                log.debug(f"  Erreur lecture cache {fname}: {e}")
                continue

            if fname.startswith("programme_"):
                # Programme files contain a list of course entries
                if isinstance(data, list):
                    for entry in data:
                        fp.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        prog_count += 1
                else:
                    fp.write(json.dumps(data, ensure_ascii=False) + "\n")
                    prog_count += 1
            elif fname.startswith("course_"):
                fc.write(json.dumps(data, ensure_ascii=False) + "\n")
                course_count += 1
    log.info(f"  JSONL programmes: {prog_count} entrées → {jsonl_programmes}")
    log.info(f"  JSONL courses: {course_count} entrées → {jsonl_courses}")

    log.info("=" * 60)
    log.info(f"TERMINÉ: {len(all_courses)} courses, {len(all_details)} détails")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
