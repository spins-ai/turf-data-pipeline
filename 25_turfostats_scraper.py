#!/usr/bin/env python3
"""
Script 25 — Scraping Turfostats stats galop
Source : turfostats.com
Collecte les stats détaillées : Keyrace index, style de course, affinité distance
CRITIQUE pour : Track Bias, Pace Profile, Field Strength
"""

import requests
import json
import time
import random
import os
import re
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

SCRIPT_NAME = "25_turfostats"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "DNT": "1",
    })
    return s

def smart_pause(base=2.5, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
    time.sleep(max(1.0, pause))

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
    except Exception:
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
    except Exception:
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

def main():
    log.info("=" * 60)
    log.info("SCRIPT 25 — Turfostats Stats Galop")
    log.info("=" * 60)

    session = new_session()

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
            with open(os.path.join(OUTPUT_DIR, "turfostats_courses.json"), "w") as f:
                json.dump(all_courses, f, ensure_ascii=False)
            with open(os.path.join(OUTPUT_DIR, "turfostats_details.json"), "w") as f:
                json.dump(all_details, f, ensure_ascii=False)

        if day_count % 100 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(0.5, 0.3)

    # Sauvegarde finale
    log.info("Sauvegarde finale...")
    with open(os.path.join(OUTPUT_DIR, "turfostats_courses.json"), "w") as f:
        json.dump(all_courses, f, ensure_ascii=False)
    with open(os.path.join(OUTPUT_DIR, "turfostats_details.json"), "w") as f:
        json.dump(all_details, f, ensure_ascii=False)

    log.info("=" * 60)
    log.info(f"TERMINÉ: {len(all_courses)} courses, {len(all_details)} détails")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
