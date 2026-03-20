#!/usr/bin/env python3
"""
Script 52 — Scraping Turfomania.fr (corrigé)
Source : turfomania.fr — pronostics, partants, stats
URL pattern : /pronostics/partants-xxx.html?idcourse={id}
Utilise cloudscraper pour contourner Cloudflare.
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import cloudscraper
from bs4 import BeautifulSoup

SCRIPT_NAME = "52_turfomania"
OUTPUT_DIR = os.path.join("output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.turfomania.fr"


def new_session():
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )


def smart_pause(base=2.5, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
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


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_course_links(session, page_url):
    """Extraire tous les liens de réunions/courses depuis une page."""
    resp = fetch_with_retry(session, page_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    courses = []
    # Pattern: courses-jeudi-19-mars-2026-caen-r3.html?idreunion=116761
    pattern = re.compile(r'courses-[^"]*\?idreunion=(\d+)')
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = pattern.search(href)
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            if not href.startswith("http"):
                if not href.startswith("/"):
                    href = "/" + href
                href = BASE_URL + href
            courses.append({
                "id_course": m.group(1),
                "url": href,
                "titre": a.get_text(strip=True)[:200],
            })
    # Also check for partants links with idcourse
    pattern2 = re.compile(r'partants[^"]*\?idcourse=(\d+)')
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = pattern2.search(href)
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            if not href.startswith("http"):
                if not href.startswith("/"):
                    href = "/" + href
                href = BASE_URL + href
            courses.append({
                "id_course": m.group(1),
                "url": href,
                "titre": a.get_text(strip=True)[:200],
            })
    return courses


def scrape_course(session, course_info, date_iso):
    """Scraper une course individuelle sur Turfomania."""
    id_course = course_info["id_course"]
    cache_file = os.path.join(CACHE_DIR, f"course_{id_course}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, course_info["url"])
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    info = {
        "date": date_iso,
        "source": "turfomania",
        "type": "course",
        "id_course": id_course,
        "url": course_info["url"],
        "titre_lien": course_info.get("titre", ""),
        "scraped_at": datetime.now().isoformat(),
    }
    h1 = soup.find("h1")
    if h1:
        info["titre"] = h1.get_text(strip=True)
    records.append(info)

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace("°", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                entry = {
                    "date": date_iso,
                    "source": "turfomania",
                    "type": "partant",
                    "id_course": id_course,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    for div in soup.find_all(["div", "span", "p", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["prono", "tip", "selection", "favori",
                                                  "base", "complement", "analyse", "avis"]):
            if text and 5 < len(text) < 1000:
                records.append({
                    "date": date_iso,
                    "source": "turfomania",
                    "type": "pronostic",
                    "id_course": id_course,
                    "contenu": text[:800],
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    for script in soup.find_all("script"):
        st = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', st, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_iso,
                    "source": "turfomania",
                    "type": "embedded_json",
                    "id_course": id_course,
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
    return records


def main():
    parser = argparse.ArgumentParser(description="Script 52 — Turfomania Scraper")
    parser.add_argument("--max-days", type=int, default=0)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 52 — Turfomania Scraper (cloudscraper)")
    log.info("=" * 60)

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "turfomania_data.jsonl")

    log.info("  Scraping page pronostics...")
    course_links = get_course_links(session, f"{BASE_URL}/pronostics/")
    log.info(f"  {len(course_links)} courses trouvées")

    total_records = 0
    date_iso = datetime.now().strftime("%Y-%m-%d")

    for i, course in enumerate(course_links):
        records = scrape_course(session, course, date_iso)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1
        if (i + 1) % 10 == 0:
            log.info(f"  {i+1}/{len(course_links)} courses, {total_records} records")
        smart_pause(1.5, 0.8)

        if (i + 1) % 40 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 10))

    save_checkpoint({
        "last_date": date_iso,
        "total_records": total_records,
        "nb_courses": len(course_links),
        "status": "done",
    })

    log.info("=" * 60)
    log.info(f"TERMINÉ: {len(course_links)} courses, {total_records} records")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
