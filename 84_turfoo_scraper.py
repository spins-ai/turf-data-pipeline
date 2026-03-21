#!/usr/bin/env python3
"""
Script 84 — Scraping Turfoo.fr (corrigé)
Source : turfoo.fr — pronostics, résultats, stats chevaux
URLs réelles:
  /programmes-courses/DDMMYY/reunion{N}-{nom}/courses/
  /programmes-courses/DDMMYY/reunion{N}-{nom}/course{N}-{titre}/
  /resultats-pmu/tierce/archives/
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

import requests
try:
    import cloudscraper
except ImportError:
    cloudscraper = None
from bs4 import BeautifulSoup

SCRIPT_NAME = "84_turfoo"
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

BASE_URL = "https://www.turfoo.fr"


def new_session():
    if cloudscraper:
        s = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    else:
        s = requests.Session()
    s.headers.update({
        "Accept-Language": "fr-FR,fr;q=0.9",
        "DNT": "1",
        "Referer": BASE_URL,
    })
    return s


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
                log.warning(f"  403 sur {url}, pause 30s...")
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


def date_to_turfoo(dt):
    """Convertir datetime en format Turfoo: YYMMDD."""
    return dt.strftime("%y%m%d")


def get_reunions(session, dt):
    """Lister les réunions d'un jour sur Turfoo."""
    date_str = date_to_turfoo(dt)
    date_iso = dt.strftime("%Y-%m-%d")
    cache_file = os.path.join(CACHE_DIR, f"index_{date_iso}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/programmes-courses/{date_str}/"
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    reunions = []

    # Chercher les liens vers les réunions
    pattern = re.compile(rf'/programmes-courses/{date_str}/reunion(\d+)-([^/]+)/courses/')
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            reunions.append({
                "num": m.group(1),
                "slug": m.group(2),
                "url": a["href"],
            })

    # Dédupliquer
    seen = set()
    unique = []
    for r in reunions:
        key = (r["num"], r["slug"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False)
    return unique


def get_courses(session, dt, reunion):
    """Lister les courses d'une réunion."""
    date_str = date_to_turfoo(dt)
    date_iso = dt.strftime("%Y-%m-%d")
    num = reunion["num"]
    slug = reunion["slug"]
    cache_file = os.path.join(CACHE_DIR, f"courses_{date_iso}_R{num}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/programmes-courses/{date_str}/reunion{num}-{slug}/courses/"
    if not url.startswith("http"):
        url = BASE_URL + url
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    courses = []

    # Pattern: /programmes-courses/200326/reunion1-vincennes/course3-prix-de-trappes/
    pattern = re.compile(rf'/programmes-courses/{date_str}/reunion{num}-{re.escape(slug)}/course(\d+)-([^/]+)/')
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            courses.append({
                "num": m.group(1),
                "slug": m.group(2),
                "url": a["href"],
            })

    seen = set()
    unique = []
    for c in courses:
        if c["num"] not in seen:
            seen.add(c["num"])
            unique.append(c)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False)
    return unique


def scrape_course(session, dt, reunion, course):
    """Scraper une course individuelle sur Turfoo."""
    date_iso = dt.strftime("%Y-%m-%d")
    date_str = date_to_turfoo(dt)
    r_num = reunion["num"]
    c_num = course["num"]
    cache_file = os.path.join(CACHE_DIR, f"race_{date_iso}_R{r_num}C{c_num}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = course["url"]
    if not url.startswith("http"):
        url = BASE_URL + url
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Course info
    info = {
        "date": date_iso,
        "source": "turfoo",
        "type": "course",
        "reunion_num": r_num,
        "reunion_slug": reunion["slug"],
        "course_num": c_num,
        "course_slug": course["slug"],
        "url": url,
        "scraped_at": datetime.now().isoformat(),
    }
    h1 = soup.find("h1")
    if h1:
        info["titre"] = h1.get_text(strip=True)
    records.append(info)

    # Tables (partants, pronostics, résultats)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                entry = {
                    "date": date_iso,
                    "source": "turfoo",
                    "type": "partant",
                    "reunion_num": r_num,
                    "course_num": c_num,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # Pronostics textuels
    for div in soup.find_all(["div", "span", "p", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["prono", "tip", "favori", "selection",
                                                  "base", "complement", "analyse"]):
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_iso,
                    "source": "turfoo",
                    "type": "pronostic",
                    "reunion_num": r_num,
                    "course_num": c_num,
                    "contenu": text,
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # JSON embarqué
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_iso,
                    "source": "turfoo",
                    "type": "embedded_json",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data:
                records.append({
                    "date": date_iso,
                    "source": "turfoo",
                    "type": "script_json",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except json.JSONDecodeError:
            pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    return records


def main():
    parser = argparse.ArgumentParser(description="Script 84 — Turfoo Scraper (cloudscraper)")
    parser.add_argument("--start", type=str, default="2024-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--max-days", type=int, default=0)
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else (datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 84 — Turfoo Scraper (cloudscraper)")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date > start_date:
            start_date = resume_date
            log.info(f"  Reprise checkpoint: {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "turfoo_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        if args.max_days and day_count >= args.max_days:
            break

        date_iso = current.strftime("%Y-%m-%d")
        reunions = get_reunions(session, current)
        smart_pause(1.0, 0.5)

        if not reunions:
            log.debug(f"  {date_iso}: aucune réunion")
        else:
            log.info(f"  {date_iso}: {len(reunions)} réunions")
            for reu in reunions:
                courses = get_courses(session, current, reu)
                smart_pause(0.8, 0.4)
                for course in courses:
                    records = scrape_course(session, current, reu, course)
                    if records:
                        for rec in records:
                            append_jsonl(output_file, rec)
                            total_records += 1
                    smart_pause(1.0, 0.5)

        day_count += 1
        if day_count % 10 == 0:
            log.info(f"  === Jour {day_count}: {total_records} records ===")
            save_checkpoint({"last_date": date_iso, "total_records": total_records})

        if day_count % 40 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)

    save_checkpoint({"last_date": (current - timedelta(days=1)).strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {day_count} jours, {total_records} records")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
