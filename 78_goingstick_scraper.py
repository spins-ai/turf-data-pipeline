#!/usr/bin/env python3
"""
Script 78 — Scraping Going/Terrain data (UK sources)
Sources : racingpost.com, britishhorseracing.com, weatherbys, goodtosoft.com
Collecte : etat du terrain (going), GoingStick readings, penetrometre,
           historique going par hippodrome, evolution intra-journee
CRITIQUE pour : Terrain Model, Track Bias, Going-adjusted Speed
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

SCRIPT_NAME = "78_goingstick"
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

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Hippodromes UK majeurs
UK_RACECOURSES = [
    "ascot", "cheltenham", "aintree", "epsom", "goodwood", "newmarket",
    "york", "doncaster", "sandown", "kempton", "newbury", "haydock",
    "leicester", "lingfield", "wolverhampton", "chester", "windsor",
    "salisbury", "bath", "brighton", "carlisle", "catterick", "chepstow",
    "exeter", "ffos-las", "fontwell", "hamilton", "huntingdon", "kelso",
    "market-rasen", "musselburgh", "newton-abbot", "nottingham", "perth",
    "plumpton", "pontefract", "redcar", "ripon", "sedgefield", "southwell",
    "stratford", "taunton", "thirsk", "uttoxeter", "warwick", "wetherby",
    "wincanton", "worcester",
]

# Mapping going descriptions -> valeurs numeriques
GOING_SCALE = {
    "hard": 10.0,
    "firm": 9.0,
    "good to firm": 7.5,
    "good": 6.0,
    "good to soft": 5.0,
    "yielding": 4.5,
    "soft": 4.0,
    "yielding to soft": 3.5,
    "soft to heavy": 2.5,
    "heavy": 2.0,
    "very soft": 1.5,
    "heavy/soft": 2.0,
    "standard": 6.0,
    "standard to slow": 5.0,
    "slow": 4.0,
    "fast": 8.0,
}

# Sources
SOURCES = {
    "bha": "https://www.britishhorseracing.com",
    "rp": "https://www.racingpost.com",
    "goodtosoft": "https://www.goodtosoft.com",
    "ruk": "https://www.racingtv.com",
    "timeform": "https://www.timeform.com",
}


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def smart_pause(base=2.5, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
    time.sleep(max(1.0, pause))


def fetch_with_retry(session, url, max_retries=3, timeout=30):
    """GET avec retry automatique (3 essais puis skip)."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Too Many Requests, pause {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                log.warning(f"  403 Forbidden sur {url}, pause 60s...")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} sur {url} (essai {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Erreur reseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Echec apres {max_retries} essais: {url}")
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


def parse_going_value(going_text):
    """Convertir une description de going en valeur numerique."""
    if not going_text:
        return None
    text = going_text.lower().strip()

    # Chercher d'abord une valeur GoingStick numerique
    gs_match = re.search(r'going\s*stick[:\s]*(\d+\.?\d*)', text)
    if gs_match:
        return float(gs_match.group(1))

    # Chercher un penetrometre
    pen_match = re.search(r'penetrometre[:\s]*(\d+\.?\d*)', text)
    if pen_match:
        return float(pen_match.group(1))

    # Mapper la description
    for desc, val in sorted(GOING_SCALE.items(), key=lambda x: -len(x[0])):
        if desc in text:
            return val

    return None


def scrape_going_day_bha(session, date_str):
    """Scraper l'etat du terrain BHA pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"bha_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{SOURCES['bha']}/racing/going/{date_str}",
        f"{SOURCES['bha']}/fixtures/results/{date_str}",
        f"{SOURCES['bha']}/racing/fixtures/{date_str}",
    ]

    soup = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    records = []

    # Extraire les goings depuis les tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue

            record = {
                "source": "bha",
                "date": date_str,
                "type": "going_report",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Chercher la valeur going
            for cell in cells:
                going_val = parse_going_value(cell)
                if going_val is not None:
                    record["going_numeric"] = going_val
                    record["going_text"] = cell
                    break

            records.append(record)

    # Extraire les goings depuis les divs/sections
    for el in soup.find_all(["div", "span", "p", "li"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() or kw in text.lower()
               for kw in ["going", "ground", "terrain", "goingstick"]):
            if text and 3 < len(text) < 500:
                record = {
                    "source": "bha",
                    "date": date_str,
                    "type": "going_element",
                    "contenu": text,
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                going_val = parse_going_value(text)
                if going_val is not None:
                    record["going_numeric"] = going_val

                # Extraire le nom de l'hippodrome
                for course in UK_RACECOURSES:
                    if course.lower() in text.lower():
                        record["racecourse"] = course
                        break

                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_going_day_rp(session, date_str):
    """Scraper l'etat du terrain Racing Post pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"rp_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{SOURCES['rp']}/racecards/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Chercher les infos going dans les racecards
    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["racecard", "meeting", "fixture"]):
            record = {
                "source": "racing_post",
                "date": date_str,
                "type": "going_racecard",
                "scraped_at": datetime.utcnow().isoformat(),
            }

            # Titre (hippodrome)
            title = section.find(["h2", "h3", "h4"])
            if title:
                record["racecourse"] = title.get_text(strip=True)

            # Going
            going_el = section.find(string=re.compile(r'(going|ground|terrain)', re.I))
            if going_el:
                parent = going_el.find_parent()
                if parent:
                    going_text = parent.get_text(strip=True)
                    record["going_text"] = going_text
                    record["going_numeric"] = parse_going_value(going_text)

            # GoingStick
            gs_el = section.find(string=re.compile(r'goingstick|going\s*stick', re.I))
            if gs_el:
                parent = gs_el.find_parent()
                if parent:
                    gs_text = parent.get_text(strip=True)
                    gs_match = re.search(r'(\d+\.?\d*)', gs_text)
                    if gs_match:
                        record["goingstick_value"] = float(gs_match.group(1))
                    record["goingstick_text"] = gs_text

            if record.get("racecourse") or record.get("going_text"):
                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_goodtosoft(session, racecourse):
    """Scraper l'historique going pour un hippodrome sur goodtosoft.com."""
    cache_file = os.path.join(CACHE_DIR, f"g2s_{racecourse}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{SOURCES['goodtosoft']}/{racecourse}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue

            record = {
                "source": "goodtosoft",
                "racecourse": racecourse,
                "type": "going_history",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extraire la date
            for cell in cells:
                date_match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})', cell)
                if date_match:
                    record["date_brut"] = date_match.group(0)
                    break

            # Going value
            for cell in cells:
                going_val = parse_going_value(cell)
                if going_val is not None:
                    record["going_numeric"] = going_val
                    record["going_text"] = cell
                    break

            records.append(record)

    # Extraire les donnees depuis les graphiques / data-attributes
    for el in soup.find_all(attrs={"data-going": True}):
        records.append({
            "source": "goodtosoft",
            "racecourse": racecourse,
            "type": "going_data_attr",
            "going_data": el.get("data-going"),
            "date_data": el.get("data-date", ""),
            "text": el.get_text(strip=True),
            "scraped_at": datetime.utcnow().isoformat(),
        })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_going_changes(session, date_str):
    """Scraper les changements de going intra-journee."""
    cache_file = os.path.join(CACHE_DIR, f"changes_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls = [
        f"{SOURCES['rp']}/news/going-changes/{date_str}",
        f"{SOURCES['bha']}/racing/going-updates/{date_str}",
    ]

    records = []
    for url in urls:
        resp = fetch_with_retry(session, url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        for el in soup.find_all(["p", "li", "div"]):
            text = el.get_text(strip=True)
            if text and any(kw in text.lower() for kw in ["going", "changed", "now", "watered", "rail"]):
                if 10 < len(text) < 500:
                    record = {
                        "source": "going_change",
                        "date": date_str,
                        "type": "going_change",
                        "contenu": text,
                        "scraped_at": datetime.utcnow().isoformat(),
                    }

                    # Extraire heure
                    time_match = re.search(r'(\d{1,2}[:.]\d{2})', text)
                    if time_match:
                        record["heure"] = time_match.group(1)

                    # Hippodrome
                    for course in UK_RACECOURSES:
                        if course.lower() in text.lower():
                            record["racecourse"] = course
                            break

                    record["going_numeric"] = parse_going_value(text)
                    records.append(record)

        smart_pause(1.5, 0.8)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 78 — GoingStick/Terrain Scraper (UK sources)")
    parser.add_argument("--start", type=str, default="2020-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--mode", choices=["daily", "history", "all"], default="all",
                        help="Mode: daily (jour par jour), history (par hippodrome), all")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 78 — GoingStick/Terrain Scraper (UK)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Mode : {args.mode}")
    log.info(f"  Hippodromes : {len(UK_RACECOURSES)}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "goingstick_data.jsonl")

    total_records = checkpoint.get("total_records", 0)

    # --- Mode DAILY ---
    if args.mode in ("daily", "all"):
        log.info("--- Phase 1: Going quotidien ---")
        last_date = checkpoint.get("last_date")
        current = start_date
        if args.resume and last_date:
            resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            if resume_date > current:
                current = resume_date
                log.info(f"  Reprise au checkpoint : {current.date()}")

        day_count = 0
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # BHA
            records = scrape_going_day_bha(session, date_str)
            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1
            smart_pause(1.5, 0.8)

            # Racing Post
            rp_records = scrape_going_day_rp(session, date_str)
            if rp_records:
                for rec in rp_records:
                    append_jsonl(output_file, rec)
                    total_records += 1
            smart_pause(1.5, 0.8)

            # Going changes
            changes = scrape_going_changes(session, date_str)
            if changes:
                for rec in changes:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | jours={day_count} records={total_records}")
                save_checkpoint({"last_date": date_str, "total_records": total_records})

            if day_count % 60 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

    # --- Mode HISTORY ---
    if args.mode in ("history", "all"):
        log.info("--- Phase 2: Historique par hippodrome ---")
        course_count = 0
        last_course = checkpoint.get("last_course", "")
        skip = bool(last_course and args.resume)

        for course in UK_RACECOURSES:
            if skip:
                if course == last_course:
                    skip = False
                continue

            log.info(f"  Hippodrome: {course}")
            records = scrape_goodtosoft(session, course)
            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1
                log.info(f"    -> {len(records)} records")

            course_count += 1
            smart_pause(2.0, 1.0)

            if course_count % 10 == 0:
                save_checkpoint({
                    "last_course": course,
                    "total_records": total_records,
                })

    save_checkpoint({
        "last_date": end_date.strftime("%Y-%m-%d"),
        "total_records": total_records,
        "status": "done",
    })

    log.info("=" * 60)
    log.info(f"TERMINE: {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
