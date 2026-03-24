#!/usr/bin/env python3
"""
Script 83 — Scraping LeTrot.com (corrigé)
Source : letrot.com — site officiel du trot français
Collecte : programmes, résultats, partants, stats
URL réelle : /courses/YYYY-MM-DD  puis  /courses/YYYY-MM-DD/{hippo_id}/{num_course}
Utilise cloudscraper pour contourner anti-bot.
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

SCRIPT_NAME = "83_letrot"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, load_checkpoint, save_checkpoint, append_jsonl, create_session
from utils.html_parsing import extract_race_links

log = setup_logging("83_letrot")

BASE_URL = "https://www.letrot.com"


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]


def scrape_race_page(session, date_str, hippo_id, race_num):
    """Scraper une course individuelle."""
    cache_file = os.path.join(CACHE_DIR, f"race_{date_str}_{hippo_id}_{race_num}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/courses/{date_str}/{hippo_id}/{race_num}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Métadonnées de la course ---
    course_info = {
        "date": date_str,
        "source": "letrot",
        "type": "course_info",
        "hippodrome_id": hippo_id,
        "numero_course": race_num,
        "url": url,
        "scraped_at": datetime.now().isoformat(),
    }
    title = soup.find("h1") or soup.find("h2")
    if title:
        course_info["titre"] = title.get_text(strip=True)
    for div in soup.find_all(["div", "p", "span"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["condition", "detail", "info-course",
                                                  "distance", "allocation", "discipline"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 1000:
                course_info["conditions"] = course_info.get("conditions", "") + " " + text
    records.append(course_info)

    # --- Tables de partants/résultats ---
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
                    "date": date_str,
                    "source": "letrot",
                    "type": "partant",
                    "hippodrome_id": hippo_id,
                    "numero_course": race_num,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # --- JSON embarqué dans les scripts ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "letrot",
                    "type": "embedded_json",
                    "hippodrome_id": hippo_id,
                    "numero_course": race_num,
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(
            r'window\[?[\'"]?(\w+)[\'"]?\]?\s*=\s*(\{[\s\S]+?\});',
            script_text
        ):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "letrot",
                    "type": "embedded_window",
                    "var_name": m.group(1),
                    "hippodrome_id": hippo_id,
                    "numero_course": race_num,
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    # --- application/json scripts ---
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "letrot",
                "type": "script_json",
                "data_id": script.get("id", ""),
                "hippodrome_id": hippo_id,
                "numero_course": race_num,
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Data-attributes ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["cheval", "horse", "runner", "race", "cote", "odd", "partant", "driver"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "letrot",
                "type": "data_attrs",
                "hippodrome_id": hippo_id,
                "numero_course": race_num,
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # Sauvegarder cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_day(session, date_str):
    """Scraper toutes les courses d'un jour via la page de listing."""
    cache_index = os.path.join(CACHE_DIR, f"index_{date_str}.json")

    if os.path.exists(cache_index):
        with open(cache_index, "r", encoding="utf-8") as f:
            return json.load(f)

    # Essayer /courses/YYYY-MM-DD
    url = f"{BASE_URL}/courses/{date_str}"
    resp = fetch_with_retry(session, url)
    race_links = []
    if resp:
        soup = BeautifulSoup(resp.text, "html.parser")
        race_links = extract_race_links(soup, base_url=BASE_URL)

    with open(cache_index, "w", encoding="utf-8") as f:
        json.dump(race_links, f)

    return race_links


def main():
    parser = argparse.ArgumentParser(description="Script 83 — LeTrot Scraper (courses trot)")
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), défaut=hier")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Nombre max de jours à traiter (0=illimité)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else (datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 83 — LeTrot Scraper (cloudscraper)")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "letrot_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0
    empty_days = 0

    while current <= end_date:
        if args.max_days and day_count >= args.max_days:
            log.info(f"  Max {args.max_days} jours atteint, arrêt.")
            break

        date_str = current.strftime("%Y-%m-%d")

        race_links = scrape_day(session, date_str)
        smart_pause(1.5, 0.8)

        if not race_links:
            empty_days += 1
            log.debug(f"  {date_str}: aucune course trouvée")
        else:
            log.info(f"  {date_str}: {len(race_links)} courses trouvées")
            for hippo_id, race_num in race_links:
                records = scrape_race_page(session, date_str, hippo_id, race_num)
                if records:
                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1
                smart_pause(1.0, 0.5)

        day_count += 1

        if day_count % 10 == 0:
            log.info(f"  Progression: {day_count} jours, {total_records} records, {empty_days} jours vides")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records,
                             "day_count": day_count})

        if day_count % 50 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": (current - timedelta(days=1)).strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {day_count} jours, {total_records} records → {output_file}")
    log.info(f"  Jours vides: {empty_days}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
