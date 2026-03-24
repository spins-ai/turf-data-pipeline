#!/usr/bin/env python3
"""
Script 102 — Racing Post Results Scraper
Source : racingpost.com/results
Collecte : résultats courses UK/IRE/FR, partants, positions, temps, cotes SP
URL pattern : /results/YYYY-MM-DD → liste des courses
              /results/{hippo_id}/{hippo_name}/{date}/{race_id} → détails course
"""

import argparse
import json
import os
import random
import sys
import re
import time
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

SCRIPT_NAME = "102_racing_post"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, append_jsonl, load_checkpoint, save_checkpoint, create_session

log = setup_logging("102_racing_post")

BASE_URL = "https://www.racingpost.com"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]



def get_race_links(session, date_str):
    """Récupérer les liens vers les courses individuelles pour un jour."""
    cache_file = os.path.join(CACHE_DIR, f"index_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/results/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links = set()
    # Pattern: /results/1353/newcastle-aw/2026-03-19/913586
    pattern = re.compile(rf'/results/(\d+)/([^/]+)/{re.escape(date_str)}/(\d+)$')
    for a in soup.find_all("a", href=True):
        href = a["href"].split("#")[0]  # Enlever les fragments
        m = pattern.search(href)
        if m:
            links.add((m.group(1), m.group(2), m.group(3)))

    result = sorted(links)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f)
    return result


def scrape_race(session, date_str, hippo_id, hippo_name, race_id):
    """Scraper les détails d'une course individuelle."""
    cache_file = os.path.join(CACHE_DIR, f"race_{date_str}_{race_id}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/results/{hippo_id}/{hippo_name}/{date_str}/{race_id}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Course info ---
    course_info = {
        "date": date_str,
        "source": "racing_post",
        "type": "course",
        "race_id": race_id,
        "hippodrome_id": hippo_id,
        "hippodrome": hippo_name.replace("-", " ").title(),
        "url": url,
        "scraped_at": datetime.now().isoformat(),
    }
    # Titre
    h1 = soup.find("h1")
    if h1:
        course_info["titre"] = h1.get_text(strip=True)
    # Conditions (distance, going, class, etc.)
    for div in soup.find_all(["div", "span", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["condition", "detail", "race-info",
                                                  "distance", "going", "class", "prize"]):
            if text and 3 < len(text) < 500:
                course_info.setdefault("details", []).append(text)
    records.append(course_info)

    # --- Table de résultats ---
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 3:
                entry = {
                    "date": date_str,
                    "source": "racing_post",
                    "type": "runner",
                    "race_id": race_id,
                    "hippodrome": hippo_name.replace("-", " ").title(),
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # --- JSON embarqué (__NEXT_DATA__ ou autre) ---
    for script in soup.find_all("script", {"id": "__NEXT_DATA__"}):
        try:
            data = json.loads(script.string or "")
            page_props = data.get("props", {}).get("pageProps", {})
            if page_props:
                records.append({
                    "date": date_str,
                    "source": "racing_post",
                    "type": "next_data",
                    "race_id": race_id,
                    "hippodrome": hippo_name.replace("-", " ").title(),
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except json.JSONDecodeError:
            pass

    for script in soup.find_all("script", {"type": "application/json"}):
        if script.get("id") == "__NEXT_DATA__":
            continue
        try:
            data = json.loads(script.string or "")
            if data:
                records.append({
                    "date": date_str,
                    "source": "racing_post",
                    "type": "embedded_json",
                    "race_id": race_id,
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except json.JSONDecodeError:
            pass

    # --- Data attributes ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["horse", "runner", "jockey", "trainer", "odds", "sp", "result", "position"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "racing_post",
                "type": "data_attrs",
                "race_id": race_id,
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    return records


def main():
    parser = argparse.ArgumentParser(description="Script 102 — Racing Post Scraper (UK/IRE/FR results)")
    parser.add_argument("--start", type=str, default="2024-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--max-days", type=int, default=0)
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else (datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 102 — Racing Post Scraper")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise checkpoint: {start_date.date()}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "racing_post_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0
    total_races = 0

    while current <= end_date:
        if args.max_days and day_count >= args.max_days:
            break

        date_str = current.strftime("%Y-%m-%d")
        race_links = get_race_links(session, date_str)
        smart_pause(1.0, 0.5)

        if not race_links:
            log.debug(f"  {date_str}: aucune course")
        else:
            log.info(f"  {date_str}: {len(race_links)} courses")
            for hippo_id, hippo_name, race_id in race_links:
                records = scrape_race(session, date_str, hippo_id, hippo_name, race_id)
                if records:
                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1
                    total_races += 1
                smart_pause(1.5, 0.8)

        day_count += 1
        if day_count % 10 == 0:
            log.info(f"  === Jour {day_count}: {total_races} courses, {total_records} records ===")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_races": total_races,
                             "total_records": total_records})

        if day_count % 50 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": (current - timedelta(days=1)).strftime("%Y-%m-%d"),
                     "total_races": total_races, "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {day_count} jours, {total_races} courses, {total_records} records")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
