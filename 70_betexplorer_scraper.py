#!/usr/bin/env python3
"""
Script 70 — Scraping BetExplorer.com
Source : betexplorer.com/horse-racing/
Collecte : cotes historiques, resultats, odds finales par bookmaker
CRITIQUE pour : Odds Model, Market Analysis, Closing Line Value (etape 7E)
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

SCRIPT_NAME = "70_betexplorer"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, append_jsonl, load_checkpoint, save_checkpoint, create_session

log = setup_logging("70_betexplorer")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.betexplorer.com"



def scrape_results_page(session, date_str):
    """Scraper la page de resultats BetExplorer pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    d = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = d.strftime("%Y%m%d")
    url = f"{BASE_URL}/results/horse-racing/?year={d.year}&month={d.month}&day={d.day}"

    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Extraire les reunions / hippodromes ---
    for header in soup.find_all(["h2", "h3", "div"], class_=True):
        classes = " ".join(header.get("class", []))
        if any(kw in classes.lower() for kw in ["league", "tournament", "group-header", "country"]):
            text = header.get_text(strip=True)
            if text and len(text) > 2:
                records.append({
                    "date": date_str,
                    "source": "betexplorer",
                    "type": "reunion",
                    "hippodrome": text,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extraire les resultats et cotes depuis les tables ---
    for table in soup.find_all("table", class_=True):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                rec = {
                    "date": date_str,
                    "source": "betexplorer",
                    "type": "result_row",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    rec[key] = cell

                # Lien vers le detail
                link = tr.find("a", href=True)
                if link:
                    rec["url_detail"] = link["href"]
                    rec["nom_course"] = link.get_text(strip=True)

                # Extraire cotes
                for cell in cells:
                    odds_match = re.search(r'(\d+\.\d+)', cell)
                    if odds_match:
                        rec["odds"] = odds_match.group(1)
                        break

                records.append(rec)

    # --- Extraire les cotes depuis data-attributes ---
    for el in soup.find_all(attrs={"data-odd": True}):
        records.append({
            "date": date_str,
            "source": "betexplorer",
            "type": "data_odd",
            "odd_value": el.get("data-odd"),
            "bookmaker": el.get("data-bookmaker", ""),
            "text": el.get_text(strip=True),
            "scraped_at": datetime.now().isoformat(),
        })

    # --- Extraire les scores / resultats ---
    for span in soup.find_all(["span", "td"], class_=lambda c: c and any(
            kw in c for kw in ["score", "result", "winner", "finish"])):
        text = span.get_text(strip=True)
        if text and len(text) > 0:
            records.append({
                "date": date_str,
                "source": "betexplorer",
                "type": "result_info",
                "contenu": text[:300],
                "scraped_at": datetime.now().isoformat(),
            })

    # Sauvegarder cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_race_detail(session, race_url, date_str):
    """Scraper le detail d'une course pour les cotes par bookmaker."""
    if not race_url.startswith("http"):
        race_url = f"{BASE_URL}{race_url}"

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', race_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, race_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    details = []

    # Nom de la course
    nom_course = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            nom_course = text
            break

    # Infos de la course (distance, terrain, etc.)
    info_divs = soup.find_all("div", class_=lambda c: c and any(
        kw in c for kw in ["info", "detail", "meta", "specification"]))
    course_info = {}
    for div in info_divs:
        text = div.get_text(strip=True)
        if text and len(text) < 300:
            # Extraire distance
            dist_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m|km|f|miles?|furlongs?)', text, re.IGNORECASE)
            if dist_match:
                course_info["distance"] = dist_match.group(0)
            # Extraire terrain / going
            going_match = re.search(
                r'(good|firm|soft|heavy|standard|yielding|good to firm|good to soft)',
                text, re.IGNORECASE)
            if going_match:
                course_info["terrain"] = going_match.group(0)

    # Extraire les cotes par bookmaker depuis les tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            detail = {
                "date": date_str,
                "source": "betexplorer",
                "type": "odds_detail",
                "nom_course": nom_course,
                "url_course": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            detail.update(course_info)
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                detail[key] = cell
            details.append(detail)

    # Closing odds
    for el in soup.find_all(["span", "td"], class_=lambda c: c and any(
            kw in c for kw in ["closing", "final", "average"])):
        text = el.get_text(strip=True)
        odds_match = re.search(r'(\d+\.\d+)', text)
        if odds_match:
            details.append({
                "date": date_str,
                "source": "betexplorer",
                "type": "closing_odds",
                "nom_course": nom_course,
                "label": " ".join(el.get("class", [])),
                "odds_value": odds_match.group(1),
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False, indent=2)

    return details


def main():
    parser = argparse.ArgumentParser(
        description="Script 70 — BetExplorer Scraper (cotes historiques, resultats)")
    parser.add_argument("--start", type=str, default="2020-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--detail", action="store_true", default=False,
                        help="Scraper aussi le detail de chaque course")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 70 — BetExplorer Scraper (horse-racing betting data)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Detail courses : {args.detail}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "betexplorer_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        records = scrape_results_page(session, date_str)

        if records:
            # Optionnel : scraper les details
            if args.detail:
                race_urls = [r.get("url_detail") for r in records
                             if r.get("url_detail")]
                for rurl in set(filter(None, race_urls)):
                    detail = scrape_race_detail(session, rurl, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(2.0, 1.0)

            # Ecrire en JSONL
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | jours={day_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

        if day_count % 80 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(1.5, 0.8)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {day_count} jours, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
