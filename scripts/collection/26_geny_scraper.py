#!/usr/bin/env python3
"""
Script 26 — Scraping Geny.com (PMU Group)
Source : geny.com/partants-pmu/{date}
Collecte : pronostics Geny, stats jockeys détaillées, commentaires experts
CRITIQUE pour : Anomaly Detector, Jockey Synergy, Meta Model, Commentaires NLP
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
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint, create_session

SCRIPT_NAME = "26_geny"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")
os.makedirs(CACHE_DIR, exist_ok=True)

log = setup_logging("26_geny_scraper")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


def scrape_day(session, date_str):
    """Scraper la page partants d'un jour"""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    url = f"https://www.geny.com/partants-pmu/{date_str}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return None
    except Exception as e:
        log.debug(f"  Erreur réseau geny: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    result = {"date": date_str, "reunions": [], "courses": []}

    # Extraire les réunions
    for div in soup.find_all(["div", "section", "article"]):
        classes = " ".join(div.get("class", []))

        # Réunions
        if "reunion" in classes.lower() or "meeting" in classes.lower():
            reunion = {"classes": classes, "text": div.get_text(strip=True)[:200]}
            links = div.find_all("a", href=True)
            for link in links:
                if "course" in link["href"].lower() or "partants" in link["href"].lower():
                    reunion["course_url"] = link["href"]
            result["reunions"].append(reunion)

    # Extraire les tables de partants
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 3:
                partant = {"date": date_str, "source": "geny"}
                for j, cell in enumerate(cells):
                    if j < len(headers) and headers[j]:
                        partant[headers[j]] = cell
                    else:
                        partant[f"col_{j}"] = cell
                result["courses"].append(partant)

    # Extraire les pronostics
    pronostics = []
    for div in soup.find_all(["div", "span", "p"]):
        text = div.get_text(strip=True)
        if re.search(r'pronostic|favori|outsider|base|chance', text, re.I):
            if len(text) < 300:
                pronostics.append(text)
    if pronostics:
        result["pronostics_raw"] = pronostics[:20]

    # Commentaires experts
    comments = []
    for div in soup.find_all(["div", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        if "comment" in classes.lower() or "avis" in classes.lower() or "analyse" in classes.lower():
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 1000:
                comments.append(text)
    if comments:
        result["commentaires_experts"] = comments[:10]

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def export_cache_to_jsonl():
    """Export cache to JSONL without scraping."""
    jsonl_file = os.path.join(OUTPUT_DIR, "geny_data.jsonl")
    log.info(f"Export cache → {jsonl_file}")
    jsonl_count = 0
    with open(jsonl_file, "w", encoding="utf-8", newline="\n") as fout:
        for fname in sorted(os.listdir(CACHE_DIR)):
            if not fname.endswith(".json"):
                continue
            cache_path = os.path.join(CACHE_DIR, fname)
            try:
                with open(cache_path, encoding="utf-8") as fin:
                    record = json.load(fin)
                if record:
                    fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                    jsonl_count += 1
            except Exception as e:
                log.debug(f"  Erreur lecture cache {fname}: {e}")
    log.info(f"  JSONL: {jsonl_count} jours écrits → {jsonl_file}")
    return jsonl_count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Script 26 — Geny.com Scraper")
    parser.add_argument("--export", action="store_true",
                        help="Export cache to JSONL without scraping")
    parser.add_argument("--start", type=str, default="2020-01-01",
                        help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), défaut=hier")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Nombre max de jours (0=illimité)")
    args = parser.parse_args()

    if args.export:
        log.info("=" * 60)
        log.info("SCRIPT 26 — Export cache → JSONL (--export)")
        log.info("=" * 60)
        export_cache_to_jsonl()
        return

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else (datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 26 — Geny.com Scraper")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "geny_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        if args.max_days and day_count >= args.max_days:
            log.info(f"  Max {args.max_days} jours atteint, arrêt.")
            break

        date_str = current.strftime("%Y-%m-%d")
        data = scrape_day(session, date_str)

        if data:
            append_jsonl(output_file, data)
            total_records += 1

        day_count += 1
        if day_count % 30 == 0:
            log.info(f"  {date_str} | {total_records} jours collectés")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

        if day_count % 50 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(10, 30))

        current += timedelta(days=1)
        smart_pause(2.0, 1.0)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": (current - timedelta(days=1)).strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    # Agrégation cache → JSONL (reconstruire le JSONL complet depuis le cache)
    jsonl_file = os.path.join(OUTPUT_DIR, "geny_data.jsonl")
    log.info(f"Agrégation cache → {jsonl_file}")
    jsonl_count = 0
    with open(jsonl_file, "w", encoding="utf-8", newline="\n") as fout:
        for fname in sorted(os.listdir(CACHE_DIR)):
            if not fname.endswith(".json"):
                continue
            cache_path = os.path.join(CACHE_DIR, fname)
            try:
                with open(cache_path, encoding="utf-8") as fin:
                    record = json.load(fin)
                if record:
                    fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                    jsonl_count += 1
            except Exception as e:
                log.debug(f"  Erreur lecture cache {fname}: {e}")
    log.info(f"  JSONL: {jsonl_count} jours écrits → {jsonl_file}")

    log.info("=" * 60)
    log.info(f"TERMINÉ: {total_records} jours collectés → {jsonl_file}")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
