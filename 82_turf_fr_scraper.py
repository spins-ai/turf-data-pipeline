#!/usr/bin/env python3
"""
Script 82 — Scraping Turf-FR.com
Source : turf-fr.com (pronostics, résultats, rapports FR)
Collecte : pronostics courses, résultats, rapports de gains, analyses
CRITIQUE pour : Pronostics Consensus, Résultats Validation, Rapports Features
"""

import argparse
import json
import logging
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "82_turf_fr"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry

log = setup_logging("82_turf_fr")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def append_jsonl(filepath, record):
    """Ajouter un enregistrement JSONL (append mode)."""
    with open(filepath, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def scrape_turf_fr_day(session, date_str):
    """Scraper les pronostics et résultats turf-fr.com pour un jour donné."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_url = dt.strftime("%d-%m-%Y")
    url = f"https://www.turf-fr.com/pronostic/{date_url}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Extraire les réunions/courses ---
    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["race", "course", "reunion", "programme",
                                                  "prono", "quinte", "tierce"]):
            record = {
                "date": date_str,
                "source": "turf-fr",
                "scraped_at": datetime.now().isoformat(),
            }
            title_el = section.find(["h2", "h3", "h4", "a", "strong"])
            if title_el:
                record["titre"] = title_el.get_text(strip=True)
            link = section.find("a", href=True)
            if link:
                record["url_course"] = link["href"]
            records.append(record)

    # --- Tables de pronostics/partants ---
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                partant = {
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "pronostic_table",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    partant[key] = cell
                for cell in cells:
                    cote_match = re.search(r'(\d+[.,]\d+)', cell)
                    if cote_match:
                        partant["cote"] = cote_match.group(1).replace(",", ".")
                        break
                records.append(partant)

    # --- Pronostics textuels ---
    for div in soup.find_all(["div", "span", "p", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["prono", "tip", "favori", "selection",
                                                  "base", "complement", "prediction"]):
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "pronostic",
                    "contenu": text,
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Résultats et rapports ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["resultat", "result", "rapport", "arrivee",
                                                  "classement", "gain", "paiement"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "resultat_rapport",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Commentaires/analyses ---
    for div in soup.find_all(["div", "p", "span", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["comment", "analyse", "avis", "editorial",
                                                  "recap", "resume", "chronique"]):
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "commentaire",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Cotes depuis data-attributes ---
    for el in soup.find_all(attrs={"data-odds": True}):
        records.append({
            "date": date_str,
            "source": "turf-fr",
            "type": "cote_data",
            "odds": el.get("data-odds"),
            "text": el.get_text(strip=True),
            "scraped_at": datetime.now().isoformat(),
        })

    # --- JSON embarqués ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(r'window\[?[\'"]?(__\w+|raceData|courseData|pronoData)[\'"]?\]?\s*=\s*(\{.+?\});',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "embedded_window_data",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "embedded_var_array",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "turf-fr",
                "type": "script_application_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Data-attributes pertinents ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["cote", "odd", "cheval", "horse", "runner", "race", "prono"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "turf-fr",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_turf_fr_resultats(session, date_str):
    """Scraper la page résultats de turf-fr.com."""
    cache_file = os.path.join(CACHE_DIR, f"resultats_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_url = dt.strftime("%d-%m-%Y")
    url = f"https://www.turf-fr.com/resultats/{date_url}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Tables de résultats ---
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
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "resultat_table",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # --- Rapports de gains ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["rapport", "gain", "paiement", "dividende"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "rapport_gains",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- JSON embarqué ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "turf-fr",
                    "type": "resultats_embedded_json",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "turf-fr",
                "type": "resultats_script_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 82 — Turf-FR Scraper (pronostics, résultats, rapports)")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), défaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 82 — Turf-FR Scraper")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "turf_fr_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        records = scrape_turf_fr_day(session, date_str)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(1.5, 0.8)

        resultats = scrape_turf_fr_resultats(session, date_str)
        if resultats:
            for rec in resultats:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | jours={day_count} records={total_records}")
            save_checkpoint({"last_date": date_str, "total_records": total_records})

        if day_count % 80 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(1.0, 0.5)

    save_checkpoint({"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {day_count} jours, {total_records} records → {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
