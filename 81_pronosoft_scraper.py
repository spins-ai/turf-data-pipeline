#!/usr/bin/env python3
"""
Script 81 — Scraping Pronosoft.com
Source : pronosoft.com (pronostics gratuits, base PMU, statistiques)
Collecte : pronostics, résultats, rapports PMU, statistiques chevaux/jockeys
CRITIQUE pour : Pronostics Model, Consensus Tips, Base PMU Features
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

SCRIPT_NAME = "81_pronosoft"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, load_checkpoint, save_checkpoint, append_jsonl, create_session

log = setup_logging("81_pronosoft")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]



def scrape_pronosoft_day(session, date_str):
    """Scraper les pronostics et données Pronosoft pour un jour donné."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # Pronosoft uses date format DD-MM-YYYY in URLs
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_url = dt.strftime("%d-%m-%Y")
    url = f"https://www.pronosoft.com/fr/turf/pronostics/{date_url}/"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Extraire les réunions et courses ---
    for section in soup.find_all(["div", "section", "article", "table"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["race", "course", "reunion", "programme",
                                                  "prono", "tierce", "quarte", "quinte"]):
            record = {
                "date": date_str,
                "source": "pronosoft",
                "scraped_at": datetime.now().isoformat(),
            }
            title_el = section.find(["h2", "h3", "h4", "a", "strong"])
            if title_el:
                record["titre"] = title_el.get_text(strip=True)
            link = section.find("a", href=True)
            if link:
                record["url_course"] = link["href"]
            records.append(record)

    # --- Extraire les tables de pronostics ---
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
                    "source": "pronosoft",
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

    # --- Extraire les pronostics textuels ---
    for div in soup.find_all(["div", "span", "p", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["prono", "tip", "favori", "prediction",
                                                  "selection", "base", "complement"]):
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "pronostic",
                    "contenu": text,
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extraire les résultats/rapports ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["resultat", "result", "rapport", "arrivee",
                                                  "classement", "paiement"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "resultat_rapport",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extraire les stats depuis data-attributes ---
    for el in soup.find_all(attrs={"data-odds": True}):
        records.append({
            "date": date_str,
            "source": "pronosoft",
            "type": "cote_data",
            "odds": el.get("data-odds"),
            "text": el.get_text(strip=True),
            "scraped_at": datetime.now().isoformat(),
        })

    # --- Extraire les commentaires/analyses ---
    for div in soup.find_all(["div", "p", "span", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["comment", "analyse", "avis", "editorial",
                                                  "recap", "resume", "chronique"]):
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "commentaire",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extraire les JSON embarqués dans les scripts ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(r'window\[?[\'"]?(__\w+|raceData|courseData|pronoData|statsData)[\'"]?\]?\s*=\s*(\{.+?\});',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
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
                    "source": "pronosoft",
                    "type": "embedded_var_array",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    # --- Extraire script type="application/json" ---
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "pronosoft",
                "type": "script_application_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Extraire TOUS les data-attributes pertinents ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["cote", "odd", "cheval", "horse", "runner", "race", "prono", "tip"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "pronosoft",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # Sauvegarder cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_pronosoft_stats(session, date_str):
    """Scraper la page stats/base PMU de Pronosoft."""
    cache_file = os.path.join(CACHE_DIR, f"stats_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_url = dt.strftime("%d-%m-%Y")
    url = f"https://www.pronosoft.com/fr/turf/base-pmu/{date_url}/"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Tables de statistiques PMU ---
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
                    "source": "pronosoft",
                    "type": "base_pmu_stats",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # --- Stats jockeys/entraineurs ---
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["jockey", "driver", "entraineur", "trainer",
                                                  "stats", "classement", "ranking"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "stats_acteurs",
                    "contenu": text[:2500],
                    "css_class": classes,
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
                    "source": "pronosoft",
                    "type": "stats_embedded_json",
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
                "source": "pronosoft",
                "type": "stats_script_json",
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
    parser = argparse.ArgumentParser(description="Script 81 — Pronosoft Scraper (pronostics, base PMU, stats)")
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
    log.info("SCRIPT 81 — Pronosoft Scraper")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "pronosoft_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Pronostics page
        records = scrape_pronosoft_day(session, date_str)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(1.5, 0.8)

        # Stats/Base PMU page
        stats = scrape_pronosoft_stats(session, date_str)
        if stats:
            for rec in stats:
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
        smart_pause(1.0, 0.5)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {day_count} jours, {total_records} records → {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
