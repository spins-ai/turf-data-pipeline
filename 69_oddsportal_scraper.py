#!/usr/bin/env python3
"""
Script 69 — Scraping OddsPortal.com
Source : oddsportal.com/horse-racing/
Collecte : cotes historiques multi-bookmakers, mouvements de cotes, odds comparison
CRITIQUE pour : Odds Model, Market Efficiency, Value Detection (etape 7E)
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

try:
    import cloudscraper
except ImportError:
    cloudscraper = None
from bs4 import BeautifulSoup

SCRIPT_NAME = "69_oddsportal"
OUTPUT_DIR = os.path.join("output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

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

BASE_URL = "https://www.oddsportal.com"


def new_session():
    s = cloudscraper.create_scraper() if cloudscraper else requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def smart_pause(base=3.0, jitter=2.0):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
    time.sleep(max(1.5, pause))


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
        except Exception as e:
            log.warning(f"  Erreur reseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Echec apres {max_retries} essais: {url}")
    return None


def append_jsonl(filepath, record):
    """Ajouter un enregistrement JSONL (append mode)."""
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    """Charger le checkpoint de reprise."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    """Sauvegarder le checkpoint."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def scrape_daily_results(session, date_str):
    """Scraper les resultats et cotes OddsPortal pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # OddsPortal utilise le format YYYYMMDD dans les URLs de resultats
    d = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = d.strftime("%Y%m%d")
    url = f"{BASE_URL}/matches/horse-racing/{url_date}/"

    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(resp.text)

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Extraire les evenements de course ---
    for row in soup.find_all("div", class_=True):
        classes = " ".join(row.get("class", []))
        if any(kw in classes.lower() for kw in ["event", "match", "deactivate", "table-participant"]):
            record = {
                "date": date_str,
                "source": "oddsportal",
                "type": "event",
                "scraped_at": datetime.now().isoformat(),
            }

            # Nom de l'evenement / course
            name_el = row.find(["a", "span", "p"], class_=lambda c: c and any(
                kw in c for kw in ["name", "event", "participant"]))
            if name_el:
                record["nom_event"] = name_el.get_text(strip=True)
                if name_el.name == "a" and name_el.get("href"):
                    record["url_detail"] = name_el["href"]

            # Pays / Hippodrome
            country_el = row.find(["span", "a"], class_=lambda c: c and "country" in c) if row else None
            if country_el:
                record["pays"] = country_el.get_text(strip=True)

            records.append(record)

    # --- Extraire les tables de cotes ---
    for table in soup.find_all("table"):
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
                    "source": "oddsportal",
                    "type": "odds_row",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    rec[key] = cell

                # Extraire les cotes numeriques
                for cell in cells:
                    odds_match = re.search(r'(\d+\.\d+)', cell)
                    if odds_match:
                        rec["best_odds"] = odds_match.group(1)
                        break

                records.append(rec)

    # --- Extraire les bookmakers et cotes depuis data-attributes ---
    for el in soup.find_all(attrs={"data-odd": True}):
        records.append({
            "date": date_str,
            "source": "oddsportal",
            "type": "bookmaker_odd",
            "odd_value": el.get("data-odd"),
            "bookmaker": el.get("data-bk", ""),
            "text": el.get_text(strip=True),
            "scraped_at": datetime.now().isoformat(),
        })

    # --- Extraire les odds history (mouvements) ---
    for div in soup.find_all("div", class_=lambda c: c and any(
            kw in c for kw in ["odds-movement", "history", "graph", "closing"])):
        text = div.get_text(strip=True)
        if text and len(text) > 3:
            records.append({
                "date": date_str,
                "source": "oddsportal",
                "type": "odds_movement",
                "contenu": text[:500],
                "scraped_at": datetime.now().isoformat(),
            })

    # Sauvegarder cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_event_detail(session, event_url, date_str):
    """Scraper le detail d'un evenement pour les cotes comparees multi-bookmakers."""
    if not event_url.startswith("http"):
        event_url = f"{BASE_URL}{event_url}"

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', event_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, event_url)
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

    # Extraire comparaison de cotes par bookmaker
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
                "source": "oddsportal",
                "type": "odds_comparison",
                "nom_course": nom_course,
                "url_event": event_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                detail[key] = cell
            details.append(detail)

    # Cotes d'ouverture / cloture
    for span in soup.find_all(["span", "div"], class_=lambda c: c and any(
            kw in c for kw in ["opening", "closing", "average", "highest"])):
        text = span.get_text(strip=True)
        odds_match = re.search(r'(\d+\.\d+)', text)
        if odds_match:
            details.append({
                "date": date_str,
                "source": "oddsportal",
                "type": "odds_summary",
                "nom_course": nom_course,
                "label": " ".join(span.get("class", [])),
                "odds_value": odds_match.group(1),
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False, indent=2)

    return details


def main():
    parser = argparse.ArgumentParser(
        description="Script 69 — OddsPortal Scraper (cotes historiques multi-bookmakers)")
    parser.add_argument("--start", type=str, default="2020-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--detail", action="store_true", default=False,
                        help="Scraper aussi le detail de chaque evenement")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 69 — OddsPortal Scraper (horse-racing odds)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Detail events : {args.detail}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date > start_date:
            start_date = resume_date
            log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "oddsportal_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        records = scrape_daily_results(session, date_str)

        if records:
            # Optionnel : scraper les details de chaque evenement
            if args.detail:
                event_urls = [r.get("url_detail") for r in records
                              if r.get("url_detail")]
                for eurl in set(filter(None, event_urls)):
                    detail = scrape_event_detail(session, eurl, date_str)
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
            save_checkpoint({"last_date": date_str, "total_records": total_records})

        if day_count % 80 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(1.5, 0.8)

    save_checkpoint({"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {day_count} jours, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
