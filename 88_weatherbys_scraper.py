#!/usr/bin/env python3
"""
Script 88 — Scraping Weatherbys.co.uk
Source : weatherbys.co.uk (UK stud book, official pedigree data)
Collecte : pedigree data, stallion book entries, breeding records, stud book info
CRITIQUE pour : Pedigree Features, Official UK Breeding Data, Stud Book Intelligence
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

SCRIPT_NAME = "88_weatherbys"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause

log = setup_logging("88_weatherbys")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.weatherbys.co.uk"


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def fetch_with_retry(session, url, max_retries=3, timeout=30):
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
            log.warning(f"  Erreur réseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Échec après {max_retries} essais: {url}")
    return None


def append_jsonl(filepath, record):
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


def scrape_weatherbys_stallion_book(session, year):
    """Scraper le stallion book Weatherbys pour une année donnée."""
    cache_file = os.path.join(CACHE_DIR, f"stallion_book_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/stallion-book/search?year={year}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Stallion book tables ---
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
                    "year": str(year),
                    "source": "weatherbys",
                    "type": "stallion_book",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                # Extract links to stallion details
                link = row.find("a", href=True)
                if link:
                    href = link["href"]
                    entry["url_stallion"] = href if href.startswith("http") else f"{BASE_URL}{href}"
                records.append(entry)

    # --- Stallion/stud details sections ---
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["stallion", "stud", "sire", "pedigree",
                                                  "breeding", "bloodline", "dam"]):
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "year": str(year),
                    "source": "weatherbys",
                    "type": "stallion_detail",
                    "contenu": text[:2500],
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- JSON embedded ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "year": str(year),
                    "source": "weatherbys",
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(r'window\[?[\'"]?(__\w+|stallionData|pedigreeData|studData|breedingData)[\'"]?\]?\s*=\s*(\{.+?\});',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "year": str(year),
                    "source": "weatherbys",
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
                    "year": str(year),
                    "source": "weatherbys",
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
                "year": str(year),
                "source": "weatherbys",
                "type": "script_application_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Data-attributes ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["stallion", "sire", "dam", "pedigree", "horse", "breeding", "stud"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "year": str(year),
                "source": "weatherbys",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_weatherbys_pedigree_search(session, letter):
    """Scraper le pedigree search Weatherbys par lettre initiale."""
    cache_file = os.path.join(CACHE_DIR, f"pedigree_{letter}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/stud-book/search?initial={letter}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Pedigree listing ---
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
                    "letter": letter,
                    "source": "weatherbys",
                    "type": "pedigree_entry",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                link = row.find("a", href=True)
                if link:
                    href = link["href"]
                    entry["url_pedigree"] = href if href.startswith("http") else f"{BASE_URL}{href}"
                records.append(entry)

    # --- Horse/pedigree list items ---
    for li in soup.find_all(["li", "div", "a"], class_=True):
        classes = " ".join(li.get("class", []))
        if any(kw in classes.lower() for kw in ["horse", "entry", "result", "pedigree"]):
            text = li.get_text(strip=True)
            if text and 3 < len(text) < 300:
                record = {
                    "letter": letter,
                    "source": "weatherbys",
                    "type": "pedigree_list",
                    "name": text,
                    "scraped_at": datetime.now().isoformat(),
                }
                if li.get("href"):
                    href = li["href"]
                    record["url"] = href if href.startswith("http") else f"{BASE_URL}{href}"
                records.append(record)

    # --- JSON embedded ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "letter": letter,
                    "source": "weatherbys",
                    "type": "pedigree_embedded_json",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "letter": letter,
                "source": "weatherbys",
                "type": "pedigree_script_json",
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
    parser = argparse.ArgumentParser(description="Script 88 — Weatherbys Scraper (UK stud book, pedigree data)")
    parser.add_argument("--start-year", type=int, default=2018,
                        help="Année de début pour le stallion book")
    parser.add_argument("--end-year", type=int, default=None,
                        help="Année de fin, défaut=année courante")
    parser.add_argument("--pedigree-letters", action="store_true", default=True,
                        help="Scraper aussi les pedigrees par lettre A-Z")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_year = args.start_year
    end_year = args.end_year or datetime.now().year

    log.info("=" * 60)
    log.info("SCRIPT 88 — Weatherbys Scraper (UK stud book)")
    log.info(f"  Stallion Book : {start_year} → {end_year}")
    log.info(f"  Pedigree A-Z : {args.pedigree_letters}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "weatherbys_data.jsonl")
    total_records = 0

    # --- Stallion Book par année ---
    last_year = checkpoint.get("last_year", start_year - 1)
    if args.resume:
        start_year = max(start_year, last_year + 1)

    for year in range(start_year, end_year + 1):
        log.info(f"  Stallion book {year}...")
        records = scrape_weatherbys_stallion_book(session, year)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1
        save_checkpoint({"last_year": year, "total_records": total_records})
        smart_pause(2.0, 1.0)

    # --- Pedigree search A-Z ---
    if args.pedigree_letters:
        last_letter = checkpoint.get("last_letter", "")
        letters = [chr(c) for c in range(ord("A"), ord("Z") + 1)]
        start_idx = 0
        if args.resume and last_letter:
            try:
                start_idx = letters.index(last_letter) + 1
            except ValueError:
                start_idx = 0

        for letter in letters[start_idx:]:
            log.info(f"  Pedigree lettre {letter}...")
            records = scrape_weatherbys_pedigree_search(session, letter)
            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1
            save_checkpoint({"last_year": end_year, "last_letter": letter,
                             "total_records": total_records})
            smart_pause(2.0, 1.0)

            if letters.index(letter) % 10 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

    save_checkpoint({"last_year": end_year, "last_letter": "Z",
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {total_records} records → {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
