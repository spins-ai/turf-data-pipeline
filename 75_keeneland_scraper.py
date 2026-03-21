#!/usr/bin/env python3
"""
Script 75 — Scraping Keeneland.com (ventes de chevaux US)
Source : keeneland.com/sales, keeneland.com/racing/entries
Collecte : resultats ventes (September Yearling, November Breeding Stock, January),
           prix, pedigree, acheteur/vendeur, historique encheres
CRITIQUE pour : Valuation Model, Pedigree US, Market Intelligence
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "75_keeneland"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("75_keeneland")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.keeneland.com"

# Types de ventes Keeneland
SALE_CODES = [
    "KEESEP",   # September Yearling Sale
    "KEENOV",   # November Breeding Stock Sale
    "KEEJAN",   # January Horses of All Ages Sale
    "KEEAPR",   # April Sale of Two-Year-Olds in Training
]


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
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


def scrape_sale_results_page(session, sale_code, year, page=1):
    """Scraper une page de resultats de vente Keeneland."""
    cache_file = os.path.join(CACHE_DIR, f"sale_{sale_code}_{year}_p{page}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # Essayer differents formats d'URL Keeneland
    urls_to_try = [
        f"{BASE_URL}/sales/results/{sale_code}/{year}?page={page}",
        f"{BASE_URL}/sales/{sale_code}/{year}/results?page={page}",
        f"{BASE_URL}/catalog/{sale_code}/{year}?page={page}",
    ]

    soup = None
    used_url = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            used_url = url
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    lots = []

    # Extraire depuis les tables de resultats
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            lot = {
                "source": "keeneland",
                "sale_code": sale_code,
                "year": year,
                "page": page,
                "type": "sale_result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                lot[key] = cell

            # Extraire prix
            for cell in cells:
                price_match = re.search(r'\$?([\d,]+)', cell)
                if price_match and len(price_match.group(1).replace(",", "")) >= 4:
                    lot["price_usd"] = int(price_match.group(1).replace(",", ""))
                    break

            # Lien detail
            link = row.find("a", href=True)
            if link:
                lot["url_detail"] = link["href"] if link["href"].startswith("http") else f"{BASE_URL}{link['href']}"

            lots.append(lot)

    # Extraire depuis les cartes/divs
    for card in soup.find_all(["div", "article", "li"], class_=True):
        classes = " ".join(card.get("class", []))
        if any(kw in classes.lower() for kw in ["lot", "horse", "result", "hip", "entry"]):
            lot = {
                "source": "keeneland",
                "sale_code": sale_code,
                "year": year,
                "type": "sale_card",
                "scraped_at": datetime.now().isoformat(),
            }

            # Hip number
            hip_el = card.find(string=re.compile(r'Hip\s*#?\s*\d+', re.I))
            if hip_el:
                hip_match = re.search(r'(\d+)', hip_el)
                if hip_match:
                    lot["hip_number"] = int(hip_match.group(1))

            # Horse name
            name_el = card.find(["h3", "h4", "a", "strong"])
            if name_el:
                lot["horse_name"] = name_el.get_text(strip=True)

            # Sire / Dam
            sire_el = card.find(string=re.compile(r'(sire|by)\s*:', re.I))
            if sire_el:
                lot["sire_brut"] = sire_el.find_parent().get_text(strip=True) if sire_el.find_parent() else sire_el.strip()

            dam_el = card.find(string=re.compile(r'(dam|out of)\s*:', re.I))
            if dam_el:
                lot["dam_brut"] = dam_el.find_parent().get_text(strip=True) if dam_el.find_parent() else dam_el.strip()

            # Price
            price_el = card.find(string=re.compile(r'\$[\d,]+'))
            if price_el:
                price_match = re.search(r'\$([\d,]+)', price_el)
                if price_match:
                    lot["price_usd"] = int(price_match.group(1).replace(",", ""))
                    lot["price_brut"] = price_el.strip()

            # Buyer / Consignor
            for label_kw, field in [("buyer", "buyer"), ("purchaser", "buyer"),
                                     ("consignor", "consignor"), ("seller", "consignor")]:
                el = card.find(string=re.compile(label_kw, re.I))
                if el:
                    parent = el.find_parent()
                    if parent:
                        lot[field] = parent.get_text(strip=True)

            if lot.get("horse_name") or lot.get("hip_number"):
                lots.append(lot)

    # Verifier s'il y a une page suivante
    has_next = bool(soup.find("a", string=re.compile(r'next|suivant|>', re.I)))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump({"lots": lots, "has_next": has_next}, f, ensure_ascii=False, indent=2)

    return lots, has_next


def scrape_racing_results(session, date_str):
    """Scraper les resultats de courses Keeneland pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"racing_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/entries/{date_str}"
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
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "source": "keeneland",
                "date": date_str,
                "type": "racing_entry",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 75 — Keeneland Scraper (ventes + courses US)")
    parser.add_argument("--year-start", type=int, default=2015,
                        help="Annee de debut")
    parser.add_argument("--year-end", type=int, default=None,
                        help="Annee de fin (defaut=annee courante)")
    parser.add_argument("--mode", choices=["sales", "racing", "all"], default="all",
                        help="Mode: sales, racing, ou all")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    year_end = args.year_end or datetime.now().year
    log.info("=" * 60)
    log.info("SCRIPT 75 — Keeneland Scraper (ventes + courses US)")
    log.info(f"  Periode : {args.year_start} -> {year_end}")
    log.info(f"  Mode : {args.mode}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "keeneland_data.jsonl")

    total_records = checkpoint.get("total_records", 0)

    # --- Mode SALES ---
    if args.mode in ("sales", "all"):
        log.info("--- Phase 1: Ventes ---")
        last_sale_key = checkpoint.get("last_sale_key", "")
        skip = bool(last_sale_key and args.resume)

        sale_count = 0
        for year in range(args.year_start, year_end + 1):
            for sale_code in SALE_CODES:
                sale_key = f"{sale_code}_{year}"
                if skip:
                    if sale_key == last_sale_key:
                        skip = False
                    continue

                log.info(f"  Vente: {sale_key}")
                page = 1
                sale_lots = 0
                while page <= 50:  # Max 50 pages de securite
                    result = scrape_sale_results_page(session, sale_code, year, page)
                    if isinstance(result, tuple):
                        lots, has_next = result
                    else:
                        lots = result
                        has_next = False

                    if not lots:
                        break

                    for lot in lots:
                        append_jsonl(output_file, lot)
                        total_records += 1
                        sale_lots += 1

                    if not has_next:
                        break
                    page += 1
                    smart_pause(2.0, 1.0)

                sale_count += 1
                log.info(f"    -> {sale_lots} lots")
                save_checkpoint({"last_sale_key": sale_key, "total_records": total_records})
                smart_pause(3.0, 1.5)

                if sale_count % 20 == 0:
                    session.close()
                    session = new_session()
                    time.sleep(random.uniform(5, 15))

    # --- Mode RACING ---
    if args.mode in ("racing", "all"):
        log.info("--- Phase 2: Courses ---")
        # Keeneland a 2 meets par an: avril et octobre
        for year in range(args.year_start, year_end + 1):
            # Spring meet: avril
            for month, days in [(4, 30), (10, 31)]:
                for day in range(1, days + 1):
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    records = scrape_racing_results(session, date_str)
                    if records:
                        for rec in records:
                            append_jsonl(output_file, rec)
                            total_records += 1
                    smart_pause(1.0, 0.5)

    save_checkpoint({"total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
