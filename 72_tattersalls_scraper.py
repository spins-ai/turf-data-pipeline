#!/usr/bin/env python3
"""
Script 72 — Scraping Tattersalls.com
Source : tattersalls.com (ventes UK/IRE)
Collecte : donnees de ventes (prix, acheteur, vendeur, pedigree, lot details)
CRITIQUE pour : Valuation Model, Market Value, Bloodline Pricing (etape 7F)
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "72_tattersalls"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause

log = setup_logging("72_tattersalls")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.tattersalls.com"

# Catalogues de ventes Tattersalls connus
SALE_TYPES = [
    "october-yearling-sale",
    "december-mare-sale",
    "december-yearling-sale",
    "february-sale",
    "guineas-breeze-up-sale",
    "july-sale",
    "autumn-horses-in-training-sale",
    "december-foal-sale",
    "park-paddocks-august-sale",
    "somerville-tattersall-sale",
]


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9,fr;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


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
    """Ajouter un enregistrement JSONL (append mode)."""
    with open(filepath, "a", encoding="utf-8", newline="\n") as f:
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


def scrape_sales_catalogue(session, year, sale_type):
    """Scraper le catalogue d'une vente Tattersalls pour une annee donnee."""
    cache_file = os.path.join(CACHE_DIR, f"catalogue_{year}_{sale_type}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/sales/{year}/{sale_type}/results"
    resp = fetch_with_retry(session, url)
    if not resp:
        # Essayer une URL alternative
        url = f"{BASE_URL}/sales/results/{year}/{sale_type}"
        resp = fetch_with_retry(session, url)
        if not resp:
            return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Extraire la liste des lots ---
    for row in soup.find_all(["tr", "div", "article"], class_=True):
        classes = " ".join(row.get("class", []))
        if any(kw in classes.lower() for kw in ["lot", "result", "entry", "catalogue-item"]):
            record = {
                "year": year,
                "sale_type": sale_type,
                "source": "tattersalls",
                "type": "lot",
                "scraped_at": datetime.now().isoformat(),
            }

            # Numero de lot
            lot_el = row.find(["td", "span", "div"], class_=lambda c: c and "lot" in c.lower()) if row else None
            if lot_el:
                lot_text = lot_el.get_text(strip=True)
                lot_match = re.search(r'(\d+)', lot_text)
                if lot_match:
                    record["lot_number"] = int(lot_match.group(1))

            # Nom du cheval
            name_el = row.find(["a", "span", "h3", "h4"], class_=lambda c: c and any(
                kw in c for kw in ["name", "horse", "lot-name"]))
            if name_el:
                record["nom_cheval"] = name_el.get_text(strip=True)
                if name_el.name == "a" and name_el.get("href"):
                    record["url_lot"] = name_el["href"]

            # Prix de vente
            price_el = row.find(["td", "span", "div"], class_=lambda c: c and any(
                kw in c for kw in ["price", "amount", "result", "hammer"]))
            if price_el:
                price_text = price_el.get_text(strip=True)
                price_match = re.search(r'[\d,]+', price_text.replace(" ", ""))
                if price_match:
                    record["prix_gns"] = price_match.group(0).replace(",", "")
                    record["prix_raw"] = price_text

            # Acheteur
            buyer_el = row.find(["td", "span"], class_=lambda c: c and any(
                kw in c for kw in ["buyer", "purchaser", "agent"]))
            if buyer_el:
                record["acheteur"] = buyer_el.get_text(strip=True)

            # Vendeur / consignor
            vendor_el = row.find(["td", "span"], class_=lambda c: c and any(
                kw in c for kw in ["vendor", "consignor", "seller"]))
            if vendor_el:
                record["vendeur"] = vendor_el.get_text(strip=True)

            records.append(record)

    # --- Extraire depuis les tables standard ---
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
            rec = {
                "year": year,
                "sale_type": sale_type,
                "source": "tattersalls",
                "type": "table_row",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                rec[key] = cell

            # Extraire le prix
            for cell in cells:
                price_match = re.search(r'(\d{1,3}(?:,\d{3})*)', cell)
                if price_match:
                    val = int(price_match.group(1).replace(",", ""))
                    if val >= 500:  # Minimum raisonnable pour une vente
                        rec["prix_gns"] = str(val)
                        break

            records.append(rec)

    # --- Extraire le resume de la vente ---
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["summary", "statistics", "aggregate", "total"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 500:
                records.append({
                    "year": year,
                    "sale_type": sale_type,
                    "source": "tattersalls",
                    "type": "sale_summary",
                    "contenu": text,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_lot_detail(session, lot_url, year, sale_type):
    """Scraper le detail d'un lot (pedigree, conformation, etc.)."""
    if not lot_url.startswith("http"):
        lot_url = f"{BASE_URL}{lot_url}"

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', lot_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"lot_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, lot_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    detail = {
        "year": year,
        "sale_type": sale_type,
        "source": "tattersalls",
        "type": "lot_detail",
        "url_lot": lot_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Nom du cheval
    title = soup.find(["h1", "h2"])
    if title:
        detail["nom_cheval"] = title.get_text(strip=True)

    # Pedigree
    pedigree = {}
    for div in soup.find_all(["div", "table", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["pedigree", "breeding", "sire", "dam"]):
            cells = div.find_all(["td", "a", "span"])
            ancestors = [c.get_text(strip=True) for c in cells if c.get_text(strip=True)]
            positions = ["sire", "dam", "sire_sire", "sire_dam", "dam_sire", "dam_dam"]
            for i, anc in enumerate(ancestors):
                if i < len(positions):
                    pedigree[positions[i]] = anc
                else:
                    pedigree[f"ancestor_{i}"] = anc
    detail["pedigree"] = pedigree

    # Details physiques (couleur, sexe, annee)
    for p in soup.find_all(["p", "div", "span", "dd"]):
        text = p.get_text(strip=True)
        if not text or len(text) > 200:
            continue

        sex_match = re.search(r'\b(Colt|Filly|Gelding|Mare|Stallion|Horse|Rig)\b', text, re.IGNORECASE)
        if sex_match and "sexe" not in detail:
            detail["sexe"] = sex_match.group(1)

        color_match = re.search(r'\b(Bay|Chestnut|Grey|Gray|Black|Brown|Dark Bay|Roan)\b', text, re.IGNORECASE)
        if color_match and "couleur" not in detail:
            detail["couleur"] = color_match.group(1)

        year_match = re.search(r'\b(20[0-2]\d|19\d{2})\b', text)
        if year_match and "annee_naissance" not in detail:
            detail["annee_naissance"] = year_match.group(1)

    # Prix
    for el in soup.find_all(["span", "div", "td"], class_=lambda c: c and any(
            kw in c for kw in ["price", "hammer", "result", "amount"])):
        text = el.get_text(strip=True)
        price_match = re.search(r'(\d{1,3}(?:,\d{3})+)', text)
        if price_match:
            detail["prix_gns"] = price_match.group(1).replace(",", "")
            detail["prix_raw"] = text
            break

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(detail, f, ensure_ascii=False, indent=2)

    return detail


def main():
    parser = argparse.ArgumentParser(
        description="Script 72 — Tattersalls Scraper (ventes UK/IRE)")
    parser.add_argument("--year-start", type=int, default=2015,
                        help="Annee de debut")
    parser.add_argument("--year-end", type=int, default=None,
                        help="Annee de fin (defaut=annee courante)")
    parser.add_argument("--sale-type", type=str, default=None,
                        help="Type de vente specifique (ex: october-yearling-sale)")
    parser.add_argument("--detail", action="store_true", default=False,
                        help="Scraper aussi le detail de chaque lot")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    year_end = args.year_end if args.year_end else datetime.now().year
    sale_types = [args.sale_type] if args.sale_type else SALE_TYPES

    log.info("=" * 60)
    log.info("SCRIPT 72 — Tattersalls Scraper (UK/IRE horse sales)")
    log.info(f"  Annees : {args.year_start} -> {year_end}")
    log.info(f"  Types de ventes : {len(sale_types)}")
    log.info(f"  Detail lots : {args.detail}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint()
    last_key = checkpoint.get("last_key", "")
    if args.resume and last_key:
        log.info(f"  Reprise au checkpoint : {last_key}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "tattersalls_data.jsonl")

    total_records = checkpoint.get("total_records", 0)
    sale_count = 0
    skip_mode = bool(args.resume and last_key)

    for year in range(args.year_start, year_end + 1):
        for sale_type in sale_types:
            key = f"{year}_{sale_type}"

            if skip_mode:
                if key == last_key:
                    skip_mode = False
                continue

            log.info(f"  [{year}] {sale_type}")
            records = scrape_sales_catalogue(session, year, sale_type)

            if records:
                # Optionnel : scraper les details de chaque lot
                if args.detail:
                    lot_urls = [r.get("url_lot") for r in records if r.get("url_lot")]
                    for lurl in set(filter(None, lot_urls)):
                        detail = scrape_lot_detail(session, lurl, year, sale_type)
                        if detail:
                            records.append(detail)
                        smart_pause(2.0, 1.0)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            sale_count += 1

            if sale_count % 5 == 0:
                log.info(f"  Checkpoint : ventes={sale_count}, records={total_records}")
                save_checkpoint({"last_key": key, "total_records": total_records})

            if sale_count % 20 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

            smart_pause(2.0, 1.0)

    save_checkpoint({"last_key": f"{year_end}_{sale_types[-1]}",
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {sale_count} ventes, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
