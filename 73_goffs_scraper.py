#!/usr/bin/env python3
"""
Script 73 — Scraping Goffs.com
Source : goffs.com (ventes irlandaises)
Collecte : donnees de ventes (prix, acheteur, vendeur, pedigree, lot details)
CRITIQUE pour : Valuation Model, Irish Market, Bloodline Pricing (etape 7F)
"""

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "73_goffs"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, load_checkpoint, save_checkpoint, append_jsonl, create_session

log = setup_logging("73_goffs")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.goffs.com"

# Principaux types de ventes Goffs
SALE_TYPES = [
    "orby-sale",
    "november-breeding-stock-sale",
    "november-foal-sale",
    "sportsman-sale",
    "february-sale",
    "land-rover-sale",
    "goffs-uk-premier-yearling-sale",
    "goffs-uk-august-sale",
    "goffs-uk-spring-sale",
    "aintree-sale",
]



def scrape_sales_list(session, year):
    """Scraper la liste des ventes Goffs pour une annee donnee."""
    cache_file = os.path.join(CACHE_DIR, f"sales_list_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/sales-results/sales/{year}/"
    resp = fetch_with_retry(session, url)
    if not resp:
        # Essayer URL alternative
        url = f"{BASE_URL}/sales/{year}/"
        resp = fetch_with_retry(session, url)
        if not resp:
            return None

    soup = BeautifulSoup(resp.text, "html.parser")
    sales = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if text and any(kw in href.lower() or kw in text.lower() for kw in [
            "sale", "orby", "november", "sportsman", "february", "land-rover",
            "premier", "august", "spring", "aintree", "breeze", "yearling"
        ]):
            sales.append({
                "nom": text,
                "url": href if href.startswith("http") else f"{BASE_URL}{href}",
                "year": year,
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(sales, f, ensure_ascii=False, indent=2)

    return sales


def scrape_sale_results(session, sale_url, year, sale_name):
    """Scraper les resultats d'une vente Goffs."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', sale_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"results_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, sale_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Extraire les lots depuis les elements structurels ---
    for row in soup.find_all(["tr", "div", "article", "li"], class_=True):
        classes = " ".join(row.get("class", []))
        if any(kw in classes.lower() for kw in ["lot", "result", "entry", "catalogue", "horse"]):
            record = {
                "year": year,
                "sale_name": sale_name,
                "sale_url": sale_url,
                "source": "goffs",
                "type": "lot",
                "scraped_at": datetime.now().isoformat(),
            }

            # Numero de lot
            lot_el = row.find(["td", "span", "div"], class_=lambda c: c and any(
                kw in c.lower() for kw in ["lot", "number", "num"]))
            if lot_el:
                lot_match = re.search(r'(\d+)', lot_el.get_text(strip=True))
                if lot_match:
                    record["lot_number"] = int(lot_match.group(1))

            # Nom du cheval
            name_el = row.find(["a", "span", "h3", "h4", "strong"], class_=lambda c: c and any(
                kw in c.lower() for kw in ["name", "horse", "lot-name", "title"]))
            if not name_el:
                name_el = row.find("a", href=True)
            if name_el:
                record["nom_cheval"] = name_el.get_text(strip=True)
                if name_el.name == "a" and name_el.get("href"):
                    record["url_lot"] = name_el["href"]

            # Sire / Dam
            sire_el = row.find(["td", "span"], class_=lambda c: c and "sire" in c.lower()) if row else None
            if sire_el:
                record["sire"] = sire_el.get_text(strip=True)

            dam_el = row.find(["td", "span"], class_=lambda c: c and "dam" in c.lower()) if row else None
            if dam_el:
                record["dam"] = dam_el.get_text(strip=True)

            # Prix de vente (en EUR)
            price_el = row.find(["td", "span", "div"], class_=lambda c: c and any(
                kw in c.lower() for kw in ["price", "amount", "result", "hammer", "sold"]))
            if price_el:
                price_text = price_el.get_text(strip=True)
                # Goffs affiche en EUR
                price_match = re.search(r'[\d,]+', price_text.replace(" ", "").replace("\u20ac", ""))
                if price_match:
                    record["prix_eur"] = price_match.group(0).replace(",", "")
                    record["prix_raw"] = price_text

            # Acheteur
            buyer_el = row.find(["td", "span"], class_=lambda c: c and any(
                kw in c.lower() for kw in ["buyer", "purchaser", "agent"]))
            if buyer_el:
                record["acheteur"] = buyer_el.get_text(strip=True)

            # Vendeur / consignor
            vendor_el = row.find(["td", "span"], class_=lambda c: c and any(
                kw in c.lower() for kw in ["vendor", "consignor", "seller"]))
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
                "sale_name": sale_name,
                "source": "goffs",
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
                    if val >= 500:
                        rec["prix_eur"] = str(val)
                        break

            # Lien vers le lot
            link = tr.find("a", href=True)
            if link:
                rec["url_lot"] = link["href"]

            records.append(rec)

    # --- Resume de la vente ---
    for div in soup.find_all(["div", "section", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["summary", "statistics", "aggregate", "total", "median"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 500:
                records.append({
                    "year": year,
                    "sale_name": sale_name,
                    "source": "goffs",
                    "type": "sale_summary",
                    "contenu": text,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_lot_detail(session, lot_url, year, sale_name):
    """Scraper le detail d'un lot Goffs (pedigree, infos physiques)."""
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
        "sale_name": sale_name,
        "source": "goffs",
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
        if any(kw in classes.lower() for kw in ["pedigree", "breeding", "sire", "dam", "family"]):
            cells = div.find_all(["td", "a", "span"])
            ancestors = [c.get_text(strip=True) for c in cells if c.get_text(strip=True)]
            positions = ["sire", "dam", "sire_sire", "sire_dam", "dam_sire", "dam_dam"]
            for i, anc in enumerate(ancestors):
                if i < len(positions):
                    pedigree[positions[i]] = anc
                else:
                    pedigree[f"ancestor_{i}"] = anc
    detail["pedigree"] = pedigree

    # Details physiques
    for p in soup.find_all(["p", "div", "span", "dd", "li"]):
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
            kw in c.lower() for kw in ["price", "hammer", "result", "amount", "sold"])):
        text = el.get_text(strip=True)
        price_match = re.search(r'(\d{1,3}(?:,\d{3})+)', text)
        if price_match:
            detail["prix_eur"] = price_match.group(1).replace(",", "")
            detail["prix_raw"] = text
            break

    # Famille / updates
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["update", "race-record", "performance", "family"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 1000:
                detail["performance_notes"] = text[:500]

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(detail, f, ensure_ascii=False, indent=2)

    return detail


def main():
    parser = argparse.ArgumentParser(
        description="Script 73 — Goffs Scraper (ventes irlandaises)")
    parser.add_argument("--year-start", type=int, default=2015,
                        help="Annee de debut")
    parser.add_argument("--year-end", type=int, default=None,
                        help="Annee de fin (defaut=annee courante)")
    parser.add_argument("--sale-type", type=str, default=None,
                        help="Type de vente specifique (ex: orby-sale)")
    parser.add_argument("--detail", action="store_true", default=False,
                        help="Scraper aussi le detail de chaque lot")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    year_end = args.year_end if args.year_end else datetime.now().year

    log.info("=" * 60)
    log.info("SCRIPT 73 — Goffs Scraper (Irish horse sales)")
    log.info(f"  Annees : {args.year_start} -> {year_end}")
    log.info(f"  Detail lots : {args.detail}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_key = checkpoint.get("last_key", "")
    if args.resume and last_key:
        log.info(f"  Reprise au checkpoint : {last_key}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "goffs_data.jsonl")

    total_records = checkpoint.get("total_records", 0)
    sale_count = 0
    skip_mode = bool(args.resume and last_key)

    for year in range(args.year_start, year_end + 1):
        log.info(f"  --- Annee {year} ---")

        # Recuperer la liste des ventes pour cette annee
        sales = scrape_sales_list(session, year)
        smart_pause(2.0, 1.0)

        if not sales:
            # Fallback : utiliser les types de ventes connus
            sale_types = [args.sale_type] if args.sale_type else SALE_TYPES
            sales = [{"nom": st, "url": f"{BASE_URL}/sales-results/{year}/{st}/", "year": year}
                     for st in sale_types]

        for sale in sales:
            sale_name = sale.get("nom", "unknown")
            sale_url = sale.get("url", "")
            key = f"{year}_{sale_name}"

            if skip_mode:
                if key == last_key:
                    skip_mode = False
                continue

            if not sale_url:
                continue

            log.info(f"  [{year}] {sale_name}")
            records = scrape_sale_results(session, sale_url, year, sale_name)

            if records:
                # Optionnel : scraper les details de chaque lot
                if args.detail:
                    lot_urls = [r.get("url_lot") for r in records if r.get("url_lot")]
                    for lurl in set(filter(None, lot_urls)):
                        detail = scrape_lot_detail(session, lurl, year, sale_name)
                        if detail:
                            records.append(detail)
                        smart_pause(2.0, 1.0)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            sale_count += 1

            if sale_count % 5 == 0:
                log.info(f"  Checkpoint : ventes={sale_count}, records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"last_key": key, "total_records": total_records})

            if sale_count % 20 == 0:
                session.close()
                session = create_session(USER_AGENTS)
                time.sleep(random.uniform(5, 15))

            smart_pause(2.0, 1.0)

    save_checkpoint(CHECKPOINT_FILE, {"last_key": f"{year_end}_final",
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {sale_count} ventes, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
