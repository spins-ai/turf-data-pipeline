#!/usr/bin/env python3
"""
Script 74 — Scraping Arqana.com (ventes de chevaux France)
Source : arqana.com/lots/, arqana.com/catalogue/
Collecte : historique ventes, prix, pedigree acheteur/vendeur, lots
CRITIQUE pour : Valuation Model, Pedigree Analysis, Market Intelligence
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "74_arqana"
OUTPUT_DIR = os.path.join("output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

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

# Catalogues de ventes Arqana connus
SALE_TYPES = [
    "vente-de-yearlings-deauville",
    "vente-de-yearlings-octobre",
    "vente-darc",
    "vente-de-breeding-stock",
    "vente-de-pur-sang-en-entrainement",
    "vente-de-trotteurs",
    "breeze-up",
]

BASE_URL = "https://www.arqana.com"


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


def scrape_sales_index(session):
    """Recuperer la liste des ventes disponibles sur Arqana."""
    url = f"{BASE_URL}/ventes/"
    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    sales = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if any(kw in href.lower() for kw in ["catalogue", "vente", "sale", "lot"]):
            if text and len(text) > 3:
                full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                sales.append({"url": full_url, "titre": text})

    log.info(f"  Index ventes: {len(sales)} liens trouves")
    return sales


def scrape_catalogue(session, sale_url, sale_name):
    """Scraper un catalogue de vente Arqana (liste des lots)."""
    cache_key = re.sub(r'[^a-zA-Z0-9]', '_', sale_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"catalogue_{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, sale_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    lots = []

    # Extraire les lots depuis les tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            lot = {
                "source": "arqana",
                "vente": sale_name,
                "url_vente": sale_url,
                "type": "lot_vente",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                lot[key] = cell
            lots.append(lot)

    # Extraire les lots depuis les cartes/divs
    for card in soup.find_all(["div", "article", "li"], class_=True):
        classes = " ".join(card.get("class", []))
        if any(kw in classes.lower() for kw in ["lot", "horse", "item", "catalogue"]):
            lot = {
                "source": "arqana",
                "vente": sale_name,
                "url_vente": sale_url,
                "type": "lot_card",
                "scraped_at": datetime.utcnow().isoformat(),
            }

            # Numero de lot
            num_el = card.find(["span", "div"], class_=lambda c: c and "num" in c.lower()) if card else None
            if num_el:
                lot["numero_lot"] = num_el.get_text(strip=True)

            # Nom du cheval
            name_el = card.find(["h3", "h4", "a", "strong"])
            if name_el:
                lot["nom_cheval"] = name_el.get_text(strip=True)

            # Prix
            price_el = card.find(string=re.compile(r'[\d.,]+\s*(EUR|€|guineas)', re.I))
            if price_el:
                lot["prix_brut"] = price_el.strip()
                price_match = re.search(r'([\d.,]+)', price_el)
                if price_match:
                    lot["prix_num"] = price_match.group(1).replace(".", "").replace(",", ".")

            # Pedigree (pere x mere)
            pedigree_el = card.find(string=re.compile(r'\bx\b|\bex\b|\bout of\b', re.I))
            if pedigree_el:
                lot["pedigree_brut"] = pedigree_el.strip()

            # Acheteur / Vendeur
            for label_kw, field in [("acheteur", "acheteur"), ("buyer", "acheteur"),
                                     ("vendeur", "vendeur"), ("consignor", "vendeur"),
                                     ("vendor", "vendeur")]:
                el = card.find(string=re.compile(label_kw, re.I))
                if el:
                    parent = el.find_parent()
                    if parent:
                        lot[field] = parent.get_text(strip=True)

            # Lien detail
            detail_link = card.find("a", href=True)
            if detail_link:
                lot["url_detail"] = detail_link["href"] if detail_link["href"].startswith("http") else f"{BASE_URL}{detail_link['href']}"

            if lot.get("nom_cheval") or lot.get("numero_lot"):
                lots.append(lot)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(lots, f, ensure_ascii=False, indent=2)

    return lots


def scrape_lot_detail(session, lot_url):
    """Scraper le detail d'un lot individuel."""
    if not lot_url:
        return None

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', lot_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"lot_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    if not lot_url.startswith("http"):
        lot_url = f"{BASE_URL}{lot_url}"

    resp = fetch_with_retry(session, lot_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    detail = {
        "source": "arqana",
        "type": "lot_detail",
        "url": lot_url,
        "scraped_at": datetime.utcnow().isoformat(),
    }

    # Nom
    h1 = soup.find("h1")
    if h1:
        detail["nom_cheval"] = h1.get_text(strip=True)

    # Extraire toutes les paires label/valeur
    for dt in soup.find_all(["dt", "th", "label"]):
        dd = dt.find_next_sibling(["dd", "td", "span"])
        if dd:
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val:
                detail[key] = val

    # Prix de vente
    for el in soup.find_all(string=re.compile(r'(prix|price|sold|vendu)', re.I)):
        parent = el.find_parent()
        if parent:
            price_match = re.search(r'([\d.,]+)\s*(EUR|€|guineas)?', parent.get_text())
            if price_match:
                detail["prix_vente"] = parent.get_text(strip=True)
                break

    # Pedigree
    pedigree_section = soup.find(["div", "section"], class_=lambda c: c and "pedigree" in c.lower()) if soup else None
    if pedigree_section:
        detail["pedigree_html"] = pedigree_section.get_text(separator=" | ", strip=True)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(detail, f, ensure_ascii=False, indent=2)

    return detail


def main():
    parser = argparse.ArgumentParser(description="Script 74 — Arqana Scraper (ventes chevaux France)")
    parser.add_argument("--year-start", type=int, default=2015,
                        help="Annee de debut")
    parser.add_argument("--year-end", type=int, default=None,
                        help="Annee de fin (defaut=annee courante)")
    parser.add_argument("--detail", action="store_true", default=False,
                        help="Scraper aussi le detail de chaque lot")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    year_end = args.year_end or datetime.now().year
    log.info("=" * 60)
    log.info("SCRIPT 74 — Arqana Scraper (ventes chevaux France)")
    log.info(f"  Periode : {args.year_start} -> {year_end}")
    log.info(f"  Detail lots : {args.detail}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "arqana_ventes.jsonl")

    total_records = checkpoint.get("total_records", 0)
    last_sale = checkpoint.get("last_sale", "")

    # 1. Scraper l'index des ventes
    sales_index = scrape_sales_index(session)
    smart_pause(2.0, 1.0)

    # 2. Construire les URLs par annee et type de vente
    sale_urls = []
    for year in range(args.year_start, year_end + 1):
        for stype in SALE_TYPES:
            sale_urls.append({
                "url": f"{BASE_URL}/catalogue/{stype}-{year}/",
                "name": f"{stype}-{year}",
            })
        # Aussi essayer les URLs numeriques
        for sid in range(1, 20):
            sale_urls.append({
                "url": f"{BASE_URL}/ventes/resultats/{year}/{sid}/",
                "name": f"vente-{year}-{sid}",
            })

    # Ajouter les URLs trouvees dans l'index
    for s in sales_index:
        sale_urls.append({"url": s["url"], "name": s["titre"]})

    # Resume
    skip = bool(last_sale and args.resume)
    sale_count = 0
    lot_count = 0

    for sale in sale_urls:
        if skip:
            if sale["name"] == last_sale:
                skip = False
            continue

        log.info(f"  Vente: {sale['name']}")
        lots = scrape_catalogue(session, sale["url"], sale["name"])
        smart_pause(2.0, 1.0)

        if not lots:
            continue

        for lot in lots:
            append_jsonl(output_file, lot)
            total_records += 1
            lot_count += 1

            # Detail optionnel
            if args.detail and lot.get("url_detail"):
                detail = scrape_lot_detail(session, lot["url_detail"])
                if detail:
                    append_jsonl(output_file, detail)
                    total_records += 1
                smart_pause(1.5, 0.8)

        sale_count += 1

        if sale_count % 10 == 0:
            log.info(f"  Progression: {sale_count} ventes, {lot_count} lots, {total_records} records")
            save_checkpoint({"last_sale": sale["name"], "total_records": total_records})

        if sale_count % 30 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

    save_checkpoint({
        "last_sale": sale_urls[-1]["name"] if sale_urls else "",
        "total_records": total_records,
        "status": "done",
    })

    log.info("=" * 60)
    log.info(f"TERMINE: {sale_count} ventes, {lot_count} lots, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
