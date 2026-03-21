#!/usr/bin/env python3
"""
Script 71 — Scraping AllBreedPedigree.com
Source : allbreedpedigree.com/
Collecte : arbres genealogiques complets (pedigree 4-5 generations), lignees, bloodlines
CRITIQUE pour : Pedigree Features, Bloodline Analysis, Breeding Value (etape 7F)
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

SCRIPT_NAME = "71_allbreedpedigree"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry

log = setup_logging("71_allbreedpedigree")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.allbreedpedigree.com"


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
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
    """Charger le checkpoint de reprise."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    """Sauvegarder le checkpoint."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_horse_list(filepath):
    """Charger la liste de chevaux depuis un fichier texte (un nom par ligne)."""
    if not os.path.exists(filepath):
        log.error(f"Fichier de liste introuvable : {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        horses = [line.strip() for line in f if line.strip()]
    return horses


def search_horse(session, horse_name):
    """Rechercher un cheval par nom sur AllBreedPedigree."""
    cache_file = os.path.join(CACHE_DIR, f"search_{re.sub(r'[^a-zA-Z0-9]', '_', horse_name)}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/search?query_type=check&search_bar=horse&g=5&query={requests.utils.quote(horse_name)}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    # Chercher les liens vers les fiches pedigree
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if text and len(text) > 2 and horse_name.lower()[:5] in text.lower():
            results.append({
                "nom": text,
                "url": href if href.startswith("http") else f"{BASE_URL}{href}",
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results


def scrape_pedigree(session, horse_name, horse_url=None):
    """Scraper l'arbre genealogique complet d'un cheval."""
    safe_name = re.sub(r'[^a-zA-Z0-9]', '_', horse_name)
    cache_file = os.path.join(CACHE_DIR, f"pedigree_{safe_name}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # Si pas d'URL fourni, construire a partir du nom
    if not horse_url:
        encoded_name = horse_name.replace(" ", "+")
        horse_url = f"{BASE_URL}/{encoded_name}"

    resp = fetch_with_retry(session, horse_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    record = {
        "nom_cheval": horse_name,
        "url": horse_url,
        "source": "allbreedpedigree",
        "scraped_at": datetime.now().isoformat(),
    }

    # --- Extraire le nom complet ---
    title = soup.find(["h1", "h2"])
    if title:
        record["nom_complet"] = title.get_text(strip=True)

    # --- Extraire les infos du cheval (sexe, couleur, annee, etc.) ---
    for div in soup.find_all(["div", "p", "span", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["info", "detail", "profile", "horse-info", "bio"]):
            if text and len(text) < 500:
                # Sexe
                sex_match = re.search(r'\b(Stallion|Mare|Gelding|Colt|Filly)\b', text, re.IGNORECASE)
                if sex_match:
                    record["sexe"] = sex_match.group(1)
                # Couleur
                color_match = re.search(
                    r'\b(Bay|Chestnut|Grey|Gray|Black|Brown|Dark Bay|Roan|Palomino)\b',
                    text, re.IGNORECASE)
                if color_match:
                    record["couleur"] = color_match.group(1)
                # Annee de naissance
                year_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text)
                if year_match:
                    record["annee_naissance"] = year_match.group(1)

    # --- Extraire l'arbre pedigree (table genealogique) ---
    pedigree = {}
    pedigree_table = soup.find("table", class_=lambda c: c and any(
        kw in c for kw in ["pedigree", "ped", "tree", "genealogy"]))
    if not pedigree_table:
        # Fallback : chercher toute table avec des liens internes
        for t in soup.find_all("table"):
            links = t.find_all("a")
            if len(links) >= 6:
                pedigree_table = t
                break

    if pedigree_table:
        cells = pedigree_table.find_all(["td", "th"])
        ancestors = []
        for cell in cells:
            link = cell.find("a")
            text = cell.get_text(strip=True)
            if text and len(text) > 1:
                ancestor = {"nom": text}
                if link and link.get("href"):
                    ancestor["url"] = link["href"]
                ancestors.append(ancestor)

        # Mapper les positions standard d'un pedigree 4-gen
        # Position 0 = cheval, 1 = pere, 2 = mere, 3 = grand-pere paternel, etc.
        positions = [
            "sire", "dam",
            "sire_sire", "sire_dam", "dam_sire", "dam_dam",
            "sire_sire_sire", "sire_sire_dam", "sire_dam_sire", "sire_dam_dam",
            "dam_sire_sire", "dam_sire_dam", "dam_dam_sire", "dam_dam_dam",
        ]
        for i, anc in enumerate(ancestors):
            if i < len(positions):
                pedigree[positions[i]] = anc["nom"]
            else:
                pedigree[f"gen4_pos{i}"] = anc["nom"]

    record["pedigree"] = pedigree

    # --- Extraire les performances / progeny si disponibles ---
    for section in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["progeny", "offspring", "produce", "race-record"]):
            items = []
            for li in section.find_all(["li", "tr", "a"]):
                text = li.get_text(strip=True)
                if text and len(text) > 2:
                    items.append(text)
            if items:
                record["progeny"] = items[:50]  # Limiter

    # Sauvegarder cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    return record


def main():
    parser = argparse.ArgumentParser(
        description="Script 71 — AllBreedPedigree Scraper (arbres genealogiques complets)")
    parser.add_argument("--horses-file", type=str, default=None,
                        help="Fichier texte avec un nom de cheval par ligne")
    parser.add_argument("--horse", type=str, default=None,
                        help="Nom d'un cheval unique a scraper")
    parser.add_argument("--search", type=str, default=None,
                        help="Rechercher un cheval par nom")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--generations", type=int, default=5,
                        help="Nombre de generations pedigree (defaut=5)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 71 — AllBreedPedigree Scraper (pedigree mondial)")
    log.info("=" * 60)

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "allbreedpedigree_data.jsonl")

    # Mode recherche
    if args.search:
        log.info(f"  Recherche : {args.search}")
        results = search_horse(session, args.search)
        if results:
            for r in results:
                log.info(f"    Trouve : {r['nom']} -> {r['url']}")
                append_jsonl(output_file, {
                    "type": "search_result",
                    "query": args.search,
                    "source": "allbreedpedigree",
                    **r,
                    "scraped_at": datetime.now().isoformat(),
                })
        else:
            log.info("    Aucun resultat.")
        return

    # Construire la liste de chevaux
    horses = []
    if args.horse:
        horses = [args.horse]
    elif args.horses_file:
        horses = load_horse_list(args.horses_file)
    else:
        log.error("Specifier --horse, --horses-file ou --search")
        return

    log.info(f"  Chevaux a scraper : {len(horses)}")

    # Checkpoint
    checkpoint = load_checkpoint()
    last_index = checkpoint.get("last_index", -1)
    if args.resume and last_index >= 0:
        log.info(f"  Reprise au checkpoint : index {last_index + 1}")

    total_records = checkpoint.get("total_records", 0)

    for i, horse_name in enumerate(horses):
        if args.resume and i <= last_index:
            continue

        log.info(f"  [{i + 1}/{len(horses)}] {horse_name}")

        # Rechercher d'abord
        search_results = search_horse(session, horse_name)
        smart_pause(2.0, 1.0)

        horse_url = None
        if search_results:
            horse_url = search_results[0].get("url")

        # Scraper le pedigree
        record = scrape_pedigree(session, horse_name, horse_url)
        if record:
            append_jsonl(output_file, record)
            total_records += 1

        # Checkpoint periodique
        if (i + 1) % 20 == 0:
            log.info(f"  Checkpoint : {i + 1}/{len(horses)}, records={total_records}")
            save_checkpoint({"last_index": i, "total_records": total_records})

        if (i + 1) % 60 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        smart_pause(2.5, 1.5)

    save_checkpoint({"last_index": len(horses) - 1,
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {len(horses)} chevaux, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
