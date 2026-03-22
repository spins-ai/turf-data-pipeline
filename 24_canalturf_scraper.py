#!/usr/bin/env python3
"""
Script 24 — Scraping Canalturf fiches chevaux
Source : canalturf.com/courses_fiche_cheval.php?idcheval={id}
Collecte les fiches détaillées : pedigree, stats PMU, historique performances, rapports
CRITIQUE pour : Pedigree Features, Survival Model, Field Strength
"""

import json
import time
import random
import os
import re
import logging
import sys
from datetime import datetime
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, create_session

SCRIPT_NAME = "24_canalturf"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

log = setup_logging("24_canalturf_scraper")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

def new_session():
    s = create_session(user_agents=USER_AGENTS)
    s.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s

def load_horse_ids():
    """Extraire les IDs chevaux depuis les partants PMU (id_nav_partant)"""
    ids = set()
    for path in [
        os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.json"),
        os.path.join(BASE_DIR, "output", "02b_liste_courses_2013", "partants_normalises.json"),
    ]:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for p in data:
                # Essayer différents champs d'ID
                for field in ["id_cheval", "id_nav_cheval", "idCheval"]:
                    val = p.get(field)
                    if val and str(val).isdigit():
                        ids.add(int(val))
    log.info(f"IDs chevaux trouvés: {len(ids)}")
    return sorted(ids)

def scrape_horse(session, horse_id):
    """Scraper la fiche d'un cheval sur Canalturf"""
    cache_file = os.path.join(CACHE_DIR, f"{horse_id}.json")
    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    url = f"https://www.canalturf.com/courses_fiche_cheval.php?idcheval={horse_id}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 403:
            time.sleep(60)
            return None
        if resp.status_code == 429:
            time.sleep(120)
            return None
        if resp.status_code != 200:
            return None
    except Exception as e:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    horse = {"id_canalturf": horse_id}

    # Nom du cheval depuis le titre
    title = soup.find("title")
    if title:
        name_match = re.match(r"^([A-Z' ]+)", title.get_text(strip=True))
        if name_match:
            horse["nom_cheval"] = name_match.group(1).strip()

    # Extraire toutes les tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower().replace(" ", "_").replace(":", "")
                val = cells[1].get_text(strip=True)
                if key and val and len(key) < 50:
                    horse[key] = val

    # Chercher les performances dans les divs
    perfs = []
    for div in soup.find_all(["div", "tr"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if "perf" in classes.lower() or "result" in classes.lower():
            if text and len(text) < 500:
                perfs.append(text)

    if perfs:
        horse["performances_raw"] = perfs[:20]

    # Stats globales
    stats_text = soup.get_text()
    gains_match = re.search(r'(\d[\d\s,.]+)\s*€', stats_text)
    if gains_match:
        horse["gains_text"] = gains_match.group(0)

    victoires_match = re.search(r'(\d+)\s*victoire', stats_text, re.I)
    if victoires_match:
        horse["victoires"] = int(victoires_match.group(1))

    courses_match = re.search(r'(\d+)\s*course', stats_text, re.I)
    if courses_match:
        horse["nb_courses"] = int(courses_match.group(1))

    # Pedigree
    for text_block in [soup.get_text()]:
        pere_match = re.search(r'[Pp]ère\s*:?\s*([A-Z\' ]{3,})', text_block)
        mere_match = re.search(r'[Mm]ère\s*:?\s*([A-Z\' ]{3,})', text_block)
        if pere_match:
            horse["pere"] = pere_match.group(1).strip()
        if mere_match:
            horse["mere"] = mere_match.group(1).strip()

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(horse, f, ensure_ascii=False, indent=2)

    return horse

def export_cache_to_jsonl():
    """Export cache to JSONL without scraping."""
    jsonl_file = os.path.join(OUTPUT_DIR, "canalturf_chevaux.jsonl")
    log.info(f"Export cache → {jsonl_file}")
    jsonl_count = 0
    with open(jsonl_file, "w", encoding="utf-8") as fout:
        for fname in sorted(os.listdir(CACHE_DIR)):
            if not fname.endswith(".json"):
                continue
            cache_path = os.path.join(CACHE_DIR, fname)
            try:
                with open(cache_path, encoding="utf-8") as fin:
                    record = json.load(fin)
                if record and record.get("nom_cheval"):
                    fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                    jsonl_count += 1
            except Exception as e:
                log.debug(f"  Erreur lecture cache {fname}: {e}")
    log.info(f"  JSONL: {jsonl_count} fiches écrites → {jsonl_file}")
    return jsonl_count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Script 24 — Canalturf Fiches Chevaux")
    parser.add_argument("--export", action="store_true",
                        help="Export cache to JSONL without scraping")
    args = parser.parse_args()

    if args.export:
        log.info("=" * 60)
        log.info("SCRIPT 24 — Export cache → JSONL (--export)")
        log.info("=" * 60)
        export_cache_to_jsonl()
        return

    log.info("=" * 60)
    log.info("SCRIPT 24 — Fiches chevaux Canalturf")
    log.info("=" * 60)

    # Charger les IDs
    horse_ids = load_horse_ids()

    if not horse_ids:
        # Fallback: générer une plage d'IDs basée sur les fiches connues
        log.info("Pas d'IDs trouvés dans les partants, utilisation d'une plage par défaut")
        horse_ids = list(range(100000, 160000))

    log.info(f"Total chevaux à scraper: {len(horse_ids)}")

    # Checkpoint
    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_24.json")
    start_idx = 0
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, encoding="utf-8") as f:
            cp = json.load(f)
        start_idx = cp.get("last_index", 0)
        log.info(f"Reprise au checkpoint: index {start_idx}")

    session = new_session()
    all_horses = []
    output_file = os.path.join(OUTPUT_DIR, "canalturf_chevaux.json")
    if os.path.exists(output_file) and start_idx > 0:
        with open(output_file, encoding="utf-8") as f:
            all_horses = json.load(f)

    collected = 0
    errors = 0

    for i in range(start_idx, len(horse_ids)):
        horse_id = horse_ids[i]
        horse = scrape_horse(session, horse_id)

        if horse and horse.get("nom_cheval"):
            all_horses.append(horse)
            collected += 1
        else:
            errors += 1

        if (i + 1 - start_idx) % 50 == 0:
            log.info(f"  [{i+1}/{len(horse_ids)}] chevaux={collected} erreurs={errors}")

        if (i + 1 - start_idx) % 200 == 0:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_horses, f, ensure_ascii=False)
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump({"last_index": i + 1}, f)
            log.info(f">>> Sauvegarde: {len(all_horses)} chevaux <<<")

            # Rotation session
            session.close()
            session = new_session()
            time.sleep(random.uniform(3, 8))

        smart_pause(1.5, 0.8)

    log.info("Sauvegarde finale...")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_horses, f, ensure_ascii=False)

    # Agrégation cache → JSONL
    jsonl_file = os.path.join(OUTPUT_DIR, "canalturf_chevaux.jsonl")
    log.info(f"Agrégation cache → {jsonl_file}")
    jsonl_count = 0
    # Overwrite: write fresh JSONL from all cache files
    with open(jsonl_file, "w", encoding="utf-8") as fout:
        for fname in sorted(os.listdir(CACHE_DIR)):
            if not fname.endswith(".json"):
                continue
            cache_path = os.path.join(CACHE_DIR, fname)
            try:
                with open(cache_path, encoding="utf-8") as fin:
                    record = json.load(fin)
                if record and record.get("nom_cheval"):
                    fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                    jsonl_count += 1
            except Exception as e:
                log.debug(f"  Erreur lecture cache {fname}: {e}")
    log.info(f"  JSONL: {jsonl_count} fiches écrites → {jsonl_file}")

    log.info("=" * 60)
    log.info(f"TERMINÉ: {len(all_horses)} fiches chevaux collectées")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
