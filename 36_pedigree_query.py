#!/usr/bin/env python3
"""
Script 36 — Pedigree Query : Pedigrees 5 générations internationaux
Source : pedigreequery.com
CRITIQUE pour : Enrichir pedigree au-delà de France Galop, inbreeding analysis

v2 : HTML brut sauvegardé en cache + retry robuste + anti-ban + checkpoint fréquent
"""

import requests
import json
import time
import random
import os
import re
import signal
import sys
from bs4 import BeautifulSoup
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "36_pedigree_query")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_DIR = os.path.join(OUTPUT_DIR, "html_raw")
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_DIR, exist_ok=True)

log = setup_logging("36_pedigree_query")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

session = requests.Session()
req_count = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10  # Pause longue après 10 erreurs d'affilée

# Sauvegarde propre sur Ctrl+C / kill
all_records = []
checkpoint_data = {}
output_file = os.path.join(OUTPUT_DIR, "pedigree_query_data.json")
checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_36.json")

def save_and_exit(signum=None, frame=None):
    """Sauvegarde proprement avant de quitter"""
    log.info(f"⚡ Signal reçu — sauvegarde {len(all_records)} records...")
    try:
        tmp = output_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False)
        os.replace(tmp, output_file)
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(checkpoint_data, f)
        log.info("✅ Sauvegarde OK avant arrêt")
    except Exception as e:
        log.error(f"❌ Erreur sauvegarde: {e}")
    if signum:
        sys.exit(0)

signal.signal(signal.SIGTERM, save_and_exit)
signal.signal(signal.SIGINT, save_and_exit)


def rotate_session():
    global session, req_count
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    })
    req_count = 0

def get_html_cached(name, clean_name):
    """Récupère le HTML — depuis cache disque si dispo, sinon fetch"""
    safe_name = clean_name.replace(' ', '_')[:50]
    html_file = os.path.join(HTML_DIR, f"{safe_name}.html")

    # HTML déjà sur disque ?
    if os.path.exists(html_file):
        with open(html_file, "r", encoding="utf-8") as f:
            return f.read()

    # Fetch
    global req_count, consecutive_errors
    url = f"https://www.pedigreequery.com/{clean_name.replace(' ', '+')}"

    try:
        resp = session.get(url, timeout=25)
        req_count += 1

        if req_count >= random.randint(15, 25):
            rotate_session()

        if resp.status_code == 429 or resp.status_code == 403:
            log.warning(f"  🚫 {resp.status_code} rate limited — pause 60-120s")
            time.sleep(random.uniform(60, 120))
            rotate_session()
            return None

        if resp.status_code == 503:
            log.warning(f"  🔧 503 service indisponible — pause 30s")
            time.sleep(30)
            return None

        if resp.status_code != 200:
            log.debug(f"  HTTP {resp.status_code} pour {clean_name}")
            return None

        html = resp.text

        # Sauvegarder HTML brut sur disque (on ne le perd JAMAIS)
        if len(html) > 500:
            with open(html_file, "w", encoding="utf-8") as f:
                f.write(html)
            consecutive_errors = 0
            return html
        else:
            return None

    except requests.exceptions.Timeout:
        log.debug(f"  ⏱ Timeout {clean_name}")
        consecutive_errors += 1
        return None
    except requests.exceptions.ConnectionError:
        log.warning(f"  🔌 Connection error — pause 30s")
        consecutive_errors += 1
        time.sleep(30)
        return None
    except Exception as e:
        log.debug(f"  Erreur {clean_name}: {e}")
        consecutive_errors += 1
        return None


def parse_pedigree(html, clean_name):
    """Parse le HTML pour extraire le pedigree"""
    soup = BeautifulSoup(html, "html.parser")

    record = {
        "name": clean_name,
        "source": "pedigree_query",
    }

    # Extraire le pedigree (tableau)
    tables = soup.find_all("table")
    for table in tables:
        cells = table.find_all("td")
        ancestors = []
        for cell in cells:
            text = cell.get_text(strip=True)
            if text and len(text) > 1 and not text.startswith("("):
                link = cell.find("a")
                if link:
                    ancestors.append(text)

        if len(ancestors) >= 6:
            record["sire"] = ancestors[0] if len(ancestors) > 0 else None
            record["dam"] = ancestors[1] if len(ancestors) > 1 else None
            record["sire_sire"] = ancestors[2] if len(ancestors) > 2 else None
            record["sire_dam"] = ancestors[3] if len(ancestors) > 3 else None
            record["dam_sire"] = ancestors[4] if len(ancestors) > 4 else None
            record["dam_dam"] = ancestors[5] if len(ancestors) > 5 else None
            # 5 générations si dispo
            if len(ancestors) >= 14:
                record["sire_sire_sire"] = ancestors[6] if len(ancestors) > 6 else None
                record["sire_sire_dam"] = ancestors[7] if len(ancestors) > 7 else None
                record["sire_dam_sire"] = ancestors[8] if len(ancestors) > 8 else None
                record["sire_dam_dam"] = ancestors[9] if len(ancestors) > 9 else None
                record["dam_sire_sire"] = ancestors[10] if len(ancestors) > 10 else None
                record["dam_sire_dam"] = ancestors[11] if len(ancestors) > 11 else None
                record["dam_dam_sire"] = ancestors[12] if len(ancestors) > 12 else None
                record["dam_dam_dam"] = ancestors[13] if len(ancestors) > 13 else None
            record["ancestors_count"] = len(ancestors)
            break

    # Extraire infos additionnelles
    text = soup.get_text()
    country_match = re.search(r'\b(FR|GB|IRE|USA|AUS|GER|JPN|HK|SAF|UAE|NZ|CAN|BRZ|CHI|ARG|ITY|SPA|SWE|DEN|NOR)\b', text)
    if country_match:
        record["country"] = country_match.group(1)

    year_match = re.search(r'\b(19[5-9]\d|20[0-2]\d)\b', text[:500])
    if year_match:
        record["birth_year"] = int(year_match.group(1))

    # Couleur / sexe si dispo
    color_match = re.search(r'\b(bay|brown|chestnut|grey|black|roan|dark bay|b\.|ch\.|gr\.|bl\.)\b', text[:500], re.IGNORECASE)
    if color_match:
        record["color"] = color_match.group(1).lower()

    sex_match = re.search(r'\b(colt|filly|mare|stallion|gelding|horse|ridgling)\b', text[:500], re.IGNORECASE)
    if sex_match:
        record["sex"] = sex_match.group(1).lower()

    return record if record.get("sire") else None


def search_horse(name):
    """Chercher un cheval : cache JSON > cache HTML > fetch"""
    clean_name = re.sub(r'[^a-zA-Z\s]', '', name).strip()
    if not clean_name:
        return None

    # Cache JSON déjà parsé ?
    safe_name = clean_name.replace(' ', '_')[:50]
    cache_file = os.path.join(CACHE_DIR, f"{safe_name}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            os.remove(cache_file)

    # Récupérer HTML (cache disque ou fetch)
    html = get_html_cached(name, clean_name)
    if not html:
        return None

    # Parser
    record = parse_pedigree(html, clean_name)

    if record and record.get("sire"):
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False)
        return record

    return None


def main():
    global all_records, checkpoint_data

    log.info("=" * 60)
    log.info("SCRIPT 36 — Pedigree Query International (v2 HTML cache)")
    log.info("=" * 60)

    rotate_session()

    # Charger les noms de chevaux depuis nos données PMU
    horse_names = set()

    for path in [
        os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_normalisees.json"),
    ]:
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    courses = json.load(f)
                for c in courses:
                    for p in c.get("partants", []):
                        name = p.get("nom_cheval", "")
                        if name and len(name) > 2:
                            horse_names.add(name.upper())
            except Exception as e:
                log.warning(f"  Erreur chargement {path}: {e}")

    # Aussi depuis le SIRE
    sire_index = os.path.join(BASE_DIR, "output", "17_sire_ifce", "index_par_nom.json")
    if os.path.exists(sire_index):
        try:
            with open(sire_index, encoding="utf-8") as f:
                sire_data = json.load(f)
            for name in list(sire_data.keys())[:10000]:
                horse_names.add(name.upper())
        except Exception as e:
            log.warning(f"  Erreur chargement SIRE: {e}")

    log.info(f"  {len(horse_names)} noms de chevaux uniques")

    names_list = sorted(horse_names)

    # Checkpoint
    start_idx = 0
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, encoding="utf-8") as f:
                cp = json.load(f)
            start_idx = cp.get("last_index", 0)
        except (json.JSONDecodeError, ValueError):
            start_idx = 0

    if os.path.exists(output_file) and start_idx > 0:
        try:
            with open(output_file, encoding="utf-8") as f:
                all_records = json.load(f)
            log.info(f"  Reprise depuis index {start_idx} ({len(all_records)} records déjà)")
        except (json.JSONDecodeError, ValueError):
            all_records = []

    # Compter aussi les HTML déjà en cache (pas encore parsés)
    html_cached = len([f for f in os.listdir(HTML_DIR) if f.endswith('.html')])
    log.info(f"  HTML en cache: {html_cached} | JSON parsés: {len(all_records)}")

    collected = len(all_records)
    errors = 0
    skipped = 0

    for i in range(start_idx, len(names_list)):
        name = names_list[i]

        # Anti-ban : si trop d'erreurs consécutives, grosse pause
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            pause = random.uniform(120, 300)
            log.warning(f"  🛑 {consecutive_errors} erreurs consécutives — pause {pause:.0f}s")
            time.sleep(pause)
            rotate_session()

        result = search_horse(name)

        if result and result.get("sire"):
            # Vérifier pas de doublon
            if not any(r.get("name") == result["name"] for r in all_records[-100:]):
                all_records.append(result)
                collected += 1
        elif result is None:
            # Vérifier si c'est juste un skip (cache HTML existe mais pas de pedigree)
            safe = re.sub(r'[^a-zA-Z\s]', '', name).strip().replace(' ', '_')[:50]
            if os.path.exists(os.path.join(HTML_DIR, f"{safe}.html")):
                skipped += 1
            else:
                errors += 1

        if (i + 1 - start_idx) % 50 == 0:
            log.info(f"  [{i+1}/{len(names_list)}] trouvés={collected} erreurs={errors} skip={skipped}")

        # Checkpoint fréquent (tous les 100 au lieu de 200)
        if (i + 1 - start_idx) % 100 == 0:
            checkpoint_data = {"last_index": i + 1, "collected": collected}
            tmp = output_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(all_records, f, ensure_ascii=False)
            os.replace(tmp, output_file)
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f)

        # Pause intelligente (seulement si on a fetch, pas si cache)
        safe = re.sub(r'[^a-zA-Z\s]', '', name).strip().replace(' ', '_')[:50]
        if not os.path.exists(os.path.join(HTML_DIR, f"{safe}.html")):
            smart_pause(5.0, 3.0)
        else:
            time.sleep(0.01)  # Juste un yield si cache

    # Sauvegarde finale
    save_and_exit()
    log.info(f"TERMINÉ: {collected} pedigrees trouvés sur {len(names_list)} chevaux")


if __name__ == "__main__":
    main()
