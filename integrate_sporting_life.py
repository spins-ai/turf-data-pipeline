#!/usr/bin/env python3
"""
integrate_sporting_life.py  (Etape 8.2)
Integre les ~32K records Sporting Life dans partants_master.
  - Lit output/57_sporting_life/sporting_life_data.jsonl
  - Filtre par type (racecard, result, tip, meeting_link, etc.)
  - Match avec partants_master par nom_cheval normalise + date
  - Ajoute champs sl_* (tips, racecards, links)
  - Ecrit data_master/partants_master_enrichi_sl.jsonl
"""

import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

from utils.normalize import normalize_date, normalize_name

BASE_DIR = Path(__file__).resolve().parent
SL_INPUT = BASE_DIR / "output" / "57_sporting_life" / "sporting_life_data.jsonl"
PARTANTS_MASTER = BASE_DIR / "data_master" / "partants_master.jsonl"
OUTPUT_FILE = BASE_DIR / "data_master" / "partants_master_enrichi_sl.jsonl"
LOG_DIR = BASE_DIR / "logs"

LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "integrate_sporting_life.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ── Extraction des chevaux depuis les records SL ──────────────────────

def extract_horse_name_from_url(url):
    """Extrait un nom de cheval depuis une URL Sporting Life."""
    if not url:
        return ""
    # Pattern: /horse/NOM-DU-CHEVAL-12345
    m = re.search(r"/horse/([a-z0-9\-]+?)(?:-\d+)?(?:\?|$|#)", str(url).lower())
    if m:
        name = m.group(1).replace("-", " ").upper()
        return normalize_name(name)
    # Pattern: /racecards/.../NOM
    m = re.search(r"/racecards/[^/]+/[^/]+/([a-z0-9\-]+)", str(url).lower())
    if m:
        name = m.group(1).replace("-", " ").upper()
        return normalize_name(name)
    return ""


def extract_horse_name_from_text(text):
    """Extrait un nom de cheval depuis le champ text."""
    if not text:
        return ""
    text = str(text).strip()
    # Ignorer les textes trop courts ou generiques
    if len(text) < 3 or text.lower() in {
        "fast results", "racecards", "full results", "tips",
        "results", "race cards", "naps", "today", "tomorrow",
    }:
        return ""
    # Ignorer les textes avec trop de mots (probablement des phrases)
    if len(text.split()) > 5:
        return ""
    return normalize_name(text)


def parse_sl_records(filepath):
    """
    Charge et classe les records Sporting Life.
    Retourne:
      - horse_data: dict (nom_norm, date) -> liste d'infos
      - date_links: dict date -> liste d'URLs/tips pour la date
      - stats: compteurs par type
    """
    horse_data = defaultdict(list)
    date_links = defaultdict(list)
    stats = defaultdict(int)
    n_records = 0
    n_errors = 0

    if not filepath.exists():
        log.warning("Fichier Sporting Life introuvable : %s", filepath)
        return horse_data, date_links, stats

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                n_records += 1
            except json.JSONDecodeError:
                n_errors += 1
                continue

            rec_type = record.get("type", "unknown")
            stats[rec_type] += 1
            date = normalize_date(record.get("date", ""))
            if not date:
                continue

            text = record.get("text", "")
            url = record.get("url", "")

            # Stocker les liens par date pour enrichissement au niveau course
            date_links[date].append({
                "type": rec_type,
                "text": text,
                "url": url,
            })

            # Essayer d'extraire un nom de cheval
            horse_name = extract_horse_name_from_url(url)
            if not horse_name:
                horse_name = extract_horse_name_from_text(text)

            if horse_name and len(horse_name) >= 3:
                entry = {
                    "name": horse_name,
                    "date": date,
                    "sl_type": rec_type,
                    "sl_text": text,
                    "sl_url": url,
                }
                horse_data[(horse_name, date)].append(entry)

    log.info(
        "Sporting Life charge : %d records, %d erreurs", n_records, n_errors
    )
    log.info("  Types : %s", dict(stats))
    log.info("  Chevaux indexes : %d cles uniques", len(horse_data))
    log.info("  Dates avec liens : %d", len(date_links))
    return horse_data, date_links, stats


def merge_sl_into_partant(partant, sl_entries, date_links_for_date):
    """Ajoute les champs sl_* a un partant."""
    enriched = False

    # Enrichissement par cheval match
    if sl_entries:
        partant["sl_source"] = "sporting_life"
        partant["sl_nb_records"] = len(sl_entries)

        # Collecter les types
        types = list(set(e.get("sl_type", "") for e in sl_entries))
        partant["sl_record_types"] = types

        # Collecter les URLs utiles
        urls = [e.get("sl_url", "") for e in sl_entries if e.get("sl_url")]
        if urls:
            partant["sl_urls"] = urls[:5]  # max 5

        # Checker si le cheval apparait dans des tips
        has_tip = any(e.get("sl_type") == "tip" for e in sl_entries)
        partant["sl_has_tip"] = has_tip

        # Texts associes
        texts = [e.get("sl_text", "") for e in sl_entries if e.get("sl_text")]
        if texts:
            partant["sl_texts"] = texts[:5]

        enriched = True

    # Enrichissement par date (liens generaux de la journee)
    if date_links_for_date:
        n_links = len(date_links_for_date)
        partant["sl_date_nb_links"] = n_links

        # Compter les types de liens pour cette date
        link_types = defaultdict(int)
        for link in date_links_for_date:
            link_types[link.get("type", "unknown")] += 1
        partant["sl_date_link_types"] = dict(link_types)

        enriched = True

    return partant, enriched


# ── Pipeline principal ────────────────────────────────────────────────

def main():
    start = time.time()
    log.info("=" * 60)
    log.info("INTEGRATION SPORTING LIFE -> partants_master")
    log.info("=" * 60)

    # 1. Charger et indexer Sporting Life
    horse_data, date_links, stats = parse_sl_records(SL_INPUT)
    if not horse_data and not date_links:
        log.warning("Aucune donnee Sporting Life a integrer. Arret.")
        return

    # 2. Lire partants_master et enrichir
    if not PARTANTS_MASTER.exists():
        log.error("partants_master.jsonl introuvable : %s", PARTANTS_MASTER)
        sys.exit(1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    n_partants = 0
    n_matched_horse = 0
    n_matched_date = 0
    n_written = 0

    with open(PARTANTS_MASTER, "r", encoding="utf-8", errors="replace") as fin, \
         open(OUTPUT_FILE, "w", encoding="utf-8") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                partant = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_partants += 1

            nom = normalize_name(partant.get("nom_cheval", ""))
            date = normalize_date(partant.get("date_reunion_iso", ""))

            sl_entries = None
            dl_entries = None

            if nom and date:
                key = (nom, date)
                sl_entries = horse_data.get(key)
                dl_entries = date_links.get(date)

            partant, enriched = merge_sl_into_partant(
                partant, sl_entries, dl_entries
            )

            if sl_entries:
                n_matched_horse += 1
            if dl_entries:
                n_matched_date += 1

            fout.write(json.dumps(partant, ensure_ascii=False) + "\n")
            n_written += 1

            if n_partants % 200000 == 0:
                log.info(
                    "  ... %d partants, %d horse-match, %d date-match",
                    n_partants, n_matched_horse, n_matched_date,
                )

    elapsed = time.time() - start

    log.info("=" * 60)
    log.info("RESULTATS INTEGRATION SPORTING LIFE")
    log.info("  Partants traites   : %d", n_partants)
    log.info("  Matches par cheval : %d (%.1f%%)", n_matched_horse,
             100.0 * n_matched_horse / max(1, n_partants))
    log.info("  Matches par date   : %d (%.1f%%)", n_matched_date,
             100.0 * n_matched_date / max(1, n_partants))
    log.info("  Output ecrit       : %s", OUTPUT_FILE)
    log.info("  Duree              : %.1f s", elapsed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
