#!/usr/bin/env python3
"""
integrate_timeform.py  (Etape 8.1)
Integre les ~68K records Timeform dans partants_master.
  - Lit output/56_timeform/timeform_data.jsonl
  - Match avec partants_master par nom_cheval normalise + date
  - Ajoute champs tf_* (rating, speed figure, position, odds, etc.)
  - Ecrit data_master/partants_master_enrichi_tf.jsonl
"""

import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

from utils.normalize import normalize_name

BASE_DIR = Path(__file__).resolve().parent
TF_INPUT = BASE_DIR / "output" / "56_timeform" / "timeform_data.jsonl"
PARTANTS_MASTER = BASE_DIR / "data_master" / "partants_master.jsonl"
OUTPUT_FILE = BASE_DIR / "data_master" / "partants_master_enrichi_tf.jsonl"
LOG_DIR = BASE_DIR / "logs"

LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "integrate_timeform.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def normalize_date(date_str):
    """Normalise une date au format YYYY-MM-DD."""
    if not date_str:
        return ""
    date_str = str(date_str).strip()
    # Deja au bon format
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return m.group(0)
    # Format DD/MM/YYYY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return date_str[:10]


# ── Parsing Timeform records ──────────────────────────────────────────

def extract_horse_from_column(col_name, col_value):
    """
    Parse les colonnes dynamiques Timeform du type:
      '3._BACKBYJET_(IRE)25/126' -> {name: BACKBYJET, position: 3, odds_num: 25, odds_den: 1, ...}
      '4. HAWKSEYE VIEW13/82.62' -> {name: HAWKSEYE VIEW, position: 4, ...}
    """
    result = {}

    # Pattern: position._NOM_(PAYS)odds_num/odds_denspeed_fig
    # Essayer de parser le nom de colonne
    m = re.match(
        r"(\d+)\.\s*([A-Z_\s'\-]+?)(?:\s*\(([A-Z]{2,4})\))?\s*(\d+)?/?(\d+)?([\d.]+)?$",
        col_name.upper().replace("_", " ").strip(),
    )
    if m:
        result["position"] = int(m.group(1))
        result["name"] = normalize_name(m.group(2))
        if m.group(3):
            result["country"] = m.group(3)
        if m.group(4):
            result["odds_numerator"] = int(m.group(4))
        if m.group(5):
            result["odds_denominator"] = int(m.group(5))
        if m.group(6):
            try:
                result["speed_figure"] = float(m.group(6))
            except (ValueError, TypeError):
                pass
        return result

    # Essayer le format de la valeur
    m2 = re.match(
        r"(\d+)\.\s*([A-Za-z\s'\-]+?)(?:\s*\(([A-Z]{2,4})\))?\s*(\d+)?/?(\d+)?([\d.]+)?(?:[fF])?$",
        str(col_value).strip(),
    )
    if m2:
        result["position"] = int(m2.group(1))
        result["name"] = normalize_name(m2.group(2))
        if m2.group(3):
            result["country"] = m2.group(3)
        if m2.group(4):
            result["odds_numerator"] = int(m2.group(4))
        if m2.group(5):
            result["odds_denominator"] = int(m2.group(5))
        if m2.group(6):
            try:
                result["speed_figure"] = float(m2.group(6))
            except (ValueError, TypeError):
                pass
        return result

    return None


def parse_tf_record(record):
    """
    Parse un record Timeform brut et extrait une liste de chevaux
    avec leurs attributs pour cette date.
    """
    date = normalize_date(record.get("date", ""))
    if not date:
        return []

    horses = []
    standard_keys = {"date", "source", "type", "scraped_at", "1st", "col_1"}

    for key, value in record.items():
        if key in standard_keys:
            continue
        # Essayer de parser comme colonne cheval
        horse_info = extract_horse_from_column(key, value)
        if horse_info and horse_info.get("name"):
            horse_info["date"] = date
            horse_info["tf_type"] = record.get("type", "")
            horses.append(horse_info)

        # Aussi parser la valeur (souvent un autre cheval)
        if value and str(value).strip():
            horse_info2 = extract_horse_from_column("", str(value))
            if horse_info2 and horse_info2.get("name"):
                horse_info2["date"] = date
                horse_info2["tf_type"] = record.get("type", "")
                horses.append(horse_info2)

    return horses


# ── Chargement ────────────────────────────────────────────────────────

def load_timeform_index():
    """Charge et indexe les records Timeform par (nom_normalise, date)."""
    if not TF_INPUT.exists():
        log.warning("Fichier Timeform introuvable : %s", TF_INPUT)
        return {}

    index = defaultdict(list)
    n_records = 0
    n_horses = 0
    n_errors = 0

    with open(TF_INPUT, "r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                n_records += 1
            except json.JSONDecodeError:
                n_errors += 1
                continue

            horses = parse_tf_record(record)
            for h in horses:
                key = (h["name"], h["date"])
                index[key].append(h)
                n_horses += 1

    log.info(
        "Timeform charge : %d records, %d chevaux indexes, %d erreurs parse",
        n_records, n_horses, n_errors,
    )
    log.info("Timeform index : %d cles uniques (nom+date)", len(index))
    return index


def merge_tf_into_partant(partant, tf_entries):
    """Ajoute les champs tf_* a un partant depuis les entries Timeform."""
    if not tf_entries:
        return partant

    # Prendre la meilleure entree (preferer result > racecard)
    best = tf_entries[0]
    for e in tf_entries:
        if e.get("tf_type") == "result":
            best = e
            break

    partant["tf_source"] = "timeform"
    if "position" in best:
        partant["tf_position"] = best["position"]
    if "speed_figure" in best:
        partant["tf_speed_figure"] = best["speed_figure"]
    if "odds_numerator" in best and "odds_denominator" in best:
        partant["tf_odds_numerator"] = best["odds_numerator"]
        partant["tf_odds_denominator"] = best["odds_denominator"]
        denom = best["odds_denominator"]
        if denom and denom > 0:
            partant["tf_odds_decimal"] = round(
                1.0 + best["odds_numerator"] / denom, 3
            )
    if "country" in best:
        partant["tf_country"] = best["country"]
    if "tf_type" in best:
        partant["tf_record_type"] = best["tf_type"]

    # Collecter toutes les speed figures disponibles
    all_speeds = [e["speed_figure"] for e in tf_entries if "speed_figure" in e]
    if all_speeds:
        partant["tf_speed_figure_max"] = max(all_speeds)
        partant["tf_speed_figure_avg"] = round(sum(all_speeds) / len(all_speeds), 2)

    return partant


# ── Pipeline principal ────────────────────────────────────────────────

def main():
    start = time.time()
    log.info("=" * 60)
    log.info("INTEGRATION TIMEFORM -> partants_master")
    log.info("=" * 60)

    # 1. Charger index Timeform
    tf_index = load_timeform_index()
    if not tf_index:
        log.warning("Aucune donnee Timeform a integrer. Arret.")
        return

    # 2. Lire partants_master et enrichir
    if not PARTANTS_MASTER.exists():
        log.error("partants_master.jsonl introuvable : %s", PARTANTS_MASTER)
        sys.exit(1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    n_partants = 0
    n_matched = 0
    n_written = 0
    match_by_date = defaultdict(int)

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

            # Cle de matching
            nom = normalize_name(partant.get("nom_cheval", ""))
            date = normalize_date(partant.get("date_reunion_iso", ""))

            if nom and date:
                key = (nom, date)
                tf_entries = tf_index.get(key)
                if tf_entries:
                    partant = merge_tf_into_partant(partant, tf_entries)
                    n_matched += 1
                    match_by_date[date] += 1

            fout.write(json.dumps(partant, ensure_ascii=False) + "\n")
            n_written += 1

            if n_partants % 200000 == 0:
                log.info("  ... %d partants traites, %d matches TF", n_partants, n_matched)

    elapsed = time.time() - start

    log.info("=" * 60)
    log.info("RESULTATS INTEGRATION TIMEFORM")
    log.info("  Partants traites  : %d", n_partants)
    log.info("  Matches Timeform  : %d (%.1f%%)", n_matched,
             100.0 * n_matched / max(1, n_partants))
    log.info("  Output ecrit      : %s", OUTPUT_FILE)
    log.info("  Duree             : %.1f s", elapsed)
    log.info("=" * 60)

    # Top 10 dates par nb matches
    if match_by_date:
        top_dates = sorted(match_by_date.items(), key=lambda x: -x[1])[:10]
        log.info("Top 10 dates par matches Timeform :")
        for d, c in top_dates:
            log.info("  %s : %d matches", d, c)


if __name__ == "__main__":
    main()
