#!/usr/bin/env python3
"""
nettoyage_global.py — Étape 3 du TODO
=======================================
Nettoyage global de toutes les données.

Opérations :
  1. Fix UTF-8 cassé
  2. Normalisation accents/casse (chevaux, jockeys, hippodromes)
  3. Uniformisation formats dates (ISO 8601)
  4. Uniformisation formats numériques
  5. Remplacement null/None/""/N/A → null cohérent
  6. Trim espaces
  7. Normalisation noms hippodromes
  8. Normalisation noms jockeys/entraîneurs
  9. Normalisation disciplines
  10. Identification champs 100% vides

Fonctionne en streaming JSONL pour rester léger en RAM.

Input : partants_normalises.jsonl (ou .json)
Output : output/nettoyage/partants_nettoyes.jsonl + rapport

Usage :
    python3 nettoyage_global.py
"""

import json
import logging
import os
import re
import sys
from collections import Counter

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "nettoyage")
os.makedirs(OUTPUT_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("nettoyage_global")


# ================================================================
# NORMALISATION
# ================================================================

def fix_utf8(text):
    """Corrige les problèmes UTF-8 courants."""
    if not isinstance(text, str):
        return text
    # Mojibake courant
    replacements = {
        "Ã©": "é", "Ã¨": "è", "Ã ": "à", "Ã¢": "â", "Ãª": "ê",
        "Ã®": "î", "Ã´": "ô", "Ã»": "û", "Ã§": "ç", "Ã¼": "ü",
        "\x00": "", "\ufffd": "",
    }
    for old, new in replacements.items():
        if old in text:
            text = text.replace(old, new)
    return text


def normalize_name(name):
    """Normalise un nom propre (cheval, jockey, entraîneur)."""
    if not name or not isinstance(name, str):
        return name
    name = fix_utf8(name)
    name = name.strip()
    # Supprimer les caractères parasites
    name = re.sub(r'[^\w\s\'-àâäéèêëïîôùûüÿçæœÀÂÄÉÈÊËÏÎÔÙÛÜŸÇÆŒ]', '', name)
    # Supprimer espaces multiples
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def normalize_hippo(hippo):
    """Normalise un nom d'hippodrome."""
    if not hippo or not isinstance(hippo, str):
        return hippo

    hippo = fix_utf8(hippo).strip().lower()
    hippo = re.sub(r'\s+', '-', hippo)
    hippo = re.sub(r'-+', '-', hippo).strip('-')

    # Aliases connus
    aliases = {
        "vincennes": "vincennes",
        "paris-vincennes": "vincennes",
        "hippodrome-de-vincennes": "vincennes",
        "longchamp": "longchamp",
        "paris-longchamp": "longchamp",
        "hippodrome-de-longchamp": "longchamp",
        "auteuil": "auteuil",
        "paris-auteuil": "auteuil",
        "enghien": "enghien",
        "enghien-soisy": "enghien",
        "maisons-laffitte": "maisons-laffitte",
        "maison-laffitte": "maisons-laffitte",
        "saint-cloud": "saint-cloud",
        "st-cloud": "saint-cloud",
        "cagnes-sur-mer": "cagnes-sur-mer",
        "cagnes": "cagnes-sur-mer",
        "lyon-parilly": "lyon-parilly",
        "lyon-la-soie": "lyon-la-soie",
        "marseille-borely": "marseille-borely",
        "marseille-borely-vivaux": "marseille-borely",
        "salon-de-provence": "salon-de-provence",
        "salon": "salon-de-provence",
        "le-croise-laroche": "le-croise-laroche",
        "croise-laroche": "le-croise-laroche",
        "la-capelle": "la-capelle",
        "cabourg": "cabourg",
        "caen": "caen",
        "clairefontaine": "clairefontaine",
        "clairefontaine-deauville": "clairefontaine",
    }

    return aliases.get(hippo, hippo)


def normalize_discipline(disc):
    """Normalise une discipline."""
    if not disc or not isinstance(disc, str):
        return disc

    d = disc.strip().lower()
    aliases = {
        "attele": "trot_attele", "trot attele": "trot_attele", "trot_attele": "trot_attele",
        "monte": "trot_monte", "trot monte": "trot_monte", "trot_monte": "trot_monte",
        "plat": "plat", "galop": "plat", "galop_plat": "plat",
        "obstacle": "obstacle",
        "steeple": "steeple", "steeplechase": "steeple", "steeple-chase": "steeple",
        "haies": "haies", "haie": "haies",
        "cross": "cross_country", "cross-country": "cross_country", "cross_country": "cross_country",
    }
    return aliases.get(d, d)


def normalize_null(value):
    """Remplace les valeurs nulles/vides par None."""
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        if v in ("", "null", "None", "N/A", "n/a", "NA", "nan", "NaN", "-", "?", "inconnu", "INCONNU"):
            return None
        return v
    if isinstance(value, float):
        import math
        if math.isnan(value) or math.isinf(value):
            return None
    return value


def normalize_date(date_str):
    """Normalise une date en ISO 8601 (YYYY-MM-DD)."""
    if not date_str or not isinstance(date_str, str):
        return date_str

    date_str = date_str.strip()

    # Déjà ISO ?
    if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]

    # DD/MM/YYYY
    m = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # DDMMYYYY
    m = re.match(r'^(\d{2})(\d{2})(\d{4})$', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    return date_str


def clean_record(record):
    """Nettoie un record partant."""
    if not isinstance(record, dict):
        return record

    cleaned = {}
    changes = 0

    for key, value in record.items():
        original = value

        # 1. Fix UTF-8
        if isinstance(value, str):
            value = fix_utf8(value)

        # 2. Normalize null
        value = normalize_null(value)

        # 3. Normalisation spécifique par champ
        if key in ("nom_cheval", "pere", "mere", "pere_mere"):
            if isinstance(value, str):
                value = normalize_name(value).upper() if value else value
        elif key in ("jockey_driver", "entraineur", "proprietaire", "eleveur"):
            if isinstance(value, str):
                value = normalize_name(value)
        elif key == "hippodrome_normalise":
            if isinstance(value, str):
                value = normalize_hippo(value)
        elif key == "discipline":
            if isinstance(value, str):
                value = normalize_discipline(value)
        elif key == "date_reunion_iso":
            if isinstance(value, str):
                value = normalize_date(value)
        elif key in ("sexe", "robe", "allure", "statut", "incident", "oeilleres", "deferre"):
            if isinstance(value, str):
                value = value.strip().lower()

        cleaned[key] = value
        if value != original:
            changes += 1

    return cleaned, changes


# ================================================================
# MAIN
# ================================================================

def main():
    log.info("=" * 70)
    log.info("NETTOYAGE GLOBAL — Étape 3")
    log.info("=" * 70)

    # Trouver le fichier source
    source_path = None
    for path in [os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.jsonl"),
                 os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.json")]:
        if os.path.exists(path):
            source_path = path
            break

    if not source_path:
        log.error("Aucun fichier partants trouvé")
        sys.exit(1)

    log.info(f"Source: {source_path}")

    output_file = os.path.join(OUTPUT_DIR, "partants_nettoyes.jsonl")
    stats = {
        "total": 0,
        "records_modifies": 0,
        "total_changes": 0,
        "field_null_before": Counter(),
        "field_null_after": Counter(),
        "hippos_normalisees": Counter(),
        "disciplines_normalisees": Counter(),
    }

    log.info("Nettoyage en cours...")

    with open(output_file, "w", encoding="utf-8", newline="\n") as fout:
        if source_path.endswith(".jsonl"):
            with open(source_path, "r", encoding="utf-8") as fin:
                for line in fin:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    stats["total"] += 1

                    # Compter null avant
                    for k, v in record.items():
                        if v is None or v == "" or v == []:
                            stats["field_null_before"][k] += 1

                    cleaned, changes = clean_record(record)

                    # Compter null après
                    for k, v in cleaned.items():
                        if v is None:
                            stats["field_null_after"][k] += 1

                    if changes > 0:
                        stats["records_modifies"] += 1
                        stats["total_changes"] += changes

                    # Stats
                    hippo = cleaned.get("hippodrome_normalise", "")
                    if hippo:
                        stats["hippos_normalisees"][hippo] += 1
                    disc = cleaned.get("discipline", "")
                    if disc:
                        stats["disciplines_normalisees"][disc] += 1

                    fout.write(json.dumps(cleaned, ensure_ascii=False, default=str) + "\n")

                    if stats["total"] % 200000 == 0:
                        log.info(f"  {stats['total']} traités, {stats['records_modifies']} modifiés")

        else:
            # JSON
            with open(source_path, "r", encoding="utf-8") as fin:
                data = json.load(fin)

            for record in data:
                stats["total"] += 1
                for k, v in record.items():
                    if v is None or v == "" or v == []:
                        stats["field_null_before"][k] += 1

                cleaned, changes = clean_record(record)

                for k, v in cleaned.items():
                    if v is None:
                        stats["field_null_after"][k] += 1

                if changes > 0:
                    stats["records_modifies"] += 1
                    stats["total_changes"] += changes

                # Stats
                hippo = cleaned.get("hippodrome_normalise", "")
                if hippo:
                    stats["hippos_normalisees"][hippo] += 1
                disc = cleaned.get("discipline", "")
                if disc:
                    stats["disciplines_normalisees"][disc] += 1

                fout.write(json.dumps(cleaned, ensure_ascii=False, default=str) + "\n")

                if stats["total"] % 200000 == 0:
                    log.info(f"  {stats['total']} traités, {stats['records_modifies']} modifiés")

            del data

    # Rapport
    log.info(f"Nettoyage terminé:")
    log.info(f"  Total: {stats['total']}")
    log.info(f"  Modifiés: {stats['records_modifies']} ({100*stats['records_modifies']/max(stats['total'],1):.1f}%)")
    log.info(f"  Changements: {stats['total_changes']}")
    log.info(f"  Output: {output_file}")

    # Sauver rapport
    report = {
        "total": stats["total"],
        "records_modifies": stats["records_modifies"],
        "total_changes": stats["total_changes"],
        "hippodromes_uniques": len(stats["hippos_normalisees"]),
        "disciplines": dict(stats["disciplines_normalisees"]),
        "champs_100pct_null": [k for k, v in stats["field_null_after"].items() if v == stats["total"]],
    }

    report_path = os.path.join(OUTPUT_DIR, "nettoyage_rapport.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    log.info(f"Rapport: {report_path}")
    log.info(f"Hippodromes uniques: {len(stats['hippos_normalisees'])}")
    log.info(f"Disciplines: {dict(stats['disciplines_normalisees'])}")

    if report["champs_100pct_null"]:
        log.warning(f"Champs 100% null (à supprimer): {report['champs_100pct_null']}")

    log.info("=" * 70)
    log.info("TERMINÉ")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
