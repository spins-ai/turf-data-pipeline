#!/usr/bin/env python3
"""
Script 21 — Rapports définitifs PMU (dividendes)
Source : online.turfinfo.api.pmu.fr/rest/client/1/programme/{date}/R{r}/C{c}/rapports-definitifs
Collecte tous les dividendes : Simple Gagnant/Placé, Couplé, Tiercé, Quarté, Quinté
CRITIQUE pour : Value Hunter, Kelly, ROI Predictor, Ticket Optimizer
"""

import requests
import json
import time
import random
import os
import logging
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "21_rapports_definitifs")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

log = setup_logging("21_rapports_definitifs")

BASE_URL = "https://online.turfinfo.api.pmu.fr/rest/client/1/programme"

def smart_pause(base=0.3, jitter=0.2):
    time.sleep(base + random.uniform(-jitter, jitter))

def load_courses_references():
    """Charger toutes les courses connues depuis les scripts 02 et 02b (mode léger)"""
    courses = []
    KEEP = {"course_uid", "date_reunion_iso", "numero_reunion", "numero_course", "hippodrome_normalise", "discipline", "distance"}
    paths = [
        os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_normalisees.json"),
        os.path.join(BASE_DIR, "output", "02b_liste_courses_2013", "courses_normalisees.json"),
    ]
    seen_uids = set()
    for path in paths:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            for c in data:
                uid = c.get("course_uid", "")
                if uid and uid not in seen_uids:
                    seen_uids.add(uid)
                    courses.append({k: c[k] for k in KEEP if k in c})
            del data
    log.info(f"Chargé {len(courses)} courses uniques (mode léger)")
    return courses

def fetch_rapports(date_str, numero_reunion, num_course):
    """Récupérer les rapports définitifs d'une course"""
    # Cache
    cache_key = f"{date_str}_R{numero_reunion}_C{num_course}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            os.remove(cache_file)

    url = f"{BASE_URL}/{date_str}/R{numero_reunion}/C{num_course}/rapports-definitifs"
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        if resp.status_code == 200:
            data = resp.json()
            if data:  # Ne pas cacher les réponses vides
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
            return data
        elif resp.status_code == 429:
            time.sleep(30)
            return None
        else:
            return None
    except Exception as e:
        log.debug(f"  Erreur réseau rapports: {e}")
        return None

def extract_rapports_flat(rapports_raw, course_info):
    """Aplatir les rapports en un dict simple"""
    result = {
        "course_uid": course_info.get("course_uid", ""),
        "date_reunion_iso": course_info.get("date_reunion_iso", ""),
        "hippodrome": course_info.get("hippodrome_normalise", ""),
        "numero_reunion": course_info.get("numero_reunion", ""),
        "num_course": course_info.get("numero_course", ""),
        "discipline": course_info.get("discipline", ""),
        "distance": course_info.get("distance", ""),
    }

    for pari in rapports_raw:
        type_pari = pari.get("typePari", "")
        famille = pari.get("famillePari", "")
        rapports = pari.get("rapports", [])

        if type_pari == "SIMPLE_GAGNANT" and rapports:
            r = rapports[0]
            result["rapport_simple_gagnant"] = r.get("dividendePourUnEuro")
            result["combinaison_gagnant"] = r.get("combinaison")

        elif type_pari == "SIMPLE_PLACE" and rapports:
            for i, r in enumerate(rapports[:3]):
                result[f"rapport_simple_place_{i+1}"] = r.get("dividendePourUnEuro")
                result[f"combinaison_place_{i+1}"] = r.get("combinaison")

        elif type_pari == "COUPLE_GAGNANT" and rapports:
            r = rapports[0]
            result["rapport_couple_gagnant"] = r.get("dividendePourUnEuro")
            result["combinaison_couple_gagnant"] = r.get("combinaison")

        elif type_pari == "COUPLE_PLACE" and rapports:
            for i, r in enumerate(rapports[:3]):
                result[f"rapport_couple_place_{i+1}"] = r.get("dividendePourUnEuro")

        elif type_pari == "TIERCE" and rapports:
            for r in rapports:
                label = r.get("libelle", "").lower()
                if "ordre" in label:
                    result["rapport_tierce_ordre"] = r.get("dividendePourUnEuro")
                elif "désordre" in label or "desordre" in label:
                    result["rapport_tierce_desordre"] = r.get("dividendePourUnEuro")
                else:
                    result["rapport_tierce"] = r.get("dividendePourUnEuro")

        elif type_pari == "QUARTE_PLUS" and rapports:
            for r in rapports:
                label = r.get("libelle", "").lower()
                if "ordre" in label and "bonus" not in label:
                    result["rapport_quarte_ordre"] = r.get("dividendePourUnEuro")
                elif "désordre" in label or "desordre" in label:
                    result["rapport_quarte_desordre"] = r.get("dividendePourUnEuro")
                elif "bonus" in label:
                    result["rapport_quarte_bonus"] = r.get("dividendePourUnEuro")

        elif type_pari == "QUINTE_PLUS" and rapports:
            for r in rapports:
                label = r.get("libelle", "").lower()
                if "ordre" in label and "bonus" not in label:
                    result["rapport_quinte_ordre"] = r.get("dividendePourUnEuro")
                elif "désordre" in label or "desordre" in label:
                    result["rapport_quinte_desordre"] = r.get("dividendePourUnEuro")
                elif "bonus" in label:
                    result["rapport_quinte_bonus3"] = r.get("dividendePourUnEuro")

        elif type_pari == "MULTI" and rapports:
            for r in rapports:
                label = r.get("libelle", "").lower()
                if "4" in label:
                    result["rapport_multi_4"] = r.get("dividendePourUnEuro")
                elif "5" in label:
                    result["rapport_multi_5"] = r.get("dividendePourUnEuro")
                elif "6" in label:
                    result["rapport_multi_6"] = r.get("dividendePourUnEuro")
                elif "7" in label:
                    result["rapport_multi_7"] = r.get("dividendePourUnEuro")

        elif type_pari == "DEUX_SUR_QUATRE" and rapports:
            result["rapport_2sur4_nb_combinaisons"] = len(rapports)
            if rapports:
                result["rapport_2sur4_min"] = min(r.get("dividendePourUnEuro", 0) for r in rapports)
                result["rapport_2sur4_max"] = max(r.get("dividendePourUnEuro", 0) for r in rapports)

    return result

def main():
    log.info("=" * 60)
    log.info("SCRIPT 21 — Rapports définitifs PMU")
    log.info("=" * 60)

    # Charger les courses
    courses = load_courses_references()
    if not courses:
        log.error("Aucune course trouvée")
        return

    # Trier par date
    courses.sort(key=lambda c: c.get("date_reunion_iso", ""))

    # Checkpoint
    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_21.json")
    output_file = os.path.join(OUTPUT_DIR, "rapports_definitifs.jsonl")
    start_idx = 0
    total_rapports = 0
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r", encoding="utf-8", errors="replace") as f:
            cp = json.load(f)
        start_idx = cp.get("last_index", 0)
        total_rapports = cp.get("total_rapports", 0)
        log.info(f"Reprise au checkpoint: index {start_idx}, {total_rapports} rapports déjà écrits")

    errors = 0
    empty = 0
    collected = 0
    req_count = 0

    for i in range(start_idx, len(courses)):
        course = courses[i]
        date_iso = course.get("date_reunion_iso", "")
        num_r = course.get("numero_reunion", "")
        num_c = course.get("numero_course", "")

        if not date_iso or not num_r or not num_c:
            continue

        try:
            dt = datetime.strptime(date_iso[:10], "%Y-%m-%d")
            date_api = dt.strftime("%d%m%Y")
        except (ValueError, TypeError):
            continue

        rapports_raw = fetch_rapports(date_api, num_r, num_c)
        req_count += 1

        if rapports_raw and len(rapports_raw) > 0:
            flat = extract_rapports_flat(rapports_raw, course)
            with open(output_file, "a", encoding="utf-8", newline="\n") as f:
                f.write(json.dumps(flat, ensure_ascii=False) + "\n")
            total_rapports += 1
            collected += 1
        elif rapports_raw is not None:
            empty += 1
        else:
            errors += 1

        if (i + 1 - start_idx) % 100 == 0:
            log.info(f"  [{i+1}/{len(courses)}] rapports={collected} total={total_rapports} vides={empty} erreurs={errors}")

        if (i + 1 - start_idx) % 500 == 0:
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump({"last_index": i + 1, "total_rapports": total_rapports}, f)
            log.info(f">>> Checkpoint: {total_rapports} rapports <<<")

        smart_pause(0.2, 0.1)

    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump({"last_index": len(courses), "total_rapports": total_rapports}, f)

    log.info("=" * 60)
    log.info(f"TERMINÉ: {total_rapports} rapports, {errors} erreurs")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
