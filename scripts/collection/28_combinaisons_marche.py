#!/usr/bin/env python3
"""
Script 28 — Combinaisons & Masse d'enjeux PMU (structure du marché des paris)
Source : offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}/R{r}/C{c}/combinaisons
CRITIQUE pour : Betting Strategy, Market Analysis, Value Detection, Rapport Probable
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json
import time
import random
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, create_session, rotate_session as _rotate_session

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "../../output", "28_combinaisons_marche")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

log = setup_logging("28_combinaisons_marche")

BASE_URL = "https://offline.turfinfo.api.pmu.fr/rest/client/7/programme"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
]

_HEADERS = {"Accept": "application/json", "Accept-Language": "fr-FR,fr;q=0.9", "DNT": "1"}
session = create_session(user_agents=USER_AGENTS)
req_count = 0

def rotate_session():
    global session, req_count
    session = _rotate_session(user_agents=USER_AGENTS, headers=_HEADERS)
    req_count = 0

def load_courses():
    courses = []
    seen = set()
    KEEP = {"course_uid", "date_reunion_iso", "numero_reunion", "numero_course", "hippodrome_normalise"}
    for path in [
        os.path.join(BASE_DIR, "../../output", "02_liste_courses", "courses_normalisees.json"),
        os.path.join(BASE_DIR, "../../output", "02b_liste_courses_2013", "courses_normalisees.json"),
    ]:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            for c in data:
                uid = c.get("course_uid", "")
                if uid and uid not in seen:
                    seen.add(uid)
                    courses.append({k: c[k] for k in KEEP if k in c})
            del data
    courses.sort(key=lambda c: c.get("date_reunion_iso", ""))
    log.info(f"Chargé {len(courses)} courses uniques (mode léger)")
    return courses

def fetch_combinaisons(date_str, num_r, num_c):
    global req_count
    cache_key = f"{date_str}_R{num_r}_C{num_c}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            os.remove(cache_file)

    url = f"{BASE_URL}/{date_str}/R{num_r}/C{num_c}/combinaisons"
    try:
        resp = session.get(url, timeout=15)
        req_count += 1
        if req_count >= random.randint(40, 50):
            rotate_session()

        if resp.status_code == 200:
            data = resp.json()
            if data and "combinaisons" in data:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                return data
        elif resp.status_code == 429:
            log.warning("429 — pause 30s")
            time.sleep(30)
            rotate_session()
        return None
    except Exception as e:
        log.debug(f"Erreur {cache_key}: {e}")
        return None

def flatten_combinaisons(data, course_info):
    """Extraire les top combinaisons par type de pari + masse totale"""
    records = []
    course_uid = course_info.get("course_uid", "")
    date_iso = course_info.get("date_reunion_iso", "")
    hippo = course_info.get("hippodrome_normalise", "")

    for combo_block in data.get("combinaisons", []):
        type_pari = combo_block.get("pariType", "")
        total_enjeu = combo_block.get("totalEnjeu", 0)
        combis = combo_block.get("listeCombinaisons", [])

        # Top 5 combinaisons par type de pari
        for rang, c in enumerate(combis[:5], 1):
            records.append({
                "course_uid": course_uid,
                "date_reunion_iso": date_iso,
                "hippodrome": hippo,
                "type_pari": type_pari,
                "total_enjeu_pari": total_enjeu,
                "rang_combinaison": rang,
                "combinaison": c.get("combinaison", []),
                "enjeu_combinaison": c.get("totalEnjeu", 0),
                "pct_masse": round(c.get("totalEnjeu", 0) / total_enjeu * 100, 2) if total_enjeu > 0 else 0,
            })

    return records

def export_cache_to_jsonl():
    """Export cache to JSONL without scraping."""
    jsonl_file = os.path.join(OUTPUT_DIR, "combinaisons_marche_cache.jsonl")
    log.info(f"Export cache → {jsonl_file}")
    count = 0
    with open(jsonl_file, "w", encoding="utf-8") as fout:
        for fname in sorted(os.listdir(CACHE_DIR)):
            if not fname.endswith(".json"):
                continue
            cache_path = os.path.join(CACHE_DIR, fname)
            try:
                with open(cache_path, encoding="utf-8") as fin:
                    data = json.load(fin)
                if isinstance(data, list):
                    for entry in data:
                        fout.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        count += 1
                elif data:
                    fout.write(json.dumps(data, ensure_ascii=False) + "\n")
                    count += 1
            except Exception as e:
                log.debug(f"  Erreur lecture cache {fname}: {e}")
    log.info(f"  JSONL: {count} entrées → {jsonl_file}")
    return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Script 28 — Combinaisons & Masse d'enjeux PMU")
    parser.add_argument("--export", action="store_true",
                        help="Export cache to JSONL without scraping")
    args = parser.parse_args()

    if args.export:
        log.info("=" * 60)
        log.info("SCRIPT 28 — Export cache → JSONL (--export)")
        log.info("=" * 60)
        export_cache_to_jsonl()
        return

    log.info("=" * 60)
    log.info("SCRIPT 28 — Combinaisons & Masse d'enjeux PMU")
    log.info("=" * 60)

    rotate_session()
    courses = load_courses()
    if not courses:
        log.error("Aucune course trouvée")
        return

    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_28.json")
    output_file = os.path.join(OUTPUT_DIR, "combinaisons_marche.jsonl")
    start_idx = 0
    total_records = 0
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r", encoding="utf-8", errors="replace") as f:
            cp = json.load(f)
        start_idx = cp.get("last_index", 0)
        total_records = cp.get("total_records", 0)
        log.info(f"Reprise au checkpoint: index {start_idx}, {total_records} records déjà écrits")

    errors = 0
    collected = 0

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

        data = fetch_combinaisons(date_api, num_r, num_c)
        if data and "combinaisons" in data:
            records = flatten_combinaisons(data, course)
            with open(output_file, "a", encoding="utf-8", newline="\n") as f:
                for r in records:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            total_records += len(records)
            collected += 1
        else:
            errors += 1

        if (i + 1 - start_idx) % 100 == 0:
            log.info(f"  [{i+1}/{len(courses)}] courses={collected} records={total_records} erreurs={errors}")

        if (i + 1 - start_idx) % 500 == 0:
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump({"last_index": i + 1, "total_records": total_records}, f)
            log.info(f">>> Checkpoint: {total_records} records <<<")

        smart_pause(0.25, 0.15)

    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump({"last_index": len(courses), "total_records": total_records}, f)

    log.info("=" * 60)
    log.info(f"TERMINÉ: {collected} courses, {total_records} records, {errors} erreurs")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
