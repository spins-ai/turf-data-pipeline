#!/usr/bin/env python3
"""
Script 27 — Citations & Enjeux PMU (distribution des paris par cheval)
Source : offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}/R{r}/C{c}/citations
CRITIQUE pour : Outsider Detection, Value Hunting, Crowd Wisdom, Betting Strategy
"""

import requests
import json
import time
import random
import os
import logging
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "27_citations_enjeux")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://offline.turfinfo.api.pmu.fr/rest/client/7/programme"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
]

session = requests.Session()
req_count = 0

def rotate_session():
    global session, req_count
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "DNT": "1",
    })
    req_count = 0

def smart_pause(base=0.3, jitter=0.15):
    time.sleep(base + random.uniform(-jitter, jitter))
    if random.random() < 0.05:
        time.sleep(random.uniform(3, 8))

def load_courses():
    """Charge seulement les champs nécessaires pour économiser la RAM."""
    courses = []
    seen = set()
    KEEP_KEYS = {"course_uid", "date_reunion_iso", "numero_reunion", "numero_course", "hippodrome_normalise"}
    for path in [
        os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_normalisees.json"),
        os.path.join(BASE_DIR, "output", "02b_liste_courses_2013", "courses_normalisees.json"),
    ]:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            for c in data:
                uid = c.get("course_uid", "")
                if uid and uid not in seen:
                    seen.add(uid)
                    courses.append({k: c[k] for k in KEEP_KEYS if k in c})
            del data  # Libère la RAM immédiatement
    courses.sort(key=lambda c: c.get("date_reunion_iso", ""))
    log.info(f"Chargé {len(courses)} courses uniques (mode léger)")
    return courses

def fetch_citations(date_str, num_r, num_c):
    global req_count
    cache_key = f"{date_str}_R{num_r}_C{num_c}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            os.remove(cache_file)

    url = f"{BASE_URL}/{date_str}/R{num_r}/C{num_c}/citations"
    try:
        resp = session.get(url, timeout=15)
        req_count += 1
        if req_count >= random.randint(40, 50):
            rotate_session()

        if resp.status_code == 200:
            data = resp.json()
            if data and "listeCitations" in data:
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

def flatten_citations(data, course_info):
    """
    The API returns one entry per bet type in listeCitations.
    Each entry has: typePari, dateProgramme, timezoneOffset,
    numeroReunion, numeroCourse, indisponible,
    numPositions, numPositionsConsolidees, numComplements.
    Some entries may also carry nested 'participants' with per-horse data.
    We extract both levels: one record per bet type (always), plus
    one record per participant/citation when available.
    """
    records = []
    for citation_block in data.get("listeCitations", []):
        type_pari = citation_block.get("typePari", "")
        base = {
            "course_uid": course_info.get("course_uid", ""),
            "date_reunion_iso": course_info.get("date_reunion_iso", ""),
            "hippodrome": course_info.get("hippodrome_normalise", ""),
            "type_pari": type_pari,
            "indisponible": citation_block.get("indisponible"),
            "num_positions": citation_block.get("numPositions"),
            "num_positions_consolidees": citation_block.get("numPositionsConsolidees"),
            "num_complements": citation_block.get("numComplements"),
        }
        participants = citation_block.get("participants", [])
        if participants:
            # Expand per-horse detail when the API provides it
            for p in participants:
                p_base = {
                    **base,
                    "num_pmu": p.get("numPmu"),
                    "nom_cheval": p.get("nom", ""),
                    "statut": p.get("statut", ""),
                    "favoris": p.get("favoris", False),
                }
                citations_list = p.get("citations", [])
                if citations_list:
                    for cit in citations_list:
                        records.append({
                            **p_base,
                            "citation_position": cit.get("position"),
                            "citation_enjeu": cit.get("enjeu"),
                            "citation_ratio": cit.get("ratio"),
                        })
                else:
                    records.append(p_base)
        else:
            # No participant detail — keep the bet-type-level record
            records.append(base)
    return records

def main():
    log.info("=" * 60)
    log.info("SCRIPT 27 — Citations & Enjeux PMU")
    log.info("=" * 60)

    rotate_session()
    courses = load_courses()
    if not courses:
        log.error("Aucune course trouvée")
        return

    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_27.json")
    output_file = os.path.join(OUTPUT_DIR, "citations_enjeux.jsonl")
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

        data = fetch_citations(date_api, num_r, num_c)
        if data and "listeCitations" in data:
            records = flatten_citations(data, course)
            with open(output_file, "a", encoding="utf-8") as f:
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
