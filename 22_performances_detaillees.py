#!/usr/bin/env python3
"""
Script 22 — Performances détaillées PMU (9 dernières courses par cheval)
Source : offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}/R{r}/C{c}/performances-detaillees/pretty
CRITIQUE pour : LSTM, GRU, TFT, Rolling Stats, Pace Profile, Race Simulation
"""

import requests
import json
import time
import random
import os
import logging
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "22_performances_detaillees")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://offline.turfinfo.api.pmu.fr/rest/client/7/programme"

def smart_pause(base=0.3, jitter=0.2):
    time.sleep(base + random.uniform(-jitter, jitter))

def load_courses_references():
    """Charger les courses depuis scripts 02 et 02b (mode léger)"""
    courses = []
    seen = set()
    KEEP = {"course_uid", "date_reunion_iso", "numero_reunion", "numero_course", "hippodrome_normalise"}
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
                    courses.append({k: c[k] for k in KEEP if k in c})
            del data
    log.info(f"Chargé {len(courses)} courses uniques (mode léger)")
    return courses

def fetch_performances(date_str, numero_reunion, num_course):
    """Récupérer les performances détaillées d'une course"""
    cache_key = f"{date_str}_R{numero_reunion}_C{num_course}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
                return json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            os.remove(cache_file)  # Cache corrompu, re-télécharger

    url = f"{BASE_URL}/{date_str}/R{numero_reunion}/C{num_course}/performances-detaillees/pretty"
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        if resp.status_code == 200:
            data = resp.json()
            if data and "participants" in data:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
            return data
        elif resp.status_code == 429:
            time.sleep(30)
            return None
        else:
            return None
    except Exception:
        return None

def flatten_performances(perf_data, course_info):
    """Aplatir les performances en records par partant"""
    records = []
    allure = perf_data.get("allure", "")

    for participant in perf_data.get("participants", []):
        num_pmu = participant.get("numPmu")
        nom_cheval = participant.get("nomCheval", "")
        courses_courues = participant.get("coursesCourues", [])

        record = {
            "course_uid": course_info.get("course_uid", ""),
            "date_reunion_iso": course_info.get("date_reunion_iso", ""),
            "hippodrome": course_info.get("hippodrome_normalise", ""),
            "num_pmu": num_pmu,
            "nom_cheval": nom_cheval,
            "allure": allure,
            "nb_courses_passees": len(courses_courues),
        }

        # Extraire les N dernières courses
        for j, course in enumerate(courses_courues[:9]):
            prefix = f"perf_{j+1}_"
            record[f"{prefix}date"] = course.get("date")
            record[f"{prefix}hippodrome"] = course.get("hippodrome", "")
            record[f"{prefix}discipline"] = course.get("discipline", "")
            record[f"{prefix}distance"] = course.get("distance")
            record[f"{prefix}nb_partants"] = course.get("nbParticipants")
            record[f"{prefix}allocation"] = course.get("allocation")
            record[f"{prefix}temps_premier"] = course.get("tempsDuPremier")
            record[f"{prefix}terrain"] = course.get("etatTerrain", "")

            # Trouver CE cheval dans les participants de la course passée
            for p in course.get("participants", []):
                if p is None:
                    continue
                if p.get("itsHim"):
                    place_info = p.get("place", {})
                    record[f"{prefix}position"] = place_info.get("place") if place_info else None
                    record[f"{prefix}jockey"] = p.get("nomJockey", "")
                    record[f"{prefix}poids"] = p.get("poidsJockey")
                    record[f"{prefix}corde"] = p.get("corde")
                    record[f"{prefix}dist_precedent"] = p.get("distanceAvecPrecedent", "")
                    record[f"{prefix}reduction_km"] = p.get("reductionKilometrique")
                    record[f"{prefix}distance_parcourue"] = p.get("distanceParcourue")
                    record[f"{prefix}oeillere"] = p.get("oeillere", "")
                    break

        records.append(record)

    return records

def main():
    log.info("=" * 60)
    log.info("SCRIPT 22 — Performances détaillées PMU")
    log.info("=" * 60)

    courses = load_courses_references()
    if not courses:
        log.error("Aucune course trouvée")
        return

    courses.sort(key=lambda c: c.get("date_reunion_iso", ""))

    # Filtrer : endpoint disponible seulement à partir de ~2020
    courses = [c for c in courses if c.get("date_reunion_iso", "") >= "2020-01-01"]
    log.info(f"Filtré à {len(courses)} courses (2020+)")

    # Checkpoint
    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_22.json")
    output_file = os.path.join(OUTPUT_DIR, "performances_detaillees.jsonl")
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

        perf_data = fetch_performances(date_api, num_r, num_c)
        req_count += 1

        if perf_data and "participants" in perf_data:
            records = flatten_performances(perf_data, course)
            with open(output_file, "a", encoding="utf-8", newline="\n") as f:
                for r in records:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            total_records += len(records)
            collected += 1
        else:
            errors += 1

        if (i + 1 - start_idx) % 100 == 0:
            log.info(f"  [{i+1}/{len(courses)}] courses={collected} partants={total_records} erreurs={errors}")

        if (i + 1 - start_idx) % 300 == 0:
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump({"last_index": i + 1, "total_records": total_records}, f)
            log.info(f">>> Checkpoint: {total_records} records <<<")

        smart_pause(0.25, 0.15)

    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump({"last_index": len(courses), "total_records": total_records}, f)

    log.info("=" * 60)
    log.info(f"TERMINÉ: {collected} courses, {total_records} partants, {errors} erreurs")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
