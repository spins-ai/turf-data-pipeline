#!/usr/bin/env python3
"""
Script 39 — Reunions enrichies : meteo, incidents, conditions, duree, commentaires, paris
Source : offline.turfinfo.api.pmu.fr/rest/client/1/programme/{DDMMYYYY}/R{r}
CRITIQUE pour : Weather Impact, Incident Analysis, Race Conditions, Betting Market Structure
"""

import requests
import json
import time
import random
import os
import logging
from datetime import datetime

OUTPUT_DIR = "output/39_reunions_enrichies"
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme"

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


def smart_pause(base=0.4, jitter=0.2):
    time.sleep(base + random.uniform(-jitter, jitter))
    if random.random() < 0.05:
        time.sleep(random.uniform(3, 8))


def load_reunions():
    """Charge les reunions PMU depuis reunions_references_02.json"""
    path = "output/01_calendrier_reunions/reunions_references_02.json"
    if not os.path.exists(path):
        log.error(f"Fichier introuvable : {path}")
        return []

    with open(path) as f:
        data = json.load(f)

    # Filtrer : uniquement les reunions PMU (ont un numero_reunion)
    reunions = [r for r in data if r.get("numero_reunion")]
    reunions.sort(key=lambda r: (r.get("date_reunion_iso", ""), r.get("numero_reunion", 0)))
    log.info(f"Charge {len(reunions)} reunions PMU (sur {len(data)} totales)")
    return reunions


def fetch_reunion(date_str, num_r):
    """Fetche les donnees enrichies d'une reunion via l'endpoint reunion."""
    global req_count
    cache_key = f"{date_str}_R{num_r}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            os.remove(cache_file)

    url = f"{BASE_URL}/{date_str}/R{num_r}"
    try:
        resp = session.get(url, timeout=15)
        req_count += 1
        if req_count >= random.randint(40, 50):
            rotate_session()

        if resp.status_code == 200:
            data = resp.json()
            if data:
                with open(cache_file, "w") as f:
                    json.dump(data, f, ensure_ascii=False)
                return data
        elif resp.status_code == 429:
            log.warning("429 — pause 30s")
            time.sleep(30)
            rotate_session()
        elif resp.status_code == 404:
            log.debug(f"404 pour {cache_key}")
        return None
    except Exception as e:
        log.debug(f"Erreur {cache_key}: {e}")
        return None


def extract_meteo(reunion_data):
    """Extrait la meteo au niveau reunion."""
    meteo = reunion_data.get("meteo") or {}
    return {
        "meteo_nebulosite": meteo.get("nebulosite"),
        "meteo_temperature": meteo.get("temperature"),
        "meteo_force_vent": meteo.get("forceVent"),
        "meteo_direction_vent": meteo.get("directionVent"),
    }


def extract_paris(course_data):
    """Extrait les infos de paris disponibles pour une course."""
    paris_list = course_data.get("paris") or []
    paris_records = []
    for p in paris_list:
        paris_records.append({
            "typePari": p.get("typePari"),
            "codePari": p.get("codePari"),
            "miseBase": p.get("miseBase"),
            "enVente": p.get("enVente"),
            "audience": p.get("audience"),
        })
    return paris_records


def extract_incidents(course_data):
    """Extrait les incidents d'une course (DQ, allure irreguliere, etc.)"""
    incidents_list = course_data.get("incidents") or []
    incidents_records = []
    for inc in incidents_list:
        incidents_records.append({
            "type_incident": inc.get("type"),
            "numero_participants": inc.get("numeroParticipants") or inc.get("numPart") or [],
        })
    return incidents_records


def flatten_reunion(reunion_data, reunion_info):
    """Aplatit les donnees reunion : un record par course avec meteo + infos course."""
    records = []

    date_iso = reunion_info.get("date_reunion_iso", "")
    num_r = reunion_info.get("numero_reunion", "")
    hippo = reunion_info.get("hippodrome_normalise", "")
    discipline = reunion_info.get("discipline_normalisee", "")
    reunion_uid = reunion_info.get("reunion_uid", "")

    # Meteo au niveau reunion
    meteo = extract_meteo(reunion_data)

    # Courses
    courses = reunion_data.get("courses") or []
    for course in courses:
        num_c = course.get("numOrdre") or course.get("numero")
        if not num_c:
            continue

        course_uid = f"{date_iso}_R{num_r}_C{num_c}"

        # Ordre d'arrivee
        ordre_arrivee = course.get("ordreArrivee")
        ordre_arrivee_str = None
        if ordre_arrivee:
            if isinstance(ordre_arrivee, list):
                ordre_arrivee_str = "-".join(str(x) for x in ordre_arrivee)
            else:
                ordre_arrivee_str = str(ordre_arrivee)

        # Incidents
        incidents = extract_incidents(course)
        incidents_types = [inc["type_incident"] for inc in incidents if inc.get("type_incident")]
        incidents_participants = []
        for inc in incidents:
            parts = inc.get("numero_participants", [])
            if isinstance(parts, list):
                incidents_participants.extend(parts)

        # Paris
        paris = extract_paris(course)
        paris_types = [p["typePari"] for p in paris if p.get("typePari")]
        paris_en_vente = [p["typePari"] for p in paris if p.get("enVente")]
        nb_paris_types = len(paris_types)

        # Audiences par pari
        audiences = {}
        for p in paris:
            tp = p.get("typePari")
            aud = p.get("audience")
            if tp and aud:
                audiences[tp] = aud

        record = {
            # Identifiants
            "course_uid": course_uid,
            "reunion_uid": reunion_uid,
            "date_reunion_iso": date_iso,
            "numero_reunion": num_r,
            "numero_course": num_c,
            "hippodrome": hippo,
            "discipline": discipline,
            # Meteo reunion
            **meteo,
            # Course — arrivee
            "ordre_arrivee": ordre_arrivee_str,
            # Course — duree
            "duree_course_ms": course.get("dureeCourse"),
            # Course — conditions
            "conditions": course.get("conditions"),
            # Course — commentaire post-course
            "commentaire_apres_course": course.get("commentaireApresCourse"),
            # Course — tracking
            "course_trackee": course.get("courseTrackee"),
            "photos_arrivee": bool(course.get("photosArrivee")),
            # Incidents
            "nb_incidents": len(incidents),
            "incidents_types": incidents_types if incidents_types else None,
            "incidents_participants": incidents_participants if incidents_participants else None,
            "incidents_detail": incidents if incidents else None,
            # Paris
            "nb_types_paris": nb_paris_types,
            "paris_types": paris_types if paris_types else None,
            "paris_en_vente": paris_en_vente if paris_en_vente else None,
            "paris_audiences": audiences if audiences else None,
            "paris_detail": paris if paris else None,
        }
        records.append(record)

    return records


def main():
    log.info("=" * 60)
    log.info("SCRIPT 39 — Reunions enrichies (meteo, incidents, paris, conditions)")
    log.info("=" * 60)

    rotate_session()
    reunions = load_reunions()
    if not reunions:
        log.error("Aucune reunion trouvee")
        return

    # Checkpoint
    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_39.json")
    start_idx = 0
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            cp = json.load(f)
        start_idx = cp.get("last_index", 0)
        log.info(f"Reprise au checkpoint: index {start_idx}")

    all_records = []
    output_file = os.path.join(OUTPUT_DIR, "reunions_enrichies.json")
    if os.path.exists(output_file) and start_idx > 0:
        with open(output_file) as f:
            all_records = json.load(f)
        log.info(f"Charge {len(all_records)} records existants")

    errors = 0
    collected = 0
    total_courses = 0

    for i in range(start_idx, len(reunions)):
        reunion = reunions[i]
        date_iso = reunion.get("date_reunion_iso", "")
        num_r = reunion.get("numero_reunion")

        if not date_iso or not num_r:
            continue

        try:
            dt = datetime.strptime(date_iso[:10], "%Y-%m-%d")
            date_api = dt.strftime("%d%m%Y")
        except Exception:
            continue

        data = fetch_reunion(date_api, num_r)
        if data:
            records = flatten_reunion(data, reunion)
            all_records.extend(records)
            collected += 1
            total_courses += len(records)
        else:
            errors += 1

        # Progression
        if (i + 1 - start_idx) % 100 == 0:
            log.info(
                f"  [{i+1}/{len(reunions)}] reunions={collected} "
                f"courses={total_courses} records={len(all_records)} erreurs={errors}"
            )

        # Sauvegarde intermediaire
        if (i + 1 - start_idx) % 500 == 0:
            with open(output_file, "w") as f:
                json.dump(all_records, f, ensure_ascii=False)
            with open(checkpoint_file, "w") as f:
                json.dump({
                    "last_index": i + 1,
                    "total_records": len(all_records),
                    "total_reunions": collected,
                }, f)
            log.info(f">>> Sauvegarde: {len(all_records)} records ({collected} reunions) <<<")

        smart_pause(0.4, 0.2)

    # Sauvegarde finale
    with open(output_file, "w") as f:
        json.dump(all_records, f, ensure_ascii=False)

    # Nettoyage checkpoint
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

    # Stats
    log.info("=" * 60)
    log.info(f"TERMINE: {collected} reunions, {total_courses} courses, {len(all_records)} records, {errors} erreurs")
    log.info("=" * 60)

    # Stats meteo
    with_meteo = sum(1 for r in all_records if r.get("meteo_temperature") is not None)
    with_incidents = sum(1 for r in all_records if r.get("nb_incidents", 0) > 0)
    with_conditions = sum(1 for r in all_records if r.get("conditions"))
    with_duree = sum(1 for r in all_records if r.get("duree_course_ms"))
    with_commentaire = sum(1 for r in all_records if r.get("commentaire_apres_course"))
    with_paris = sum(1 for r in all_records if r.get("nb_types_paris", 0) > 0)

    log.info(f"  Avec meteo       : {with_meteo}/{len(all_records)}")
    log.info(f"  Avec incidents   : {with_incidents}/{len(all_records)}")
    log.info(f"  Avec conditions  : {with_conditions}/{len(all_records)}")
    log.info(f"  Avec duree       : {with_duree}/{len(all_records)}")
    log.info(f"  Avec commentaire : {with_commentaire}/{len(all_records)}")
    log.info(f"  Avec paris       : {with_paris}/{len(all_records)}")


if __name__ == "__main__":
    main()
