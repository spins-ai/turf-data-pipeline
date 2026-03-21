#!/usr/bin/env python3
"""
Script 101 — PMU Official API Scraper
Source : online.turfinfo.api.pmu.fr/rest/client/1
Collecte : programmes, participants, courses, cotes, arrivées
API JSON publique, pas de clé nécessaire.
FORMAT DATE: DDMMYYYY pour l'API
C'est LA source #1 pour le ML hippique français.
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta

import requests

SCRIPT_NAME = "101_pmu_api"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("101_pmu_api")

BASE_API = "https://online.turfinfo.api.pmu.fr/rest/client/1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer": "https://www.pmu.fr/turf/",
}


def api_get(session, endpoint, max_retries=3, timeout=20):
    """GET sur l'API PMU avec retry."""
    url = BASE_API + endpoint
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 30 * attempt
                log.warning(f"  429 Rate limit, pause {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 420:
                # Endpoint non disponible pour cette course
                return None
            if resp.status_code == 400:
                return None
            log.warning(f"  HTTP {resp.status_code} sur {endpoint} (essai {attempt})")
            time.sleep(3 * attempt)
        except requests.RequestException as e:
            log.warning(f"  Erreur réseau: {e} (essai {attempt})")
            time.sleep(3 * attempt)
    return None



def date_to_pmu(dt):
    """Convertir datetime en format PMU: DDMMYYYY."""
    return dt.strftime("%d%m%Y")


def scrape_day(session, dt, output_programmes, output_participants, output_courses):
    """Scraper toutes les données PMU pour un jour donné."""
    date_pmu = date_to_pmu(dt)
    date_iso = dt.strftime("%Y-%m-%d")

    # Cache pour le programme du jour
    cache_prog = os.path.join(CACHE_DIR, f"prog_{date_iso}.json")
    if os.path.exists(cache_prog) and os.path.getsize(cache_prog) > 2:
        try:
            with open(cache_prog, "r", encoding="utf-8") as f:
                prog_data = json.load(f)
        except (json.JSONDecodeError, ValueError):
            log.warning(f"  Cache corrompu: {cache_prog}, re-téléchargement")
            os.remove(cache_prog)
            prog_data = None
    else:
        prog_data = None
    if prog_data is None:
        prog_data = api_get(session, f"/programmes/{date_pmu}")
        if not prog_data:
            return 0, 0, 0
        with open(cache_prog, "w", encoding="utf-8") as f:
            json.dump(prog_data, f, ensure_ascii=False)

    programme = prog_data.get("programme", {})
    reunions = programme.get("reunions", [])

    if not reunions:
        return 0, 0, 0

    total_courses = 0
    total_participants = 0
    total_records = 0

    for reunion in reunions:
        num_reunion = reunion.get("numOfficiel", reunion.get("numExterneReunion", 0))
        hippo = reunion.get("hippodrome", {})
        hippo_nom = hippo.get("libelleLong", hippo.get("libelleCourt", ""))
        courses = reunion.get("courses", [])

        # Sauvegarder les infos de réunion
        reu_record = {
            "date": date_iso,
            "type": "reunion",
            "num_reunion": num_reunion,
            "hippodrome": hippo_nom,
            "hippodrome_code": hippo.get("codeHippodrome", ""),
            "nb_courses": len(courses),
            "pays": reunion.get("pays", {}).get("libelle", ""),
            "specialite": reunion.get("specialite", ""),
            "audience": reunion.get("audience", ""),
            "scraped_at": datetime.now().isoformat(),
        }
        append_jsonl(output_programmes, reu_record)
        total_records += 1

        for course_summary in courses:
            num_course = course_summary.get("numOrdre", course_summary.get("numExterne", 0))

            # --- Détails de la course ---
            cache_course = os.path.join(CACHE_DIR, f"course_{date_iso}_R{num_reunion}C{num_course}.json")
            course_data = None
            if os.path.exists(cache_course) and os.path.getsize(cache_course) > 2:
                try:
                    with open(cache_course, "r", encoding="utf-8") as f:
                        course_data = json.load(f)
                except (json.JSONDecodeError, ValueError, OSError):
                    log.warning("  Cache corrompu: %s, re-téléchargement", cache_course)
                    os.remove(cache_course)
            if course_data is None:
                course_data = api_get(session, f"/programmes/{date_pmu}/R{num_reunion}/C{num_course}")
                if course_data:
                    with open(cache_course, "w", encoding="utf-8") as f:
                        json.dump(course_data, f, ensure_ascii=False)
                    time.sleep(random.uniform(0.3, 0.8))

            if course_data:
                # Extraire les infos de course
                course_record = {
                    "date": date_iso,
                    "source": "pmu_api",
                    "type": "course",
                    "num_reunion": num_reunion,
                    "num_course": num_course,
                    "hippodrome": hippo_nom,
                    "libelle": course_data.get("libelle", ""),
                    "distance": course_data.get("distance", 0),
                    "discipline": course_data.get("discipline", ""),
                    "specialite": course_data.get("specialite", ""),
                    "corde": course_data.get("corde", ""),
                    "montantPrix": course_data.get("montantPrix", 0),
                    "conditions": course_data.get("conditions", ""),
                    "conditionSexe": course_data.get("conditionSexe", ""),
                    "nbPartants": course_data.get("nombreDeclaresPartants", 0),
                    "statut": course_data.get("statut", ""),
                    "parcours": course_data.get("parcours", ""),
                    "dureeCourse": course_data.get("dureeCourse", None),
                    "ordreArrivee": course_data.get("ordreArrivee", []),
                    "scraped_at": datetime.now().isoformat(),
                }
                # Commentaire après course si disponible
                cmt = course_data.get("commentaireApresCourse")
                if isinstance(cmt, dict):
                    course_record["commentaire"] = cmt.get("texte", "")
                elif isinstance(cmt, str):
                    course_record["commentaire"] = cmt

                append_jsonl(output_courses, course_record)
                total_courses += 1
                total_records += 1

            # --- Participants ---
            cache_parts = os.path.join(CACHE_DIR, f"parts_{date_iso}_R{num_reunion}C{num_course}.json")
            parts_data = None
            if os.path.exists(cache_parts) and os.path.getsize(cache_parts) > 2:
                try:
                    with open(cache_parts, "r", encoding="utf-8") as f:
                        parts_data = json.load(f)
                except (json.JSONDecodeError, ValueError, OSError):
                    log.warning("  Cache corrompu: %s, re-téléchargement", cache_parts)
                    os.remove(cache_parts)
            if parts_data is None:
                parts_data = api_get(session, f"/programmes/{date_pmu}/R{num_reunion}/C{num_course}/participants")
                if parts_data:
                    with open(cache_parts, "w", encoding="utf-8") as f:
                        json.dump(parts_data, f, ensure_ascii=False)
                    time.sleep(random.uniform(0.3, 0.8))

            if parts_data:
                participants = parts_data.get("participants", [])
                for p in participants:
                    gains = p.get("gainsParticipant", {})
                    cote_direct = p.get("dernierRapportDirect", {})
                    cote_ref = p.get("dernierRapportReference", {})

                    part_record = {
                        "date": date_iso,
                        "source": "pmu_api",
                        "type": "participant",
                        "num_reunion": num_reunion,
                        "num_course": num_course,
                        "hippodrome": hippo_nom,
                        "nom": p.get("nom", ""),
                        "numPmu": p.get("numPmu", 0),
                        "age": p.get("age", 0),
                        "sexe": p.get("sexe", ""),
                        "race": p.get("race", ""),
                        "statut": p.get("statut", ""),
                        "driver": p.get("driver", ""),
                        "entraineur": p.get("entraineur", ""),
                        "proprietaire": p.get("proprietaire", ""),
                        "eleveur": p.get("eleveur", ""),
                        "musique": p.get("musique", ""),
                        "oeilleres": p.get("oeilleres", ""),
                        "deferre": p.get("deferre", ""),
                        "nomPere": p.get("nomPere", ""),
                        "nomMere": p.get("nomMere", ""),
                        "placeCorde": p.get("placeCorde", None),
                        "handicapDistance": p.get("handicapDistance", None),
                        "poidsConditionMonte": p.get("poidsConditionMonte", None),
                        "nombreCourses": p.get("nombreCourses", 0),
                        "nombreVictoires": p.get("nombreVictoires", 0),
                        "nombrePlaces": p.get("nombrePlaces", 0),
                        "gainsCarriere": gains.get("gainsCarriere", 0),
                        "gainsAnnee": gains.get("gainsAnneeEnCours", 0),
                        "cote_direct": cote_direct.get("rapport", None),
                        "cote_reference": cote_ref.get("rapport", None),
                        "cote_tendance": cote_ref.get("indicateurTendance", ""),
                        "ordreArrivee": p.get("ordreArrivee", None),
                        "tempsObtenu": p.get("tempsObtenu", None),
                        "reductionKm": p.get("reductionKilometrique", None),
                        "allure": p.get("allure", ""),
                        "avisEntraineur": p.get("avisEntraineur", ""),
                        "scraped_at": datetime.now().isoformat(),
                    }
                    # Commentaire après course par partant
                    cmt_p = p.get("commentaireApresCourse")
                    if isinstance(cmt_p, dict):
                        part_record["commentaire"] = cmt_p.get("texte", "")

                    append_jsonl(output_participants, part_record)
                    total_participants += 1
                    total_records += 1

        # Pause entre réunions
        time.sleep(random.uniform(0.5, 1.5))

    return total_courses, total_participants, total_records


def main():
    parser = argparse.ArgumentParser(description="Script 101 — PMU API Scraper (programmes, participants, courses)")
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), défaut=hier")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Nombre max de jours (0=illimité)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else (datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 101 — PMU API Scraper")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info(f"  API : {BASE_API}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = requests.Session()
    session.headers.update(HEADERS)

    output_programmes = os.path.join(OUTPUT_DIR, "pmu_programmes.jsonl")
    output_participants = os.path.join(OUTPUT_DIR, "pmu_participants.jsonl")
    output_courses = os.path.join(OUTPUT_DIR, "pmu_courses.jsonl")

    current = start_date
    day_count = 0
    grand_total_courses = 0
    grand_total_participants = 0
    grand_total_records = 0

    while current <= end_date:
        if args.max_days and day_count >= args.max_days:
            log.info(f"  Max {args.max_days} jours atteint.")
            break

        date_str = current.strftime("%Y-%m-%d")
        courses, participants, records = scrape_day(
            session, current, output_programmes, output_participants, output_courses
        )

        grand_total_courses += courses
        grand_total_participants += participants
        grand_total_records += records
        day_count += 1

        if courses > 0:
            log.info(f"  {date_str}: {courses} courses, {participants} partants")
        else:
            log.debug(f"  {date_str}: aucune donnée")

        if day_count % 10 == 0:
            log.info(f"  === Jour {day_count}: total {grand_total_courses} courses, "
                     f"{grand_total_participants} partants, {grand_total_records} records ===")
            save_checkpoint(CHECKPOINT_FILE, {
                "last_date": date_str,
                "total_courses": grand_total_courses,
                "total_participants": grand_total_participants,
                "total_records": grand_total_records,
            })

        # Rotation session tous les 100 jours
        if day_count % 100 == 0:
            session.close()
            session = requests.Session()
            session.headers.update(HEADERS)
            time.sleep(random.uniform(3, 8))

        current += timedelta(days=1)
        # Pause très légère entre jours (API pas agressive)
        time.sleep(random.uniform(0.5, 1.5))

    save_checkpoint(CHECKPOINT_FILE, {
        "last_date": (current - timedelta(days=1)).strftime("%Y-%m-%d"),
        "total_courses": grand_total_courses,
        "total_participants": grand_total_participants,
        "total_records": grand_total_records,
        "status": "done",
    })

    log.info("=" * 60)
    log.info(f"TERMINÉ: {day_count} jours")
    log.info(f"  Courses:      {grand_total_courses}")
    log.info(f"  Participants:  {grand_total_participants}")
    log.info(f"  Records total: {grand_total_records}")
    log.info(f"  Fichiers: {output_programmes}")
    log.info(f"           {output_participants}")
    log.info(f"           {output_courses}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
