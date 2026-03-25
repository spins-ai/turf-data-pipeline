#!/usr/bin/env python3
"""
Export output/02b_liste_courses_2013/cache/ which has NO JSONL output at all.

7,291 cache files covering 2013-2019 historical reunion+participant data.
Same PMU API format as 02_liste_courses but for an earlier period.

Outputs:
  output/02b_liste_courses_2013/partants_2013.jsonl   (participant-level)
  output/02b_liste_courses_2013/courses_2013.jsonl    (course-level)
  output/02b_liste_courses_2013/reunions_2013.jsonl   (reunion-level)
"""
import json
import os
import sys
from datetime import datetime, timezone

BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "output", "02b_liste_courses_2013")
CACHE_DIR = os.path.join(BASE, "cache")

OUT_PARTANTS = os.path.join(BASE, "partants_2013.jsonl")
OUT_COURSES = os.path.join(BASE, "courses_2013.jsonl")
OUT_REUNIONS = os.path.join(BASE, "reunions_2013.jsonl")


def ts_to_iso_date(ts_ms):
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return None


def process_file(filepath, fn):
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return None, [], []

    if not isinstance(data, dict):
        return None, [], []

    rd = data.get("reunion_data", {})
    if not isinstance(rd, dict):
        return None, [], []

    date_str = ts_to_iso_date(rd.get("dateReunion"))
    if not date_str:
        # Try from filename: 2013-02-19_R1.json
        parts = fn.replace(".json", "").split("_")
        if parts:
            date_str = parts[0]

    num_reunion = rd.get("numOfficiel")
    hippo = rd.get("hippodrome", {})
    hippo_name = hippo.get("libelleCourt") if isinstance(hippo, dict) else str(hippo)
    hippo_code = hippo.get("code") if isinstance(hippo, dict) else None
    pays = rd.get("pays", {})
    pays_code = pays.get("code") if isinstance(pays, dict) else None

    reunion_uid = f"{date_str}_R{num_reunion}"

    # --- Reunion row ---
    reunion_row = {
        "reunion_uid": reunion_uid,
        "date_reunion_iso": date_str,
        "numero_reunion": num_reunion,
        "hippodrome": hippo_name,
        "hippodrome_code": hippo_code,
        "pays": pays_code,
        "nature": rd.get("nature"),
        "audience": rd.get("audience"),
        "statut": rd.get("statut"),
        "disciplines_mere": rd.get("disciplinesMere"),
        "specialites": rd.get("specialites"),
    }

    # --- Courses from reunion_data.courses ---
    course_rows = []
    for c in (rd.get("courses") or []):
        if not isinstance(c, dict):
            continue
        num_course = c.get("numOrdre") or c.get("numExterne")
        course_uid = f"{date_str}_R{num_reunion}C{num_course}"
        course_rows.append({
            "course_uid": course_uid,
            "reunion_uid": reunion_uid,
            "date_reunion_iso": date_str,
            "numero_reunion": num_reunion,
            "numero_course": num_course,
            "hippodrome": hippo_name,
            "libelle": c.get("libelle"),
            "distance": c.get("distance"),
            "discipline": c.get("discipline"),
            "specialite": c.get("specialite"),
            "corde": c.get("corde"),
            "condition_sexe": c.get("conditionSexe"),
            "montant_prix": c.get("montantPrix"),
            "nb_partants": c.get("nombreDeclaresPartants"),
            "duree_course_ms": c.get("dureeCourse"),
            "ordre_arrivee": c.get("ordreArrivee"),
            "statut": c.get("statut"),
        })

    # --- Participants ---
    partant_rows = []
    participants = data.get("participants", {})
    for cnum_str, val in participants.items():
        # val can be a list of dicts or a dict with 'participants' key
        plist = []
        if isinstance(val, list):
            plist = val
        elif isinstance(val, dict):
            plist = val.get("participants", [])

        for p in plist:
            if not isinstance(p, dict):
                continue

            course_uid = f"{date_str}_R{num_reunion}C{cnum_str}"

            gains = p.get("gainsParticipant", {}) or {}

            partant_rows.append({
                "course_uid": course_uid,
                "reunion_uid": reunion_uid,
                "date_reunion_iso": date_str,
                "numero_reunion": num_reunion,
                "numero_course": int(cnum_str) if cnum_str.isdigit() else cnum_str,
                "hippodrome": hippo_name,
                "nom": p.get("nom"),
                "num_pmu": p.get("numPmu"),
                "age": p.get("age"),
                "sexe": p.get("sexe"),
                "race": p.get("race"),
                "statut": p.get("statut"),
                "musique": p.get("musique"),
                "nombre_courses": p.get("nombreCourses"),
                "nombre_victoires": p.get("nombreVictoires"),
                "nombre_places": p.get("nombrePlaces"),
                "nombre_places_second": p.get("nombrePlacesSecond"),
                "nombre_places_troisieme": p.get("nombrePlacesTroisieme"),
                "gains_carriere": gains.get("gainsCarriere"),
                "gains_victoires": gains.get("gainsVictoires"),
                "gains_place": gains.get("gainsPlace"),
                "gains_annee_en_cours": gains.get("gainsAnneeEnCours"),
                "gains_annee_precedente": gains.get("gainsAnneePrecedente"),
                "indicateur_inedit": p.get("indicateurInedit"),
                "driver": p.get("driver"),
                "driver_change": p.get("driverChange"),
                "entraineur": p.get("entraineur"),
                "proprietaire": p.get("proprietaire"),
                "eleveur": p.get("eleveur"),
                "oeilleres": p.get("oeilleres"),
                "deferre": p.get("deferre"),
                "place_corde": p.get("placeCorde"),
                "handicap_distance": p.get("handicapDistance"),
                "poids_condition_monte": p.get("poidsConditionMonte"),
                "nom_pere": p.get("nomPere"),
                "nom_mere": p.get("nomMere"),
                "ordre_arrivee": p.get("ordreArrivee"),
                "temps_obtenu": p.get("tempsObtenu"),
                "reduction_km": p.get("reductionKilometrique"),
                "allure": p.get("allure"),
                "avis_entraineur": p.get("avisEntraineur"),
            })

    return reunion_row, course_rows, partant_rows


def main():
    if not os.path.isdir(CACHE_DIR):
        print(f"ERROR: Cache dir not found: {CACHE_DIR}")
        sys.exit(1)

    files = sorted([f for f in os.listdir(CACHE_DIR) if f.endswith(".json")])
    print(f"Processing {len(files)} cache files from {CACHE_DIR}")

    n_reunions = 0
    n_courses = 0
    n_partants = 0

    with open(OUT_REUNIONS, "w", encoding="utf-8") as fr, \
         open(OUT_COURSES, "w", encoding="utf-8") as fc, \
         open(OUT_PARTANTS, "w", encoding="utf-8") as fp:

        for i, fn in enumerate(files):
            if i % 2000 == 0 and i > 0:
                print(f"  ...{i}/{len(files)} files processed")

            filepath = os.path.join(CACHE_DIR, fn)
            reunion_row, course_rows, partant_rows = process_file(filepath, fn)

            if reunion_row:
                fr.write(json.dumps(reunion_row, ensure_ascii=False) + "\n")
                n_reunions += 1

            for cr in course_rows:
                fc.write(json.dumps(cr, ensure_ascii=False) + "\n")
                n_courses += 1

            for pr in partant_rows:
                fp.write(json.dumps(pr, ensure_ascii=False) + "\n")
                n_partants += 1

    print(f"\nDone!")
    print(f"  {OUT_REUNIONS}: {n_reunions} reunions")
    print(f"  {OUT_COURSES}: {n_courses} courses")
    print(f"  {OUT_PARTANTS}: {n_partants} participants")


if __name__ == "__main__":
    main()
