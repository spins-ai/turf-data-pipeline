#!/usr/bin/env python3
"""
Re-export output/39_reunions_enrichies/cache/ with CORRECT meteo extraction.

The existing reunions_enrichies.jsonl has meteo fields but they are ALL NULL.
The cache files (18,921) contain rich meteo data in ~94% of files:
  - temperature, forceVent, directionVent
  - nebulositeCode, nebulositeLibelleCourt, nebulositeLibelleLong

Also extracts reunion-level fields missing from the JSONL:
  - nature (DIURNE/NOCTURNE)
  - audience (NATIONAL/REGIONAL/INTERNATIONAL)
  - pays (country code)
  - specialites, disciplinesMere
  - offresInternet
  - reportPlusFpaMax
  - parisEvenement (Quinte+ indicator)

Outputs:
  output/39_reunions_enrichies/reunions_meteo_complete.jsonl   (reunion+meteo)
  output/39_reunions_enrichies/courses_enrichies_complete.jsonl (course-level with incidents)
"""
import json
import os
import sys
from datetime import datetime, timezone

BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "output", "39_reunions_enrichies")
CACHE_DIR = os.path.join(BASE, "cache")

OUT_REUNIONS = os.path.join(BASE, "reunions_meteo_complete.jsonl")
OUT_COURSES = os.path.join(BASE, "courses_enrichies_complete.jsonl")


def ts_to_iso_date(ts_ms):
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return None


def ts_to_datetime(ts_ms):
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError):
        return None


def process_file(filepath):
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return None, []

    if not isinstance(data, dict):
        return None, []

    date_str = ts_to_iso_date(data.get("dateReunion"))
    num_reunion = data.get("numOfficiel")

    hippo = data.get("hippodrome", {})
    if isinstance(hippo, dict):
        hippo_name = hippo.get("libelleCourt") or hippo.get("libelleLong")
        hippo_code = hippo.get("code")
    else:
        hippo_name = str(hippo) if hippo else None
        hippo_code = None

    pays = data.get("pays", {})
    pays_code = pays.get("code") if isinstance(pays, dict) else None

    reunion_uid = f"{date_str}_R{num_reunion}"

    # --- Meteo ---
    meteo = data.get("meteo") or {}
    if not isinstance(meteo, dict):
        meteo = {}

    # --- Quinte indicator ---
    paris_evt = data.get("parisEvenement", []) or []
    has_quinte = any(
        p.get("codePari") == "QUINTE_PLUS"
        for p in paris_evt if isinstance(p, dict)
    )

    reunion_row = {
        "reunion_uid": reunion_uid,
        "date_reunion_iso": date_str,
        "numero_reunion": num_reunion,
        "hippodrome": hippo_name,
        "hippodrome_code": hippo_code,
        "pays": pays_code,
        "nature": data.get("nature"),
        "audience": data.get("audience"),
        "statut": data.get("statut"),
        "disciplines_mere": data.get("disciplinesMere"),
        "specialites": data.get("specialites"),
        "offres_internet": data.get("offresInternet"),
        "report_plus_fpa_max": data.get("reportPlusFpaMax"),
        "has_quinte": has_quinte,
        # Meteo fields
        "meteo_temperature": meteo.get("temperature"),
        "meteo_force_vent": meteo.get("forceVent"),
        "meteo_direction_vent": meteo.get("directionVent"),
        "meteo_nebulosite_code": meteo.get("nebulositeCode"),
        "meteo_nebulosite_court": meteo.get("nebulositeLibelleCourt"),
        "meteo_nebulosite_long": meteo.get("nebulositeLibelleLong"),
        "meteo_date_prevision": ts_to_datetime(meteo.get("datePrevision")),
    }

    # --- Courses ---
    course_rows = []
    for c in (data.get("courses") or []):
        if not isinstance(c, dict):
            continue
        num_course = c.get("numOrdre") or c.get("numExterne")
        course_uid = f"{date_str}_R{num_reunion}C{num_course}"

        incidents = c.get("incidents") or []
        inc_types = [inc.get("type") for inc in incidents if isinstance(inc, dict)]
        inc_participants = []
        for inc in incidents:
            if isinstance(inc, dict):
                inc_participants.extend(inc.get("numeroParticipants", []))

        course_rows.append({
            "course_uid": course_uid,
            "reunion_uid": reunion_uid,
            "date_reunion_iso": date_str,
            "numero_reunion": num_reunion,
            "numero_course": num_course,
            "hippodrome": hippo_name,
            "discipline": c.get("discipline"),
            "specialite": c.get("specialite"),
            "distance": c.get("distance"),
            "corde": c.get("corde"),
            "condition_sexe": c.get("conditionSexe"),
            "categorie_particularite": c.get("categorieParticularite"),
            "montant_prix": c.get("montantPrix"),
            "montant_total_offert": c.get("montantTotalOffert"),
            "duree_course_ms": c.get("dureeCourse"),
            "heure_depart": ts_to_datetime(c.get("heureDepart")),
            "conditions": c.get("conditions"),
            "nb_partants": c.get("nombreDeclaresPartants"),
            "ordre_arrivee": c.get("ordreArrivee"),
            "nb_incidents": len(incidents),
            "incidents_types": inc_types if inc_types else None,
            "incidents_participants": inc_participants if inc_participants else None,
            "commentaire": c.get("commentaireApresCourse"),
            "nb_types_paris": len(c.get("paris") or []),
            "replay_disponible": c.get("replayDisponible"),
            # Meteo inherited from reunion
            "meteo_temperature": meteo.get("temperature"),
            "meteo_force_vent": meteo.get("forceVent"),
            "meteo_direction_vent": meteo.get("directionVent"),
            "meteo_nebulosite_code": meteo.get("nebulositeCode"),
        })

    return reunion_row, course_rows


def main():
    if not os.path.isdir(CACHE_DIR):
        print(f"ERROR: Cache dir not found: {CACHE_DIR}")
        sys.exit(1)

    files = sorted([f for f in os.listdir(CACHE_DIR) if f.endswith(".json")])
    print(f"Processing {len(files)} cache files from {CACHE_DIR}")

    n_reunions = 0
    n_courses = 0
    n_meteo = 0

    with open(OUT_REUNIONS, "w", encoding="utf-8") as fr, \
         open(OUT_COURSES, "w", encoding="utf-8") as fc:

        for i, fn in enumerate(files):
            if i % 5000 == 0 and i > 0:
                print(f"  ...{i}/{len(files)} files processed")

            filepath = os.path.join(CACHE_DIR, fn)
            reunion_row, course_rows = process_file(filepath)

            if reunion_row:
                fr.write(json.dumps(reunion_row, ensure_ascii=False) + "\n")
                n_reunions += 1
                if reunion_row.get("meteo_temperature") is not None:
                    n_meteo += 1

            for cr in course_rows:
                fc.write(json.dumps(cr, ensure_ascii=False) + "\n")
                n_courses += 1

    print(f"\nDone!")
    print(f"  {OUT_REUNIONS}: {n_reunions} reunions ({n_meteo} with meteo = {n_meteo/max(n_reunions,1)*100:.1f}%)")
    print(f"  {OUT_COURSES}: {n_courses} courses")


if __name__ == "__main__":
    main()
