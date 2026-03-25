#!/usr/bin/env python3
"""
Export unexploited fields from output/101_pmu_api/cache/ into JSONL.

Cache has 225K+ files with rich race-level data. The existing pmu_courses.jsonl
exports ~20 fields, but cache has ~50+ including:
  - incidents (disqualifications, allure irreguliere, etc.)
  - prize breakdown per position (montantOffert1er..5eme)
  - paris details (12+ bet types with audiences, stakes)
  - heureDepart (exact start timestamp)
  - categorieParticularite, grandPrixNationalTrot
  - hippodrome codes (codeHippodrome, libelleCourt, libelleLong)
  - numSocieteMere, pariMultiCourses, pariSpecial
  - courseTrackee, replayDisponible

Outputs:
  output/101_pmu_api/pmu_courses_enriched.jsonl  (course-level extra fields)
  output/101_pmu_api/pmu_incidents.jsonl          (incident details)
  output/101_pmu_api/pmu_paris_detail.jsonl       (bet type details per course)
"""
import json
import os
import sys
from datetime import datetime, timezone

BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "output", "101_pmu_api")
CACHE_DIR = os.path.join(BASE, "cache")

OUT_COURSES = os.path.join(BASE, "pmu_courses_enriched.jsonl")
OUT_INCIDENTS = os.path.join(BASE, "pmu_incidents.jsonl")
OUT_PARIS = os.path.join(BASE, "pmu_paris_detail.jsonl")


def ts_to_iso(ts_ms):
    """Convert PMU timestamp (ms since epoch) to ISO date string."""
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return None


def ts_to_datetime(ts_ms):
    """Convert PMU timestamp to ISO datetime string."""
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError):
        return None


def extract_date_from_filename(fn):
    """Extract date from filename like course_2020-01-01_R1C1.json."""
    parts = fn.replace("course_", "").replace(".json", "").split("_")
    if len(parts) >= 1:
        return parts[0]
    return None


def process_file(filepath, fn):
    """Process a single cache file, yield (courses_row, incidents_rows, paris_rows)."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return None, [], []

    if not isinstance(data, dict):
        return None, [], []

    date_str = extract_date_from_filename(fn)
    num_reunion = data.get("numReunion")
    num_course = data.get("numOrdre") or data.get("numExterne")

    hippo = data.get("hippodrome", {})
    if isinstance(hippo, dict):
        hippo_code = hippo.get("codeHippodrome")
        hippo_court = hippo.get("libelleCourt")
        hippo_long = hippo.get("libelleLong")
    else:
        hippo_code = hippo_court = hippo_long = str(hippo) if hippo else None

    course_uid = f"{date_str}_R{num_reunion}C{num_course}"

    # --- Course enriched row ---
    course_row = {
        "course_uid": course_uid,
        "date": date_str,
        "num_reunion": num_reunion,
        "num_course": num_course,
        "hippodrome_code": hippo_code,
        "hippodrome_court": hippo_court,
        "hippodrome_long": hippo_long,
        "heure_depart": ts_to_datetime(data.get("heureDepart")),
        "montant_total_offert": data.get("montantTotalOffert"),
        "montant_offert_1er": data.get("montantOffert1er"),
        "montant_offert_2eme": data.get("montantOffert2eme"),
        "montant_offert_3eme": data.get("montantOffert3eme"),
        "montant_offert_4eme": data.get("montantOffert4eme"),
        "montant_offert_5eme": data.get("montantOffert5eme"),
        "categorie_particularite": data.get("categorieParticularite"),
        "grand_prix_national_trot": data.get("grandPrixNationalTrot"),
        "num_societe_mere": data.get("numSocieteMere"),
        "pari_multi_courses": data.get("pariMultiCourses"),
        "pari_special": data.get("pariSpecial"),
        "course_trackee": data.get("courseTrackee"),
        "replay_disponible": data.get("replayDisponible"),
        "epc_pour_tous_paris": data.get("epcPourTousParis"),
        "course_exclusive_internet": data.get("courseExclusiveInternet"),
        "num_course_dedoublee": data.get("numCourseDedoublee"),
        "nb_incidents": len(data.get("incidents", []) or []),
        "nb_types_paris": len(data.get("paris", []) or []),
        "nb_photos_arrivee": len(data.get("photosArrivee", []) or []),
    }

    # --- Incidents ---
    incidents_rows = []
    for inc in (data.get("incidents") or []):
        if isinstance(inc, dict):
            incidents_rows.append({
                "course_uid": course_uid,
                "date": date_str,
                "num_reunion": num_reunion,
                "num_course": num_course,
                "incident_type": inc.get("type"),
                "participants_nums": inc.get("numeroParticipants", []),
            })

    # --- Paris detail ---
    paris_rows = []
    for p in (data.get("paris") or []):
        if isinstance(p, dict):
            paris_rows.append({
                "course_uid": course_uid,
                "date": date_str,
                "num_reunion": num_reunion,
                "num_course": num_course,
                "type_pari": p.get("typePari") or p.get("codePari"),
                "mise_base": p.get("miseBase"),
                "en_vente": p.get("enVente"),
                "audience": p.get("audience"),
                "nb_chevaux_reglementaire": p.get("nbChevauxReglementaire"),
                "ordre": p.get("ordre"),
                "combine": p.get("combine"),
                "complement": p.get("complement"),
            })

    return course_row, incidents_rows, paris_rows


def main():
    if not os.path.isdir(CACHE_DIR):
        print(f"ERROR: Cache dir not found: {CACHE_DIR}")
        sys.exit(1)

    files = sorted([f for f in os.listdir(CACHE_DIR) if f.endswith(".json")])
    print(f"Processing {len(files)} cache files from {CACHE_DIR}")

    n_courses = 0
    n_incidents = 0
    n_paris = 0

    with open(OUT_COURSES, "w", encoding="utf-8") as fc, \
         open(OUT_INCIDENTS, "w", encoding="utf-8") as fi, \
         open(OUT_PARIS, "w", encoding="utf-8") as fp:

        for i, fn in enumerate(files):
            if i % 25000 == 0 and i > 0:
                print(f"  ...{i}/{len(files)} files processed")

            filepath = os.path.join(CACHE_DIR, fn)
            course_row, incidents, paris = process_file(filepath, fn)

            if course_row:
                fc.write(json.dumps(course_row, ensure_ascii=False) + "\n")
                n_courses += 1

            for inc in incidents:
                fi.write(json.dumps(inc, ensure_ascii=False) + "\n")
                n_incidents += 1

            for p in paris:
                fp.write(json.dumps(p, ensure_ascii=False) + "\n")
                n_paris += 1

    print(f"\nDone!")
    print(f"  {OUT_COURSES}: {n_courses} rows")
    print(f"  {OUT_INCIDENTS}: {n_incidents} rows")
    print(f"  {OUT_PARIS}: {n_paris} rows")


if __name__ == "__main__":
    main()
