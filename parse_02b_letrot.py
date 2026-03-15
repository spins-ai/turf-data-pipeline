#!/usr/bin/env python3
"""
Parse Le Trot HTML cache files into normalised courses and partants JSON.

Reads cache files from output/02b_scraper_letrot/cache/
Outputs:
  - output/02b_scraper_letrot/courses_normalisees.json
  - output/02b_scraper_letrot/partants_normalises.json
"""

import json
import os
import re
import sys
import hashlib
import html as html_module
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "output", "02b_scraper_letrot", "cache")
OUT_DIR = os.path.join(BASE_DIR, "output", "02b_scraper_letrot")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_uid(text: str) -> str:
    """Deterministic 16-hex-char UID from a string key."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def parse_discipline(code: str) -> str:
    """Map Le Trot discipline code to normalised discipline string."""
    mapping = {
        "A": "trot_attele",
        "M": "trot_monte",
        "AM": "trot_attele",   # fallback
    }
    return mapping.get(code, f"trot_{code.lower()}" if code else "trot_attele")


def parse_temps_to_ms(temps_str: str) -> int | None:
    """
    Convert Le Trot time string like 3'33"7 or 1'19"30 to milliseconds.
    Returns None if the time is not parseable (TNC, empty, DA, etc.).
    """
    if not temps_str or temps_str.strip().upper() in ("TNC", "", "DA", "NP", "CV"):
        return None
    temps_str = temps_str.strip()
    # Format: M'SS"D  where D can be 1 or 2 digits (tenths or hundredths)
    m = re.match(r"(\d+)'(\d+)\"(\d+)", temps_str)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        frac = m.group(3)
        if len(frac) == 1:
            ms_frac = int(frac) * 100
        elif len(frac) == 2:
            ms_frac = int(frac) * 10
        else:
            ms_frac = int(frac)
        return (minutes * 60 + seconds) * 1000 + ms_frac
    return None


def parse_reduction_to_ms(red_str: str) -> int | None:
    """Parse reduction kilométrique like 1'19"0 to ms per km."""
    return parse_temps_to_ms(red_str)


def parse_rang(rang_str: str) -> tuple:
    """
    Parse rang string. Returns (position_arrivee: int|None, is_disqualifie: bool, incident: str).
    Examples: "1 " -> (1, False, ""), "DA" -> (None, True, "DA"), "4D" -> (4, True, "D")
    """
    if not rang_str:
        return (None, False, "")
    rang = rang_str.strip()
    if not rang:
        return (None, False, "")

    # Pure number
    if rang.isdigit():
        return (int(rang), False, "")

    # Number + letter suffix: "4D" (disqualified), "6R" (rétrogradé), "4H"
    m = re.match(r"^(\d+)([A-Z]+)$", rang)
    if m:
        pos = int(m.group(1))
        suffix = m.group(2)
        is_dq = suffix in ("D", "H")  # D=disqualifié, H=hors course
        return (pos, is_dq, suffix)

    # Pure letters: DA (distancé/allure), CV (constat vétérinaire), NP (non-partant)
    if rang in ("DA", "DI", "DAI"):
        return (None, True, "DA")
    if rang in ("CV", "NP", "RET", "AR", "TB"):
        return (None, False, rang)

    return (None, False, rang)


def normalise_hippodrome(name: str) -> str:
    """Normalise hippodrome name to lowercase, stripped."""
    if not name:
        return ""
    # Remove parenthetical info like "(A PARILLY)"
    n = re.sub(r'\s*\([^)]*\)\s*', ' ', name)
    n = n.strip().lower()
    n = re.sub(r'[^a-z0-9àâäéèêëïîôùûüÿçœæ]+', '_', n)
    n = n.strip('_')
    return n


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

def extract_program_from_html(html_content: str) -> list | None:
    """Extract the :program JSON from the meeting-detail Vue component."""
    match = re.search(r':program="([^"]+)"', html_content)
    if not match:
        return None
    try:
        return json.loads(html_module.unescape(match.group(1)))
    except (json.JSONDecodeError, ValueError):
        return None


def process_file(filepath: str, filename: str) -> tuple:
    """
    Process a single cache file.
    Returns (courses_list, partants_list).
    """
    courses = []
    partants = []

    with open(filepath, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return courses, partants

    html_content = data.get("html", "")
    if not html_content:
        return courses, partants

    program = extract_program_from_html(html_content)
    if not program:
        return courses, partants

    # Identify target reunion from filename: YYYY-MM-DD_HIPPO.json
    parts = filename.replace(".json", "").split("_")
    if len(parts) < 2:
        return courses, partants
    file_date = parts[0]
    file_hippo = parts[1]

    # Find matching reunion
    reunion = None
    for r in program:
        if r.get("numHippodrome") == file_hippo and r.get("dateReunion") == file_date:
            reunion = r
            break

    if not reunion:
        # Fallback: try to find by hippo only (some files might have slight date mismatches)
        for r in program:
            if r.get("numHippodrome") == file_hippo:
                reunion = r
                break

    if not reunion or not reunion.get("races"):
        return courses, partants

    date_reunion = reunion.get("dateReunion", file_date)
    hippodrome_raw = reunion.get("nomHippodrome", "")
    hippodrome_norm = normalise_hippodrome(hippodrome_raw)
    num_hippodrome = reunion.get("numHippodrome", file_hippo)
    num_reunion = reunion.get("numReunion", 0)

    reunion_key = f"{date_reunion}|{hippodrome_norm}|{num_hippodrome}"
    reunion_uid = make_uid(reunion_key)

    for race in reunion["races"]:
        num_course = race.get("numCourse") or race.get("raceNbr", 0)
        discipline_code = race.get("discipline", "A")
        discipline = parse_discipline(discipline_code)
        distance = race.get("distance", 0)
        race_name = race.get("raceName", "")
        date_course = race.get("dateCourse", date_reunion)
        allocation = race.get("allocation", 0)
        type_piste = race.get("typePiste", "")
        corde = race.get("corde", "")
        autostart = race.get("autostart", 0)
        nb_partants = race.get("countPartant", 0)
        statut = race.get("statut", race.get("status", ""))
        condition_age = race.get("conditionAge", "")
        type_depart = race.get("typeDepart", "")
        competition = race.get("competition", "")
        race_id = race.get("id", f"{date_course}-{num_hippodrome}-{num_course}")

        course_key = f"{date_course}|{hippodrome_norm}|{num_hippodrome}|C{num_course}"
        course_uid = make_uid(course_key)

        # Compute corde normalised
        corde_norm = ""
        if corde:
            corde_lower = corde.lower()
            if corde_lower in ("g", "gauche"):
                corde_norm = "gauche"
            elif corde_lower in ("d", "droite"):
                corde_norm = "droite"
            else:
                corde_norm = corde_lower

        # Determine mode_depart
        if autostart:
            mode_depart = "autostart"
        elif type_depart:
            mode_depart = type_depart.lower()
        else:
            mode_depart = "volte"

        # Build ordre_arrivee from partants sorted by rang
        ordre_arrivee = []
        partants_race = race.get("partants", [])
        sorted_partants = []
        for p in partants_race:
            pos, is_dq, incident = parse_rang(p.get("rang", ""))
            if pos is not None and not is_dq:
                sorted_partants.append((pos, p.get("leavingNumber", 0)))
        sorted_partants.sort(key=lambda x: x[0])
        ordre_arrivee = [[sp[1]] for sp in sorted_partants[:5]]

        course_record = {
            "course_uid": course_uid,
            "reunion_uid": reunion_uid,
            "cle_course": f"{date_course}|{hippodrome_norm}|R{num_reunion}|C{num_course}",
            "source": "letrot",
            "date_reunion_iso": date_course,
            "hippodrome_normalise": hippodrome_norm,
            "hippodrome": hippodrome_raw,
            "pays": "France",
            "numero_reunion": num_reunion,
            "numero_course": num_course,
            "libelle": race_name,
            "distance": distance,
            "parcours": race.get("codeParcours", ""),
            "corde": corde_norm,
            "discipline": discipline,
            "specialite": discipline,
            "conditions_texte": condition_age,
            "condition_sexe": "",
            "condition_age": condition_age,
            "categorie": race.get("categorie", ""),
            "mode_depart": mode_depart,
            "nombre_partants": nb_partants,
            "heure_depart": "",
            "allocation_totale": allocation,
            "allocation_1er": None,
            "type_piste": type_piste,
            "penetrometre": "",
            "statut": statut.lower() if statut else "",
            "ordre_arrivee": ordre_arrivee,
            "duree_course_ms": None,
            "incidents": [],
            "paris_types": [],
            "replay_disponible": bool(race.get("videoUrl")),
            "course_trackee": False,
            "timestamp_collecte": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url_source": f"https://www.letrot.com/courses/{date_course}/{num_hippodrome}/{num_course}",
        }

        # Extract hour
        hour_data = race.get("hour")
        if hour_data and isinstance(hour_data, dict):
            date_str = hour_data.get("date", "")
            if date_str:
                m = re.search(r'(\d{2}:\d{2}):\d{2}', date_str)
                if m:
                    course_record["heure_depart"] = m.group(1)

        courses.append(course_record)

        # Process partants
        for p in partants_race:
            pos, is_dq, incident = parse_rang(p.get("rang", ""))
            non_partant = p.get("nonPartant", False)

            # Determine statut
            if non_partant:
                statut_p = "non_partant"
            elif is_dq:
                statut_p = "disqualifie"
            else:
                statut_p = "partant"

            temps_ms = parse_temps_to_ms(p.get("temps", ""))
            reduction_ms = parse_reduction_to_ms(p.get("reduction", ""))

            leaving_num = p.get("leavingNumber", 0)

            partant_key = f"{date_course}|{hippodrome_norm}|R{num_reunion}|C{num_course}|{leaving_num}"
            partant_uid = make_uid(partant_key)

            # Horse ID from numSire in indexEntraineurs or use the id field
            horse_id_raw = p.get("id", "")
            horse_id = make_uid(f"letrot|{horse_id_raw}") if horse_id_raw else ""

            # Musique from song field
            musique = p.get("song", "")

            # Gains
            earnings_str = p.get("earnings", "")
            try:
                gains = int(earnings_str) if earnings_str else None
            except (ValueError, TypeError):
                gains = None

            partant_record = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "reunion_uid": reunion_uid,
                "cle_partant": partant_key,
                "source": "letrot",
                "date_reunion_iso": date_course,
                "hippodrome_normalise": hippodrome_norm,
                "numero_reunion": num_reunion,
                "numero_course": num_course,
                "distance": p.get("distance", distance),
                "discipline": discipline,
                "horse_id": horse_id,
                "nom_cheval": p.get("name", ""),
                "num_pmu": leaving_num,
                "age": p.get("age"),
                "sexe": p.get("sexe", ""),
                "race": "TROTTEUR FRANCAIS",
                "robe": p.get("robe", ""),
                "musique": musique,
                "nb_courses_carriere": None,
                "nb_victoires_carriere": None,
                "nb_places_carriere": None,
                "nb_places_2eme": None,
                "nb_places_3eme": None,
                "gains_carriere_euros": gains,
                "gains_annee_euros": None,
                "is_inedit": p.get("inedit", False),
                "jockey_driver": p.get("driver", ""),
                "jockey_driver_change": p.get("firstJockey", False),
                "entraineur": p.get("coach", ""),
                "proprietaire": p.get("owner", ""),
                "pere": p.get("father", "") or "",
                "mere": p.get("mother", "") or "",
                "eleveur": p.get("breeder", "") or "",
                "oeilleres": "",
                "deferre": p.get("ferrure", ""),
                "statut": statut_p,
                "engagement": False,
                "supplement_euros": 0.0,
                "handicap_distance_m": p.get("distance", distance),
                "poids_porte_kg": None,
                "poids_base_kg": None,
                "surcharge_decharge_kg": None,
                "handicap_valeur": None,
                "poids_monte_change": False,
                "taux_reclamation_euros": None,
                "place_corde": None,
                "allure": "trot",
                "pays_cheval": "",
                "pays_entrainement": "",
                "pere_mere": p.get("fatherMother", "") or "",
                "incident": incident,
                "ecart_precedent": "",
                "commentaire_apres_course": p.get("commentairePartant", "") or "",
                "avis_entraineur": "",
                "jument_pleine": False,
                "position_arrivee": pos,
                "temps_ms": temps_ms,
                "reduction_km_ms": reduction_ms,
                "is_gagnant": pos == 1 and not is_dq,
                "is_place": pos is not None and pos <= 3 and not is_dq,
                "is_disqualifie": is_dq,
                "cote_finale": None,
                "cote_reference": None,
                "proba_implicite": None,
                "record": p.get("record", ""),
                "allocation_partant": p.get("allocation", 0),
                "timestamp_collecte": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            partants.append(partant_record)

    return courses, partants


def main():
    print(f"Scanning cache directory: {CACHE_DIR}")
    files = sorted([f for f in os.listdir(CACHE_DIR) if f.endswith(".json")])
    total = len(files)
    print(f"Found {total} cache files to process")

    all_courses = []
    all_partants = []
    errors = 0
    skipped = 0

    for i, filename in enumerate(files):
        filepath = os.path.join(CACHE_DIR, filename)
        try:
            c, p = process_file(filepath, filename)
            if not c and not p:
                skipped += 1
            all_courses.extend(c)
            all_partants.extend(p)
        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  ERROR processing {filename}: {e}", file=sys.stderr)

        if (i + 1) % 500 == 0:
            print(f"  Progress: {i+1}/{total} files processed | {len(all_courses)} courses | {len(all_partants)} partants")

    print(f"\nProcessing complete:")
    print(f"  Files processed: {total}")
    print(f"  Files skipped (no data): {skipped}")
    print(f"  Errors: {errors}")
    print(f"  Courses extracted: {len(all_courses)}")
    print(f"  Partants extracted: {len(all_partants)}")

    # Deduplicate by course_uid / partant_uid
    seen_courses = {}
    for c in all_courses:
        seen_courses[c["course_uid"]] = c
    unique_courses = list(seen_courses.values())

    seen_partants = {}
    for p in all_partants:
        seen_partants[p["partant_uid"]] = p
    unique_partants = list(seen_partants.values())

    print(f"\n  After dedup:")
    print(f"    Unique courses: {len(unique_courses)}")
    print(f"    Unique partants: {len(unique_partants)}")

    # Write output
    courses_path = os.path.join(OUT_DIR, "courses_normalisees.json")
    partants_path = os.path.join(OUT_DIR, "partants_normalises.json")

    with open(courses_path, "w", encoding="utf-8") as f:
        json.dump(unique_courses, f, ensure_ascii=False, indent=None)
    print(f"\n  Written: {courses_path} ({os.path.getsize(courses_path):,} bytes)")

    with open(partants_path, "w", encoding="utf-8") as f:
        json.dump(unique_partants, f, ensure_ascii=False, indent=None)
    print(f"  Written: {partants_path} ({os.path.getsize(partants_path):,} bytes)")

    # Quick stats
    disciplines = {}
    for c in unique_courses:
        d = c.get("discipline", "unknown")
        disciplines[d] = disciplines.get(d, 0) + 1
    print(f"\n  Disciplines: {disciplines}")

    dates = [c["date_reunion_iso"] for c in unique_courses if c["date_reunion_iso"]]
    if dates:
        print(f"  Date range: {min(dates)} -> {max(dates)}")

    with_temps = sum(1 for p in unique_partants if p["temps_ms"] is not None)
    with_pere = sum(1 for p in unique_partants if p["pere"])
    print(f"  Partants with temps_ms: {with_temps} ({100*with_temps/max(1,len(unique_partants)):.1f}%)")
    print(f"  Partants with pere: {with_pere} ({100*with_pere/max(1,len(unique_partants)):.1f}%)")


if __name__ == "__main__":
    main()
