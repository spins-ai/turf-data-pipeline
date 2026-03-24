#!/usr/bin/env python3
"""
Script 23 — Pronostics : collecte multi-source
Sources :
  1. API PMU /pronostics : ~30 derniers jours (JSON structuré)
  2. Geny pronostics_raw : 2020-2026 (HTML brut → parser)
  3. Cotes probables PMU dans partants : historique complet (cote_prob = proxy prono)

CRITIQUE pour : Anomaly Detector, Retour Forme, Outsider Detection, Value Bet

v2 : multi-source + HTML brut sauvegardé + graceful shutdown
v3 : ajout export JSONL (--export pour agréger le cache, ou écriture incrémentale)
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import argparse
import requests
import json
import time
import os
import re
import signal
import sys
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "../../output", "23_pronostics")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_DIR = os.path.join(OUTPUT_DIR, "html_raw")
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_DIR, exist_ok=True)

log = setup_logging("23_pronostics_equidia")

BASE_URL = "https://offline.turfinfo.api.pmu.fr/rest/client/7/programme"

all_records = []
output_file = os.path.join(OUTPUT_DIR, "pronostics_all.json")
jsonl_file = os.path.join(OUTPUT_DIR, "pronostics.jsonl")
checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_23v2.json")


def save_state(reason="checkpoint"):
    """Sauvegarde atomique"""
    try:
        tmp = output_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False)
        os.replace(tmp, output_file)
        log.info(f"💾 {reason}: {len(all_records)} records sauvés")
    except Exception as e:
        log.error(f"❌ Erreur sauvegarde: {e}")


def save_and_exit(signum=None, frame=None):
    save_state("Signal reçu")
    if signum:
        sys.exit(0)

signal.signal(signal.SIGTERM, save_and_exit)
signal.signal(signal.SIGINT, save_and_exit)


def append_jsonl(records, mode="a"):
    """Append records to JSONL output file."""
    if not records:
        return
    with open(jsonl_file, mode, encoding="utf-8", newline="\n") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _normalize_api_cache(data, cache_filename):
    """Convert raw API cache (selection format) into a flat record."""
    # Extract date/reunion/course from filename: api_DDMMYYYY_RX_CY.json
    parts = cache_filename.replace(".json", "").split("_")
    # parts = ['api', 'DDMMYYYY', 'RX', 'CY']
    if len(parts) < 4:
        return None
    date_raw = parts[1]
    num_r = parts[2].replace("R", "")
    num_c = parts[3].replace("C", "")
    try:
        dt = datetime.strptime(date_raw, "%d%m%Y")
        date_iso = dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    record = {
        "date_reunion_iso": date_iso,
        "numero_reunion": int(num_r) if num_r.isdigit() else num_r,
        "num_course": int(num_c) if num_c.isdigit() else num_c,
        "source_prono": "pmu_api",
    }
    for sel in data.get("selection", []):
        rang = sel.get("rang", 0)
        record[f"prono_rang_{rang}_num"] = sel.get("num_partant", "")
        record[f"prono_rang_{rang}_cote"] = sel.get("cote_prob", "")
    return record


def export_cache_to_jsonl():
    """Read all cache files and write a single JSONL output file."""
    log.info("=" * 60)
    log.info("EXPORT : Agrégation cache → JSONL")
    log.info("=" * 60)

    cache_files = sorted(os.listdir(CACHE_DIR))
    log.info(f"  {len(cache_files)} fichiers cache trouvés")

    records = []
    errors = 0

    for i, fname in enumerate(cache_files):
        fpath = os.path.join(CACHE_DIR, fname)
        try:
            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, ValueError, OSError) as e:
            log.debug(f"  Erreur lecture cache {fname}: {e}")
            errors += 1
            continue

        if fname.startswith("api_"):
            # Raw API cache has 'selection' key, needs normalization
            record = _normalize_api_cache(data, fname)
            if record:
                records.append(record)
        elif fname.startswith("cotes_"):
            # Cotes cache is already a flat record
            if isinstance(data, dict) and data:
                records.append(data)
        else:
            # Unknown format, skip
            errors += 1

        if (i + 1) % 50000 == 0:
            log.info(f"  [{i+1}/{len(cache_files)}] records={len(records)} erreurs={errors}")

    # Write all records to JSONL
    append_jsonl(records, mode="w")
    log.info(f"  JSONL écrit: {len(records)} records → {jsonl_file}")
    log.info(f"  Erreurs: {errors}")
    return len(records)


# ═══════════════════════════════════════════════════════════
# SOURCE 1 : API PMU pronostics (~30 derniers jours)
# ═══════════════════════════════════════════════════════════

def fetch_pronostics_api(date_str, numero_reunion, num_course):
    cache_key = f"api_{date_str}_R{numero_reunion}_C{num_course}"
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/{date_str}/R{numero_reunion}/C{num_course}/pronostics"
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        if resp.status_code == 200:
            data = resp.json()
            if data and data.get("selection"):
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                return data
        elif resp.status_code == 429:
            time.sleep(30)
        return None
    except Exception as e:
        log.debug(f"  Erreur réseau pronostics: {e}")
        return None


def collect_api_pronostics(courses):
    log.info("=" * 60)
    log.info("SOURCE 1 : API PMU pronostics (30 derniers jours)")
    log.info("=" * 60)

    cutoff = (datetime.now() - timedelta(days=35)).strftime("%Y-%m-%d")
    recent = [c for c in courses if c.get("date_reunion_iso", "") >= cutoff]
    log.info(f"  {len(recent)} courses dans les 35 derniers jours")

    records = []
    errors = 0

    for i, course in enumerate(recent):
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

        prono = fetch_pronostics_api(date_api, num_r, num_c)

        if prono and prono.get("selection"):
            record = {
                "course_uid": course.get("course_uid", ""),
                "date_reunion_iso": date_iso,
                "hippodrome": course.get("hippodrome_normalise", ""),
                "numero_reunion": num_r,
                "num_course": num_c,
                "source_prono": "pmu_api",
            }
            for sel in prono.get("selection", []):
                rang = sel.get("rang", 0)
                record[f"prono_rang_{rang}_num"] = sel.get("num_partant", "")
                record[f"prono_rang_{rang}_cote"] = sel.get("cote_prob", "")
            records.append(record)
        else:
            errors += 1

        if (i + 1) % 100 == 0:
            log.info(f"  API [{i+1}/{len(recent)}] trouvés={len(records)} vides={errors}")

        smart_pause(0.2, 0.1)

    log.info(f"  ✅ API terminé: {len(records)} pronostics récupérés")
    return records


# ═══════════════════════════════════════════════════════════
# SOURCE 2 : Geny pronostics HTML → parser + sauvegarde HTML brut
# ═══════════════════════════════════════════════════════════

def parse_geny_pronostics():
    log.info("=" * 60)
    log.info("SOURCE 2 : Geny pronostics HTML (2020-2026)")
    log.info("=" * 60)

    geny_path = os.path.join(BASE_DIR, "../../output", "26_geny", "geny_data.json")
    if not os.path.exists(geny_path):
        log.warning("  Pas de données Geny trouvées")
        return []

    with open(geny_path, encoding="utf-8") as f:
        geny_data = json.load(f)

    records = []
    html_saved = 0
    parsed = 0

    for entry in geny_data:
        date = entry.get("date", "")
        raw_list = entry.get("pronostics_raw", [])

        if not raw_list or not isinstance(raw_list, list):
            continue

        raw_html = " ".join(str(x) for x in raw_list if x)
        if len(raw_html) < 50:
            continue

        # TOUJOURS sauvegarder le HTML brut (on le parsera mieux après)
        html_file = os.path.join(HTML_DIR, f"geny_{date}.html")
        if not os.path.exists(html_file):
            with open(html_file, "w", encoding="utf-8") as f:
                f.write(raw_html)
            html_saved += 1

        try:
            soup = BeautifulSoup(raw_html, "html.parser")
            text = soup.get_text(" ", strip=True)

            # Pattern 1 : "N° X - NOM" ou "X. NOM"
            nums = re.findall(r'(?:^|\s)(\d{1,2})[\s\.\-–]+([A-Z][A-Z\s\'\-]{2,25})', text)

            if nums and len(nums) >= 3:
                record = {
                    "date": date,
                    "source_prono": "geny",
                    "html_file": f"geny_{date}.html",
                }
                for rank, (num, nom) in enumerate(nums[:7], 1):
                    record[f"prono_rang_{rank}_num"] = int(num)
                    record[f"prono_rang_{rank}_nom"] = nom.strip()
                records.append(record)
                parsed += 1

        except Exception as e:
            log.debug(f"  Erreur parsing Geny: {e}")

        if (html_saved + parsed) % 200 == 0 and (html_saved + parsed) > 0:
            log.info(f"  Geny [{html_saved + parsed}/{len(geny_data)}] parsés={parsed} HTML sauvés={html_saved}")

    log.info(f"  ✅ Geny: {parsed} parsés, {html_saved} HTML bruts sauvés dans {HTML_DIR}/")
    return records


# ═══════════════════════════════════════════════════════════
# SOURCE 3 : Cotes probables PMU comme proxy pronostic
# ═══════════════════════════════════════════════════════════

def collect_cotes_probables(courses):
    log.info("=" * 60)
    log.info("SOURCE 3 : Cotes probables PMU (proxy pronostic historique)")
    log.info("=" * 60)

    # Charger checkpoint source 3
    start_idx = 0
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, encoding="utf-8") as f:
                cp = json.load(f)
            start_idx = cp.get("source3_index", 0)
            if start_idx > 0:
                log.info(f"  Reprise source 3 à index {start_idx}")
        except (json.JSONDecodeError, ValueError):
            pass

    filtered = [c for c in courses if c.get("date_reunion_iso", "") >= "2014-01-01"]
    log.info(f"  {len(filtered)} courses 2014+")

    records = []
    errors = 0
    cached = 0

    for i in range(start_idx, len(filtered)):
        course = filtered[i]
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

        cache_key = f"cotes_{date_api}_R{num_r}_C{num_c}"
        cache_file_local = os.path.join(CACHE_DIR, f"{cache_key}.json")

        if os.path.exists(cache_file_local):
            try:
                with open(cache_file_local, encoding="utf-8") as f:
                    data = json.load(f)
                if data:
                    records.append(data)
                    cached += 1
                continue
            except (json.JSONDecodeError, ValueError):
                os.remove(cache_file_local)

        url = f"{BASE_URL}/{date_api}/R{num_r}/C{num_c}/participants"
        try:
            resp = requests.get(url, timeout=15, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            })
            if resp.status_code == 200:
                api_data = resp.json()
                participants = api_data.get("participants", []) if isinstance(api_data, dict) else (api_data if isinstance(api_data, list) else [])

                if participants:
                    partants = []
                    for p in participants:
                        gains_data = p.get("gainsParticipant", {})
                        gains = gains_data.get("gainsCarriere", 0) if isinstance(gains_data, dict) else 0
                        partants.append({
                            "num": p.get("numPmu", 0),
                            "nom": p.get("nom", ""),
                            "nb_victoires": p.get("nombreVictoires", 0) or 0,
                            "nb_courses": p.get("nombreCourses", 0) or 0,
                            "gains": gains or 0,
                        })

                    record = {
                        "course_uid": course.get("course_uid", ""),
                        "date_reunion_iso": date_iso,
                        "hippodrome": course.get("hippodrome_normalise", ""),
                        "numero_reunion": num_r,
                        "num_course": num_c,
                        "nb_partants": len(partants),
                        "source_prono": "pmu_participants",
                    }

                    # Trier par gains décroissants = proxy favori
                    partants.sort(key=lambda x: x.get("gains", 0), reverse=True)
                    for rank, p in enumerate(partants[:7], 1):
                        record[f"rank_{rank}_num"] = p["num"]
                        record[f"rank_{rank}_nom"] = p["nom"]
                        record[f"rank_{rank}_gains"] = p["gains"]
                        record[f"rank_{rank}_victoires"] = p["nb_victoires"]

                    with open(cache_file_local, "w", encoding="utf-8") as f:
                        json.dump(record, f, ensure_ascii=False)
                    records.append(record)
            elif resp.status_code == 429:
                time.sleep(30)
                errors += 1
            else:
                errors += 1

        except Exception as e:
            log.debug(f"  Erreur cotes PMU: {e}")
            errors += 1

        if (i + 1) % 500 == 0:
            log.info(f"  Cotes [{i+1}/{len(filtered)}] récup={len(records)} (cache={cached}) erreurs={errors}")
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump({"source3_index": i + 1}, f)

        if (i + 1) % 2000 == 0:
            # Sauvegarder dans all_records périodiquement
            save_state(f"checkpoint source 3 à {i+1}")

        smart_pause(0.2, 0.1)

    log.info(f"  ✅ Cotes terminé: {len(records)} records (cache={cached})")
    return records


def load_courses():
    courses = []
    seen = set()
    for path in [
        os.path.join(BASE_DIR, "../../output", "02_liste_courses", "courses_normalisees.json"),
        os.path.join(BASE_DIR, "../../output", "02b_liste_courses_2013", "courses_normalisees.json"),
    ]:
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                for c in data:
                    uid = c.get("course_uid", "")
                    if uid and uid not in seen:
                        seen.add(uid)
                        courses.append(c)
            except Exception as e:
                log.warning(f"  Erreur chargement {path}: {e}")
    courses.sort(key=lambda c: c.get("date_reunion_iso", ""))
    log.info(f"Chargé {len(courses)} courses uniques")
    return courses


def main():
    global all_records

    log.info("=" * 60)
    log.info("SCRIPT 23 — Pronostics Multi-Source (v2)")
    log.info("=" * 60)

    courses = load_courses()
    if not courses:
        log.error("Aucune course trouvée")
        return

    # Charger records existants v2
    if os.path.exists(output_file):
        try:
            with open(output_file, encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, list) and len(existing) > 0 and existing[0].get("source_prono"):
                all_records = existing
                log.info(f"  Reprise: {len(all_records)} records existants v2")
        except (json.JSONDecodeError, ValueError):
            pass

    existing_sources = set(r.get("source_prono", "") for r in all_records)

    # Reset JSONL if starting fresh (no existing records)
    jsonl_mode = "w" if not all_records else "a"

    # Source 1 : API PMU (30 derniers jours)
    if "pmu_api" not in existing_sources:
        api_records = collect_api_pronostics(courses)
        all_records.extend(api_records)
        append_jsonl(api_records, mode=jsonl_mode)
        jsonl_mode = "a"
        save_state("après API PMU")

    # Source 2 : Geny HTML
    if "geny" not in existing_sources:
        geny_records = parse_geny_pronostics()
        all_records.extend(geny_records)
        append_jsonl(geny_records, mode=jsonl_mode)
        jsonl_mode = "a"
        save_state("après Geny")

    # Source 3 : Cotes probables historiques (le plus long)
    if "pmu_participants" not in existing_sources:
        cotes_records = collect_cotes_probables(courses)
        all_records.extend(cotes_records)
        append_jsonl(cotes_records, mode=jsonl_mode)
        save_state("final")

    # Stats finales
    log.info("=" * 60)
    log.info(f"TERMINÉ: {len(all_records)} pronostics total")
    sources = {}
    for r in all_records:
        src = r.get("source_prono", "unknown")
        sources[src] = sources.get(src, 0) + 1
    for src, count in sorted(sources.items()):
        log.info(f"  {src}: {count}")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script 23 — Pronostics multi-source")
    parser.add_argument("--export", action="store_true",
                        help="Exporter tous les fichiers cache en un seul JSONL (sans scraping)")
    args = parser.parse_args()

    if args.export:
        export_cache_to_jsonl()
    else:
        main()
