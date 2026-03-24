#!/usr/bin/env python3
"""
normalize_disciplines.py
========================
Normalise les noms de disciplines a travers tous les fichiers de donnees.

Mapping :
  TROT_ATTELE / trot_attele / Trot Attele / ATTELE / attele / trot attele -> TROT_ATTELE
  TROT_MONTE / trot_monte / Trot Monte / MONTE / monte                   -> TROT_MONTE
  PLAT / plat / Plat / GALOP_PLAT / galop_plat / galop plat / flat       -> PLAT
  HAIE / haie / Haie / haies / HAIES / hurdle                            -> HAIE
  STEEPLE / steeple / Steeple / steeplechase / steeple chase / chase     -> STEEPLE
  CROSS_COUNTRY / cross_country / cross country / Cross Country           -> CROSS_COUNTRY

Applique a tous les fichiers data_master + output.
Streaming JSONL -> JSONL pour les gros fichiers.

Usage :
    python normalize_disciplines.py
"""

import json
import os
import time
import re
from pathlib import Path

from utils.normalize import strip_accents

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"


# -----------------------------------------------------------------------
# Mapping des disciplines
# -----------------------------------------------------------------------

def _norm_key(val):
    """Normalise une valeur pour le lookup : minuscules, sans accents, sans tirets."""
    if not val:
        return ""
    val = str(val).strip().lower()
    val = strip_accents(val)
    val = val.replace("-", "_").replace(" ", "_")
    val = re.sub(r"_+", "_", val).strip("_")
    return val


# Table de mapping : clef normalisee -> valeur canonique
_DISCIPLINE_MAP = {}

_DISCIPLINE_ALIASES = {
    "TROT_ATTELE": [
        "trot_attele", "trot attele", "trot-attele", "TROT_ATTELE",
        "Trot Attele", "Trot Attelé", "trot_attele", "ATTELE",
        "attele", "attelé", "attele", "trot attele", "trotattele",
        "atele", "trot_atele", "trot attelé", "Trot Attele",
    ],
    "TROT_MONTE": [
        "trot_monte", "trot monte", "trot-monte", "TROT_MONTE",
        "Trot Monte", "Trot Monté", "trot monte", "MONTE",
        "monte", "monté", "trotmonte", "trot_monte",
    ],
    "PLAT": [
        "plat", "PLAT", "Plat", "GALOP_PLAT", "galop_plat",
        "galop plat", "galop-plat", "Galop Plat", "flat",
        "FLAT", "Flat", "galop", "GALOP", "Galop",
    ],
    "HAIE": [
        "haie", "HAIE", "Haie", "haies", "HAIES", "Haies",
        "hurdle", "HURDLE", "Hurdle", "hurdles", "HURDLES",
    ],
    "STEEPLE": [
        "steeple", "STEEPLE", "Steeple", "steeplechase",
        "steeple_chase", "steeple chase", "steeple-chase",
        "STEEPLECHASE", "Steeplechase", "chase", "CHASE", "Chase",
    ],
    "CROSS_COUNTRY": [
        "cross_country", "cross country", "cross-country",
        "CROSS_COUNTRY", "Cross Country", "Cross-Country",
        "crosscountry", "CROSSCOUNTRY",
    ],
}

# Construire le mapping
for canonical, aliases in _DISCIPLINE_ALIASES.items():
    for alias in aliases:
        key = _norm_key(alias)
        if key:
            _DISCIPLINE_MAP[key] = canonical
    # Aussi la forme canonique elle-meme
    _DISCIPLINE_MAP[_norm_key(canonical)] = canonical


def normalize_discipline(val):
    """Normalise une valeur de discipline. Retourne la forme canonique ou la valeur nettoyee."""
    if not val or not isinstance(val, str):
        return val
    key = _norm_key(val)
    if not key:
        return val
    return _DISCIPLINE_MAP.get(key, val)


# -----------------------------------------------------------------------
# Champs de discipline a normaliser
# -----------------------------------------------------------------------

DISCIPLINE_FIELDS = [
    "discipline",
    "discipline_norm",
    "discipline_course",
    "specialite",
    "specialiste_discipline",
]


def normalize_record(record):
    """Normalise tous les champs discipline d'un record. Retourne True si modifie."""
    changed = False
    for field in DISCIPLINE_FIELDS:
        val = record.get(field)
        if val and isinstance(val, str):
            new_val = normalize_discipline(val)
            if new_val != val:
                record[field] = new_val
                changed = True

    # Aussi normaliser les listes de disciplines (ex: hippodromes_db)
    for field in ["disciplines"]:
        val = record.get(field)
        if isinstance(val, list):
            new_list = []
            list_changed = False
            for item in val:
                if isinstance(item, str):
                    new_item = normalize_discipline(item)
                    if new_item != item:
                        list_changed = True
                    new_list.append(new_item)
                else:
                    new_list.append(item)
            if list_changed:
                record[field] = new_list
                changed = True

    return changed


# -----------------------------------------------------------------------
# Traitement fichiers
# -----------------------------------------------------------------------

def process_jsonl(input_path, output_path):
    """Lit un JSONL, normalise les disciplines, ecrit le resultat."""
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        print(f"  [SKIP] {input_path} introuvable")
        return 0, 0

    print(f"  Traitement: {input_path.name} ...")
    t0 = time.time()
    total = 0
    changed = 0
    tmp_path = output_path.with_suffix(".jsonl.tmp")

    with open(input_path, "r", encoding="utf-8", errors="replace", buffering=1024*1024) as fin, \
         open(tmp_path, "w", encoding="utf-8", errors="replace", newline="\n") as fout:
        while True:
            line = fin.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                fout.write(line + "\n")
                total += 1
                continue

            if normalize_record(record):
                changed += 1

            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            total += 1

            if total % 500000 == 0:
                print(f"    {total:,} lignes, {changed:,} modifiees ...")

    os.replace(str(tmp_path), str(output_path))
    dt = time.time() - t0
    print(f"    -> {total:,} lignes, {changed:,} modifiees ({dt:.1f}s)")
    return total, changed


def process_json(input_path, output_path):
    """Lit un JSON (array), normalise les disciplines, ecrit le resultat."""
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        print(f"  [SKIP] {input_path} introuvable")
        return 0, 0

    print(f"  Traitement: {input_path.name} ...")
    t0 = time.time()

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print(f"  [SKIP] {input_path.name} n'est pas un array JSON")
        return 0, 0

    changed = 0
    for record in data:
        if normalize_record(record):
            changed += 1

    tmp_path = output_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(str(tmp_path), str(output_path))

    dt = time.time() - t0
    print(f"    -> {len(data):,} records, {changed:,} modifies ({dt:.1f}s)")
    return len(data), changed


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    t_start = time.time()
    print("=" * 70)
    print("NORMALIZE DISCIPLINES — Normalisation des noms de disciplines")
    print("=" * 70)

    print(f"\n  {len(_DISCIPLINE_MAP)} variantes mappees vers {len(_DISCIPLINE_ALIASES)} formes canoniques")

    total_records = 0
    total_changed = 0

    # -- JSONL files in data_master/
    jsonl_files = [
        "partants_master.jsonl",
        "partants_master_enrichi.jsonl",
        "courses_master.jsonl",
    ]

    print("\n[1] Fichiers JSONL (data_master/) ...")
    for fname in jsonl_files:
        fpath = DATA_MASTER / fname
        if fpath.exists():
            n, c = process_jsonl(fpath, fpath)
            total_records += n
            total_changed += c

    # -- JSON files in data_master/
    json_files = [
        "equipements_master.json",
        "meteo_master.json",
        "rapports_master.json",
        "marche_master.json",
        "horse_stats_master.json",
        "partants_complets.json",
    ]

    print("\n[2] Fichiers JSON (data_master/) ...")
    for fname in json_files:
        fpath = DATA_MASTER / fname
        if fpath.exists():
            n, c = process_json(fpath, fpath)
            total_records += n
            total_changed += c

    # -- Output JSON files
    output_json = [
        os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_enrichies.json"),
        os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_normalisees.json"),
        os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.json"),
    ]

    print("\n[3] Fichiers JSON (output/) ...")
    for rel_path in output_json:
        fpath = BASE_DIR / rel_path
        if fpath.exists():
            n, c = process_json(fpath, fpath)
            total_records += n
            total_changed += c

    # -- Output JSONL files
    output_jsonl = [
        os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.jsonl"),
        os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_normalisees.jsonl"),
        os.path.join(BASE_DIR, "output", "41_sequences", "sequences_performances.jsonl"),
        os.path.join(BASE_DIR, "output", "42_croisement_rp_pmu", "croisement_rp_pmu.jsonl"),
        os.path.join(BASE_DIR, "output", "43_croisement_meteo_courses", "croisement_meteo_courses.jsonl"),
    ]

    print("\n[4] Fichiers JSONL (output/) ...")
    for rel_path in output_jsonl:
        fpath = BASE_DIR / rel_path
        if fpath.exists():
            n, c = process_jsonl(fpath, fpath)
            total_records += n
            total_changed += c

    # -- Rapport
    print("\n" + "=" * 70)
    print("RESULTATS")
    print("=" * 70)
    print(f"  Records traites: {total_records:,}")
    print(f"  Records modifies: {total_changed:,}")
    print(f"  Taux modification: {total_changed * 100 / max(total_records, 1):.1f}%")

    dt = time.time() - t_start
    print(f"\nTermine en {dt:.0f}s ({dt / 60:.1f} min)")


if __name__ == "__main__":
    main()
