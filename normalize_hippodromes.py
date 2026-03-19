#!/usr/bin/env python3
"""
normalize_hippodromes.py
========================
Normalise les noms d'hippodromes a travers toutes les sources.

Regles :
  - Tout en minuscules, sans accents, sans tirets, sans apostrophes
  - Mapping des variantes : VINCENNES/vincennes/Vincennes -> vincennes
  - Mapping des alias : SAINT-CLOUD/ST-CLOUD/ST CLOUD -> saint cloud
  - Reference : hippodromes_db.py (cles = noms canoniques)
  - Applique a : partants_master, courses_master, tous les merge outputs

Streaming JSONL -> JSONL pour supporter des fichiers de plusieurs GB.

Usage :
    python normalize_hippodromes.py
"""

import json
import os
import sys
import time
import unicodedata
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"
LOG_PREFIX = "normalize_hippodromes"

# -----------------------------------------------------------------------
# Chargement de la reference hippodromes_db
# -----------------------------------------------------------------------

def load_hippodromes_db():
    """Charge HIPPODROMES_DB depuis hippodromes_db.py."""
    db_path = BASE_DIR / "hippodromes_db.py"
    if not db_path.exists():
        print(f"[WARN] hippodromes_db.py introuvable: {db_path}")
        return {}
    ns = {}
    with open(db_path, "r", encoding="utf-8", errors="replace") as f:
        exec(f.read(), ns)
    return ns.get("HIPPODROMES_DB", {})


# -----------------------------------------------------------------------
# Normalisation
# -----------------------------------------------------------------------

def strip_accents(text):
    """Supprime les accents d'une chaine."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_raw(name):
    """Normalisation brute : minuscules, sans accents, sans ponctuation speciale."""
    if not name:
        return ""
    name = str(name).strip().lower()
    name = strip_accents(name)
    name = name.replace("-", " ").replace("'", " ").replace("'", " ").replace("`", " ")
    name = re.sub(r"[^a-z0-9 ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


# Alias courants : variante -> forme canonique
# La forme canonique doit etre une cle de HIPPODROMES_DB
ALIAS_MAP = {
    # Saint -> saint (sans tiret)
    "st cloud": "saint cloud",
    "st-cloud": "saint cloud",
    "stcloud": "saint cloud",
    "st malo": "saint malo",
    "st-malo": "saint malo",
    "st brieuc": "saint brieuc",
    "st-brieuc": "saint brieuc",
    "st omer": "saint omer",
    "st-omer": "saint omer",
    "st galmier": "saint galmier",
    "st-galmier": "saint galmier",
    "ste genevieve des bois": "sainte genevieve des bois",

    # Abreviations courantes
    "lyon la soie": "lyon",
    "lyon parilly": "lyon parilly",
    "paris vincennes": "vincennes",
    "hippodrome de vincennes": "vincennes",
    "paris longchamp": "longchamp",
    "hippodrome de longchamp": "longchamp",
    "hippodrome de chantilly": "chantilly",
    "hippodrome dauteuil": "auteuil",
    "hippodrome de auteuil": "auteuil",
    "hippodrome de deauville": "deauville",
    "maisons laffitte": "maisons laffitte",
    "maisonnais": "maisons laffitte",
    "enghien soisy": "enghien",
    "enghien les bains": "enghien",

    # Cagnes
    "cagnes": "cagnes sur mer",
    "cagnessurmer": "cagnes sur mer",
    "cagnes sur mer gazon": "cagnes sur mer",

    # Bordeaux
    "bordeaux le bouscat": "bordeaux",
    "le bouscat": "bordeaux",

    # Marseille
    "marseille borely": "borely",
    "marseille vivaux": "marseille vivaux",

    # Le Mans
    "lemans": "le mans",

    # Le Croise-Laroche
    "le croise laroche": "le croise laroche",
    "croiselaroche": "le croise laroche",

    # Chatelaillon
    "chatelaillon la rochelle": "chatelaillon",

    # Clairefontaine
    "clairefontaine deauville": "clairefontaine",

    # Avignon
    "avignon le pontet": "avignon",
}


def build_canonical_lookup(hippo_db):
    """Construit un dictionnaire normalise -> canonical a partir de hippo_db + alias."""
    lookup = {}

    # D'abord, chaque cle de la DB est deja canonique (minuscules sans accents)
    for key in hippo_db:
        norm = normalize_raw(key)
        if norm:
            lookup[norm] = norm

    # Ensuite, appliquer les alias
    for alias_raw, canonical in ALIAS_MAP.items():
        norm_alias = normalize_raw(alias_raw)
        norm_canon = normalize_raw(canonical)
        if norm_alias:
            lookup[norm_alias] = norm_canon

    return lookup


def normalize_hippodrome(name, lookup):
    """Normalise un nom d'hippodrome via le lookup. Retourne le nom canonique."""
    if not name:
        return name
    norm = normalize_raw(name)
    if not norm:
        return name
    # Lookup exact
    if norm in lookup:
        return lookup[norm]
    # Essayer sans suffixes courants (pays, etc.)
    for suffix in [" france", " suede", " grande bretagne", " australie",
                   " allemagne", " italie", " belgique", " norvege"]:
        if norm.endswith(suffix):
            trimmed = norm[: -len(suffix)].strip()
            if trimmed in lookup:
                return lookup[trimmed]
    return norm


# -----------------------------------------------------------------------
# Champs a normaliser selon le type de fichier
# -----------------------------------------------------------------------

HIPPODROME_FIELDS = [
    "hippodrome",
    "hippodrome_normalise",
    "hippo",
    "hippodrome_norm",
    "hippodrome_course",
]


def normalize_record(record, lookup):
    """Normalise tous les champs hippodrome d'un record."""
    changed = False
    for field in HIPPODROME_FIELDS:
        val = record.get(field)
        if val and isinstance(val, str):
            new_val = normalize_hippodrome(val, lookup)
            if new_val != val:
                record[field] = new_val
                changed = True
    return changed


# -----------------------------------------------------------------------
# Traitement d'un fichier JSONL
# -----------------------------------------------------------------------

def process_jsonl(input_path, output_path, lookup):
    """Lit un JSONL, normalise les hippodromes, ecrit le resultat."""
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
         open(tmp_path, "w", encoding="utf-8", errors="replace") as fout:
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

            if normalize_record(record, lookup):
                changed += 1

            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            total += 1

            if total % 500000 == 0:
                print(f"    {total:,} lignes traitees, {changed:,} modifiees ...")

    os.replace(str(tmp_path), str(output_path))
    dt = time.time() - t0
    print(f"    -> {total:,} lignes, {changed:,} modifiees ({dt:.1f}s)")
    return total, changed


def process_json(input_path, output_path, lookup):
    """Lit un JSON (array), normalise les hippodromes, ecrit le resultat."""
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
        if normalize_record(record, lookup):
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
    print("NORMALIZE HIPPODROMES — Normalisation des noms d'hippodromes")
    print("=" * 70)

    # 1. Charger la DB de reference
    print("\n[1] Chargement hippodromes_db ...")
    hippo_db = load_hippodromes_db()
    print(f"    {len(hippo_db):,} hippodromes en reference")

    # 2. Construire le lookup
    lookup = build_canonical_lookup(hippo_db)
    print(f"    {len(lookup):,} variantes dans le lookup")

    # 3. Fichiers a traiter
    total_records = 0
    total_changed = 0

    # -- JSONL files in data_master/
    jsonl_files = [
        "partants_master.jsonl",
        "partants_master_enrichi.jsonl",
        "courses_master.jsonl",
    ]

    print("\n[2] Traitement des fichiers JSONL (data_master/) ...")
    for fname in jsonl_files:
        fpath = DATA_MASTER / fname
        if fpath.exists():
            n, c = process_jsonl(fpath, fpath, lookup)
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

    print("\n[3] Traitement des fichiers JSON (data_master/) ...")
    for fname in json_files:
        fpath = DATA_MASTER / fname
        if fpath.exists():
            n, c = process_json(fpath, fpath, lookup)
            total_records += n
            total_changed += c

    # -- Output merge files
    merge_outputs = [
        "output/02_liste_courses/courses_enrichies.json",
        "output/02_liste_courses/courses_normalisees.json",
        "output/02_liste_courses/partants_normalises.json",
    ]

    print("\n[4] Traitement des fichiers merge (output/) ...")
    for rel_path in merge_outputs:
        fpath = BASE_DIR / rel_path
        if fpath.exists():
            if rel_path.endswith(".jsonl"):
                n, c = process_jsonl(fpath, fpath, lookup)
            else:
                n, c = process_json(fpath, fpath, lookup)
            total_records += n
            total_changed += c

    # -- JSONL output files
    jsonl_outputs = [
        "output/02_liste_courses/partants_normalises.jsonl",
        "output/02_liste_courses/courses_normalisees.jsonl",
    ]

    print("\n[5] Traitement des fichiers JSONL (output/) ...")
    for rel_path in jsonl_outputs:
        fpath = BASE_DIR / rel_path
        if fpath.exists():
            n, c = process_jsonl(fpath, fpath, lookup)
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
