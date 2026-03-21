#!/usr/bin/env python3
"""
rebuild_all_indexes.py
======================
Reconstruit tous les index de lookup a partir des donnees brutes.

Index generes (petits fichiers JSON pour lookup rapide):
  - data_master/indexes/horse_index.json     (depuis pedigree_master)
  - data_master/indexes/course_index.json    (depuis courses_master)
  - data_master/indexes/jockey_index.json    (depuis historique_jockeys)
  - data_master/indexes/hippodrome_index.json (depuis courses_master)

Chaque index est un dict { cle: { champs_essentiels } }

Usage:
    python3 rebuild_all_indexes.py
"""

import json
import logging
import os
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)
nBASE_DIR = os.path.dirname(os.path.abspath(__file__))

INDEX_DIR = os.path.join("data_master", "indexes")
os.makedirs(INDEX_DIR, exist_ok=True)


def load_json_safe(path):
    """Charge un fichier JSON avec gestion d'encodage."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    except (json.JSONDecodeError, MemoryError) as e:
        log.warning("  Erreur chargement %s: %s", path, e)
        return None


def stream_jsonl(path, max_lines=None):
    """Generateur qui lit un fichier JSONL ligne par ligne."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if max_lines and i >= max_lines:
                break
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def save_index(index, name):
    """Sauvegarde un index en JSON."""
    path = os.path.join(INDEX_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=None, separators=(",", ":"))
    size = os.path.getsize(path)
    log.info("  -> Sauvegarde: %s (%d entrees, %.1f MB)", path, len(index), size / (1024 * 1024))
    return path


# ================================================================
# INDEX 1: horse_index (depuis pedigree_master)
# ================================================================

def build_horse_index():
    """Construit l'index des chevaux a partir de pedigree_master."""
    log.info("")
    log.info("--- Horse Index (pedigree_master) ---")

    index = {}
    keep_fields = ["nom_cheval", "nom", "pere", "mere", "pere_mere",
                   "sexe", "annee_naissance", "pays_naissance", "robe",
                   "naisseur", "proprietaire"]

    # Essayer pedigree_master.json
    path_json = os.path.join(BASE_DIR, "data_master", "pedigree_master.json")
    path_csv = os.path.join(BASE_DIR, "data_master", "pedigree_master.csv")

    if os.path.exists(path_json):
        log.info("  Source: %s", path_json)
        data = load_json_safe(path_json)
        if data and isinstance(data, list):
            for rec in data:
                nom = (rec.get("nom_cheval") or rec.get("nom") or "").upper().strip()
                if not nom:
                    continue
                entry = {}
                for f in keep_fields:
                    if f in rec and rec[f] is not None and rec[f] != "":
                        entry[f] = rec[f]
                if entry:
                    index[nom] = entry
            del data
    elif os.path.exists(path_csv):
        log.info("  Source: %s", path_csv)
        import csv
        with open(path_csv, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for rec in reader:
                nom = (rec.get("nom_cheval") or rec.get("nom") or "").upper().strip()
                if not nom:
                    continue
                entry = {}
                for field in keep_fields:
                    val = rec.get(field, "").strip()
                    if val:
                        entry[field] = val
                if entry:
                    index[nom] = entry
    else:
        # Fallback: essayer les fichiers output
        for path in [os.path.join(BASE_DIR, "output", "08_pedigree", "pedigree.json"),
                      os.path.join(BASE_DIR, "output", "12_pedigree", "pedigree.json"),
                      os.path.join(BASE_DIR, "output", "14_pedigree", "pedigree.json")]:
            if os.path.exists(path):
                log.info("  Source fallback: %s", path)
                data = load_json_safe(path)
                if data and isinstance(data, list):
                    for rec in data:
                        nom = (rec.get("nom_cheval") or rec.get("nom") or "").upper().strip()
                        if not nom:
                            continue
                        entry = {}
                        for f in keep_fields:
                            if f in rec and rec[f] is not None and rec[f] != "":
                                entry[f] = rec[f]
                        if entry:
                            index[nom] = entry
                    del data

    log.info("  Chevaux indexes: %d", len(index))
    if index:
        save_index(index, "horse_index.json")
    return index


# ================================================================
# INDEX 2: course_index (depuis courses_master)
# ================================================================

def build_course_index():
    """Construit l'index des courses a partir de courses_master."""
    log.info("")
    log.info("--- Course Index (courses_master) ---")

    index = {}
    keep_fields = ["course_uid", "date_reunion_iso", "hippodrome",
                   "hippodrome_normalise", "discipline", "distance_metres",
                   "terrain", "type_course", "nb_partants",
                   "numero_reunion", "numero_course", "allocation_totale"]

    path = os.path.join(BASE_DIR, "data_master", "courses_master.jsonl")
    if not os.path.exists(path):
        log.warning("  [ABSENT] %s", path)
        return index

    log.info("  Source: %s", path)
    for rec in stream_jsonl(path):
        cuid = rec.get("course_uid", "")
        if not cuid:
            continue
        entry = {}
        for f in keep_fields:
            if f in rec and rec[f] is not None and rec[f] != "":
                entry[f] = rec[f]
        if entry:
            index[cuid] = entry

    log.info("  Courses indexees: %d", len(index))
    if index:
        save_index(index, "course_index.json")
    return index


# ================================================================
# INDEX 3: jockey_index (depuis historique_jockeys)
# ================================================================

def build_jockey_index():
    """Construit l'index des jockeys a partir de historique_jockeys."""
    log.info("")
    log.info("--- Jockey Index (historique_jockeys) ---")

    index = {}
    keep_fields = ["nom", "nb_montes", "nb_victoires", "nb_places",
                   "taux_victoire", "taux_place", "gains_total_euros",
                   "premiere_course_date", "derniere_course_date"]

    # Essayer output/06_historique_jockeys/
    sources = [
        os.path.join(BASE_DIR, "output", "06_historique_jockeys", "historique_jockeys.json"),
        os.path.join(BASE_DIR, "output", "06_historique_jockeys", "historique_jockeys.jsonl"),
    ]

    loaded = False
    for path in sources:
        if not os.path.exists(path):
            continue
        log.info("  Source: %s", path)

        if path.endswith(".jsonl"):
            for rec in stream_jsonl(path):
                nom = (rec.get("nom") or rec.get("nom_jockey") or "").upper().strip()
                if not nom:
                    continue
                entry = {}
                for f in keep_fields:
                    if f in rec and rec[f] is not None and rec[f] != "":
                        entry[f] = rec[f]
                if entry:
                    index[nom] = entry
        else:
            data = load_json_safe(path)
            if data and isinstance(data, list):
                for rec in data:
                    nom = (rec.get("nom") or rec.get("nom_jockey") or "").upper().strip()
                    if not nom:
                        continue
                    entry = {}
                    for f in keep_fields:
                        if f in rec and rec[f] is not None and rec[f] != "":
                            entry[f] = rec[f]
                    if entry:
                        index[nom] = entry
                del data
        loaded = True
        break

    if not loaded:
        log.warning("  Aucune source trouvee pour historique_jockeys")

    log.info("  Jockeys indexes: %d", len(index))
    if index:
        save_index(index, "jockey_index.json")
    return index


# ================================================================
# INDEX 4: hippodrome_index (depuis courses_master)
# ================================================================

def build_hippodrome_index():
    """Construit l'index des hippodromes a partir de courses_master."""
    log.info("")
    log.info("--- Hippodrome Index (courses_master) ---")

    hippo_stats = {}

    path = os.path.join(BASE_DIR, "data_master", "courses_master.jsonl")
    if not os.path.exists(path):
        log.warning("  [ABSENT] %s", path)
        return {}

    log.info("  Source: %s", path)
    for rec in stream_jsonl(path):
        hippo = (rec.get("hippodrome_normalise") or rec.get("hippodrome") or "").upper().strip()
        if not hippo:
            continue

        if hippo not in hippo_stats:
            hippo_stats[hippo] = {
                "nom": hippo,
                "nb_courses": 0,
                "disciplines": set(),
                "distances": [],
                "dates": [],
            }

        hippo_stats[hippo]["nb_courses"] += 1

        disc = rec.get("discipline", "")
        if disc:
            hippo_stats[hippo]["disciplines"].add(disc)

        dist = rec.get("distance_metres")
        if dist and isinstance(dist, (int, float)):
            hippo_stats[hippo]["distances"].append(int(dist))

        date = rec.get("date_reunion_iso", "")
        if date:
            hippo_stats[hippo]["dates"].append(date)

    # Compiler les stats
    index = {}
    for hippo, stats in hippo_stats.items():
        entry = {
            "nom": stats["nom"],
            "nb_courses": stats["nb_courses"],
            "disciplines": sorted(stats["disciplines"]),
        }
        if stats["distances"]:
            entry["distance_min"] = min(stats["distances"])
            entry["distance_max"] = max(stats["distances"])
            entry["distance_moyenne"] = round(sum(stats["distances"]) / len(stats["distances"]))
        if stats["dates"]:
            entry["date_min"] = min(stats["dates"])
            entry["date_max"] = max(stats["dates"])
        index[hippo] = entry

    del hippo_stats

    log.info("  Hippodromes indexes: %d", len(index))
    if index:
        save_index(index, "hippodrome_index.json")
    return index


# ================================================================
# MAIN
# ================================================================

def main():
    t0 = time.time()

    log.info("=" * 70)
    log.info("REBUILD ALL INDEXES")
    log.info("  Output: %s", os.path.abspath(INDEX_DIR))
    log.info("=" * 70)

    results = {}

    # 1. Horse index
    horse_idx = build_horse_index()
    results["horse_index"] = len(horse_idx)
    del horse_idx

    # 2. Course index
    course_idx = build_course_index()
    results["course_index"] = len(course_idx)
    del course_idx

    # 3. Jockey index
    jockey_idx = build_jockey_index()
    results["jockey_index"] = len(jockey_idx)
    del jockey_idx

    # 4. Hippodrome index
    hippo_idx = build_hippodrome_index()
    results["hippodrome_index"] = len(hippo_idx)
    del hippo_idx

    elapsed = time.time() - t0

    log.info("")
    log.info("=" * 70)
    log.info("RESUME")
    log.info("=" * 70)
    for name, count in results.items():
        log.info("  %s: %d entrees", name, count)
    log.info("")
    log.info("Duree totale: %.1fs", elapsed)

    # Lister les fichiers generes
    log.info("")
    log.info("Fichiers generes:")
    for f in sorted(os.listdir(INDEX_DIR)):
        fp = os.path.join(INDEX_DIR, f)
        size = os.path.getsize(fp)
        log.info("  %s (%.1f MB)", fp, size / (1024 * 1024))

    log.info("=" * 70)
    log.info("TERMINE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
