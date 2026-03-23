#!/usr/bin/env python3
"""
deduplication.py — Étape 3.3 du TODO
======================================
Déduplication des données entre sources.

Opérations :
  1. Dédupliquer courses (02 + 02b) par course_uid
  2. Dédupliquer partants par partant_uid
  3. Dédupliquer pedigrees (08+12+14+36) par nom_cheval
  4. Dédupliquer rapports (21+38) par course_uid
  5. Garder la version la plus complète en cas de doublon

Compatible Windows + Mac — utilise os.path partout.

Input : fichiers JSONL/JSON dans output/
Output : output/dedup/ + rapports

Usage :
    python3 deduplication.py
"""

import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

OUTPUT_DIR = os.path.join("output", "dedup")
os.makedirs(OUTPUT_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("deduplication")


def count_non_null(record):
    """Compte les champs non-null d'un record (mesure de complétude)."""
    if not isinstance(record, dict):
        return 0
    return sum(1 for v in record.values() if v is not None and v != "" and v != [])


def stream_records(path):
    """Itère sur les records d'un fichier JSON ou JSONL."""
    if not os.path.exists(path):
        return

    if path.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    elif path.endswith(".json"):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for r in data:
                    yield r
            del data
        except (json.JSONDecodeError, MemoryError) as e:
            log.warning(f"Erreur lecture {path}: {e}")


def dedup_by_key(paths, key_field, output_name, description):
    """Déduplique des records provenant de plusieurs fichiers par un champ clé.

    Garde la version la plus complète (plus de champs non-null).
    """
    log.info(f"Déduplication {description}...")

    seen = {}  # key -> (record, completeness)
    total_input = 0

    for path in paths:
        if not os.path.exists(path):
            log.info(f"  [ABSENT] {path}")
            continue
        log.info(f"  Lecture: {path}")
        count = 0
        for record in stream_records(path):
            total_input += 1
            count += 1
            key = record.get(key_field, "")
            if not key:
                continue

            completeness = count_non_null(record)

            if key not in seen or completeness > seen[key][1]:
                seen[key] = (record, completeness)

        log.info(f"    → {count} records")

    # Écrire les résultats dédupliqués
    output_path = os.path.join(OUTPUT_DIR, output_name)
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        for key in sorted(seen.keys()):
            record, _ = seen[key]
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    nb_dedup = len(seen)
    nb_removed = total_input - nb_dedup
    log.info(f"  Résultat: {total_input} → {nb_dedup} (supprimé {nb_removed} doublons)")
    log.info(f"  Output: {output_path}")

    return {
        "input": total_input,
        "output": nb_dedup,
        "removed": nb_removed,
    }


def main():
    log.info("=" * 70)
    log.info("DÉDUPLICATION — Étape 3.3")
    log.info("=" * 70)

    rapport = {}

    # 1. Courses (02 + 02b)
    rapport["courses"] = dedup_by_key(
        paths=[
            os.path.join("output", "02_liste_courses", "courses_normalisees.jsonl"),
            os.path.join("output", "02_liste_courses", "courses_normalisees.json"),
            os.path.join("output", "02b_liste_courses_2013", "courses_normalisees.json"),
        ],
        key_field="course_uid",
        output_name="courses_dedup.jsonl",
        description="courses (02 + 02b)",
    )

    # 2. Partants (02 + 02b)
    rapport["partants"] = dedup_by_key(
        paths=[
            os.path.join("output", "02_liste_courses", "partants_normalises.jsonl"),
            os.path.join("output", "02_liste_courses", "partants_normalises.json"),
            os.path.join("output", "02b_liste_courses_2013", "partants_normalises.json"),
        ],
        key_field="partant_uid",
        output_name="partants_dedup.jsonl",
        description="partants (02 + 02b)",
    )

    # 3. Pedigrees (08 + 12 + 14 + 36)
    # Les pedigrees n'ont pas de partant_uid, on déduplique par nom_cheval
    rapport["pedigrees"] = dedup_by_key(
        paths=[
            os.path.join("output", "08_pedigree", "pedigree.json"),
            os.path.join("output", "14_pedigree", "pedigrees_pq.jsonl"),
            os.path.join("output", "14_pedigree", "pedigrees_pq.json"),
            os.path.join("output", "36_pedigree_query", "pedigree_query.json"),
            os.path.join("data_master", "pedigree_master.json"),
        ],
        key_field="nom_cheval",
        output_name="pedigrees_dedup.jsonl",
        description="pedigrees (08 + 14 + 36 + master)",
    )

    # 4. Rapports (21 + 38)
    rapport["rapports"] = dedup_by_key(
        paths=[
            os.path.join("output", "21_rapports_definitifs", "rapports_definitifs.jsonl"),
            os.path.join("output", "38_rapports_internet", "rapports_internet.jsonl"),
            os.path.join("data_master", "rapports_master.json"),
        ],
        key_field="course_uid",
        output_name="rapports_dedup.jsonl",
        description="rapports (21 + 38 + master)",
    )

    # Sauver rapport
    rapport_path = os.path.join(OUTPUT_DIR, "dedup_rapport.json")
    with open(rapport_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    log.info("")
    log.info("=" * 70)
    log.info("RÉSUMÉ DÉDUPLICATION:")
    for name, stats in rapport.items():
        log.info(f"  {name}: {stats['input']} → {stats['output']} (−{stats['removed']})")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
