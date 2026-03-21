#!/usr/bin/env python3
"""
merge_02_02b_courses_master.py — Étape 5.1 du TODO
====================================================
Merger 02 + 02b → courses_master.jsonl

Fusionne les courses PMU (02) et Le Trot 2004-2013 (02b) en un fichier maître.
Déduplique par course_uid, garde la version la plus complète.

Compatible Windows + Mac.

Input :
  - output/02_liste_courses/courses_normalisees.jsonl (ou .json)
  - output/02b_liste_courses_2013/courses_normalisees.json

Output : data_master/courses_master.jsonl

Usage :
    python3 merge_02_02b_courses_master.py
"""

import json
import logging
import os
import sys

OUTPUT_DIR = "data_master"
os.makedirs(OUTPUT_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("merge_02_02b_courses_master")


def count_non_null(record):
    if not isinstance(record, dict):
        return 0
    return sum(1 for v in record.values() if v is not None and v != "" and v != [])


def main():
    log.info("=" * 60)
    log.info("MERGE 02 + 02b → courses_master.jsonl")
    log.info("=" * 60)

    paths = [
        os.path.join("output", "02_liste_courses", "courses_normalisees.jsonl"),
        os.path.join("output", "02_liste_courses", "courses_normalisees.json"),
        os.path.join("output", "02b_liste_courses_2013", "courses_normalisees.json"),
    ]

    seen = {}  # course_uid -> (record, completeness)
    total_input = 0

    for path in paths:
        if not os.path.exists(path):
            log.info(f"  [ABSENT] {path}")
            continue

        log.info(f"  Lecture: {path}")
        count = 0

        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    total_input += 1
                    count += 1
                    uid = r.get("course_uid", "")
                    if uid:
                        comp = count_non_null(r)
                        if uid not in seen or comp > seen[uid][1]:
                            seen[uid] = (r, comp)
        else:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for r in data:
                total_input += 1
                count += 1
                uid = r.get("course_uid", "")
                if uid:
                    comp = count_non_null(r)
                    if uid not in seen or comp > seen[uid][1]:
                        seen[uid] = (r, comp)
            del data

        log.info(f"    → {count} records")

    # Trier par date + numéro
    sorted_records = sorted(
        seen.values(),
        key=lambda x: (x[0].get("date_reunion_iso", ""), x[0].get("numero_reunion", 0), x[0].get("numero_course", 0))
    )

    output_path = os.path.join(OUTPUT_DIR, "courses_master.jsonl")
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        for record, _ in sorted_records:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    nb = len(sorted_records)
    removed = total_input - nb
    log.info(f"Résultat: {total_input} → {nb} courses uniques (−{removed} doublons)")
    log.info(f"Output: {output_path}")

    # Dates couvertes
    dates = sorted(set(r[0].get("date_reunion_iso", "")[:4] for r in sorted_records if r[0].get("date_reunion_iso")))
    log.info(f"Années couvertes: {', '.join(dates)}")

    log.info("TERMINÉ")


if __name__ == "__main__":
    main()
