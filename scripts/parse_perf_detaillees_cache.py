#!/usr/bin/env python3
"""
Parse 22_performances_detaillees cache (97K files).

Reads JSON files from output/22_performances_detaillees/cache/
Extracts per-participant: tempsDuPremier, reductionKilometrique, distanceAvecPrecedent
from the nested coursesCourues.participants structure.

Outputs to output/22_performances_detaillees/perf_detaillees_enriched.jsonl

Usage:
    python scripts/parse_perf_detaillees_cache.py              # full run
    python scripts/parse_perf_detaillees_cache.py --sample 100 # sample first 100 files
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "output" / "22_performances_detaillees" / "cache"
OUTPUT_FILE = ROOT / "output" / "22_performances_detaillees" / "perf_detaillees_enriched.jsonl"


def parse_filename(filename: str):
    """Extract date, reunion, course from filename like 01012020_R1_C1.json."""
    base = filename.replace(".json", "")
    parts = base.split("_")
    if len(parts) != 3:
        return None, None, None
    date_str = parts[0]  # DDMMYYYY
    reunion = parts[1]   # R1
    course = parts[2]    # C1
    # Convert DDMMYYYY to ISO
    if len(date_str) == 8:
        iso_date = f"{date_str[4:8]}-{date_str[2:4]}-{date_str[0:2]}"
    else:
        iso_date = date_str
    return iso_date, reunion, course


def process_file(filepath: Path) -> list:
    """Process a single cache file and return enriched records."""
    records = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return records

    if not isinstance(data, dict):
        return records

    iso_date, reunion, course = parse_filename(filepath.name)
    if iso_date is None:
        return records

    allure = data.get("allure")
    participants = data.get("participants", [])
    if not isinstance(participants, list):
        return records

    for participant in participants:
        if not isinstance(participant, dict):
            continue

        num_pmu = participant.get("numPmu")
        nom_cheval = participant.get("nomCheval")
        courses_courues = participant.get("coursesCourues", [])

        if not isinstance(courses_courues, list):
            continue

        for idx, course_courue in enumerate(courses_courues):
            if not isinstance(course_courue, dict):
                continue

            # Top-level course info
            cc_date = course_courue.get("date")
            cc_hippo = course_courue.get("hippodrome")
            cc_nom_prix = course_courue.get("nomPrix")
            cc_discipline = course_courue.get("discipline")
            cc_distance = course_courue.get("distance")
            cc_nb_participants = course_courue.get("nbParticipants")
            cc_temps_premier = course_courue.get("tempsDuPremier")
            cc_allocation = course_courue.get("allocation")

            # Per-participant details within course_courue
            sub_participants = course_courue.get("participants", [])
            if not isinstance(sub_participants, list):
                continue

            for sub_p in sub_participants:
                if not isinstance(sub_p, dict):
                    continue

                # Only keep the record for the participant itself (itsHim=True)
                is_him = sub_p.get("itsHim", False)
                if not is_him:
                    continue

                record = {
                    "source_file": filepath.name,
                    "date_course": iso_date,
                    "reunion": reunion,
                    "course": course,
                    "allure": allure,
                    "numPmu": num_pmu,
                    "nomCheval": nom_cheval,
                    "perf_index": idx,
                    "perf_date": cc_date,
                    "perf_hippodrome": cc_hippo,
                    "perf_nomPrix": cc_nom_prix,
                    "perf_discipline": cc_discipline,
                    "perf_distance": cc_distance,
                    "perf_nbParticipants": cc_nb_participants,
                    "perf_tempsDuPremier": cc_temps_premier,
                    "perf_allocation": cc_allocation,
                    # Target fields
                    "tempsDuPremier": cc_temps_premier,
                    "reductionKilometrique": sub_p.get("reductionKilometrique"),
                    "distanceAvecPrecedent": sub_p.get("distanceAvecPrecedent"),
                    # Bonus fields from nested participant
                    "place": sub_p.get("place"),
                    "nomJockey": sub_p.get("nomJockey"),
                    "poidsJockey": sub_p.get("poidsJockey"),
                    "corde": sub_p.get("corde"),
                    "distanceParcourue": sub_p.get("distanceParcourue"),
                    "oeillere": sub_p.get("oeillere"),
                }
                records.append(record)

    return records


def main():
    parser = argparse.ArgumentParser(description="Parse 22_performances_detaillees cache")
    parser.add_argument("--sample", type=int, default=0, help="Process only first N files (0=all)")
    args = parser.parse_args()

    if not CACHE_DIR.exists():
        print(f"ERROR: Cache directory not found: {CACHE_DIR}")
        sys.exit(1)

    files = sorted([f for f in CACHE_DIR.iterdir() if f.suffix == ".json"])
    total_files = len(files)
    print(f"Found {total_files} JSON files in cache")

    if args.sample > 0:
        files = files[: args.sample]
        print(f"Sample mode: processing first {len(files)} files")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    total_records = 0
    errors = 0
    non_null_rk = 0
    non_null_dap = 0
    non_null_tdp = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        for i, filepath in enumerate(files):
            try:
                records = process_file(filepath)
                for rec in records:
                    out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    total_records += 1
                    if rec.get("reductionKilometrique") is not None:
                        non_null_rk += 1
                    if rec.get("distanceAvecPrecedent") is not None:
                        non_null_dap += 1
                    if rec.get("tempsDuPremier") is not None:
                        non_null_tdp += 1
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  ERROR on {filepath.name}: {e}")

            if (i + 1) % 5000 == 0 or (i + 1) == len(files):
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                print(
                    f"  [{i+1}/{len(files)}] {total_records:,} records | "
                    f"{rate:.0f} files/s | {elapsed:.1f}s | errors={errors}"
                )

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Files processed: {len(files)}")
    print(f"Total records: {total_records:,}")
    print(f"Errors: {errors}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Output size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"\nField coverage:")
    print(f"  tempsDuPremier:        {non_null_tdp:,} / {total_records:,} ({100*non_null_tdp/max(total_records,1):.1f}%)")
    print(f"  reductionKilometrique: {non_null_rk:,} / {total_records:,} ({100*non_null_rk/max(total_records,1):.1f}%)")
    print(f"  distanceAvecPrecedent: {non_null_dap:,} / {total_records:,} ({100*non_null_dap/max(total_records,1):.1f}%)")


if __name__ == "__main__":
    main()
