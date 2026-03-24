#!/usr/bin/env python3
"""
scripts/flatten_turfostats_data.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Consolidate TurfoStats JSONL files into a single flat file.

The scraper hit a cookie wall, so turfostats_courses.jsonl has 33K records
with empty ``partants`` arrays and "Cookies" as titre.  However,
turfostats_programmes.jsonl has useful course-level metadata (id_course,
nom_prix, date, url).

This script:
  1. Reads turfostats_programmes.jsonl (course metadata with dates).
  2. Reads turfostats_courses.jsonl and extracts any non-empty partants.
  3. Joins on id_course to produce the richest possible flat record.
  4. Outputs one JSONL line per course to turfostats_flat.jsonl.
"""

from __future__ import annotations

import json
import os
import sys

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ts_dir = os.path.join(base, "output", "25_turfostats")
    outpath = os.path.join(ts_dir, "turfostats_flat.jsonl")

    programmes_path = os.path.join(ts_dir, "turfostats_programmes.jsonl")
    courses_path = os.path.join(ts_dir, "turfostats_courses.jsonl")

    # ------------------------------------------------------------------
    # 1. Load programmes index  (id_course -> metadata)
    # ------------------------------------------------------------------
    prog_idx: dict[str, dict] = {}
    prog_count = 0
    if os.path.isfile(programmes_path):
        with open(programmes_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                cid = str(rec.get("id_course", ""))
                if cid:
                    prog_idx[cid] = rec
                    prog_count += 1
        print(f"Loaded {prog_count} programme records.")
    else:
        print(f"WARN: {programmes_path} not found, continuing without.", file=sys.stderr)

    # ------------------------------------------------------------------
    # 2. Load courses and merge
    # ------------------------------------------------------------------
    courses_count = 0
    partants_found = 0
    courses_idx: dict[str, dict] = {}

    if os.path.isfile(courses_path):
        with open(courses_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                cid = str(rec.get("id_course", ""))
                courses_count += 1
                partants = rec.get("partants", [])
                if partants:
                    partants_found += len(partants)
                courses_idx[cid] = rec
        print(f"Loaded {courses_count} course records, "
              f"{partants_found} total partants found.")
    else:
        print(f"WARN: {courses_path} not found.", file=sys.stderr)

    # ------------------------------------------------------------------
    # 3. Merge and write flat output
    # ------------------------------------------------------------------
    # Use programmes as the primary source since it has dates;
    # fall back to courses-only for ids not in programmes.
    all_ids = set(prog_idx.keys()) | set(courses_idx.keys())
    written = 0
    written_with_partants = 0

    with open(outpath, "w", encoding="utf-8") as fout:
        for cid in sorted(all_ids):
            prog = prog_idx.get(cid, {})
            course = courses_idx.get(cid, {})

            partants = course.get("partants", [])
            titre = course.get("titre", "")
            # Filter out cookie-wall placeholder titles
            if titre and titre.lower().strip() == "cookies":
                titre = ""

            if partants:
                # Expand to horse-level rows
                for p in partants:
                    flat = {
                        "id_course": cid,
                        "date": prog.get("date", ""),
                        "nom_prix": prog.get("nom_prix", ""),
                        "url": prog.get("url", ""),
                        "source": "turfostats",
                    }
                    flat.update(p)
                    fout.write(json.dumps(flat, ensure_ascii=False) + "\n")
                    written += 1
                    written_with_partants += 1
            else:
                # No horse data -- output course-level metadata
                flat = {
                    "id_course": cid,
                    "date": prog.get("date", ""),
                    "nom_prix": prog.get("nom_prix", ""),
                    "url": prog.get("url", ""),
                    "titre": titre,
                    "source": "turfostats",
                    "has_partants": False,
                }
                fout.write(json.dumps(flat, ensure_ascii=False) + "\n")
                written += 1

    print(f"Done. {written} flat records written "
          f"({written_with_partants} with partant data). Output: {outpath}")


if __name__ == "__main__":
    main()
