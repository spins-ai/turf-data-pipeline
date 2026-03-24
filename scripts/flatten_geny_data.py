#!/usr/bin/env python3
"""
scripts/flatten_geny_data.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Flatten nested geny_data.jsonl (day-level records with reunions/courses)
into horse-level flat records in geny_flat.jsonl.

Each input line is a day record:
  { "date": "...", "reunions": [...], "courses": [...], "pronostics_raw": [...] }

The ``courses`` array already contains per-horse rows with French headers.
This script:
  1. Reads every day record.
  2. Filters out empty/blank horse rows.
  3. Maps French column headers to standard field names.
  4. Propagates the parent date into every row.
  5. Writes one JSONL line per horse to geny_flat.jsonl.
"""

from __future__ import annotations

import json
import os
import sys

# ---------------------------------------------------------------------------
# Column mapping: French scraper headers -> standard fields
# ---------------------------------------------------------------------------

COLUMN_MAP = {
    "N°": "numero",
    "Cheval": "nom_cheval",
    "SA": "sexe_age",
    "Poids": "poids",
    "Déch.": "decharge",
    "Jockey": "jockey",
    "Entraîneur": "entraineur",
    "Musique": "musique",
    "Valeur": "valeur",
    "Cotes références": "cote_ref",
    "Dernières cotes": "derniere_cote",
    "col_11": "col_11",
    "col_12": "col_12",
    "col_13": "col_13",
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    inpath = os.path.join(base, "output", "26_geny", "geny_data.jsonl")
    outpath = os.path.join(base, "output", "26_geny", "geny_flat.jsonl")

    if not os.path.isfile(inpath):
        print(f"ERROR: Input not found: {inpath}", file=sys.stderr)
        sys.exit(1)

    total_days = 0
    total_horses = 0
    skipped_empty = 0

    with open(inpath, "r", encoding="utf-8") as fin, \
         open(outpath, "w", encoding="utf-8") as fout:

        for line_no, raw in enumerate(fin, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                day = json.loads(raw)
            except json.JSONDecodeError as exc:
                print(f"WARN: Bad JSON line {line_no}: {exc}", file=sys.stderr)
                continue

            total_days += 1
            parent_date = str(day.get("date", ""))[:10]

            courses = day.get("courses", [])
            for entry in courses:
                cheval = (entry.get("Cheval") or "").strip()

                # Skip rows without a horse name (blank filler rows)
                if not cheval:
                    skipped_empty += 1
                    continue

                flat: dict = {"date": parent_date, "source": "geny"}

                # Map French headers to standard names
                for fr_key, std_key in COLUMN_MAP.items():
                    val = entry.get(fr_key)
                    if val is not None:
                        flat[std_key] = val

                # Keep any extra keys not in the map (future-proof)
                mapped_fr_keys = set(COLUMN_MAP.keys()) | {"date", "source"}
                for k, v in entry.items():
                    if k not in mapped_fr_keys:
                        flat[k] = v

                total_horses += 1
                fout.write(json.dumps(flat, ensure_ascii=False) + "\n")

    print(f"Done. {total_days} day records -> {total_horses} horse rows "
          f"(skipped {skipped_empty} empty). Output: {outpath}")


if __name__ == "__main__":
    main()
