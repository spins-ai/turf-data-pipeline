#!/usr/bin/env python3
"""
Export FULL rapport details from output/21_rapports_definitifs/cache/ (200K files).

The existing rapports_definitifs.jsonl extracts only top-level rapport values.
Cache has per-combinaison details including:
  - dividendePourUnEuro (exact per-euro dividend)
  - nombreGagnants (number of winners per bet type)
  - combinaison details (exact horse numbers)
  - famillePari grouping
  - audience (NATIONAL/REGIONAL)
  - miseBase per bet type

This is critical for calculating pool sizes, bet popularity, and value detection.

Output:
  output/21_rapports_definitifs/rapports_detail_complet.jsonl
"""
import json
import os
import sys
from datetime import datetime, timezone

BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "output", "21_rapports_definitifs")
CACHE_DIR = os.path.join(BASE, "cache")

OUT_FILE = os.path.join(BASE, "rapports_detail_complet.jsonl")


def extract_date_from_filename(fn):
    """Extract date from filename like 01012014_R1_C1.json -> 2014-01-01."""
    parts = fn.replace(".json", "").split("_")
    if len(parts) >= 3:
        ddmmyyyy = parts[0]
        if len(ddmmyyyy) == 8:
            try:
                return f"{ddmmyyyy[4:8]}-{ddmmyyyy[2:4]}-{ddmmyyyy[0:2]}"
            except (IndexError, ValueError):
                pass
    return None


def extract_rc_from_filename(fn):
    """Extract R and C numbers from filename."""
    parts = fn.replace(".json", "").split("_")
    num_r = num_c = None
    for p in parts:
        if p.startswith("R") and p[1:].isdigit():
            num_r = int(p[1:])
        elif p.startswith("C") and p[1:].isdigit():
            num_c = int(p[1:])
    return num_r, num_c


def process_file(filepath, fn):
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return []

    if not isinstance(data, list):
        return []

    date_str = extract_date_from_filename(fn)
    num_r, num_c = extract_rc_from_filename(fn)
    course_uid = f"{date_str}_R{num_r}C{num_c}" if date_str else fn.replace(".json", "")

    rows = []
    for pari in data:
        if not isinstance(pari, dict):
            continue

        type_pari = pari.get("typePari")
        famille = pari.get("famillePari")
        mise_base = pari.get("miseBase")
        audience = pari.get("audience")
        rembourse = pari.get("rembourse")
        dividende_unite = pari.get("dividendeUnite")

        for rapport in (pari.get("rapports") or []):
            if not isinstance(rapport, dict):
                continue

            rows.append({
                "course_uid": course_uid,
                "date": date_str,
                "num_reunion": num_r,
                "num_course": num_c,
                "type_pari": type_pari,
                "famille_pari": famille,
                "mise_base": mise_base,
                "audience": audience,
                "rembourse": rembourse,
                "dividende_unite": dividende_unite,
                "libelle": rapport.get("libelle"),
                "dividende": rapport.get("dividende"),
                "dividende_pour_un_euro": rapport.get("dividendePourUnEuro"),
                "dividende_pour_mise_base": rapport.get("dividendePourUneMiseDeBase"),
                "combinaison": rapport.get("combinaison"),
                "nombre_gagnants": rapport.get("nombreGagnants"),
            })

    return rows


def main():
    if not os.path.isdir(CACHE_DIR):
        print(f"ERROR: Cache dir not found: {CACHE_DIR}")
        sys.exit(1)

    files = sorted([f for f in os.listdir(CACHE_DIR) if f.endswith(".json")])
    print(f"Processing {len(files)} cache files from {CACHE_DIR}")

    n_rows = 0
    with open(OUT_FILE, "w", encoding="utf-8") as fo:
        for i, fn in enumerate(files):
            if i % 25000 == 0 and i > 0:
                print(f"  ...{i}/{len(files)} files processed")

            filepath = os.path.join(CACHE_DIR, fn)
            rows = process_file(filepath, fn)
            for r in rows:
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                n_rows += 1

    print(f"\nDone!")
    print(f"  {OUT_FILE}: {n_rows} rows")


if __name__ == "__main__":
    main()
