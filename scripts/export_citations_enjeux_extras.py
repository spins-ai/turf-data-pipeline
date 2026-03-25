#!/usr/bin/env python3
"""
Export FULL citation/enjeux data from output/27_citations_enjeux/cache/ (177K files).

The existing citations_enjeux.jsonl only has course-level metadata.
Cache has per-horse, per-bet-type citation data including:
  - citations[].position: betting rank for this horse
  - citations[].enjeu: total stakes on this horse (in centimes)
  - citations[].ratio: ratio of stakes vs average
  - participants with favoris flag
  - risques (risk assessment per bet type)
  - updatetime (last update timestamp for live market data)

This is CRITICAL for market analysis, implied probability, and pool depth features.

Outputs:
  output/27_citations_enjeux/citations_par_cheval.jsonl  (horse-level betting positions)
  output/27_citations_enjeux/citations_resume.jsonl      (course-level summary with pool sizes)
"""
import json
import os
import sys
from datetime import datetime, timezone

BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "output", "27_citations_enjeux")
CACHE_DIR = os.path.join(BASE, "cache")

OUT_CHEVAUX = os.path.join(BASE, "citations_par_cheval.jsonl")
OUT_RESUME = os.path.join(BASE, "citations_resume.jsonl")


def extract_date_from_filename(fn):
    """01012014_R1_C1.json -> 2014-01-01."""
    parts = fn.replace(".json", "").split("_")
    if parts:
        ddmmyyyy = parts[0]
        if len(ddmmyyyy) == 8:
            try:
                return f"{ddmmyyyy[4:8]}-{ddmmyyyy[2:4]}-{ddmmyyyy[0:2]}"
            except (IndexError, ValueError):
                pass
    return None


def extract_rc(fn):
    parts = fn.replace(".json", "").split("_")
    num_r = num_c = None
    for p in parts:
        if p.startswith("R") and p[1:].isdigit():
            num_r = int(p[1:])
        elif p.startswith("C") and p[1:].isdigit():
            num_c = int(p[1:])
    return num_r, num_c


def ts_to_datetime(ts_ms):
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError):
        return None


def process_file(filepath, fn):
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return [], []

    if not isinstance(data, dict):
        return [], []

    date_str = extract_date_from_filename(fn)
    num_r, num_c = extract_rc(fn)
    course_uid = f"{date_str}_R{num_r}C{num_c}" if date_str else fn.replace(".json", "")

    cheval_rows = []
    resume_rows = []

    for cit in (data.get("listeCitations") or []):
        if not isinstance(cit, dict):
            continue

        type_pari = cit.get("typePari")
        num_positions = cit.get("numPositions", 0)
        num_positions_cons = cit.get("numPositionsConsolidees", 0)
        num_complements = cit.get("numComplements", 0)
        indisponible = cit.get("indisponible", False)

        # Course-level summary
        total_enjeu = 0
        nb_favoris = 0
        participants = cit.get("participants") or []

        for part in participants:
            if not isinstance(part, dict):
                continue

            num_pmu = part.get("numPmu")
            nom = part.get("nom")
            favoris = part.get("favoris", False)
            if favoris:
                nb_favoris += 1

            for citation in (part.get("citations") or []):
                if not isinstance(citation, dict):
                    continue

                enjeu = citation.get("enjeu", 0)
                total_enjeu += (enjeu or 0)

                cheval_rows.append({
                    "course_uid": course_uid,
                    "date": date_str,
                    "num_reunion": num_r,
                    "num_course": num_c,
                    "type_pari": type_pari,
                    "num_pmu": num_pmu,
                    "nom_cheval": nom,
                    "favoris": favoris,
                    "position": citation.get("position"),
                    "enjeu": enjeu,
                    "ratio": citation.get("ratio"),
                })

        resume_rows.append({
            "course_uid": course_uid,
            "date": date_str,
            "num_reunion": num_r,
            "num_course": num_c,
            "type_pari": type_pari,
            "indisponible": indisponible,
            "num_positions": num_positions,
            "num_positions_consolidees": num_positions_cons,
            "num_complements": num_complements,
            "nb_participants": len(participants),
            "nb_favoris": nb_favoris,
            "total_enjeu": total_enjeu,
        })

    return cheval_rows, resume_rows


def main():
    if not os.path.isdir(CACHE_DIR):
        print(f"ERROR: Cache dir not found: {CACHE_DIR}")
        sys.exit(1)

    files = sorted([f for f in os.listdir(CACHE_DIR) if f.endswith(".json")])
    print(f"Processing {len(files)} cache files from {CACHE_DIR}")

    n_chevaux = 0
    n_resume = 0

    with open(OUT_CHEVAUX, "w", encoding="utf-8") as fc, \
         open(OUT_RESUME, "w", encoding="utf-8") as fr:

        for i, fn in enumerate(files):
            if i % 25000 == 0 and i > 0:
                print(f"  ...{i}/{len(files)} files processed")

            filepath = os.path.join(CACHE_DIR, fn)
            cheval_rows, resume_rows = process_file(filepath, fn)

            for r in cheval_rows:
                fc.write(json.dumps(r, ensure_ascii=False) + "\n")
                n_chevaux += 1

            for r in resume_rows:
                fr.write(json.dumps(r, ensure_ascii=False) + "\n")
                n_resume += 1

    print(f"\nDone!")
    print(f"  {OUT_CHEVAUX}: {n_chevaux} horse-level rows")
    print(f"  {OUT_RESUME}: {n_resume} course-level rows")


if __name__ == "__main__":
    main()
