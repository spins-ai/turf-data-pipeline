#!/usr/bin/env python3
"""
scripts/merge_2013_historical.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge 2013-2016 historical participant data from partants_2013.jsonl (~627K
records) into the partants master (partants_normalises.jsonl).

This dataset covers 2013-02-18 to 2016-03-10 and may fill gaps in early years
where the main pipeline has missing fields.

Strategy:
  - Build a lookup from (date, numReunion, numCourse, numPmu) -> record
  - Stream the master; for each partant matching a 2013 record, backfill
    any null/empty fields from the historical data.
  - Field name mapping: the 2013 file uses slightly different names than
    the master (e.g. ``nom`` vs ``nom_cheval``, ``driver`` vs
    ``jockey_driver``).

Memory: the 627K lookup is kept in RAM as a dict of dicts (~1.5 GB max).
"""

from __future__ import annotations

import json
import os
import sys
import time

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST_PATH = os.path.join(BASE, "output", "02b_liste_courses_2013", "partants_2013.jsonl")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_2013.jsonl")

# ---------------------------------------------------------------------------
# Field mapping: 2013 field -> master field
# Only map fields that exist in the master schema.
# ---------------------------------------------------------------------------

FIELD_MAP: dict[str, str] = {
    "nom":                    "nom_cheval",
    "driver":                 "jockey_driver",
    "driver_change":          "jockey_driver_change",
    "nom_pere":               "pere",
    "nom_mere":               "mere",
    "nombre_courses":         "nb_courses_carriere",
    "nombre_victoires":       "nb_victoires_carriere",
    "nombre_places":          "nb_places_carriere",
    "nombre_places_second":   "nb_places_2eme",
    "nombre_places_troisieme":"nb_places_3eme",
    "gains_carriere":         "gains_carriere_euros",
    "gains_annee_en_cours":   "gains_annee_euros",
    "indicateur_inedit":      "is_inedit",
    "ordre_arrivee":          "position_arrivee",
    "temps_obtenu":           "temps_ms",
    "reduction_km":           "reduction_km_ms",
    "handicap_distance":      "handicap_distance_m",
    "poids_condition_monte":  "poids_porte_kg",
    "hippodrome":             "hippodrome_normalise",
}

# Fields that map 1:1 (same name in both)
DIRECT_FIELDS = [
    "age", "sexe", "race", "statut", "musique", "entraineur",
    "proprietaire", "eleveur", "oeilleres", "deferre", "place_corde",
    "allure", "avis_entraineur",
]

# Fields tracked for fill-rate reporting
TRACK_FIELDS = [
    "nom_cheval", "jockey_driver", "pere", "mere",
    "nb_courses_carriere", "nb_victoires_carriere", "nb_places_carriere",
    "gains_carriere_euros", "gains_annee_euros",
    "position_arrivee", "temps_ms", "reduction_km_ms",
    "hippodrome_normalise", "eleveur", "musique",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_empty(val) -> bool:
    """True if the value is considered missing/empty."""
    if val is None:
        return True
    if isinstance(val, str) and val.strip() in ("", "null", "None"):
        return True
    return False


def _remap_record(raw: dict) -> dict:
    """Convert a 2013-format record to master field names."""
    out: dict = {}
    for old_key, new_key in FIELD_MAP.items():
        val = raw.get(old_key)
        if not _is_empty(val):
            out[new_key] = val

    for field in DIRECT_FIELDS:
        val = raw.get(field)
        if not _is_empty(val):
            out[field] = val

    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---------------------------------------------------
    for label, path in [("Historical", HIST_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: build lookup from 2013 data ------------------------------
    print(f"[1/3] Loading 2013 historical data from {HIST_PATH} ...")

    # Key: (date, reunion, course, num_pmu) -> remapped dict
    hist_lookup: dict[tuple, dict] = {}
    total_hist = 0
    indexed = 0

    with open(HIST_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_hist += 1
            date = str(rec.get("date_reunion_iso", ""))[:10]
            num_reunion = rec.get("numero_reunion")
            num_course = rec.get("numero_course")
            num_pmu = rec.get("num_pmu")

            if not date or num_reunion is None or num_course is None or num_pmu is None:
                continue

            key = (date, int(num_reunion), int(num_course), int(num_pmu))
            remapped = _remap_record(rec)
            if remapped:
                hist_lookup[key] = remapped
                indexed += 1

    print(f"       {total_hist:,} historical records -> {indexed:,} indexed")

    # --- Phase 2: compute before fill rates --------------------------------
    print(f"[2/3] Computing pre-merge fill rates ...")

    total_master = 0
    before_counts: dict[str, int] = {f: 0 for f in TRACK_FIELDS}

    with open(MASTER_IN, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                total_master += 1
                continue
            total_master += 1
            for field in TRACK_FIELDS:
                if not _is_empty(rec.get(field)):
                    before_counts[field] += 1

    print(f"       Master has {total_master:,} rows")

    # --- Phase 3: stream master, backfill, write out -----------------------
    print(f"[3/3] Streaming master -> enriched output ...")

    total = 0
    matched = 0
    fields_filled = 0
    after_counts: dict[str, int] = {f: 0 for f in TRACK_FIELDS}

    with open(MASTER_IN, "r", encoding="utf-8") as fin, \
         open(MASTER_OUT, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                fout.write(line + "\n")
                total += 1
                continue

            total += 1

            date_iso = str(rec.get("date_reunion_iso", ""))[:10]
            num_reunion = rec.get("numero_reunion")
            num_course = rec.get("numero_course")
            num_pmu = rec.get("num_pmu")

            if date_iso and num_reunion is not None and num_course is not None and num_pmu is not None:
                key = (date_iso, int(num_reunion), int(num_course), int(num_pmu))
                hist = hist_lookup.get(key)
                if hist:
                    did_fill = False
                    for field, val in hist.items():
                        if _is_empty(rec.get(field)):
                            rec[field] = val
                            fields_filled += 1
                            did_fill = True
                    if did_fill:
                        matched += 1

            # Count final fill rates
            for field in TRACK_FIELDS:
                if not _is_empty(rec.get(field)):
                    after_counts[field] += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s.")
    print(f"  Total partants:      {total:,}")
    print(f"  Matched from 2013:   {matched:,} ({matched/total*100:.1f}%)")
    print(f"  Fields backfilled:   {fields_filled:,}")
    print(f"\n  Fill rate changes (tracked fields):")
    print(f"  {'Field':<25} {'Before':>10} {'After':>10} {'Gain':>10}")
    print(f"  {'-'*55}")
    for field in TRACK_FIELDS:
        b = before_counts[field]
        a = after_counts[field]
        bp = b / total * 100 if total else 0
        ap = a / total * 100 if total else 0
        gain = ap - bp
        marker = " *" if gain > 0.01 else ""
        print(f"  {field:<25} {bp:>9.1f}% {ap:>9.1f}% {gain:>+9.1f}%{marker}")

    print(f"\n  Output: {MASTER_OUT}")


if __name__ == "__main__":
    main()
