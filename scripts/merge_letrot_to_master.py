#!/usr/bin/env python3
"""
scripts/merge_letrot_to_master.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge Le Trot data (1.4M records) from letrot_data.jsonl into the
partants master (partants_normalises.jsonl).

Le Trot records contain two types:
  - type="course_info": course-level metadata (titre, url)
  - type="partant": individual horse data with race results

Partant records carry:
  - date, hippodrome_id, numero_course
  - rang (finishing position), n (horse number)
  - chevalcrack_series_au_partant (horse name + series)
  - fer (shoeing), sa (sex+age), driverentraineur
  - dist., temps, red.<br>km, alloc._(euro), rap._prob.

Merge strategy:
  - Build a lookup keyed by (date, hippodrome_normalise, numero_course)
    containing all partants for that race.
  - For each master partant, match by (date, hippodrome, course) then
    by horse number (num_pmu == n).
  - Fallback: match by horse name similarity.

Fields added to each matching partant:
  - letrot_rang          : finishing position from Le Trot
  - letrot_temps         : race time string
  - letrot_reduction_km  : reduction au km string
  - letrot_allocation    : allocation euros
  - letrot_rapport_prob  : probable odds from Le Trot
  - letrot_fer           : shoeing info (D4, etc.)
  - letrot_avis          : trainer opinion from Le Trot
  - letrot_driver        : driver/trainer from Le Trot
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LETROT_PATH = os.path.join(BASE, "output", "83_letrot", "letrot_data.jsonl")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_letrot.jsonl")

# ---------------------------------------------------------------------------
# Hippodrome ID -> normalised name mapping (Le Trot uses numeric IDs)
# Common Le Trot hippodrome IDs
# ---------------------------------------------------------------------------

_LETROT_HIPPO_MAP: dict[str, str] = {
    "5307": "vincennes",
    "5214": "enghien",
    "5217": "cabourg",
    "5208": "caen",
    "5102": "lyon la soie",
    "5103": "lyon parilly",
    "5405": "laval",
    "5301": "vichy",
    "5308": "maisons laffitte",
    "5201": "chantilly",
    "5402": "nantes",
    "5211": "deauville",
    "5215": "lisieux",
    "5404": "angers",
    "5303": "bordeaux le bouscat",
    "5501": "toulouse",
    "5503": "agen",
    "5206": "rouen mauquenchy",
    "5207": "evreux navarre",
    "5209": "argentan",
    "5210": "graignes",
    "5212": "meslay du maine",
    "5213": "bihorel",
    "5306": "pontchateau",
    "5401": "cholet",
    "5403": "les sables d olonne",
    "5502": "tarbes",
    "5504": "mont de marsan",
    "5505": "pau",
    "5101": "lyon",
    "5302": "cagnes sur mer",
    "5304": "marseille borely",
    "5305": "salon de provence",
}


def _normalise_hippo(name: str) -> str:
    """Lowercase, strip, collapse spaces, remove dashes."""
    return re.sub(r"\s+", " ", name.strip().lower().replace("-", " "))


def _normalise_name(name: str) -> str:
    """Normalise horse name for fuzzy matching."""
    if not name:
        return ""
    # Remove digits at end (series number), uppercase, strip
    cleaned = re.sub(r"\d+$", "", name).strip().upper()
    # Remove non-alpha
    cleaned = re.sub(r"[^A-Z ]", "", cleaned).strip()
    return re.sub(r"\s+", " ", cleaned)


def _parse_allocation(val: str) -> float | None:
    """Parse allocation string like '9 000 €' -> 9000.0"""
    if not val:
        return None
    cleaned = re.sub(r"[^\d]", "", val)
    if cleaned:
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _parse_rapport(val: str) -> float | None:
    """Parse rapport probable like '2.5' -> 2.5"""
    if not val:
        return None
    try:
        return float(val.replace(",", "."))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---
    for label, path in [("Le Trot", LETROT_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: build lookup from Le Trot partant records ---
    print(f"[1/2] Streaming Le Trot data from {LETROT_PATH} ...")

    # Key: (date, hippo_norm, course_num) -> list of partant dicts
    race_partants: dict[str, list[dict]] = defaultdict(list)
    total_lt = 0
    partant_lt = 0
    course_lt = 0

    with open(LETROT_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_lt += 1
            rec_type = rec.get("type", "")

            if rec_type == "course_info":
                course_lt += 1
                continue
            elif rec_type != "partant":
                continue

            partant_lt += 1
            date = str(rec.get("date", ""))[:10]
            hippo_id = str(rec.get("hippodrome_id", ""))
            course_num = str(rec.get("numero_course", ""))

            if not date or not hippo_id:
                continue

            # Map hippodrome ID to normalised name
            hippo_norm = _LETROT_HIPPO_MAP.get(hippo_id, "")
            if not hippo_norm:
                # Use the ID as-is for unknown hippodromes
                hippo_norm = hippo_id

            # Extract horse name from "chevalcrack_series_au_partant"
            raw_name = rec.get("chevalcrack_series_au_partant", "")
            horse_name_norm = _normalise_name(raw_name)

            info = {
                "n": str(rec.get("n", "")),
                "horse_name_norm": horse_name_norm,
                "letrot_rang": rec.get("rang"),
                "letrot_temps": rec.get("temps", ""),
                "letrot_reduction_km": rec.get("red.<br>km", rec.get("red.km", "")),
                "letrot_allocation": _parse_allocation(rec.get("alloc._(€)", rec.get("alloc._(â\u201a¬)", ""))),
                "letrot_rapport_prob": _parse_rapport(rec.get("rap._prob.", "")),
                "letrot_fer": rec.get("fer", ""),
                "letrot_avis": rec.get("avis_entraineur", ""),
                "letrot_driver": rec.get("driverentraîneur", rec.get("driverentra\u00c3\u00aeneur", "")),
            }

            key = f"{date}|{hippo_norm}|{course_num}"
            race_partants[key].append(info)

    print(f"       {total_lt:,} total records, {partant_lt:,} partants, "
          f"{course_lt:,} course_info, {len(race_partants):,} race groups")

    # --- Phase 2: stream master, enrich, write out ---
    print(f"[2/2] Streaming master -> enriched output ...")

    total = 0
    matched = 0

    os.makedirs(os.path.dirname(MASTER_OUT), exist_ok=True)

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
            hippo_norm = _normalise_hippo(rec.get("hippodrome_normalise", ""))
            num_course = str(rec.get("numero_course", ""))
            num_pmu = str(rec.get("num_pmu", ""))
            nom_cheval = _normalise_name(rec.get("nom_cheval", ""))

            hit: dict | None = None

            if date_iso and hippo_norm:
                key = f"{date_iso}|{hippo_norm}|{num_course}"
                candidates = race_partants.get(key, [])

                # Try exact match by horse number
                if num_pmu:
                    for c in candidates:
                        if c.get("n") == num_pmu:
                            hit = c
                            break

                # Fallback: match by horse name
                if hit is None and nom_cheval:
                    for c in candidates:
                        if c.get("horse_name_norm") and c["horse_name_norm"] == nom_cheval:
                            hit = c
                            break

                # Second fallback: try without course number (broader match)
                if hit is None and num_pmu:
                    for k, cands in race_partants.items():
                        if k.startswith(f"{date_iso}|{hippo_norm}|"):
                            for c in cands:
                                if c.get("n") == num_pmu:
                                    hit = c
                                    break
                            if hit:
                                break

            if hit is not None:
                for fld in ("letrot_rang", "letrot_temps", "letrot_reduction_km",
                            "letrot_allocation", "letrot_rapport_prob",
                            "letrot_fer", "letrot_avis", "letrot_driver"):
                    val = hit.get(fld)
                    if val is not None and val != "":
                        rec[fld] = val
                matched += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0
    pct = (matched / total * 100) if total else 0
    print(f"Done in {elapsed:.1f}s. {total:,} partants, {matched:,} enriched ({pct:.1f}%).")
    print(f"Output: {MASTER_OUT}")


if __name__ == "__main__":
    main()
