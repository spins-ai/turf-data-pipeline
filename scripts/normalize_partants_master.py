#!/usr/bin/env python3
"""
Normalize partants_master.jsonl fields in a streaming fashion.

Fixes:
  - sexe: H → hongres, F → femelles, M → males (lowercase)
  - type_piste: gazon → herbe (both mean grass)
  - pgr_race: PUR SANG → PUR-SANG, TROTTEUR FR. → TROTTEUR FRANCAIS, etc. Uppercase all.
  - pgr_robe: Uppercase all values
  - robe: Replace empty strings with None

Streams line-by-line to stay under 2 GB RAM on a 17 GB file.
Writes to a .tmp file, then does an atomic replace.
"""

import json
import os
import sys
import time

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT = os.path.join(BASE_DIR, "data_master", "partants_master.jsonl")
OUTPUT_TMP = INPUT + ".tmp"

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------
SEXE_MAP = {
    "H": "hongres",
    "h": "hongres",
    "F": "femelles",
    "f": "femelles",
    "M": "males",
    "m": "males",
    # already-correct forms kept as-is
    "hongres": "hongres",
    "femelles": "femelles",
    "males": "males",
}

TYPE_PISTE_MAP = {
    "gazon": "herbe",
    "Gazon": "herbe",
    "GAZON": "herbe",
}

# pgr_race: canonical forms (uppercase). Map known variants.
PGR_RACE_MAP = {
    "PUR SANG": "PUR-SANG",
    "TROTTEUR FR.": "TROTTEUR FRANCAIS",
    "*ANGLO-ARABE*": "ANGLO-ARABE",
    "*AA COMPL.*": "AA COMPLEMENT",
    "SF": "SELLE FRANCAIS",
}


def normalize_record(rec: dict, counters: dict) -> dict:
    """Apply all normalization rules to a single record (mutates in place)."""

    # --- sexe ---
    val = rec.get("sexe")
    if val is not None:
        mapped = SEXE_MAP.get(val)
        if mapped is not None and mapped != val:
            rec["sexe"] = mapped
            counters["sexe"] += 1
        elif mapped is None and val not in SEXE_MAP.values():
            # Unknown value – lowercase it as a fallback
            lower = val.lower()
            if lower != val:
                rec["sexe"] = lower
                counters["sexe"] += 1

    # --- type_piste ---
    val = rec.get("type_piste")
    if val is not None:
        mapped = TYPE_PISTE_MAP.get(val)
        if mapped is not None:
            rec["type_piste"] = mapped
            counters["type_piste"] += 1

    # --- pgr_race ---
    val = rec.get("pgr_race")
    if val is not None and isinstance(val, str):
        upper = val.upper()
        mapped = PGR_RACE_MAP.get(upper, upper)
        if mapped == upper:
            mapped = PGR_RACE_MAP.get(val, upper)
        if mapped != val:
            rec["pgr_race"] = mapped
            counters["pgr_race"] += 1

    # --- pgr_robe ---
    val = rec.get("pgr_robe")
    if val is not None and isinstance(val, str):
        upper = val.upper()
        if upper != val:
            rec["pgr_robe"] = upper
            counters["pgr_robe"] += 1

    # --- robe ---
    val = rec.get("robe")
    if val is not None and isinstance(val, str) and val == "":
        rec["robe"] = None
        counters["robe"] += 1

    return rec


def main() -> None:
    if not os.path.isfile(INPUT):
        print(f"ERROR: input file not found: {INPUT}", file=sys.stderr)
        sys.exit(1)

    counters = {
        "sexe": 0,
        "type_piste": 0,
        "pgr_race": 0,
        "pgr_robe": 0,
        "robe": 0,
    }
    total = 0
    t0 = time.time()

    with (
        open(INPUT, "r", encoding="utf-8") as fin,
        open(OUTPUT_TMP, "w", encoding="utf-8", newline="\n") as fout,
    ):
        for line in fin:
            rec = json.loads(line)
            rec = normalize_record(rec, counters)
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            total += 1
            if total % 500_000 == 0:
                elapsed = time.time() - t0
                print(f"  ... {total:>10,} lines  ({elapsed:.1f}s)", flush=True)

    elapsed = time.time() - t0

    # Atomic replace
    os.replace(OUTPUT_TMP, INPUT)

    # Report
    print(f"\nDone – {total:,} records processed in {elapsed:.1f}s\n")
    print("Changes per field:")
    total_changes = 0
    for field, cnt in sorted(counters.items()):
        pct = cnt / total * 100 if total else 0
        print(f"  {field:<15s}: {cnt:>10,}  ({pct:.2f}%)")
        total_changes += cnt
    print(f"  {'TOTAL':<15s}: {total_changes:>10,}")


if __name__ == "__main__":
    main()
