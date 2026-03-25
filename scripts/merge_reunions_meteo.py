#!/usr/bin/env python3
"""
scripts/merge_reunions_meteo.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge real meteo data from the cache goldmine audit into partants_master.jsonl.

The existing master has met_impact_meteo_score and met_is_psf but the actual
weather fields (temperature, wind, cloud cover) were ALL NULL.  This script
fixes that using the extracted reunions_meteo_complete.jsonl (18K reunions).

Source: output/39_reunions_enrichies/reunions_meteo_complete.jsonl
  Keys: reunion_uid (format: "YYYY-MM-DD_R{n}"), date_reunion_iso, numero_reunion,
        meteo_temperature, meteo_force_vent, meteo_direction_vent,
        meteo_nebulosite_code, meteo_nebulosite_court

Strategy:
  Phase 1 - Load meteo lookup by (date, numero_reunion).  Only 18K rows -> trivial.
  Phase 2 - Stream master, enrich matching rows, write in-place.

Fields added per partant:
  - met_temperature      : temperature in Celsius
  - met_vent_vitesse     : wind speed (km/h)
  - met_vent_direction   : wind direction code (e.g. "NO", "SE")
  - met_nebulosite       : cloud cover short label (e.g. "Couvert", "Soleil")
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
METEO_PATH = os.path.join(BASE, "output", "39_reunions_enrichies", "reunions_meteo_complete.jsonl")
MASTER_IN = os.path.join(BASE, "data_master", "partants_master.jsonl")
MASTER_OUT = os.path.join(BASE, "data_master", "partants_master.jsonl.tmp")


def make_reunion_key(date: str, num_r: int) -> str:
    return f"{date}|{num_r}"


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---------------------------------------------------
    for label, path in [("Meteo", METEO_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: load meteo lookup (tiny, ~18K rows) ----------------------
    print(f"[1/2] Loading meteo from {os.path.basename(METEO_PATH)} ...")

    meteo_lookup: dict[str, dict] = {}
    total_meteo = 0
    with_data = 0

    with open(METEO_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_meteo += 1

            date = rec.get("date_reunion_iso", "")
            num_r = rec.get("numero_reunion")

            if not date or num_r is None:
                continue

            temp = rec.get("meteo_temperature")
            vent = rec.get("meteo_force_vent")
            vent_dir = rec.get("meteo_direction_vent")
            nebulo = rec.get("meteo_nebulosite_court")

            # Skip if no meteo data at all
            if temp is None and vent is None and nebulo is None:
                continue

            rk = make_reunion_key(date, int(num_r))
            meteo_lookup[rk] = {
                "met_temperature": temp,
                "met_vent_vitesse": vent,
                "met_vent_direction": vent_dir,
                "met_nebulosite": nebulo,
            }
            with_data += 1

    print(f"       {total_meteo:,} reunion records read, {with_data:,} with meteo data")

    # --- Phase 2: stream master, enrich, write tmp -------------------------
    print(f"[2/2] Streaming master -> enriched output ...")

    total = 0
    matched = 0

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

            date = rec.get("date_reunion_iso", "")
            num_r = rec.get("numero_reunion")

            if date and num_r is not None:
                rk = make_reunion_key(date, int(num_r))
                hit = meteo_lookup.get(rk)
                if hit:
                    # Only add non-None values
                    for k, v in hit.items():
                        if v is not None:
                            rec[k] = v
                    matched += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

            if total % 500_000 == 0:
                print(f"       ... {total / 1e6:.1f}M master lines processed")

    # --- Atomic replace (Windows-safe) -------------------------------------
    bak = MASTER_IN + ".bak"
    if os.path.exists(bak):
        os.remove(bak)
    os.rename(MASTER_IN, bak)
    os.rename(MASTER_OUT, MASTER_IN)
    os.remove(bak)

    elapsed = time.time() - t0
    pct = (matched / total * 100) if total else 0
    print(f"\nDone in {elapsed:.1f}s. {total:,} partants processed.")
    print(f"  Reunions matched: {matched:,} ({pct:.1f}%)")
    print(f"Output: {MASTER_IN} (in-place)")


if __name__ == "__main__":
    main()
