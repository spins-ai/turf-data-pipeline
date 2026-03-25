#!/usr/bin/env python3
"""
scripts/merge_paris_turf_to_master.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge Paris-Turf runner-level data from paris_turf_runners.jsonl and
race-level data from paris_turf_races.jsonl into the partants master
(partants_normalises.jsonl).

Paris-Turf runners carry rich per-horse data:
  - horseName, age, sex, draw, weightKg, jockeyName, trainerName
  - numberOfRuns, numberOfWins, numberOfPlaces, totalPrize
  - formFigs, ranking, margin, records, daysSincePreviousRace
  - raceDate, raceName, meetingName, raceSpeciality, raceSurface

Paris-Turf races carry course-level metadata:
  - going, surface, winnerTimeKm, penetrometer, totalPrize, distance

Merge strategy:
  - Build a lookup keyed by (date, nom_cheval_normalise) from runners.
  - Optionally enrich with race-level fields from races.
  - For each master partant, match on (date_reunion_iso, nom_cheval).

Fields added to each matching partant:
  - pt_going           : going description from Paris-Turf
  - pt_surface         : surface code (PH, PS, etc.)
  - pt_winner_time_km  : winner time per km
  - pt_penetrometer    : penetrometer reading
  - pt_draw            : draw position
  - pt_weight_kg       : weight in kg
  - pt_form_figs       : form figures string
  - pt_ranking         : finishing ranking
  - pt_margin          : margin string
  - pt_days_since_prev : days since previous race
  - pt_nb_runs         : number of career runs
  - pt_nb_wins         : number of career wins
  - pt_total_prize     : total career prize money
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
PT_RUNNERS = os.path.join(BASE, "output", "53_paris_turf", "paris_turf_runners.jsonl")
PT_RACES = os.path.join(BASE, "output", "53_paris_turf", "paris_turf_races.jsonl")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_paris_turf.jsonl")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_name(name: str) -> str:
    """Normalise horse name: uppercase, strip accents-ish, collapse spaces."""
    if not name:
        return ""
    cleaned = name.strip().upper()
    cleaned = re.sub(r"[^A-Z ]", "", cleaned).strip()
    return re.sub(r"\s+", " ", cleaned)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---
    if not os.path.isfile(PT_RUNNERS):
        print(f"ERROR: Paris-Turf runners file not found: {PT_RUNNERS}", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(MASTER_IN):
        print(f"ERROR: Master file not found: {MASTER_IN}", file=sys.stderr)
        sys.exit(1)

    # --- Phase 1a: build race lookup (optional, for going/surface) ---
    race_lookup: dict[str, dict] = {}  # key: raceId -> race fields
    if os.path.isfile(PT_RACES):
        print(f"[1/3] Loading Paris-Turf races from {PT_RACES} ...")
        race_count = 0
        with open(PT_RACES, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                race_count += 1
                race_id = str(rec.get("id", ""))
                if race_id:
                    race_lookup[race_id] = {
                        "pt_going": rec.get("going", ""),
                        "pt_surface": rec.get("surface", ""),
                        "pt_winner_time_km": rec.get("winnerTimeKm", ""),
                        "pt_penetrometer": rec.get("penetrometer"),
                        "pt_race_total_prize": rec.get("totalPrize"),
                    }
        print(f"       {race_count:,} race records, {len(race_lookup):,} indexed")
    else:
        print("[1/3] Paris-Turf races file not found, skipping race-level enrichment.")

    # --- Phase 1b: build runner lookup keyed by (date, horse_name_norm) ---
    print(f"[2/3] Loading Paris-Turf runners from {PT_RUNNERS} ...")

    # Key: "date|horse_name_norm" -> runner info
    runner_lookup: dict[str, dict] = {}
    total_pt = 0
    indexed = 0

    with open(PT_RUNNERS, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_pt += 1
            # Use raceDate (the actual race date) for matching
            date = str(rec.get("raceDate", rec.get("date", "")))[:10]
            horse_name = _normalise_name(rec.get("horseName", ""))

            if not date or not horse_name:
                continue

            race_id = str(rec.get("raceId", ""))
            race_fields = race_lookup.get(race_id, {})

            info = {
                "pt_draw": rec.get("draw"),
                "pt_weight_kg": rec.get("weightKg"),
                "pt_form_figs": rec.get("formFigs", ""),
                "pt_ranking": rec.get("ranking"),
                "pt_margin": rec.get("margin", ""),
                "pt_days_since_prev": rec.get("daysSincePreviousRace"),
                "pt_nb_runs": rec.get("numberOfRuns"),
                "pt_nb_wins": rec.get("numberOfWins"),
                "pt_total_prize": rec.get("totalPrize"),
            }
            # Merge race-level fields
            info.update(race_fields)

            key = f"{date}|{horse_name}"
            if key not in runner_lookup:
                runner_lookup[key] = info
                indexed += 1

    print(f"       {total_pt:,} runner records, {indexed:,} indexed by date+name")

    # --- Phase 2: stream master, enrich, write out ---
    print(f"[3/3] Streaming master -> enriched output ...")

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
            nom_cheval = _normalise_name(rec.get("nom_cheval", ""))

            if date_iso and nom_cheval:
                key = f"{date_iso}|{nom_cheval}"
                hit = runner_lookup.get(key)
                if hit:
                    for fld, val in hit.items():
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
