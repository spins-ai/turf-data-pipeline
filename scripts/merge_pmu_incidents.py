#!/usr/bin/env python3
"""
scripts/merge_pmu_incidents.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge PMU API incident data from pmu_incidents.jsonl (~98K course-level
incident rows) into the partants master (partants_normalises.jsonl).

Each incident record is course-level:
  - date, num_reunion, num_course, incident_type, participants_nums

We explode participants_nums so that each (date, num_reunion, num_course,
num_pmu) gets the incident_type written into the master's ``incident`` field.

Only fills the ``incident`` field when it is currently empty/null.

Memory-efficient: builds a dict keyed by (date, numReunion, numCourse) ->
{incident_type, participants_nums set}, then streams the master line-by-line.
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
INCIDENTS_PATH = os.path.join(BASE, "output", "101_pmu_api", "pmu_incidents.jsonl")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_incidents.jsonl")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---------------------------------------------------
    for label, path in [("Incidents", INCIDENTS_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: build incident lookup ------------------------------------
    # Key: (date, num_reunion, num_course) -> list of (incident_type, frozenset of nums)
    # A course can have multiple incident types (e.g. DISQUALIFIE + ARRETE).
    print(f"[1/3] Loading incidents from {INCIDENTS_PATH} ...")

    # For each (date, reunion, course, num_pmu) -> incident_type
    # Use a dict of (date, reunion, course) -> dict[num_pmu -> incident_type]
    incident_lookup: dict[tuple, dict[int, str]] = {}
    total_incidents = 0
    total_participant_hits = 0

    with open(INCIDENTS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_incidents += 1
            date = str(rec.get("date", ""))[:10]
            num_reunion = rec.get("num_reunion")
            num_course = rec.get("num_course")
            incident_type = rec.get("incident_type", "")
            participants = rec.get("participants_nums", [])

            if not date or num_reunion is None or num_course is None:
                continue
            if not incident_type or not participants:
                continue

            key = (date, int(num_reunion), int(num_course))
            if key not in incident_lookup:
                incident_lookup[key] = {}

            for num in participants:
                num_int = int(num)
                # If a horse has multiple incidents, keep the first one
                if num_int not in incident_lookup[key]:
                    incident_lookup[key][num_int] = incident_type
                    total_participant_hits += 1

    print(f"       {total_incidents:,} incident records -> "
          f"{len(incident_lookup):,} course keys, "
          f"{total_participant_hits:,} participant-incident pairs")

    # --- Phase 2: compute before fill rate ---------------------------------
    print(f"[2/3] Computing pre-merge fill rate ...")

    total_master = 0
    filled_before = 0

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
            v = rec.get("incident")
            if v is not None and v != "" and v != "null":
                filled_before += 1

    pct_before = (filled_before / total_master * 100) if total_master else 0
    print(f"       Before: incident filled = {filled_before:,} / {total_master:,} ({pct_before:.1f}%)")

    # --- Phase 3: stream master, enrich, write out -------------------------
    print(f"[3/3] Streaming master -> enriched output ...")

    total = 0
    newly_filled = 0
    filled_after = 0

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

            # Check if incident is already filled
            existing = rec.get("incident")
            already_has = (existing is not None and existing != "" and existing != "null")

            if not already_has:
                date_iso = str(rec.get("date_reunion_iso", ""))[:10]
                num_reunion = rec.get("numero_reunion")
                num_course = rec.get("numero_course")
                num_pmu = rec.get("num_pmu")

                if date_iso and num_reunion is not None and num_course is not None and num_pmu is not None:
                    key = (date_iso, int(num_reunion), int(num_course))
                    course_incidents = incident_lookup.get(key)
                    if course_incidents:
                        inc_type = course_incidents.get(int(num_pmu))
                        if inc_type:
                            rec["incident"] = inc_type
                            newly_filled += 1

            # Count final fill rate
            v = rec.get("incident")
            if v is not None and v != "" and v != "null":
                filled_after += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    pct_after = (filled_after / total * 100) if total else 0
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s.")
    print(f"  Total partants:     {total:,}")
    print(f"  incident before:    {filled_before:,} ({pct_before:.1f}%)")
    print(f"  incident after:     {filled_after:,} ({pct_after:.1f}%)")
    print(f"  Newly filled:       {newly_filled:,} (+{(newly_filled/total*100) if total else 0:.1f}%)")
    print(f"  Output: {MASTER_OUT}")


if __name__ == "__main__":
    main()
