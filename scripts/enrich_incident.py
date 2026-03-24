#!/usr/bin/env python3
"""
enrich_incident.py
==================
Cross-reference reunions_enrichies (39) incident data with partants_master
to fill the 'incident' field.

Reunions have per-course incident details with participant numbers.
Partants have num_pmu per record.  We join on (date, reunion, course, num_pmu).

Streaming JSONL -> JSONL to keep RAM under 2 GB.

Usage:
    python scripts/enrich_incident.py
    python scripts/enrich_incident.py --dry-run
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_MASTER = ROOT / "data_master"
REUNIONS_JSONL = ROOT / "output" / "39_reunions_enrichies" / "reunions_enrichies.jsonl"
PARTANTS_IN = DATA_MASTER / "partants_master_enrichi.jsonl"
PARTANTS_OUT = DATA_MASTER / "partants_master_enrichi.jsonl"  # in-place
PARTANTS_TMP = DATA_MASTER / "partants_master_enrichi_incident.jsonl.tmp"


def build_incident_index():
    """Build {(date, R, C, num_pmu): incident_description} from reunions data."""
    idx = {}
    count = 0
    courses_with_incidents = 0

    if not REUNIONS_JSONL.exists():
        print(f"  [WARN] {REUNIONS_JSONL} not found, trying .json")
        return idx

    print(f"  Loading incidents from {REUNIONS_JSONL} ...")
    t0 = time.time()

    with open(REUNIONS_JSONL, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            count += 1

            incidents_detail = rec.get("incidents_detail") or []
            if not incidents_detail:
                continue

            courses_with_incidents += 1
            date = rec.get("date_reunion_iso", "")
            # course_uid in reunions is like "2013-02-19_R1_C1"
            course_uid = rec.get("course_uid", "")

            # Parse R and C from course_uid
            parts = course_uid.split("_")
            reunion_num = None
            course_num = None
            for p in parts:
                if p.startswith("R") and p[1:].isdigit():
                    reunion_num = int(p[1:])
                elif p.startswith("C") and p[1:].isdigit():
                    course_num = int(p[1:])

            if reunion_num is None:
                reunion_num = rec.get("numero_reunion")
            if course_num is None:
                course_num = rec.get("numero_course")

            if not date or reunion_num is None or course_num is None:
                continue

            # Build incident text for each participant
            for inc in incidents_detail:
                inc_type = inc.get("type_incident", "").replace("_", " ").lower()
                participants = inc.get("numero_participants") or []
                for num in participants:
                    key = (date, int(reunion_num), int(course_num), int(num))
                    # Append if multiple incidents for same participant
                    if key in idx:
                        idx[key] = idx[key] + "; " + inc_type
                    else:
                        idx[key] = inc_type

    elapsed = time.time() - t0
    print(f"  Loaded {count} courses, {courses_with_incidents} with incidents, "
          f"{len(idx)} participant-incidents in {elapsed:.1f}s")
    return idx


def enrich_partants(incident_idx, dry_run=False):
    """Stream partants JSONL, fill incident field where missing."""
    if not PARTANTS_IN.exists():
        print(f"  [ERROR] {PARTANTS_IN} not found")
        return

    total = 0
    enriched = 0
    already_filled = 0
    t0 = time.time()

    print(f"  Enriching {PARTANTS_IN} ...")

    if dry_run:
        with open(PARTANTS_IN, "r", encoding="utf-8", errors="replace") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total += 1

                existing = (rec.get("incident") or "").strip()
                if existing:
                    already_filled += 1
                    continue

                date = rec.get("date_reunion_iso", "")
                reunion = rec.get("numero_reunion")
                course = rec.get("numero_course")
                num_pmu = rec.get("num_pmu")

                if not all([date, reunion is not None, course is not None, num_pmu is not None]):
                    continue

                key = (date, int(reunion), int(course), int(num_pmu))
                if key in incident_idx:
                    enriched += 1

        print(f"  [DRY-RUN] Would enrich {enriched}/{total} records "
              f"({already_filled} already filled)")
        return

    # Actual enrichment: write to tmp, then replace
    with open(PARTANTS_IN, "r", encoding="utf-8", errors="replace") as fin, \
         open(PARTANTS_TMP, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                fout.write(line + "\n")
                continue
            total += 1

            existing = (rec.get("incident") or "").strip()
            if existing:
                already_filled += 1
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue

            date = rec.get("date_reunion_iso", "")
            reunion = rec.get("numero_reunion")
            course = rec.get("numero_course")
            num_pmu = rec.get("num_pmu")

            if all([date, reunion is not None, course is not None, num_pmu is not None]):
                key = (date, int(reunion), int(course), int(num_pmu))
                if key in incident_idx:
                    rec["incident"] = incident_idx[key]
                    enriched += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Replace original -- retry on Windows permission errors
    for attempt in range(5):
        try:
            os.replace(str(PARTANTS_TMP), str(PARTANTS_IN))
            break
        except PermissionError:
            import shutil
            try:
                shutil.move(str(PARTANTS_TMP), str(PARTANTS_IN))
                break
            except Exception:
                if attempt < 4:
                    time.sleep(2)
                else:
                    # Last resort: keep the tmp file
                    print(f"  [WARN] Could not replace, output saved as {PARTANTS_TMP}")
                    return

    elapsed = time.time() - t0
    pct = 100 * enriched / total if total > 0 else 0
    print(f"  Done: {enriched}/{total} records enriched ({pct:.1f}%), "
          f"{already_filled} already filled, in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Enrich incident field in partants_master")
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying")
    args = parser.parse_args()

    print("=" * 60)
    print("INCIDENT ENRICHMENT — Cross-reference reunions (39) data")
    print("=" * 60)

    incident_idx = build_incident_index()
    if not incident_idx:
        print("  [WARN] No incidents found, nothing to enrich")
        return

    enrich_partants(incident_idx, dry_run=args.dry_run)
    print("  Done!")


if __name__ == "__main__":
    main()
