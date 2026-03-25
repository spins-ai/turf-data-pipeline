#!/usr/bin/env python3
"""
scripts/merge_citations_to_master.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge betting-market citation/enjeu data from citations_enjeux.jsonl (5.26M
records) into the partants master (partants_normalises.jsonl).

The citations file contains per-course, per-bet-type rows.  Many rows are
marked ``indisponible=true`` (no market data).  Available rows carry
horse-level citation fields (num_pmu, nom_cheval, citation_position, etc.).

Merge strategy:
  - Index available citation records by course_uid.
  - For each course_uid, aggregate:
      * List of distinct type_pari values offered.
      * Per-horse citation data (position, ratio) keyed by num_pmu.
  - Stream master line-by-line, enrich matching partants by course_uid +
    num_pmu.

Fields added to each matching partant:
  - types_pari       : list of bet types available for the course
  - citation_position: horse's citation position (if cited)
  - citation_ratio   : horse's citation ratio (if available)
  - citation_favoris : whether the horse is flagged as favourite
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CITATIONS_PATH = os.path.join(BASE, "output", "27_citations_enjeux", "citations_enjeux.jsonl")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_citations.jsonl")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---------------------------------------------------
    for label, path in [("Citations", CITATIONS_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: build course-level and horse-level lookups ---------------
    print(f"[1/2] Streaming citations from {CITATIONS_PATH} ...")

    # course_uid -> set of type_pari
    course_types: dict[str, set[str]] = defaultdict(set)
    # (course_uid, num_pmu) -> horse citation fields
    horse_citations: dict[str, dict] = {}

    total_citations = 0
    available = 0
    horse_entries = 0

    with open(CITATIONS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_citations += 1
            course_uid = rec.get("course_uid", "")
            type_pari = rec.get("type_pari", "")

            if not course_uid:
                continue

            # Track bet types per course (even unavailable ones tell us
            # which markets exist).
            if type_pari:
                course_types[course_uid].add(type_pari)

            # Skip unavailable records (no horse-level data)
            if rec.get("indisponible"):
                continue

            available += 1

            num_pmu = rec.get("num_pmu")
            if num_pmu is None:
                continue

            hk = f"{course_uid}|{int(num_pmu)}"
            if hk not in horse_citations:
                horse_citations[hk] = {
                    "citation_position": rec.get("citation_position"),
                    "citation_ratio": rec.get("citation_ratio"),
                    "citation_favoris": rec.get("favoris"),
                }
                horse_entries += 1

            # Progress indicator every 1M lines
            if total_citations % 1_000_000 == 0:
                print(f"       ... {total_citations / 1e6:.0f}M citation lines read")

    print(f"       {total_citations:,} citation records, {available:,} available, "
          f"{horse_entries:,} horse entries, {len(course_types):,} courses with bet types")

    # Convert sets to sorted lists for JSON serialisation
    course_types_list: dict[str, list[str]] = {
        uid: sorted(types) for uid, types in course_types.items()
    }
    # Free the set version
    del course_types

    # --- Phase 2: stream master, enrich, write out -------------------------
    print(f"[2/2] Streaming master -> enriched output ...")

    total = 0
    matched_course = 0
    matched_horse = 0

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
            course_uid = rec.get("course_uid", "")

            if course_uid:
                # Add bet-type list at course level
                types = course_types_list.get(course_uid)
                if types:
                    rec["types_pari"] = types
                    matched_course += 1

                # Add horse-level citation data
                num_pmu = rec.get("num_pmu")
                if num_pmu is not None:
                    hk = f"{course_uid}|{int(num_pmu)}"
                    hit = horse_citations.get(hk)
                    if hit:
                        rec.update(hit)
                        matched_horse += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0
    pct_c = (matched_course / total * 100) if total else 0
    pct_h = (matched_horse / total * 100) if total else 0
    print(f"Done in {elapsed:.1f}s. {total:,} partants.")
    print(f"  Course-level enriched: {matched_course:,} ({pct_c:.1f}%)")
    print(f"  Horse-level enriched:  {matched_horse:,} ({pct_h:.1f}%)")
    print(f"Output: {MASTER_OUT}")


if __name__ == "__main__":
    main()
