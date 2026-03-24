#!/usr/bin/env python3
"""
scripts/enrich_hippodromes_db.py
================================
Enriches hippodromes_db.py with computed statistics derived from
partants_master.jsonl and courses_master.jsonl.

For each hippodrome found in the data, the script computes:
  - nb_courses          : total distinct courses
  - nb_partants         : total starters
  - date_min / date_max : earliest / latest race date
  - top_3_disciplines   : most frequent disciplines
  - avg_nb_partants     : average starters per course
  - avg_allocation      : average allocation_totale per course (from courses_master)

Output: data_master/hippodromes_enriched.json

Architecture:
  1. Stream partants_master.jsonl once, accumulate per-hippodrome counters
     (course_uid sets stay as sets; discipline counts in Counter dicts).
  2. Stream courses_master.jsonl once to collect allocation_totale per
     hippodrome (summed, then averaged).
  3. Merge with existing hippodromes_db.py metadata (GPS, region, etc.).
  4. Write enriched JSON.

Memory budget: < 3 GB.
  - Per-hippodrome accumulators: ~1K hippodromes * small dicts -> negligible.
  - course_uid sets: ~200K entries * ~20 bytes -> ~4 MB.

No external dependencies -- stdlib only.
No API calls -- 100% local processing.

Usage:
    python scripts/enrich_hippodromes_db.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_MASTER = _PROJECT_ROOT / "data_master"
PARTANTS_PATH = DATA_MASTER / "partants_master.jsonl"
COURSES_PATH = DATA_MASTER / "courses_master.jsonl"
OUTPUT_PATH = DATA_MASTER / "hippodromes_enriched.json"

# Try to import the existing hippodromes_db for merging GPS/metadata
try:
    sys.path.insert(0, str(_PROJECT_ROOT))
    from hippodromes_db import HIPPODROMES_DB  # type: ignore[import-untyped]
except ImportError:
    HIPPODROMES_DB = {}

_REPORT_INTERVAL = 30  # seconds


def human_size(nbytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


# ---------------------------------------------------------------------------
# PASS 1 -- partants_master.jsonl
# ---------------------------------------------------------------------------
def _stream_partants(path: Path) -> dict[str, dict]:
    """Stream partants_master.jsonl and accumulate per-hippodrome stats.

    Returns: { hippodrome_normalise: { ... accumulators ... } }
    """
    hippo_stats: dict[str, dict] = {}
    total = 0
    t0 = time.time()
    last_report = t0

    print(f"[Pass 1] Streaming {path.name} ...")

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total += 1
            hippo = rec.get("hippodrome_normalise")
            if not hippo:
                continue

            if hippo not in hippo_stats:
                hippo_stats[hippo] = {
                    "course_uids": set(),
                    "nb_partants": 0,
                    "date_min": None,
                    "date_max": None,
                    "disciplines": Counter(),
                }

            st = hippo_stats[hippo]
            st["nb_partants"] += 1

            cuid = rec.get("course_uid")
            if cuid:
                st["course_uids"].add(cuid)

            disc = rec.get("discipline")
            if disc:
                st["disciplines"][disc] += 1

            d = rec.get("date_reunion_iso")
            if d:
                if st["date_min"] is None or d < st["date_min"]:
                    st["date_min"] = d
                if st["date_max"] is None or d > st["date_max"]:
                    st["date_max"] = d

            now = time.time()
            if now - last_report > _REPORT_INTERVAL:
                elapsed = now - t0
                print(
                    f"  [{int(elapsed)}s] {total:,} partants, "
                    f"{len(hippo_stats):,} hippodromes"
                )
                last_report = now

    elapsed = time.time() - t0
    print(
        f"  Done: {total:,} partants, {len(hippo_stats):,} hippodromes "
        f"in {elapsed:.1f}s"
    )
    return hippo_stats


# ---------------------------------------------------------------------------
# PASS 2 -- courses_master.jsonl  (for allocation_totale)
# ---------------------------------------------------------------------------
def _stream_courses(path: Path) -> dict[str, dict]:
    """Stream courses_master.jsonl and collect allocation stats per hippodrome.

    Returns: { hippodrome: { "alloc_sum": float, "alloc_count": int } }
    """
    alloc_stats: dict[str, dict] = {}
    total = 0
    t0 = time.time()
    last_report = t0

    if not path.exists():
        print(f"[Pass 2] {path.name} not found -- skipping allocation stats.")
        return alloc_stats

    print(f"[Pass 2] Streaming {path.name} ...")

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total += 1
            hippo = rec.get("hippodrome_normalise")
            if not hippo:
                continue

            alloc = rec.get("allocation_totale")
            if alloc is not None and alloc > 0:
                if hippo not in alloc_stats:
                    alloc_stats[hippo] = {"alloc_sum": 0.0, "alloc_count": 0}
                alloc_stats[hippo]["alloc_sum"] += float(alloc)
                alloc_stats[hippo]["alloc_count"] += 1

            now = time.time()
            if now - last_report > _REPORT_INTERVAL:
                elapsed = now - t0
                print(f"  [{int(elapsed)}s] {total:,} courses")
                last_report = now

    elapsed = time.time() - t0
    print(f"  Done: {total:,} courses in {elapsed:.1f}s")
    return alloc_stats


# ---------------------------------------------------------------------------
# MERGE & OUTPUT
# ---------------------------------------------------------------------------
def _merge_and_write(
    hippo_stats: dict[str, dict],
    alloc_stats: dict[str, dict],
    output_path: Path,
) -> int:
    """Merge partant stats, allocation stats, and hippodromes_db metadata.

    Returns the number of enriched hippodromes written.
    """
    enriched: dict[str, dict] = {}

    for hippo, st in hippo_stats.items():
        nb_courses = len(st["course_uids"])
        nb_partants = st["nb_partants"]

        # Top 3 disciplines by frequency
        top_3 = [d for d, _ in st["disciplines"].most_common(3)]

        # Avg partants per course
        avg_nb_partants = round(nb_partants / nb_courses, 2) if nb_courses else 0

        # Avg allocation (from courses_master)
        a = alloc_stats.get(hippo, {})
        avg_alloc = (
            round(a["alloc_sum"] / a["alloc_count"], 2)
            if a.get("alloc_count", 0) > 0
            else None
        )

        entry: dict = {
            "nb_courses": nb_courses,
            "nb_partants": nb_partants,
            "date_min": st["date_min"],
            "date_max": st["date_max"],
            "top_3_disciplines": top_3,
            "avg_nb_partants": avg_nb_partants,
            "avg_allocation": avg_alloc,
        }

        # Merge with existing hippodromes_db metadata if available
        db_entry = HIPPODROMES_DB.get(hippo, {})
        if db_entry:
            for key in ("region", "lat", "lon", "pays", "altitude",
                        "type_piste", "corde"):
                if key in db_entry:
                    entry[key] = db_entry[key]

        enriched[hippo] = entry

    # Also include hippodromes from DB that have no partants data
    for hippo, db_entry in HIPPODROMES_DB.items():
        if hippo not in enriched:
            enriched[hippo] = {
                "nb_courses": 0,
                "nb_partants": 0,
                "date_min": None,
                "date_max": None,
                "top_3_disciplines": [],
                "avg_nb_partants": 0,
                "avg_allocation": None,
            }
            for key in ("region", "lat", "lon", "pays", "altitude",
                        "type_piste", "corde"):
                if key in db_entry:
                    enriched[hippo][key] = db_entry[key]

    # Sort by hippodrome name for deterministic output
    enriched_sorted = dict(sorted(enriched.items()))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(enriched_sorted, fp, ensure_ascii=False, indent=2)

    size = output_path.stat().st_size
    print(f"\nWrote {len(enriched_sorted):,} hippodromes to {output_path}")
    print(f"  File size: {human_size(size)}")

    # Quick stats
    with_data = sum(1 for v in enriched_sorted.values() if v["nb_courses"] > 0)
    print(f"  With race data: {with_data:,}")
    print(f"  DB-only (no races): {len(enriched_sorted) - with_data:,}")

    return len(enriched_sorted)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich hippodromes_db with computed stats from race data"
    )
    parser.add_argument(
        "--partants",
        type=Path,
        default=PARTANTS_PATH,
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--courses",
        type=Path,
        default=COURSES_PATH,
        help="Path to courses_master.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help="Output path (default: data_master/hippodromes_enriched.json)",
    )
    args = parser.parse_args()

    if not args.partants.exists():
        print(f"ERROR: {args.partants} not found", file=sys.stderr)
        sys.exit(1)

    print("=" * 60)
    print("Hippodromes DB Enrichment")
    print("=" * 60)
    print(f"Partants : {args.partants}")
    print(f"Courses  : {args.courses}")
    print(f"Output   : {args.output}")
    print(f"DB entries loaded: {len(HIPPODROMES_DB):,}")
    print()

    t0 = time.time()

    # Pass 1: partants
    hippo_stats = _stream_partants(args.partants)

    # Pass 2: courses (for allocation)
    alloc_stats = _stream_courses(args.courses)

    # Merge & write
    _merge_and_write(hippo_stats, alloc_stats, args.output)

    elapsed = time.time() - t0
    print(f"\nTotal elapsed: {elapsed:.1f}s")
    print("Done.")


if __name__ == "__main__":
    main()
