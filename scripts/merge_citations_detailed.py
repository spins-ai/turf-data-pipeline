#!/usr/bin/env python3
"""
scripts/merge_citations_detailed.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge horse-level betting citation data from the cache goldmine audit
(citations_par_cheval.jsonl, 12.98M rows) into partants_master.jsonl.

Source: output/27_citations_enjeux/citations_par_cheval.jsonl
  Keys: date, num_reunion, num_course, num_pmu, enjeu, ratio, favoris, type_pari

Strategy:
  Phase 1 - Build two lookup dicts from citations (streamed, ~2GB RAM budget):
    a) course_key -> enjeu_total  (sum of enjeu across all bet types & horses)
    b) course_key -> ratio_list   (all ratios for market concentration)
    c) (course_key, num_pmu) -> {enjeu_total, ratio_marche, is_favori_citations}

  Phase 2 - Stream partants_master, enrich matching rows, write in-place.

Fields added per partant:
  - cit_enjeu_total         : total enjeu for the course (sum across bet types)
  - cit_ratio_marche        : horse's average ratio across bet types
  - cit_is_favori_citations : True if horse marked as favori in any bet type
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
CITATIONS_PATH = os.path.join(BASE, "output", "27_citations_enjeux", "citations_par_cheval.jsonl")
MASTER_IN = os.path.join(BASE, "data_master", "partants_master.jsonl")
MASTER_OUT = os.path.join(BASE, "data_master", "partants_master.jsonl.tmp")

PROGRESS_EVERY = 2_000_000


def make_course_key(date: str, num_r: int, num_c: int) -> str:
    """Composite key for course-level join."""
    return f"{date}|{num_r}|{num_c}"


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---------------------------------------------------
    for label, path in [("Citations", CITATIONS_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: build course-level and horse-level lookups ---------------
    print(f"[1/3] Streaming citations from {os.path.basename(CITATIONS_PATH)} ...")

    # Course-level aggregates
    course_enjeu: dict[str, float] = defaultdict(float)

    # Horse-level: (course_key, num_pmu) -> {ratios: list, is_favori: bool}
    horse_data: dict[str, dict] = {}

    total_cit = 0
    skipped = 0

    with open(CITATIONS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            total_cit += 1

            date = rec.get("date", "")
            num_r = rec.get("num_reunion")
            num_c = rec.get("num_course")
            num_pmu = rec.get("num_pmu")

            if not date or num_r is None or num_c is None or num_pmu is None:
                skipped += 1
                continue

            ck = make_course_key(date, int(num_r), int(num_c))
            enjeu = rec.get("enjeu") or 0
            ratio = rec.get("ratio")
            favoris = rec.get("favoris", False)

            # Course-level: accumulate total enjeu
            if enjeu:
                course_enjeu[ck] += float(enjeu)

            # Horse-level: accumulate ratios + favori flag
            hk = f"{ck}|{int(num_pmu)}"
            if hk not in horse_data:
                horse_data[hk] = {"ratios": [], "is_favori": False}

            if ratio is not None:
                horse_data[hk]["ratios"].append(float(ratio))
            if favoris:
                horse_data[hk]["is_favori"] = True

            if total_cit % PROGRESS_EVERY == 0:
                print(f"       ... {total_cit / 1e6:.1f}M citation lines read")

    print(f"       {total_cit:,} citations read, {skipped:,} skipped")
    print(f"       {len(course_enjeu):,} courses with enjeu, {len(horse_data):,} horse entries")

    # --- Phase 2: pre-compute horse-level aggregates -----------------------
    print("[2/3] Pre-computing horse aggregates ...")

    horse_agg: dict[str, dict] = {}
    for hk, hd in horse_data.items():
        ratios = hd["ratios"]
        avg_ratio = round(sum(ratios) / len(ratios), 2) if ratios else None
        horse_agg[hk] = {
            "cit_ratio_marche": avg_ratio,
            "cit_is_favori_citations": hd["is_favori"],
        }

    # Free raw data
    del horse_data

    # Round course enjeu
    course_enjeu_final: dict[str, float] = {
        ck: round(v, 2) for ck, v in course_enjeu.items()
    }
    del course_enjeu

    # --- Phase 3: stream master, enrich, write tmp -------------------------
    print(f"[3/3] Streaming master -> enriched output ...")

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

            date = rec.get("date_reunion_iso", "")
            num_r = rec.get("numero_reunion")
            num_c = rec.get("numero_course")
            num_pmu = rec.get("num_pmu")

            if date and num_r is not None and num_c is not None:
                ck = make_course_key(date, int(num_r), int(num_c))

                # Course-level enjeu
                enjeu = course_enjeu_final.get(ck)
                if enjeu is not None:
                    rec["cit_enjeu_total"] = enjeu
                    matched_course += 1

                # Horse-level
                if num_pmu is not None:
                    hk = f"{ck}|{int(num_pmu)}"
                    hit = horse_agg.get(hk)
                    if hit:
                        rec.update(hit)
                        matched_horse += 1

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
    pct_c = (matched_course / total * 100) if total else 0
    pct_h = (matched_horse / total * 100) if total else 0
    print(f"\nDone in {elapsed:.1f}s. {total:,} partants processed.")
    print(f"  Course-level (cit_enjeu_total):       {matched_course:,} ({pct_c:.1f}%)")
    print(f"  Horse-level  (cit_ratio, cit_favori): {matched_horse:,} ({pct_h:.1f}%)")
    print(f"Output: {MASTER_IN} (in-place)")


if __name__ == "__main__":
    main()
