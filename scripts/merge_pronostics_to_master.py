#!/usr/bin/env python3
"""
scripts/merge_pronostics_to_master.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge expert pronostic rankings from pronostics_all.json (204K course-level
records) into the partants master (partants_normalises.jsonl).

For each pronostic record, the script unpivots the wide prono_rang_N_num /
prono_rang_N_cote columns into per-horse rows, then joins on the composite
key (date_reunion_iso, numero_reunion, numero_course, num_pmu).

Fields added to each matching partant:
  - prono_rang   : expert ranking (1-based position in the pronostic)
  - prono_cote   : expert odds string (e.g. "2/1")
  - source_prono : origin of the pronostic (e.g. "pmu_api")

Streaming approach:
  1. Build an in-memory lookup from pronostics_all.json  (~200K records,
     each expanded to ~5-8 horse entries => ~1.2M lookup entries, <500 MB).
  2. Stream partants_normalises.jsonl line by line, enrich, write out.
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
PRONO_PATH = os.path.join(BASE, "output", "23_pronostics", "pronostics_all.json")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_pronostics.jsonl")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_course_key(date_iso: str, num_reunion: int, num_course: int) -> str:
    """Composite course key for joining."""
    return f"{date_iso}|{num_reunion}|{num_course}"


def _parse_prono_record(rec: dict) -> list[tuple[str, int, dict]]:
    """
    Unpivot a wide pronostic record into (course_key, num_pmu, fields) tuples.

    Input keys look like:
      prono_rang_1_num, prono_rang_1_cote, prono_rang_2_num, ...
    """
    date_iso = str(rec.get("date_reunion_iso", ""))
    num_reunion = rec.get("numero_reunion")
    num_course = rec.get("num_course")
    source = rec.get("source_prono", "")

    if not date_iso or num_reunion is None or num_course is None:
        return []

    course_key = _make_course_key(date_iso, int(num_reunion), int(num_course))
    results: list[tuple[str, int, dict]] = []

    for rank in range(1, 20):  # up to 19 ranked horses
        num_key = f"prono_rang_{rank}_num"
        cote_key = f"prono_rang_{rank}_cote"
        pmu_num = rec.get(num_key)
        if pmu_num is None:
            break
        cote = rec.get(cote_key, "")
        results.append((
            course_key,
            int(pmu_num),
            {"prono_rang": rank, "prono_cote": cote, "source_prono": source},
        ))

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---------------------------------------------------
    for label, path in [("Pronostics", PRONO_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: build lookup from pronostics -----------------------------
    print(f"[1/2] Loading pronostics from {PRONO_PATH} ...")

    # Use streaming JSON array parsing to limit peak memory.
    # pronostics_all.json is a JSON array; we load it chunk-aware.
    lookup: dict[str, dict] = {}  # key = "course_key|num_pmu" -> fields
    prono_count = 0
    horse_entries = 0

    with open(PRONO_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    for rec in data:
        for course_key, num_pmu, fields in _parse_prono_record(rec):
            lk = f"{course_key}|{num_pmu}"
            # Keep first occurrence (highest priority source)
            if lk not in lookup:
                lookup[lk] = fields
                horse_entries += 1
        prono_count += 1

    # Free the raw list immediately
    del data

    print(f"       {prono_count:,} pronostic records -> {horse_entries:,} horse lookup entries")

    # --- Phase 2: stream master, enrich, write out -------------------------
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

            # Build lookup key from master record
            date_iso = str(rec.get("date_reunion_iso", ""))
            num_reunion = rec.get("numero_reunion")
            num_course = rec.get("numero_course")
            num_pmu = rec.get("num_pmu")

            if date_iso and num_reunion is not None and num_course is not None and num_pmu is not None:
                lk = f"{_make_course_key(date_iso, int(num_reunion), int(num_course))}|{int(num_pmu)}"
                hit = lookup.get(lk)
                if hit:
                    rec.update(hit)
                    matched += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0
    pct = (matched / total * 100) if total else 0
    print(f"Done in {elapsed:.1f}s. {total:,} partants, {matched:,} enriched ({pct:.1f}%).")
    print(f"Output: {MASTER_OUT}")


if __name__ == "__main__":
    main()
