#!/usr/bin/env python3
"""
scripts/merge_zeturf_to_master.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge ZeTurf course-level data from zeturf_data.jsonl (416K records) into the
partants master (partants_normalises.jsonl).

ZeTurf records are course-level (no individual horse odds in the JSONL).
Each record carries:
  - date, titre (e.g. "R1FR1VINCENNES"), url_course
  - nom_prix, distance, partants (count), pronos (e.g. "8-4-3-2-13...")
  - type, statut, paris

Merge strategy:
  - Parse the ``titre`` field to extract (reunion_number, hippodrome).
  - Parse ``n\u00b0`` field (e.g. "C11") for course number when available.
  - Build a lookup keyed by (date, hippodrome_normalise) at course level.
  - For each master partant, match on (date, hippodrome).  If the partant's
    num_pmu appears in the pronos string, derive a pronostic rank.

Fields added to each matching partant:
  - zeturf_pronos      : raw pronostic string from ZeTurf (course-level)
  - zeturf_prono_rang  : horse's rank in the ZeTurf pronos (null if not cited)
  - zeturf_nom_prix    : prize name from ZeTurf
  - zeturf_distance    : distance string from ZeTurf
  - zeturf_url         : ZeTurf course URL (alternative odds source link)
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
ZETURF_PATH = os.path.join(BASE, "output", "51_zeturf", "zeturf_data.jsonl")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_zeturf.jsonl")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Pattern for titre like "R1FR1VINCENNES" -> reunion=1, hippo="VINCENNES"
_RE_TITRE = re.compile(
    r"R(\d+)"           # R + reunion number
    r"[A-Z]{2}\d+"      # country code + sub-number (FR1, US1, etc.)
    r"(.+)",            # hippodrome name (rest of string)
    re.IGNORECASE,
)

# Pattern for n° like "C11" -> course number 11
_RE_COURSE_NUM = re.compile(r"C(\d+)", re.IGNORECASE)


def _normalise_hippo(name: str) -> str:
    """Lowercase, strip, collapse spaces, remove accents-ish."""
    return re.sub(r"\s+", " ", name.strip().lower().replace("-", " "))


def _parse_pronos(pronos_str: str) -> list[int]:
    """
    Parse a pronos string like "8-4-3-2-13..." into a list of horse numbers.
    Handles trailing "..." and various separators.
    """
    if not pronos_str or not isinstance(pronos_str, str):
        return []
    # Remove trailing dots
    cleaned = re.sub(r"\.+$", "", pronos_str.strip())
    if not cleaned:
        return []
    parts = re.split(r"[-,/\s]+", cleaned)
    result: list[int] = []
    for p in parts:
        p = p.strip()
        if p.isdigit():
            result.append(int(p))
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---------------------------------------------------
    for label, path in [("ZeTurf", ZETURF_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: build lookup from ZeTurf --------------------------------
    print(f"[1/2] Streaming ZeTurf data from {ZETURF_PATH} ...")

    # Key: (date, hippo_normalised, reunion_num) -> list of course records
    # We keep multiple courses per reunion.
    CourseInfo = dict  # type alias
    # Key: (date, hippo_norm, course_num_or_index) -> course info
    course_lookup: dict[str, CourseInfo] = {}

    # Also build a simpler (date, hippo_norm) -> list[CourseInfo] for fallback
    hippo_day_courses: dict[str, list[CourseInfo]] = defaultdict(list)

    total_zt = 0
    indexed = 0

    with open(ZETURF_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_zt += 1
            date = str(rec.get("date", ""))[:10]
            titre = rec.get("titre", "")
            if not date or not titre:
                continue

            m = _RE_TITRE.match(titre)
            if not m:
                continue

            reunion_num = int(m.group(1))
            hippo_raw = m.group(2)
            hippo_norm = _normalise_hippo(hippo_raw)

            # Try to extract course number from n° field
            course_num: int | None = None
            n_field = rec.get("n\u00b0", rec.get("n°", ""))
            if n_field:
                mc = _RE_COURSE_NUM.match(str(n_field))
                if mc:
                    course_num = int(mc.group(1))

            pronos_list = _parse_pronos(rec.get("pronos", ""))

            info: CourseInfo = {
                "zeturf_pronos": rec.get("pronos", ""),
                "zeturf_nom_prix": rec.get("course", rec.get("nom_prix", "")),
                "zeturf_distance": rec.get("distance", ""),
                "zeturf_url": rec.get("url_course", ""),
                "_pronos_list": pronos_list,
                "_reunion_num": reunion_num,
                "_course_num": course_num,
            }

            day_hippo_key = f"{date}|{hippo_norm}"
            hippo_day_courses[day_hippo_key].append(info)

            if course_num is not None:
                ck = f"{date}|{hippo_norm}|{reunion_num}|{course_num}"
                if ck not in course_lookup:
                    course_lookup[ck] = info
                    indexed += 1

    print(f"       {total_zt:,} zeturf records, {indexed:,} indexed by course, "
          f"{len(hippo_day_courses):,} day-hippo groups")

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

            date_iso = str(rec.get("date_reunion_iso", ""))
            hippo_norm = _normalise_hippo(rec.get("hippodrome_normalise", ""))
            num_reunion = rec.get("numero_reunion")
            num_course = rec.get("numero_course")
            num_pmu = rec.get("num_pmu")

            hit: CourseInfo | None = None

            if date_iso and hippo_norm:
                # Try exact match by reunion + course number
                if num_reunion is not None and num_course is not None:
                    ck = f"{date_iso}|{hippo_norm}|{int(num_reunion)}|{int(num_course)}"
                    hit = course_lookup.get(ck)

                # Fallback: match by day + hippo, find course by reunion number
                if hit is None and num_reunion is not None:
                    day_key = f"{date_iso}|{hippo_norm}"
                    candidates = hippo_day_courses.get(day_key, [])
                    for c in candidates:
                        if c.get("_reunion_num") == int(num_reunion):
                            if num_course is not None and c.get("_course_num") == int(num_course):
                                hit = c
                                break
                    # If still no exact course match, take first with same reunion
                    if hit is None:
                        for c in candidates:
                            if c.get("_reunion_num") == int(num_reunion):
                                hit = c
                                break

            if hit is not None:
                # Add course-level fields
                for fld in ("zeturf_pronos", "zeturf_nom_prix",
                            "zeturf_distance", "zeturf_url"):
                    val = hit.get(fld)
                    if val:
                        rec[fld] = val

                # Derive horse rank from pronos list
                pronos_list = hit.get("_pronos_list", [])
                if num_pmu is not None and pronos_list:
                    try:
                        rank = pronos_list.index(int(num_pmu)) + 1
                        rec["zeturf_prono_rang"] = rank
                    except ValueError:
                        rec["zeturf_prono_rang"] = None
                else:
                    rec["zeturf_prono_rang"] = None

                matched += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0
    pct = (matched / total * 100) if total else 0
    print(f"Done in {elapsed:.1f}s. {total:,} partants, {matched:,} enriched ({pct:.1f}%).")
    print(f"Output: {MASTER_OUT}")


if __name__ == "__main__":
    main()
