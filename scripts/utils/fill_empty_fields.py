#!/usr/bin/env python3
"""
fill_empty_fields.py
====================

Fills empty/missing fields in partants_normalises.json and courses_normalisees.json
by cross-referencing data, parsing text fields, and fixing encoding issues.

Fields handled:
  1. condition_age   (courses)  - Parsed from conditions_texte via regex
  2. is_disqualifie  (partants) - Cross-referenced with course incidents & ordre_arrivee
  3. penetrometre    (courses)  - Propagated from same hippodrome +/-3 days
  4. pays_cheval     (partants) - Extracted from nom_cheval suffix e.g. "(IRE)"
  5. UTF-8 encoding  (both)     - Fixes double-encoded UTF-8

Input:
    output/02_merged/courses_normalisees.json   (~375 MB, ~257k courses)
    output/02_merged/partants_normalises.json    (~4.6 GB, ~2.9M partants)

Output (new files, originals untouched):
    output/02_filled/courses_normalisees.json
    output/02_filled/partants_normalises.json

The partants file is stream-processed via ijson to avoid loading 4.6 GB into RAM.

Usage:
    python3 fill_empty_fields.py
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json
import re
import time
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import ijson


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal objects returned by ijson."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Return int if it's a whole number, else float
            if obj == int(obj):
                return int(obj)
            return float(obj)
        return super().default(obj)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

# Input — merged files
COURSES_PATH = BASE_DIR / "../../output" / "02_merged" / "courses_normalisees.json"
PARTANTS_PATH = BASE_DIR / "../../output" / "02_merged" / "partants_normalises.json"
METEO_PATH = BASE_DIR / "../../output" / "13_meteo_historique" / "meteo_historique.json"

# Output — new directory, never overwrites originals
OUT_DIR = BASE_DIR / "../../output" / "02_filled"
COURSES_OUT = OUT_DIR / "courses_normalisees.json"
PARTANTS_OUT = OUT_DIR / "partants_normalises.json"

# ---------------------------------------------------------------------------
# 1. condition_age --- parse from conditions_texte
# ---------------------------------------------------------------------------

def parse_condition_age(conditions_texte: str) -> str:
    """Extract a normalised condition_age string from free-text conditions.

    Returns strings like:
        "3_ans", "4_ans_et_plus", "3_a_5_ans", "2_ans", "tous_ages"
    or "" if nothing could be inferred.
    """
    if not conditions_texte:
        return ""

    txt = conditions_texte.lower()

    # Fix double-encoded UTF-8 before parsing
    txt = fix_encoding(txt)

    # "tous chevaux" / "tous ages" / "tous ages"
    if re.search(r"tous\s+(?:chevaux|[aa]ges?)", txt):
        return "tous_ages"

    # "de X ans et au-dessus" / "X ans et plus"
    m = re.search(r"(\d{1,2})\s*ans\s+et\s+(?:au[- ]?dessus|plus)", txt)
    if m:
        return f"{m.group(1)}_ans_et_plus"

    # "de X a Y ans"
    m = re.search(r"(?:de\s+)?(\d{1,2})\s*[aa]\s*(\d{1,2})\s*ans", txt)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if a != b:
            return f"{a}_a_{b}_ans"
        else:
            return f"{a}_ans"

    # "de X ans" / "pour ... X ans" (exact age)
    m = re.search(r"(\d{1,2})\s*ans", txt)
    if m:
        age = int(m.group(1))
        if 1 <= age <= 12:  # sanity check
            return f"{age}_ans"

    return ""


# ---------------------------------------------------------------------------
# 2. is_disqualifie --- cross-reference with course incidents
# ---------------------------------------------------------------------------

def build_disqualified_set(courses: list[dict]) -> dict[str, set[int]]:
    """Build a mapping of course_uid -> set of num_pmu that were disqualified."""
    disq_map: dict[str, set[int]] = {}

    for course in courses:
        uid = course.get("course_uid", "")
        if not uid:
            continue

        nums_disq: set[int] = set()

        for inc in (course.get("incidents") or []):
            inc_type = (inc.get("type") or "").upper()
            if any(kw in inc_type for kw in (
                "DISQUALIFIE", "ARRETE", "TOMBE", "DISTANCE",
                "DEROBE", "RESTE_AU_POTEAU",
            )):
                for n in (inc.get("numeroParticipants") or []):
                    if isinstance(n, int):
                        nums_disq.add(n)

        if nums_disq:
            disq_map[uid] = nums_disq

    return disq_map


def build_placed_set(courses: list[dict]) -> dict[str, set[int]]:
    """Build a mapping of course_uid -> set of num_pmu that appear in ordre_arrivee."""
    placed_map: dict[str, set[int]] = {}
    for course in courses:
        uid = course.get("course_uid", "")
        if not uid:
            continue
        oa = course.get("ordre_arrivee") or []
        nums: set[int] = set()
        for group in oa:
            if isinstance(group, list):
                for n in group:
                    if isinstance(n, int):
                        nums.add(n)
        if nums:
            placed_map[uid] = nums
    return placed_map


def _to_int(val) -> int | None:
    """Convert int or Decimal to int, or return None."""
    if isinstance(val, int):
        return val
    if isinstance(val, Decimal):
        try:
            return int(val)
        except (ValueError, OverflowError):
            return None
    return None


def fix_is_disqualifie(
    partant: dict,
    disq_map: dict[str, set[int]],
    placed_map: dict[str, set[int]],
) -> bool | None:
    """Return the corrected is_disqualifie value, or None if uncertain."""
    uid = partant.get("course_uid", "")
    num = _to_int(partant.get("num_pmu"))
    statut = (partant.get("statut") or "").lower()

    if statut == "non_partant":
        return False

    if uid in disq_map and num is not None and num in disq_map[uid]:
        return True

    if uid in placed_map and num is not None:
        if num in placed_map[uid]:
            return False

    pos = _to_int(partant.get("position_arrivee"))
    if pos is not None and pos >= 1:
        return False

    return None


# ---------------------------------------------------------------------------
# 3. penetrometre --- propagate from same hippodrome +/-3 days
# ---------------------------------------------------------------------------

def build_penetrometre_index(courses: list[dict]) -> dict[str, list[tuple[date, str]]]:
    """Index known penetrometre values by hippodrome_normalise."""
    idx: dict[str, list[tuple[date, str]]] = defaultdict(list)

    for c in courses:
        val = (c.get("penetrometre") or "").strip()
        if not val:
            continue
        hippo = (c.get("hippodrome_normalise") or "").strip()
        if not hippo:
            continue
        d = _parse_date(c.get("date_reunion_iso", ""))
        if d:
            idx[hippo].append((d, val))

    for hippo in idx:
        idx[hippo].sort(key=lambda x: x[0])

    return dict(idx)


def build_meteo_rain_index(meteo_path: Path) -> dict[str, dict[str, float]]:
    """Build {hippo: {date_iso: precip_mm}} from meteo data."""
    rain: dict[str, dict[str, float]] = defaultdict(dict)

    if not meteo_path.exists():
        return dict(rain)

    try:
        with open(meteo_path, "r", encoding="utf-8") as f:
            meteo = json.load(f)
    except (json.JSONDecodeError, OSError):
        return dict(rain)

    for rec in meteo:
        hippo = (rec.get("hippodrome_normalise") or "").strip()
        d = rec.get("date_reunion_iso", "")
        precip = rec.get("precipitation_mm") or rec.get("precip_total_mm")
        if hippo and d and precip is not None:
            try:
                rain[hippo][d] = float(precip)
            except (ValueError, TypeError):
                pass

    return dict(rain)


def infer_penetrometre(
    course: dict,
    pene_idx: dict[str, list[tuple[date, str]]],
    rain_idx: dict[str, dict[str, float]],
) -> str:
    """Try to infer penetrometre for a course.

    Strategy 1: Same hippodrome, +/-3 days -- take the closest known value.
    Strategy 2: If rain data shows >5mm in last 3 days -> "souple",
                >15mm -> "tres_souple", 0mm -> "bon".
    """
    hippo = (course.get("hippodrome_normalise") or "").strip()
    d = _parse_date(course.get("date_reunion_iso", ""))
    if not hippo or not d:
        return ""

    entries = pene_idx.get(hippo, [])
    best_val = ""
    best_delta = 999
    for entry_date, entry_val in entries:
        delta = abs((d - entry_date).days)
        if delta <= 3 and delta < best_delta:
            best_delta = delta
            best_val = entry_val

    if best_val:
        return best_val

    hippo_rain = rain_idx.get(hippo, {})
    if hippo_rain:
        total_rain_3d = 0.0
        for offset in range(0, 4):
            day_iso = (d - timedelta(days=offset)).isoformat()
            total_rain_3d += hippo_rain.get(day_iso, 0.0)

        if total_rain_3d > 15:
            return "tres_souple"
        elif total_rain_3d > 5:
            return "souple"
        elif total_rain_3d == 0:
            return "bon"
        else:
            return "leger"

    return ""


def _parse_date(iso: str) -> date | None:
    try:
        return date.fromisoformat(iso)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# 4. pays_cheval --- extract from horse name suffix
# ---------------------------------------------------------------------------

COUNTRY_CODE_MAP = {
    "FR": "France",
    "IRE": "Irlande",
    "GB": "Grande-Bretagne",
    "USA": "Etats-Unis",
    "GER": "Allemagne",
    "ITY": "Italie",
    "SPA": "Espagne",
    "BEL": "Belgique",
    "SWE": "Suede",
    "NOR": "Norvege",
    "DEN": "Danemark",
    "NZ": "Nouvelle-Zelande",
    "AUS": "Australie",
    "BRZ": "Bresil",
    "ARG": "Argentine",
    "JPN": "Japon",
    "CAN": "Canada",
    "HOL": "Pays-Bas",
    "SWI": "Suisse",
    "CZE": "Tchequie",
    "HUN": "Hongrie",
    "POL": "Pologne",
    "FIN": "Finlande",
    "TUR": "Turquie",
    "SAF": "Afrique-du-Sud",
    "HK": "Hong-Kong",
    "MAC": "Macao",
    "SGP": "Singapour",
    "UAE": "Emirats-Arabes-Unis",
    "PER": "Perou",
    "URU": "Uruguay",
    "CHI": "Chili",
}

_COUNTRY_RE = re.compile(r"\(([A-Z]{2,4})\)\s*$")


def extract_pays_cheval(partant: dict) -> str:
    """Extract country from horse name suffix or existing fields."""
    existing = (partant.get("pays_cheval") or "").strip()
    if existing:
        return existing

    nom = (partant.get("nom_cheval") or "").strip()
    m = _COUNTRY_RE.search(nom)
    if m:
        code = m.group(1).upper()
        return COUNTRY_CODE_MAP.get(code, code)

    discipline = (partant.get("discipline") or "").lower()
    if "trot" in discipline:
        return "France"

    return ""


# ---------------------------------------------------------------------------
# 5. UTF-8 encoding fix --- double-encoded characters
# ---------------------------------------------------------------------------

_ENCODING_FIXES = {
    "\u00c3\u00a9": "\u00e9",   # e with accent
    "\u00c3\u00a8": "\u00e8",
    "\u00c3\u00aa": "\u00ea",
    "\u00c3\u00ab": "\u00eb",
    "\u00c3\u00a0": "\u00e0",
    "\u00c3\u00a2": "\u00e2",
    "\u00c3\u00a4": "\u00e4",
    "\u00c3\u00af": "\u00ef",
    "\u00c3\u00ae": "\u00ee",
    "\u00c3\u00b4": "\u00f4",
    "\u00c3\u00b6": "\u00f6",
    "\u00c3\u00b9": "\u00f9",
    "\u00c3\u00bb": "\u00fb",
    "\u00c3\u00bc": "\u00fc",
    "\u00c3\u00a7": "\u00e7",
    "\u00c3\u00b1": "\u00f1",
    "\u00c3\u0080": "\u00c0",
    "\u00c3\u0089": "\u00c9",
    "\u00c3\u0088": "\u00c8",
    "\u00c3\u008a": "\u00ca",
    "\u00c3\u0087": "\u00c7",
    "\u00c3\u0094": "\u00d4",
    "\u00c3\u009c": "\u00dc",
    "\u00c3\u00a1": "\u00e1",
    "\u00c3\u00b3": "\u00f3",
    "\u00c3\u00ad": "\u00ed",
    "\u00c3\u00ba": "\u00fa",
    "\u00c5\u0093": "oe",
    "\u00c2\u00b0": "\u00b0",
    "\u00c2\u00ab": "\u00ab",
    "\u00c2\u00bb": "\u00bb",
    "\u00c2 ": " ",
}

_ENCODING_RE = re.compile(
    "|".join(re.escape(k) for k in sorted(_ENCODING_FIXES, key=len, reverse=True))
)


def fix_encoding(text: str) -> str:
    """Fix common double-UTF8-encoded characters in a string."""
    if not text or not isinstance(text, str):
        return text

    if "\u00c3" not in text and "\u00c2" not in text:
        return text

    try:
        fixed = text.encode("latin-1").decode("utf-8")
        if fixed.count("\u00c3") < text.count("\u00c3"):
            return fixed
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass

    return _ENCODING_RE.sub(lambda m: _ENCODING_FIXES[m.group(0)], text)


def fix_record_encoding(record: dict) -> tuple[dict, int]:
    """Fix encoding in all string values of a record."""
    fixes = 0
    for key, val in record.items():
        if isinstance(val, str) and ("\u00c3" in val or "\u00c2" in val):
            fixed = fix_encoding(val)
            if fixed != val:
                record[key] = fixed
                fixes += 1
    return record, fixes


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_courses(courses: list[dict], pene_idx, rain_idx) -> dict:
    """Process all courses in-memory and return fill statistics."""
    stats = {
        "total": len(courses),
        "condition_age_before": 0,
        "condition_age_after": 0,
        "penetrometre_before": 0,
        "penetrometre_after": 0,
        "encoding_fixes": 0,
    }

    for i, course in enumerate(courses):
        # --- condition_age ---
        existing_age = (course.get("condition_age") or "").strip()
        if existing_age:
            stats["condition_age_before"] += 1
            stats["condition_age_after"] += 1
        else:
            inferred = parse_condition_age(course.get("conditions_texte", ""))
            if inferred:
                course["condition_age"] = inferred
                stats["condition_age_after"] += 1

        # --- penetrometre ---
        existing_pene = (course.get("penetrometre") or "").strip()
        if existing_pene:
            stats["penetrometre_before"] += 1
            stats["penetrometre_after"] += 1
        else:
            inferred = infer_penetrometre(course, pene_idx, rain_idx)
            if inferred:
                course["penetrometre"] = inferred
                stats["penetrometre_after"] += 1

        # --- encoding fix ---
        _, nfix = fix_record_encoding(course)
        stats["encoding_fixes"] += nfix

        if (i + 1) % 50_000 == 0:
            print(f"  [courses] {i+1:>10,} / {len(courses):,} processed")

    return stats


def stream_process_partants(
    partants_path: Path,
    partants_out: Path,
    disq_map: dict[str, set[int]],
    placed_map: dict[str, set[int]],
) -> dict:
    """Stream-process the large partants file record by record using ijson.

    Reads one JSON object at a time, processes it, and writes immediately
    to the output file. Peak memory stays at ~one record.
    """
    stats = {
        "total": 0,
        "is_disqualifie_corrections": 0,
        "pays_cheval_before": 0,
        "pays_cheval_after": 0,
        "encoding_fixes": 0,
    }

    with open(partants_path, "rb") as fin, \
         open(partants_out, "w", encoding="utf-8") as fout:

        fout.write("[")
        first = True

        for partant in ijson.items(fin, "item"):
            stats["total"] += 1

            # --- is_disqualifie ---
            corrected = fix_is_disqualifie(partant, disq_map, placed_map)
            if corrected is not None:
                old_val = partant.get("is_disqualifie")
                if old_val != corrected:
                    partant["is_disqualifie"] = corrected
                    stats["is_disqualifie_corrections"] += 1

            # --- pays_cheval ---
            existing_pays = (partant.get("pays_cheval") or "").strip()
            if existing_pays:
                stats["pays_cheval_before"] += 1

            inferred_pays = extract_pays_cheval(partant)
            if inferred_pays:
                partant["pays_cheval"] = inferred_pays
                stats["pays_cheval_after"] += 1

            # --- encoding fix ---
            _, nfix = fix_record_encoding(partant)
            stats["encoding_fixes"] += nfix

            # --- write record ---
            if not first:
                fout.write(",\n")
            else:
                fout.write("\n")
                first = False

            json.dump(partant, fout, ensure_ascii=False, cls=DecimalEncoder)

            i = stats["total"]
            if i % 100_000 == 0:
                print(f"  [partants] {i:>10,} processed ...", flush=True)

        fout.write("\n]")

    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    t0 = time.time()

    # Ensure output directory exists
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Load courses (375 MB -- fits in memory, needed for building indices)
    # ------------------------------------------------------------------
    print(f"Loading courses from {COURSES_PATH} ...")
    with open(COURSES_PATH, "r", encoding="utf-8") as f:
        courses = json.load(f)
    print(f"  Loaded {len(courses):,} courses in {time.time()-t0:.1f}s")

    # ------------------------------------------------------------------
    # Build indices (from courses data, kept in memory)
    # ------------------------------------------------------------------
    print("Building indices ...")
    t2 = time.time()

    pene_idx = build_penetrometre_index(courses)
    print(f"  penetrometre index: {sum(len(v) for v in pene_idx.values()):,} entries "
          f"across {len(pene_idx)} hippodromes")

    rain_idx = build_meteo_rain_index(METEO_PATH)
    print(f"  meteo rain index: {sum(len(v) for v in rain_idx.values()):,} entries")

    disq_map = build_disqualified_set(courses)
    print(f"  disqualified index: {sum(len(v) for v in disq_map.values()):,} "
          f"disqualified entries across {len(disq_map)} courses")

    placed_map = build_placed_set(courses)
    print(f"  placed index: {sum(len(v) for v in placed_map.values()):,} "
          f"placed entries across {len(placed_map)} courses")

    print(f"  Indices built in {time.time()-t2:.1f}s")

    # ------------------------------------------------------------------
    # Process courses (in memory -- 375 MB is fine)
    # ------------------------------------------------------------------
    print("\nProcessing courses ...")
    t3 = time.time()
    course_stats = process_courses(courses, pene_idx, rain_idx)
    print(f"  Done in {time.time()-t3:.1f}s")

    # Save courses
    print(f"\nSaving courses -> {COURSES_OUT} ...")
    t5 = time.time()
    with open(COURSES_OUT, "w", encoding="utf-8") as f:
        json.dump(courses, f, ensure_ascii=False, indent=None)
    print(f"  Saved in {time.time()-t5:.1f}s")

    # Free courses from memory before processing partants
    del courses
    import gc; gc.collect()

    # ------------------------------------------------------------------
    # Stream-process partants (4.6 GB -- one record at a time via ijson)
    # ------------------------------------------------------------------
    print(f"\nStream-processing partants from {PARTANTS_PATH} ...")
    print(f"  (writing to {PARTANTS_OUT})")
    t4 = time.time()
    partant_stats = stream_process_partants(
        PARTANTS_PATH, PARTANTS_OUT, disq_map, placed_map,
    )
    print(f"  Done in {time.time()-t4:.1f}s")

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("FILL RATES REPORT")
    print("=" * 70)

    total_c = course_stats["total"]
    print(f"\n--- Courses ({total_c:,} records) ---")
    print(f"  condition_age : {course_stats['condition_age_before']:>8,} -> "
          f"{course_stats['condition_age_after']:>8,}  "
          f"({100*course_stats['condition_age_before']/total_c:.1f}% -> "
          f"{100*course_stats['condition_age_after']/total_c:.1f}%)")
    print(f"  penetrometre  : {course_stats['penetrometre_before']:>8,} -> "
          f"{course_stats['penetrometre_after']:>8,}  "
          f"({100*course_stats['penetrometre_before']/total_c:.1f}% -> "
          f"{100*course_stats['penetrometre_after']/total_c:.1f}%)")
    print(f"  encoding fixes: {course_stats['encoding_fixes']:>8,} fields corrected")

    total_p = partant_stats["total"]
    print(f"\n--- Partants ({total_p:,} records) ---")
    print(f"  is_disqualifie corrections: {partant_stats['is_disqualifie_corrections']:>8,}")
    print(f"  pays_cheval   : {partant_stats['pays_cheval_before']:>8,} -> "
          f"{partant_stats['pays_cheval_after']:>8,}  "
          f"({100*partant_stats['pays_cheval_before']/total_p:.1f}% -> "
          f"{100*partant_stats['pays_cheval_after']/total_p:.1f}%)")
    print(f"  encoding fixes: {partant_stats['encoding_fixes']:>8,} fields corrected")

    print(f"\nOutput files:")
    print(f"  {COURSES_OUT}")
    print(f"  {PARTANTS_OUT}")
    print(f"\nTotal time: {time.time()-t0:.1f}s")
    print("Done.")


if __name__ == "__main__":
    main()
