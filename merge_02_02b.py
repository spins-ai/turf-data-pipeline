#!/usr/bin/env python3
"""
merge_02_02b.py
Merge PMU (02) and Le Trot (02b) normalised courses and partants into unified files.

Strategy:
- Stream-parse all files with ijson to avoid loading multi-GB files into RAM.
- Build lookup indexes from Le Trot (smaller dataset) in memory.
- Stream PMU records, merge where matches exist, write to output.
- Append Le-Trot-only records at the end.
- Match courses on `cle_course` (date|hippodrome|Rx|Cx).
- Match partants on `cle_partant` (date|hippodrome|Rx|Cx|num) first,
  fallback to (course_uid, nom_cheval_normalised).
"""

import json
import os
import sys
import time
from collections import defaultdict
from decimal import Decimal

try:
    import ijson
except ImportError:
    sys.exit("ijson is required: pip install ijson")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE = os.path.dirname(os.path.abspath(__file__))

PMU_COURSES   = os.path.join(BASE, os.path.join(BASE, "output", "02_liste_courses", "courses_normalisees.json"))
PMU_PARTANTS  = os.path.join(BASE, os.path.join(BASE, "output", "02_liste_courses", "partants_normalises.json"))
LT_COURSES    = os.path.join(BASE, os.path.join(BASE, "output", "02b_scraper_letrot", "courses_normalisees.json"))
LT_PARTANTS   = os.path.join(BASE, os.path.join(BASE, "output", "02b_scraper_letrot", "partants_normalises.json"))

OUT_DIR       = os.path.join(BASE, os.path.join(BASE, "output", "02_merged"))
OUT_COURSES   = os.path.join(OUT_DIR, "courses_normalisees.json")
OUT_PARTANTS  = os.path.join(OUT_DIR, "partants_normalises.json")

SYMLINK_DIR   = os.path.join(BASE, "pipeline/data/baseline_ml")

os.makedirs(OUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class DecimalEncoder(json.JSONEncoder):
    """Serialize Decimal as float-like numbers."""
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


def normalise_name(name):
    """Lower-case, strip accents (basic), collapse whitespace."""
    if not name:
        return ""
    return name.strip().upper()


def merge_record(primary, secondary):
    """Merge secondary into primary: fill None/empty fields from secondary.
    Returns merged dict (new object)."""
    merged = dict(primary)
    for k, v in secondary.items():
        if k in ("source", "course_uid", "reunion_uid", "partant_uid",
                 "timestamp_collecte", "url_source"):
            continue  # skip identity / meta fields from secondary
        cur = merged.get(k)
        # Fill if primary value is missing / empty
        if cur is None or cur == "" or cur == [] or cur == {}:
            merged[k] = v
    return merged


def build_course_key(rec):
    """Build a matching key from date + hippodrome + numero_course."""
    date = rec.get("date_reunion_iso", "")
    hippo = (rec.get("hippodrome_normalise") or "").lower().strip()
    num = rec.get("numero_course")
    if date and hippo and num is not None:
        return f"{date}|{hippo}|{num}"
    return None


def build_partant_key_primary(rec):
    """cle_partant is the best key."""
    cle = rec.get("cle_partant")
    if cle:
        return cle
    return None


def build_partant_key_secondary(rec):
    """Fallback: date|hippo|num_course|nom_cheval."""
    date = rec.get("date_reunion_iso", "")
    hippo = (rec.get("hippodrome_normalise") or "").lower().strip()
    num_c = rec.get("numero_course")
    nom = normalise_name(rec.get("nom_cheval", ""))
    if date and hippo and num_c is not None and nom:
        return f"{date}|{hippo}|{num_c}|{nom}"
    return None


# ---------------------------------------------------------------------------
# Phase 1: Index Le Trot courses (small, ~36k records, ~41 MB)
# ---------------------------------------------------------------------------
print("=== Phase 1: Indexing Le Trot courses ===")
t0 = time.time()

lt_courses_by_key = {}   # course_key -> record
lt_courses_by_uid = {}   # course_uid -> course_key (for partant mapping)

with open(LT_COURSES, "rb") as f:
    for rec in ijson.items(f, "item"):
        key = build_course_key(rec)
        if key:
            lt_courses_by_key[key] = rec
        uid = rec.get("course_uid")
        if uid and key:
            lt_courses_by_uid[uid] = key

print(f"  Le Trot courses indexed: {len(lt_courses_by_key):,} "
      f"(took {time.time()-t0:.1f}s)")


# ---------------------------------------------------------------------------
# Phase 2: Stream PMU courses -> merge -> write
# ---------------------------------------------------------------------------
print("\n=== Phase 2: Merging courses ===")
t0 = time.time()

merged_course_keys = set()
pmu_only_count = 0
merged_count = 0
total_courses = 0
# Map PMU course_uid -> merged course_uid for partant phase
pmu_uid_to_course_key = {}

# We also need to map: for merged courses, remember the LT course_uid
# so we can match LT partants to their merged course later.
lt_uid_merged = set()  # LT course_uids that were merged

with open(OUT_COURSES, "w", encoding="utf-8") as out:
    out.write("[\n")
    first = True

    # Stream PMU courses
    with open(PMU_COURSES, "rb") as f:
        for rec in ijson.items(f, "item"):
            total_courses += 1
            key = build_course_key(rec)
            pmu_uid = rec.get("course_uid")
            if pmu_uid and key:
                pmu_uid_to_course_key[pmu_uid] = key

            lt_rec = lt_courses_by_key.get(key) if key else None

            if lt_rec:
                # Merge
                merged = merge_record(rec, lt_rec)
                merged["source"] = "pmu+letrot"
                merged_course_keys.add(key)
                merged_count += 1
                lt_uid = lt_rec.get("course_uid")
                if lt_uid:
                    lt_uid_merged.add(lt_uid)
            else:
                merged = dict(rec)
                if "source" not in merged or not merged["source"]:
                    merged["source"] = "pmu"
                pmu_only_count += 1

            if not first:
                out.write(",\n")
            json.dump(merged, out, cls=DecimalEncoder, ensure_ascii=False)
            first = False

            if total_courses % 50000 == 0:
                print(f"  ... processed {total_courses:,} PMU courses")

    # Append Le-Trot-only courses
    lt_only_count = 0
    for key, rec in lt_courses_by_key.items():
        if key not in merged_course_keys:
            lt_only_count += 1
            total_courses += 1
            rec_out = dict(rec)
            rec_out["source"] = "letrot"
            if not first:
                out.write(",\n")
            json.dump(rec_out, out, cls=DecimalEncoder, ensure_ascii=False)
            first = False

    out.write("\n]\n")

print(f"  Courses done in {time.time()-t0:.1f}s")
print(f"  Total courses written: {total_courses:,}")
print(f"    PMU-only:      {pmu_only_count:,}")
print(f"    Merged:        {merged_count:,}")
print(f"    Le-Trot-only:  {lt_only_count:,}")


# ---------------------------------------------------------------------------
# Phase 3: Index Le Trot partants (252k records, ~443 MB -- fits in RAM)
# ---------------------------------------------------------------------------
print("\n=== Phase 3: Indexing Le Trot partants ===")
t0 = time.time()

lt_partants_by_cle = {}       # cle_partant -> record
lt_partants_by_fallback = {}  # fallback key -> record
lt_partants_by_course_uid = defaultdict(list)  # for LT-only courses

with open(LT_PARTANTS, "rb") as f:
    for rec in ijson.items(f, "item"):
        pk = build_partant_key_primary(rec)
        if pk:
            lt_partants_by_cle[pk] = rec

        fk = build_partant_key_secondary(rec)
        if fk:
            lt_partants_by_fallback[fk] = rec

        cuid = rec.get("course_uid")
        if cuid:
            lt_partants_by_course_uid[cuid].append(rec)

print(f"  Le Trot partants indexed: {len(lt_partants_by_cle):,} by cle, "
      f"{len(lt_partants_by_fallback):,} by fallback "
      f"(took {time.time()-t0:.1f}s)")


# ---------------------------------------------------------------------------
# Phase 4: Stream PMU partants -> merge -> write
# ---------------------------------------------------------------------------
print("\n=== Phase 4: Merging partants (streaming PMU, ~2.7M records) ===")
t0 = time.time()

matched_lt_cles = set()
matched_lt_fallbacks = set()
p_pmu_only = 0
p_merged = 0
p_total = 0

with open(OUT_PARTANTS, "w", encoding="utf-8") as out:
    out.write("[\n")
    first = True

    with open(PMU_PARTANTS, "rb") as f:
        for rec in ijson.items(f, "item"):
            p_total += 1
            lt_rec = None

            # Try primary key
            pk = build_partant_key_primary(rec)
            if pk and pk in lt_partants_by_cle:
                lt_rec = lt_partants_by_cle[pk]
                matched_lt_cles.add(pk)

            # Try fallback key
            if lt_rec is None:
                fk = build_partant_key_secondary(rec)
                if fk and fk in lt_partants_by_fallback:
                    lt_rec = lt_partants_by_fallback[fk]
                    matched_lt_fallbacks.add(fk)

            if lt_rec:
                merged = merge_record(rec, lt_rec)
                merged["source"] = "pmu+letrot"
                p_merged += 1
            else:
                merged = dict(rec)
                if "source" not in merged or not merged["source"]:
                    merged["source"] = "pmu"
                p_pmu_only += 1

            if not first:
                out.write(",\n")
            json.dump(merged, out, cls=DecimalEncoder, ensure_ascii=False)
            first = False

            if p_total % 500000 == 0:
                print(f"  ... processed {p_total:,} PMU partants")

    # Append Le-Trot-only partants (from non-merged courses)
    p_lt_only = 0
    for lt_course_uid, partants in lt_partants_by_course_uid.items():
        if lt_course_uid in lt_uid_merged:
            # This course was merged; partants already handled above
            # But some LT partants may not have matched any PMU partant
            for prec in partants:
                pk = build_partant_key_primary(prec)
                fk = build_partant_key_secondary(prec)
                if (pk and pk in matched_lt_cles) or (fk and fk in matched_lt_fallbacks):
                    continue  # already merged
                # Unmatched LT partant from a merged course -> add it
                p_lt_only += 1
                p_total += 1
                prec_out = dict(prec)
                prec_out["source"] = "letrot"
                if not first:
                    out.write(",\n")
                json.dump(prec_out, out, cls=DecimalEncoder, ensure_ascii=False)
                first = False
        else:
            # LT-only course -> all partants are new
            for prec in partants:
                p_lt_only += 1
                p_total += 1
                prec_out = dict(prec)
                prec_out["source"] = "letrot"
                if not first:
                    out.write(",\n")
                json.dump(prec_out, out, cls=DecimalEncoder, ensure_ascii=False)
                first = False

    out.write("\n]\n")

print(f"  Partants done in {time.time()-t0:.1f}s")
print(f"  Total partants written: {p_total:,}")
print(f"    PMU-only:      {p_pmu_only:,}")
print(f"    Merged:        {p_merged:,}")
print(f"      (by cle_partant: {len(matched_lt_cles):,}, by fallback: {len(matched_lt_fallbacks):,})")
print(f"    Le-Trot-only:  {p_lt_only:,}")


# ---------------------------------------------------------------------------
# Phase 5: Update symlinks
# ---------------------------------------------------------------------------
print("\n=== Phase 5: Updating symlinks ===")

for fname in ("courses_normalisees.json", "partants_normalises.json"):
    link_path = os.path.join(SYMLINK_DIR, fname)
    target = os.path.relpath(
        os.path.join(OUT_DIR, fname),
        start=SYMLINK_DIR
    )
    if os.path.islink(link_path):
        old_target = os.readlink(link_path)
        print(f"  {fname}: {old_target} -> {target}")
        os.remove(link_path)
    elif os.path.exists(link_path):
        print(f"  {fname}: removing existing file, replacing with symlink")
        os.remove(link_path)
    else:
        print(f"  {fname}: creating new symlink -> {target}")
    os.symlink(target, link_path)

print("\n=== Done ===")

# Final file sizes
for p in (OUT_COURSES, OUT_PARTANTS):
    sz = os.path.getsize(p) / (1024*1024)
    print(f"  {os.path.basename(p)}: {sz:,.1f} MB")
