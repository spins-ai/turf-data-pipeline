#!/usr/bin/env python3
"""
mega_merge_courses.py
Enriches courses_normalisees.json by merging all available course-level data sources.
Outputs: output/02_liste_courses/courses_enrichies.json
"""

import json
import sys
import time
import os
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))


def load_json(path):
    """Load a JSON file and return its content."""
    full = os.path.join(BASE, path)
    print(f"  Loading {path} ...", end=" ", flush=True)
    t0 = time.time()
    with open(full, "r", encoding="utf-8") as f:
        data = json.load(f)
    dt = time.time() - t0
    if isinstance(data, list):
        print(f"{len(data):,} records in {dt:.1f}s")
    elif isinstance(data, dict):
        print(f"{len(data):,} keys in {dt:.1f}s")
    return data


def load_json_tolerant(path):
    """Load a JSON file with tolerance for truncation/corruption."""
    full = os.path.join(BASE, path)
    print(f"  Loading {path} (tolerant) ...", end=" ", flush=True)
    t0 = time.time()
    with open(full, "r", encoding="utf-8") as f:
        raw = f.read()
    # Try full parse first
    try:
        data = json.loads(raw)
        dt = time.time() - t0
        print(f"{len(data):,} records in {dt:.1f}s")
        return data
    except json.JSONDecodeError as e:
        # Find last complete record by locating the last '},'  or '}]'
        pos = e.pos
        print(f"JSON error at pos {pos:,}, attempting recovery...", end=" ", flush=True)
        # Truncate to last complete object and close the array
        truncated = raw[:pos]
        # Find the last complete JSON object boundary
        last_brace = truncated.rfind("}")
        if last_brace > 0:
            truncated = truncated[:last_brace + 1] + "]"
            try:
                data = json.loads(truncated)
                dt = time.time() - t0
                print(f"recovered {len(data):,} records in {dt:.1f}s")
                return data
            except json.JSONDecodeError:
                pass
        print("recovery failed, skipping")
        return []


def build_index(records, key="course_uid"):
    """Build a dict mapping key -> record for a list of dicts."""
    idx = {}
    for rec in records:
        k = rec.get(key)
        if k:
            idx[k] = rec
    return idx


def main():
    print("=" * 70)
    print("MEGA MERGE COURSES - Enrichissement du dataset courses")
    print("=" * 70)
    t_start = time.time()

    # ─── 1. Load main courses ───────────────────────────────────────────
    print("\n[1/7] Loading main courses dataset...")
    courses = load_json("output/02_liste_courses/courses_normalisees.json")
    n_courses = len(courses)
    fields_before = set()
    for c in courses[:100]:
        fields_before.update(c.keys())
    print(f"  -> {n_courses:,} courses, {len(fields_before)} fields")

    # Build course_uid index for reference
    courses_by_uid = {}
    for i, c in enumerate(courses):
        uid = c.get("course_uid")
        if uid:
            courses_by_uid[uid] = i

    # ─── 2. Merge rapports_complets ─────────────────────────────────────
    print("\n[2/7] Merging rapports_complets (payout data)...")
    rapports = load_json("output/rapports_merged/rapports_complets.json")
    rapports_idx = build_index(rapports, "course_uid")

    # Fields to skip (already in courses or join keys)
    rapports_skip = {"course_uid", "date_reunion_iso", "discipline", "distance",
                     "hippodrome", "num_course", "numero_reunion"}
    rapports_fields = set()
    for r in rapports[:100]:
        rapports_fields.update(r.keys())
    rapports_new = rapports_fields - rapports_skip
    print(f"  -> {len(rapports_idx):,} indexed, {len(rapports_new)} new fields")

    matched_rapports = 0
    for c in courses:
        uid = c.get("course_uid")
        if uid and uid in rapports_idx:
            r = rapports_idx[uid]
            for k in rapports_new:
                if k in r:
                    c[k] = r[k]
            matched_rapports += 1
    print(f"  -> Matched: {matched_rapports:,} / {n_courses:,} ({100*matched_rapports/n_courses:.1f}%)")
    del rapports, rapports_idx

    # ─── 3. Merge meteo_complete ────────────────────────────────────────
    print("\n[3/7] Merging meteo_complete (weather data)...")
    meteo = load_json("output/meteo_complete/meteo_complete.json")
    meteo_idx = build_index(meteo, "course_uid")

    meteo_skip = {"course_uid", "date_reunion_iso", "hippodrome_normalise",
                  "reunion_uid", "source"}
    meteo_fields = set()
    for m in meteo[:100]:
        meteo_fields.update(m.keys())
    meteo_new = meteo_fields - meteo_skip
    print(f"  -> {len(meteo_idx):,} indexed, {len(meteo_new)} new fields")

    matched_meteo = 0
    for c in courses:
        uid = c.get("course_uid")
        if uid and uid in meteo_idx:
            m = meteo_idx[uid]
            for k in meteo_new:
                if k in m:
                    # Prefix meteo fields that don't already have a clear prefix
                    if k.startswith(("meteo_", "is_", "weather_", "precip_",
                                     "temp_", "wind_", "humidity")):
                        c[k] = m[k]
                    else:
                        c["meteo_" + k] = m[k]
            matched_meteo += 1
    print(f"  -> Matched: {matched_meteo:,} / {n_courses:,} ({100*matched_meteo/n_courses:.1f}%)")
    del meteo, meteo_idx

    # ─── 4. Merge reunions_enrichies ────────────────────────────────────
    print("\n[4/7] Merging reunions_enrichies (detailed course info)...")
    reunions = load_json("output/39_reunions_enrichies/reunions_enrichies.json")

    # Reunions use date-based UIDs like "2013-02-19_R1_C1" while courses use
    # hash UIDs. Join on date + hippodrome_normalise + numero_course instead.
    reunions_by_composite = {}
    for r in reunions:
        date = r.get("date_reunion_iso", "")
        hippo = (r.get("hippodrome") or "").lower().strip()
        num = r.get("numero_course")
        if date and hippo and num is not None:
            key = f"{date}|{hippo}|{num}"
            reunions_by_composite[key] = r
    print(f"  -> {len(reunions_by_composite):,} indexed by date|hippodrome|num_course")

    reunions_skip = {"course_uid", "date_reunion_iso", "discipline",
                     "duree_course_ms", "hippodrome", "numero_course",
                     "numero_reunion", "ordre_arrivee", "paris_types",
                     "reunion_uid", "course_trackee"}
    reunions_fields = set()
    for r in reunions[:200]:
        reunions_fields.update(r.keys())
    reunions_new = reunions_fields - reunions_skip
    print(f"  -> {len(reunions_new)} new fields to merge")

    matched_reunions = 0
    for c in courses:
        date = c.get("date_reunion_iso", "")
        hippo = (c.get("hippodrome_normalise") or "").lower().strip()
        num = c.get("numero_course")
        if date and hippo and num is not None:
            key = f"{date}|{hippo}|{num}"
            if key in reunions_by_composite:
                r = reunions_by_composite[key]
                for k in reunions_new:
                    if k in r:
                        c[k] = r[k]
                matched_reunions += 1
    print(f"  -> Matched: {matched_reunions:,} / {n_courses:,} ({100*matched_reunions/n_courses:.1f}%)")
    del reunions, reunions_by_composite

    # ─── 5. Merge hippodromes_db ────────────────────────────────────────
    print("\n[5/7] Merging hippodromes_db (hippodrome reference data)...")
    sys.path.insert(0, BASE)
    from hippodromes_db import HIPPODROMES_DB
    print(f"  -> {len(HIPPODROMES_DB):,} hippodromes in DB")

    hippo_fields = set()
    for v in list(HIPPODROMES_DB.values())[:50]:
        hippo_fields.update(v.keys())
    print(f"  -> Fields: {sorted(hippo_fields)}")

    matched_hippo = 0
    for c in courses:
        h = c.get("hippodrome_normalise", "")
        if h and h in HIPPODROMES_DB:
            info = HIPPODROMES_DB[h]
            for k, v in info.items():
                c["hippo_" + k] = v
            matched_hippo += 1
    print(f"  -> Matched: {matched_hippo:,} / {n_courses:,} ({100*matched_hippo/n_courses:.1f}%)")

    # ─── 6. Merge field_strength (aggregate per course) ─────────────────
    print("\n[6/7] Merging field_strength (aggregated per course via partants)...")

    # Build partant_uid -> course_uid mapping from parquet (fast)
    import pyarrow.parquet as pq
    print("  Loading partant->course mapping from parquet...", end=" ", flush=True)
    t0 = time.time()
    table = pq.read_table(
        os.path.join(BASE, "output/02_liste_courses_raw_pmu/partants_normalises.parquet"),
        columns=["partant_uid", "course_uid"]
    )
    partant_to_course = {}
    p_uids = table.column("partant_uid").to_pylist()
    c_uids = table.column("course_uid").to_pylist()
    for pu, cu in zip(p_uids, c_uids):
        if pu and cu:
            partant_to_course[pu] = cu
    del table, p_uids, c_uids
    print(f"{len(partant_to_course):,} mappings in {time.time()-t0:.1f}s")

    # Load field strength data
    fs_data = load_json("output/field_strength/field_strength.json")

    # Course-level fields (same for all partants in a course)
    course_level_fs_fields = [
        "rating_moyen", "gains_moyen", "handicap_moyen",
        "rating_std", "gains_std", "rating_range",
        "hhi_marche", "proba_top1", "proba_top3_sum",
        "nb_competitifs", "ratio_competitifs",
        "ecart_favori_2eme", "ecart_1er_dernier",
        "is_open_race", "experience_moyenne",
        "nb_inedits", "pct_inedits", "nb_partants"
    ]

    # Group field_strength by course_uid (take first partant per course)
    fs_by_course = {}
    for rec in fs_data:
        pu = rec.get("partant_uid")
        cu = partant_to_course.get(pu)
        if cu and cu not in fs_by_course:
            fs_by_course[cu] = {k: rec.get(k) for k in course_level_fs_fields if rec.get(k) is not None}
    print(f"  -> {len(fs_by_course):,} courses with field strength data")

    matched_fs = 0
    for c in courses:
        uid = c.get("course_uid")
        if uid and uid in fs_by_course:
            fs = fs_by_course[uid]
            for k, v in fs.items():
                c["fs_" + k] = v
            matched_fs += 1
    print(f"  -> Matched: {matched_fs:,} / {n_courses:,} ({100*matched_fs/n_courses:.1f}%)")
    del fs_data, fs_by_course, partant_to_course

    # ─── 7. Merge citations_enjeux + combinaisons_marche ────────────────
    print("\n[7/7] Merging citations_enjeux & combinaisons_marche...")

    # Citations: aggregate per course_uid
    citations = load_json("output/27_citations_enjeux/citations_enjeux.json")
    citations_by_course = defaultdict(list)
    for rec in citations:
        cu = rec.get("course_uid")
        if cu:
            citations_by_course[cu].append(rec)
    del citations

    # Aggregate: count paris types, sum positions
    citations_agg = {}
    for cu, recs in citations_by_course.items():
        total_positions = 0
        total_complements = 0
        nb_paris_types = len(recs)
        nb_indisponible = 0
        for r in recs:
            pos = r.get("num_positions_consolidees") or r.get("num_positions") or 0
            comp = r.get("num_complements") or 0
            total_positions += pos if isinstance(pos, (int, float)) else 0
            total_complements += comp if isinstance(comp, (int, float)) else 0
            if r.get("indisponible"):
                nb_indisponible += 1
        citations_agg[cu] = {
            "cite_nb_paris_types": nb_paris_types,
            "cite_total_positions": total_positions,
            "cite_total_complements": total_complements,
            "cite_nb_indisponible": nb_indisponible,
        }
    del citations_by_course
    print(f"  -> Citations aggregated: {len(citations_agg):,} courses")

    # Combinaisons: aggregate per course_uid (tolerant load - large file may be truncated)
    combis = load_json_tolerant("output/28_combinaisons_marche/combinaisons_marche.json")
    combis_by_course = defaultdict(list)
    for rec in combis:
        cu = rec.get("course_uid")
        if cu:
            combis_by_course[cu].append(rec)
    del combis

    combis_agg = {}
    for cu, recs in combis_by_course.items():
        nb_combinaisons = len(recs)
        total_enjeu = 0
        nb_paris_types = len(set(r.get("type_pari", "") for r in recs))
        for r in recs:
            e = r.get("enjeu_combinaison")
            if e and isinstance(e, (int, float)):
                total_enjeu += e
        combis_agg[cu] = {
            "combi_nb_combinaisons": nb_combinaisons,
            "combi_total_enjeu": round(total_enjeu, 2),
            "combi_nb_paris_types": nb_paris_types,
        }
    del combis_by_course
    print(f"  -> Combinaisons aggregated: {len(combis_agg):,} courses")

    matched_cite = 0
    matched_combi = 0
    for c in courses:
        uid = c.get("course_uid")
        if uid:
            if uid in citations_agg:
                c.update(citations_agg[uid])
                matched_cite += 1
            if uid in combis_agg:
                c.update(combis_agg[uid])
                matched_combi += 1
    print(f"  -> Citations matched: {matched_cite:,} / {n_courses:,} ({100*matched_cite/n_courses:.1f}%)")
    print(f"  -> Combinaisons matched: {matched_combi:,} / {n_courses:,} ({100*matched_combi/n_courses:.1f}%)")
    del citations_agg, combis_agg

    # ─── Final stats & save ─────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("FINAL STATISTICS")
    print("=" * 70)

    fields_after = set()
    for c in courses[:1000]:
        fields_after.update(c.keys())

    new_fields = sorted(fields_after - fields_before)
    print(f"\nFields BEFORE merge: {len(fields_before)}")
    print(f"Fields AFTER merge:  {len(fields_after)}")
    print(f"New fields added:    {len(new_fields)}")
    print(f"\nNew fields list:")
    for f in new_fields:
        print(f"  + {f}")

    print(f"\nCoverage summary:")
    print(f"  Rapports:       {matched_rapports:>7,} / {n_courses:,} ({100*matched_rapports/n_courses:.1f}%)")
    print(f"  Meteo:          {matched_meteo:>7,} / {n_courses:,} ({100*matched_meteo/n_courses:.1f}%)")
    print(f"  Reunions:       {matched_reunions:>7,} / {n_courses:,} ({100*matched_reunions/n_courses:.1f}%)")
    print(f"  Hippodromes:    {matched_hippo:>7,} / {n_courses:,} ({100*matched_hippo/n_courses:.1f}%)")
    print(f"  Field strength: {matched_fs:>7,} / {n_courses:,} ({100*matched_fs/n_courses:.1f}%)")
    print(f"  Citations:      {matched_cite:>7,} / {n_courses:,} ({100*matched_cite/n_courses:.1f}%)")
    print(f"  Combinaisons:   {matched_combi:>7,} / {n_courses:,} ({100*matched_combi/n_courses:.1f}%)")

    # Save
    out_path = os.path.join(BASE, "output/02_liste_courses/courses_enrichies.json")
    print(f"\nSaving to {out_path} ...", end=" ", flush=True)
    t0 = time.time()
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(courses, f, ensure_ascii=False)
    dt = time.time() - t0
    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f"{size_mb:.0f} MB in {dt:.1f}s")

    total_time = time.time() - t_start
    print(f"\nTotal time: {total_time:.0f}s ({total_time/60:.1f} min)")
    print("Done!")


if __name__ == "__main__":
    main()
