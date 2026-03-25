#!/usr/bin/env python3
"""
Cross-reference Paris-Turf data with Geny data using externalId.GENY.

Paris-Turf runners contain an 'externalId.GENY' field with format 'raceId-horseId'
which can be used to link to Geny's flat data for enrichment.

The externalId.GENY format is 'raceId-horseId' (e.g. '1648416-1425200').
All 3775 PT runners (100%) have this ID.

Current date coverage:
- Paris-Turf: 2026-03-19 to 2026-03-21 (3 days, recent scrape)
- Geny flat:  2020-01-01 to 2026-03-14 (2265 days, historical)
- NO DATE OVERLAP currently => 0% match on name+date

This script:
1. Builds an externalId.GENY index from Paris-Turf runners
2. Attempts name+date matching with Geny flat data
3. Creates a GENY ID registry for future cross-referencing
4. Produces merged output + match report

When both sources cover the same dates, the match rate should be high.

Output: output/cross_reference/paris_turf_geny_merged.jsonl
        output/cross_reference/geny_id_registry.jsonl
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
PARIS_TURF_RUNNERS = ROOT / "output" / "53_paris_turf" / "paris_turf_runners.jsonl"
GENY_FLAT = ROOT / "output" / "26_geny" / "geny_flat.jsonl"
GENY_RAW = ROOT / "output" / "26_geny" / "geny_data.jsonl"
OUTPUT_DIR = ROOT / "output" / "cross_reference"
OUTPUT_FILE = OUTPUT_DIR / "paris_turf_geny_merged.jsonl"
REPORT_FILE = OUTPUT_DIR / "cross_reference_report.json"


def normalize_name(name):
    """Normalize horse name for fuzzy matching."""
    if not name:
        return ""
    return (
        name.upper()
        .strip()
        .replace("-", " ")
        .replace("'", " ")
        .replace(".", " ")
        .replace("  ", " ")
    )


def load_geny_flat():
    """Load Geny flat data indexed by (date, normalized_name)."""
    index = {}
    count = 0
    if not GENY_FLAT.exists():
        print(f"WARNING: {GENY_FLAT} not found")
        return index
    with open(GENY_FLAT, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                count += 1
                date = rec.get("date", "")
                name = normalize_name(rec.get("nom_cheval", ""))
                if date and name:
                    key = (date, name)
                    index[key] = rec
            except json.JSONDecodeError:
                continue
    print(f"Loaded {count} Geny flat records, indexed {len(index)} by (date, name)")
    return index


def load_geny_raw():
    """Load Geny raw data for additional fields, indexed by date."""
    index = defaultdict(dict)
    count = 0
    if not GENY_RAW.exists():
        print(f"WARNING: {GENY_RAW} not found")
        return index
    with open(GENY_RAW, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                count += 1
                date = rec.get("date", "")
                if date:
                    index[date] = rec
            except json.JSONDecodeError:
                continue
    print(f"Loaded {count} Geny raw records")
    return index


def load_paris_turf_runners():
    """Load Paris-Turf runners."""
    runners = []
    if not PARIS_TURF_RUNNERS.exists():
        print(f"ERROR: {PARIS_TURF_RUNNERS} not found")
        return runners
    with open(PARIS_TURF_RUNNERS, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                runners.append(rec)
            except json.JSONDecodeError:
                continue
    print(f"Loaded {len(runners)} Paris-Turf runners")
    return runners


def merge_records(pt_rec, geny_rec):
    """Merge a Paris-Turf record with a Geny record."""
    merged = dict(pt_rec)

    # Add Geny-specific fields with geny_ prefix to avoid collisions
    geny_fields = {
        "musique": "geny_musique",
        "valeur": "geny_valeur",
        "cote_ref": "geny_cote_ref",
        "derniere_cote": "geny_derniere_cote",
        "poids": "geny_poids",
        "decharge": "geny_decharge",
        "col_11": "geny_col_11",
        "col_12": "geny_col_12",
        "col_13": "geny_col_13",
    }

    for src_key, dst_key in geny_fields.items():
        val = geny_rec.get(src_key)
        if val is not None and val != "" and val != "null":
            merged[dst_key] = val

    merged["_cross_ref_source"] = "paris_turf_x_geny"
    merged["_cross_ref_method"] = "externalId.GENY + name_match"
    merged["_cross_ref_date"] = datetime.now().isoformat()

    return merged


def main():
    print("=" * 60)
    print("Cross-Reference: Paris-Turf <-> Geny")
    print("=" * 60)

    # Load data
    geny_index = load_geny_flat()
    pt_runners = load_paris_turf_runners()

    if not pt_runners:
        print("No Paris-Turf runners to process")
        return

    # Create output dir
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Match statistics
    stats = {
        "total_pt_runners": len(pt_runners),
        "with_geny_external_id": 0,
        "matched_by_name_date": 0,
        "unmatched": 0,
        "geny_flat_records": len(geny_index),
        "unique_geny_ids": set(),
    }

    merged_records = []
    unmatched_records = []

    for pt_rec in pt_runners:
        ext_id = pt_rec.get("externalId", {})
        geny_id = ext_id.get("GENY", "") if isinstance(ext_id, dict) else ""

        if geny_id:
            stats["with_geny_external_id"] += 1
            stats["unique_geny_ids"].add(geny_id)

        # Try matching by (date, name)
        pt_date = pt_rec.get("raceDate", pt_rec.get("date", ""))
        pt_name = normalize_name(pt_rec.get("horseName", ""))

        # Try date in different formats
        matched = False
        for date_key in [pt_date, pt_date[:10] if len(pt_date) >= 10 else pt_date]:
            key = (date_key, pt_name)
            if key in geny_index:
                merged = merge_records(pt_rec, geny_index[key])
                merged["geny_external_id"] = geny_id
                merged_records.append(merged)
                stats["matched_by_name_date"] += 1
                matched = True
                break

        if not matched:
            # Still include PT record with GENY ID for future reference
            pt_rec_copy = dict(pt_rec)
            pt_rec_copy["geny_external_id"] = geny_id
            pt_rec_copy["_cross_ref_status"] = "unmatched"
            unmatched_records.append(pt_rec_copy)
            stats["unmatched"] += 1

    # Write merged output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for rec in merged_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        for rec in unmatched_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Clean up stats for JSON serialization
    stats["unique_geny_ids"] = len(stats["unique_geny_ids"])
    stats["match_rate"] = (
        f"{100 * stats['matched_by_name_date'] / stats['total_pt_runners']:.1f}%"
        if stats["total_pt_runners"] > 0
        else "0%"
    )
    stats["output_file"] = str(OUTPUT_FILE)
    stats["generated_at"] = datetime.now().isoformat()

    # Write GENY ID registry (maps GENY IDs to horse/race info for future use)
    registry_file = OUTPUT_DIR / "geny_id_registry.jsonl"
    geny_registry = {}
    for pt_rec in pt_runners:
        ext_id = pt_rec.get("externalId", {})
        geny_id = ext_id.get("GENY", "") if isinstance(ext_id, dict) else ""
        if geny_id and geny_id not in geny_registry:
            parts = geny_id.split("-")
            geny_registry[geny_id] = {
                "geny_id": geny_id,
                "geny_race_id": parts[0] if len(parts) >= 1 else "",
                "geny_horse_id": parts[1] if len(parts) >= 2 else "",
                "horse_name": pt_rec.get("horseName", ""),
                "race_date": pt_rec.get("raceDate", ""),
                "race_id": pt_rec.get("raceId", ""),
                "race_name": pt_rec.get("raceName", ""),
                "jockey": pt_rec.get("jockeyName", ""),
                "trainer": pt_rec.get("trainerName", ""),
                "source": "paris_turf",
            }
    with open(registry_file, "w", encoding="utf-8") as f:
        for entry in geny_registry.values():
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    print(f"  GENY ID Registry:         {len(geny_registry)} entries -> {registry_file}")
    stats["geny_registry_entries"] = len(geny_registry)

    # Write report
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    print(f"\nResults:")
    print(f"  Total Paris-Turf runners: {stats['total_pt_runners']}")
    print(f"  With externalId.GENY:     {stats['with_geny_external_id']}")
    print(f"  Matched by name+date:     {stats['matched_by_name_date']}")
    print(f"  Unmatched:                {stats['unmatched']}")
    print(f"  Match rate:               {stats['match_rate']}")
    print(f"  Unique GENY IDs:          {stats['unique_geny_ids']}")
    print(f"\nOutput: {OUTPUT_FILE}")
    print(f"Report: {REPORT_FILE}")


if __name__ == "__main__":
    main()
