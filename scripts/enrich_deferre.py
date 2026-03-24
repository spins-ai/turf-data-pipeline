#!/usr/bin/env python3
"""
enrich_deferre.py
=================
Cross-reference equipements_master (09) and PMU API participants (101) to
fill the 'deferre' field in partants_master_enrichi.jsonl.

Sources:
  1. data_master/equipements_master.json  -> join on partant_uid
  2. output/101_pmu_api/pmu_participants.jsonl -> join on (date, reunion, course, numPmu)

Streaming JSONL -> JSONL to keep RAM under 2 GB.

Usage:
    python scripts/enrich_deferre.py
    python scripts/enrich_deferre.py --dry-run
"""

import argparse
import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_MASTER = ROOT / "data_master"
EQUIP_PATH = DATA_MASTER / "equipements_master.json"
PMU_PATH = ROOT / "output" / "101_pmu_api" / "pmu_participants.jsonl"
PARTANTS_IN = DATA_MASTER / "partants_master_enrichi.jsonl"
PARTANTS_OUT = DATA_MASTER / "partants_master_enrichi.jsonl"
PARTANTS_TMP = DATA_MASTER / "partants_master_enrichi_deferre.jsonl.tmp"

# Normalize PMU API deferre values to simpler French equivalents
DEFERRE_MAP = {
    "DEFERRE_ANTERIEURS": "anterieurs",
    "DEFERRE_POSTERIEURS": "posterieurs",
    "DEFERRE_ANTERIEURS_POSTERIEURS": "4_pieds",
    "PROTEGE_ANTERIEURS": "protege_anterieurs",
    "PROTEGE_POSTERIEURS": "protege_posterieurs",
    "PROTEGE_ANTERIEURS_POSTERIEURS": "protege_4_pieds",
    "PROTEGE_ANTERIEURS_DEFERRRE_POSTERIEURS": "protege_ant_deferre_post",
    "DEFERRE_ANTERIEURS_PROTEGE_POSTERIEURS": "deferre_ant_protege_post",
    "REFERRE_ANTERIEURS_POSTERIEURS": "4_pieds",
}


def build_equip_index():
    """Build two indexes from equipements_master:
    - {partant_uid: deferre}
    - {(date, nom_cheval_upper): deferre}  (fallback)
    """
    uid_idx = {}
    name_idx = {}
    if not EQUIP_PATH.exists():
        print(f"  [WARN] {EQUIP_PATH} not found, skipping equipements source")
        return uid_idx, name_idx

    print(f"  Loading equipements from {EQUIP_PATH} ...")
    t0 = time.time()
    with open(EQUIP_PATH, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)

    for rec in data:
        deferre = (rec.get("deferre") or "").strip()
        if not deferre:
            continue
        uid = rec.get("partant_uid", "")
        if uid:
            uid_idx[uid] = deferre
        date = rec.get("date_reunion_iso", "")
        nom = (rec.get("nom_cheval") or "").strip().upper()
        if date and nom:
            name_idx[(date, nom)] = deferre

    print(f"  Loaded {len(uid_idx)} by UID, {len(name_idx)} by (date,nom) "
          f"from equipements ({time.time()-t0:.1f}s)")
    return uid_idx, name_idx


def build_pmu_index():
    """Build {(date, R, C, numPmu): deferre} from PMU API participants."""
    idx = {}
    if not PMU_PATH.exists():
        print(f"  [WARN] {PMU_PATH} not found, skipping PMU API source")
        return idx

    print(f"  Loading PMU API participants from {PMU_PATH} ...")
    t0 = time.time()
    count = 0

    with open(PMU_PATH, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            deferre_raw = (rec.get("deferre") or "").strip()
            if not deferre_raw:
                continue

            date = rec.get("date", "")
            reunion = rec.get("num_reunion")
            course = rec.get("num_course")
            num_pmu = rec.get("numPmu")

            if not all([date, reunion is not None, course is not None, num_pmu is not None]):
                continue

            # Normalize deferre value
            deferre = DEFERRE_MAP.get(deferre_raw, deferre_raw.lower().replace("_", " "))
            key = (date, int(reunion), int(course), int(num_pmu))
            idx[key] = deferre
            count += 1

    print(f"  Loaded {len(idx)} deferre values from PMU API ({time.time()-t0:.1f}s)")
    return idx


def enrich_partants(equip_uid_idx, equip_name_idx, pmu_idx, dry_run=False):
    """Stream partants JSONL, fill deferre field where missing."""
    if not PARTANTS_IN.exists():
        print(f"  [ERROR] {PARTANTS_IN} not found")
        return

    total = 0
    enriched_equip = 0
    enriched_pmu = 0
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

                existing = (rec.get("deferre") or "").strip()
                if existing:
                    already_filled += 1
                    continue

                # Try equipements first (partant_uid join, then date+nom fallback)
                uid = rec.get("partant_uid", "")
                if uid and uid in equip_uid_idx:
                    enriched_equip += 1
                    continue
                date = rec.get("date_reunion_iso", "")
                nom = (rec.get("nom_cheval") or "").strip().upper()
                if date and nom and (date, nom) in equip_name_idx:
                    enriched_equip += 1
                    continue

                # Try PMU API (date+R+C+num join)
                date = rec.get("date_reunion_iso", "")
                reunion = rec.get("numero_reunion")
                course = rec.get("numero_course")
                num_pmu = rec.get("num_pmu")
                if all([date, reunion is not None, course is not None, num_pmu is not None]):
                    key = (date, int(reunion), int(course), int(num_pmu))
                    if key in pmu_idx:
                        enriched_pmu += 1

        print(f"  [DRY-RUN] Would enrich {enriched_equip} from equipements, "
              f"{enriched_pmu} from PMU API, {already_filled} already filled "
              f"(total: {total})")
        return

    # Actual enrichment
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

            existing = (rec.get("deferre") or "").strip()
            if existing:
                already_filled += 1
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue

            filled = False

            # Try equipements first (UID, then date+nom)
            uid = rec.get("partant_uid", "")
            if uid and uid in equip_uid_idx:
                rec["deferre"] = equip_uid_idx[uid]
                enriched_equip += 1
                filled = True
            if not filled:
                date = rec.get("date_reunion_iso", "")
                nom = (rec.get("nom_cheval") or "").strip().upper()
                if date and nom and (date, nom) in equip_name_idx:
                    rec["deferre"] = equip_name_idx[(date, nom)]
                    enriched_equip += 1
                    filled = True

            # Try PMU API as fallback
            if not filled:
                date = rec.get("date_reunion_iso", "")
                reunion = rec.get("numero_reunion")
                course = rec.get("numero_course")
                num_pmu = rec.get("num_pmu")
                if all([date, reunion is not None, course is not None, num_pmu is not None]):
                    key = (date, int(reunion), int(course), int(num_pmu))
                    if key in pmu_idx:
                        rec["deferre"] = pmu_idx[key]
                        enriched_pmu += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Replace original
    os.replace(str(PARTANTS_TMP), str(PARTANTS_IN))

    elapsed = time.time() - t0
    total_enriched = enriched_equip + enriched_pmu
    pct = 100 * total_enriched / total if total > 0 else 0
    print(f"  Done: {total_enriched}/{total} records enriched ({pct:.1f}%)")
    print(f"    From equipements: {enriched_equip}")
    print(f"    From PMU API: {enriched_pmu}")
    print(f"    Already filled: {already_filled}")
    print(f"    Time: {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Enrich deferre field in partants_master")
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying")
    args = parser.parse_args()

    print("=" * 60)
    print("DEFERRE ENRICHMENT — Cross-reference equipements (09) + PMU API (101)")
    print("=" * 60)

    equip_uid_idx, equip_name_idx = build_equip_index()
    pmu_idx = build_pmu_index()

    if not equip_uid_idx and not equip_name_idx and not pmu_idx:
        print("  [WARN] No deferre data found from any source")
        return

    enrich_partants(equip_uid_idx, equip_name_idx, pmu_idx, dry_run=args.dry_run)
    print("  Done!")


if __name__ == "__main__":
    main()
