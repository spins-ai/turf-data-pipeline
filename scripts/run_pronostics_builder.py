#!/usr/bin/env python3
"""
run_pronostics_builder.py - B8: Features pronostics experts
============================================================
Parse 23_pronostics/pronostics.jsonl (204K records) et crée des features
par partant basées sur le rang donné par les pronostiqueurs.

Features créées:
- prono_x__rang : rang du cheval dans les pronostics (1-8, null si absent)
- prono_x__is_top3 : 1 si dans top 3 des pronostics
- prono_x__is_favori : 1 si rang 1
- prono_x__cote_prono : cote donnée par le pronostiqueur
- prono_x__consensus_score : score inversé du rang (8/rang)

Max RAM: ~2 Go
"""

import sys
import time
import json
import math
from pathlib import Path
from collections import defaultdict

import pyarrow.parquet as pq

PRONOSTICS = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/23_pronostics/pronostics.jsonl")
PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")


def parse_cote(cote_str):
    """Parse '4/1' or '5.5' into float."""
    if not cote_str:
        return None
    try:
        if '/' in str(cote_str):
            parts = str(cote_str).split('/')
            return float(parts[0]) / float(parts[1])
        return float(cote_str)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def main():
    start = time.time()
    print("=" * 70)
    print("  B8: PRONOSTICS EXPERTS BUILDER")
    print("=" * 70)

    # Phase 1: Parse pronostics into index
    # Key: (date, reunion, course, num_cheval) -> {rang, cote}
    print("\nPhase 1: Parsing pronostics...")
    prono_index = {}  # (date, reunion, course, num) -> {rang, cote}
    n_records = 0

    with open(PRONOSTICS, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            n_records += 1

            date = rec.get("date_reunion_iso", "")
            reunion = rec.get("numero_reunion")
            course = rec.get("num_course")

            if not date or reunion is None or course is None:
                continue

            # Parse each prono rank (1-8)
            for rang in range(1, 9):
                num_key = f"prono_rang_{rang}_num"
                cote_key = f"prono_rang_{rang}_cote"
                num = rec.get(num_key)
                cote = rec.get(cote_key)

                if num is not None:
                    key = (date, int(reunion), int(course), int(num))
                    prono_index[key] = {
                        "rang": rang,
                        "cote": parse_cote(cote),
                    }

    print(f"  {n_records:,} records parsés, {len(prono_index):,} entrées dans l'index")

    # Phase 2: Match with partants
    print("\nPhase 2: Matching avec partants_master...")
    pf = pq.ParquetFile(str(PARTANTS))
    n_rg = pf.metadata.num_row_groups

    needed = ["partant_uid", "date_reunion_iso", "numero_reunion", "numero_course", "num_pmu"]
    schema_names = set(pf.schema_arrow.names)
    needed = [c for c in needed if c in schema_names]

    records = []
    total = 0
    matched = 0

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=needed)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            uid = r.get("partant_uid")
            total += 1

            feat = {"partant_uid": uid}

            date = str(r.get("date_reunion_iso", ""))[:10]
            reunion = r.get("numero_reunion")
            course = r.get("numero_course")
            num = r.get("num_pmu")

            prono = None
            if date and reunion is not None and course is not None and num is not None:
                try:
                    key = (date, int(reunion), int(course), int(num))
                    prono = prono_index.get(key)
                except (ValueError, TypeError):
                    pass

            if prono:
                matched += 1
                rang = prono["rang"]
                feat["prono_x__rang"] = float(rang)
                feat["prono_x__is_top3"] = 1.0 if rang <= 3 else 0.0
                feat["prono_x__is_favori"] = 1.0 if rang == 1 else 0.0
                feat["prono_x__cote_prono"] = prono["cote"]
                feat["prono_x__consensus_score"] = 8.0 / rang
            else:
                feat["prono_x__rang"] = None
                feat["prono_x__is_top3"] = None
                feat["prono_x__is_favori"] = None
                feat["prono_x__cote_prono"] = None
                feat["prono_x__consensus_score"] = None

            records.append(feat)

        del df

        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            if records:
                out_dir = OUTPUT_DIR / "prono_x"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / "prono_x_features.jsonl"
                mode = "a" if rg_idx >= 5 else "w"
                with open(out_file, mode, encoding="utf-8", newline="\n") as f:
                    for rec in records:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                records.clear()
            print(f"  RG {rg_idx+1}/{n_rg} | {total:,} rows | matched={matched:,} | {time.time()-start:.0f}s")

    elapsed = time.time() - start
    out_file = OUTPUT_DIR / "prono_x" / "prono_x_features.jsonl"
    print(f"\n{'='*70}")
    print(f"  TERMINE en {elapsed:.0f}s | {total:,} lignes | {matched:,} matched ({matched*100/max(total,1):.1f}%)")
    if out_file.exists():
        size_mb = out_file.stat().st_size / 1024 / 1024
        with open(out_file, "r") as f:
            n = sum(1 for _ in f)
        print(f"  prono_x: {n:,} records, {size_mb:.0f} Mo")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
