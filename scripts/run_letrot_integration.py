#!/usr/bin/env python3
"""
run_letrot_integration.py - B7: Integrer donnees LeTrot (trot)
===============================================================
Parse 83_letrot/letrot_data.jsonl (1.4M lignes, ~1.35M partants trot)
et cree des features trot supplementaires.

Matching: nom_cheval (normalise) + date + hippodrome
Impact: FAIBLE pour galop, ELEVE pour trot (la plupart de nos partants sont galop)

Features creees:
- letrot_x__temps_sec : temps en secondes
- letrot_x__reduction_km : reduction au km (ex: 1'15"0 -> 75.0)
- letrot_x__rapport_prob : rapport probable
- letrot_x__rang : classement final
- letrot_x__has_letrot_data : 1 si donnee LeTrot disponible

Max RAM: ~3 Go (index 1.35M partants en memoire)

Usage:
    python scripts/run_letrot_integration.py
    python scripts/run_letrot_integration.py --dry-run  # juste compter les matches
"""

import sys
import time
import json
import re
import argparse
from pathlib import Path
from collections import defaultdict

import pyarrow.parquet as pq

LETROT = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/83_letrot/letrot_data.jsonl")
PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/letrot_x")


def normalize_name(name: str) -> str:
    """Normalize horse name for matching."""
    if not name:
        return ""
    # Remove digits at end (series numbers like "4833")
    name = re.sub(r'\d+$', '', str(name))
    # Uppercase, remove accents roughly, strip
    name = name.upper().strip()
    # Remove common prefixes/suffixes
    for prefix in ["MISS ", "MISTER ", "SIR "]:
        if name.startswith(prefix):
            break  # keep these, they're part of the name
    return name


def parse_temps(temps_str: str) -> float | None:
    """Parse '3\\'35\"5' into seconds (215.5)."""
    if not temps_str:
        return None
    try:
        # Format: M'SS"D or M'SS"DD
        m = re.match(r"(\d+)'(\d+)\"(\d+)", str(temps_str))
        if m:
            minutes = int(m.group(1))
            seconds = int(m.group(2))
            tenths = int(m.group(3))
            return minutes * 60 + seconds + tenths / 10.0
        return None
    except (ValueError, TypeError):
        return None


def parse_reduction_km(red_str: str) -> float | None:
    """Parse '1\\'15\"0' into seconds (75.0)."""
    return parse_temps(red_str)


def parse_rapport_prob(rap_str: str) -> float | None:
    """Parse rapport probable."""
    if not rap_str:
        return None
    try:
        return float(str(rap_str).replace(',', '.'))
    except (ValueError, TypeError):
        return None


def parse_rang(rang_str: str) -> int | None:
    """Parse ranking."""
    if not rang_str:
        return None
    try:
        return int(re.sub(r'[^\d]', '', str(rang_str)))
    except (ValueError, TypeError):
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    start = time.time()
    print("=" * 70)
    print("  B7: INTEGRATION DONNEES LETROT (TROT)")
    print("=" * 70)

    if not LETROT.exists():
        print(f"  ERREUR: {LETROT} non trouve")
        sys.exit(1)

    # Phase 1: Build LeTrot index
    # Key: (date, nom_cheval_normalized) -> features
    print("\nPhase 1: Parsing LeTrot data...")
    letrot_index = {}  # (date, nom_norm) -> {features}
    n_parsed = 0
    n_errors = 0

    with open(LETROT, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_errors += 1
                continue

            if rec.get("type") != "partant":
                continue

            n_parsed += 1
            date = rec.get("date", "")[:10]
            # Extract horse name from chevalcrack_series_au_partant
            raw_name = rec.get("chevalcrack_series_au_partant", "")
            nom = normalize_name(raw_name)

            if not date or not nom:
                continue

            key = (date, nom)
            letrot_index[key] = {
                "temps_sec": parse_temps(rec.get("temps")),
                "reduction_km": parse_reduction_km(rec.get("red.<br>km", rec.get("red.km", ""))),
                "rapport_prob": parse_rapport_prob(rec.get("rap._prob.", rec.get("rap_prob", ""))),
                "rang": parse_rang(rec.get("rang")),
            }

            if n_parsed % 500000 == 0:
                print(f"  ... {n_parsed:,} partants parses, {len(letrot_index):,} dans l'index")

    print(f"  {n_parsed:,} partants LeTrot parses ({n_errors} erreurs)")
    print(f"  {len(letrot_index):,} entrees dans l'index (date, nom)")

    # Phase 2: Match with partants_master
    print("\nPhase 2: Matching avec partants_master...")
    pf = pq.ParquetFile(str(PARTANTS))
    n_rg = pf.metadata.num_row_groups

    needed = ["partant_uid", "date_reunion_iso", "nom_cheval"]
    schema_names = set(pf.schema_arrow.names)
    needed = [c for c in needed if c in schema_names]

    total = 0
    matched = 0
    records = []

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=needed)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            uid = r.get("partant_uid")
            total += 1

            date = str(r.get("date_reunion_iso", ""))[:10]
            nom = normalize_name(str(r.get("nom_cheval", "")))

            key = (date, nom)
            lt = letrot_index.get(key)

            feat = {"partant_uid": uid}
            if lt:
                matched += 1
                feat["letrot_x__temps_sec"] = lt["temps_sec"]
                feat["letrot_x__reduction_km"] = lt["reduction_km"]
                feat["letrot_x__rapport_prob"] = lt["rapport_prob"]
                feat["letrot_x__rang"] = float(lt["rang"]) if lt["rang"] else None
                feat["letrot_x__has_letrot_data"] = 1.0
            else:
                feat["letrot_x__temps_sec"] = None
                feat["letrot_x__reduction_km"] = None
                feat["letrot_x__rapport_prob"] = None
                feat["letrot_x__rang"] = None
                feat["letrot_x__has_letrot_data"] = 0.0

            records.append(feat)

        del df

        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            if not args.dry_run and records:
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                out_file = OUTPUT_DIR / "letrot_x_features.jsonl"
                mode = "a" if rg_idx >= 5 else "w"
                with open(out_file, mode, encoding="utf-8", newline="\n") as f:
                    for rec in records:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                records.clear()
            else:
                records.clear()
            print(f"  RG {rg_idx+1}/{n_rg} | {total:,} rows | matched={matched:,} ({matched*100/max(total,1):.1f}%) | {time.time()-start:.0f}s")

    elapsed = time.time() - start
    print(f"\n{'='*70}")
    print(f"  TERMINE en {elapsed:.0f}s")
    print(f"  {total:,} partants | {matched:,} matched ({matched*100/max(total,1):.1f}%)")
    if not args.dry_run:
        out_file = OUTPUT_DIR / "letrot_x_features.jsonl"
        if out_file.exists():
            print(f"  Output: {out_file} ({out_file.stat().st_size/1024/1024:.0f} Mo)")
    else:
        print(f"  (dry-run, pas d'ecriture)")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
