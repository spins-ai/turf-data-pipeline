#!/usr/bin/env python3
"""
run_perf_builders.py - Builders basés sur performances_master.parquet
=====================================================================
C9.  musique_lag    : positions des 5 dernières courses
C10. speed_form     : meilleur chrono par distance, vitesse relative
C11. sectional_pace : vitesse par segment (sectionals)

Lit performances_master.parquet + sectionals.parquet,
construit un index par cheval, puis streame partants_master.parquet
pour calculer les features en respectant l'ordre temporel (pas de leakage).

Max RAM: ~4 Go
"""

import sys
import time
import json
import math
import numpy as np
from pathlib import Path
from collections import defaultdict
from datetime import datetime

import pyarrow.parquet as pq

PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
PERFS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/performances_master.parquet")
SECTIONALS = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/11_sectionals/sectionals.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")


def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def parse_date(d):
    if d is None:
        return None
    try:
        if isinstance(d, (int, float)):
            # Timestamp in ms
            return datetime.fromtimestamp(d / 1000).strftime("%Y-%m-%d")
        return str(d)[:10]
    except:
        return None


def main():
    start = time.time()
    print("=" * 70)
    print("  BUILDERS PERFORMANCES (C9, C10, C11)")
    print("=" * 70)

    # =====================================================================
    # Phase 1: Build horse performance history from performances_master
    # =====================================================================
    print("\nPhase 1: Indexation des performances par cheval...")

    pf_perf = pq.ParquetFile(str(PERFS))
    print(f"  performances_master: {pf_perf.metadata.num_rows:,} rows")

    # Index: horse_name -> list of {date, position, distance, time, ...}
    horse_perfs = defaultdict(list)

    for rg_idx in range(pf_perf.metadata.num_row_groups):
        table = pf_perf.read_row_group(rg_idx)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            nom = r.get("nomCheval")
            if not nom or not isinstance(nom, str):
                continue
            nom_upper = nom.upper().strip()

            perf_date = parse_date(r.get("perf_date"))
            position = safe_float(r.get("place_position"))
            distance = safe_float(r.get("perf_distance") or r.get("distanceParcourue"))
            temps = safe_float(r.get("perf_tempsDuPremier") or r.get("tempsDuPremier"))
            nb_partants = safe_float(r.get("perf_nbParticipants"))
            reduction_km = safe_float(r.get("reductionKilometrique"))

            if perf_date:
                horse_perfs[nom_upper].append({
                    "date": perf_date,
                    "pos": position,
                    "dist": distance,
                    "temps": temps,
                    "nb": nb_partants,
                    "redkm": reduction_km,
                })
        del df
        if (rg_idx + 1) % 5 == 0:
            print(f"  RG {rg_idx+1}/{pf_perf.metadata.num_row_groups}")

    # Sort each horse's perfs by date
    for nom in horse_perfs:
        horse_perfs[nom].sort(key=lambda x: x["date"])

    print(f"  {len(horse_perfs):,} chevaux indexes")

    # =====================================================================
    # Phase 1b: Sectionals index
    # =====================================================================
    print("\nPhase 1b: Index sectionals...")
    sectional_index = {}  # partant_uid -> {vitesse_kmh, reduction_km_sec, ecart_gagnant}
    if SECTIONALS.exists():
        pf_sect = pq.ParquetFile(str(SECTIONALS))
        table = pf_sect.read()
        df = table.to_pandas()
        del table
        for _, r in df.iterrows():
            uid = r.get("partant_uid")
            if uid:
                sectional_index[uid] = {
                    "vitesse_kmh": safe_float(r.get("vitesse_kmh")),
                    "reduction_km_sec": safe_float(r.get("reduction_km_sec")),
                    "ecart_gagnant": safe_float(r.get("ecart_temps_gagnant")),
                    "temps_ms": safe_float(r.get("temps_ms")),
                }
        del df
        print(f"  {len(sectional_index):,} sectionals indexes")
    else:
        print("  sectionals.parquet non trouve, skip")

    # =====================================================================
    # Phase 2: Compute features for each partant
    # =====================================================================
    print("\nPhase 2: Calcul des features...")
    pf_part = pq.ParquetFile(str(PARTANTS))
    n_rg = pf_part.metadata.num_row_groups

    needed_cols = ["partant_uid", "nom_cheval", "date_reunion_iso", "distance"]
    schema_names = set(pf_part.schema_arrow.names)
    needed_cols = [c for c in needed_cols if c in schema_names]

    builders = {"musique_x": [], "speedform_x": [], "sectional_x": []}
    total = 0

    for rg_idx in range(n_rg):
        table = pf_part.read_row_group(rg_idx, columns=needed_cols)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            uid = r.get("partant_uid")
            nom = r.get("nom_cheval")
            date_course = str(r.get("date_reunion_iso", ""))[:10]
            distance = safe_float(r.get("distance"))
            total += 1

            if not uid or not nom:
                builders["musique_x"].append({"partant_uid": uid})
                builders["speedform_x"].append({"partant_uid": uid})
                builders["sectional_x"].append({"partant_uid": uid})
                continue

            nom_upper = nom.upper().strip() if isinstance(nom, str) else ""
            perfs = horse_perfs.get(nom_upper, [])

            # Filter: only perfs BEFORE this race (temporal integrity)
            past_perfs = [p for p in perfs if p["date"] < date_course] if date_course else perfs

            # ===== C9: MUSIQUE LAG =====
            feat_m = {"partant_uid": uid}
            recent = past_perfs[-5:] if past_perfs else []

            for i in range(5):
                if i < len(recent):
                    p = recent[-(i + 1)]
                    feat_m[f"musique_x__pos_last_{i+1}"] = p["pos"]
                    if p["pos"] and p["nb"] and p["nb"] > 0:
                        feat_m[f"musique_x__pos_norm_last_{i+1}"] = p["pos"] / p["nb"]
                    else:
                        feat_m[f"musique_x__pos_norm_last_{i+1}"] = None
                else:
                    feat_m[f"musique_x__pos_last_{i+1}"] = None
                    feat_m[f"musique_x__pos_norm_last_{i+1}"] = None

            # Mean position last 3/5
            pos_last_3 = [p["pos"] for p in recent[-3:] if p["pos"] is not None]
            pos_last_5 = [p["pos"] for p in recent if p["pos"] is not None]
            feat_m["musique_x__mean_pos_3"] = np.mean(pos_last_3) if pos_last_3 else None
            feat_m["musique_x__mean_pos_5"] = np.mean(pos_last_5) if pos_last_5 else None

            # Trend (regression slope): negative = improving
            if len(pos_last_5) >= 3:
                x = np.arange(len(pos_last_5))
                y = np.array(pos_last_5)
                slope = np.polyfit(x, y, 1)[0]
                feat_m["musique_x__trend_5"] = float(slope)
            else:
                feat_m["musique_x__trend_5"] = None

            # Win count in last 5
            wins_last_5 = sum(1 for p in recent if p["pos"] == 1)
            feat_m["musique_x__wins_last_5"] = float(wins_last_5) if recent else None

            builders["musique_x"].append(feat_m)

            # ===== C10: SPEED FORM =====
            feat_s = {"partant_uid": uid}

            # Best time at this distance
            if distance:
                same_dist = [p for p in past_perfs if p["dist"] and abs(p["dist"] - distance) < 100 and p["temps"]]
                if same_dist:
                    times = [p["temps"] for p in same_dist]
                    feat_s["speedform_x__best_time_dist"] = min(times)
                    feat_s["speedform_x__avg_time_dist"] = np.mean(times)
                    feat_s["speedform_x__nb_runs_dist"] = float(len(same_dist))
                else:
                    feat_s["speedform_x__best_time_dist"] = None
                    feat_s["speedform_x__avg_time_dist"] = None
                    feat_s["speedform_x__nb_runs_dist"] = 0.0
            else:
                feat_s["speedform_x__best_time_dist"] = None
                feat_s["speedform_x__avg_time_dist"] = None
                feat_s["speedform_x__nb_runs_dist"] = None

            # Reduction km (speed metric)
            redkms = [p["redkm"] for p in past_perfs[-5:] if p["redkm"] is not None]
            feat_s["speedform_x__best_redkm"] = min(redkms) if redkms else None
            feat_s["speedform_x__avg_redkm_5"] = np.mean(redkms) if redkms else None

            # Speed vs field: position relative moyenne
            pos_norms = []
            for p in past_perfs[-5:]:
                if p["pos"] and p["nb"] and p["nb"] > 0:
                    pos_norms.append(p["pos"] / p["nb"])
            feat_s["speedform_x__avg_pos_rel"] = np.mean(pos_norms) if pos_norms else None

            builders["speedform_x"].append(feat_s)

            # ===== C11: SECTIONAL =====
            feat_sect = {"partant_uid": uid}
            sect = sectional_index.get(uid, {})
            feat_sect["sectional_x__vitesse_kmh"] = sect.get("vitesse_kmh")
            feat_sect["sectional_x__reduction_km_sec"] = sect.get("reduction_km_sec")
            feat_sect["sectional_x__ecart_gagnant"] = sect.get("ecart_gagnant")
            feat_sect["sectional_x__temps_ms"] = sect.get("temps_ms")

            builders["sectional_x"].append(feat_sect)

        del df

        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            for name, records in builders.items():
                if records:
                    out_dir = OUTPUT_DIR / name
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file = out_dir / f"{name}_features.jsonl"
                    mode = "a" if rg_idx >= 5 else "w"
                    with open(out_file, mode, encoding="utf-8", newline="\n") as f:
                        for rec in records:
                            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    records.clear()
            print(f"  RG {rg_idx+1}/{n_rg} | {total:,} rows | {time.time()-start:.0f}s")

    elapsed = time.time() - start
    print(f"\n{'='*70}")
    print(f"  TERMINE en {elapsed:.0f}s | {total:,} lignes")
    for name in builders:
        out_file = OUTPUT_DIR / name / f"{name}_features.jsonl"
        if out_file.exists():
            size_mb = out_file.stat().st_size / 1024 / 1024
            with open(out_file, "r") as f:
                n = sum(1 for _ in f)
            print(f"  {name}: {n:,} records, {size_mb:.0f} Mo")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
