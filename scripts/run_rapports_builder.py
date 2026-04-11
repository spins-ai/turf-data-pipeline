#!/usr/bin/env python3
"""
run_rapports_builder.py - C7: Rapports historiques par hippodrome
=================================================================
ATTENTION LEAKAGE: les colonnes rap_* contiennent les rapports de la course
ACTUELLE. On ne peut PAS les utiliser pour prédire cette course.

Approche safe: pour chaque course, on calcule les stats HISTORIQUES
(seulement courses PASSÉES) par hippodrome + discipline.

Features créées (toutes basées sur données PASSÉES uniquement):
- rapphist_x__avg_simple_gagnant_hippo : rapport gagnant moyen historique
- rapphist_x__std_simple_gagnant_hippo : volatilité des rapports
- rapphist_x__avg_market_concentration_hippo : concentration marché
- rapphist_x__avg_dividend_hippo : dividende moyen historique
- rapphist_x__upset_rate_hippo : % courses où favori perd (rapport > 1000)
- rapphist_x__avg_simple_gagnant_discipline : rapport moyen par discipline
- rapphist_x__nb_courses_historiques : nb courses passées à cet hippo

Max RAM: ~3 Go
"""

import sys
import time
import json
import math
import numpy as np
from pathlib import Path
from collections import defaultdict

import pyarrow.parquet as pq

PARQUET = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")


def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def main():
    start = time.time()
    print("=" * 70)
    print("  C7: RAPPORTS HISTORIQUES BUILDER")
    print("  (safe: seulement données PASSÉES par hippodrome)")
    print("=" * 70)

    pf = pq.ParquetFile(str(PARQUET))
    n_rg = pf.metadata.num_row_groups
    n_rows = pf.metadata.num_rows
    print(f"  {n_rows:,} rows, {n_rg} row groups")

    # Phase 1: Collect all course-level rapport data, sorted by date
    # We need one entry per course (not per partant)
    print("\nPhase 1: Collecte rapports par course...")
    needed_p1 = ["course_uid", "date_reunion_iso", "rap_rapport_simple_gagnant",
                  "rap_rapport_simple_place_1", "rap_dividend_moyen",
                  "rap_market_concentration", "rap_hippodrome", "rap_discipline"]
    schema_names = set(pf.schema_arrow.names)
    needed_p1 = [c for c in needed_p1 if c in schema_names]

    # course_uid -> {date, hippo, discipline, rapport_gagnant, ...}
    course_data = {}
    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=needed_p1)
        df = table.to_pandas()
        del table
        for _, r in df.iterrows():
            cuid = r.get("course_uid")
            if not cuid or cuid in course_data:
                continue
            hippo_raw = r.get("rap_hippodrome")
            hippo = str(hippo_raw).lower().strip() if hippo_raw and not (isinstance(hippo_raw, float) and math.isnan(hippo_raw)) else ""
            disc_raw = r.get("rap_discipline")
            disc = str(disc_raw).upper().strip() if disc_raw and not (isinstance(disc_raw, float) and math.isnan(disc_raw)) else ""
            course_data[cuid] = {
                "date": str(r.get("date_reunion_iso", ""))[:10],
                "hippo": hippo,
                "disc": disc,
                "gagnant": safe_float(r.get("rap_rapport_simple_gagnant")),
                "place1": safe_float(r.get("rap_rapport_simple_place_1")),
                "dividend": safe_float(r.get("rap_dividend_moyen")),
                "concentration": safe_float(r.get("rap_market_concentration")),
            }
        del df
        if (rg_idx + 1) % 10 == 0:
            print(f"  RG {rg_idx+1}/{n_rg}")

    print(f"  {len(course_data):,} courses uniques collectées")

    # Sort courses by date
    sorted_courses = sorted(course_data.items(), key=lambda x: x[1]["date"])

    # Phase 1b: Build historical cumulative stats per hippo and per discipline
    print("\nPhase 1b: Construction index historique cumulatif...")

    # For each hippodrome: maintain running stats
    # hippo -> {gagnants: [], concentrations: [], dividends: [], nb_courses: int}
    hippo_history = defaultdict(lambda: {"gagnants": [], "concentrations": [], "dividends": []})
    disc_history = defaultdict(lambda: {"gagnants": []})

    # For each course_uid, store the historical snapshot AT THAT DATE
    course_hist_snapshot = {}

    for cuid, data in sorted_courses:
        hippo = data["hippo"]
        disc = data["disc"]

        if hippo:
            h = hippo_history[hippo]
            # Snapshot BEFORE adding this course (= historical data only)
            if h["gagnants"]:
                course_hist_snapshot[cuid] = {
                    "avg_gagnant_hippo": np.mean(h["gagnants"]),
                    "std_gagnant_hippo": np.std(h["gagnants"]) if len(h["gagnants"]) >= 2 else None,
                    "avg_concentration_hippo": np.mean(h["concentrations"]) if h["concentrations"] else None,
                    "avg_dividend_hippo": np.mean(h["dividends"]) if h["dividends"] else None,
                    "upset_rate_hippo": sum(1 for g in h["gagnants"] if g > 1000) / len(h["gagnants"]),
                    "nb_courses_hippo": len(h["gagnants"]),
                }
            else:
                course_hist_snapshot[cuid] = {}

            # Add discipline stats
            if disc and disc_history[disc]["gagnants"]:
                course_hist_snapshot[cuid]["avg_gagnant_disc"] = np.mean(disc_history[disc]["gagnants"])
            else:
                course_hist_snapshot.setdefault(cuid, {})["avg_gagnant_disc"] = None

            # NOW add this course's data to the running history
            if data["gagnant"]:
                h["gagnants"].append(data["gagnant"])
            if data["concentration"]:
                h["concentrations"].append(data["concentration"])
            if data["dividend"]:
                h["dividends"].append(data["dividend"])
            if disc and data["gagnant"]:
                disc_history[disc]["gagnants"].append(data["gagnant"])

            # Keep only last 200 races per hippo to save memory
            for key in ["gagnants", "concentrations", "dividends"]:
                if len(h[key]) > 200:
                    h[key] = h[key][-200:]
        else:
            course_hist_snapshot[cuid] = {}

    print(f"  {len(course_hist_snapshot):,} snapshots historiques")
    print(f"  {len(hippo_history):,} hippodromes, {len(disc_history):,} disciplines")

    # Free memory
    del hippo_history, disc_history, sorted_courses, course_data

    # Phase 2: Assign features to each partant
    print("\nPhase 2: Calcul features par partant...")
    needed_p2 = ["partant_uid", "course_uid"]
    needed_p2 = [c for c in needed_p2 if c in schema_names]

    records = []
    total = 0

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=needed_p2)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            uid = r.get("partant_uid")
            cuid = r.get("course_uid")
            total += 1

            feat = {"partant_uid": uid}
            hist = course_hist_snapshot.get(cuid, {})

            feat["rapphist_x__avg_simple_gagnant_hippo"] = hist.get("avg_gagnant_hippo")
            feat["rapphist_x__std_simple_gagnant_hippo"] = hist.get("std_gagnant_hippo")
            feat["rapphist_x__avg_market_concentration_hippo"] = hist.get("avg_concentration_hippo")
            feat["rapphist_x__avg_dividend_hippo"] = hist.get("avg_dividend_hippo")
            feat["rapphist_x__upset_rate_hippo"] = hist.get("upset_rate_hippo")
            feat["rapphist_x__nb_courses_historiques"] = float(hist.get("nb_courses_hippo")) if hist.get("nb_courses_hippo") else None
            feat["rapphist_x__avg_simple_gagnant_discipline"] = hist.get("avg_gagnant_disc")

            records.append(feat)

        del df

        # Flush every 5 row groups
        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            if records:
                out_dir = OUTPUT_DIR / "rapphist_x"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / "rapphist_x_features.jsonl"
                mode = "a" if rg_idx >= 5 else "w"
                with open(out_file, mode, encoding="utf-8", newline="\n") as f:
                    for rec in records:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                records.clear()
            print(f"  RG {rg_idx+1}/{n_rg} | {total:,} rows | {time.time()-start:.0f}s")

    elapsed = time.time() - start
    out_file = OUTPUT_DIR / "rapphist_x" / "rapphist_x_features.jsonl"
    print(f"\n{'='*70}")
    print(f"  TERMINE en {elapsed:.0f}s | {total:,} lignes")
    if out_file.exists():
        size_mb = out_file.stat().st_size / 1024 / 1024
        with open(out_file, "r") as f:
            n = sum(1 for _ in f)
        print(f"  rapphist_x: {n:,} records, {size_mb:.0f} Mo")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
