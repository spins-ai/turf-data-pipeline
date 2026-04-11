#!/usr/bin/env python3
"""
run_reunions_enrichies_builder.py - Integrer donnees enrichies des courses
==========================================================================
Source: 39_reunions_enrichies/courses_enrichies_complete.jsonl (128K courses)

Features creees (par partant, via course_uid):
- renr_x__montant_prix         : dotation principale (euros)
- renr_x__montant_total_offert : dotation totale (euros)
- renr_x__nb_incidents         : nombre d'incidents dans la course
- renr_x__has_incidents        : 1 si incidents, 0 sinon
- renr_x__nb_types_paris       : nombre de types de paris disponibles
- renr_x__replay_disponible    : 1 si replay dispo, 0 sinon
- renr_x__meteo_pmu_temp       : temperature PMU (source directe)
- renr_x__meteo_pmu_vent       : force du vent PMU
- renr_x__duree_course_ms      : duree de la course en ms (si dispo)
"""

from __future__ import annotations

import gc
import json
import sys
import time
from pathlib import Path

import duckdb

COURSES_ENRICHIES = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/39_reunions_enrichies/courses_enrichies_complete.jsonl")
PARTANTS_MASTER = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/renr_x")


def main():
    start = time.time()
    print("=" * 70, flush=True)
    print("  BUILDER: REUNIONS ENRICHIES (incidents, paris, dotations)", flush=True)
    print("=" * 70, flush=True)

    # Phase 1: Index courses enrichies
    print("\nPhase 1: Parser courses_enrichies_complete.jsonl...", flush=True)
    course_features = {}

    with open(COURSES_ENRICHIES, "r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            uid = rec.get("course_uid", "")
            if not uid:
                continue

            feats = {}

            # Dotation
            mp = rec.get("montant_prix")
            if mp is not None and mp > 0:
                feats["renr_x__montant_prix"] = float(mp)
            mto = rec.get("montant_total_offert")
            if mto is not None and mto > 0:
                feats["renr_x__montant_total_offert"] = float(mto)

            # Incidents
            nb_inc = rec.get("nb_incidents", 0)
            feats["renr_x__nb_incidents"] = float(nb_inc) if nb_inc else 0.0
            feats["renr_x__has_incidents"] = 1.0 if nb_inc and nb_inc > 0 else 0.0

            # Paris
            nb_paris = rec.get("nb_types_paris")
            if nb_paris is not None:
                feats["renr_x__nb_types_paris"] = float(nb_paris)

            # Replay
            replay = rec.get("replay_disponible")
            if replay is not None:
                feats["renr_x__replay_disponible"] = 1.0 if replay else 0.0

            # Meteo PMU directe
            temp = rec.get("meteo_temperature")
            if temp is not None:
                feats["renr_x__meteo_pmu_temp"] = float(temp)
            vent = rec.get("meteo_force_vent")
            if vent is not None:
                feats["renr_x__meteo_pmu_vent"] = float(vent)

            # Duree course
            duree = rec.get("duree_course_ms")
            if duree is not None and duree > 0:
                feats["renr_x__duree_course_ms"] = float(duree)

            if feats:
                course_features[uid] = feats

            if (i + 1) % 50000 == 0:
                print(f"  {i+1:,} lignes, {len(course_features):,} courses indexees", flush=True)

    print(f"  {len(course_features):,} courses avec features", flush=True)

    # Phase 2: Map to partants via DuckDB using (date, reunion, course) key
    print("\nPhase 2: Assignation aux partants...", flush=True)
    con = duckdb.connect()
    partants = con.execute(f"""
        SELECT partant_uid,
               CAST(date_reunion_iso AS VARCHAR) || '_R' || CAST(numero_reunion AS VARCHAR) || 'C' || CAST(numero_course AS VARCHAR) as match_key
        FROM read_parquet('{PARTANTS_MASTER}')
    """).fetchall()
    con.close()
    print(f"  {len(partants):,} partants lus", flush=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "renr_x_features.jsonl"
    matched = 0

    with open(out_path, "w", encoding="utf-8", newline="\n") as fout:
        for j, (puid, match_key) in enumerate(partants):
            rec = {"partant_uid": puid}
            feats = course_features.get(match_key, {})
            if feats:
                matched += 1
                rec.update(feats)
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if (j + 1) % 500000 == 0:
                print(f"  {j+1:,} partants, {matched:,} matched", flush=True)

    elapsed = time.time() - start
    print(f"\n{'='*70}", flush=True)
    print(f"  TERMINE en {elapsed:.0f}s", flush=True)
    print(f"  Partants matched: {matched:,} / {len(partants):,} ({matched*100/max(len(partants),1):.1f}%)", flush=True)
    if out_path.exists():
        print(f"  Output: {out_path} ({out_path.stat().st_size/1024/1024:.0f} Mo)", flush=True)
    print(f"{'='*70}", flush=True)


if __name__ == "__main__":
    main()
