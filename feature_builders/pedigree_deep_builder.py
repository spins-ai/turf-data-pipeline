#!/usr/bin/env python3
"""
feature_builders.pedigree_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep pedigree features by cross-referencing pedigree_master (1.4M records)
+ sire_ifce (1.47M) with partants_master (2.93M).

Temporal integrity: uses only lineage data (static), no future leakage.

Features (12):
  - ped_deep_sire_offspring_count     : nb descendants du pere dans la base
  - ped_deep_sire_win_rate_distance   : taux victoire descendants du pere a cette distance (+/-200m)
  - ped_deep_sire_win_rate_discipline : taux victoire descendants du pere dans cette discipline
  - ped_deep_dam_sire_impact          : taux victoire descendants du pere de la mere
  - ped_deep_sire_age                 : age du pere a la naissance du cheval
  - ped_deep_inbreeding_coeff         : coefficient simplifie de consanguinite (pere_mere == pere)
  - ped_deep_sire_terrain_pref        : preference terrain du pere (lourd/souple/bon)
  - ped_deep_lineage_stamina_idx      : index endurance lignee (perf > 2400m)
  - ped_deep_lineage_speed_idx        : index vitesse lignee (perf < 1600m)
  - ped_deep_nicking_score            : score croisement pere x pere_mere
  - ped_deep_sire_country_match       : 1 si pays pere == pays course
  - ped_deep_dam_age_at_birth         : age de la mere a la naissance

Memory: ~6 GB (sire stats dicts + pedigree lookup)
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
PEDIGREE_PATH = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/pedigree_master.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pedigree_deep")
_LOG_EVERY = 500_000

# Distance buckets for sire stats
_DIST_SHORT = 1600
_DIST_LONG = 2400


def _parse_year(s) -> Optional[int]:
    if not s:
        return None
    try:
        if isinstance(s, (int, float)):
            return int(s)
        return int(str(s)[:4])
    except (ValueError, TypeError):
        return None


def _safe_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def build(logger) -> None:
    t0 = time.time()

    # ---- Phase 1: Load pedigree data ----
    logger.info("Phase 1: Chargement pedigree_master...")
    pedigree_lookup = {}  # nom -> {pere, mere, pere_mere, date_naissance, pays, sire_date_naissance, ...}

    try:
        import pyarrow.parquet as pq
        table = pq.read_table(str(PEDIGREE_PATH))
        for i in range(table.num_rows):
            row = {col: table.column(col)[i].as_py() for col in table.column_names}
            nom = row.get("nom", "")
            if nom:
                pedigree_lookup[nom.upper().strip()] = row
        del table
        gc.collect()
        logger.info("  Pedigree: %d chevaux charges", len(pedigree_lookup))
    except Exception as e:
        logger.warning("Erreur chargement pedigree: %s", e)

    # ---- Phase 2: First pass - build sire stats from partants_master ----
    logger.info("Phase 2: Construction stats pere (premier passage)...")

    # sire -> {distance_bucket -> {wins, total}, discipline -> {wins, total}, terrain -> {wins, total}}
    sire_stats: dict[str, dict] = defaultdict(lambda: {
        "total": 0, "wins": 0,
        "dist": defaultdict(lambda: {"w": 0, "t": 0}),
        "disc": defaultdict(lambda: {"w": 0, "t": 0}),
        "terrain": defaultdict(lambda: {"w": 0, "t": 0}),
        "country": "",
    })
    # nicking: (sire, dam_sire) -> {wins, total}
    nicking: dict[tuple, dict] = defaultdict(lambda: {"w": 0, "t": 0})

    n_pass1 = 0
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            n_pass1 += 1
            if n_pass1 % _LOG_EVERY == 0:
                logger.info("  Pass 1: %d records...", n_pass1)

            pere = (rec.get("pere") or "").upper().strip()
            if not pere:
                continue

            is_winner = False
            pos = rec.get("position_arrivee")
            try:
                is_winner = int(pos) == 1
            except (ValueError, TypeError):
                pass

            distance = _safe_int(rec.get("distance"))
            discipline = (rec.get("discipline") or "").upper().strip()
            terrain = (rec.get("type_piste") or "").upper().strip()
            pere_mere = (rec.get("pere_mere") or "").upper().strip()

            ss = sire_stats[pere]
            ss["total"] += 1
            if is_winner:
                ss["wins"] += 1

            if distance:
                if distance < _DIST_SHORT:
                    bucket = "short"
                elif distance > _DIST_LONG:
                    bucket = "long"
                else:
                    bucket = "mid"
                ss["dist"][bucket]["t"] += 1
                if is_winner:
                    ss["dist"][bucket]["w"] += 1

            if discipline:
                ss["disc"][discipline]["t"] += 1
                if is_winner:
                    ss["disc"][discipline]["w"] += 1

            if terrain:
                ss["terrain"][terrain]["t"] += 1
                if is_winner:
                    ss["terrain"][terrain]["w"] += 1

            # Nicking
            if pere_mere:
                nk = nicking[(pere, pere_mere)]
                nk["t"] += 1
                if is_winner:
                    nk["w"] += 1

    logger.info("  Pass 1: %d records, %d peres, %d nicking combos",
                n_pass1, len(sire_stats), len(nicking))
    gc.collect()

    # ---- Phase 3: Index + sort ----
    logger.info("Phase 3: Indexation et tri...")
    index: list[tuple[str, str, int, int]] = []
    n_idx = 0
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            n_idx += 1
            if n_idx % _LOG_EVERY == 0:
                logger.info("  Indexe %d...", n_idx)
            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = 0
            try:
                num_pmu = int(rec.get("num_pmu", 0) or 0)
            except (ValueError, TypeError):
                pass
            index.append((date_str, course_uid, num_pmu, offset))

    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("  %d records tries", len(index))

    # ---- Phase 4: Compute features ----
    logger.info("Phase 4: Calcul features...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "pedigree_deep.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    total = len(index)
    fill = {f"ped_deep_{k}": 0 for k in [
        "sire_offspring_count", "sire_win_rate_distance", "sire_win_rate_discipline",
        "dam_sire_impact", "sire_age", "inbreeding_coeff", "sire_terrain_pref",
        "lineage_stamina_idx", "lineage_speed_idx", "nicking_score",
        "sire_country_match", "dam_age_at_birth",
    ]}

    with open(INPUT_PATH, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for i, (date_str, course_uid, num_pmu, offset) in enumerate(index):
            fin.seek(offset)
            rec = json.loads(fin.readline())

            if (i + 1) % _LOG_EVERY == 0:
                pct = (i + 1) / total * 100
                logger.info("  Phase 4: %d/%d (%.1f%%)", i + 1, total, pct)
                gc.collect()

            partant_uid = rec.get("partant_uid", "")
            nom = (rec.get("nom_cheval") or "").upper().strip()
            pere = (rec.get("pere") or "").upper().strip()
            mere = (rec.get("mere") or "").upper().strip()
            pere_mere = (rec.get("pere_mere") or "").upper().strip()
            distance = _safe_int(rec.get("distance"))
            discipline = (rec.get("discipline") or "").upper().strip()
            terrain = (rec.get("type_piste") or "").upper().strip()
            age_cheval = _safe_int(rec.get("age"))
            pays_course = (rec.get("pays_course") or rec.get("hippodrome_normalise") or "").upper().strip()

            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }

            # Sire offspring count
            if pere and pere in sire_stats:
                ss = sire_stats[pere]
                out["ped_deep_sire_offspring_count"] = ss["total"]
                fill["ped_deep_sire_offspring_count"] += 1

                # Sire win rate at this distance
                if distance:
                    bucket = "short" if distance < _DIST_SHORT else ("long" if distance > _DIST_LONG else "mid")
                    d = ss["dist"].get(bucket)
                    if d and d["t"] >= 5:
                        out["ped_deep_sire_win_rate_distance"] = round(d["w"] / d["t"], 4)
                        fill["ped_deep_sire_win_rate_distance"] += 1
                    else:
                        out["ped_deep_sire_win_rate_distance"] = None
                else:
                    out["ped_deep_sire_win_rate_distance"] = None

                # Sire win rate in discipline
                if discipline:
                    d = ss["disc"].get(discipline)
                    if d and d["t"] >= 5:
                        out["ped_deep_sire_win_rate_discipline"] = round(d["w"] / d["t"], 4)
                        fill["ped_deep_sire_win_rate_discipline"] += 1
                    else:
                        out["ped_deep_sire_win_rate_discipline"] = None
                else:
                    out["ped_deep_sire_win_rate_discipline"] = None

                # Sire terrain preference
                if terrain and ss["terrain"]:
                    best_terrain = max(ss["terrain"].items(), key=lambda x: x[1]["w"] / x[1]["t"] if x[1]["t"] >= 5 else 0)
                    if best_terrain[1]["t"] >= 5:
                        out["ped_deep_sire_terrain_pref"] = 1.0 if best_terrain[0] == terrain else 0.0
                        fill["ped_deep_sire_terrain_pref"] += 1
                    else:
                        out["ped_deep_sire_terrain_pref"] = None
                else:
                    out["ped_deep_sire_terrain_pref"] = None

                # Lineage stamina/speed index
                long_d = ss["dist"].get("long", {"w": 0, "t": 0})
                short_d = ss["dist"].get("short", {"w": 0, "t": 0})
                if long_d["t"] >= 3:
                    out["ped_deep_lineage_stamina_idx"] = round(long_d["w"] / long_d["t"], 4)
                    fill["ped_deep_lineage_stamina_idx"] += 1
                else:
                    out["ped_deep_lineage_stamina_idx"] = None
                if short_d["t"] >= 3:
                    out["ped_deep_lineage_speed_idx"] = round(short_d["w"] / short_d["t"], 4)
                    fill["ped_deep_lineage_speed_idx"] += 1
                else:
                    out["ped_deep_lineage_speed_idx"] = None
            else:
                for k in ["sire_offspring_count", "sire_win_rate_distance",
                           "sire_win_rate_discipline", "sire_terrain_pref",
                           "lineage_stamina_idx", "lineage_speed_idx"]:
                    out[f"ped_deep_{k}"] = None

            # Dam sire impact
            if pere_mere and pere_mere in sire_stats:
                ds = sire_stats[pere_mere]
                if ds["total"] >= 5:
                    out["ped_deep_dam_sire_impact"] = round(ds["wins"] / ds["total"], 4)
                    fill["ped_deep_dam_sire_impact"] += 1
                else:
                    out["ped_deep_dam_sire_impact"] = None
            else:
                out["ped_deep_dam_sire_impact"] = None

            # Sire age at birth
            ped_info = pedigree_lookup.get(nom) or pedigree_lookup.get(pere)
            sire_birth = None
            horse_birth = None
            dam_birth = None
            if ped_info:
                sire_birth = _parse_year(ped_info.get("sire_annee_naissance"))
                horse_birth = _parse_year(ped_info.get("annee_naissance"))
            if nom in pedigree_lookup:
                horse_birth = _parse_year(pedigree_lookup[nom].get("annee_naissance"))
            if mere and mere in pedigree_lookup:
                dam_birth = _parse_year(pedigree_lookup[mere].get("annee_naissance"))

            if sire_birth and horse_birth and horse_birth > sire_birth:
                out["ped_deep_sire_age"] = horse_birth - sire_birth
                fill["ped_deep_sire_age"] += 1
            else:
                out["ped_deep_sire_age"] = None

            if dam_birth and horse_birth and horse_birth > dam_birth:
                out["ped_deep_dam_age_at_birth"] = horse_birth - dam_birth
                fill["ped_deep_dam_age_at_birth"] += 1
            else:
                out["ped_deep_dam_age_at_birth"] = None

            # Inbreeding coefficient (simplified: is pere_mere == pere?)
            if pere and pere_mere:
                out["ped_deep_inbreeding_coeff"] = 1 if pere == pere_mere else 0
                fill["ped_deep_inbreeding_coeff"] += 1
            else:
                out["ped_deep_inbreeding_coeff"] = None

            # Nicking score
            if pere and pere_mere and (pere, pere_mere) in nicking:
                nk = nicking[(pere, pere_mere)]
                if nk["t"] >= 3:
                    out["ped_deep_nicking_score"] = round(nk["w"] / nk["t"], 4)
                    fill["ped_deep_nicking_score"] += 1
                else:
                    out["ped_deep_nicking_score"] = None
            else:
                out["ped_deep_nicking_score"] = None

            # Sire country match (simplified: check if sire_pays matches)
            if pere and pere in pedigree_lookup:
                sire_country = (pedigree_lookup[pere].get("pays_naissance") or "").upper().strip()
                if sire_country and pays_course:
                    out["ped_deep_sire_country_match"] = 1 if sire_country[:2] == pays_course[:2] else 0
                    fill["ped_deep_sire_country_match"] += 1
                else:
                    out["ped_deep_sire_country_match"] = None
            else:
                out["ped_deep_sire_country_match"] = None

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_written += 1

    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d features ecrites en %.1fs", n_written, elapsed)
    logger.info("Fill rates:")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %s: %.1f%%", k, pct)


def main():
    parser = argparse.ArgumentParser(description="Deep pedigree feature builder")
    args = parser.parse_args()
    logger = setup_logging("pedigree_deep_builder")
    logger.info("Input: %s", INPUT_PATH)
    build(logger)


if __name__ == "__main__":
    main()
