#!/usr/bin/env python3
"""
feature_builders.combinaisons_marche_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Market combination features from 28_combinaisons_marche (6M records).

Cross-references betting market structure (which combinations the public bets on,
how concentrated the money is) with partants_master.

Features (8):
  - cmb_nb_pari_types          : nb de types de paris sur cette course
  - cmb_total_enjeu_course     : enjeu total sur la course (tous paris)
  - cmb_enjeu_per_partant      : enjeu moyen par partant
  - cmb_horse_in_top3_combos   : 1 si le cheval est dans les 3 combos les plus jouees
  - cmb_horse_combo_frequency  : nb de combos contenant ce cheval / nb total combos
  - cmb_favorite_concentration : % de l'enjeu sur la combo la plus jouee (Herfindahl)
  - cmb_exotic_ratio           : ratio enjeu exotique (tierce+) vs simple
  - cmb_horse_pct_masse_avg    : moyenne pct_masse des combos contenant ce cheval

Memory: ~4 GB (course_uid -> aggregated combo stats)
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_COMBOS = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/28_combinaisons_marche/combinaisons_marche.json")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/combinaisons_marche")
_LOG_EVERY = 500_000

_EXOTIC_TYPES = {"TIERCE", "QUARTE", "QUARTE_PLUS", "QUINTE", "QUINTE_PLUS", "MULTI", "2SUR4", "PICK5"}
_SIMPLE_TYPES = {"SIMPLE_GAGNANT", "SIMPLE_PLACE", "COUPLE_GAGNANT", "COUPLE_PLACE", "COUPLE_ORDRE"}


def build(logger) -> None:
    t0 = time.time()

    # ---- Phase 1: Load and aggregate combo data by course_uid ----
    logger.info("Phase 1: Chargement combinaisons_marche (6M records)...")

    # course_uid -> {
    #   pari_types: set,
    #   total_enjeu: float,
    #   combos: [{num_pmu_list, enjeu, pct_masse, type_pari}],
    #   top_combos: [num_pmu sets for top 3 by enjeu],
    #   max_pct: float (concentration),
    #   enjeu_exotic: float,
    #   enjeu_simple: float,
    # }
    course_combos: dict[str, dict] = {}

    # Stream the JSON array efficiently
    n_loaded = 0
    with open(INPUT_COMBOS, "r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info("  Fichier charge: %d records", len(data))

    for rec in data:
        course_uid = rec.get("course_uid", "")
        if not course_uid:
            continue

        if course_uid not in course_combos:
            course_combos[course_uid] = {
                "pari_types": set(),
                "total_enjeu": 0.0,
                "combos": [],
                "enjeu_exotic": 0.0,
                "enjeu_simple": 0.0,
            }

        cc = course_combos[course_uid]
        type_pari = (rec.get("type_pari") or "").upper()
        cc["pari_types"].add(type_pari)

        enjeu = rec.get("enjeu_combinaison") or 0
        try:
            enjeu = float(enjeu)
        except (ValueError, TypeError):
            enjeu = 0.0

        total_enjeu_pari = rec.get("total_enjeu_pari") or 0
        try:
            total_enjeu_pari = float(total_enjeu_pari)
        except (ValueError, TypeError):
            total_enjeu_pari = 0.0

        pct = rec.get("pct_masse") or 0
        try:
            pct = float(pct)
        except (ValueError, TypeError):
            pct = 0.0

        combo_nums = rec.get("combinaison") or []
        if isinstance(combo_nums, list):
            combo_set = set(combo_nums)
        else:
            combo_set = set()

        cc["combos"].append({
            "nums": combo_set,
            "enjeu": enjeu,
            "pct": pct,
            "type": type_pari,
        })

        cc["total_enjeu"] += enjeu

        if type_pari in _EXOTIC_TYPES:
            cc["enjeu_exotic"] += enjeu
        elif type_pari in _SIMPLE_TYPES:
            cc["enjeu_simple"] += enjeu

        n_loaded += 1
        if n_loaded % 1_000_000 == 0:
            logger.info("  Agrege %d records...", n_loaded)

    del data
    gc.collect()

    # Pre-compute top 3 combos per course
    for uid, cc in course_combos.items():
        sorted_combos = sorted(cc["combos"], key=lambda x: x["enjeu"], reverse=True)
        cc["top3_nums"] = [c["nums"] for c in sorted_combos[:3]]
        cc["max_pct"] = sorted_combos[0]["pct"] if sorted_combos else 0
        cc["nb_combos"] = len(cc["combos"])

    logger.info("  %d courses avec combos", len(course_combos))

    # ---- Phase 2: Stream partants_master and compute features ----
    logger.info("Phase 2: Calcul features sur partants_master...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "combinaisons_marche_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    n_matched = 0
    fill = {k: 0 for k in [
        "cmb_nb_pari_types", "cmb_total_enjeu_course", "cmb_enjeu_per_partant",
        "cmb_horse_in_top3_combos", "cmb_horse_combo_frequency",
        "cmb_favorite_concentration", "cmb_exotic_ratio", "cmb_horse_pct_masse_avg",
    ]}

    with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_written += 1
            if n_written % _LOG_EVERY == 0:
                logger.info("  %d records traites, %d matched", n_written, n_matched)

            partant_uid = rec.get("partant_uid", "")
            course_uid = rec.get("course_uid", "")
            date_str = rec.get("date_reunion_iso", "")
            num_pmu = rec.get("num_pmu")
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = None
            nb_partants = rec.get("nombre_partants")
            try:
                nb_partants = int(nb_partants)
            except (ValueError, TypeError):
                nb_partants = None

            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }

            cc = course_combos.get(course_uid)
            if cc and num_pmu is not None:
                n_matched += 1

                out["cmb_nb_pari_types"] = len(cc["pari_types"])
                fill["cmb_nb_pari_types"] += 1

                out["cmb_total_enjeu_course"] = round(cc["total_enjeu"], 2)
                fill["cmb_total_enjeu_course"] += 1

                if nb_partants and nb_partants > 0:
                    out["cmb_enjeu_per_partant"] = round(cc["total_enjeu"] / nb_partants, 2)
                    fill["cmb_enjeu_per_partant"] += 1
                else:
                    out["cmb_enjeu_per_partant"] = None

                # Horse in top 3 combos
                in_top3 = any(num_pmu in nums for nums in cc["top3_nums"])
                out["cmb_horse_in_top3_combos"] = 1 if in_top3 else 0
                fill["cmb_horse_in_top3_combos"] += 1

                # Horse combo frequency
                combos_with_horse = sum(1 for c in cc["combos"] if num_pmu in c["nums"])
                if cc["nb_combos"] > 0:
                    out["cmb_horse_combo_frequency"] = round(combos_with_horse / cc["nb_combos"], 4)
                    fill["cmb_horse_combo_frequency"] += 1
                else:
                    out["cmb_horse_combo_frequency"] = None

                # Favorite concentration
                out["cmb_favorite_concentration"] = round(cc["max_pct"], 4)
                fill["cmb_favorite_concentration"] += 1

                # Exotic ratio
                total = cc["enjeu_exotic"] + cc["enjeu_simple"]
                if total > 0:
                    out["cmb_exotic_ratio"] = round(cc["enjeu_exotic"] / total, 4)
                    fill["cmb_exotic_ratio"] += 1
                else:
                    out["cmb_exotic_ratio"] = None

                # Horse avg pct_masse
                horse_pcts = [c["pct"] for c in cc["combos"] if num_pmu in c["nums"] and c["pct"] > 0]
                if horse_pcts:
                    out["cmb_horse_pct_masse_avg"] = round(sum(horse_pcts) / len(horse_pcts), 4)
                    fill["cmb_horse_pct_masse_avg"] += 1
                else:
                    out["cmb_horse_pct_masse_avg"] = None
            else:
                for k in fill:
                    out[k] = None

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d records, %d matched (%.1f%%) en %.1fs",
                n_written, n_matched, n_matched / n_written * 100 if n_written else 0, elapsed)
    logger.info("Fill rates:")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %s: %.1f%%", k, pct)


def main():
    parser = argparse.ArgumentParser(description="Combinaisons marche feature builder")
    args = parser.parse_args()
    logger = setup_logging("combinaisons_marche_builder")
    build(logger)


if __name__ == "__main__":
    main()
