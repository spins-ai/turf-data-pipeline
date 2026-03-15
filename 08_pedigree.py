#!/usr/bin/env python3
"""
08_pedigree.py
==============
Calcule les statistiques de performance par pere et par mere
a partir des partants normalises.

Input :
  - output/02_liste_courses/partants_normalises.json

Output : output/08_pedigree/
  - pedigree_peres.json / .parquet / .csv
  - pedigree_meres.json / .parquet / .csv

Usage :
    python3 08_pedigree.py
    python3 08_pedigree.py --help
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional

# Imports optionnels
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_PATH = Path("output/02_liste_courses/partants_normalises.json")
OUTPUT_DIR = Path("output/08_pedigree")
LOG_DIR = Path("logs")


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("08_pedigree")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "08_pedigree.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ===========================================================================
# SAUVEGARDE
# ===========================================================================

def sauver_json(data: list[dict], path: Path, logger: logging.Logger):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)
    logger.info("Sauve: %s (%d entrees)", path.name, len(data))


def sauver_parquet(data: list[dict], path: Path, logger: logging.Logger):
    if not HAS_PARQUET or not data:
        return
    try:
        flat = []
        for row in data:
            r = {}
            for k, v in row.items():
                if isinstance(v, (set, frozenset)):
                    r[k] = sorted(v)
                else:
                    r[k] = v
            flat.append(r)
        table = pa.Table.from_pylist(flat)
        pq.write_table(table, path)
        logger.info("Sauve: %s", path.name)
    except Exception as e:
        logger.warning("Parquet ignore: %s", e)


def sauver_csv(data: list[dict], path: Path, logger: logging.Logger):
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(data[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            flat_row = {}
            for k, v in row.items():
                if isinstance(v, (list, set, frozenset)):
                    flat_row[k] = json.dumps(sorted(v) if isinstance(v, (set, frozenset)) else v,
                                             ensure_ascii=False, default=str)
                else:
                    flat_row[k] = v
            writer.writerow(flat_row)
    logger.info("Sauve: %s", path.name)


# ===========================================================================
# LOGIQUE
# ===========================================================================

def build_pedigree_stats(
    partants: list[dict],
    champ_parent: str,
    logger: logging.Logger,
) -> list[dict]:
    """
    Construit les stats par parent (pere ou mere).
    champ_parent : 'pere' ou 'mere'
    """
    parents: dict[str, list[dict]] = defaultdict(list)
    skipped = 0

    for p in partants:
        nom_parent = p.get(champ_parent, "").strip()
        if not nom_parent:
            skipped += 1
            continue
        if p.get("statut") == "non_partant":
            continue
        parents[nom_parent].append(p)

    if skipped:
        logger.info("Partants sans %s ignores: %d", champ_parent, skipped)

    logger.info("Parents uniques (%s): %d", champ_parent, len(parents))

    # Calculer le taux de victoire global pour determiner les hippodromes forts
    global_wins = 0
    global_total = 0
    for descendants in parents.values():
        global_total += len(descendants)
        global_wins += sum(1 for d in descendants if d.get("is_gagnant"))
    taux_victoire_global = global_wins / global_total if global_total else 0
    logger.info("Taux de victoire global: %.4f (%d/%d)", taux_victoire_global, global_wins, global_total)

    results = []

    for nom_parent, descendants in parents.items():
        nb_courses = len(descendants)
        nb_victoires = sum(1 for d in descendants if d.get("is_gagnant"))
        taux_victoire = round(nb_victoires / nb_courses, 4) if nb_courses else None

        # Distances des victoires pour la distance de predilection
        distances_victoires = []
        for d in descendants:
            if d.get("is_gagnant") and d.get("distance") is not None:
                distances_victoires.append(d["distance"])

        distance_predilection = None
        if distances_victoires:
            distance_predilection = int(statistics.median(distances_victoires))

        # Disciplines
        disciplines = set()
        for d in descendants:
            disc = d.get("discipline", "")
            if disc:
                disciplines.add(disc)

        # Hippodromes forts : ceux ou le taux de victoire est superieur a la moyenne globale
        hippo_counter: Counter = Counter()  # total courses par hippodrome
        hippo_wins: Counter = Counter()  # victoires par hippodrome
        for d in descendants:
            h = d.get("hippodrome_normalise", "")
            if h:
                hippo_counter[h] += 1
                if d.get("is_gagnant"):
                    hippo_wins[h] += 1

        hippodromes_forts = []
        for h, total in hippo_counter.items():
            if total < 2:
                # Ignorer les hippodromes avec trop peu de courses (non significatif)
                continue
            taux_h = hippo_wins.get(h, 0) / total
            if taux_h > taux_victoire_global:
                hippodromes_forts.append(h)
        hippodromes_forts.sort()

        nom_field = f"nom_{champ_parent}"
        record = {
            nom_field: nom_parent,
            "nb_descendants_courses": nb_courses,
            "nb_descendants_victoires": nb_victoires,
            "taux_victoire_descendants": taux_victoire,
            "distances_predilection": distance_predilection,
            "disciplines": sorted(disciplines),
            "hippodromes_forts": hippodromes_forts,
        }
        results.append(record)

    # Trier par nombre de courses decroissant
    results.sort(key=lambda x: (-x["nb_descendants_courses"], x[f"nom_{champ_parent}"]))
    return results


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Calcule les stats de performance par pere et par mere (pedigree)."
    )
    parser.add_argument(
        "--partants", type=str, default=str(PARTANTS_PATH),
        help=f"Chemin vers partants_normalises.json (defaut: {PARTANTS_PATH})"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})"
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("08 — PEDIGREE (STATS PAR PERE / MERE)")
    logger.info("=" * 70)

    partants_path = Path(args.partants)
    output_dir = Path(args.output_dir)

    # Charger partants
    if not partants_path.exists():
        logger.error("Fichier introuvable: %s", partants_path)
        sys.exit(1)
    with open(partants_path, "r", encoding="utf-8") as f:
        partants = json.load(f)
    logger.info("Partants charges: %d", len(partants))

    output_dir.mkdir(parents=True, exist_ok=True)

    # === Peres ===
    logger.info("-" * 50)
    logger.info("Construction stats PERES...")
    pedigree_peres = build_pedigree_stats(partants, "pere", logger)
    logger.info("Peres construits: %d", len(pedigree_peres))

    sauver_json(pedigree_peres, output_dir / "pedigree_peres.json", logger)
    sauver_parquet(pedigree_peres, output_dir / "pedigree_peres.parquet", logger)
    sauver_csv(pedigree_peres, output_dir / "pedigree_peres.csv", logger)

    # === Meres ===
    logger.info("-" * 50)
    logger.info("Construction stats MERES...")
    pedigree_meres = build_pedigree_stats(partants, "mere", logger)
    logger.info("Meres construits: %d", len(pedigree_meres))

    sauver_json(pedigree_meres, output_dir / "pedigree_meres.json", logger)
    sauver_parquet(pedigree_meres, output_dir / "pedigree_meres.parquet", logger)
    sauver_csv(pedigree_meres, output_dir / "pedigree_meres.csv", logger)

    # Stats
    logger.info("-" * 50)
    logger.info("RESUME PERES:")
    if pedigree_peres:
        total_desc_p = sum(p["nb_descendants_courses"] for p in pedigree_peres)
        total_vic_p = sum(p["nb_descendants_victoires"] for p in pedigree_peres)
        logger.info("  Peres uniques               : %d", len(pedigree_peres))
        logger.info("  Total courses descendants    : %d", total_desc_p)
        logger.info("  Total victoires descendants  : %d", total_vic_p)
        logger.info("  Taux victoire moyen          : %.4f",
                     total_vic_p / total_desc_p if total_desc_p else 0)
        with_dist = [p for p in pedigree_peres if p["distances_predilection"] is not None]
        if with_dist:
            dists = [p["distances_predilection"] for p in with_dist]
            logger.info("  Distance predilection moy.   : %dm", int(statistics.mean(dists)))

    logger.info("RESUME MERES:")
    if pedigree_meres:
        total_desc_m = sum(p["nb_descendants_courses"] for p in pedigree_meres)
        total_vic_m = sum(p["nb_descendants_victoires"] for p in pedigree_meres)
        logger.info("  Meres uniques               : %d", len(pedigree_meres))
        logger.info("  Total courses descendants    : %d", total_desc_m)
        logger.info("  Total victoires descendants  : %d", total_vic_m)
        logger.info("  Taux victoire moyen          : %.4f",
                     total_vic_m / total_desc_m if total_desc_m else 0)

    logger.info("=" * 70)
    logger.info("TERMINE")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
