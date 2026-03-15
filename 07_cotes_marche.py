#!/usr/bin/env python3
"""
07_cotes_marche.py
==================
Calcule des features derivees du marche des cotes pour chaque partant.

Input :
  - output/02_liste_courses/partants_normalises.json

Output : output/07_cotes_marche/
  - cotes_marche.json / .parquet / .csv

Usage :
    python3 07_cotes_marche.py
    python3 07_cotes_marche.py --help
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import statistics
import sys
from collections import defaultdict
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
OUTPUT_DIR = Path("output/07_cotes_marche")
LOG_DIR = Path("logs")


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("07_cotes_marche")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "07_cotes_marche.log", encoding="utf-8")
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
        table = pa.Table.from_pylist(data)
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
        writer.writerows(data)
    logger.info("Sauve: %s", path.name)


# ===========================================================================
# LOGIQUE
# ===========================================================================

def build_cotes_marche(
    partants: list[dict],
    logger: logging.Logger,
) -> list[dict]:
    """Calcule les features de marche pour chaque partant."""

    # Grouper par course_uid
    courses: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        if p.get("statut") == "non_partant":
            continue
        course_uid = p.get("course_uid", "")
        if course_uid:
            courses[course_uid].append(p)

    logger.info("Courses avec partants: %d", len(courses))

    results = []
    no_cote_count = 0

    for course_uid, field in courses.items():
        # Collecter les cotes valides dans cette course
        cotes_valides = []
        for p in field:
            cote = p.get("cote_finale")
            if cote is not None and cote > 0:
                cotes_valides.append(cote)

        nb_partants_course = len(field)

        # Stats de cotes pour la course
        if cotes_valides:
            cote_moyenne = round(statistics.mean(cotes_valides), 2)
            cote_mediane = round(statistics.median(cotes_valides), 2)
            cote_min = min(cotes_valides)
        else:
            cote_moyenne = None
            cote_mediane = None
            cote_min = None

        # Trier par cote pour le rang
        field_with_cote = [(p, p.get("cote_finale")) for p in field]
        # Trier: cote croissante, None a la fin
        field_with_cote.sort(key=lambda x: (x[1] is None, x[1] if x[1] is not None else float("inf")))

        for rank_idx, (p, cote) in enumerate(field_with_cote, 1):
            cote_finale = p.get("cote_finale")
            cote_reference = p.get("cote_reference")

            # Probabilite implicite
            proba = None
            if cote_finale is not None and cote_finale > 0:
                proba = round(1.0 / cote_finale, 4)

            # Rang par cote (1 = favori)
            rang_cote = rank_idx if cote_finale is not None else None

            # Favori / outsider
            is_favori = (cote_finale is not None and cote_min is not None
                         and cote_finale == cote_min)
            is_outsider = cote_finale is not None and cote_finale > 20

            # Ecart cote vs moyenne
            ecart = None
            if cote_finale is not None and cote_moyenne is not None:
                ecart = round(cote_finale - cote_moyenne, 2)

            if cote_finale is None:
                no_cote_count += 1

            record = {
                "partant_uid": p.get("partant_uid", ""),
                "course_uid": course_uid,
                "date_reunion_iso": p.get("date_reunion_iso", ""),
                "cote_finale": cote_finale,
                "cote_reference": cote_reference,
                "proba_implicite": proba,
                "rang_cote": rang_cote,
                "is_favori": is_favori,
                "is_outsider": is_outsider,
                "nb_partants_course": nb_partants_course,
                "cote_moyenne_course": cote_moyenne,
                "cote_mediane_course": cote_mediane,
                "ecart_cote_moyenne": ecart,
            }
            results.append(record)

    if no_cote_count:
        logger.info("Partants sans cote finale: %d", no_cote_count)

    return results


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Calcule des features de marche derivees des cotes pour chaque partant."
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
    logger.info("07 — COTES & FEATURES MARCHE")
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

    # Construire les features de marche
    cotes_marche = build_cotes_marche(partants, logger)
    logger.info("Records construits: %d", len(cotes_marche))

    # Sauvegarder
    output_dir.mkdir(parents=True, exist_ok=True)
    sauver_json(cotes_marche, output_dir / "cotes_marche.json", logger)
    sauver_parquet(cotes_marche, output_dir / "cotes_marche.parquet", logger)
    sauver_csv(cotes_marche, output_dir / "cotes_marche.csv", logger)

    # Stats
    if cotes_marche:
        with_cote = [r for r in cotes_marche if r["cote_finale"] is not None]
        favoris = [r for r in cotes_marche if r["is_favori"]]
        outsiders = [r for r in cotes_marche if r["is_outsider"]]

        logger.info("-" * 50)
        logger.info("RESUME:")
        logger.info("  Total partants traites : %d", len(cotes_marche))
        logger.info("  Avec cote finale       : %d (%.1f%%)",
                     len(with_cote),
                     100 * len(with_cote) / len(cotes_marche) if cotes_marche else 0)
        logger.info("  Favoris                : %d", len(favoris))
        logger.info("  Outsiders (cote > 20)  : %d", len(outsiders))
        if with_cote:
            cotes = [r["cote_finale"] for r in with_cote]
            logger.info("  Cote moyenne globale   : %.2f", statistics.mean(cotes))
            logger.info("  Cote mediane globale   : %.2f", statistics.median(cotes))
            logger.info("  Cote min               : %.1f", min(cotes))
            logger.info("  Cote max               : %.1f", max(cotes))

    logger.info("=" * 70)
    logger.info("TERMINE")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
