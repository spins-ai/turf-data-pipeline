#!/usr/bin/env python3
"""
10_poids_handicaps.py
=====================
Calcul des metriques poids / handicaps par partant
a partir des donnees collectees par 02_liste_courses.py.

Aucun appel API : traitement 100% local.

Produit :
  - poids_handicaps.json / .parquet / .csv

Usage :
    python3 10_poids_handicaps.py
    python3 10_poids_handicaps.py --partants output/02_liste_courses/partants_normalises.json
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

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

INPUT_PARTANTS = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "partants_normalises.json"
INPUT_COURSES = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "courses_normalisees.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "10_poids_handicaps"

from utils.logging_setup import setup_logging
from utils.output import sauver_json, sauver_csv


# ===========================================================================
# SAUVEGARDE
# ===========================================================================




def sauver_parquet(data: list[dict], path: Path, logger: logging.Logger):
    if not HAS_PARQUET or not data:
        return
    try:
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path)
        logger.info("Sauve: %s", path.name)
    except Exception as e:
        logger.warning("Parquet ignore: %s", e)





# ===========================================================================
# TRAITEMENT
# ===========================================================================

def charger_json(path: Path, logger: logging.Logger) -> list[dict]:
    logger.info("Chargement: %s", path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info("  %d entrees chargees", len(data))
    return data


def construire_poids_handicaps(
    partants: list[dict],
    courses: list[dict],
    logger: logging.Logger,
) -> list[dict]:
    """
    Pour chaque partant ayant un poids_porte_kg, calcule les metriques
    relatives au poids et aux handicaps.
    """
    # Index des courses par course_uid pour la distance
    courses_idx: dict[str, dict] = {}
    for c in courses:
        uid = c.get("course_uid", "")
        if uid:
            courses_idx[uid] = c

    # Regrouper partants par course_uid (pour stats de course)
    par_course: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        cuid = p.get("course_uid", "")
        if cuid and p.get("statut", "") != "non_partant":
            par_course[cuid].append(p)

    # Calculer poids moyen et max par course
    stats_course: dict[str, dict] = {}
    for cuid, parts in par_course.items():
        poids_list = [
            p["poids_porte_kg"]
            for p in parts
            if p.get("poids_porte_kg") is not None
        ]
        if poids_list:
            stats_course[cuid] = {
                "poids_moyen": sum(poids_list) / len(poids_list),
                "poids_max": max(poids_list),
            }

    # Historique poids par cheval (pour poids_precedent)
    par_cheval: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        nom = p.get("nom_cheval", "")
        if nom and p.get("statut", "") != "non_partant":
            par_cheval[nom].append(p)

    # Trier chaque cheval par date
    for nom in par_cheval:
        par_cheval[nom].sort(key=lambda x: x.get("date_reunion_iso", ""))

    # Index : (nom_cheval, course_uid) -> index dans la liste triee
    cheval_course_idx: dict[tuple[str, str], int] = {}
    for nom, courses_list in par_cheval.items():
        for i, p in enumerate(courses_list):
            cheval_course_idx[(nom, p.get("course_uid", ""))] = i

    # Construire les records
    resultats = []
    nb_skip = 0

    for p in partants:
        poids = p.get("poids_porte_kg")
        if poids is None:
            nb_skip += 1
            continue
        if p.get("statut", "") == "non_partant":
            nb_skip += 1
            continue

        course_uid = p.get("course_uid", "")
        nom_cheval = p.get("nom_cheval", "")
        partant_uid = p.get("partant_uid", "")
        date_iso = p.get("date_reunion_iso", "")

        # Distance depuis la course
        course_info = courses_idx.get(course_uid, {})
        distance = course_info.get("distance") or p.get("distance")

        # Stats de course
        sc = stats_course.get(course_uid, {})
        poids_moyen = sc.get("poids_moyen")
        poids_max = sc.get("poids_max")

        poids_relatif = None
        ecart_top_weight = None
        if poids_moyen is not None:
            poids_relatif = round(poids - poids_moyen, 2)
        if poids_max is not None:
            ecart_top_weight = round(poids - poids_max, 2)

        # Poids precedent
        poids_precedent = None
        evolution_poids = None
        idx = cheval_course_idx.get((nom_cheval, course_uid))
        if idx is not None and idx > 0:
            prev = par_cheval[nom_cheval][idx - 1]
            poids_precedent = prev.get("poids_porte_kg")
            if poids_precedent is not None:
                evolution_poids = round(poids - poids_precedent, 2)

        # Poids par km
        poids_par_km = None
        if distance and distance > 0:
            poids_par_km = round(poids / (distance / 1000), 2)

        record = {
            "partant_uid": partant_uid,
            "course_uid": course_uid,
            "nom_cheval": nom_cheval,
            "date_reunion_iso": date_iso,
            "poids_porte_kg": poids,
            "handicap_valeur": p.get("handicap_valeur"),
            "handicap_distance_m": p.get("handicap_distance_m"),
            "poids_moyen_course": round(poids_moyen, 2) if poids_moyen is not None else None,
            "poids_relatif": poids_relatif,
            "ecart_top_weight": ecart_top_weight,
            "poids_precedent": poids_precedent,
            "evolution_poids": evolution_poids,
            "poids_par_km": poids_par_km,
        }
        resultats.append(record)

    logger.info("  %d enregistrements poids generes (%d ignores sans poids)", len(resultats), nb_skip)
    return resultats


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Metriques poids et handicaps par partant (local, sans API)"
    )
    parser.add_argument(
        "--partants", type=str, default=str(INPUT_PARTANTS),
        help="Chemin vers partants_normalises.json"
    )
    parser.add_argument(
        "--courses", type=str, default=str(INPUT_COURSES),
        help="Chemin vers courses_normalisees.json"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie"
    )
    args = parser.parse_args()

    logger = setup_logging("10_poids_handicaps")
    logger.info("=" * 70)
    logger.info("10_poids_handicaps.py — Metriques poids / handicaps")
    logger.info("=" * 70)

    partants_path = Path(args.partants)
    courses_path = Path(args.courses)
    output_dir = Path(args.output_dir)

    if not partants_path.exists():
        logger.error("Fichier introuvable: %s", partants_path)
        sys.exit(1)
    if not courses_path.exists():
        logger.error("Fichier introuvable: %s", courses_path)
        sys.exit(1)

    partants = charger_json(partants_path, logger)
    courses = charger_json(courses_path, logger)
    resultats = construire_poids_handicaps(partants, courses, logger)

    # Export
    output_dir.mkdir(parents=True, exist_ok=True)
    sauver_json(resultats, output_dir / "poids_handicaps.json", logger)
    sauver_parquet(resultats, output_dir / "poids_handicaps.parquet", logger)
    sauver_csv(resultats, output_dir / "poids_handicaps.csv", logger)

    logger.info("Termine — %d enregistrements", len(resultats))


if __name__ == "__main__":
    main()
