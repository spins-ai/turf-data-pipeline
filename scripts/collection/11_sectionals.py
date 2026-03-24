#!/usr/bin/env python3
"""
11_sectionals.py
================
Calcul des metriques de temps / vitesse / reduction kilometrique
a partir des donnees collectees par 02_liste_courses.py.

Aucun appel API : traitement 100% local.

Produit :
  - sectionals.json / .parquet / .csv

Usage :
    python3 11_sectionals.py
    python3 11_sectionals.py --partants output/02_liste_courses/partants_normalises.json
"""

from __future__ import annotations

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import argparse
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional


# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path(__file__).resolve().parent / "../../output" / "02_liste_courses" / "partants_normalises.json"
INPUT_COURSES = Path(__file__).resolve().parent / "../../output" / "02_liste_courses" / "courses_normalisees.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "../../output" / "11_sectionals"

from utils.logging_setup import setup_logging
from utils.output import sauver_json, sauver_csv, sauver_parquet
from utils.loaders import load_json_safe


# ===========================================================================
# SAUVEGARDE
# ===========================================================================


# ===========================================================================
# UTILITAIRES
# ===========================================================================

def redkm_ms_to_sec_str(redkm_ms: Optional[int]) -> Optional[str]:
    """
    Convertit reduction_km_ms (ms/km) en format XX"Y (secondes).
    Ex: 73000 ms/km -> 1'13"0
    """
    if redkm_ms is None:
        return None
    total_sec = redkm_ms / 1000.0
    minutes = int(total_sec // 60)
    sec = total_sec - minutes * 60
    if minutes > 0:
        return f"{minutes}'{sec:04.1f}\""
    return f'{sec:.1f}"'


# ===========================================================================
# TRAITEMENT
# ===========================================================================


def construire_sectionals(
    partants: list[dict],
    courses: list[dict],
    logger: logging.Logger,
) -> list[dict]:
    """
    Pour chaque partant avec temps_ms et/ou reduction_km_ms, calcule
    les metriques de vitesse et ecarts.
    """
    # Index des courses par course_uid
    courses_idx: dict[str, dict] = {}
    for c in courses:
        uid = c.get("course_uid", "")
        if uid:
            courses_idx[uid] = c

    # Regrouper partants par course_uid
    par_course: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        cuid = p.get("course_uid", "")
        if cuid and p.get("statut", "") != "non_partant":
            par_course[cuid].append(p)

    # Stats par course : vitesse moyenne, temps gagnant, redkm gagnant
    stats_course: dict[str, dict] = {}
    for cuid, parts in par_course.items():
        course_info = courses_idx.get(cuid, {})
        distance = course_info.get("distance") or 0

        vitesses = []
        temps_gagnant = None
        redkm_gagnant = None

        for p in parts:
            temps = p.get("temps_ms")
            if temps and temps > 0 and distance and distance > 0:
                v = (distance / 1000.0) / (temps / 3600000.0)
                vitesses.append(v)

            # Le gagnant : position_arrivee == 1 ou is_gagnant
            if p.get("is_gagnant") or p.get("position_arrivee") == 1:
                if temps and temps > 0:
                    temps_gagnant = temps
                redkm = p.get("reduction_km_ms")
                if redkm and redkm > 0:
                    redkm_gagnant = redkm

        stats_course[cuid] = {
            "vitesse_moyenne": (sum(vitesses) / len(vitesses)) if vitesses else None,
            "temps_gagnant": temps_gagnant,
            "redkm_gagnant": redkm_gagnant,
            "distance": distance,
        }

    # Construire les records
    resultats = []
    nb_skip = 0

    for p in partants:
        temps_ms = p.get("temps_ms")
        redkm_ms = p.get("reduction_km_ms")

        # On garde les partants qui ont au moins un des deux
        if temps_ms is None and redkm_ms is None:
            nb_skip += 1
            continue
        if p.get("statut", "") == "non_partant":
            nb_skip += 1
            continue

        course_uid = p.get("course_uid", "")
        partant_uid = p.get("partant_uid", "")
        nom_cheval = p.get("nom_cheval", "")
        date_iso = p.get("date_reunion_iso", "")

        sc = stats_course.get(course_uid, {})
        distance = sc.get("distance") or 0

        # Vitesse km/h
        vitesse_kmh = None
        if temps_ms and temps_ms > 0 and distance and distance > 0:
            vitesse_kmh = round((distance / 1000.0) / (temps_ms / 3600000.0), 2)

        # Reduction km en secondes
        reduction_km_sec = None
        if redkm_ms is not None:
            reduction_km_sec = round(redkm_ms / 1000.0, 1)

        reduction_km_str = redkm_ms_to_sec_str(redkm_ms)

        # Vitesse relative
        vitesse_relative = None
        vitesse_moy = sc.get("vitesse_moyenne")
        if vitesse_kmh and vitesse_moy and vitesse_moy > 0:
            vitesse_relative = round(vitesse_kmh / vitesse_moy, 4)

        # Ecart temps gagnant
        ecart_temps_gagnant = None
        temps_gagnant = sc.get("temps_gagnant")
        if temps_ms and temps_gagnant and temps_ms > 0 and temps_gagnant > 0:
            ecart_temps_gagnant = temps_ms - temps_gagnant

        # Ecart redkm gagnant
        ecart_redkm_gagnant = None
        redkm_gagnant = sc.get("redkm_gagnant")
        if redkm_ms and redkm_gagnant and redkm_ms > 0 and redkm_gagnant > 0:
            ecart_redkm_gagnant = redkm_ms - redkm_gagnant

        record = {
            "partant_uid": partant_uid,
            "course_uid": course_uid,
            "nom_cheval": nom_cheval,
            "date_reunion_iso": date_iso,
            "distance_m": distance if distance else None,
            "temps_ms": temps_ms,
            "reduction_km_ms": redkm_ms,
            "vitesse_kmh": vitesse_kmh,
            "reduction_km_sec": reduction_km_sec,
            "reduction_km_str": reduction_km_str,
            "vitesse_relative": vitesse_relative,
            "ecart_temps_gagnant": ecart_temps_gagnant,
            "ecart_redkm_gagnant": ecart_redkm_gagnant,
        }
        resultats.append(record)

    logger.info("  %d enregistrements sectionals generes (%d ignores)", len(resultats), nb_skip)
    return resultats


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Metriques temps / vitesse / sectionals (local, sans API)"
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

    logger = setup_logging("11_sectionals")
    logger.info("=" * 70)
    logger.info("11_sectionals.py — Metriques temps / vitesse")
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

    partants = load_json_safe(partants_path, str(partants_path), logger)
    courses = load_json_safe(courses_path, str(courses_path), logger)
    resultats = construire_sectionals(partants, courses, logger)

    # Export
    output_dir.mkdir(parents=True, exist_ok=True)
    sauver_json(resultats, output_dir / "sectionals.json", logger)
    sauver_parquet(resultats, output_dir / "sectionals.parquet", logger)
    sauver_csv(resultats, output_dir / "sectionals.csv", logger)

    logger.info("Termine — %d enregistrements", len(resultats))


if __name__ == "__main__":
    main()
