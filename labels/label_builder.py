#!/usr/bin/env python3
"""
labels/label_builder.py
=======================
Construit les labels (variables cibles) pour chaque partant
a partir des donnees collectees par 02_liste_courses.py.

Aucun appel API : traitement 100% local.

Labels produits :
  - y_gagnant, y_place_top3, y_place_top5
  - y_rang, y_rang_normalise
  - y_roi_simple_gagnant, y_roi_simple_place
  - y_surprise, y_favori_gagne

Produit :
  - labels.json / .parquet / .csv

Usage :
    python3 labels/label_builder.py
    python3 labels/label_builder.py --input output/02_liste_courses/partants_normalises.json
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


# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
INPUT_PARTANTS = _PROJECT_ROOT / "output" / "02_liste_courses" / "partants_normalises.json"
OUTPUT_DIR = _PROJECT_ROOT / "output" / "labels"


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging
from utils.output import sauver_json, sauver_csv, sauver_parquet


# ===========================================================================
# SAUVEGARDE
# ===========================================================================





# ===========================================================================
# TRAITEMENT
# ===========================================================================

def charger_json(path: Path, logger: logging.Logger) -> list[dict]:
    logger.info("Chargement: %s", path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info("  %d entrees chargees", len(data))
    return data


def construire_labels(partants: list[dict], logger: logging.Logger) -> list[dict]:
    """
    Construit les labels pour chaque partant.
    """
    # Regrouper par course_uid pour calculer nb_partants et favori
    par_course: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        cuid = p.get("course_uid", "")
        if cuid and p.get("statut", "") != "non_partant":
            par_course[cuid].append(p)

    # Pre-calculer : nb_partants, favori (cote la plus basse), gagnant de chaque course
    info_course: dict[str, dict] = {}
    for cuid, parts in par_course.items():
        nb = len(parts)

        # Favori = cote_finale la plus basse (hors None)
        cotes = [
            (p.get("partant_uid", ""), p.get("cote_finale"))
            for p in parts
            if p.get("cote_finale") is not None
        ]
        favori_uid = None
        if cotes:
            favori_uid = min(cotes, key=lambda x: x[1])[0]

        # Gagnant
        gagnant_uid = None
        gagnant_cote = None
        for p in parts:
            if p.get("is_gagnant") or p.get("position_arrivee") == 1:
                gagnant_uid = p.get("partant_uid", "")
                gagnant_cote = p.get("cote_finale")
                break

        info_course[cuid] = {
            "nb_partants": nb,
            "favori_uid": favori_uid,
            "gagnant_uid": gagnant_uid,
            "gagnant_cote": gagnant_cote,
        }

    resultats = []
    nb_skip = 0

    for p in partants:
        if p.get("statut", "") == "non_partant":
            nb_skip += 1
            continue

        partant_uid = p.get("partant_uid", "")
        course_uid = p.get("course_uid", "")
        date_iso = p.get("date_reunion_iso", "")
        position = p.get("position_arrivee")
        cote = p.get("cote_finale")
        is_gagnant = p.get("is_gagnant", False)
        is_place = p.get("is_place", False)
        is_dq = p.get("is_disqualifie", False)

        ic = info_course.get(course_uid, {})
        nb_partants = ic.get("nb_partants", 0)
        gagnant_uid = ic.get("gagnant_uid")
        gagnant_cote = ic.get("gagnant_cote")
        favori_uid = ic.get("favori_uid")

        # y_gagnant
        y_gagnant = 1 if is_gagnant else 0

        # y_place_top3
        y_place_top3 = 1 if (is_place or (position is not None and 1 <= position <= 3)) else 0

        # y_place_top5
        y_place_top5 = 1 if (position is not None and 1 <= position <= 5) else 0

        # y_rang : None si DNF / disqualifie sans position
        y_rang = None
        if is_dq and position is None:
            y_rang = None
        elif position is not None:
            y_rang = position
        # Si pas de position et pas disqualifie, on laisse None (course non terminee?)

        # y_rang_normalise
        y_rang_normalise = None
        if y_rang is not None and nb_partants > 0:
            y_rang_normalise = round(y_rang / nb_partants, 4)

        # y_roi_simple_gagnant : (cote * y_gagnant) - 1
        y_roi_simple_gagnant = None
        if cote is not None:
            y_roi_simple_gagnant = round((cote * y_gagnant) - 1, 2)

        # y_roi_simple_place : approximation (cote/3 * y_place_top3) - 1
        y_roi_simple_place = None
        if cote is not None:
            y_roi_simple_place = round((cote / 3.0 * y_place_top3) - 1, 2)

        # y_surprise : 1 si le gagnant avait une cote > 10
        y_surprise = None
        if gagnant_cote is not None:
            y_surprise = 1 if (y_gagnant == 1 and gagnant_cote > 10) else 0
        elif y_gagnant == 1 and cote is not None and cote > 10:
            y_surprise = 1
        else:
            y_surprise = 0

        # y_favori_gagne : 1 si le favori a gagne (meme valeur pour tous les partants de la course)
        y_favori_gagne = None
        if favori_uid and gagnant_uid:
            y_favori_gagne = 1 if favori_uid == gagnant_uid else 0

        record = {
            "partant_uid": partant_uid,
            "course_uid": course_uid,
            "date_reunion_iso": date_iso,
            "y_gagnant": y_gagnant,
            "y_place_top3": y_place_top3,
            "y_place_top5": y_place_top5,
            "y_rang": y_rang,
            "y_rang_normalise": y_rang_normalise,
            "y_roi_simple_gagnant": y_roi_simple_gagnant,
            "y_roi_simple_place": y_roi_simple_place,
            "y_surprise": y_surprise,
            "y_favori_gagne": y_favori_gagne,
        }
        resultats.append(record)

    logger.info("  %d labels generes (%d non-partants ignores)", len(resultats), nb_skip)
    return resultats


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Construction des labels / variables cibles (local, sans API)"
    )
    parser.add_argument(
        "--input", type=str, default=str(INPUT_PARTANTS),
        help="Chemin vers partants_normalises.json"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie"
    )
    args = parser.parse_args()

    logger = setup_logging("label_builder")
    logger.info("=" * 70)
    logger.info("label_builder.py — Construction des labels")
    logger.info("=" * 70)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    partants = charger_json(input_path, logger)
    resultats = construire_labels(partants, logger)

    # Export
    output_dir.mkdir(parents=True, exist_ok=True)
    sauver_json(resultats, output_dir / "labels.json", logger)
    sauver_parquet(resultats, output_dir / "labels.parquet", logger)
    sauver_csv(resultats, output_dir / "labels.csv", logger)

    logger.info("Termine — %d labels", len(resultats))


if __name__ == "__main__":
    main()
