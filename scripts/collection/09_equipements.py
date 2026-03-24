#!/usr/bin/env python3
"""
09_equipements.py
=================
Reconstruit l'historique d'equipements (oeilleres, deferre) par cheval
a partir des partants normalises collectes par 02_liste_courses.py.

Aucun appel API : traitement 100% local.

Produit :
  - equipements_historique.json / .parquet / .csv

Usage :
    python3 09_equipements.py
    python3 09_equipements.py --input output/02_liste_courses/partants_normalises.json
"""

from __future__ import annotations

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Optional


# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path(__file__).resolve().parent / "../../output" / "02_liste_courses" / "partants_normalises.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "../../output" / "09_equipements"

from utils.logging_setup import setup_logging
from utils.output import sauver_json, sauver_csv, sauver_parquet


# ===========================================================================
# SAUVEGARDE
# ===========================================================================





# ===========================================================================
# TRAITEMENT
# ===========================================================================

def charger_partants(path: Path, logger: logging.Logger) -> list[dict]:
    logger.info("Chargement partants: %s", path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info("  %d partants charges", len(data))
    return data


def construire_historique_equipements(partants: list[dict], logger: logging.Logger) -> list[dict]:
    """
    Pour chaque cheval, trie ses courses par date et construit l'historique
    des changements d'equipement.
    """
    # Regrouper par nom_cheval
    par_cheval: dict[str, list[dict]] = {}
    for p in partants:
        nom = p.get("nom_cheval", "")
        if not nom:
            continue
        # Ignorer les non-partants
        statut = p.get("statut", "")
        if statut == "non_partant":
            continue
        par_cheval.setdefault(nom, []).append(p)

    logger.info("  %d chevaux uniques", len(par_cheval))

    resultats = []

    for nom_cheval, courses in par_cheval.items():
        # Trier par date
        courses.sort(key=lambda x: x.get("date_reunion_iso", ""))

        nb_avec_oeilleres = 0
        nb_sans_oeilleres = 0
        premiere_oeilleres_vue = False

        for i, p in enumerate(courses):
            oeilleres = p.get("oeilleres", "") or ""
            deferre = p.get("deferre", "") or ""
            date_iso = p.get("date_reunion_iso", "")
            course_uid = p.get("course_uid", "")
            partant_uid = p.get("partant_uid", "")

            # Valeurs precedentes
            if i > 0:
                oeilleres_prev = courses[i - 1].get("oeilleres", "") or ""
                deferre_prev = courses[i - 1].get("deferre", "") or ""
            else:
                oeilleres_prev = ""
                deferre_prev = ""

            # Changements
            oeilleres_change = (i > 0) and (oeilleres != oeilleres_prev)
            deferre_change = (i > 0) and (deferre != deferre_prev)

            # Oeilleres : avec = toute valeur non vide et != "sans"
            a_oeilleres = oeilleres not in ("", "sans")
            avait_oeilleres = oeilleres_prev not in ("", "sans")

            # Premiere oeilleres : premiere fois qu'on voit des oeilleres
            premiere_oeilleres = False
            if a_oeilleres and not premiere_oeilleres_vue:
                premiere_oeilleres = True
                premiere_oeilleres_vue = True

            # Retrait oeilleres : avait oeilleres, plus maintenant
            retrait_oeilleres = (i > 0) and avait_oeilleres and not a_oeilleres

            # Compteurs
            if a_oeilleres:
                nb_avec_oeilleres += 1
            else:
                nb_sans_oeilleres += 1

            record = {
                "nom_cheval": nom_cheval,
                "date_reunion_iso": date_iso,
                "course_uid": course_uid,
                "partant_uid": partant_uid,
                "oeilleres": oeilleres,
                "deferre": deferre,
                "oeilleres_prev": oeilleres_prev,
                "deferre_prev": deferre_prev,
                "oeilleres_change": oeilleres_change,
                "deferre_change": deferre_change,
                "premiere_oeilleres": premiere_oeilleres,
                "retrait_oeilleres": retrait_oeilleres,
                "nb_courses_avec_oeilleres": nb_avec_oeilleres,
                "nb_courses_sans_oeilleres": nb_sans_oeilleres,
            }
            resultats.append(record)

    logger.info("  %d enregistrements equipements generes", len(resultats))
    return resultats


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Historique equipements par cheval (local, sans API)"
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

    logger = setup_logging("09_equipements")
    logger.info("=" * 70)
    logger.info("09_equipements.py — Historique equipements")
    logger.info("=" * 70)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    partants = charger_partants(input_path, logger)
    resultats = construire_historique_equipements(partants, logger)

    # Export
    output_dir.mkdir(parents=True, exist_ok=True)
    sauver_json(resultats, output_dir / "equipements_historique.json", logger)
    sauver_parquet(resultats, output_dir / "equipements_historique.parquet", logger)
    sauver_csv(resultats, output_dir / "equipements_historique.csv", logger)

    logger.info("Termine — %d enregistrements", len(resultats))


if __name__ == "__main__":
    main()
