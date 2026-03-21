#!/usr/bin/env python3
"""
feature_builders.profil_cheval_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
24 features from horse profile (age, sex, breed, career stats, engagement).

Usage:
    python feature_builders/profil_cheval_features.py
    python feature_builders/profil_cheval_features.py --input output/02_liste_courses/partants_normalises.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "profil_cheval_features")

SEXE_MAP = {
    "MALES": 0, "MALE": 0, "M": 0, "H": 0,
    "FEMELLES": 1, "FEMELLE": 1, "F": 1,
    "HONGRES": 2, "HONGRE": 2,
}

RACE_MAP = {
    "PUR-SANG": 0, "PS": 0, "THOROUGHBRED": 0,
    "AQPS": 1,
    "TROTTEUR": 2, "TROTTEUR FRANCAIS": 2, "TF": 2,
}

ROBE_MAP = {
    "BAI": 1, "B": 1,
    "BAI BRUN": 2, "BB": 2, "BAI FONCE": 2, "BBF": 2,
    "ALEZAN": 3, "AL": 3,
    "GRIS": 4, "GR": 4,
    "NOIR": 5, "N": 5,
    "BAI CLAIR": 6, "BC": 6,
    "ROUAN": 7,
    "AUBERE": 8,
}

BREED_MAP = {
    "PUR-SANG": 0, "PS": 0, "THOROUGHBRED": 0,
    "AQPS": 1,
    "TROTTEUR FRANCAIS": 2, "TF": 2, "TROTTEUR": 2,
    "ANGLO-ARABE": 3, "AA": 3,
    "ARABE": 4, "AR": 4,
    "SELLE FRANCAIS": 5, "SF": 5,
    "STANDARDBRED": 6,
}

# ===========================================================================
# LOAD
# ===========================================================================

def load_jsonl(path: str, logger: logging.Logger) -> list:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    logger.info("Charge %d enregistrements depuis %s", len(records), path)
    return records


def load_json_or_jsonl(path: str, logger: logging.Logger) -> list:
    if path.endswith(".jsonl"):
        return load_jsonl(path, logger)
    jsonl_path = path.replace(".json", ".jsonl")
    if os.path.exists(jsonl_path):
        return load_jsonl(jsonl_path, logger)
    if os.path.exists(path):
        logger.info("Chargement JSON: %s", path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("  %d entrees chargees", len(data))
        return data
    logger.error("Fichier introuvable: %s", path)
    sys.exit(1)

# ===========================================================================
# BUILDER
# ===========================================================================

def build_profil_cheval_features(partants: list, logger: logging.Logger = None) -> list:
    """Build 24 horse profile features."""
    if logger is None:
        logger = logging.getLogger(__name__)

    # Group by course for relative place_corde
    course_nb_partants: dict[str, int] = defaultdict(int)
    for p in partants:
        cuid = p.get("course_uid")
        if cuid:
            course_nb_partants[cuid] += 1

    enriched = 0
    results = []

    for idx, p in enumerate(partants):
        feat = {}

        # Age
        age = p.get("age")
        feat["profil_age"] = age
        if age is not None:
            if age <= 2:
                feat["profil_age_category"] = 2
            elif age == 3:
                feat["profil_age_category"] = 3
            elif age <= 5:
                feat["profil_age_category"] = 4
            else:
                feat["profil_age_category"] = 5
        else:
            feat["profil_age_category"] = None

        # Sex encoding
        sexe = (p.get("sexe") or "").upper().strip()
        feat["profil_sexe_code"] = SEXE_MAP.get(sexe, 0)
        feat["profil_is_male"] = 1 if SEXE_MAP.get(sexe, 0) == 0 and sexe else 0
        feat["profil_is_female"] = 1 if SEXE_MAP.get(sexe) == 1 else 0
        feat["profil_is_hongre"] = 1 if SEXE_MAP.get(sexe) == 2 else 0

        # Race / breed encoding
        race = (p.get("race") or "").upper().strip()
        feat["profil_race_code"] = RACE_MAP.get(race, 3)
        feat["profil_race_breed_encoded"] = BREED_MAP.get(race, 99)

        # Robe (coat color) encoding
        robe = (p.get("robe") or "").upper().strip()
        feat["profil_robe_encoded"] = ROBE_MAP.get(robe, 0)

        # Gains (log-transformed)
        gains_c = p.get("gains_carriere_euros")
        gains_a = p.get("gains_annee_euros")
        feat["profil_gains_carriere_log"] = round(math.log1p(gains_c), 2) if gains_c is not None and gains_c >= 0 else None
        feat["profil_gains_annee_log"] = round(math.log1p(gains_a), 2) if gains_a is not None and gains_a >= 0 else None

        # Career stats
        nb_courses = p.get("nb_courses_carriere")
        feat["profil_nb_courses_carriere"] = nb_courses
        feat["profil_is_inedit"] = 1 if p.get("is_inedit") else 0

        # Career length category
        if nb_courses is not None:
            if nb_courses <= 2:
                feat["profil_carriere_longueur"] = 0
            elif nb_courses <= 10:
                feat["profil_carriere_longueur"] = 1
            elif nb_courses <= 30:
                feat["profil_carriere_longueur"] = 2
            else:
                feat["profil_carriere_longueur"] = 3
        else:
            feat["profil_carriere_longueur"] = None

        # Career win rate and place rate
        nb_vic = p.get("nb_victoires_carriere")
        nb_place = p.get("nb_places_carriere")
        if nb_courses is not None and nb_courses > 0:
            feat["profil_taux_victoire_carriere"] = round((nb_vic or 0) / nb_courses, 3)
            feat["profil_taux_place_carriere"] = round((nb_place or 0) / nb_courses, 3)
            if gains_c is not None and gains_c >= 0:
                feat["profil_gains_par_course"] = round(gains_c / nb_courses, 2)
            else:
                feat["profil_gains_par_course"] = None
        else:
            feat["profil_taux_victoire_carriere"] = None
            feat["profil_taux_place_carriere"] = None
            feat["profil_gains_par_course"] = None

        # Place a la corde
        cuid = p.get("course_uid")
        corde = p.get("place_corde")
        feat["profil_place_corde"] = corde
        nb = course_nb_partants.get(cuid, 0)
        if corde is not None and nb > 0:
            feat["profil_place_corde_relative"] = round(corde / nb, 3)
        else:
            feat["profil_place_corde_relative"] = None

        # Other
        eng = p.get("engagement")
        feat["profil_engagement"] = eng if eng is not None else 0
        feat["profil_jument_pleine"] = 1 if p.get("jument_pleine") else 0

        if any(v is not None for k, v in feat.items() if k.startswith("profil_")):
            enriched += 1

        p.update(feat)
        results.append(p)

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(partants), enriched)

    logger.info("Features profil_cheval: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="24 horse profile features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("profil_cheval_features")
    logger.info("=" * 70)
    logger.info("profil_cheval_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_profil_cheval_features(partants, logger)

    out_path = os.path.join(args.output_dir, "profil_cheval_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
