#!/usr/bin/env python3
"""
feature_builders.equipement_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
16 features from equipment (oeilleres, deferre) and their changes.

Temporal integrity: for each partant at date D, only races with date < D
are used for historical equipment comparisons (no future leakage).

Usage:
    python feature_builders/equipement_features.py
    python feature_builders/equipement_features.py --input output/02_liste_courses/partants_normalises.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "equipement_features")
LOG_DIR = os.path.join("logs")

_OEILLERES_VALUES = {
    "SANS": 0, None: 0, "": 0,
    "AVEC": 1, "AUSTRALIENNES": 2,
}

_OEILLERES_TYPE_VALUES = {
    "AUSTRALIENNES": 1, "CLASSIQUES": 2, "AVEC": 2,
    "AMERICAINES": 3, "SANS": 0, None: 0, "": 0,
}

_DEFERRE_VALUES = {
    "SANS": 0, None: 0, "": 0,
    "DEFERRE_ANTERIEURS": 1, "DEFERRE_POSTERIEURS": 2,
    "DEFERRE_4_PIEDS": 3,
}

_DEFERRE_TYPE_VALUES = {
    "SANS": 0, None: 0, "": 0,
    "DEFERRE_ANTERIEURS": 1, "DEFERRE_POSTERIEURS": 2,
    "DEFERRE_4_PIEDS": 3,
    "DEFERRE_ANTERIEURS_GAUCHE": 4, "DEFERRE_ANTERIEURS_DROIT": 5,
    "DEFERRE_POSTERIEURS_GAUCHE": 6, "DEFERRE_POSTERIEURS_DROIT": 7,
}

# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("equipement_features")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    os.makedirs(LOG_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(LOG_DIR, "equipement_features.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

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

def build_equipement_features(partants: list, logger: logging.Logger = None) -> list:
    """Build 16 equipment features with point-in-time safety."""
    if logger is None:
        logger = logging.getLogger(__name__)

    # Sort chronologically
    sorted_p = sorted(
        partants,
        key=lambda p: (
            str(p.get("date_reunion_iso", "") or ""),
            str(p.get("course_uid", "") or ""),
            p.get("num_pmu", 0) or 0,
        ),
    )

    # Horse history accumulator: list of {date, oeil_code, def_code}
    horse_history: dict[str, list[dict]] = defaultdict(list)
    enriched = 0
    results = []

    for idx, p in enumerate(sorted_p):
        cheval = (p.get("nom_cheval") or "").upper().strip()
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]

        oeilleres = p.get("oeilleres")
        deferre = p.get("deferre")

        oeil_code = _OEILLERES_VALUES.get(oeilleres, 0)
        def_code = _DEFERRE_VALUES.get(deferre, 0)

        feat = {}

        # Current equipment
        feat["equip_oeilleres_code"] = oeil_code
        feat["equip_has_oeilleres"] = 1 if oeil_code > 0 else 0
        feat["equip_oeilleres_type"] = _OEILLERES_TYPE_VALUES.get(oeilleres, 0)
        feat["equip_deferre_code"] = def_code
        feat["equip_has_deferre"] = 1 if def_code > 0 else 0
        feat["equip_deferre_type"] = _DEFERRE_TYPE_VALUES.get(deferre, 0)
        feat["equip_poids_monte_change"] = 1 if p.get("poids_monte_change") else 0

        # Historical comparisons (point-in-time)
        if cheval:
            past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso]

            if past:
                enriched += 1
                prev = past[-1]
                prev_oeil = prev["oeil_code"]
                prev_def = prev["def_code"]

                feat["equip_oeilleres_change"] = 1 if oeil_code != prev_oeil else 0
                feat["equip_deferre_change"] = 1 if def_code != prev_def else 0

                # First time with oeilleres
                prior_oeil_codes = [r["oeil_code"] for r in past]
                prior_def_codes = [r["def_code"] for r in past]

                feat["equip_premier_oeilleres"] = 1 if (oeil_code > 0 and all(v == 0 for v in prior_oeil_codes)) else 0
                feat["equip_premier_deferre"] = 1 if (def_code > 0 and all(v == 0 for v in prior_def_codes)) else 0
                feat["equip_first_time_oeilleres"] = feat["equip_premier_oeilleres"]
                feat["equip_nb_courses_avec_oeilleres"] = sum(1 for v in prior_oeil_codes if v > 0)

                # Count equipment changes in last 5 races
                last_5 = past[-5:]
                nb_changes = 0
                for i_prev in range(1, len(last_5)):
                    if (last_5[i_prev]["oeil_code"] != last_5[i_prev - 1]["oeil_code"] or
                            last_5[i_prev]["def_code"] != last_5[i_prev - 1]["def_code"]):
                        nb_changes += 1
                feat["equip_nb_equipement_changes_5"] = nb_changes
            else:
                feat["equip_oeilleres_change"] = None
                feat["equip_deferre_change"] = None
                feat["equip_premier_oeilleres"] = None
                feat["equip_premier_deferre"] = None
                feat["equip_first_time_oeilleres"] = None
                feat["equip_nb_courses_avec_oeilleres"] = None
                feat["equip_nb_equipement_changes_5"] = None
        else:
            feat["equip_oeilleres_change"] = None
            feat["equip_deferre_change"] = None
            feat["equip_premier_oeilleres"] = None
            feat["equip_premier_deferre"] = None
            feat["equip_first_time_oeilleres"] = None
            feat["equip_nb_courses_avec_oeilleres"] = None
            feat["equip_nb_equipement_changes_5"] = None

        p.update(feat)
        results.append(p)

        # Update history
        if cheval:
            horse_history[cheval].append({
                "date": date_iso,
                "oeil_code": oeil_code,
                "def_code": def_code,
            })

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(sorted_p), enriched)

    logger.info("Features equipement: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="16 equipment change features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("equipement_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_equipement_features(partants, logger)

    out_path = os.path.join(args.output_dir, "equipement_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
