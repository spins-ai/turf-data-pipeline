#!/usr/bin/env python3
"""
feature_builders.interaction_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
10 cross-feature interaction terms.

These multiply or combine features from different builders to capture
non-linear relationships. Operates on an ALREADY-MERGED feature matrix
(after all other builders have been merged).

Usage:
    python feature_builders/interaction_features.py
    python feature_builders/interaction_features.py --input output/merged_features/merged_features.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from typing import Optional

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "merged_features", "merged_features.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "interaction_features")
LOG_DIR = os.path.join("logs")

# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("interaction_features")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    os.makedirs(LOG_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(LOG_DIR, "interaction_features.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

# ===========================================================================
# HELPERS
# ===========================================================================

def _get_float(row: dict, key: str) -> Optional[float]:
    val = row.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _multiply(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return round(a * b, 6)

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

def build_interaction_features(partants: list, logger: logging.Logger) -> list:
    """Build 10 interaction features from merged feature matrix."""

    enriched = 0
    results = []

    for idx, row in enumerate(partants):
        # Gather source features (try multiple column names)
        forme = (
            _get_float(row, "forme_victoire_5")
            or _get_float(row, "musique_taux_victoire")
            or _get_float(row, "taux_victoire_carriere")
        )
        proba = _get_float(row, "proba_implicite") or _get_float(row, "proba_normalisee")
        age = _get_float(row, "profil_age") or _get_float(row, "age")
        distance = _get_float(row, "distance")
        poids = _get_float(row, "poids_porte_kg")
        jockey_taux = (
            _get_float(row, "jockey_taux_victoire_90j")
            or _get_float(row, "jockey_taux_victoire_365j")
        )
        cheval_taux = (
            _get_float(row, "forme_victoire_5")
            or _get_float(row, "taux_victoire_carriere")
        )
        affin_terrain = (
            _get_float(row, "affin_disc_taux_victoire")
            or _get_float(row, "affin_hippo_taux_victoire")
        )
        rang_cote_pct = _get_float(row, "rang_cote_pct") or _get_float(row, "rang_cote")
        nb_partants = _get_float(row, "nb_partants")
        allocation_rel = (
            _get_float(row, "allocation_relative")
            or _get_float(row, "allocation_diff_vs_last")
        )
        jours_repos = _get_float(row, "jours_depuis_derniere")
        nb_courses = (
            _get_float(row, "profil_nb_courses_carriere")
            or _get_float(row, "nb_courses_avant")
        )
        is_favori = _get_float(row, "is_favori")

        # Normalize
        dist_norm = distance / 1000.0 if distance is not None else None
        rest_norm = math.log1p(jours_repos) if jours_repos is not None and jours_repos >= 0 else None
        exp_norm = math.log1p(nb_courses) if nb_courses is not None and nb_courses >= 0 else None

        feat = {
            "forme_x_cote": _multiply(forme, proba),
            "age_x_distance": _multiply(age, dist_norm),
            "poids_x_distance": _multiply(poids, dist_norm),
            "jockey_taux_x_cheval_taux": _multiply(jockey_taux, cheval_taux),
            "forme_x_terrain": _multiply(forme, affin_terrain),
            "cote_x_nb_partants": _multiply(rang_cote_pct, nb_partants),
            "allocation_x_forme": _multiply(allocation_rel, forme),
            "rest_x_forme": _multiply(rest_norm, forme),
            "age_x_nb_courses": _multiply(age, exp_norm),
            "is_favori_x_forme": _multiply(is_favori, forme),
        }

        if any(v is not None for v in feat.values()):
            enriched += 1

        row.update(feat)
        results.append(row)

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(partants), enriched)

    logger.info("Features interaction: %d/%d enrichis (%.1f%%)",
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
    parser = argparse.ArgumentParser(description="10 cross-feature interaction terms")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Merged features JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("interaction_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_interaction_features(partants, logger)

    out_path = os.path.join(args.output_dir, "interaction_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
