#!/usr/bin/env python3
"""
feature_builders.precomputed_entity_joiner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
22 features joined from pre-computed per-entity data (scripts 05/06/08).

These files are indexed by name (nom_cheval, jockey, entraineur, pere, mere)
and contain aggregated career-level stats.

Usage:
    python feature_builders/precomputed_entity_joiner.py
    python feature_builders/precomputed_entity_joiner.py --input output/02_liste_courses/partants_normalises.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "precomputed_entity_features")
_OUTPUT_BASE = os.path.join("output")

# ===========================================================================
# HELPERS
# ===========================================================================

def _normalize_name(name) -> str:
    if not name:
        return ""
    return str(name).strip().upper()

# ===========================================================================
# LOAD
# ===========================================================================

def _load_json_index(path: str, key: str, logger: logging.Logger) -> dict:
    """Load a JSON/JSONL file and build a lookup dict by key."""
    data = load_json_or_jsonl(path, logger)
    index = {}
    for rec in data:
        k = rec.get(key)
        if k:
            index[k] = rec
    if index:
        logger.info("  Index %s: %d entities", os.path.basename(path), len(index))
    return index

# ===========================================================================
# BUILDER
# ===========================================================================

def build_precomputed_entity_features(partants: list, logger: logging.Logger = None) -> list:
    """Join 22 pre-computed per-entity features from scripts 05, 06, 08."""
    if logger is None:
        logger = logging.getLogger(__name__)

    # Load all entity files
    cheval_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "05_historique_chevaux", "historique_chevaux.json"),
        "nom_cheval", logger,
    )
    jockey_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "06_historique_jockeys", "historique_jockeys.json"),
        "nom", logger,
    )
    entraineur_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "06_historique_jockeys", "historique_entraineurs.json"),
        "nom", logger,
    )
    pere_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "08_pedigree", "pedigree_peres.json"),
        "nom_pere", logger,
    )
    mere_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "08_pedigree", "pedigree_meres.json"),
        "nom_mere", logger,
    )

    # Normalize all keys for fuzzy matching
    cheval_norm = {_normalize_name(k): v for k, v in cheval_idx.items()}
    jockey_norm = {_normalize_name(k): v for k, v in jockey_idx.items()}
    entraineur_norm = {_normalize_name(k): v for k, v in entraineur_idx.items()}
    pere_norm = {_normalize_name(k): v for k, v in pere_idx.items()}
    mere_norm = {_normalize_name(k): v for k, v in mere_idx.items()}

    enriched = 0
    stats = {"cheval": 0, "jockey": 0, "entraineur": 0, "pere": 0, "mere": 0}
    results = []

    for idx, p in enumerate(partants):
        feat = {}

        # --- Historique cheval (script 05) ---
        nom = _normalize_name(p.get("nom_cheval"))
        cheval = cheval_norm.get(nom, {})
        if cheval:
            stats["cheval"] += 1

        feat["ent_cheval_nb_courses_total"] = cheval.get("nb_courses_total")
        feat["ent_cheval_gains_total"] = cheval.get("gains_total_euros")

        disciplines = cheval.get("disciplines")
        feat["ent_cheval_nb_disciplines"] = len(disciplines) if isinstance(disciplines, list) else None

        hippos = cheval.get("hippodromes")
        feat["ent_cheval_nb_hippodromes"] = len(hippos) if isinstance(hippos, list) else None

        distances = cheval.get("distances_courues")
        feat["ent_cheval_nb_distances"] = len(set(distances)) if isinstance(distances, list) else None

        # Anciennete: days since first race
        premiere = cheval.get("premiere_course_date")
        date_course = p.get("date_reunion_iso")
        if premiere and date_course:
            try:
                d1 = datetime.fromisoformat(str(premiere)[:10])
                d2 = datetime.fromisoformat(str(date_course)[:10])
                feat["ent_cheval_anciennete_jours"] = (d2 - d1).days
            except (ValueError, TypeError):
                feat["ent_cheval_anciennete_jours"] = None
        else:
            feat["ent_cheval_anciennete_jours"] = None

        # --- Historique jockey (script 06) ---
        jockey_name = _normalize_name(p.get("jockey_driver"))
        jockey = jockey_norm.get(jockey_name, {})
        if jockey:
            stats["jockey"] += 1

        feat["ent_jockey_nb_montes_total"] = jockey.get("nb_montes")
        feat["ent_jockey_taux_victoire_global"] = jockey.get("taux_victoire")
        feat["ent_jockey_taux_place_global"] = jockey.get("taux_place")
        feat["ent_jockey_nb_chevaux_montes"] = jockey.get("chevaux_montes")
        feat["ent_jockey_gains_total"] = jockey.get("gains_total_euros")

        # --- Historique entraineur (script 06) ---
        ent_name = _normalize_name(p.get("entraineur"))
        entraineur = entraineur_norm.get(ent_name, {})
        if entraineur:
            stats["entraineur"] += 1

        feat["ent_entraineur_nb_partants_total"] = entraineur.get("nb_montes")
        feat["ent_entraineur_taux_victoire_global"] = entraineur.get("taux_victoire")
        feat["ent_entraineur_taux_place_global"] = entraineur.get("taux_place")
        feat["ent_entraineur_nb_chevaux"] = entraineur.get("chevaux_montes")
        feat["ent_entraineur_gains_total"] = entraineur.get("gains_total_euros")

        # --- Pedigree pere (script 08) ---
        pere_name = _normalize_name(p.get("pere"))
        pere = pere_norm.get(pere_name, {})
        if pere:
            stats["pere"] += 1

        feat["ent_pere_nb_descendants"] = pere.get("nb_descendants_courses")
        feat["ent_pere_taux_victoire"] = pere.get("taux_victoire_descendants")
        pere_disc = pere.get("disciplines")
        feat["ent_pere_nb_disciplines"] = len(pere_disc) if isinstance(pere_disc, list) else None

        # --- Pedigree mere (script 08) ---
        mere_name = _normalize_name(p.get("mere"))
        mere = mere_norm.get(mere_name, {})
        if mere:
            stats["mere"] += 1

        feat["ent_mere_nb_descendants"] = mere.get("nb_descendants_courses")
        feat["ent_mere_taux_victoire"] = mere.get("taux_victoire_descendants")
        mere_disc = mere.get("disciplines")
        feat["ent_mere_nb_disciplines"] = len(mere_disc) if isinstance(mere_disc, list) else None

        if any(v is not None for v in feat.values()):
            enriched += 1

        p.update(feat)
        results.append(p)

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites", idx + 1, len(partants))

    n = len(partants)
    logger.info("Match rates: cheval=%d/%d, jockey=%d/%d, entraineur=%d/%d, pere=%d/%d, mere=%d/%d",
                stats["cheval"], n, stats["jockey"], n, stats["entraineur"], n,
                stats["pere"], n, stats["mere"], n)
    logger.info("Features precomputed_entity: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================



# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="22 pre-computed per-entity features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("precomputed_entity_joiner")
    logger.info("=" * 70)
    logger.info("precomputed_entity_joiner.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_precomputed_entity_features(partants, logger)

    out_path = os.path.join(args.output_dir, "precomputed_entity_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
