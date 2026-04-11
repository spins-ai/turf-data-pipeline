#!/usr/bin/env python3
"""
feature_builders.race_history_richness_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Measures the richness and completeness of a horse's racing history data.

Reads partants_master.jsonl in single-pass streaming mode and computes
8 per-partant features entirely from the current record's own fields
(no cross-record state, no temporal look-back).

Produces:
  - race_history_richness.jsonl  in output/race_history_richness/

Features per partant (8):
  - rhr_has_musique         : 1 if musique field is not empty / None
  - rhr_musique_length      : length of musique string (more history = more data)
  - rhr_has_odds            : 1 if cote_finale is not None and > 0
  - rhr_has_weight          : 1 if poids_porte_kg or poids_porte is not None
  - rhr_has_gains           : 1 if gains_carriere_euros > 0
  - rhr_data_completeness   : count of non-None key fields / total key fields (0-1)
  - rhr_has_pedigree        : 1 if nom_pere and nom_mere are both present
  - rhr_has_terrain         : 1 if etat_terrain is not empty

Key fields for completeness check:
  cote_finale, musique, gains_carriere_euros, nb_courses_carriere,
  nb_victoires_carriere, poids_porte_kg, nom_pere, nom_mere,
  etat_terrain, distance, age, sexe, jockey, entraineur

Usage:
    python feature_builders/race_history_richness_builder.py
    python feature_builders/race_history_richness_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/race_history_richness_builder.py --output-dir /path/to/output/
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

# Fallback candidates when the canonical path does not exist
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_history_richness")

# Key fields used for the data-completeness score
_KEY_FIELDS = [
    "cote_finale",
    "musique",
    "gains_carriere_euros",
    "nb_courses_carriere",
    "nb_victoires_carriere",
    "poids_porte_kg",
    "nom_pere",
    "nom_mere",
    "etat_terrain",
    "distance",
    "age",
    "sexe",
    "jockey",
    "entraineur",
]
_N_KEY_FIELDS = len(_KEY_FIELDS)

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _is_present(value: Any) -> bool:
    """Return True when a field value is considered populated (non-empty)."""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _safe_float(value: Any) -> Optional[float]:
    """Convert value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# FEATURE COMPUTATION (single record, no state)
# ===========================================================================


def _compute_features(rec: dict[str, Any]) -> dict[str, Any]:
    """Compute 8 race-history-richness features from a single partant record."""

    # --- rhr_has_musique ---
    musique = rec.get("musique")
    has_musique = 1 if (musique is not None and str(musique).strip() != "") else 0

    # --- rhr_musique_length ---
    musique_length = len(str(musique).strip()) if has_musique else 0

    # --- rhr_has_odds ---
    cote_finale = _safe_float(rec.get("cote_finale") or rec.get("rapport_final"))
    has_odds = 1 if (cote_finale is not None and cote_finale > 0) else 0

    # --- rhr_has_weight ---
    poids_kg = rec.get("poids_porte_kg")
    poids_raw = rec.get("poids_porte")
    has_weight = 1 if (_is_present(poids_kg) or _is_present(poids_raw)) else 0

    # --- rhr_has_gains ---
    gains = _safe_float(rec.get("gains_carriere_euros"))
    has_gains = 1 if (gains is not None and gains > 0) else 0

    # --- rhr_data_completeness ---
    present_count = sum(1 for field in _KEY_FIELDS if _is_present(rec.get(field)))
    data_completeness = round(present_count / _N_KEY_FIELDS, 6)

    # --- rhr_has_pedigree ---
    nom_pere = rec.get("nom_pere")
    nom_mere = rec.get("nom_mere")
    has_pedigree = 1 if (_is_present(nom_pere) and _is_present(nom_mere)) else 0

    # --- rhr_has_terrain ---
    etat_terrain = rec.get("etat_terrain")
    has_terrain = 1 if _is_present(etat_terrain) else 0

    return {
        "partant_uid": rec.get("partant_uid"),
        "rhr_has_musique": has_musique,
        "rhr_musique_length": musique_length,
        "rhr_has_odds": has_odds,
        "rhr_has_weight": has_weight,
        "rhr_has_gains": has_gains,
        "rhr_data_completeness": data_completeness,
        "rhr_has_pedigree": has_pedigree,
        "rhr_has_terrain": has_terrain,
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_race_history_richness(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Stream partants_master.jsonl and emit richness features for each record."""
    logger.info("=== Race History Richness Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Traite %d records...", n_read)

        results.append(_compute_features(rec))

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features en %.1fs",
        len(results),
        elapsed,
    )

    gc.collect()
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path from CLI argument or auto-detection."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in _INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features race_history_richness a partir de partants_master"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/...)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_history_richness_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_race_history_richness(input_path, logger)

    # Save
    out_path = output_dir / "race_history_richness.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total = len(results)
        logger.info("=== Fill rates ===")
        for key in feature_keys:
            filled = sum(1 for r in results if r.get(key) is not None)
            logger.info("  %s: %d/%d (%.1f%%)", key, filled, total, 100 * filled / total)

        # Completeness distribution
        scores = [r["rhr_data_completeness"] for r in results]
        avg_completeness = sum(scores) / total
        logger.info("  rhr_data_completeness moyenne: %.4f", avg_completeness)

        # Musique coverage
        musique_filled = sum(1 for r in results if r["rhr_has_musique"] == 1)
        logger.info(
            "  rhr_has_musique=1: %d/%d (%.1f%%)",
            musique_filled, total, 100 * musique_filled / total,
        )

        # Pedigree coverage
        pedigree_filled = sum(1 for r in results if r["rhr_has_pedigree"] == 1)
        logger.info(
            "  rhr_has_pedigree=1: %d/%d (%.1f%%)",
            pedigree_filled, total, 100 * pedigree_filled / total,
        )


if __name__ == "__main__":
    main()
