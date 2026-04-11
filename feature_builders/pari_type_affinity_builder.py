#!/usr/bin/env python3
"""
feature_builders.pari_type_affinity_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pari (bet) type affinity features -- which bet types favor this kind of
horse/race configuration.

Reads partants_master.jsonl in a single streaming pass, derives all features
from existing columns (no temporal history required).

Temporal integrity: all features are computed from pre-race data (career stats,
odds, field size) -- no future leakage.

Produces:
  - pari_type_affinity.jsonl   in builder_outputs/pari_type_affinity/

Features per partant (8):
  - pta_simple_gagnant_value  : cote * (nb_victoires / max(nb_courses, 1)) -- EV for simple gagnant
  - pta_simple_place_value    : (cote / 3) * (nb_places / max(nb_courses, 1)) -- rough EV for simple place
  - pta_is_tierce_candidate   : 1 if place_rate > 30% and nombre_partants >= 8
  - pta_is_quinte_candidate   : 1 if place_rate > 25% and nombre_partants >= 12
  - pta_couple_value          : (place_rate * 0.7) * cote -- estimated couple bet value
  - pta_is_banker             : 1 if place_rate > 50% and nb_courses > 10
  - pta_is_surprise_candidate : 1 if cote > 10 and place_rate > 20%
  - pta_optimal_bet_type      : categorical 0=simple_gagnant, 1=simple_place, 2=exotic, 3=avoid

Usage:
    python feature_builders/pari_type_affinity_builder.py
    python feature_builders/pari_type_affinity_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pari_type_affinity")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v and v > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _compute_features(rec: dict) -> dict[str, Any]:
    """Compute all 8 pari type affinity features for a single partant."""
    uid = rec.get("partant_uid")
    cote = _safe_float(rec.get("cote_finale"))
    nb_partants = _safe_int(rec.get("nombre_partants"))
    nb_victoires = _safe_int(rec.get("nb_victoires_carriere"))
    nb_courses = _safe_int(rec.get("nb_courses_carriere"))
    nb_places = _safe_int(rec.get("nb_places_carriere"))

    feats: dict[str, Any] = {
        "partant_uid": uid,
        "pta_simple_gagnant_value": None,
        "pta_simple_place_value": None,
        "pta_is_tierce_candidate": None,
        "pta_is_quinte_candidate": None,
        "pta_couple_value": None,
        "pta_is_banker": None,
        "pta_is_surprise_candidate": None,
        "pta_optimal_bet_type": None,
    }

    # Derived rates
    courses_denom = max(nb_courses, 1) if nb_courses is not None else None
    win_rate: Optional[float] = None
    place_rate: Optional[float] = None

    if nb_victoires is not None and courses_denom is not None:
        win_rate = nb_victoires / courses_denom

    if nb_places is not None and courses_denom is not None:
        place_rate = nb_places / courses_denom

    # --- Feature 1: pta_simple_gagnant_value ---
    if cote is not None and win_rate is not None:
        feats["pta_simple_gagnant_value"] = round(cote * win_rate, 4)

    # --- Feature 2: pta_simple_place_value ---
    if cote is not None and place_rate is not None:
        feats["pta_simple_place_value"] = round((cote / 3.0) * place_rate, 4)

    # --- Feature 3: pta_is_tierce_candidate ---
    if place_rate is not None and nb_partants is not None:
        feats["pta_is_tierce_candidate"] = (
            1 if place_rate > 0.30 and nb_partants >= 8 else 0
        )

    # --- Feature 4: pta_is_quinte_candidate ---
    if place_rate is not None and nb_partants is not None:
        feats["pta_is_quinte_candidate"] = (
            1 if place_rate > 0.25 and nb_partants >= 12 else 0
        )

    # --- Feature 5: pta_couple_value ---
    if place_rate is not None and cote is not None:
        feats["pta_couple_value"] = round(place_rate * 0.7 * cote, 4)

    # --- Feature 6: pta_is_banker ---
    if place_rate is not None and nb_courses is not None:
        feats["pta_is_banker"] = (
            1 if place_rate > 0.50 and nb_courses > 10 else 0
        )

    # --- Feature 7: pta_is_surprise_candidate ---
    if cote is not None and place_rate is not None:
        feats["pta_is_surprise_candidate"] = (
            1 if cote > 10.0 and place_rate > 0.20 else 0
        )

    # --- Feature 8: pta_optimal_bet_type ---
    #   0 = simple_gagnant (favorite with good win rate)
    #   1 = simple_place (consistent placer)
    #   2 = exotic (longshot with some form -> tierce/quinte value)
    #   3 = avoid (poor form)
    if cote is not None and win_rate is not None and place_rate is not None:
        if cote <= 5.0 and win_rate > 0.15:
            feats["pta_optimal_bet_type"] = 0  # simple_gagnant
        elif place_rate > 0.35:
            feats["pta_optimal_bet_type"] = 1  # simple_place
        elif cote > 8.0 and place_rate > 0.15:
            feats["pta_optimal_bet_type"] = 2  # exotic
        else:
            feats["pta_optimal_bet_type"] = 3  # avoid

    return feats


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_pari_type_affinity_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build pari type affinity features from partants_master.jsonl."""
    logger.info("=== Pari Type Affinity Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        feats = _compute_features(rec)
        results.append(feats)

    elapsed = time.time() - t0
    logger.info(
        "Pari type affinity build termine: %d features en %.1fs",
        len(results), elapsed,
    )
    return results


# ===========================================================================
# SAVE (atomic .tmp -> rename)
# ===========================================================================


def _save_jsonl(records: list[dict], filepath: Path, logger) -> None:
    """Write JSONL with atomic .tmp -> rename pattern."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    tmp = filepath.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    tmp.replace(filepath)
    logger.info("Sauve JSONL: %s (%d records)", filepath, len(records))


# ===========================================================================
# CLI & MAIN
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features pari type affinity a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("pari_type_affinity_builder")
    logger.info("=" * 70)
    logger.info("pari_type_affinity_builder.py -- Pari Type Affinity Features")
    logger.info("=" * 70)

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    results = build_pari_type_affinity_features(input_path, logger)

    # Free intermediate memory
    gc.collect()

    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "pari_type_affinity.jsonl"
    _save_jsonl(results, out_path, logger)

    # Free results after save
    gc.collect()

    # Fill rates
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info("  %s: %d/%d (%.1f%%)", k, filled, total_count, 100 * filled / total_count)


if __name__ == "__main__":
    main()
