#!/usr/bin/env python3
"""
feature_builders.stacking_helper_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Helper features for the stacking / blending / meta-model modules.

These features help a meta-learner decide how to weight different base
models by encoding which data signals are actually present for each
partant.

Simple single-pass streaming -- no grouping, no history needed.

Temporal integrity: all features are derived from the partant's own
record (data available before the race), no future leakage.

Memory-optimised:
  - Single streaming pass, writes directly to disk
  - .tmp then atomic rename
  - gc.collect() every 500K records

Produces:
  - stacking_helper_features.jsonl

Features per partant (10):
  - stk_speed_data_present       : 1 if spd_speed_figure is not None
  - stk_market_data_present      : 1 if cote_finale is not None
  - stk_pedigree_data_present    : 1 if ped_has_pedigree
  - stk_history_length_bucket    : 0/1/2/3 based on race history depth
  - stk_field_size_impact        : nombre_partants / 16 (normalised)
  - stk_is_trot                  : 1 if discipline contains "trot"
  - stk_is_quinte_race           : 1 if cnd_cond_is_quinte
  - stk_weight_data_present      : 1 if poids_porte_kg is not None
  - stk_form_data_present        : 1 if seq_serie_places is not None and seq_nb_courses_historique > 0
  - stk_model_routing_code       : binary encoding of available data types

Usage:
    python feature_builders/stacking_helper_builder.py
    python feature_builders/stacking_helper_builder.py --input path/to/partants_master.jsonl
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

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/stacking_helper")
OUTPUT_FILENAME = "stacking_helper_features.jsonl"

_LOG_EVERY = 500_000
_GC_EVERY = 500_000


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


def _safe_int(val) -> Optional[int]:
    """Try to convert val to int, return None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    """Try to convert val to float, return None on failure."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _history_bucket(nb_courses: Optional[int]) -> int:
    """0 (none), 1 (1-5), 2 (5-20), 3 (20+)."""
    if nb_courses is None or nb_courses <= 0:
        return 0
    if nb_courses <= 5:
        return 1
    if nb_courses <= 20:
        return 2
    return 3


# ===========================================================================
# FEATURE EXTRACTION
# ===========================================================================


def _compute_features(rec: dict) -> dict[str, Any]:
    """Compute the 10 stacking helper features for a single record."""

    # --- Data presence flags ---
    speed_present = 1 if rec.get("spd_speed_figure") is not None else 0
    market_present = 1 if rec.get("cote_finale") is not None else 0
    pedigree_present = 1 if rec.get("ped_has_pedigree") else 0

    # Weight data
    poids = _safe_float(rec.get("poids_porte_kg"))
    weight_present = 1 if poids is not None else 0

    # Form data: seq_serie_places not None AND seq_nb_courses_historique > 0
    serie_places = rec.get("seq_serie_places")
    nb_courses_hist = _safe_int(rec.get("seq_nb_courses_historique"))
    form_present = 1 if (serie_places is not None and nb_courses_hist is not None and nb_courses_hist > 0) else 0

    # --- History length bucket ---
    history_bucket = _history_bucket(nb_courses_hist)

    # --- Field size impact ---
    nb_partants = _safe_int(rec.get("nombre_partants"))
    if nb_partants is not None and nb_partants > 0:
        field_impact = round(nb_partants / 16.0, 4)
    else:
        field_impact = None

    # --- Discipline ---
    discipline = rec.get("discipline") or rec.get("type_course") or ""
    if isinstance(discipline, str):
        is_trot = 1 if "trot" in discipline.lower() else 0
    else:
        is_trot = 0

    # --- Quinte ---
    is_quinte = 1 if rec.get("cnd_cond_is_quinte") else 0

    # --- Model routing code (binary encoding) ---
    routing = 0
    if speed_present:
        routing += 1
    if market_present:
        routing += 2
    if pedigree_present:
        routing += 4
    if history_bucket > 0:
        routing += 8
    if weight_present:
        routing += 16

    return {
        "partant_uid": rec.get("partant_uid"),
        "course_uid": rec.get("course_uid"),
        "date_reunion_iso": rec.get("date_reunion_iso"),
        "stk_speed_data_present": speed_present,
        "stk_market_data_present": market_present,
        "stk_pedigree_data_present": pedigree_present,
        "stk_history_length_bucket": history_bucket,
        "stk_field_size_impact": field_impact,
        "stk_is_trot": is_trot,
        "stk_is_quinte_race": is_quinte,
        "stk_weight_data_present": weight_present,
        "stk_form_data_present": form_present,
        "stk_model_routing_code": routing,
    }


# ===========================================================================
# FEATURE NAMES (for fill rate tracking)
# ===========================================================================

_FEATURE_KEYS = [
    "stk_speed_data_present",
    "stk_market_data_present",
    "stk_pedigree_data_present",
    "stk_history_length_bucket",
    "stk_field_size_impact",
    "stk_is_trot",
    "stk_is_quinte_race",
    "stk_weight_data_present",
    "stk_form_data_present",
    "stk_model_routing_code",
]


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_stacking_helper_features(input_path: Path, output_path: Path, logger) -> int:
    """Build stacking helper features via single-pass streaming."""
    logger.info("=== Stacking Helper Builder ===")
    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", output_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    fill_counts = {k: 0 for k in _FEATURE_KEYS}

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            features = _compute_features(rec)

            # Track fill rates
            for k in _FEATURE_KEYS:
                if features.get(k) is not None:
                    fill_counts[k] += 1

            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

            if n_written % _LOG_EVERY == 0:
                logger.info("  Ecrit %d records...", n_written)

            if n_written % _GC_EVERY == 0:
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Stacking helper build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0.0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features stacking helper a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/stacking_helper/)",
    )
    args = parser.parse_args()

    logger = setup_logging("stacking_helper_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_stacking_helper_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
