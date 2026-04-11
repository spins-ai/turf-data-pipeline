#!/usr/bin/env python3
"""
feature_builders.discipline_specific_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Discipline-specific features -- trot vs galop have very different dynamics.
This builder creates features that capture discipline-specific patterns.

Simple single-pass streaming over partants_master.jsonl.

Produces:
  - discipline_specific_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/discipline_specific/

Features per partant (10):
  - dsp_is_trot_attele           : 1 if trot attele
  - dsp_is_trot_monte            : 1 if trot monte
  - dsp_is_galop_plat            : 1 if galop plat
  - dsp_is_galop_obstacle        : 1 if galop obstacle (haies, steeple, cross)
  - dsp_discipline_x_distance    : discipline_code * distance / 3000 (interaction)
  - dsp_discipline_x_age         : discipline_code * age (different age curves per discipline)
  - dsp_allure_encoded           : attele=0, monte=1, galop=2
  - dsp_is_autostart             : 1 if "autostart" in conditions text
  - dsp_has_weight_info          : 1 if poids_porte_kg not null (galop indicator)
  - dsp_discipline_distance_bucket : unique encoding of (discipline, distance_category)

Usage:
    python feature_builders/discipline_specific_builder.py
    python feature_builders/discipline_specific_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PATH = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/discipline_specific")
OUTPUT_FILE = OUTPUT_DIR / "discipline_specific_features.jsonl"

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# ---------------------------------------------------------------------------
# Discipline classification
# ---------------------------------------------------------------------------

# discipline field values observed: trot_attele, trot_monte, plat, haie, steeple, cross_country
_TROT_ATTELE = {"trot_attele"}
_TROT_MONTE = {"trot_monte"}
_GALOP_PLAT = {"plat"}
_GALOP_OBSTACLE = {"haie", "haies", "steeple", "cross_country", "cross", "obstacle"}

# Numeric codes for discipline interaction features
_DISCIPLINE_CODE = {
    "trot_attele": 1,
    "trot_monte": 2,
    "plat": 3,
    "haie": 4,
    "haies": 4,
    "steeple": 5,
    "cross_country": 6,
    "cross": 6,
    "obstacle": 5,
}

# Allure encoding: attele=0, monte=1, galop=2
_ALLURE_CODE = {
    "trot_attele": 0,
    "trot_monte": 1,
    "plat": 2,
    "haie": 2,
    "haies": 2,
    "steeple": 2,
    "cross_country": 2,
    "cross": 2,
    "obstacle": 2,
}

# Distance buckets for discipline_distance_bucket encoding
_DISTANCE_BUCKETS = {
    "sprint": 0,      # < 1400m
    "mile": 1,         # 1400-1799m
    "inter": 2,        # 1800-2199m
    "classique": 3,    # 2200-2599m
    "long": 4,         # 2600-3199m
    "marathon": 5,     # >= 3200m
}


def _distance_category(distance_m: Optional[int]) -> Optional[str]:
    """Classify distance into a bucket."""
    if distance_m is None or distance_m <= 0:
        return None
    if distance_m < 1400:
        return "sprint"
    elif distance_m < 1800:
        return "mile"
    elif distance_m < 2200:
        return "inter"
    elif distance_m < 2600:
        return "classique"
    elif distance_m < 3200:
        return "long"
    else:
        return "marathon"


def _discipline_distance_bucket(discipline: str, dist_cat: Optional[str]) -> Optional[int]:
    """Unique encoding of (discipline, distance_category) for model routing.

    Returns discipline_code * 10 + distance_bucket, giving a unique int
    per (discipline, distance_category) pair.
    """
    d_code = _DISCIPLINE_CODE.get(discipline)
    if d_code is None or dist_cat is None:
        return None
    b_code = _DISTANCE_BUCKETS.get(dist_cat)
    if b_code is None:
        return None
    return d_code * 10 + b_code


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
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _compute_features(rec: dict) -> dict:
    """Compute all 10 discipline-specific features for a single record."""
    discipline_raw = rec.get("discipline") or rec.get("type_course") or ""
    discipline = discipline_raw.strip().lower()

    distance = _safe_int(rec.get("distance"))
    age = _safe_int(rec.get("age"))
    poids = _safe_float(rec.get("poids_porte_kg"))
    conditions = (rec.get("cnd_conditions_texte_original") or "").lower()

    # --- Binary discipline flags ---
    dsp_is_trot_attele = 1 if discipline in _TROT_ATTELE else 0
    dsp_is_trot_monte = 1 if discipline in _TROT_MONTE else 0
    dsp_is_galop_plat = 1 if discipline in _GALOP_PLAT else 0
    dsp_is_galop_obstacle = 1 if discipline in _GALOP_OBSTACLE else 0

    # --- Interaction: discipline_code * distance / 3000 ---
    d_code = _DISCIPLINE_CODE.get(discipline)
    if d_code is not None and distance is not None and distance > 0:
        dsp_discipline_x_distance = round(d_code * distance / 3000, 4)
    else:
        dsp_discipline_x_distance = None

    # --- Interaction: discipline_code * age ---
    if d_code is not None and age is not None and age > 0:
        dsp_discipline_x_age = d_code * age
    else:
        dsp_discipline_x_age = None

    # --- Allure encoding ---
    dsp_allure_encoded = _ALLURE_CODE.get(discipline)

    # --- Autostart detection ---
    dsp_is_autostart = 1 if "autostart" in conditions else 0

    # --- Weight info presence (galop indicator) ---
    dsp_has_weight_info = 1 if poids is not None else 0

    # --- Discipline x distance bucket ---
    dist_cat = _distance_category(distance)
    dsp_discipline_distance_bucket = _discipline_distance_bucket(discipline, dist_cat)

    return {
        "dsp_is_trot_attele": dsp_is_trot_attele,
        "dsp_is_trot_monte": dsp_is_trot_monte,
        "dsp_is_galop_plat": dsp_is_galop_plat,
        "dsp_is_galop_obstacle": dsp_is_galop_obstacle,
        "dsp_discipline_x_distance": dsp_discipline_x_distance,
        "dsp_discipline_x_age": dsp_discipline_x_age,
        "dsp_allure_encoded": dsp_allure_encoded,
        "dsp_is_autostart": dsp_is_autostart,
        "dsp_has_weight_info": dsp_has_weight_info,
        "dsp_discipline_distance_bucket": dsp_discipline_distance_bucket,
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_discipline_features(input_path: Path, output_path: Path, logger) -> int:
    """Build discipline-specific features in a single streaming pass.

    Returns the total number of feature records written.
    """
    logger.info("=== Discipline-Specific Builder ===")
    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", output_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    feature_names = [
        "dsp_is_trot_attele",
        "dsp_is_trot_monte",
        "dsp_is_galop_plat",
        "dsp_is_galop_obstacle",
        "dsp_discipline_x_distance",
        "dsp_discipline_x_age",
        "dsp_allure_encoded",
        "dsp_is_autostart",
        "dsp_has_weight_info",
        "dsp_discipline_distance_bucket",
    ]
    fill_counts = {name: 0 for name in feature_names}

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            # Extract identifiers
            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid")
            date_reunion_iso = rec.get("date_reunion_iso")

            if not partant_uid:
                continue

            # Compute features
            feats = _compute_features(rec)

            # Build output record: identifiers + features
            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_reunion_iso,
            }
            out.update(feats)

            # Track fill rates
            for name in feature_names:
                if feats[name] is not None:
                    fill_counts[name] += 1

            fout.write(json.dumps(out, ensure_ascii=False, default=str) + "\n")
            n_written += 1

            if n_written % _LOG_EVERY == 0:
                logger.info("  Ecrit %d records...", n_written)

            if n_written % _GC_EVERY == 0:
                gc.collect()

    # Atomic replace
    if tmp_out.exists():
        tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features ecrites en %.1fs",
        n_written, elapsed,
    )

    # Fill rate summary
    logger.info("=== Fill rates ===")
    for name in feature_names:
        cnt = fill_counts[name]
        pct = 100 * cnt / n_written if n_written else 0
        logger.info("  %-40s %d / %d  (%.1f%%)", name, cnt, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PATH.exists():
        return INPUT_PATH
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PATH}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features discipline-specific a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/discipline_specific/)",
    )
    args = parser.parse_args()

    logger = setup_logging("discipline_specific_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "discipline_specific_features.jsonl"
    build_discipline_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
