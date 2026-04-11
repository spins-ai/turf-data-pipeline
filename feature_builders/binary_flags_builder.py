#!/usr/bin/env python3
"""
feature_builders.binary_flags_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Binary flag features derived from static partants_master fields.

Each flag is computed directly from the current record -- no temporal
accumulation required.  The builder streams partants_master.jsonl in a
single pass and emits one output record per input record.

Produces:
  - binary_flags.jsonl   in builder_outputs/binary_flags/

Features (12):
  - bf_has_blinkers      : 1 if oeilleres is not None/empty
  - bf_is_gelding        : 1 if sexe in ("hongre", "H", "gelding")
  - bf_is_female         : 1 if sexe in ("femelle", "F", "jument")
  - bf_is_young          : 1 if age <= 3
  - bf_is_veteran        : 1 if age >= 7
  - bf_is_heavy_weight   : 1 if poids_porte_kg >= 60
  - bf_is_light_weight   : 1 if poids_porte_kg <= 52
  - bf_is_short_distance : 1 if distance < 1600
  - bf_is_long_distance  : 1 if distance >= 2400
  - bf_has_supplement    : 1 if supplement > 0
  - bf_is_newcomer       : 1 if nb_courses_carriere <= 2
  - bf_is_class_dropper  : 1 if is_class_drop or spd_is_class_drop is truthy

Usage:
    python feature_builders/binary_flags_builder.py
    python feature_builders/binary_flags_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/binary_flags_builder.py --output-dir /path/to/output
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/binary_flags")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# Sexe sets
_GELDING_SEXES = {"hongre", "h", "gelding"}
_FEMALE_SEXES = {"femelle", "f", "jument"}

# Weight thresholds (kg)
_HEAVY_WEIGHT_KG = 60.0
_LIGHT_WEIGHT_KG = 52.0

# Distance thresholds (metres)
_SHORT_DISTANCE_M = 1600
_LONG_DISTANCE_M = 2400

# Age thresholds
_YOUNG_AGE = 3
_VETERAN_AGE = 7

# Newcomer threshold (career races)
_NEWCOMER_MAX_RACES = 2


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # filter NaN
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _iter_jsonl(path: Path, logger):
    """Yield parsed dicts from a JSONL file, one line at a time."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning(
                        "Ligne JSON invalide ignoree (erreur #%d)", errors
                    )
    logger.info(
        "Lecture terminee: %d records, %d erreurs JSON", count, errors
    )


# ===========================================================================
# FLAG COMPUTATION
# ===========================================================================


def _flag(condition) -> Optional[int]:
    """Return 1/0 int flag, or None if condition is None."""
    if condition is None:
        return None
    return 1 if condition else 0


def compute_binary_flags(rec: dict[str, Any]) -> dict[str, Any]:
    """Compute all 12 binary flag features from a single record."""

    # -- bf_has_blinkers --
    oeilleres = rec.get("oeilleres")
    has_blinkers: Optional[int]
    if oeilleres is None:
        has_blinkers = 0
    else:
        s = str(oeilleres).strip()
        has_blinkers = 1 if s else 0

    # -- bf_is_gelding / bf_is_female --
    sexe_raw = rec.get("sexe")
    is_gelding: Optional[int]
    is_female: Optional[int]
    if sexe_raw is None:
        is_gelding = None
        is_female = None
    else:
        sexe_norm = str(sexe_raw).strip().lower()
        is_gelding = 1 if sexe_norm in _GELDING_SEXES else 0
        is_female = 1 if sexe_norm in _FEMALE_SEXES else 0

    # -- bf_is_young / bf_is_veteran --
    age = _safe_int(rec.get("age"))
    is_young = _flag(age is not None and age <= _YOUNG_AGE)
    is_veteran = _flag(age is not None and age >= _VETERAN_AGE)

    # -- bf_is_heavy_weight / bf_is_light_weight --
    poids = _safe_float(rec.get("poids_porte_kg"))
    is_heavy = _flag(poids is not None and poids >= _HEAVY_WEIGHT_KG)
    is_light = _flag(poids is not None and poids <= _LIGHT_WEIGHT_KG)

    # -- bf_is_short_distance / bf_is_long_distance --
    distance = _safe_float(rec.get("distance"))
    is_short = _flag(distance is not None and distance < _SHORT_DISTANCE_M)
    is_long = _flag(distance is not None and distance >= _LONG_DISTANCE_M)

    # -- bf_has_supplement --
    supplement = _safe_float(rec.get("supplement"))
    has_supplement = _flag(supplement is not None and supplement > 0)

    # -- bf_is_newcomer --
    nb_courses = _safe_int(rec.get("nb_courses_carriere"))
    is_newcomer = _flag(nb_courses is not None and nb_courses <= _NEWCOMER_MAX_RACES)

    # -- bf_is_class_dropper --
    raw_icd = rec.get("is_class_drop")
    raw_spd = rec.get("spd_is_class_drop")

    def _is_truthy(v) -> bool:
        if v is None:
            return False
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "oui")
        return False

    any_class_drop = _is_truthy(raw_icd) or _is_truthy(raw_spd)
    # Flag is defined as long as at least one field was present in the record
    if "is_class_drop" not in rec and "spd_is_class_drop" not in rec:
        is_class_dropper: Optional[int] = None
    else:
        is_class_dropper = 1 if any_class_drop else 0

    return {
        "partant_uid": rec.get("partant_uid"),
        "course_uid": rec.get("course_uid"),
        "date_reunion_iso": rec.get("date_reunion_iso"),
        "bf_has_blinkers": has_blinkers,
        "bf_is_gelding": is_gelding,
        "bf_is_female": is_female,
        "bf_is_young": is_young,
        "bf_is_veteran": is_veteran,
        "bf_is_heavy_weight": is_heavy,
        "bf_is_light_weight": is_light,
        "bf_is_short_distance": is_short,
        "bf_is_long_distance": is_long,
        "bf_has_supplement": has_supplement,
        "bf_is_newcomer": is_newcomer,
        "bf_is_class_dropper": is_class_dropper,
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_binary_flags(input_path: Path, logger) -> list[dict[str, Any]]:
    """Single-pass streaming build of binary flag features."""
    logger.info("=== Binary Flags Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Traite %d records...", n_read)

        results.append(compute_binary_flags(rec))

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features en %.1fs", len(results), elapsed
    )

    gc.collect()
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file, preferring CLI argument then known candidates."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve parmi: "
        + str([str(c) for c in _INPUT_CANDIDATES])
    )


def _print_fill_rates(results: list[dict[str, Any]], logger) -> None:
    """Log fill rate for each flag feature."""
    if not results:
        return
    feature_keys = [k for k in results[0] if k.startswith("bf_")]
    total = len(results)
    logger.info("=== Fill rates (%d records) ===", total)
    for k in feature_keys:
        filled = sum(1 for r in results if r.get(k) is not None)
        ones = sum(1 for r in results if r.get(k) == 1)
        logger.info(
            "  %-30s  fill=%d/%d (%.1f%%)  positives=%.1f%%",
            k,
            filled,
            total,
            100.0 * filled / total if total else 0.0,
            100.0 * ones / filled if filled else 0.0,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features binaires a partir de partants_master"
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
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/binary_flags/)",
    )
    args = parser.parse_args()

    logger = setup_logging("binary_flags_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_binary_flags(input_path, logger)

    out_path = output_dir / "binary_flags.jsonl"
    save_jsonl(results, out_path, logger)

    _print_fill_rates(results, logger)


if __name__ == "__main__":
    main()
