#!/usr/bin/env python3
"""
feature_builders.pedigree_distance_cross_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Crosses pedigree data with distance/surface/discipline to create
targeted interaction features.

Temporal integrity: uses only static pedigree columns already present
in the partant record + race-level fields. No future leakage.

Features (10):
  - pdx_speed_x_short_dist       : ped_speed_index * (distance < 1600)
  - pdx_stamina_x_long_dist      : ped_stamina_index * (distance > 2200)
  - pdx_sire_speed_x_plat        : ped_sire_speed_flag * (discipline == Plat)
  - pdx_sire_stamina_x_obstacle  : ped_sire_stamina_flag * (discipline contains Obstacle/Haie/Steeple)
  - pdx_lignee_dist_match         : ped_lignee_adaptee_distance - distance
  - pdx_inbreeding_x_age          : ped_inbreeding_score * age
  - pdx_speed_stamina_ratio       : ped_speed_index / (ped_stamina_index + 0.01)
  - pdx_pedigree_surface_match    : speed flag if PSF track, else stamina index
  - pdx_broodmare_sire_signal     : 1 if pere_mere is non-null
  - pdx_pedigree_completeness     : fraction of non-null pedigree fields

Usage:
    python feature_builders/pedigree_distance_cross_builder.py
    python feature_builders/pedigree_distance_cross_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pedigree_distance_cross")
_LOG_EVERY = 500_000

# Pedigree fields used for completeness score
_PEDIGREE_FIELDS = (
    "pere", "mere", "pere_mere",
    "ped_speed_index", "ped_stamina_index",
    "ped_sire_speed_flag", "ped_sire_stamina_flag",
    "ped_lignee_adaptee_distance", "ped_inbreeding_score",
)


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _is_obstacle(discipline: str) -> bool:
    """Return True if discipline contains Obstacle, Haie, or Steeple."""
    d = discipline.upper()
    return "OBSTACLE" in d or "HAIE" in d or "STEEPLE" in d


def _is_psf(type_piste: str, met_is_psf) -> bool:
    """Return True if the track is PSF (polytrack/synthetic)."""
    if met_is_psf:
        try:
            if int(met_is_psf) == 1:
                return True
        except (ValueError, TypeError):
            pass
    if type_piste:
        return "PSF" in type_piste.upper()
    return False


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _compute_features(rec: dict) -> dict[str, Any]:
    """Compute the 10 pedigree x distance/surface cross features."""
    out: dict[str, Any] = {"partant_uid": rec.get("partant_uid", "")}

    # Extract raw fields
    distance = _safe_float(rec.get("distance"))
    discipline = (rec.get("discipline") or "").strip()
    type_piste = (rec.get("type_piste") or "").strip()
    met_is_psf = rec.get("met_is_psf")
    age = _safe_int(rec.get("age"))

    ped_speed = _safe_float(rec.get("ped_speed_index"))
    ped_stamina = _safe_float(rec.get("ped_stamina_index"))
    sire_speed_flag = _safe_float(rec.get("ped_sire_speed_flag"))
    sire_stamina_flag = _safe_float(rec.get("ped_sire_stamina_flag"))
    lignee_dist = _safe_float(rec.get("ped_lignee_adaptee_distance"))
    inbreeding = _safe_float(rec.get("ped_inbreeding_score"))
    pere = rec.get("pere")
    mere = rec.get("mere")
    pere_mere = rec.get("pere_mere")

    # 1. pdx_speed_x_short_dist
    if ped_speed is not None and distance is not None:
        out["pdx_speed_x_short_dist"] = round(ped_speed * (1 if distance < 1600 else 0), 4)
    else:
        out["pdx_speed_x_short_dist"] = None

    # 2. pdx_stamina_x_long_dist
    if ped_stamina is not None and distance is not None:
        out["pdx_stamina_x_long_dist"] = round(ped_stamina * (1 if distance > 2200 else 0), 4)
    else:
        out["pdx_stamina_x_long_dist"] = None

    # 3. pdx_sire_speed_x_plat
    if sire_speed_flag is not None and discipline:
        out["pdx_sire_speed_x_plat"] = round(sire_speed_flag * (1 if discipline.upper() == "PLAT" else 0), 4)
    else:
        out["pdx_sire_speed_x_plat"] = None

    # 4. pdx_sire_stamina_x_obstacle
    if sire_stamina_flag is not None and discipline:
        out["pdx_sire_stamina_x_obstacle"] = round(sire_stamina_flag * (1 if _is_obstacle(discipline) else 0), 4)
    else:
        out["pdx_sire_stamina_x_obstacle"] = None

    # 5. pdx_lignee_dist_match
    if lignee_dist is not None and distance is not None:
        out["pdx_lignee_dist_match"] = round(lignee_dist - distance, 1)
    else:
        out["pdx_lignee_dist_match"] = None

    # 6. pdx_inbreeding_x_age
    if inbreeding is not None and age is not None:
        out["pdx_inbreeding_x_age"] = round(inbreeding * age, 4)
    else:
        out["pdx_inbreeding_x_age"] = None

    # 7. pdx_speed_stamina_ratio
    if ped_speed is not None and ped_stamina is not None:
        out["pdx_speed_stamina_ratio"] = round(ped_speed / (ped_stamina + 0.01), 4)
    else:
        out["pdx_speed_stamina_ratio"] = None

    # 8. pdx_pedigree_surface_match
    psf = _is_psf(type_piste, met_is_psf)
    if psf and sire_speed_flag is not None:
        out["pdx_pedigree_surface_match"] = round(sire_speed_flag, 4)
    elif not psf and ped_stamina is not None:
        out["pdx_pedigree_surface_match"] = round(ped_stamina, 4)
    else:
        out["pdx_pedigree_surface_match"] = None

    # 9. pdx_broodmare_sire_signal
    if pere_mere is not None and str(pere_mere).strip():
        out["pdx_broodmare_sire_signal"] = 1
    else:
        out["pdx_broodmare_sire_signal"] = 0

    # 10. pdx_pedigree_completeness
    n_total = len(_PEDIGREE_FIELDS)
    n_filled = 0
    for field in _PEDIGREE_FIELDS:
        val = rec.get(field)
        if val is not None and str(val).strip():
            n_filled += 1
    out["pdx_pedigree_completeness"] = round(n_filled / n_total, 4)

    return out


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build(input_path: Path, output_dir: Path, logger) -> None:
    logger.info("=== Pedigree Distance Cross Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "pedigree_distance_cross.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    feature_keys = [
        "pdx_speed_x_short_dist", "pdx_stamina_x_long_dist",
        "pdx_sire_speed_x_plat", "pdx_sire_stamina_x_obstacle",
        "pdx_lignee_dist_match", "pdx_inbreeding_x_age",
        "pdx_speed_stamina_ratio", "pdx_pedigree_surface_match",
        "pdx_broodmare_sire_signal", "pdx_pedigree_completeness",
    ]
    fill = {k: 0 for k in feature_keys}

    n_read = 0
    n_errors = 0
    n_written = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Lu %d records...", n_read)
                gc.collect()

            out = _compute_features(rec)
            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_written += 1

            # Track fill
            for k in feature_keys:
                if out.get(k) is not None:
                    fill[k] += 1

    # Atomic rename
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d features ecrites en %.1fs (%d erreurs JSON)",
                n_written, elapsed, n_errors)

    # Fill rates
    logger.info("=== Fill rates ===")
    for k in feature_keys:
        v = fill[k]
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features pedigree x distance/surface cross"
    )
    parser.add_argument("--input", type=str, default=None,
                        help="Chemin vers partants_master.jsonl")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Repertoire de sortie")
    args = parser.parse_args()

    logger = setup_logging("pedigree_distance_cross_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    build(input_path, output_dir, logger)


if __name__ == "__main__":
    main()
