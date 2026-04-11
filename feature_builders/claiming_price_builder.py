#!/usr/bin/env python3
"""
feature_builders.claiming_price_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Claiming / engagement / supplement price features and financial indicators.

Single-pass streaming builder: features are mostly static (derived from the
current record), no temporal tracking needed.

Temporal integrity: all values are known before the race starts -- no future
leakage.

Produces:
  - claiming_price.jsonl  in builder_outputs/claiming_price/

Features per partant:
  - clm_claiming_price      : taux_reclamation_euros as float (0 if not a claimer)
  - clm_is_claimer          : 1 if taux_reclamation > 0, 0 otherwise
  - clm_claiming_vs_gains   : taux_reclamation / gains_carriere (value ratio)
  - clm_engagement_fee      : engagement as float
  - clm_supplement_fee      : supplement_euros as float
  - clm_total_investment    : engagement + supplement (total owner investment)
  - clm_gains_per_race      : gains_carriere / nb_courses_carriere (efficiency)
  - clm_gains_roi           : gains_carriere / (gains_carriere + nb_courses * 500)

Usage:
    python feature_builders/claiming_price_builder.py
    python feature_builders/claiming_price_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/claiming_price")

_LOG_EVERY = 500_000

# Approximate cost per race entry (euros) for ROI computation
_COST_PER_ENTRY = 500


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
    """Convert to float, return None on failure or NaN."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Convert to int, return None on failure."""
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
    """Compute claiming price features from a single record."""
    uid = rec.get("partant_uid")

    taux_recl = _safe_float(rec.get("taux_reclamation_euros"))
    engagement = _safe_float(rec.get("engagement"))
    supplement = _safe_float(rec.get("supplement_euros"))
    gains_carriere = _safe_float(rec.get("gains_carriere_euros"))
    gains_annee = _safe_float(rec.get("gains_annee_euros"))
    nb_courses = _safe_int(rec.get("nb_courses_carriere"))
    nb_victoires = _safe_int(rec.get("nb_victoires_carriere"))

    feats: dict[str, Any] = {"partant_uid": uid}

    # --- clm_claiming_price ---
    if taux_recl is not None:
        feats["clm_claiming_price"] = taux_recl
    else:
        feats["clm_claiming_price"] = 0.0

    # --- clm_is_claimer ---
    if taux_recl is not None and taux_recl > 0:
        feats["clm_is_claimer"] = 1
    else:
        feats["clm_is_claimer"] = 0

    # --- clm_claiming_vs_gains ---
    if taux_recl is not None and taux_recl > 0 and gains_carriere is not None and gains_carriere > 0:
        feats["clm_claiming_vs_gains"] = round(taux_recl / gains_carriere, 6)
    else:
        feats["clm_claiming_vs_gains"] = None

    # --- clm_engagement_fee ---
    feats["clm_engagement_fee"] = engagement if engagement is not None else None

    # --- clm_supplement_fee ---
    feats["clm_supplement_fee"] = supplement if supplement is not None else None

    # --- clm_total_investment ---
    eng_val = engagement if engagement is not None else 0.0
    sup_val = supplement if supplement is not None else 0.0
    if engagement is not None or supplement is not None:
        feats["clm_total_investment"] = round(eng_val + sup_val, 2)
    else:
        feats["clm_total_investment"] = None

    # --- clm_gains_per_race ---
    if gains_carriere is not None and nb_courses is not None and nb_courses > 0:
        feats["clm_gains_per_race"] = round(gains_carriere / nb_courses, 2)
    else:
        feats["clm_gains_per_race"] = None

    # --- clm_gains_roi ---
    if gains_carriere is not None and nb_courses is not None and nb_courses > 0:
        denominator = gains_carriere + nb_courses * _COST_PER_ENTRY
        if denominator > 0:
            feats["clm_gains_roi"] = round(gains_carriere / denominator, 6)
        else:
            feats["clm_gains_roi"] = None
    else:
        feats["clm_gains_roi"] = None

    return feats


# ===========================================================================
# MAIN BUILD (single-pass streaming to disk)
# ===========================================================================


def build_claiming_price_features(input_path: Path, output_path: Path, logger) -> int:
    """Build claiming price features in a single streaming pass.

    Reads input line by line, computes features, writes directly to disk.
    Returns the total number of feature records written.
    """
    logger.info("=== Claiming Price Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_read = 0
    n_written = 0
    fill_counts: dict[str, int] = {
        "clm_claiming_price": 0,
        "clm_is_claimer": 0,
        "clm_claiming_vs_gains": 0,
        "clm_engagement_fee": 0,
        "clm_supplement_fee": 0,
        "clm_total_investment": 0,
        "clm_gains_per_race": 0,
        "clm_gains_roi": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_read)
                gc.collect()

            feats = _compute_features(rec)

            # Track fill rates
            for k in fill_counts:
                if feats.get(k) is not None:
                    fill_counts[k] += 1

            fout.write(json.dumps(feats, ensure_ascii=False))
            fout.write("\n")
            n_written += 1

    # Atomic rename
    final_path = output_path
    if final_path.exists():
        final_path.unlink()
    tmp_out.rename(final_path)

    elapsed = time.time() - t0
    logger.info(
        "Claiming price build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written > 0 else 0.0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features claiming price a partir de partants_master"
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

    logger = setup_logging("claiming_price_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "claiming_price.jsonl"
    build_claiming_price_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
