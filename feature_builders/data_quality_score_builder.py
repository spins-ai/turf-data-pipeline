#!/usr/bin/env python3
"""
feature_builders.data_quality_score_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Data quality / completeness score features per partant.

Measures how much data is available for each record -- useful for
meta-learning (weighting samples by reliability) and model routing
(choosing a specialised model based on available feature groups).

Single-pass streaming: reads partants_master.jsonl once, writes features
directly to disk via a .tmp file that is atomically renamed on success.

Produces:
  - data_quality_score.jsonl   in builder_outputs/data_quality_score/

Features per partant (10):
  - dqs_core_completeness         : fraction of core fields non-null (7 fields)
  - dqs_performance_completeness  : fraction of performance fields non-null (4 fields)
  - dqs_history_completeness      : fraction of history fields non-null (4 fields)
  - dqs_pedigree_completeness     : fraction of pedigree fields non-null (6 fields)
  - dqs_market_completeness       : fraction of market fields non-null (3 fields)
  - dqs_overall_completeness      : average of the 5 completeness scores above
  - dqs_nb_null_fields            : total count of null fields across all columns
  - dqs_is_well_documented        : 1 if overall_completeness > 0.7, else 0
  - dqs_has_timing_data           : 1 if temps_ms is non-null and > 0
  - dqs_era_indicator             : 0 (<2010), 1 (2010-2015), 2 (2015-2020), 3 (2020+)

Usage:
    python feature_builders/data_quality_score_builder.py
    python feature_builders/data_quality_score_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/data_quality_score")

_LOG_EVERY = 500_000

# ---------------------------------------------------------------------------
# Field groups for completeness scores
# ---------------------------------------------------------------------------

_CORE_FIELDS = [
    "age", "sexe", "distance", "discipline",
    "cote_finale", "nombre_partants", "poids_porte_kg",
]  # 7 fields

_PERFORMANCE_FIELDS = [
    "position_arrivee", "temps_ms", "reduction_km_ms", "is_gagnant",
]  # 4 fields

_HISTORY_FIELDS = [
    "nb_victoires_carriere", "nb_courses_carriere",
    "gains_carriere_euros", "musique",
]  # 4 fields

_PEDIGREE_FIELDS = [
    "pere", "mere", "pere_mere",
    "ped_speed_index", "ped_stamina_index", "ped_has_pedigree",
]  # 6 fields

_MARKET_FIELDS = [
    "cote_finale", "cote_reference", "proba_implicite",
]  # 3 fields


# ===========================================================================
# STREAMING READER
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


# ===========================================================================
# HELPERS
# ===========================================================================


def _field_present(rec: dict, field: str) -> bool:
    """Return True if the field exists and is not None / empty string."""
    val = rec.get(field)
    if val is None:
        return False
    if isinstance(val, str) and val.strip() == "":
        return False
    return True


def _completeness(rec: dict, fields: list[str]) -> float:
    """Fraction of fields that are non-null in *rec*."""
    if not fields:
        return 0.0
    present = sum(1 for f in fields if _field_present(rec, f))
    return round(present / len(fields), 4)


def _era_indicator(date_str: Optional[str]) -> Optional[int]:
    """Assign era bucket: 0 (<2010), 1 (2010-2015), 2 (2015-2020), 3 (2020+)."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        year = int(date_str[:4])
    except (ValueError, IndexError):
        return None
    if year < 2010:
        return 0
    if year < 2015:
        return 1
    if year < 2020:
        return 2
    return 3


# ===========================================================================
# MAIN BUILD (single-pass, streaming output)
# ===========================================================================


def build_data_quality_score_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build data quality score features from partants_master.jsonl.

    Single-pass streaming: reads records one at a time, computes features,
    writes directly to a .tmp file that is atomically renamed on success.

    Returns the total number of feature records written.
    """
    logger.info("=== Data Quality Score Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0

    fill_counts = {
        "dqs_core_completeness": 0,
        "dqs_performance_completeness": 0,
        "dqs_history_completeness": 0,
        "dqs_pedigree_completeness": 0,
        "dqs_market_completeness": 0,
        "dqs_overall_completeness": 0,
        "dqs_nb_null_fields": 0,
        "dqs_is_well_documented": 0,
        "dqs_has_timing_data": 0,
        "dqs_era_indicator": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_processed)
                gc.collect()

            partant_uid = rec.get("partant_uid")

            # --- Completeness scores ---
            core_comp = _completeness(rec, _CORE_FIELDS)
            perf_comp = _completeness(rec, _PERFORMANCE_FIELDS)
            hist_comp = _completeness(rec, _HISTORY_FIELDS)
            pedi_comp = _completeness(rec, _PEDIGREE_FIELDS)
            mkt_comp = _completeness(rec, _MARKET_FIELDS)

            overall_comp = round(
                (core_comp + perf_comp + hist_comp + pedi_comp + mkt_comp) / 5.0,
                4,
            )

            # --- Null count across all columns ---
            nb_null = sum(1 for v in rec.values() if v is None or (isinstance(v, str) and v.strip() == ""))

            # --- Binary flags ---
            is_well_doc = 1 if overall_comp > 0.7 else 0

            temps_ms = rec.get("temps_ms")
            has_timing = 0
            if temps_ms is not None:
                try:
                    has_timing = 1 if float(temps_ms) > 0 else 0
                except (ValueError, TypeError):
                    has_timing = 0

            # --- Era indicator ---
            date_str = rec.get("date_reunion_iso") or rec.get("date_reunion")
            era = _era_indicator(date_str)

            # --- Build feature dict ---
            features: dict[str, Any] = {
                "partant_uid": partant_uid,
                "dqs_core_completeness": core_comp,
                "dqs_performance_completeness": perf_comp,
                "dqs_history_completeness": hist_comp,
                "dqs_pedigree_completeness": pedi_comp,
                "dqs_market_completeness": mkt_comp,
                "dqs_overall_completeness": overall_comp,
                "dqs_nb_null_fields": nb_null,
                "dqs_is_well_documented": is_well_doc,
                "dqs_has_timing_data": has_timing,
                "dqs_era_indicator": era,
            }

            # --- Track fill rates ---
            for k in fill_counts:
                if features.get(k) is not None:
                    fill_counts[k] += 1

            # Write to output
            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Data quality score build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
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
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de qualite de donnees a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/data_quality_score/)",
    )
    args = parser.parse_args()

    logger = setup_logging("data_quality_score_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "data_quality_score.jsonl"
    build_data_quality_score_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
