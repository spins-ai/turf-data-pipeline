#!/usr/bin/env python3
"""
feature_builders.polynomial_interaction_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Top-5 polynomial interaction features from the most predictive base features.

Reads partants_master.jsonl in streaming mode (16 GB), processes all records,
and computes per-partant polynomial interaction terms (products, ratios,
squares) from pre-existing feature values.

These interactions capture nonlinear relationships that tree models can
find on their own but that linear/NN models benefit from having explicit:

  1. proba_implicite x elo_combined      (market-vs-skill signal)
  2. seq_serie_places x cote_finale       (form streak amplified by odds)
  3. nombre_partants x draw_position_norm  (field size context for draw)
  4. age x distance                        (maturity-distance aptitude)
  5. momentum_3 x field_elo_mean           (form relative to field quality)

For each combination we emit: product, ratio (a/b), and squared dominant
term -- yielding 15 features total.

Temporal integrity: all input features are themselves computed with no
future leakage, so combining them preserves that property.

Produces:
  - polynomial_interactions.jsonl   in output/polynomial_interactions/

Usage:
    python feature_builders/polynomial_interaction_builder.py
    python feature_builders/polynomial_interaction_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
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
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "polynomial_interactions"

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# POLYNOMIAL COMBINATIONS DEFINITION
# ===========================================================================

# Each tuple: (name_prefix, field_a, field_b)
# We generate: {prefix}_product, {prefix}_ratio, {prefix}_sq_a
POLY_COMBOS = [
    (
        "proba_elo",
        ["proba_implicite", "win_probability_implied"],
        ["elo_combined"],
    ),
    (
        "serie_cote",
        ["seq_serie_places", "serie_places"],
        ["cote_finale", "cote_reference", "rapport_final"],
    ),
    (
        "partants_draw",
        ["nombre_partants", "nb_partants"],
        ["draw_position_normalized"],
    ),
    (
        "age_dist",
        ["age"],
        ["distance", "distance_metres"],
    ),
    (
        "momentum_field",
        ["momentum_3"],
        ["field_elo_mean", "elo_mean_field"],
    ),
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert value to float, returning None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return f
    except (TypeError, ValueError):
        return None


def _resolve_field(rec: dict, candidates: list[str]) -> Optional[float]:
    """Try multiple field name candidates, return first non-None float."""
    for field in candidates:
        val = _safe_float(rec.get(field))
        if val is not None:
            return val
    return None


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
# MAIN BUILD
# ===========================================================================


def build_polynomial_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build polynomial interaction features from partants_master.jsonl.

    Single-pass approach: read each record, extract the 10 base feature
    values, compute 15 polynomial terms, emit one result dict per partant.
    """
    logger.info("=== Polynomial Interaction Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Traite %d records...", n_read)

        uid = rec.get("partant_uid")

        out: dict[str, Any] = {"partant_uid": uid}

        for prefix, fields_a, fields_b in POLY_COMBOS:
            a = _resolve_field(rec, fields_a)
            b = _resolve_field(rec, fields_b)

            # Product: a * b
            if a is not None and b is not None:
                product = a * b
                out[f"poly_{prefix}_product"] = round(product, 6)
            else:
                out[f"poly_{prefix}_product"] = None

            # Ratio: a / b (safe division)
            if a is not None and b is not None and b != 0.0:
                ratio = a / b
                out[f"poly_{prefix}_ratio"] = round(ratio, 6)
            else:
                out[f"poly_{prefix}_ratio"] = None

            # Squared dominant (first term): a^2
            if a is not None:
                out[f"poly_{prefix}_sq"] = round(a * a, 6)
            else:
                out[f"poly_{prefix}_sq"] = None

        results.append(out)

    elapsed = time.time() - t0
    logger.info(
        "Polynomial build termine: %d features en %.1fs",
        len(results), elapsed,
    )

    return results


# ===========================================================================
# SAVE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features polynomiales a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/polynomial_interactions/)",
    )
    args = parser.parse_args()

    logger = setup_logging("polynomial_interaction_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_polynomial_features(input_path, logger)

    # Save
    out_path = output_dir / "polynomial_interactions.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
