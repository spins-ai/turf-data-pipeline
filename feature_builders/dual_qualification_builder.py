#!/usr/bin/env python3
"""
feature_builders.dual_qualification_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Dual qualification features -- measuring if horses qualify for multiple
types of races/bets.

Reads partants_master.jsonl in single-pass streaming mode and computes
per-partant dual-qualification features.

Temporal integrity: all features are derived from the race conditions and
the horse's own attributes at race time -- no future leakage.

Produces:
  - dual_qualification.jsonl   in builder_outputs/dual_qualification/

Features per partant (8):
  - dql_is_quinte             : cnd_cond_is_quinte flag
  - dql_is_tierce             : cnd_cond_is_tierce flag
  - dql_is_international      : cnd_cond_is_international flag
  - dql_is_debut              : 1 if is_inedit is truthy (first-time runner)
  - dql_age_eligible_range    : cnd_cond_age_max - cnd_cond_age_min (width of age range)
  - dql_horse_age_in_range    : 1 if horse age between cnd_cond_age_min and cnd_cond_age_max
  - dql_distance_match        : abs(cnd_cond_distance_m - distance) if cnd_cond_distance_m exists
  - dql_race_restriction_score: count of restriction conditions (quinte + tierce + international + narrow_age)

Usage:
    python feature_builders/dual_qualification_builder.py
    python feature_builders/dual_qualification_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/dual_qualification")

_LOG_EVERY = 500_000

# Age range narrower than this counts as a "narrow age" restriction
_NARROW_AGE_THRESHOLD = 3


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
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _is_truthy(val) -> bool:
    """Return True for truthy values (1, True, "1", "true", "oui", etc.)."""
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "oui", "yes", "vrai")
    return False


# ===========================================================================
# MAIN BUILD (single-pass streaming)
# ===========================================================================


def build_dual_qualification_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build dual-qualification features from partants_master.jsonl.

    Single-pass streaming: read input, compute features, write to disk.
    No historical state needed -- all features depend on the current record only.

    Returns the total number of feature records written.
    """
    logger.info("=== Dual Qualification Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    fill_counts = {
        "dql_is_quinte": 0,
        "dql_is_tierce": 0,
        "dql_is_international": 0,
        "dql_is_debut": 0,
        "dql_age_eligible_range": 0,
        "dql_horse_age_in_range": 0,
        "dql_distance_match": 0,
        "dql_race_restriction_score": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_processed)
                gc.collect()

            uid = rec.get("partant_uid")
            features: dict[str, Any] = {"partant_uid": uid}

            # --- Raw values ---
            is_quinte = rec.get("cnd_cond_is_quinte")
            is_tierce = rec.get("cnd_cond_is_tierce")
            is_international = rec.get("cnd_cond_is_international")
            is_inedit = rec.get("is_inedit")
            age_min = _safe_int(rec.get("cnd_cond_age_min"))
            age_max = _safe_int(rec.get("cnd_cond_age_max"))
            cond_distance = _safe_float(rec.get("cnd_cond_distance_m"))
            distance = _safe_float(rec.get("distance"))
            age = _safe_int(rec.get("age"))

            # --- 1. dql_is_quinte ---
            if is_quinte is not None:
                features["dql_is_quinte"] = 1 if _is_truthy(is_quinte) else 0
                fill_counts["dql_is_quinte"] += 1
            else:
                features["dql_is_quinte"] = None

            # --- 2. dql_is_tierce ---
            if is_tierce is not None:
                features["dql_is_tierce"] = 1 if _is_truthy(is_tierce) else 0
                fill_counts["dql_is_tierce"] += 1
            else:
                features["dql_is_tierce"] = None

            # --- 3. dql_is_international ---
            if is_international is not None:
                features["dql_is_international"] = 1 if _is_truthy(is_international) else 0
                fill_counts["dql_is_international"] += 1
            else:
                features["dql_is_international"] = None

            # --- 4. dql_is_debut ---
            if is_inedit is not None:
                features["dql_is_debut"] = 1 if _is_truthy(is_inedit) else 0
                fill_counts["dql_is_debut"] += 1
            else:
                features["dql_is_debut"] = None

            # --- 5. dql_age_eligible_range ---
            if age_min is not None and age_max is not None:
                features["dql_age_eligible_range"] = age_max - age_min
                fill_counts["dql_age_eligible_range"] += 1
            else:
                features["dql_age_eligible_range"] = None

            # --- 6. dql_horse_age_in_range ---
            if age is not None and age_min is not None and age_max is not None:
                features["dql_horse_age_in_range"] = 1 if age_min <= age <= age_max else 0
                fill_counts["dql_horse_age_in_range"] += 1
            else:
                features["dql_horse_age_in_range"] = None

            # --- 7. dql_distance_match ---
            if cond_distance is not None and distance is not None:
                features["dql_distance_match"] = round(abs(cond_distance - distance), 1)
                fill_counts["dql_distance_match"] += 1
            else:
                features["dql_distance_match"] = None

            # --- 8. dql_race_restriction_score ---
            #  Count: quinte + tierce + international + narrow_age
            score = 0
            has_any = False

            if is_quinte is not None:
                has_any = True
                if _is_truthy(is_quinte):
                    score += 1
            if is_tierce is not None:
                has_any = True
                if _is_truthy(is_tierce):
                    score += 1
            if is_international is not None:
                has_any = True
                if _is_truthy(is_international):
                    score += 1
            if age_min is not None and age_max is not None:
                has_any = True
                if (age_max - age_min) < _NARROW_AGE_THRESHOLD:
                    score += 1

            if has_any:
                features["dql_race_restriction_score"] = score
                fill_counts["dql_race_restriction_score"] += 1
            else:
                features["dql_race_restriction_score"] = None

            # Stream to output
            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Dual qualification build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)",
            k, v, n_written, 100 * v / n_written if n_written else 0,
        )

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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features dual qualification a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/dual_qualification/)",
    )
    args = parser.parse_args()

    logger = setup_logging("dual_qualification_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "dual_qualification.jsonl"
    build_dual_qualification_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
