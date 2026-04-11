#!/usr/bin/env python3
"""
feature_builders.basic_ratio_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fundamental ratio features derived from raw partants_master fields.

Single-pass streaming (no temporal state, no grouping required).
Every feature is computed solely from the current record's own fields,
so there is zero risk of future leakage.

Produces:
  - basic_ratio_features.jsonl   in output/basic_ratio_features/

Features per partant (12):
  - br_win_rate            : nb_victoires / max(nb_courses, 1)
  - br_place_rate          : (nb_victoires + nb_places_2eme + nb_places_3eme) / max(nb_courses, 1)
  - br_earnings_per_race   : gains_carriere / max(nb_courses, 1)
  - br_earnings_per_win    : gains_carriere / max(nb_victoires, 1)
  - br_win_place_ratio     : nb_victoires / max(nb_victoires + nb_places_2eme + nb_places_3eme, 1)
  - br_career_roi          : gains_carriere / max(allocation * nb_courses, 1)
  - br_age_experience_ratio: nb_courses / max(age - 1, 1)   (races per year of career)
  - br_distance_km         : distance / 1000
  - br_weight_burden       : poids_porte / max(distance / 1000, 1)
  - br_field_size_log      : log(1 + nombre_partants)
  - br_allocation_log      : log(1 + allocation)
  - br_is_winner_career    : 1 if nb_victoires > 0 else 0

Usage:
    python feature_builders/basic_ratio_features_builder.py
    python feature_builders/basic_ratio_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
    python feature_builders/basic_ratio_features_builder.py --output-dir /path/to/output
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/basic_ratio_features")

# Fallback search paths when the canonical path is missing (dev / CI)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

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
    """Return float or None; treats NaN and non-numeric values as None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN -> None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Return int or None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _fmax(val, minimum: float) -> float:
    """Return max(safe_float(val), minimum), falling back to minimum if val is None."""
    v = _safe_float(val)
    if v is None:
        return minimum
    return max(v, minimum)


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _compute_features(rec: dict[str, Any]) -> dict[str, Any]:
    """Compute the 12 basic ratio features for a single partant record."""

    # --- Raw field extraction ---
    partant_uid  = rec.get("partant_uid")
    course_uid   = rec.get("course_uid")
    date         = rec.get("date_reunion_iso")

    nb_courses   = _safe_float(rec.get("nb_courses_carriere"))
    nb_victoires = _safe_float(rec.get("nb_victoires_carriere"))
    nb_2eme      = _safe_float(rec.get("nb_places_2eme"))
    nb_3eme      = _safe_float(rec.get("nb_places_3eme"))
    gains        = _safe_float(rec.get("gains_carriere_euros"))
    allocation   = _safe_float(rec.get("allocation"))
    age          = _safe_float(rec.get("age"))
    distance     = _safe_float(rec.get("distance"))
    poids        = _safe_float(rec.get("poids_porte_kg"))
    nb_partants  = _safe_float(rec.get("nombre_partants"))

    # --- Derived denominators (never zero) ---
    courses_d    = max(nb_courses  or 0.0, 1.0)
    victoires_d  = max(nb_victoires or 0.0, 1.0)
    age_career_d = max((age or 2.0) - 1.0, 1.0)   # horses start at age 2

    nb_v     = nb_victoires or 0.0
    nb_2     = nb_2eme      or 0.0
    nb_3     = nb_3eme      or 0.0
    nb_g     = gains        or 0.0
    alloc    = allocation   or 0.0
    dist_val = distance     or 0.0

    nb_places_total = nb_v + nb_2 + nb_3

    # --- Feature calculations ---

    # 1. br_win_rate
    br_win_rate = round(nb_v / courses_d, 6)

    # 2. br_place_rate
    br_place_rate = round(nb_places_total / courses_d, 6)

    # 3. br_earnings_per_race
    br_earnings_per_race = round(nb_g / courses_d, 4)

    # 4. br_earnings_per_win
    br_earnings_per_win = round(nb_g / victoires_d, 4)

    # 5. br_win_place_ratio
    places_d = max(nb_places_total, 1.0)
    br_win_place_ratio = round(nb_v / places_d, 6)

    # 6. br_career_roi
    # gains_carriere / (allocation * nb_courses)  -- ROI per euro engaged
    total_engaged = alloc * (nb_courses or 0.0)
    roi_d = max(total_engaged, 1.0)
    br_career_roi = round(nb_g / roi_d, 6) if alloc > 0 and (nb_courses or 0) > 0 else None

    # 7. br_age_experience_ratio
    br_age_experience_ratio = round((nb_courses or 0.0) / age_career_d, 4)

    # 8. br_distance_km
    br_distance_km = round(dist_val / 1000.0, 4) if dist_val > 0 else None

    # 9. br_weight_burden  (kg per km)
    if poids is not None and dist_val > 0:
        dist_km = max(dist_val / 1000.0, 1.0)
        br_weight_burden = round(poids / dist_km, 4)
    else:
        br_weight_burden = None

    # 10. br_field_size_log
    if nb_partants is not None and nb_partants >= 0:
        br_field_size_log = round(math.log1p(nb_partants), 6)
    else:
        br_field_size_log = None

    # 11. br_allocation_log
    if alloc >= 0:
        br_allocation_log = round(math.log1p(alloc), 6)
    else:
        br_allocation_log = None

    # 12. br_is_winner_career
    br_is_winner_career = 1 if nb_v > 0 else 0

    return {
        "partant_uid":             partant_uid,
        "course_uid":              course_uid,
        "date_reunion_iso":        date,
        "br_win_rate":             br_win_rate,
        "br_place_rate":           br_place_rate,
        "br_earnings_per_race":    br_earnings_per_race,
        "br_earnings_per_win":     br_earnings_per_win,
        "br_win_place_ratio":      br_win_place_ratio,
        "br_career_roi":           br_career_roi,
        "br_age_experience_ratio": br_age_experience_ratio,
        "br_distance_km":          br_distance_km,
        "br_weight_burden":        br_weight_burden,
        "br_field_size_log":       br_field_size_log,
        "br_allocation_log":       br_allocation_log,
        "br_is_winner_career":     br_is_winner_career,
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_basic_ratio_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Single-pass streaming build of basic ratio features."""
    logger.info("=== Basic Ratio Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0
    n_errors = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Traite %d records...", n_read)

        try:
            feat = _compute_features(rec)
            results.append(feat)
        except Exception as exc:  # pragma: no cover
            n_errors += 1
            if n_errors <= 10:
                logger.warning("Erreur calcul record %d: %s", n_read, exc)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features en %.1fs (%.0f rec/s) - %d erreurs",
        len(results),
        elapsed,
        n_read / elapsed if elapsed > 0 else 0,
        n_errors,
    )

    # Free stream memory immediately
    gc.collect()

    return results


# ===========================================================================
# STATS & REPORTING
# ===========================================================================


def _print_fill_rates(results: list[dict[str, Any]], logger) -> None:
    """Log fill rate (non-None) per feature column."""
    if not results:
        return

    feature_keys = [
        k for k in results[0]
        if k not in ("partant_uid", "course_uid", "date_reunion_iso")
    ]
    counts: dict[str, int] = {k: 0 for k in feature_keys}
    total = len(results)

    for r in results:
        for k in feature_keys:
            if r.get(k) is not None:
                counts[k] += 1

    logger.info("=== Fill rates (%d records) ===", total)
    for k in feature_keys:
        pct = 100.0 * counts[k] / total if total else 0.0
        logger.info("  %-30s %d/%d  (%.1f%%)", k, counts[k], total, pct)


# ===========================================================================
# INPUT RESOLUTION & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file: CLI arg > canonical > fallback candidates."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve. Candidates: "
        + ", ".join(str(c) for c in _INPUT_CANDIDATES)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features de ratio de base a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("basic_ratio_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_basic_ratio_features(input_path, logger)

    out_path = output_dir / "basic_ratio_features.jsonl"
    save_jsonl(results, out_path, logger)

    _print_fill_rates(results, logger)

    logger.info("Sortie: %s", out_path)


if __name__ == "__main__":
    main()
