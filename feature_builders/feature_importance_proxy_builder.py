#!/usr/bin/env python3
"""
feature_builders.feature_importance_proxy_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Computes proxy features for feature importance and selection, based on
high-signal ratios and interactions known to be predictive in horse-racing
ML models.

All features are computed from point-in-time safe fields already present in
partants_master.jsonl. No historical aggregation is needed - each record is
fully self-contained. This makes the builder a true single-pass stream with
no sorting or look-ahead required.

Features produced (10):
  fip_odds_form_ratio        : cote_finale / max(recent_avg_position, 0.5)
  fip_win_pct_career         : nb_victoires_carriere / max(nb_courses_carriere, 1)
  fip_place_pct_career       : podium finishes / max(nb_courses_carriere, 1)
  fip_earnings_per_start     : gains_carriere_euros / max(nb_courses_carriere, 1)
  fip_recent_earnings_share  : gains_annee_euros / max(gains_carriere_euros, 1)
  fip_odds_rank_proxy        : 1 / (1 + cote_finale)  -- sigmoid-like transform
  fip_field_size_inv         : 1 / max(nombre_partants, 1) -- base win probability
  fip_class_earnings_ratio   : gains_carriere_euros / max(allocation, 1)
  fip_weight_to_distance     : poids_porte_kg / max(distance_metres / 1000, 1)
  fip_experience_log         : log(1 + nb_courses_carriere)

Usage:
    python feature_builders/feature_importance_proxy_builder.py
    python feature_builders/feature_importance_proxy_builder.py \\
        --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl \\
        --output D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/feature_importance_proxy
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

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path(
    "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"
)
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/feature_importance_proxy"
)

_LOG_EVERY = 500_000

# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Return float(value) or default if value is None / non-numeric."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _recent_avg_position(rec: dict) -> Optional[float]:
    """
    Best available recent average finishing position.
    Tries seq_position_moy_5, then position_moy_5, then None.
    """
    for key in ("seq_position_moy_5", "position_moy_5"):
        val = rec.get(key)
        if val is not None:
            try:
                v = float(val)
                if v > 0:
                    return v
            except (TypeError, ValueError):
                pass
    return None


def compute_features(rec: dict) -> dict[str, Optional[float]]:
    """
    Compute the 10 FIP features for a single partant record.
    Returns a dict with None where inputs are missing/invalid.
    """
    # ---- raw field extraction ----
    cote_finale = _safe_float(
        rec.get("cote_finale") or rec.get("rapport_final"), 0.0
    )
    nb_victoires = _safe_float(rec.get("nb_victoires_carriere"), 0.0)
    nb_2eme = _safe_float(rec.get("nb_places_2eme"), 0.0)
    nb_3eme = _safe_float(rec.get("nb_places_3eme"), 0.0)
    nb_courses = _safe_float(rec.get("nb_courses_carriere"), 0.0)
    gains_carriere = _safe_float(rec.get("gains_carriere_euros"), 0.0)
    gains_annee = _safe_float(rec.get("gains_annee_euros"), 0.0)
    nombre_partants = _safe_float(rec.get("nombre_partants"), 0.0)
    allocation = _safe_float(rec.get("allocation"), 0.0)
    poids_kg = _safe_float(rec.get("poids_porte_kg"), 0.0)
    distance = _safe_float(
        rec.get("distance") or rec.get("distance_metres"), 0.0
    )

    avg_pos = _recent_avg_position(rec)

    # ---- feature computations ----

    # fip_odds_form_ratio: market odds relative to recent form position
    # High ratio = market favors a horse that has been finishing poorly
    if cote_finale > 0 and avg_pos is not None:
        fip_odds_form_ratio: Optional[float] = cote_finale / max(avg_pos, 0.5)
    elif cote_finale > 0:
        fip_odds_form_ratio = None  # can't compute without form
    else:
        fip_odds_form_ratio = None

    # fip_win_pct_career: lifetime win rate
    if nb_courses > 0:
        fip_win_pct_career: Optional[float] = nb_victoires / nb_courses
    else:
        fip_win_pct_career = None

    # fip_place_pct_career: top-3 finish rate
    if nb_courses > 0:
        fip_place_pct_career: Optional[float] = (
            nb_victoires + nb_2eme + nb_3eme
        ) / nb_courses
    else:
        fip_place_pct_career = None

    # fip_earnings_per_start: average prize money per race start
    if nb_courses > 0:
        fip_earnings_per_start: Optional[float] = gains_carriere / nb_courses
    else:
        fip_earnings_per_start = None

    # fip_recent_earnings_share: this year's earnings as fraction of career
    # High value = horse is currently performing near its career best
    if gains_carriere > 0:
        fip_recent_earnings_share: Optional[float] = (
            gains_annee / gains_carriere
        )
    elif gains_annee == 0.0:
        fip_recent_earnings_share = 0.0
    else:
        fip_recent_earnings_share = None

    # fip_odds_rank_proxy: sigmoid-like transform of market odds
    # 1/(1+cote) gives a value in (0,1): higher = more favored by market
    if cote_finale > 0:
        fip_odds_rank_proxy: Optional[float] = 1.0 / (1.0 + cote_finale)
    else:
        fip_odds_rank_proxy = None

    # fip_field_size_inv: base win probability given field size
    if nombre_partants > 0:
        fip_field_size_inv: Optional[float] = 1.0 / nombre_partants
    else:
        fip_field_size_inv = None

    # fip_class_earnings_ratio: horse quality (earnings) vs race purse
    # High = horse has earned much more than the race is worth (class advantage)
    if allocation > 0:
        fip_class_earnings_ratio: Optional[float] = (
            gains_carriere / allocation
        )
    else:
        fip_class_earnings_ratio = None

    # fip_weight_to_distance: burden index (kg per km)
    # Higher = more weight relative to distance, generally unfavorable
    distance_km = distance / 1000.0 if distance > 0 else 0.0
    if poids_kg > 0 and distance_km > 0:
        fip_weight_to_distance: Optional[float] = poids_kg / distance_km
    else:
        fip_weight_to_distance = None

    # fip_experience_log: log(1 + n_starts) - diminishing returns on experience
    if nb_courses >= 0:
        fip_experience_log: Optional[float] = math.log1p(nb_courses)
    else:
        fip_experience_log = None

    return {
        "fip_odds_form_ratio": fip_odds_form_ratio,
        "fip_win_pct_career": fip_win_pct_career,
        "fip_place_pct_career": fip_place_pct_career,
        "fip_earnings_per_start": fip_earnings_per_start,
        "fip_recent_earnings_share": fip_recent_earnings_share,
        "fip_odds_rank_proxy": fip_odds_rank_proxy,
        "fip_field_size_inv": fip_field_size_inv,
        "fip_class_earnings_ratio": fip_class_earnings_ratio,
        "fip_weight_to_distance": fip_weight_to_distance,
        "fip_experience_log": fip_experience_log,
    }


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
                    logger.warning(
                        "Ligne JSON invalide ignoree (erreur %d)", errors
                    )
    logger.info(
        "Lecture terminee: %d records, %d erreurs JSON", count, errors
    )


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_feature_importance_proxy(input_path: Path, output_dir: Path, logger):
    """Single-pass streaming build of FIP features."""
    logger.info("=== Feature Importance Proxy Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "feature_importance_proxy.jsonl"

    n_read = 0
    n_written = 0
    n_null_all = 0  # records where every feature is None

    # per-feature fill counters
    feature_names = [
        "fip_odds_form_ratio",
        "fip_win_pct_career",
        "fip_place_pct_career",
        "fip_earnings_per_start",
        "fip_recent_earnings_share",
        "fip_odds_rank_proxy",
        "fip_field_size_inv",
        "fip_class_earnings_ratio",
        "fip_weight_to_distance",
        "fip_experience_log",
    ]
    fill_counts: dict[str, int] = {f: 0 for f in feature_names}

    with open(output_path, "w", encoding="utf-8") as out_f:
        for rec in _iter_jsonl(input_path, logger):
            n_read += 1
            if n_read % _LOG_EVERY == 0:
                elapsed = time.time() - t0
                logger.info(
                    "  Traite %d records en %.1fs (%.0f rec/s)",
                    n_read,
                    elapsed,
                    n_read / max(elapsed, 0.001),
                )

            features = compute_features(rec)

            # update fill counters
            all_none = True
            for fname in feature_names:
                v = features[fname]
                if v is not None:
                    fill_counts[fname] += 1
                    all_none = False

            if all_none:
                n_null_all += 1

            out_rec = {
                "partant_uid": rec.get("partant_uid"),
                "course_uid": rec.get("course_uid"),
                "date_reunion_iso": rec.get("date_reunion_iso"),
                "num_pmu": rec.get("num_pmu"),
                **features,
            }
            out_f.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            n_written += 1

    elapsed_total = time.time() - t0
    logger.info("=== Build termine en %.1fs ===", elapsed_total)
    logger.info("Records lus    : %d", n_read)
    logger.info("Records ecrits : %d", n_written)
    logger.info("Records tout-null: %d (%.1f%%)", n_null_all, 100 * n_null_all / max(n_read, 1))
    logger.info("Fichier de sortie: %s", output_path)
    logger.info("--- Fill rates par feature ---")
    for fname in feature_names:
        pct = 100.0 * fill_counts[fname] / max(n_read, 1)
        logger.info("  %-35s %7d / %d  (%.1f%%)", fname, fill_counts[fname], n_read, pct)

    gc.collect()
    return output_path


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Feature Importance Proxy Builder - correlation-based signals"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=INPUT_PARTANTS,
        help="Chemin vers partants_master.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help="Dossier de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("feature_importance_proxy_builder")

    if not args.input.exists():
        logger.error("Fichier d'entree introuvable: %s", args.input)
        sys.exit(1)

    build_feature_importance_proxy(args.input, args.output, logger)


if __name__ == "__main__":
    main()
