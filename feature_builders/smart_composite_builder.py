#!/usr/bin/env python3
"""
feature_builders.smart_composite_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
High-signal composite features designed for gradient boosting models
(CatBoost / XGBoost / LightGBM).

All 10 features are computed in a single streaming pass over
partants_master.jsonl using only point-in-time fields already present
in each record — no temporal state, no future leakage.

Produces:
  - smart_composite.jsonl  in builder_outputs/smart_composite/

Features per partant:
  sc_form_odds_product       : (1/position_moy_5) * (1/cote_finale)
  sc_class_form_signal       : is_class_drop * (1/position_moy_5)
  sc_value_bet_signal        : (1/cote_finale) - (nb_victoires/nb_courses)
  sc_experience_class        : log(1+nb_courses_carriere) * log(1+gains_carriere_euros)
  sc_momentum_signal         : (gains_annee_euros/max(gains_carriere_euros,1)) * (1/max(cote_finale,1))
  sc_weight_adjusted_form    : (1/position_moy_5) / max(poids_porte_kg,50) * 55
  sc_age_form_interaction    : age * (nb_victoires/max(nb_courses_carriere,1))
  sc_market_confidence       : 1 / (1 + abs(cote_finale - cote_reference))
  sc_field_position_signal   : (nombre_partants - num_pmu) / max(nombre_partants,1)
  sc_earnings_efficiency     : gains_carriere_euros / max(nb_courses_carriere,1) / max(allocation,1)

Usage:
    python feature_builders/smart_composite_builder.py
    python feature_builders/smart_composite_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/smart_composite_builder.py --output-dir /path/to/output/
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/smart_composite")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Fallback candidates when the primary path does not exist
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# Reference weight used for weight-adjusted form normalisation
_REF_WEIGHT_KG: float = 55.0
_MIN_WEIGHT_KG: float = 50.0


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning(
                        "Ligne JSON invalide ignoree (erreur %d)", errors
                    )
    logger.info(
        "Lecture terminee : %d records, %d erreurs JSON", count, errors
    )


def _sf(val) -> Optional[float]:
    """Safe float conversion; returns None for None / NaN / invalid."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN guard
    except (TypeError, ValueError):
        return None


def _si(val) -> Optional[int]:
    """Safe int conversion."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _round6(v: float) -> float:
    return round(v, 6)


# ===========================================================================
# FEATURE COMPUTATION (point-in-time, single record)
# ===========================================================================


def _compute_features(rec: dict) -> dict[str, Any]:
    """
    Compute all 10 composite features from a single partants_master record.
    Returns a dict keyed by feature name; missing inputs yield None.
    """

    # ── Extract raw fields ──────────────────────────────────────────────────
    uid: Optional[str] = rec.get("partant_uid")

    cote_finale: Optional[float] = _sf(rec.get("cote_finale"))
    cote_reference: Optional[float] = _sf(rec.get("cote_reference"))
    position_moy_5: Optional[float] = _sf(rec.get("position_moy_5"))
    is_class_drop_raw = rec.get("spd_is_class_drop") or rec.get("is_class_drop")
    is_class_drop: Optional[float] = _sf(is_class_drop_raw)
    nb_victoires: Optional[float] = _sf(rec.get("nb_victoires"))
    nb_courses: Optional[float] = _sf(
        rec.get("nb_courses") or rec.get("nb_courses_carriere")
    )
    nb_courses_carriere: Optional[float] = _sf(rec.get("nb_courses_carriere"))
    gains_carriere: Optional[float] = _sf(rec.get("gains_carriere_euros"))
    gains_annee: Optional[float] = _sf(rec.get("gains_annee_euros"))
    poids_kg: Optional[float] = _sf(rec.get("poids_porte_kg"))
    age: Optional[float] = _sf(rec.get("age"))
    nombre_partants: Optional[float] = _sf(rec.get("nombre_partants"))
    num_pmu: Optional[float] = _sf(rec.get("num_pmu"))
    allocation: Optional[float] = _sf(rec.get("allocation"))

    # Convenience: inverse of cote_finale (implied probability)
    inv_cote: Optional[float] = (
        1.0 / cote_finale if cote_finale is not None and cote_finale > 0 else None
    )
    # Inverse of position_moy_5 (form signal: lower avg position = better)
    inv_pos: Optional[float] = (
        1.0 / position_moy_5
        if position_moy_5 is not None and position_moy_5 > 0
        else None
    )

    # ── Feature 1 : sc_form_odds_product ────────────────────────────────────
    # (1/position_moy_5) * (1/cote_finale)
    # Combines recent form with market assessment; high value = well-fancied in-form horse
    sc_form_odds_product: Optional[float] = None
    if inv_pos is not None and inv_cote is not None:
        sc_form_odds_product = _round6(inv_pos * inv_cote)

    # ── Feature 2 : sc_class_form_signal ────────────────────────────────────
    # is_class_drop * (1/position_moy_5)
    # Flags horses stepping down in class that are also in good form
    sc_class_form_signal: Optional[float] = None
    if is_class_drop is not None and inv_pos is not None:
        sc_class_form_signal = _round6(is_class_drop * inv_pos)

    # ── Feature 3 : sc_value_bet_signal ─────────────────────────────────────
    # (1/cote_finale) - (nb_victoires/nb_courses)
    # Positive = market over-estimates the horse relative to its strike rate
    # Negative = horse is underbet relative to its record
    sc_value_bet_signal: Optional[float] = None
    if inv_cote is not None:
        win_rate: Optional[float] = None
        if (
            nb_victoires is not None
            and nb_courses is not None
            and nb_courses > 0
        ):
            win_rate = nb_victoires / nb_courses
        if win_rate is not None:
            sc_value_bet_signal = _round6(inv_cote - win_rate)

    # ── Feature 4 : sc_experience_class ─────────────────────────────────────
    # log(1+nb_courses_carriere) * log(1+gains_carriere_euros)
    # Quality-adjusted experience: veteran winners outrank inexperienced horses
    sc_experience_class: Optional[float] = None
    if nb_courses_carriere is not None and gains_carriere is not None:
        sc_experience_class = _round6(
            math.log1p(max(nb_courses_carriere, 0))
            * math.log1p(max(gains_carriere, 0))
        )

    # ── Feature 5 : sc_momentum_signal ──────────────────────────────────────
    # (gains_annee_euros / max(gains_carriere_euros, 1)) * (1 / max(cote_finale, 1))
    # Horse on a hot streak that is also backed by the market
    sc_momentum_signal: Optional[float] = None
    if gains_annee is not None and gains_carriere is not None and cote_finale is not None:
        career_denom = max(gains_carriere, 1.0)
        cote_denom = max(cote_finale, 1.0)
        sc_momentum_signal = _round6(
            (gains_annee / career_denom) * (1.0 / cote_denom)
        )

    # ── Feature 6 : sc_weight_adjusted_form ─────────────────────────────────
    # (1/position_moy_5) / max(poids_porte_kg, 50) * 55
    # Good form normalised by weight carried (lighter = less penalty)
    sc_weight_adjusted_form: Optional[float] = None
    if inv_pos is not None and poids_kg is not None:
        effective_weight = max(poids_kg, _MIN_WEIGHT_KG)
        sc_weight_adjusted_form = _round6(
            inv_pos / effective_weight * _REF_WEIGHT_KG
        )

    # ── Feature 7 : sc_age_form_interaction ─────────────────────────────────
    # age * (nb_victoires / max(nb_courses_carriere, 1))
    # Age-adjusted ability: seasoned winners score higher than young or
    # inexperienced horses with the same raw win count
    sc_age_form_interaction: Optional[float] = None
    if (
        age is not None
        and nb_victoires is not None
        and nb_courses_carriere is not None
    ):
        career_denom2 = max(nb_courses_carriere, 1.0)
        sc_age_form_interaction = _round6(age * (nb_victoires / career_denom2))

    # ── Feature 8 : sc_market_confidence ────────────────────────────────────
    # 1 / (1 + abs(cote_finale - cote_reference))
    # Market consensus: cote_finale close to cote_reference = stable market,
    # high confidence; wide spread = uncertainty
    sc_market_confidence: Optional[float] = None
    if cote_finale is not None and cote_reference is not None:
        sc_market_confidence = _round6(
            1.0 / (1.0 + abs(cote_finale - cote_reference))
        )

    # ── Feature 9 : sc_field_position_signal ────────────────────────────────
    # (nombre_partants - num_pmu) / max(nombre_partants, 1)
    # Draw / barrier position advantage: closer to the inside (low num_pmu)
    # in large fields produces higher values
    sc_field_position_signal: Optional[float] = None
    if nombre_partants is not None and num_pmu is not None:
        denom_field = max(nombre_partants, 1.0)
        sc_field_position_signal = _round6(
            (nombre_partants - num_pmu) / denom_field
        )

    # ── Feature 10 : sc_earnings_efficiency ─────────────────────────────────
    # gains_carriere_euros / max(nb_courses_carriere, 1) / max(allocation, 1)
    # Earnings per start relative to the prize value of the race:
    # high = earns well vs the prize pool offered
    sc_earnings_efficiency: Optional[float] = None
    if (
        gains_carriere is not None
        and nb_courses_carriere is not None
        and allocation is not None
    ):
        per_race = gains_carriere / max(nb_courses_carriere, 1.0)
        alloc_denom = max(allocation, 1.0)
        sc_earnings_efficiency = _round6(per_race / alloc_denom)

    return {
        "partant_uid": uid,
        "sc_form_odds_product": sc_form_odds_product,
        "sc_class_form_signal": sc_class_form_signal,
        "sc_value_bet_signal": sc_value_bet_signal,
        "sc_experience_class": sc_experience_class,
        "sc_momentum_signal": sc_momentum_signal,
        "sc_weight_adjusted_form": sc_weight_adjusted_form,
        "sc_age_form_interaction": sc_age_form_interaction,
        "sc_market_confidence": sc_market_confidence,
        "sc_field_position_signal": sc_field_position_signal,
        "sc_earnings_efficiency": sc_earnings_efficiency,
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================

_FEATURE_NAMES = [
    "sc_form_odds_product",
    "sc_class_form_signal",
    "sc_value_bet_signal",
    "sc_experience_class",
    "sc_momentum_signal",
    "sc_weight_adjusted_form",
    "sc_age_form_interaction",
    "sc_market_confidence",
    "sc_field_position_signal",
    "sc_earnings_efficiency",
]


def build_smart_composite_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """
    Stream partants_master.jsonl, compute features, return list of dicts.

    Single-pass: each record is processed independently — no temporal state
    is maintained, no look-back window is needed.
    """
    logger.info("=== Smart Composite Builder ===")
    logger.info("Source : %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    fill_counts: dict[str, int] = {k: 0 for k in _FEATURE_NAMES}
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            elapsed = time.time() - t0
            logger.info(
                "  Traite %d records en %.1fs...", n_read, elapsed
            )

        feats = _compute_features(rec)
        results.append(feats)

        # Accumulate fill counts
        for k in _FEATURE_NAMES:
            if feats.get(k) is not None:
                fill_counts[k] += 1

    elapsed_total = time.time() - t0
    logger.info(
        "Build termine : %d features en %.1fs", len(results), elapsed_total
    )

    # Fill-rate summary
    total = len(results)
    if total > 0:
        logger.info("=== Fill rates ===")
        for k in _FEATURE_NAMES:
            cnt = fill_counts[k]
            logger.info(
                "  %-35s : %d / %d  (%.1f%%)",
                k, cnt, total, 100.0 * cnt / total,
            )

    gc.collect()
    return results


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file: CLI arg > primary path > fallback candidates."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable : {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve. Candidates: "
        + ", ".join(str(c) for c in _INPUT_CANDIDATES)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compute high-signal composite features for gradient boosting "
            "models from partants_master.jsonl."
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut : auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut : builder_outputs/smart_composite/)",
    )
    args = parser.parse_args()

    logger = setup_logging("smart_composite_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_smart_composite_features(input_path, logger)

    out_path = output_dir / "smart_composite.jsonl"
    save_jsonl(results, out_path, logger)
    logger.info("Sortie : %s", out_path)


if __name__ == "__main__":
    main()
