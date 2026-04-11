#!/usr/bin/env python3
"""
feature_builders.bet_liquidity_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Betting market liquidity and depth features.

Reads partants_master.jsonl in two streaming passes, groups by course_uid,
and computes per-partant market liquidity metrics.

Two-pass approach:
  Pass 1: Scan partants_master, group by course_uid, collect cote_finale +
          nombre_partants + totalEnjeu for each horse to build race-level
          aggregates (overround, min/max cote, total enjeu, etc.).
  Pass 2: Stream partants_master again, emit one feature row per partant
          using the race-level aggregates computed in pass 1.

Temporal integrity: all features are derived from pre-race betting market
data (odds, pool sizes) -- no future leakage.

Produces:
  - bet_liquidity_features.jsonl  in builder_outputs/bet_liquidity/

Features per partant:
  - liq_total_enjeu_course   : total betting pool for this race (totalEnjeu / enjeu)
  - liq_enjeu_per_partant    : total enjeu / nombre_partants
  - liq_horse_implied_prob   : 1 / cote_finale (implied probability from odds)
  - liq_market_overround     : sum of all 1/cote for all horses (>1 = bookmaker margin)
  - liq_horse_pct_of_pool    : estimated horse share = (1/cote) / overround
  - liq_odds_spread          : max_cote - min_cote in the race (market breadth)
  - liq_favorite_dominance   : implied_prob of favorite / sum of all implied_probs
  - liq_nb_short_odds        : count of horses with cote_finale < 10

Usage:
    python feature_builders/bet_liquidity_builder.py
    python feature_builders/bet_liquidity_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

_DEFAULT_INPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
_DEFAULT_OUTPUT = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/bet_liquidity/bet_liquidity_features.jsonl"
)

# Fallback candidates when default path is unavailable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_FALLBACKS = [
    _DEFAULT_INPUT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger, pass_label: str = ""):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    prefix = f"[{pass_label}] " if pass_label else ""
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
                    logger.warning("%sLigne JSON invalide ignoree (erreur %d)", prefix, errors)
    logger.info(
        "%sLecture terminee: %d records, %d erreurs JSON", prefix, count, errors
    )


def _safe_float(val) -> Optional[float]:
    """Return float or None; rejects NaN, zero, and negatives for odds."""
    if val is None:
        return None
    try:
        v = float(val)
        # NaN check
        return v if v == v else None
    except (ValueError, TypeError):
        return None


def _safe_float_pos(val) -> Optional[float]:
    """Return float > 0 or None (for odds which must be strictly positive)."""
    v = _safe_float(val)
    return v if (v is not None and v > 0.0) else None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PASS 1: BUILD RACE-LEVEL AGGREGATES
# ===========================================================================


def _build_race_aggregates(input_path: Path, logger) -> dict[str, dict]:
    """
    Pass 1: stream partants_master once and collect per-course data.

    Returns a dict keyed by course_uid:
        {
            "total_enjeu"   : float | None,
            "nb_partants"   : int | None,
            "cotes"         : list[float],   # all valid cote_finale values
            "overround"     : float | None,  # sum 1/cote
            "min_cote"      : float | None,
            "max_cote"      : float | None,
            "fav_implied"   : float | None,  # implied prob of favorite (lowest cote)
            "sum_implied"   : float | None,  # sum of all 1/cote
            "nb_short_odds" : int,           # count cote < 10
        }
    """
    logger.info("=== Pass 1: collecte des agregats par course ===")
    t0 = time.time()

    # Intermediate storage: course_uid -> raw lists for aggregation
    # We use minimal per-course dicts to keep memory low
    course_data: dict[str, dict] = {}

    n_read = 0
    for rec in _iter_jsonl(input_path, logger, pass_label="Pass1"):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass1 Lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid") or ""
        if not course_uid:
            continue

        if course_uid not in course_data:
            course_data[course_uid] = {
                "total_enjeu": None,
                "nb_partants": None,
                "cotes": [],
            }

        cd = course_data[course_uid]

        # Total enjeu: pick first non-None value seen for this course
        if cd["total_enjeu"] is None:
            enjeu = _safe_float(rec.get("totalEnjeu")) or _safe_float(rec.get("enjeu"))
            if enjeu is not None and enjeu > 0:
                cd["total_enjeu"] = enjeu

        # nombre_partants: pick first non-None value
        if cd["nb_partants"] is None:
            nb = _safe_int(rec.get("nombre_partants"))
            if nb is not None and nb > 0:
                cd["nb_partants"] = nb

        # Collect cote
        cote = _safe_float_pos(rec.get("cote_finale")) or _safe_float_pos(rec.get("cote_reference"))
        if cote is not None:
            cd["cotes"].append(cote)

    logger.info(
        "Pass 1 terminee: %d courses en %.1fs", len(course_data), time.time() - t0
    )

    # Post-process: compute derived race-level stats
    t1 = time.time()
    aggregates: dict[str, dict] = {}
    for course_uid, cd in course_data.items():
        cotes = cd["cotes"]
        total_enjeu = cd["total_enjeu"]
        nb_partants = cd["nb_partants"]

        if cotes:
            implied_probs = [1.0 / c for c in cotes]
            sum_implied = sum(implied_probs)
            min_cote = min(cotes)
            max_cote = max(cotes)
            # Favorite = lowest cote = highest implied prob
            fav_implied = 1.0 / min_cote
            nb_short_odds = sum(1 for c in cotes if c < 10.0)
        else:
            implied_probs = []
            sum_implied = None
            min_cote = None
            max_cote = None
            fav_implied = None
            nb_short_odds = 0

        aggregates[course_uid] = {
            "total_enjeu": total_enjeu,
            "nb_partants": nb_partants,
            "sum_implied": sum_implied,
            "min_cote": min_cote,
            "max_cote": max_cote,
            "fav_implied": fav_implied,
            "nb_short_odds": nb_short_odds,
        }

    # Free raw data
    del course_data
    gc.collect()

    logger.info(
        "Post-traitement Pass 1 termine en %.1fs", time.time() - t1
    )
    return aggregates


# ===========================================================================
# PASS 2: COMPUTE PER-PARTANT FEATURES
# ===========================================================================


def _build_features(
    input_path: Path,
    aggregates: dict[str, dict],
    logger,
) -> list[dict[str, Any]]:
    """
    Pass 2: stream partants_master again, compute 8 liquidity features
    per partant using pre-computed race-level aggregates.
    """
    logger.info("=== Pass 2: calcul des features par partant ===")
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0
    n_no_course = 0

    for rec in _iter_jsonl(input_path, logger, pass_label="Pass2"):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass2 Lu %d records...", n_read)
            gc.collect()

        partant_uid = rec.get("partant_uid")
        course_uid = rec.get("course_uid") or ""

        if not course_uid:
            n_no_course += 1
            # Still emit a row with all-None features
            results.append({
                "partant_uid": partant_uid,
                "liq_total_enjeu_course": None,
                "liq_enjeu_per_partant": None,
                "liq_horse_implied_prob": None,
                "liq_market_overround": None,
                "liq_horse_pct_of_pool": None,
                "liq_odds_spread": None,
                "liq_favorite_dominance": None,
                "liq_nb_short_odds": None,
            })
            continue

        agg = aggregates.get(course_uid, {})

        # Per-partant odds
        cote = _safe_float_pos(rec.get("cote_finale")) or _safe_float_pos(rec.get("cote_reference"))
        horse_implied = (1.0 / cote) if cote is not None else None

        # Race-level values from aggregates
        total_enjeu = agg.get("total_enjeu")
        nb_partants = agg.get("nb_partants")
        sum_implied = agg.get("sum_implied")
        min_cote = agg.get("min_cote")
        max_cote = agg.get("max_cote")
        fav_implied = agg.get("fav_implied")
        nb_short_odds = agg.get("nb_short_odds")

        # Feature: liq_total_enjeu_course
        liq_total_enjeu_course = total_enjeu

        # Feature: liq_enjeu_per_partant
        liq_enjeu_per_partant: Optional[float] = None
        if total_enjeu is not None and nb_partants is not None and nb_partants > 0:
            liq_enjeu_per_partant = round(total_enjeu / nb_partants, 2)

        # Feature: liq_horse_implied_prob
        liq_horse_implied_prob: Optional[float] = None
        if horse_implied is not None:
            liq_horse_implied_prob = round(horse_implied, 6)

        # Feature: liq_market_overround (sum of 1/cote for all horses in race)
        liq_market_overround: Optional[float] = None
        if sum_implied is not None:
            liq_market_overround = round(sum_implied, 6)

        # Feature: liq_horse_pct_of_pool = (1/cote) / overround
        liq_horse_pct_of_pool: Optional[float] = None
        if horse_implied is not None and sum_implied is not None and sum_implied > 0:
            liq_horse_pct_of_pool = round(horse_implied / sum_implied, 6)

        # Feature: liq_odds_spread = max_cote - min_cote
        liq_odds_spread: Optional[float] = None
        if min_cote is not None and max_cote is not None:
            liq_odds_spread = round(max_cote - min_cote, 4)

        # Feature: liq_favorite_dominance = fav_implied / sum_implied
        liq_favorite_dominance: Optional[float] = None
        if fav_implied is not None and sum_implied is not None and sum_implied > 0:
            liq_favorite_dominance = round(fav_implied / sum_implied, 6)

        # Feature: liq_nb_short_odds
        liq_nb_short_odds: Optional[int] = nb_short_odds if nb_short_odds is not None else None

        results.append({
            "partant_uid": partant_uid,
            "liq_total_enjeu_course": liq_total_enjeu_course,
            "liq_enjeu_per_partant": liq_enjeu_per_partant,
            "liq_horse_implied_prob": liq_horse_implied_prob,
            "liq_market_overround": liq_market_overround,
            "liq_horse_pct_of_pool": liq_horse_pct_of_pool,
            "liq_odds_spread": liq_odds_spread,
            "liq_favorite_dominance": liq_favorite_dominance,
            "liq_nb_short_odds": liq_nb_short_odds,
        })

    elapsed = time.time() - t0
    logger.info(
        "Pass 2 terminee: %d features en %.1fs (%d sans course_uid)",
        len(results), elapsed, n_no_course,
    )
    return results


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_bet_liquidity_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Full two-pass build of betting market liquidity features."""
    logger.info("=== Bet Liquidity Builder ===")
    logger.info("Input: %s", input_path)
    t_total = time.time()

    # Pass 1
    aggregates = _build_race_aggregates(input_path, logger)

    # Pass 2
    results = _build_features(input_path, aggregates, logger)

    # Free aggregates
    del aggregates
    gc.collect()

    logger.info(
        "Build total termine: %d partants en %.1fs",
        len(results), time.time() - t_total,
    )
    return results


# ===========================================================================
# INPUT RESOLUTION & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path from CLI arg or fallback candidates."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_FALLBACKS:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in _INPUT_FALLBACKS]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de liquidite du marche pari a partir de partants_master"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help=(
            "Chemin vers partants_master.jsonl "
            f"(defaut: {_DEFAULT_INPUT})"
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=(
            "Chemin complet du fichier de sortie JSONL "
            f"(defaut: {_DEFAULT_OUTPUT})"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("bet_liquidity_builder")

    input_path = _find_input(args.input)
    output_path = Path(args.output) if args.output else _DEFAULT_OUTPUT
    output_path.parent.mkdir(parents=True, exist_ok=True)

    results = build_bet_liquidity_features(input_path, logger)

    # Atomic write via save_jsonl (.tmp then rename)
    save_jsonl(results, output_path, logger)

    # Fill-rate report
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total_count = len(results)
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info(
                "  %s: %d/%d (%.1f%%)",
                k, v, total_count, 100.0 * v / total_count,
            )


if __name__ == "__main__":
    main()
