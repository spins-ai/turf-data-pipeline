#!/usr/bin/env python3
"""
feature_builders.monte_carlo_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Monte Carlo simulation and race simulation features.  Estimate race outcome
probabilities through simplified simulation-like calculations derived from
market odds.

Two-pass architecture:
  Pass 1 -- Stream partants_master.jsonl, extract minimal fields, group by
            course to collect per-race field data.
  Pass 2 -- For every partant, compute 10 simulation-derived features using
            the field context gathered in Pass 1.

Features per partant (10):
  - mc_win_probability_simple      : 1/cote normalised so field sums to 1.0
  - mc_place_probability_simple    : min(1, 3*wp*(1-wp)^0.5 + wp)
  - mc_exacta_partner_strength     : avg win_prob of top-2 OTHER horses
  - mc_trifecta_difficulty          : product of top-3 win probs
  - mc_field_entropy                : -sum(p*log(p)) Shannon entropy
  - mc_upset_probability            : 1 - max(win_probs)
  - mc_each_way_expected            : place_prob * place_odds_estimate - 1
  - mc_position_distribution_mean   : expected finishing position
  - mc_position_distribution_std    : std dev of position distribution
  - mc_competitive_balance          : 1 - HHI (Herfindahl-Hirschman)

Produces:
  - monte_carlo_features.jsonl  in builder_outputs/monte_carlo_features/

Usage:
    python feature_builders/monte_carlo_features_builder.py
    python feature_builders/monte_carlo_features_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/monte_carlo_features")

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# Place fraction: typical PMU place odds ~ gagnant / 4 for 3 places
_PLACE_FRACTION = 4.0

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
# FEATURE COMPUTATION HELPERS
# ===========================================================================


def _normalise_probs(raw_probs: list[float]) -> list[float]:
    """Normalise raw 1/cote values so they sum to 1.0."""
    total = sum(raw_probs)
    if total <= 0:
        return [0.0] * len(raw_probs)
    return [p / total for p in raw_probs]


def _place_probability(wp: float) -> float:
    """Estimated place probability from win probability."""
    return min(1.0, 3.0 * wp * math.sqrt(1.0 - wp) + wp)


def _shannon_entropy(probs: list[float]) -> float:
    """Shannon entropy: -sum(p * log(p))."""
    h = 0.0
    for p in probs:
        if p > 0:
            h -= p * math.log(p)
    return h


def _hhi(probs: list[float]) -> float:
    """Herfindahl-Hirschman Index: sum(p^2)."""
    return sum(p * p for p in probs)


def _position_distribution(win_probs: list[float], horse_idx: int):
    """
    Approximate expected finishing position and its std dev.

    Approximation: sort horses by win_prob descending; assign ranks 1..N.
    The horse's expected position is its rank in that ordering.  For std dev,
    treat it as a simple distribution where each rank k has probability
    proportional to how close it is to the expected rank.

    More precise approximation: for horse i with win_prob p_i, the probability
    of finishing at rank k can be approximated.  We use a simpler method:
    expected_pos = 1 + number of horses with higher win_prob,
    std = sqrt(sum over other horses of p_other * (1 - p_other)) scaled.
    """
    n = len(win_probs)
    if n == 0:
        return None, None

    wp_i = win_probs[horse_idx]

    # Expected position: 1 + count of horses with strictly higher win prob
    # For ties, add 0.5 per tied horse (average rank)
    rank_sum = 1.0
    for j, wp_j in enumerate(win_probs):
        if j == horse_idx:
            continue
        if wp_j > wp_i:
            rank_sum += 1.0
        elif wp_j == wp_i:
            rank_sum += 0.5

    expected_pos = rank_sum

    # Std dev approximation: based on variance of position
    # Each other horse with prob p_j has ~p_j chance of beating this horse
    # Variance contribution ~ p_j * (1 - p_j)
    var = 0.0
    for j, wp_j in enumerate(win_probs):
        if j == horse_idx:
            continue
        # Probability that horse j finishes ahead of horse i
        if wp_i + wp_j > 0:
            p_ahead = wp_j / (wp_i + wp_j)
        else:
            p_ahead = 0.5
        var += p_ahead * (1.0 - p_ahead)

    std_pos = math.sqrt(var) if var > 0 else 0.0

    return round(expected_pos, 4), round(std_pos, 4)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_monte_carlo_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build Monte Carlo simulation features in two passes."""
    logger.info("=== Monte Carlo Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -----------------------------------------------------------------------
    # Pass 1: Read minimal fields, group by course
    # -----------------------------------------------------------------------
    # course_key -> list of {uid, date, course, cote}
    course_groups: dict[str, list[dict]] = {}
    n_read = 0
    n_no_cote = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 — lu %d records...", n_read)
        if n_read % _GC_EVERY == 0:
            gc.collect()

        partant_uid = rec.get("partant_uid")
        course_uid = rec.get("course_uid", "")
        date_iso = rec.get("date_reunion_iso", "")

        # Parse odds
        cote_raw = rec.get("simple_gagnant") or rec.get("cote_finale") or rec.get("rapport_final")
        cote: Optional[float] = None
        if cote_raw is not None:
            try:
                cote = float(cote_raw)
                if cote <= 1.0:
                    cote = None
            except (ValueError, TypeError):
                cote = None

        if cote is None:
            n_no_cote += 1

        key = f"{date_iso}|{course_uid}"
        slim = {
            "uid": partant_uid,
            "date": date_iso,
            "course": course_uid,
            "cote": cote,
        }

        if key not in course_groups:
            course_groups[key] = []
        course_groups[key].append(slim)

    logger.info(
        "Pass 1 terminee: %d records, %d courses, %d sans cote (%.1f%%) en %.1fs",
        n_read,
        len(course_groups),
        n_no_cote,
        100 * n_no_cote / max(n_read, 1),
        time.time() - t0,
    )

    gc.collect()

    # -----------------------------------------------------------------------
    # Pass 2: Compute features per course
    # -----------------------------------------------------------------------
    t1 = time.time()
    results: list[dict[str, Any]] = []
    n_processed = 0

    for course_key, runners in course_groups.items():
        n_runners = len(runners)

        # --- Compute raw implied probs (1/cote) ---
        raw_probs = []
        has_all_cotes = True
        for r in runners:
            if r["cote"] is not None:
                raw_probs.append(1.0 / r["cote"])
            else:
                raw_probs.append(0.0)
                has_all_cotes = False

        # Normalised win probabilities
        win_probs = _normalise_probs(raw_probs)

        # --- Field-level features (computed once per course) ---
        field_entropy: Optional[float] = None
        upset_prob: Optional[float] = None
        trifecta_diff: Optional[float] = None
        competitive_balance: Optional[float] = None

        if has_all_cotes and n_runners >= 2:
            field_entropy = round(_shannon_entropy(win_probs), 6)
            upset_prob = round(1.0 - max(win_probs), 6)
            competitive_balance = round(1.0 - _hhi(win_probs), 6)

            # Trifecta difficulty: product of top 3 win probs
            sorted_probs = sorted(win_probs, reverse=True)
            if n_runners >= 3:
                trifecta_diff = round(
                    sorted_probs[0] * sorted_probs[1] * sorted_probs[2], 8
                )
            else:
                trifecta_diff = round(sorted_probs[0] * sorted_probs[1], 8)

        # --- Per-runner features ---
        # Pre-sort indices by win_prob descending for exacta partner calc
        sorted_indices = sorted(range(n_runners), key=lambda k: win_probs[k], reverse=True)

        for idx, r in enumerate(runners):
            wp = win_probs[idx]
            cote_val = r["cote"]

            # mc_win_probability_simple
            mc_win_prob: Optional[float] = None
            if has_all_cotes:
                mc_win_prob = round(wp, 6)

            # mc_place_probability_simple
            mc_place_prob: Optional[float] = None
            if mc_win_prob is not None:
                mc_place_prob = round(_place_probability(wp), 6)

            # mc_exacta_partner_strength: avg win_prob of top-2 OTHER horses
            mc_exacta_partner: Optional[float] = None
            if has_all_cotes and n_runners >= 3:
                top_others = []
                for si in sorted_indices:
                    if si == idx:
                        continue
                    top_others.append(win_probs[si])
                    if len(top_others) == 2:
                        break
                if len(top_others) == 2:
                    mc_exacta_partner = round(sum(top_others) / 2.0, 6)

            # mc_each_way_expected: place_prob * place_odds_estimate - 1
            mc_each_way: Optional[float] = None
            if mc_place_prob is not None and cote_val is not None:
                place_odds = cote_val / _PLACE_FRACTION
                mc_each_way = round(mc_place_prob * place_odds - 1.0, 6)

            # mc_position_distribution_mean / std
            mc_pos_mean: Optional[float] = None
            mc_pos_std: Optional[float] = None
            if has_all_cotes and n_runners >= 2:
                mc_pos_mean, mc_pos_std = _position_distribution(win_probs, idx)

            results.append({
                "partant_uid": r["uid"],
                "course_uid": r["course"],
                "date_reunion_iso": r["date"],
                "mc_win_probability_simple": mc_win_prob,
                "mc_place_probability_simple": mc_place_prob,
                "mc_exacta_partner_strength": mc_exacta_partner,
                "mc_trifecta_difficulty": trifecta_diff,
                "mc_field_entropy": field_entropy,
                "mc_upset_probability": upset_prob,
                "mc_each_way_expected": mc_each_way,
                "mc_position_distribution_mean": mc_pos_mean,
                "mc_position_distribution_std": mc_pos_std,
                "mc_competitive_balance": competitive_balance,
            })

        n_processed += n_runners
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Pass 2 — traite %d / %d records...", n_processed, n_read)

        # Periodic GC
        if n_processed % _GC_EVERY == 0:
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features en %.1fs (%d courses)",
        len(results),
        elapsed,
        len(course_groups),
    )

    # Free course_groups memory
    del course_groups
    gc.collect()

    return results


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
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features Monte Carlo simulation a partir de partants_master"
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
        help="Repertoire de sortie (defaut: builder_outputs/monte_carlo_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("monte_carlo_features_builder")
    logger.info("=" * 70)
    logger.info("monte_carlo_features_builder.py — Monte Carlo Simulation Features")
    logger.info("=" * 70)

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_monte_carlo_features(input_path, logger)

    # Save
    out_path = output_dir / "monte_carlo_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rate summary
    if results:
        feature_keys = [
            k for k in results[0]
            if k not in ("partant_uid", "course_uid", "date_reunion_iso")
        ]
        total = len(results)
        logger.info("=== Fill rates (%d records) ===", total)
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info("  %s: %d/%d (%.1f%%)", k, filled, total, 100 * filled / total)

    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
