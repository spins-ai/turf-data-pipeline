"""
feature_builders.market_entropy_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Betting market efficiency metrics per race.

Features
--------
- market_entropy        : Shannon entropy of implied probabilities (-sum(p*log(p))).
                          Higher = more competitive / uncertain race.
- market_overround      : Total overround (sum of 1/odds for all runners).
                          Measures bookmaker margin.
- implied_probability   : This horse's 1/cote_finale normalised by field sum.
- odds_vs_implied       : Ratio of actual odds to "fair" odds (overround-adjusted).
                          >1 = potential value.
- favourite_strength    : Implied probability of favourite / 2nd favourite.
                          High = dominant favourite.
- field_competitiveness : Number of runners with implied prob > 5%.
                          More = more competitive.

Input: partants_master.jsonl -- uses cote_finale, course_uid, partant_uid.

Streams line-by-line, buffers by course_uid (~20 records per course).

Usage:
    python feature_builders/market_entropy_features.py
    python feature_builders/market_entropy_features.py --input data_master/partants_master.jsonl
    python feature_builders/market_entropy_features.py --output output/market_entropy_features
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = os.path.join("data_master", "partants_master.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "market_entropy_features")

COMPETITIVENESS_THRESHOLD = 0.05  # 5 % implied probability

# ===========================================================================
# HELPERS
# ===========================================================================


def _shannon_entropy(probs: list[float]) -> Optional[float]:
    """Shannon entropy: -sum(p * log(p)) for p > 0."""
    if not probs:
        return None
    entropy = 0.0
    for p in probs:
        if p > 0:
            entropy -= p * math.log(p)
    return round(entropy, 6)


def _implied_probs(runners: list[dict]) -> dict[str, float]:
    """Return {partant_uid: raw_implied_prob} for runners with valid cote_finale."""
    result: dict[str, float] = {}
    for p in runners:
        uid = p.get("partant_uid")
        cote = p.get("cote_finale")
        if uid and cote is not None and cote > 0:
            result[uid] = 1.0 / cote
    return result


# ===========================================================================
# BUILDER  (race-level, called per course_uid group)
# ===========================================================================


def _build_race_features(runners: list[dict]) -> list[dict]:
    """Compute market-entropy features for all runners in a single race.

    Parameters
    ----------
    runners : list[dict]
        Partant dicts sharing the same course_uid.

    Returns
    -------
    list[dict]
        One feature dict per partant_uid.
    """
    raw_implied = _implied_probs(runners)

    # Overround = sum of raw implied probs (bookmaker margin)
    overround = sum(raw_implied.values()) if raw_implied else None

    # Normalised implied probabilities (sum to 1.0)
    normalised: dict[str, float] = {}
    if overround and overround > 0:
        normalised = {uid: ip / overround for uid, ip in raw_implied.items()}

    # Shannon entropy of normalised probs
    norm_values = list(normalised.values())
    entropy = _shannon_entropy(norm_values) if norm_values else None

    # Favourite strength: implied_prob(fav) / implied_prob(2nd fav)
    sorted_impl = sorted(normalised.values(), reverse=True)
    fav_strength: Optional[float] = None
    if len(sorted_impl) >= 2 and sorted_impl[1] > 0:
        fav_strength = round(sorted_impl[0] / sorted_impl[1], 6)

    # Field competitiveness: count runners with normalised implied > 5%
    competitiveness = sum(1 for p in normalised.values() if p > COMPETITIVENESS_THRESHOLD)

    results = []
    for p in runners:
        uid = p.get("partant_uid")
        cote = p.get("cote_finale")

        # implied_probability (normalised)
        impl_prob: Optional[float] = None
        if uid in normalised:
            impl_prob = round(normalised[uid], 6)

        # odds_vs_implied: actual odds / fair odds
        # fair odds = 1 / normalised_prob = overround * cote_finale
        # so ratio = cote_finale / (1 / normalised_prob) = cote_finale * normalised_prob
        # equivalently: ratio = cote_finale / (overround / raw_implied)
        # simplifies to: ratio = raw_implied * cote_finale / overround ... but raw_implied = 1/cote
        # so ratio = (1/cote * cote) / overround = 1/overround ... NO
        # Correct: fair_odds = 1/normalised_prob, odds_vs_implied = actual_odds / fair_odds
        odds_vs: Optional[float] = None
        if uid in normalised and normalised[uid] > 0 and cote is not None and cote > 0:
            fair_odds = 1.0 / normalised[uid]
            odds_vs = round(cote / fair_odds, 6)

        results.append({
            "partant_uid": uid,
            "market_entropy": entropy,
            "market_overround": round(overround, 6) if overround is not None else None,
            "implied_probability": impl_prob,
            "odds_vs_implied": odds_vs,
            "favourite_strength": fav_strength,
            "field_competitiveness": competitiveness,
        })

    return results


# ===========================================================================
# PUBLIC API  (matches pattern of other feature builders)
# ===========================================================================


def build_market_entropy_features(partants: list[dict]) -> list[dict]:
    """Build market-entropy features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records.  Expected fields: partant_uid, course_uid,
        cote_finale.

    Returns
    -------
    list[dict]
        One dict per partant_uid with market-entropy features.
    """
    course_groups: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        course_groups[p.get("course_uid", "")].append(p)

    results: list[dict] = []
    for runners in course_groups.values():
        results.extend(_build_race_features(runners))

    return results


# ===========================================================================
# STREAMING CLI  (line-by-line, RAM-friendly)
# ===========================================================================


def stream_from_jsonl(input_path: str, output_dir: str,
                      logger: logging.Logger | None = None) -> int:
    """Stream partants_master.jsonl, buffer by course_uid, compute features
    and write output JSONL.

    Keeps only the fields needed (partant_uid, course_uid, cote_finale) to
    cap RAM at ~200 bytes per partant (~20 runners per course).

    Returns the number of feature records written.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "market_entropy_features.jsonl")

    KEEP_FIELDS = ("partant_uid", "course_uid", "cote_finale")

    # Pass 1: stream & group
    course_groups: dict[str, list[dict]] = defaultdict(list)
    n_read = 0
    with open(input_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            slim = {k: row.get(k) for k in KEEP_FIELDS}
            course_groups[slim.get("course_uid", "")].append(slim)
            n_read += 1

    logger.info("Read %d partants across %d races.", n_read, len(course_groups))

    # Pass 2: compute & write
    all_results: list[dict] = []
    for runners in course_groups.values():
        all_results.extend(_build_race_features(runners))

    save_jsonl(all_results, out_path, logger)
    logger.info("Wrote %d feature records to %s", len(all_results), out_path)
    return len(all_results)


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    logger = setup_logging("market_entropy_features")

    parser = argparse.ArgumentParser(
        description="Compute market-entropy features from partants_master.jsonl"
    )
    parser.add_argument("--input", default=INPUT_DEFAULT,
                        help="Path to partants_master.jsonl")
    parser.add_argument("--output", default=OUTPUT_DIR_DEFAULT,
                        help="Output directory for features JSONL")
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        logger.error("Input file not found: %s", args.input)
        sys.exit(1)

    import time
    t0 = time.time()
    n = stream_from_jsonl(args.input, args.output, logger)
    elapsed = time.time() - t0
    logger.info("Done -- %d records in %.1fs", n, elapsed)


if __name__ == "__main__":
    main()
