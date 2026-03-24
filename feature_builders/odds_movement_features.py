"""
feature_builders.odds_movement_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Odds movement / market-shift features per partant.

Features
--------
- odds_drift_pct       : % change from opening to final odds (positive = drifted out)
- odds_steam_pct       : % shortening (clamped to negative drift, 0 when drifted out)
- is_market_mover      : bool, abs odds change > 20 % from opening
- odds_rank_change     : change in rank position (negative = moved up in favouritism)
- market_confidence    : share of implied probability vs field (odds-implied)

Input: partants_master.jsonl — uses cote_reference (opening), cote_finale (final),
       proba_implicite, course_uid, partant_uid.

Streams line-by-line to keep RAM under 3 GB.

Usage:
    python feature_builders/odds_movement_features.py
    python feature_builders/odds_movement_features.py --input data_master/partants_master.jsonl
    python feature_builders/odds_movement_features.py --output output/odds_movement_features
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = os.path.join("data_master", "partants_master.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "odds_movement_features")

MARKET_MOVER_THRESHOLD = 0.20  # 20 %

# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_pct_change(opening: float, final: float) -> Optional[float]:
    """Percentage change from opening to final.  Positive = drifted out."""
    if opening is None or final is None or opening <= 0:
        return None
    return round((final - opening) / opening, 6)


def _rank_list(uid_odds: list[tuple[str, float]]) -> dict[str, int]:
    """Return {partant_uid: 1-based rank} sorted by odds ascending (fav=1)."""
    sorted_pairs = sorted(uid_odds, key=lambda x: x[1])
    return {uid: rank for rank, (uid, _) in enumerate(sorted_pairs, 1)}


# ===========================================================================
# BUILDER  (race-level, called per course_uid group)
# ===========================================================================


def _build_race_features(runners: list[dict]) -> list[dict]:
    """Compute odds-movement features for all runners in a single race.

    Parameters
    ----------
    runners : list[dict]
        Partant dicts sharing the same course_uid.

    Returns
    -------
    list[dict]
        One feature dict per partant_uid.
    """
    # Collect opening / final odds per runner
    opening_pairs: list[tuple[str, float]] = []
    final_pairs: list[tuple[str, float]] = []

    for p in runners:
        uid = p.get("partant_uid")
        cote_open = p.get("cote_reference")
        cote_final = p.get("cote_finale")
        if uid and cote_open is not None and cote_open > 0:
            opening_pairs.append((uid, cote_open))
        if uid and cote_final is not None and cote_final > 0:
            final_pairs.append((uid, cote_final))

    opening_ranks = _rank_list(opening_pairs) if opening_pairs else {}
    final_ranks = _rank_list(final_pairs) if final_pairs else {}

    # Sum of implied probabilities (for market_confidence denominator)
    total_implied_prob = 0.0
    runner_implied: dict[str, float] = {}
    for p in runners:
        uid = p.get("partant_uid")
        cote = p.get("cote_finale") or p.get("cote_reference")
        if uid and cote is not None and cote > 0:
            imp = 1.0 / cote
            runner_implied[uid] = imp
            total_implied_prob += imp

    results = []
    for p in runners:
        uid = p.get("partant_uid")
        cote_open = p.get("cote_reference")
        cote_final = p.get("cote_finale")

        # 1) odds_drift_pct
        drift = _safe_pct_change(cote_open, cote_final)

        # 2) odds_steam_pct  (only negative drift = steamed in; 0 otherwise)
        steam: Optional[float] = None
        if drift is not None:
            steam = round(min(drift, 0.0), 6)

        # 3) is_market_mover
        is_mm: Optional[bool] = None
        if drift is not None:
            is_mm = abs(drift) > MARKET_MOVER_THRESHOLD

        # 4) odds_rank_change  (final_rank - opening_rank; negative = moved up)
        rank_change: Optional[int] = None
        if uid in opening_ranks and uid in final_ranks:
            rank_change = final_ranks[uid] - opening_ranks[uid]

        # 5) market_confidence  (implied prob share vs field)
        confidence: Optional[float] = None
        if uid in runner_implied and total_implied_prob > 0:
            confidence = round(runner_implied[uid] / total_implied_prob, 6)

        results.append({
            "partant_uid": uid,
            "odds_drift_pct": drift,
            "odds_steam_pct": steam,
            "is_market_mover": is_mm,
            "odds_rank_change": rank_change,
            "market_confidence": confidence,
        })

    return results


# ===========================================================================
# PUBLIC API  (matches pattern of other feature builders)
# ===========================================================================


def build_odds_movement_features(partants: list[dict]) -> list[dict]:
    """Build odds-movement features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records.  Expected fields: partant_uid, course_uid,
        cote_reference, cote_finale, proba_implicite.

    Returns
    -------
    list[dict]
        One dict per partant_uid with odds-movement features.
    """
    # Group by course_uid
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
    """Stream partants_master.jsonl, group by course_uid, compute features
    and write output JSONL.  Two-pass approach:

    Pass 1 — read lines, group minimal data by course_uid (keeps only the
    fields needed: partant_uid, course_uid, cote_reference, cote_finale,
    proba_implicite).  This caps RAM to ~500 bytes per partant.

    Pass 2 — compute features per race group, write output.

    Returns the number of feature records written.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "odds_movement_features.jsonl")

    KEEP_FIELDS = ("partant_uid", "course_uid", "cote_reference",
                   "cote_finale", "proba_implicite")

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
    n_written = 0
    with open(out_path, "w", encoding="utf-8", newline="\n") as out:
        for runners in course_groups.values():
            feats = _build_race_features(runners)
            for f in feats:
                out.write(json.dumps(f, ensure_ascii=False) + "\n")
                n_written += 1

    logger.info("Wrote %d feature records to %s", n_written, out_path)
    return n_written


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Compute odds-movement features from partants_master.jsonl"
    )
    parser.add_argument("--input", default=INPUT_DEFAULT,
                        help="Path to partants_master.jsonl")
    parser.add_argument("--output", default=OUTPUT_DIR_DEFAULT,
                        help="Output directory for features JSONL")
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        logger.error("Input file not found: %s", args.input)
        sys.exit(1)

    t0 = time.time()
    n = stream_from_jsonl(args.input, args.output, logger)
    elapsed = time.time() - t0
    logger.info("Done — %d records in %.1fs", n, elapsed)


if __name__ == "__main__":
    import time
    main()
