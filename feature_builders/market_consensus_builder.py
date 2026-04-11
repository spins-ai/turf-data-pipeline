#!/usr/bin/env python3
"""
feature_builders.market_consensus_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Analyzes consensus between different market signals (cote_finale,
cote_reference, pronostics) to derive race-level and per-partant
market-structure features.

Two-pass approach:
  Pass 1 — stream partants_master.jsonl, group minimal fields by course_uid.
  Pass 2 — compute 8 consensus features per partant.

Features
--------
  mc_odds_rank          : rank of this horse by cote_finale within the race
                          (1 = favourite, i.e. lowest odds)
  mc_odds_rank_pct      : odds_rank / nombre_partants  (0 = fav, 1 = outsider)
  mc_is_favorite        : 1 if this horse has the lowest cote_finale in race
  mc_is_second_fav      : 1 if this horse has the second-lowest cote_finale
  mc_odds_gap_to_fav    : (this odds - fav odds) / fav odds
                          relative distance from the favourite (0 for fav itself)
  mc_odds_gap_to_next   : (this odds - next-lower odds) / this odds
                          proximity to the horse just below in the odds ladder
                          (0 for the horse with no one below = favourite)
  mc_fav_strength       : fav implied_prob / second_fav implied_prob
                          values > 2 indicate a dominant favourite
  mc_odds_cluster       : count of horses with cote_finale within ±20 % of
                          this horse's odds (including itself; contested market
                          when cluster is large)

Input  : D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
Output : D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_consensus/
         market_consensus.jsonl

Usage:
    python feature_builders/market_consensus_builder.py
    python feature_builders/market_consensus_builder.py --input <path>
    python feature_builders/market_consensus_builder.py --output-dir <dir>
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict
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
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_consensus"
)

_LOG_EVERY = 500_000

# Cluster window: horses within this fraction of a horse's own odds are counted
_CLUSTER_WINDOW = 0.20  # ±20 %

# Fields extracted during pass-1 (keeps RAM minimal)
_KEEP_FIELDS = (
    "partant_uid",
    "course_uid",
    "num_pmu",
    "nombre_partants",
    "cote_finale",
    "cote_reference",
    "date_reunion_iso",
)


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert *val* to float, return None on failure or non-positive."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _safe_positive(val: Any) -> Optional[float]:
    """Return float only when strictly positive."""
    f = _safe_float(val)
    return f if (f is not None and f > 0.0) else None


# ===========================================================================
# PASS-2: FEATURE COMPUTATION  (one race at a time)
# ===========================================================================


def _build_race_features(runners: list[dict]) -> list[dict]:
    """Compute market-consensus features for all runners in one race.

    Parameters
    ----------
    runners : list[dict]
        Slim dicts (keys from _KEEP_FIELDS) sharing the same course_uid.

    Returns
    -------
    list[dict]
        One feature dict per partant_uid.
    """
    # Collect (partant_uid, cote_finale) for every runner with a valid odds
    uid_odds: list[tuple[str, float]] = []
    for p in runners:
        uid = p.get("partant_uid")
        cote = _safe_positive(p.get("cote_finale"))
        if uid and cote is not None:
            uid_odds.append((uid, cote))

    n_valid = len(uid_odds)

    # Sort ascending by odds: index 0 = favourite
    uid_odds_sorted = sorted(uid_odds, key=lambda x: x[1])

    # Build lookup: uid -> (0-based sorted index, cote)
    rank_map: dict[str, tuple[int, float]] = {}
    for idx, (uid, cote) in enumerate(uid_odds_sorted):
        rank_map[uid] = (idx, cote)

    # Favourite and second favourite
    fav_cote = uid_odds_sorted[0][1] if n_valid >= 1 else None
    sec_cote = uid_odds_sorted[1][1] if n_valid >= 2 else None

    # Race-level: favourite strength
    mc_fav_strength: Optional[float] = None
    if fav_cote is not None and sec_cote is not None and fav_cote > 0 and sec_cote > 0:
        # implied_prob = 1 / cote
        fav_imp = 1.0 / fav_cote
        sec_imp = 1.0 / sec_cote
        if sec_imp > 0:
            mc_fav_strength = round(fav_imp / sec_imp, 4)

    results: list[dict] = []

    for p in runners:
        uid = p.get("partant_uid")
        nb_raw = _safe_positive(p.get("nombre_partants"))
        nb_partants = int(nb_raw) if nb_raw is not None else n_valid
        if nb_partants <= 0:
            nb_partants = max(n_valid, 1)

        cote = _safe_positive(p.get("cote_finale"))

        if uid not in rank_map or cote is None:
            # Horse has no valid odds — return nulls
            results.append(
                {
                    "partant_uid": uid,
                    "mc_odds_rank": None,
                    "mc_odds_rank_pct": None,
                    "mc_is_favorite": None,
                    "mc_is_second_fav": None,
                    "mc_odds_gap_to_fav": None,
                    "mc_odds_gap_to_next": None,
                    "mc_fav_strength": mc_fav_strength,
                    "mc_odds_cluster": None,
                }
            )
            continue

        zero_idx, _ = rank_map[uid]
        rank_1based = zero_idx + 1  # 1-indexed

        # mc_odds_rank
        mc_odds_rank = rank_1based

        # mc_odds_rank_pct  — 0 for fav, approaching 1 for outsider
        mc_odds_rank_pct = round((rank_1based - 1) / nb_partants, 4)

        # mc_is_favorite
        mc_is_favorite = 1 if zero_idx == 0 else 0

        # mc_is_second_fav
        mc_is_second_fav = 1 if zero_idx == 1 else 0

        # mc_odds_gap_to_fav : (this - fav) / fav
        mc_odds_gap_to_fav: Optional[float] = None
        if fav_cote is not None and fav_cote > 0:
            mc_odds_gap_to_fav = round((cote - fav_cote) / fav_cote, 4)

        # mc_odds_gap_to_next : (this - next_lower) / this
        # "next lower" = the horse with next smaller cote (index zero_idx - 1)
        mc_odds_gap_to_next: Optional[float] = None
        if zero_idx > 0 and cote > 0:
            next_lower_cote = uid_odds_sorted[zero_idx - 1][1]
            mc_odds_gap_to_next = round((cote - next_lower_cote) / cote, 4)
        else:
            # Favourite has no horse below it
            mc_odds_gap_to_next = 0.0

        # mc_odds_cluster : count horses (including self) within ±20 % of this cote
        lower_bound = cote * (1.0 - _CLUSTER_WINDOW)
        upper_bound = cote * (1.0 + _CLUSTER_WINDOW)
        mc_odds_cluster = sum(
            1 for _, other_cote in uid_odds if lower_bound <= other_cote <= upper_bound
        )

        results.append(
            {
                "partant_uid": uid,
                "mc_odds_rank": mc_odds_rank,
                "mc_odds_rank_pct": mc_odds_rank_pct,
                "mc_is_favorite": mc_is_favorite,
                "mc_is_second_fav": mc_is_second_fav,
                "mc_odds_gap_to_fav": mc_odds_gap_to_fav,
                "mc_odds_gap_to_next": mc_odds_gap_to_next,
                "mc_fav_strength": mc_fav_strength,
                "mc_odds_cluster": mc_odds_cluster,
            }
        )

    return results


# ===========================================================================
# PUBLIC API
# ===========================================================================


def build_market_consensus_features(partants: list[dict]) -> list[dict]:
    """Compute market-consensus features for every partant record.

    Parameters
    ----------
    partants : list[dict]
        All partant records.  Expected fields: partant_uid, course_uid,
        nombre_partants, cote_finale.

    Returns
    -------
    list[dict]
        One feature dict per record (same order as input).
    """
    course_groups: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        course_groups[p.get("course_uid", "")].append(p)

    results: list[dict] = []
    for runners in course_groups.values():
        results.extend(_build_race_features(runners))
    return results


# ===========================================================================
# STREAMING CLI  (two-pass, RAM-friendly)
# ===========================================================================


def stream_from_jsonl(
    input_path: Path,
    output_dir: Path,
    logger: Any,
) -> int:
    """Stream partants_master.jsonl, compute market-consensus features, write output.

    Pass 1 — Read file, keep only _KEEP_FIELDS per line, group by course_uid.
    Pass 2 — Compute features per race group, write output JSONL.

    Returns
    -------
    int
        Number of feature records written.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "market_consensus.jsonl"

    # ------------------------------------------------------------------
    # Pass 1: stream & group
    # ------------------------------------------------------------------
    logger.info("Pass 1: streaming %s", input_path)
    course_groups: dict[str, list[dict]] = defaultdict(list)
    n_read = 0
    n_skip = 0

    with open(input_path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                row = json.loads(raw_line)
            except json.JSONDecodeError:
                n_skip += 1
                continue

            slim = {k: row.get(k) for k in _KEEP_FIELDS}
            cuid = slim.get("course_uid") or ""
            course_groups[cuid].append(slim)
            n_read += 1

            if n_read % _LOG_EVERY == 0:
                logger.info(
                    "  Pass 1: %d lignes lues, %d courses",
                    n_read,
                    len(course_groups),
                )

    logger.info(
        "Pass 1 terminé: %d partants, %d courses, %d lignes ignorées",
        n_read,
        len(course_groups),
        n_skip,
    )

    # ------------------------------------------------------------------
    # Pass 2: compute features & write
    # ------------------------------------------------------------------
    logger.info("Pass 2: calcul des features et écriture vers %s", out_path)
    n_written = 0
    n_races = 0

    with open(out_path, "w", encoding="utf-8", newline="\n") as fout:
        for cuid, runners in course_groups.items():
            feats = _build_race_features(runners)
            for feat in feats:
                fout.write(json.dumps(feat, ensure_ascii=False) + "\n")
                n_written += 1
            n_races += 1

            if n_races % 10_000 == 0:
                logger.info(
                    "  Pass 2: %d courses traitées, %d features écrites",
                    n_races,
                    n_written,
                )

    logger.info(
        "Pass 2 terminé: %d features écrites pour %d courses",
        n_written,
        n_races,
    )

    # Free memory before returning
    del course_groups
    gc.collect()

    return n_written


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    logger = setup_logging("market_consensus_builder")
    logger.info("=" * 70)
    logger.info("market_consensus_builder.py")
    logger.info("=" * 70)

    parser = argparse.ArgumentParser(
        description="Market consensus features: rank, gap, cluster from cote_finale"
    )
    parser.add_argument(
        "--input",
        default=str(INPUT_PARTANTS),
        help="Path to partants_master.jsonl  (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Output directory  (default: %(default)s)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    t0 = time.time()
    n = stream_from_jsonl(input_path, output_dir, logger)
    elapsed = time.time() - t0

    logger.info("Done — %d records écrits en %.1f s", n, elapsed)


if __name__ == "__main__":
    main()
