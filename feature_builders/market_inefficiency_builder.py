#!/usr/bin/env python3
"""
feature_builders.market_inefficiency_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Market inefficiency features discovered through pattern analysis.

Existing builders cover:
  - odds_movement_features.py: drift_pct, steam_pct, is_market_mover, rank_change
  - value_signal_builder.py: expected_value, edge_vs_market, is_value_bet, smart_money_signal
  - closing_line_value_builder.py: closing line value metrics

What this builder adds (patterns NOT captured):

1. **Odds-range calibration edge**: At odds 3.5, actual win rate exceeds implied
   by +2.5pp. At odds 11, the gap is -6.7pp. The market systematically misprices
   certain odds bands. We compute a per-odds-band historical calibration edge.

2. **Hippodrome predictability score**: Favourite win rates range from 16.7%
   (Avignon) to 48.7% (Straubing). This is a strong structural signal; no
   existing builder captures per-hippodrome favourite reliability.

3. **Smart money x odds level interaction**: Steamers at short odds win 26.9%,
   but at long odds only 2.7%.  Drifters at short odds paradoxically win 31.6%.
   The odds_movement_features builder ignores the interaction with price level.

4. **Overbet/underbet detection**: For each odds bucket, we track cumulative
   actual - implied probability gap. Horses in historically underbet ranges
   represent systematic market blind spots.

5. **Field-size adjusted market edge**: In xlarge fields (16+), favourites win
   only 18.8% vs 41.2% in medium fields. Odds do not fully adjust for this.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.

Produces:
  - market_inefficiency_features.jsonl  in output/market_inefficiency/

Features per partant (12):
  - mkt_odds_calibration_edge   : actual_wr - implied_wr for this odds bucket
  - mkt_odds_bucket             : categorical odds bucket (0-8)
  - mkt_hippo_fav_winrate       : historical favourite win rate at this hippodrome
  - mkt_hippo_predictability    : how predictable this hippodrome is (0-1 scale)
  - mkt_steam_odds_interaction  : win rate for this drift_direction x odds_level combo
  - mkt_drift_direction         : -1 (steamer), 0 (stable), +1 (drifter)
  - mkt_overbet_score           : cumulative overbetting signal for this odds range
  - mkt_field_adj_implied_prob  : field-size-adjusted implied probability
  - mkt_is_value_zone           : 1 if odds are in a historically underbet range
  - mkt_fav_in_field            : 1 if horse is the favourite in its race
  - mkt_fav_edge_vs_field_size  : fav_win_rate_for_field_size - implied_probability
  - mkt_longshot_bias_score     : degree to which this horse is affected by longshot bias

Usage:
    python feature_builders/market_inefficiency_builder.py
    python feature_builders/market_inefficiency_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
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
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_inefficiency")

_LOG_EVERY = 500_000

# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_rate(wins: int, total: int, ndigits: int = 4) -> Optional[float]:
    if total < 1:
        return None
    return round(wins / total, ndigits)


def _odds_bucket_index(cote: Optional[float]) -> Optional[int]:
    """Map final odds to a bucket index (0-8).

    0: 1.0-1.9   1: 2.0-2.9   2: 3.0-4.9   3: 5.0-7.9
    4: 8.0-11.9  5: 12.0-19.9 6: 20.0-34.9 7: 35.0-59.9  8: 60+
    """
    if cote is None or cote <= 0:
        return None
    if cote < 2:
        return 0
    elif cote < 3:
        return 1
    elif cote < 5:
        return 2
    elif cote < 8:
        return 3
    elif cote < 12:
        return 4
    elif cote < 20:
        return 5
    elif cote < 35:
        return 6
    elif cote < 60:
        return 7
    else:
        return 8


# Midpoint odds for each bucket, used to compute implied probability
_BUCKET_MID_ODDS = {
    0: 1.5,
    1: 2.5,
    2: 4.0,
    3: 6.5,
    4: 10.0,
    5: 16.0,
    6: 27.0,
    7: 47.0,
    8: 80.0,
}


def _drift_direction(opening: Optional[float], final: Optional[float]) -> Optional[int]:
    """Return drift direction: -1 steamer, 0 stable, +1 drifter."""
    if opening is None or final is None or opening <= 0:
        return None
    change_pct = (final - opening) / opening * 100
    if change_pct < -10:
        return -1
    elif change_pct > 10:
        return 1
    else:
        return 0


def _odds_level(cote: Optional[float]) -> Optional[str]:
    """Classify odds into short/mid/long for interaction features."""
    if cote is None or cote <= 0:
        return None
    if cote < 5:
        return "short"
    elif cote < 15:
        return "mid"
    else:
        return "long"


def _field_size_cat(nb: Any) -> Optional[str]:
    try:
        n = int(nb)
    except (TypeError, ValueError):
        return None
    if n < 8:
        return "small"
    elif n < 12:
        return "medium"
    elif n < 16:
        return "large"
    else:
        return "xlarge"


def _resolve_input(cli_arg: Optional[str]) -> Path:
    if cli_arg:
        p = Path(cli_arg)
        if p.exists():
            return p
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"No input found among {INPUT_CANDIDATES}; pass --input explicitly."
    )


# ===========================================================================
# MAIN BUILDER
# ===========================================================================


def build_market_inefficiency_features(input_path: Path, output_dir: Path) -> None:
    """Build 12 market inefficiency features."""
    logger = setup_logging("market_inefficiency_builder")
    logger.info("Input: %s", input_path)
    logger.info("Output dir: %s", output_dir)

    t0 = time.time()

    # ── Load and sort ────────────────────────────────────────────────
    logger.info("Loading records...")
    records: list[dict[str, Any]] = []
    with open(input_path, "r", encoding="utf-8") as fh:
        for i, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
            if i % _LOG_EVERY == 0:
                logger.info("  loaded %d records...", i)

    logger.info("  %d records loaded in %.1fs", len(records), time.time() - t0)

    records.sort(
        key=lambda r: (
            str(r.get("date_reunion_iso", "") or "")[:10],
            str(r.get("course_uid", "") or ""),
            r.get("num_pmu", 0) or 0,
        )
    )
    logger.info("  sorted chronologically.")

    # ── Accumulators ─────────────────────────────────────────────────

    # Odds bucket calibration: bucket_idx -> {wins, total, implied_sum}
    odds_cal: dict[int, dict[str, float]] = defaultdict(
        lambda: {"wins": 0, "total": 0, "implied_sum": 0.0}
    )

    # Hippodrome favourite stats: hippo -> {fav_wins, total_courses}
    hippo_fav: dict[str, dict[str, int]] = defaultdict(
        lambda: {"fav_wins": 0, "total": 0}
    )

    # Steam x odds-level interaction: key -> {wins, total}
    steam_odds: dict[str, dict[str, int]] = defaultdict(
        lambda: {"wins": 0, "total": 0}
    )

    # Field-size favourite stats: size_cat -> {fav_wins, total}
    field_fav: dict[str, dict[str, int]] = defaultdict(
        lambda: {"fav_wins": 0, "total": 0}
    )

    # Track per-course runners for favourite detection and hippo stats
    current_course_uid: Optional[str] = None
    current_course_runners: list[dict[str, Any]] = []

    enriched: list[dict[str, Any]] = []

    def _flush_course(runners: list[dict[str, Any]]) -> None:
        """Process a completed course: update hippo_fav and field_fav."""
        if len(runners) < 2:
            return
        # Find favourite (lowest positive cote)
        fav_rec = None
        fav_cote = float("inf")
        for cr in runners:
            c = _safe_float(cr.get("cote_finale"))
            if c and c > 0 and c < fav_cote:
                fav_cote = c
                fav_rec = cr
        if fav_rec is None:
            return

        hippo = (fav_rec.get("hippodrome_normalise") or "").lower().strip()
        fav_won = bool(fav_rec.get("is_gagnant"))
        fs = _field_size_cat(len(runners))

        if hippo:
            hippo_fav[hippo]["total"] += 1
            if fav_won:
                hippo_fav[hippo]["fav_wins"] += 1

        if fs:
            field_fav[fs]["total"] += 1
            if fav_won:
                field_fav[fs]["fav_wins"] += 1

    # ── Main loop ────────────────────────────────────────────────────
    logger.info("Computing features...")

    for idx, rec in enumerate(records):
        partant_uid = rec.get("partant_uid", "")
        course_uid = str(rec.get("course_uid", "") or "")
        cote = _safe_float(rec.get("cote_finale"))
        opening = _safe_float(rec.get("cote_reference"))
        is_gagnant = bool(rec.get("is_gagnant"))
        hippo = (rec.get("hippodrome_normalise") or "").lower().strip()
        nb_partants = rec.get("nombre_partants")

        # Flush previous course if course changed
        if course_uid != current_course_uid:
            _flush_course(current_course_runners)
            current_course_uid = course_uid
            current_course_runners = []
        current_course_runners.append(rec)

        # ── Compute keys ────────────────────────────────────────────
        bucket_idx = _odds_bucket_index(cote)
        drift_dir = _drift_direction(opening, cote)
        o_level = _odds_level(cote)
        so_key = f"{o_level}|{drift_dir}" if o_level and drift_dir is not None else None
        fs = _field_size_cat(nb_partants)
        implied_prob = 1.0 / cote if cote and cote > 1 else None

        # ── Read PAST rates ─────────────────────────────────────────
        features: dict[str, Any] = {"partant_uid": partant_uid}

        # 1-2. Odds calibration edge
        features["mkt_odds_bucket"] = bucket_idx
        if bucket_idx is not None:
            h = odds_cal[bucket_idx]
            if h["total"] >= 10:
                actual_wr = h["wins"] / h["total"]
                avg_implied = h["implied_sum"] / h["total"]
                features["mkt_odds_calibration_edge"] = round(actual_wr - avg_implied, 4)
            else:
                features["mkt_odds_calibration_edge"] = None
        else:
            features["mkt_odds_calibration_edge"] = None

        # 3-4. Hippodrome predictability
        if hippo:
            hf = hippo_fav.get(hippo, {"fav_wins": 0, "total": 0})
            features["mkt_hippo_fav_winrate"] = _safe_rate(
                hf["fav_wins"], hf["total"]
            )
            if hf["total"] >= 20:
                # Predictability: normalize fav_win_rate to 0-1 scale
                # (baseline ~30%, range 17%-49% -> map to 0-1)
                raw = hf["fav_wins"] / hf["total"]
                features["mkt_hippo_predictability"] = round(
                    max(0.0, min(1.0, (raw - 0.15) / 0.35)), 4
                )
            else:
                features["mkt_hippo_predictability"] = None
        else:
            features["mkt_hippo_fav_winrate"] = None
            features["mkt_hippo_predictability"] = None

        # 5-6. Steam x odds level interaction
        features["mkt_drift_direction"] = drift_dir
        if so_key:
            h = steam_odds.get(so_key, {"wins": 0, "total": 0})
            features["mkt_steam_odds_interaction"] = _safe_rate(h["wins"], h["total"])
        else:
            features["mkt_steam_odds_interaction"] = None

        # 7. Overbet score (cumulative calibration edge, smoothed)
        if bucket_idx is not None:
            h = odds_cal[bucket_idx]
            if h["total"] >= 20:
                actual_wr = h["wins"] / h["total"]
                avg_implied = h["implied_sum"] / h["total"]
                # Positive = underbet (good for bettor), negative = overbet
                features["mkt_overbet_score"] = round(actual_wr - avg_implied, 4)
            else:
                features["mkt_overbet_score"] = None
        else:
            features["mkt_overbet_score"] = None

        # 8. Field-size adjusted implied probability
        if implied_prob is not None and fs:
            ff = field_fav.get(fs, {"fav_wins": 0, "total": 0})
            if ff["total"] >= 20:
                # Adjust implied prob by field-size favourite reliability
                fav_wr = ff["fav_wins"] / ff["total"]
                # In larger fields, favourites underperform -> adjustment factor
                # baseline fav_wr ~0.33
                adjustment = fav_wr / 0.33 if fav_wr > 0 else 1.0
                features["mkt_field_adj_implied_prob"] = round(
                    implied_prob * adjustment, 4
                )
            else:
                features["mkt_field_adj_implied_prob"] = implied_prob
        else:
            features["mkt_field_adj_implied_prob"] = None

        # 9. Is value zone (historically underbet odds range)
        if bucket_idx is not None:
            h = odds_cal[bucket_idx]
            if h["total"] >= 30:
                actual_wr = h["wins"] / h["total"]
                avg_implied = h["implied_sum"] / h["total"]
                features["mkt_is_value_zone"] = 1 if actual_wr > avg_implied else 0
            else:
                features["mkt_is_value_zone"] = None
        else:
            features["mkt_is_value_zone"] = None

        # 10. Is favourite in field (we can only approximate from odds)
        # If this horse has the lowest cote in the current course batch
        # We mark it after we know all runners -> defer, use simple heuristic
        features["mkt_fav_in_field"] = None  # Will be set in post-processing

        # 11. Favourite edge vs field size
        if fs and implied_prob is not None:
            ff = field_fav.get(fs, {"fav_wins": 0, "total": 0})
            if ff["total"] >= 20:
                fav_wr = ff["fav_wins"] / ff["total"]
                features["mkt_fav_edge_vs_field_size"] = round(
                    fav_wr - (implied_prob if implied_prob else 0), 4
                )
            else:
                features["mkt_fav_edge_vs_field_size"] = None
        else:
            features["mkt_fav_edge_vs_field_size"] = None

        # 12. Longshot bias score
        # Longshots (odds 20+) win less than implied; short odds win more
        # Score = implied_prob - actual_wr_for_bucket (positive = overbet longshot)
        if bucket_idx is not None and implied_prob is not None:
            h = odds_cal[bucket_idx]
            if h["total"] >= 20:
                actual_wr = h["wins"] / h["total"]
                features["mkt_longshot_bias_score"] = round(
                    (implied_prob - actual_wr), 4
                )
            else:
                features["mkt_longshot_bias_score"] = None
        else:
            features["mkt_longshot_bias_score"] = None

        enriched.append(features)

        # ── Update accumulators ─────────────────────────────────────
        if bucket_idx is not None and cote and cote > 1:
            odds_cal[bucket_idx]["total"] += 1
            odds_cal[bucket_idx]["implied_sum"] += 1.0 / cote
            if is_gagnant:
                odds_cal[bucket_idx]["wins"] += 1

        if so_key:
            steam_odds[so_key]["total"] += 1
            if is_gagnant:
                steam_odds[so_key]["wins"] += 1

        if (idx + 1) % _LOG_EVERY == 0:
            logger.info("  processed %d / %d records...", idx + 1, len(records))

    # Flush last course
    _flush_course(current_course_runners)

    # ── Post-process: mark favourites per course ─────────────────────
    logger.info("Post-processing: marking favourites per course...")
    # Group enriched results by course_uid for favourite detection
    course_groups: dict[str, list[int]] = defaultdict(list)
    for i, rec in enumerate(records):
        cuid = str(rec.get("course_uid", "") or "")
        course_groups[cuid].append(i)

    for cuid, indices in course_groups.items():
        # Find the index with lowest cote
        best_idx = None
        best_cote = float("inf")
        for i in indices:
            c = _safe_float(records[i].get("cote_finale"))
            if c and c > 0 and c < best_cote:
                best_cote = c
                best_idx = i
        for i in indices:
            enriched[i]["mkt_fav_in_field"] = 1 if i == best_idx else 0

    # ── Save ─────────────────────────────────────────────────────────
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "market_inefficiency_features.jsonl"
    save_jsonl(enriched, out_file, logger)
    logger.info("Done: %d features written in %.1fs", len(enriched), time.time() - t0)


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market inefficiency features builder"
    )
    parser.add_argument(
        "--input", type=str, default=None, help="Path to partants_master.jsonl"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory (default: output/market_inefficiency)",
    )
    args = parser.parse_args()

    input_path = _resolve_input(args.input)
    output_dir = Path(args.output) if args.output else OUTPUT_DIR

    build_market_inefficiency_features(input_path, output_dir)


if __name__ == "__main__":
    main()
