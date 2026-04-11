#!/usr/bin/env python3
"""
feature_builders.market_efficiency_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Market efficiency features detecting how well-calibrated the betting
market is.

Reads partants_master.jsonl in streaming mode, indexes + sorts
chronologically, then processes course-by-course with snapshot-before-
update semantics to avoid future leakage.

Temporal integrity: for any partant at date D, only races with date < D
contribute to rolling statistics -- no future leakage.

Produces:
  - market_efficiency.jsonl   in builder_outputs/market_efficiency/

Features per partant (8):
  - mef_market_overround        : sum(1/cote) for all runners in this race
  - mef_horse_market_share      : (1/cote) / overround -- corrected market probability
  - mef_is_overlay              : 1 if implied prob < horse historical win rate (value)
  - mef_is_underlay             : 1 if implied prob > horse historical win rate * 1.2
  - mef_fav_reliability_score   : how often the favourite wins at this hippodrome (rolling)
  - mef_odds_bracket_edge       : historical profit/loss per unit bet in this odds bracket
  - mef_kelly_fraction          : (wr * cote - 1) / (cote - 1) capped [0, 0.25]
  - mef_expected_value          : wr * cote_finale -- EV per unit bet (>1 = positive EV)

Usage:
    python feature_builders/market_efficiency_builder.py
    python feature_builders/market_efficiency_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_efficiency")

_LOG_EVERY = 500_000

# Kelly cap
_KELLY_CAP = 0.25

# Bayesian prior for horse win rate (shrinkage toward global mean)
_PRIOR_WEIGHT = 10
_GLOBAL_WIN_RATE = 0.08  # ~8% baseline (approx 1/12 runners)

# Minimum races for hippodrome favourite stats to be meaningful
_MIN_HIPPO_RACES = 20

# Odds bracket width (e.g. 2.0 means [2,4), [4,6), ...)
_BRACKET_WIDTH = 2.0


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
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    if v is None:
        return None
    try:
        val = float(v)
        return val if val == val else None  # NaN check
    except (TypeError, ValueError):
        return None


def _odds_bracket(cote: float) -> int:
    """Map a cote to an integer bracket index.

    E.g. with _BRACKET_WIDTH = 2:  [1,3) -> 1, [3,5) -> 3, [5,7) -> 5, ...
    """
    return max(1, int(cote / _BRACKET_WIDTH) * int(_BRACKET_WIDTH))


def _bayes_wr(wins: int, total: int) -> float:
    """Bayesian-shrinkage win rate toward global mean."""
    return (_GLOBAL_WIN_RATE * _PRIOR_WEIGHT + wins) / (_PRIOR_WEIGHT + total)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_market_efficiency(input_path: Path, output_path: Path, logger) -> int:
    """Build market efficiency features -- two-phase index+sort then course-by-course."""
    logger.info("=== Market Efficiency Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Read minimal fields into slim records
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("rapport_final"))

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "hippo": (rec.get("hippodrome_normalise") or "").strip().lower(),
            "cote": cote,
            "is_gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)
    gc.collect()

    # ------------------------------------------------------------------
    # Phase 3: Course-by-course processing
    # ------------------------------------------------------------------
    logger.info("Phase 3: traitement course par course...")
    t2 = time.time()

    # --- Global state ---
    # Per-horse: {wins, total}
    horse_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    # Odds bracket PnL: bracket -> [profit, total_bets]
    odds_bracket_pnl: dict[int, list[float]] = defaultdict(lambda: [0.0, 0])
    # Hippodrome favourite stats: hippo -> [fav_wins, total_races]
    hippo_fav_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    fill_counts = {
        "mef_market_overround": 0,
        "mef_horse_market_share": 0,
        "mef_is_overlay": 0,
        "mef_is_underlay": 0,
        "mef_fav_reliability_score": 0,
        "mef_odds_bracket_edge": 0,
        "mef_kelly_fraction": 0,
        "mef_expected_value": 0,
    }

    i = 0
    total = len(slim_records)

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        while i < total:
            course_uid = slim_records[i]["course"]
            course_date = slim_records[i]["date"]
            course_group: list[dict] = []

            while (
                i < total
                and slim_records[i]["course"] == course_uid
                and slim_records[i]["date"] == course_date
            ):
                course_group.append(slim_records[i])
                i += 1

            if not course_group:
                continue

            # ----- Compute race-level: overround -----
            implied_probs: list[Optional[float]] = []
            for rec in course_group:
                c = rec["cote"]
                if c is not None and c > 0:
                    implied_probs.append(1.0 / c)
                else:
                    implied_probs.append(None)

            overround = sum(p for p in implied_probs if p is not None)

            # Identify favourite (lowest cote)
            fav_idx: Optional[int] = None
            fav_cote = float("inf")
            for idx, rec in enumerate(course_group):
                c = rec["cote"]
                if c is not None and 0 < c < fav_cote:
                    fav_cote = c
                    fav_idx = idx

            hippo = course_group[0]["hippo"]

            # ----- Snapshot BEFORE update, emit features -----
            post_updates: list[dict] = []

            for idx, rec in enumerate(course_group):
                h = rec["cheval"]
                c = rec["cote"]
                ip = implied_probs[idx]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                # 1. mef_market_overround
                if overround > 0:
                    features["mef_market_overround"] = round(overround, 4)
                    fill_counts["mef_market_overround"] += 1
                else:
                    features["mef_market_overround"] = None

                # 2. mef_horse_market_share
                if ip is not None and overround > 0:
                    features["mef_horse_market_share"] = round(ip / overround, 6)
                    fill_counts["mef_horse_market_share"] += 1
                else:
                    features["mef_horse_market_share"] = None

                # Horse win rate (snapshot before update)
                wr: Optional[float] = None
                if h:
                    hs = horse_stats[h]
                    if hs[1] > 0:
                        wr = _bayes_wr(hs[0], hs[1])
                    else:
                        wr = None  # no history yet

                # 3. mef_is_overlay
                if ip is not None and wr is not None:
                    features["mef_is_overlay"] = int(ip < wr)
                    fill_counts["mef_is_overlay"] += 1
                else:
                    features["mef_is_overlay"] = None

                # 4. mef_is_underlay
                if ip is not None and wr is not None:
                    features["mef_is_underlay"] = int(ip > wr * 1.2)
                    fill_counts["mef_is_underlay"] += 1
                else:
                    features["mef_is_underlay"] = None

                # 5. mef_fav_reliability_score
                if hippo and hippo in hippo_fav_stats:
                    hfs = hippo_fav_stats[hippo]
                    if hfs[1] >= _MIN_HIPPO_RACES:
                        features["mef_fav_reliability_score"] = round(hfs[0] / hfs[1], 4)
                        fill_counts["mef_fav_reliability_score"] += 1
                    else:
                        features["mef_fav_reliability_score"] = None
                else:
                    features["mef_fav_reliability_score"] = None

                # 6. mef_odds_bracket_edge
                if c is not None and c > 0:
                    bracket = _odds_bracket(c)
                    bp = odds_bracket_pnl.get(bracket)
                    if bp is not None and bp[1] >= 50:
                        features["mef_odds_bracket_edge"] = round(bp[0] / bp[1], 4)
                        fill_counts["mef_odds_bracket_edge"] += 1
                    else:
                        features["mef_odds_bracket_edge"] = None
                else:
                    features["mef_odds_bracket_edge"] = None

                # 7. mef_kelly_fraction: (wr * cote - 1) / (cote - 1) capped [0, 0.25]
                if wr is not None and c is not None and c > 1:
                    raw_kelly = (wr * c - 1.0) / (c - 1.0)
                    features["mef_kelly_fraction"] = round(
                        max(0.0, min(raw_kelly, _KELLY_CAP)), 6
                    )
                    fill_counts["mef_kelly_fraction"] += 1
                else:
                    features["mef_kelly_fraction"] = None

                # 8. mef_expected_value: wr * cote_finale
                if wr is not None and c is not None and c > 0:
                    features["mef_expected_value"] = round(wr * c, 4)
                    fill_counts["mef_expected_value"] += 1
                else:
                    features["mef_expected_value"] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update info
                post_updates.append({
                    "cheval": h,
                    "is_gagnant": rec["is_gagnant"],
                    "cote": c,
                    "is_fav": (idx == fav_idx),
                })

            # ----- Update global state AFTER snapshot -----
            for upd in post_updates:
                h = upd["cheval"]
                if h:
                    horse_stats[h][1] += 1
                    if upd["is_gagnant"]:
                        horse_stats[h][0] += 1

                # Odds bracket PnL update
                c = upd["cote"]
                if c is not None and c > 0:
                    bracket = _odds_bracket(c)
                    odds_bracket_pnl[bracket][1] += 1
                    if upd["is_gagnant"]:
                        # Profit = cote - 1 (net win per unit bet)
                        odds_bracket_pnl[bracket][0] += (c - 1.0)
                    else:
                        # Loss = -1 per unit bet
                        odds_bracket_pnl[bracket][0] -= 1.0

            # Hippodrome favourite update (once per race)
            if hippo and fav_idx is not None:
                hippo_fav_stats[hippo][1] += 1
                if course_group[fav_idx]["is_gagnant"]:
                    hippo_fav_stats[hippo][0] += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Market efficiency build termine: %d features en %.1fs "
        "(chevaux: %d, hippos: %d, brackets: %d)",
        n_written, elapsed,
        len(horse_stats), len(hippo_fav_stats), len(odds_bracket_pnl),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features market efficiency a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/market_efficiency/)",
    )
    args = parser.parse_args()

    logger = setup_logging("market_efficiency_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "market_efficiency.jsonl"
    build_market_efficiency(input_path, out_path, logger)


if __name__ == "__main__":
    main()
