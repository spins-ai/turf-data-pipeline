#!/usr/bin/env python3
"""
feature_builders.betting_edge_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Kelly-criterion and edge-based betting features for Phase 11 (Betting)
and Phase 13 (Bet Sizing) model modules.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant betting-edge features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the Bayesian win rate used as model-probability proxy --
no future leakage.

Produces:
  - betting_edge_features.jsonl   in output/betting_edge/

Features per partant (6):
  - kelly_fraction        : (p*b - q) / b  where p = Bayesian win rate,
                            b = cote_finale - 1 (net payout), q = 1 - p.
                            Clamped to [0, 0.25] (quarter-Kelly cap).
  - edge_percentage       : (p_model - p_market) / p_market * 100
                            Positive = model sees more value than market.
  - edge_consistency      : rolling hit-rate of positive-edge bets over
                            the horse's last 10 races (0..1).
  - kelly_bankroll_pct    : kelly_fraction * edge_consistency
                            Risk-adjusted Kelly (zero if no track record).
  - market_prob           : 1 / cote_finale  (implied market probability).
  - model_prob            : Bayesian shrinkage win-rate used as proxy.

Usage:
    python feature_builders/betting_edge_features_builder.py
    python feature_builders/betting_edge_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict, deque
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
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "betting_edge"

# Bayesian prior for win-rate shrinkage
PRIOR_WEIGHT = 10
GLOBAL_WIN_RATE = 0.08  # ~8% baseline (approx 1/12 runners)

# Kelly parameters
KELLY_CAP = 0.25        # quarter-Kelly maximum fraction
ROLLING_WINDOW = 10     # last N races for edge consistency

# Progress log every N records
_LOG_EVERY = 500_000

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
# BAYESIAN WIN-RATE TRACKER
# ===========================================================================


class _BayesTracker:
    """Lightweight Bayesian win-rate tracker per horse."""

    __slots__ = ("wins", "races")

    def __init__(self) -> None:
        self.wins: int = 0
        self.races: int = 0

    def bayes_win_rate(self) -> float:
        """Shrinkage estimator toward global average."""
        return (GLOBAL_WIN_RATE * PRIOR_WEIGHT + self.wins) / (
            PRIOR_WEIGHT + self.races
        )


# ===========================================================================
# EDGE CONSISTENCY TRACKER
# ===========================================================================


class _EdgeHistory:
    """Tracks whether past edge-positive bets actually won."""

    __slots__ = ("history",)

    def __init__(self) -> None:
        # deque of bools: True = positive-edge bet was a winner
        self.history: deque = deque(maxlen=ROLLING_WINDOW)

    def hit_rate(self) -> Optional[float]:
        """Rolling hit rate of positive-edge bets. None if no history."""
        if not self.history:
            return None
        return sum(self.history) / len(self.history)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_betting_edge_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build Kelly-criterion and edge-based betting features."""
    logger.info("=== Betting Edge Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        cote_finale = rec.get("cote_finale") or rec.get("rapport_final")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "cote_finale": cote_finale,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_bayes: dict[str, _BayesTracker] = defaultdict(_BayesTracker)
    horse_edge_hist: dict[str, _EdgeHistory] = defaultdict(_EdgeHistory)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

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

        # -- Snapshot pre-race values and emit features --
        post_updates: list[tuple[str, bool, bool]] = []  # (horse, is_winner, had_edge)

        for rec in course_group:
            h = rec["cheval"]

            # Model probability (Bayesian win-rate)
            p = horse_bayes[h].bayes_win_rate() if h else GLOBAL_WIN_RATE
            q = 1.0 - p

            # Parse cote_finale
            cote = None
            if rec["cote_finale"] is not None:
                try:
                    cote = float(rec["cote_finale"])
                    if cote <= 1.0:
                        cote = None  # cote must be > 1 for net payout
                except (ValueError, TypeError):
                    cote = None

            # Market implied probability
            market_prob = None
            if cote is not None:
                market_prob = round(1.0 / cote, 6)

            # Edge percentage: (p_model - p_market) / p_market * 100
            edge_pct = None
            if market_prob is not None and market_prob > 0:
                edge_pct = round((p - market_prob) / market_prob * 100.0, 4)

            # Kelly fraction: (p * b - q) / b, where b = cote - 1
            kelly = None
            if cote is not None:
                b = cote - 1.0  # net payout per unit staked
                if b > 0:
                    raw_kelly = (p * b - q) / b
                    kelly = round(max(0.0, min(raw_kelly, KELLY_CAP)), 6)

            # Edge consistency: rolling hit-rate of positive-edge races
            edge_cons = None
            if h:
                edge_cons_val = horse_edge_hist[h].hit_rate()
                if edge_cons_val is not None:
                    edge_cons = round(edge_cons_val, 4)

            # Kelly bankroll pct: risk-adjusted Kelly
            kelly_bk = None
            if kelly is not None and edge_cons is not None:
                kelly_bk = round(kelly * edge_cons, 6)

            results.append({
                "partant_uid": rec["uid"],
                "kelly_fraction": kelly,
                "edge_percentage": edge_pct,
                "edge_consistency": edge_cons,
                "kelly_bankroll_pct": kelly_bk,
                "market_prob": market_prob,
                "model_prob": round(p, 6),
            })

            # Prepare deferred update
            had_edge = edge_pct is not None and edge_pct > 0
            post_updates.append((h, rec["gagnant"], had_edge))

        # -- Update trackers after race (post-race, no leakage) --
        for h, is_winner, had_edge in post_updates:
            if not h:
                continue
            horse_bayes[h].races += 1
            if is_winner:
                horse_bayes[h].wins += 1

            # Track: when model saw positive edge, did the horse win?
            if had_edge:
                horse_edge_hist[h].history.append(is_winner)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Betting edge build termine: %d features en %.1fs (chevaux: %d)",
        len(results),
        elapsed,
        len(horse_bayes),
    )

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features betting-edge a partir de partants_master"
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
        help="Repertoire de sortie (defaut: output/betting_edge/)",
    )
    args = parser.parse_args()

    logger = setup_logging("betting_edge_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_betting_edge_features(input_path, logger)

    # Save
    out_path = output_dir / "betting_edge_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
