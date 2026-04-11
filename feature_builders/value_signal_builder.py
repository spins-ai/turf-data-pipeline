#!/usr/bin/env python3
"""
feature_builders.value_signal_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Value-signal features that compare model-implied probabilities against
market odds to identify overlay / underlay situations.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant value-signal features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the Bayesian win rate used here -- no future leakage.

Produces:
  - value_signals.jsonl   in output/value_signals/

Features per partant (5):
  - expected_value      : model_implied_prob * cote_finale - 1
                          (using Bayesian win rate as proxy for model prob)
  - edge_vs_market      : bayes_win_rate - (1 / cote_finale)
  - is_value_bet        : 1 if expected_value > 0, else 0
  - cote_vs_elo_gap     : normalised gap between market-odds rank and Elo rank
                          within the field (positive = market underestimates horse)
  - smart_money_signal  : 1 if odds shortened > 15% AND Elo rank <= 3

Usage:
    python feature_builders/value_signal_builder.py
    python feature_builders/value_signal_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/value_signal")

# Bayesian prior weight for win-rate shrinkage
PRIOR_WEIGHT = 10
GLOBAL_WIN_RATE = 0.08  # ~8% baseline (approx 1/12 runners)

# Smart-money odds shortening threshold
SMART_MONEY_DROP_PCT = 0.10  # 10% (was 15%, too strict with sparse morning odds)
SMART_MONEY_ELO_MAX_RANK = 5  # top-5 Elo (was 3, too strict for larger fields)

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
        return (GLOBAL_WIN_RATE * PRIOR_WEIGHT + self.wins) / (PRIOR_WEIGHT + self.races)


# ===========================================================================
# ELO MINI-ENGINE (simplified for ranking only)
# ===========================================================================

_BASE_ELO = 1500.0
_K = 24


class _EloMini:
    """Minimal Elo tracker used only for ranking within a field."""

    __slots__ = ("rating", "nb_races")

    def __init__(self) -> None:
        self.rating: float = _BASE_ELO
        self.nb_races: int = 0


def _expected_score(rating: float, opponent_avg: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((opponent_avg - rating) / 400.0))


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_value_signal_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build value-signal features from partants_master.jsonl."""
    logger.info("=== Value Signal Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        cote_finale = rec.get("cote_finale") or rec.get("rapport_final") or rec.get("rapport_pmu")
        cote_matin = (
            rec.get("cote_probable")
            or rec.get("cote_matin")
            or rec.get("rapport_probable")
            or rec.get("cote_depart")
            or rec.get("odds_start")
            or rec.get("cote_tendance")
        )

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "cote_finale": cote_finale,
            "cote_matin": cote_matin,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process course by course ──
    t2 = time.time()
    horse_bayes: dict[str, _BayesTracker] = defaultdict(_BayesTracker)
    horse_elo: dict[str, _EloMini] = defaultdict(_EloMini)
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

        n_runners = len(course_group)

        # ── Snapshot pre-race values ──
        pre_race: list[dict[str, Any]] = []
        for rec in course_group:
            h = rec["cheval"]
            bwr = horse_bayes[h].bayes_win_rate() if h else GLOBAL_WIN_RATE
            elo = horse_elo[h].rating if h else _BASE_ELO

            cote = None
            if rec["cote_finale"] is not None:
                try:
                    cote = float(rec["cote_finale"])
                    if cote <= 0:
                        cote = None
                except (ValueError, TypeError):
                    cote = None

            cote_m = None
            if rec["cote_matin"] is not None:
                try:
                    cote_m = float(rec["cote_matin"])
                    if cote_m <= 0:
                        cote_m = None
                except (ValueError, TypeError):
                    cote_m = None

            pre_race.append({
                "rec": rec,
                "bayes_wr": bwr,
                "elo": elo,
                "cote": cote,
                "cote_matin": cote_m,
            })

        # ── Compute Elo ranks and odds ranks within field ──
        # Elo rank: 1 = highest Elo
        elo_sorted = sorted(
            range(len(pre_race)),
            key=lambda idx: pre_race[idx]["elo"],
            reverse=True,
        )
        elo_rank = [0] * len(pre_race)
        for rank, idx in enumerate(elo_sorted, 1):
            elo_rank[idx] = rank

        # Odds rank: 1 = shortest odds (lowest cote = market favourite)
        odds_sorted = sorted(
            range(len(pre_race)),
            key=lambda idx: pre_race[idx]["cote"] if pre_race[idx]["cote"] is not None else 999.0,
        )
        odds_rank = [0] * len(pre_race)
        for rank, idx in enumerate(odds_sorted, 1):
            odds_rank[idx] = rank

        # ── Emit features ──
        for j, pr in enumerate(pre_race):
            cote = pr["cote"]
            bwr = pr["bayes_wr"]

            # expected_value = bayes_win_rate * cote_finale - 1
            ev = None
            if cote is not None:
                ev = round(bwr * cote - 1.0, 4)

            # edge_vs_market = bayes_win_rate - (1 / cote_finale)
            edge = None
            if cote is not None:
                edge = round(bwr - (1.0 / cote), 4)

            # is_value_bet
            is_vb = None
            if ev is not None:
                is_vb = 1 if ev > 0 else 0

            # cote_vs_elo_gap: normalised rank difference
            cote_elo_gap = None
            if n_runners > 1 and cote is not None:
                # Positive = market undervalues horse (odds rank worse than Elo rank)
                cote_elo_gap = round(
                    (odds_rank[j] - elo_rank[j]) / (n_runners - 1), 4
                )

            # smart_money_signal: odds shortened > threshold AND good Elo rank
            smart = None
            if pr["cote_matin"] is not None and cote is not None and pr["cote_matin"] > 0:
                drop_pct = (pr["cote_matin"] - cote) / pr["cote_matin"]
                smart = (
                    1 if (drop_pct > SMART_MONEY_DROP_PCT and elo_rank[j] <= SMART_MONEY_ELO_MAX_RANK)
                    else 0
                )
            elif cote is not None:
                # No morning odds available: default to 0 (no signal detected)
                smart = 0

            results.append({
                "partant_uid": pr["rec"]["uid"],
                "expected_value": ev,
                "edge_vs_market": edge,
                "is_value_bet": is_vb,
                "cote_vs_elo_gap": cote_elo_gap,
                "smart_money_signal": smart,
            })

        # ── Update trackers after race ──
        total_elo = sum(pr["elo"] for pr in pre_race)

        for pr in pre_race:
            rec = pr["rec"]
            h = rec["cheval"]
            if not h:
                continue

            # Bayesian tracker
            horse_bayes[h].races += 1
            if rec["gagnant"]:
                horse_bayes[h].wins += 1

            # Elo mini-update
            if n_runners >= 2:
                opp_avg = (total_elo - pr["elo"]) / (n_runners - 1)
                expected = _expected_score(pr["elo"], opp_avg)
                actual = 1.0 if rec["gagnant"] else 0.0
                horse_elo[h].rating += _K * (actual - expected)
            horse_elo[h].nb_races += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Value signal build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_bayes),
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
        description="Construction des features value-signal a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/value_signals/)",
    )
    args = parser.parse_args()

    logger = setup_logging("value_signal_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_value_signal_features(input_path, logger)

    # Save
    out_path = output_dir / "value_signals.jsonl"
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
