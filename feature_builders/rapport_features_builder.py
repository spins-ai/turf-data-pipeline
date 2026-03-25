#!/usr/bin/env python3
"""
feature_builders.rapport_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Exploits the 40 unused rap_* (rapport/dividend) fields from partants_master
to build 8 betting-market features per partant.

Reads partants_master.jsonl in streaming mode, sorts chronologically,
then processes course-by-course to preserve temporal integrity.

Temporal integrity: for any partant at date D, only dividend data from
races with date < D contribute to running statistics -- no future leakage.

Produces:
  - rapport_features.jsonl   in output/rapport_features/

Features per partant (8):
  - avg_gagnant_dividend_hippo   : running avg of rap_rapport_simple_gagnant
                                    at this hippodrome (Bayesian-shrunk).
  - std_gagnant_dividend_hippo   : running stdev of gagnant dividends at hippo.
  - avg_couple_dividend_hippo    : running avg of rap_rapport_couple_gagnant
                                    at this hippodrome (Bayesian-shrunk).
  - horse_avg_winning_dividend   : running avg of gagnant dividends when this
                                    horse won (only updated post-race).
  - horse_upset_dividend_avg     : running avg of gagnant dividends when this
                                    horse won at high odds (>= 10 EUR).
  - dividend_vs_cote_ratio       : actual gagnant dividend / cote_finale
                                    (market efficiency: >1 = underbet winner).
  - market_overround_actual      : sum of implied probs from place dividends
                                    (overround from actual payouts; >1 = book margin).
  - is_historically_undervalued  : 1 if horse's historical avg winning dividend
                                    exceeds its current implied value (cote_finale).
                                    Signals the market consistently underestimates
                                    this horse.

Usage:
    python feature_builders/rapport_features_builder.py
    python feature_builders/rapport_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
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
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "rapport_features"

# Bayesian prior for hippodrome dividend averages
PRIOR_RACES = 20
GLOBAL_AVG_GAGNANT = 800.0   # centimes (~8.00 EUR typical winning dividend)
GLOBAL_AVG_COUPLE = 3000.0   # centimes (~30.00 EUR typical couple gagnant)

# Upset dividend threshold (in EUR)
UPSET_DIVIDEND_EUR = 10.0

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
# RUNNING STATISTICS TRACKER
# ===========================================================================


class _RunningStats:
    """Welford's online algorithm for mean and standard deviation."""

    __slots__ = ("n", "mean", "m2")

    def __init__(self) -> None:
        self.n: int = 0
        self.mean: float = 0.0
        self.m2: float = 0.0

    def update(self, value: float) -> None:
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def current_mean(self) -> Optional[float]:
        if self.n == 0:
            return None
        return self.mean

    def current_std(self) -> Optional[float]:
        if self.n < 2:
            return None
        return math.sqrt(self.m2 / self.n)


class _BayesianMean:
    """Bayesian-shrunk running mean toward a global prior."""

    __slots__ = ("total", "n", "prior_mean", "prior_weight")

    def __init__(self, prior_mean: float, prior_weight: int) -> None:
        self.total: float = 0.0
        self.n: int = 0
        self.prior_mean = prior_mean
        self.prior_weight = prior_weight

    def update(self, value: float) -> None:
        self.total += value
        self.n += 1

    def shrunk_mean(self) -> float:
        return (self.prior_mean * self.prior_weight + self.total) / (
            self.prior_weight + self.n
        )


class _HorseWinTracker:
    """Track dividends when a specific horse wins."""

    __slots__ = ("win_dividends", "upset_dividends")

    def __init__(self) -> None:
        self.win_dividends: list[float] = []
        self.upset_dividends: list[float] = []

    def avg_win_dividend(self) -> Optional[float]:
        if not self.win_dividends:
            return None
        return sum(self.win_dividends) / len(self.win_dividends)

    def avg_upset_dividend(self) -> Optional[float]:
        if not self.upset_dividends:
            return None
        return sum(self.upset_dividends) / len(self.upset_dividends)


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    """Convert value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_rapport_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build 8 rapport/dividend features from partants_master.jsonl."""
    logger.info("=== Rapport Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read needed fields (slim) --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "hippodrome": rec.get("hippodrome_normalise", ""),
            "gagnant": bool(rec.get("is_gagnant")),
            "cote_finale": _safe_float(
                rec.get("cote_finale") or rec.get("rapport_final")
            ),
            # Dividend fields (stored in centimes in the data)
            "rap_rapport_simple_gagnant": _safe_float(rec.get("rap_rapport_simple_gagnant")),
            "rap_rapport_couple_gagnant": _safe_float(rec.get("rap_rapport_couple_gagnant")),
            # Place dividends for overround calculation
            "rap_rapport_simple_place_1": _safe_float(rec.get("rap_rapport_simple_place_1")),
            "rap_rapport_simple_place_2": _safe_float(rec.get("rap_rapport_simple_place_2")),
            "rap_rapport_simple_place_3": _safe_float(rec.get("rap_rapport_simple_place_3")),
            # Number of runners for overround denominator
            "nombre_partants": _safe_float(rec.get("nombre_partants")),
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

    # Per-hippodrome trackers
    hippo_gagnant_bayes: dict[str, _BayesianMean] = defaultdict(
        lambda: _BayesianMean(GLOBAL_AVG_GAGNANT, PRIOR_RACES)
    )
    hippo_gagnant_stats: dict[str, _RunningStats] = defaultdict(_RunningStats)
    hippo_couple_bayes: dict[str, _BayesianMean] = defaultdict(
        lambda: _BayesianMean(GLOBAL_AVG_COUPLE, PRIOR_RACES)
    )

    # Per-horse win dividend tracker
    horse_win_tracker: dict[str, _HorseWinTracker] = defaultdict(_HorseWinTracker)

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

        # Common course-level fields (same for all partants in course)
        hippo = course_group[0]["hippodrome"]
        rsg = course_group[0]["rap_rapport_simple_gagnant"]   # centimes or None
        rcg = course_group[0]["rap_rapport_couple_gagnant"]   # centimes or None
        rsp1 = course_group[0]["rap_rapport_simple_place_1"]  # centimes
        rsp2 = course_group[0]["rap_rapport_simple_place_2"]
        rsp3 = course_group[0]["rap_rapport_simple_place_3"]
        nb_partants = course_group[0]["nombre_partants"]

        # -- Snapshot pre-race hippodrome stats (temporal: before this race) --
        avg_gagnant_hippo: Optional[float] = None
        std_gagnant_hippo: Optional[float] = None
        avg_couple_hippo: Optional[float] = None

        if hippo:
            avg_gagnant_hippo = round(
                hippo_gagnant_bayes[hippo].shrunk_mean() / 100.0, 4
            )  # convert centimes to EUR
            std_raw = hippo_gagnant_stats[hippo].current_std()
            if std_raw is not None:
                std_gagnant_hippo = round(std_raw / 100.0, 4)
            avg_couple_hippo = round(
                hippo_couple_bayes[hippo].shrunk_mean() / 100.0, 4
            )

        # -- Compute course-level actual overround from place dividends --
        # overround = sum of (1 / place_dividend_eur) for each placing
        # A typical place pool pays 3 horses; overround > 1 = bookmaker margin
        market_overround: Optional[float] = None
        place_divs_eur = []
        for pd in (rsp1, rsp2, rsp3):
            if pd is not None and pd > 0:
                place_divs_eur.append(pd / 100.0)
        if len(place_divs_eur) >= 2 and nb_partants is not None and nb_partants > 0:
            # Each place dividend implies a probability: 1/dividend
            # The number of paying places is typically 3 (for >= 8 runners)
            nb_places_paid = len(place_divs_eur)
            sum_implied = sum(1.0 / d for d in place_divs_eur)
            # Scale to fraction of field: if 3 pay out of 16, the fair sum = 3/16
            # overround = actual_sum / fair_sum
            fair_sum = nb_places_paid / nb_partants
            if fair_sum > 0:
                market_overround = round(sum_implied / fair_sum, 4)

        # -- Compute dividend_vs_cote_ratio (course-level actual dividend vs cote) --
        # This is per-partant since each horse has a different cote
        gagnant_div_eur: Optional[float] = None
        if rsg is not None and rsg > 0:
            gagnant_div_eur = rsg / 100.0

        # -- Emit features for each partant --
        # Collect post-race updates to apply after all partants are processed
        post_updates: list[tuple[Optional[str], bool, Optional[float]]] = []

        for rec in course_group:
            horse = rec["cheval"]
            cote = rec["cote_finale"]

            # 6. dividend_vs_cote_ratio = actual_gagnant_dividend / cote_finale
            div_cote_ratio: Optional[float] = None
            if gagnant_div_eur is not None and cote is not None and cote > 0:
                div_cote_ratio = round(gagnant_div_eur / cote, 4)

            # 4 & 5. Horse-level historical winning dividends (pre-race snapshot)
            horse_avg_win: Optional[float] = None
            horse_upset_avg: Optional[float] = None
            if horse and horse in horse_win_tracker:
                tracker = horse_win_tracker[horse]
                raw_avg = tracker.avg_win_dividend()
                if raw_avg is not None:
                    horse_avg_win = round(raw_avg, 4)
                raw_upset = tracker.avg_upset_dividend()
                if raw_upset is not None:
                    horse_upset_avg = round(raw_upset, 4)

            # 8. is_historically_undervalued
            # Horse's avg winning dividend > current cote => market underestimates
            is_undervalued: Optional[int] = None
            if horse_avg_win is not None and cote is not None and cote > 0:
                is_undervalued = 1 if horse_avg_win > cote else 0

            results.append({
                "partant_uid": rec["uid"],
                "avg_gagnant_dividend_hippo": avg_gagnant_hippo,
                "std_gagnant_dividend_hippo": std_gagnant_hippo,
                "avg_couple_dividend_hippo": avg_couple_hippo,
                "horse_avg_winning_dividend": horse_avg_win,
                "horse_upset_dividend_avg": horse_upset_avg,
                "dividend_vs_cote_ratio": div_cote_ratio,
                "market_overround_actual": market_overround,
                "is_historically_undervalued": is_undervalued,
            })

            post_updates.append((horse, rec["gagnant"], gagnant_div_eur))

        # -- Post-race updates: update trackers with this race's data --

        # Update hippodrome dividend stats
        if hippo:
            if rsg is not None and rsg > 0:
                hippo_gagnant_bayes[hippo].update(rsg)
                hippo_gagnant_stats[hippo].update(rsg)
            if rcg is not None and rcg > 0:
                hippo_couple_bayes[hippo].update(rcg)

        # Update per-horse win dividend trackers
        for horse, is_winner, gd_eur in post_updates:
            if not horse or not is_winner or gd_eur is None:
                continue
            horse_win_tracker[horse].win_dividends.append(gd_eur)
            if gd_eur >= UPSET_DIVIDEND_EUR:
                horse_win_tracker[horse].upset_dividends.append(gd_eur)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Rapport features build termine: %d features en %.1fs "
        "(hippodromes: %d, chevaux avec victoires: %d)",
        len(results),
        elapsed,
        len(hippo_gagnant_bayes),
        sum(1 for t in horse_win_tracker.values() if t.win_dividends),
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
        description="Construction de 8 features rapport/dividend a partir de partants_master"
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
        help="Repertoire de sortie (defaut: output/rapport_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("rapport_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_rapport_features(input_path, logger)

    # Save
    out_path = output_dir / "rapport_features.jsonl"
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
