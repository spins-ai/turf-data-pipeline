#!/usr/bin/env python3
"""
feature_builders.roi_prediction_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features specifically for the ROI predictor module -- predicting the
actual monetary return of betting on each horse.

Reads partants_master.jsonl in streaming mode, builds a lightweight
index, sorts chronologically, then re-reads records via seek to compute
per-partant ROI prediction features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to state -- no future leakage.  State is snapshotted BEFORE
being updated with the current race.

Produces:
  - roi_prediction_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/roi_prediction_features/

Features per partant (10):
  - roi_horse_lifetime_roi      : horse's lifetime ROI (sum returns / num_bets - 1)
  - roi_horse_roi_last10        : ROI over last 10 races
  - roi_cote_range_roi_hippo    : actual ROI for this cote range at this hippodrome (rolling 100)
  - roi_discipline_roi          : actual ROI for this discipline overall (rolling 200)
  - roi_avg_return_when_wins    : horse's average return when it wins (avg winning cote)
  - roi_expected_return         : estimated_win_prob * cote (expected return per euro)
  - roi_variance_return         : variance of historical returns for this profile (risk metric)
  - roi_sharpe_ratio            : (avg_return - 1) / std_return -- risk-adjusted return
  - roi_max_drawdown_proxy      : longest losing streak * avg_bet_cost -- worst case indicator
  - roi_positive_ev_contexts    : count of context factors with positive EV

Usage:
    python feature_builders/roi_prediction_features_builder.py
    python feature_builders/roi_prediction_features_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/roi_prediction_features")

_GC_EVERY = 500_000
_LOG_EVERY = 500_000

# Bayesian prior for win-rate estimation
_PRIOR_WEIGHT = 10
_GLOBAL_WIN_RATE = 0.08  # ~8% baseline


# ===========================================================================
# HELPERS
# ===========================================================================


def _cote_range(cote: float) -> str:
    """Bucket a cote value into a discrete range label."""
    if cote <= 2.0:
        return "1-2"
    elif cote <= 4.0:
        return "2-4"
    elif cote <= 7.0:
        return "4-7"
    elif cote <= 10.0:
        return "7-10"
    elif cote <= 15.0:
        return "10-15"
    elif cote <= 25.0:
        return "15-25"
    else:
        return "25+"


def _compute_roi(returns: list[float] | deque) -> Optional[float]:
    """Compute ROI from a list of returns (each = cote if won, 0 if lost).

    ROI = (sum_returns / num_bets) - 1
    A bet of 1 euro returns `cote` on win, 0 on loss.
    """
    if not returns:
        return None
    avg = sum(returns) / len(returns)
    return round(avg - 1.0, 6)


def _compute_variance(returns: list[float] | deque) -> Optional[float]:
    """Variance of returns."""
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / len(returns)
    return round(var, 6)


def _compute_sharpe(returns: list[float] | deque) -> Optional[float]:
    """Sharpe-like ratio: (avg_return - 1) / std_return."""
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / len(returns)
    std = math.sqrt(var)
    if std < 1e-9:
        return None
    return round((mean - 1.0) / std, 6)


def _compute_max_drawdown_proxy(returns: list[float] | deque) -> Optional[float]:
    """Longest losing streak * average bet cost (1 euro per bet).

    Losing streak = consecutive 0 returns. Proxy for worst-case capital burn.
    """
    if not returns:
        return None
    max_streak = 0
    current_streak = 0
    for r in returns:
        if r == 0.0:
            current_streak += 1
            if current_streak > max_streak:
                max_streak = current_streak
        else:
            current_streak = 0
    return float(max_streak)


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class _HorseState:
    """Per-horse ROI tracking state."""

    __slots__ = ("wins", "runs", "returns", "winning_cotes", "current_losing_streak",
                 "max_losing_streak")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0
        self.returns: deque = deque(maxlen=20)  # last 20 returns
        self.winning_cotes: list[float] = []    # cotes when horse won
        self.current_losing_streak: int = 0
        self.max_losing_streak: int = 0

    def bayes_win_prob(self) -> float:
        """Bayesian shrinkage win probability."""
        return (_GLOBAL_WIN_RATE * _PRIOR_WEIGHT + self.wins) / (
            _PRIOR_WEIGHT + self.runs
        )

    def lifetime_roi(self) -> Optional[float]:
        """ROI across all recorded returns."""
        if self.runs == 0:
            return None
        return _compute_roi(list(self.returns))

    def roi_last10(self) -> Optional[float]:
        """ROI over last 10 races."""
        if len(self.returns) < 1:
            return None
        last10 = list(self.returns)[-10:]
        return _compute_roi(last10)

    def avg_return_when_wins(self) -> Optional[float]:
        """Average cote when horse wins."""
        if not self.winning_cotes:
            return None
        return round(sum(self.winning_cotes) / len(self.winning_cotes), 4)

    def variance_return(self) -> Optional[float]:
        """Variance of returns in the rolling window."""
        return _compute_variance(self.returns)

    def sharpe_ratio(self) -> Optional[float]:
        """Risk-adjusted return."""
        return _compute_sharpe(self.returns)

    def max_drawdown_proxy(self) -> Optional[float]:
        """Longest losing streak from rolling window."""
        return _compute_max_drawdown_proxy(self.returns)

    def update(self, cote: Optional[float], is_winner: bool) -> None:
        """Update state AFTER feature snapshot."""
        self.runs += 1
        if is_winner and cote is not None and cote > 0:
            self.wins += 1
            self.returns.append(cote)
            self.winning_cotes.append(cote)
            # Cap winning_cotes memory
            if len(self.winning_cotes) > 100:
                self.winning_cotes = self.winning_cotes[-100:]
            self.current_losing_streak = 0
        else:
            self.returns.append(0.0)
            self.current_losing_streak += 1
            if self.current_losing_streak > self.max_losing_streak:
                self.max_losing_streak = self.current_losing_streak


class _CoteRangeHippoState:
    """Per (hippodrome, cote_range) ROI tracking."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        self.data: dict[tuple[str, str], deque] = defaultdict(
            lambda: deque(maxlen=100)
        )

    def roi(self, hippo: str, cote_range: str) -> Optional[float]:
        key = (hippo, cote_range)
        d = self.data.get(key)
        if d is None or len(d) < 5:
            return None
        return _compute_roi(d)

    def add(self, hippo: str, cote_range: str, ret: float) -> None:
        self.data[(hippo, cote_range)].append(ret)


class _DisciplineState:
    """Per discipline ROI tracking."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        self.data: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=200)
        )

    def roi(self, discipline: str) -> Optional[float]:
        d = self.data.get(discipline)
        if d is None or len(d) < 10:
            return None
        return _compute_roi(d)

    def add(self, discipline: str, ret: float) -> None:
        self.data[discipline].append(ret)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_roi_prediction_features(input_path: Path, output_path: Path, logger) -> int:
    """Build ROI prediction features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         snapshot BEFORE update, and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== ROI Prediction Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    index: list[tuple[str, str, int, int]] = []  # (date, course_uid, num_pmu, offset)
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()

    # State trackers
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)
    cote_hippo_state = _CoteRangeHippoState()
    discipline_state = _DisciplineState()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "roi_horse_lifetime_roi",
        "roi_horse_roi_last10",
        "roi_cote_range_roi_hippo",
        "roi_discipline_roi",
        "roi_avg_return_when_wins",
        "roi_expected_return",
        "roi_variance_return",
        "roi_sharpe_ratio",
        "roi_max_drawdown_proxy",
        "roi_positive_ev_contexts",
    ]
    fill_counts = {f: 0 for f in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            cote = rec.get("rapport_final") or rec.get("cote_finale") or rec.get("cote_probable")
            try:
                cote = float(cote) if cote else None
            except (ValueError, TypeError):
                cote = None

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "cheval": rec.get("nom_cheval") or "",
                "gagnant": bool(rec.get("is_gagnant")),
                "hippo": rec.get("hippodrome_normalise", "") or "",
                "discipline": discipline,
                "cote": cote,
            }

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read this course's records from disk
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            # ------- SNAPSHOT BEFORE UPDATE (temporal integrity) -------
            post_updates: list[tuple[str, Optional[float], bool, str, str, str]] = []

            for rec in course_group:
                cheval = rec["cheval"]
                cote = rec["cote"]
                is_winner = rec["gagnant"]
                hippo = rec["hippo"]
                discipline = rec["discipline"]

                features: dict[str, Any] = {
                    "partant_uid": rec["uid"],
                    "course_uid": rec["course"],
                    "date_reunion_iso": rec["date"],
                }

                hs = horse_state[cheval] if cheval else None

                # --- roi_horse_lifetime_roi ---
                val = hs.lifetime_roi() if hs else None
                features["roi_horse_lifetime_roi"] = val
                if val is not None:
                    fill_counts["roi_horse_lifetime_roi"] += 1

                # --- roi_horse_roi_last10 ---
                val = hs.roi_last10() if hs else None
                features["roi_horse_roi_last10"] = val
                if val is not None:
                    fill_counts["roi_horse_roi_last10"] += 1

                # --- roi_cote_range_roi_hippo ---
                if cote is not None and cote > 0 and hippo:
                    cr = _cote_range(cote)
                    val = cote_hippo_state.roi(hippo, cr)
                    features["roi_cote_range_roi_hippo"] = val
                    if val is not None:
                        fill_counts["roi_cote_range_roi_hippo"] += 1
                else:
                    features["roi_cote_range_roi_hippo"] = None

                # --- roi_discipline_roi ---
                if discipline:
                    val = discipline_state.roi(discipline)
                    features["roi_discipline_roi"] = val
                    if val is not None:
                        fill_counts["roi_discipline_roi"] += 1
                else:
                    features["roi_discipline_roi"] = None

                # --- roi_avg_return_when_wins ---
                val = hs.avg_return_when_wins() if hs else None
                features["roi_avg_return_when_wins"] = val
                if val is not None:
                    fill_counts["roi_avg_return_when_wins"] += 1

                # --- roi_expected_return ---
                if hs and cote is not None and cote > 0:
                    win_prob = hs.bayes_win_prob()
                    val = round(win_prob * cote, 6)
                    features["roi_expected_return"] = val
                    fill_counts["roi_expected_return"] += 1
                else:
                    features["roi_expected_return"] = None

                # --- roi_variance_return ---
                val = hs.variance_return() if hs else None
                features["roi_variance_return"] = val
                if val is not None:
                    fill_counts["roi_variance_return"] += 1

                # --- roi_sharpe_ratio ---
                val = hs.sharpe_ratio() if hs else None
                features["roi_sharpe_ratio"] = val
                if val is not None:
                    fill_counts["roi_sharpe_ratio"] += 1

                # --- roi_max_drawdown_proxy ---
                val = hs.max_drawdown_proxy() if hs else None
                features["roi_max_drawdown_proxy"] = val
                if val is not None:
                    fill_counts["roi_max_drawdown_proxy"] += 1

                # --- roi_positive_ev_contexts ---
                # Count how many context factors have positive EV
                ev_count = 0
                ev_available = False

                # horse lifetime ROI > 0?
                horse_roi = features["roi_horse_lifetime_roi"]
                if horse_roi is not None:
                    ev_available = True
                    if horse_roi > 0:
                        ev_count += 1

                # hippo cote range ROI > 0?
                hippo_roi = features["roi_cote_range_roi_hippo"]
                if hippo_roi is not None:
                    ev_available = True
                    if hippo_roi > 0:
                        ev_count += 1

                # discipline ROI > 0?
                disc_roi = features["roi_discipline_roi"]
                if disc_roi is not None:
                    ev_available = True
                    if disc_roi > 0:
                        ev_count += 1

                if ev_available:
                    features["roi_positive_ev_contexts"] = ev_count
                    fill_counts["roi_positive_ev_contexts"] += 1
                else:
                    features["roi_positive_ev_contexts"] = None

                # Stream to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Collect update info for post-race update
                post_updates.append((cheval, cote, is_winner, hippo, discipline,
                                     _cote_range(cote) if cote is not None and cote > 0 else ""))

            # ------- UPDATE STATE AFTER RACE -------
            for cheval, cote, is_winner, hippo, discipline, cr in post_updates:
                # Compute the return for this bet: cote if won, 0 if lost
                if is_winner and cote is not None and cote > 0:
                    ret = cote
                else:
                    ret = 0.0

                # Horse state
                if cheval:
                    horse_state[cheval].update(cote, is_winner)

                # Cote range x hippo
                if hippo and cr and cote is not None and cote > 0:
                    cote_hippo_state.add(hippo, cr, ret)

                # Discipline
                if discipline and cote is not None and cote > 0:
                    discipline_state.add(discipline, ret)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)

            if n_processed % _GC_EVERY < len(course_group):
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "ROI prediction build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features ROI prediction a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/roi_prediction_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("roi_prediction_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "roi_prediction_features.jsonl"
    build_roi_prediction_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
