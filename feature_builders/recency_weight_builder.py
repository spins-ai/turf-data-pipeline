#!/usr/bin/env python3
"""
feature_builders.recency_weight_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Recency-weighted statistics giving more weight to recent races via
exponential decay: weight = exp(-lambda * days_since_race), lambda=0.01.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the weighted statistics — no future leakage.

Reads partants_master.jsonl in streaming mode (sort+seek pattern):
  Pass 1 — collect minimal fields, sort chronologically.
  Pass 2 — process course by course; features computed BEFORE state update.

Produces:
  - recency_weight_features.jsonl   in OUTPUT_DIR

Features per partant (8):
  - rw_weighted_win_rate         : recency-weighted win rate
  - rw_weighted_place_rate       : recency-weighted place rate (top 3)
  - rw_weighted_avg_position_pct : recency-weighted average position percentage
  - rw_weighted_earnings_rate    : recency-weighted average earnings per race
  - rw_momentum_30d              : weighted win rate (last 30d races only) vs overall
  - rw_decay_factor_avg          : average weight across all past races (recency indicator)
  - rw_weighted_odds_accuracy    : recency-weighted average of (1/cote - was_winner)
  - rw_nb_recent_60d             : count of races in last 60 days (activity level)

Usage:
    python feature_builders/recency_weight_builder.py
    python feature_builders/recency_weight_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/recency_weight_builder.py --output /path/to/output_dir
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/recency_weight")

# Exponential decay parameter: weight = exp(-LAMBDA * days_since_race)
# lambda=0.01 means a race 100 days ago has weight ~0.368, 200 days ~0.135
DECAY_LAMBDA = 0.01

# State deque size per horse
DEQUE_MAXLEN = 50

# 30-day window for momentum comparison
MOMENTUM_WINDOW_DAYS = 30

# 60-day window for activity count
ACTIVITY_WINDOW_DAYS = 60

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# DATE HELPERS
# ===========================================================================


def _parse_date(date_str: Any) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    s = str(date_str).strip()
    # Accept YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt[:8] if "T" not in s else fmt)
        except ValueError:
            pass
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _days_between(earlier: datetime, later: datetime) -> float:
    """Return number of days from earlier to later (non-negative)."""
    delta = later - earlier
    return max(0.0, delta.total_seconds() / 86400.0)


def _decay_weight(days_ago: float) -> float:
    """Exponential decay weight: exp(-LAMBDA * days_ago)."""
    return math.exp(-DECAY_LAMBDA * days_ago)


# ===========================================================================
# HORSE STATE
# ===========================================================================


class _HorseState:
    """Per-horse recency state.

    Stores a bounded deque of past races with fields needed for all 8 features.
    Each entry: (date: datetime, position_pct: float, won: bool, placed: bool,
                 cote: float|None, earnings: float)
    """

    __slots__ = ("races",)

    def __init__(self) -> None:
        # Each element: (date, position_pct, won, placed, cote, earnings)
        self.races: deque = deque(maxlen=DEQUE_MAXLEN)

    def compute_features(self, current_date: datetime) -> dict[str, Optional[float]]:
        """Compute all 8 recency-weighted features based on races BEFORE current_date.

        Returns a dict with feature values (None when no data available).
        """
        if not self.races:
            return {
                "rw_weighted_win_rate": None,
                "rw_weighted_place_rate": None,
                "rw_weighted_avg_position_pct": None,
                "rw_weighted_earnings_rate": None,
                "rw_momentum_30d": None,
                "rw_decay_factor_avg": None,
                "rw_weighted_odds_accuracy": None,
                "rw_nb_recent_60d": None,
            }

        # Accumulate weighted sums
        w_sum = 0.0
        w_win_sum = 0.0
        w_place_sum = 0.0
        w_pos_sum = 0.0
        w_earn_sum = 0.0

        # Odds accuracy (only races with valid cote)
        w_odds_sum = 0.0
        w_odds_w_sum = 0.0

        # Momentum: last 30d only
        m_w_sum = 0.0
        m_w_win_sum = 0.0

        # Activity count: last 60 days
        nb_60d = 0

        for (race_date, pos_pct, won, placed, cote, earnings) in self.races:
            days_ago = _days_between(race_date, current_date)
            w = _decay_weight(days_ago)

            w_sum += w
            w_win_sum += w * (1.0 if won else 0.0)
            w_place_sum += w * (1.0 if placed else 0.0)
            w_pos_sum += w * pos_pct
            w_earn_sum += w * earnings

            # Odds accuracy: 1/cote - was_winner
            if cote is not None and cote > 0:
                implied = 1.0 / cote
                accuracy = implied - (1.0 if won else 0.0)
                w_odds_sum += w * accuracy
                w_odds_w_sum += w

            # Momentum (30d window)
            if days_ago <= MOMENTUM_WINDOW_DAYS:
                m_w_sum += w
                m_w_win_sum += w * (1.0 if won else 0.0)

            # Activity (60d window)
            if days_ago <= ACTIVITY_WINDOW_DAYS:
                nb_60d += 1

        # Compute final features
        if w_sum <= 0.0:
            return {
                "rw_weighted_win_rate": None,
                "rw_weighted_place_rate": None,
                "rw_weighted_avg_position_pct": None,
                "rw_weighted_earnings_rate": None,
                "rw_momentum_30d": None,
                "rw_decay_factor_avg": None,
                "rw_weighted_odds_accuracy": None,
                "rw_nb_recent_60d": nb_60d,
            }

        n = len(self.races)

        rw_win = w_win_sum / w_sum
        rw_place = w_place_sum / w_sum
        rw_pos = w_pos_sum / w_sum
        rw_earn = w_earn_sum / w_sum
        rw_decay_avg = w_sum / n  # average weight = how recent the races are

        # Momentum: ratio of 30d win rate vs overall win rate
        # Positive = doing better recently; None if no 30d races
        rw_momentum: Optional[float]
        if m_w_sum > 0.0:
            m_win_rate = m_w_win_sum / m_w_sum
            # Express as relative difference: (recent - overall) / (overall + epsilon)
            # Use additive difference for simplicity (can be negative)
            rw_momentum = round(m_win_rate - rw_win, 6)
        else:
            rw_momentum = None

        rw_odds_acc: Optional[float]
        if w_odds_w_sum > 0.0:
            rw_odds_acc = round(w_odds_sum / w_odds_w_sum, 6)
        else:
            rw_odds_acc = None

        return {
            "rw_weighted_win_rate": round(rw_win, 6),
            "rw_weighted_place_rate": round(rw_place, 6),
            "rw_weighted_avg_position_pct": round(rw_pos, 6),
            "rw_weighted_earnings_rate": round(rw_earn, 2),
            "rw_momentum_30d": rw_momentum,
            "rw_decay_factor_avg": round(rw_decay_avg, 6),
            "rw_weighted_odds_accuracy": rw_odds_acc,
            "rw_nb_recent_60d": nb_60d,
        }

    def update(
        self,
        race_date: datetime,
        position_pct: float,
        won: bool,
        placed: bool,
        cote: Optional[float],
        earnings: float,
    ) -> None:
        """Add a race to the state (called AFTER features have been computed)."""
        self.races.append((race_date, position_pct, won, placed, cote, earnings))


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger) -> Any:
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
# FIELD EXTRACTION HELPERS
# ===========================================================================


def _get_horse_id(rec: dict) -> str:
    """Return a stable horse identifier from a partant record."""
    # Prefer horse_id, fall back to nom_cheval
    hid = rec.get("horse_id") or rec.get("id_cheval")
    if hid:
        return str(hid)
    name = rec.get("nom_cheval") or rec.get("cheval")
    return str(name).strip().upper() if name else ""


def _get_position_pct(rec: dict) -> Optional[float]:
    """Compute position percentage (0=last, 1=first) from position and field size."""
    pos = rec.get("position_arrivee") or rec.get("classement_arrivee")
    nb = rec.get("nombre_partants") or rec.get("nb_partants")
    try:
        pos = int(pos)
        nb = int(nb)
        if pos > 0 and nb > 1:
            # 1st place = 1.0, last place = ~0.0
            return round(1.0 - (pos - 1) / (nb - 1), 6)
        elif pos == 1:
            return 1.0
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return None


def _get_earnings(rec: dict) -> float:
    """Extract earnings from gains_carriere_euros or similar fields."""
    for key in ("gains_course", "gains_reunion", "gains_carriere_euros",
                "gains", "prize_money"):
        val = rec.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def _is_placed(rec: dict, position: Optional[int], nb_partants: Optional[int]) -> bool:
    """Return True if horse finished in top 3."""
    if position is None:
        pos_raw = rec.get("position_arrivee") or rec.get("classement_arrivee")
        try:
            position = int(pos_raw)
        except (TypeError, ValueError):
            return False
    return 1 <= position <= 3


# ===========================================================================
# MAIN BUILD FUNCTION
# ===========================================================================


def build_recency_weight_features(
    input_path: Path, output_dir: Path, logger
) -> int:
    """Stream partants_master.jsonl, compute recency-weighted features.

    Temporal pattern: index + sort + seek.
    For each race group (course_uid + date), features are computed from the
    horse's PREVIOUS races, then state is updated with the current race.

    Returns the number of feature records written.
    """
    logger.info("=== Recency Weight Builder (lambda=%.3f) ===", DECAY_LAMBDA)
    logger.info("Input: %s", input_path)
    logger.info("Output: %s", output_dir)
    t0 = time.time()

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "recency_weight_features.jsonl"

    # -----------------------------------------------------------------------
    # Pass 1: Read minimal fields, collect slim records
    # -----------------------------------------------------------------------
    logger.info("Pass 1: lecture en streaming...")
    KEEP_FIELDS = (
        "partant_uid", "course_uid", "date_reunion_iso",
        "position_arrivee", "nombre_partants", "cote_finale",
        "gains_carriere_euros", "gains_course", "is_gagnant", "is_place",
        "horse_id", "id_cheval", "nom_cheval", "num_pmu",
        "classement_arrivee", "nb_partants",
    )

    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {k: rec.get(k) for k in KEEP_FIELDS}
        slim_records.append(slim)

    logger.info(
        "Pass 1 terminee: %d records en %.1fs", n_read, time.time() - t0
    )

    # -----------------------------------------------------------------------
    # Pass 2: Sort chronologically
    # -----------------------------------------------------------------------
    t1 = time.time()
    logger.info("Pass 2: tri chronologique...")
    slim_records.sort(
        key=lambda r: (
            r.get("date_reunion_iso") or "",
            r.get("course_uid") or "",
            r.get("num_pmu") or 0,
        )
    )
    logger.info("Tri termine en %.1fs", time.time() - t1)

    # -----------------------------------------------------------------------
    # Pass 3: Process course by course, compute features BEFORE update
    # -----------------------------------------------------------------------
    t2 = time.time()
    logger.info("Pass 3: calcul features par course...")

    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)
    n_written = 0
    n_processed = 0
    n_no_horse = 0
    n_no_date = 0

    i = 0
    total = len(slim_records)

    with open(out_path, "w", encoding="utf-8", newline="\n") as out_fh:
        while i < total:
            # Collect all partants in this course_uid group
            course_uid = slim_records[i].get("course_uid", "")
            course_date_str = slim_records[i].get("date_reunion_iso", "")
            course_group: list[dict] = []

            while (
                i < total
                and slim_records[i].get("course_uid", "") == course_uid
                and slim_records[i].get("date_reunion_iso", "") == course_date_str
            ):
                course_group.append(slim_records[i])
                i += 1

            # Parse course date once
            course_date = _parse_date(course_date_str)

            # Step A: compute features for all runners (BEFORE update)
            feature_batch: list[dict] = []

            for rec in course_group:
                n_processed += 1
                if n_processed % _LOG_EVERY == 0:
                    logger.info(
                        "  Traite %d/%d partants, %d features ecrits...",
                        n_processed, total, n_written,
                    )

                horse_id = _get_horse_id(rec)
                partant_uid = rec.get("partant_uid")

                if not horse_id:
                    n_no_horse += 1
                    feature_batch.append(None)
                    continue

                if course_date is None:
                    n_no_date += 1
                    feature_batch.append(None)
                    continue

                # Compute features from past races (before this race is added)
                features = horse_states[horse_id].compute_features(course_date)

                feature_batch.append({
                    "partant_uid": partant_uid,
                    "course_uid": course_uid,
                    **features,
                })

            # Step B: write features
            for feat in feature_batch:
                if feat is not None:
                    out_fh.write(json.dumps(feat, ensure_ascii=False) + "\n")
                    n_written += 1

            # Step C: update horse states AFTER all features are computed
            for rec in course_group:
                horse_id = _get_horse_id(rec)
                if not horse_id or course_date is None:
                    continue

                # Extract outcome fields
                pos_raw = rec.get("position_arrivee") or rec.get("classement_arrivee")
                nb_raw = rec.get("nombre_partants") or rec.get("nb_partants")

                try:
                    pos = int(pos_raw) if pos_raw is not None else None
                except (TypeError, ValueError):
                    pos = None

                try:
                    nb = int(nb_raw) if nb_raw is not None else None
                except (TypeError, ValueError):
                    nb = None

                # Position percentage
                pos_pct: float
                if pos is not None and nb is not None and nb > 1 and pos > 0:
                    pos_pct = round(1.0 - (pos - 1) / (nb - 1), 6)
                elif pos == 1:
                    pos_pct = 1.0
                else:
                    pos_pct = 0.5  # unknown, use neutral value

                # Win / place
                # Prefer explicit flags, fall back to position
                won_raw = rec.get("is_gagnant")
                if won_raw is not None:
                    won = bool(won_raw)
                else:
                    won = pos == 1

                placed_raw = rec.get("is_place")
                if placed_raw is not None:
                    placed = bool(placed_raw)
                else:
                    placed = pos is not None and 1 <= pos <= 3

                # Cote
                cote_raw = rec.get("cote_finale")
                cote: Optional[float]
                try:
                    cote = float(cote_raw) if cote_raw is not None else None
                    if cote is not None and cote <= 0:
                        cote = None
                except (TypeError, ValueError):
                    cote = None

                # Earnings
                earnings = 0.0
                for earn_key in ("gains_course", "gains_carriere_euros"):
                    val = rec.get(earn_key)
                    if val is not None:
                        try:
                            earnings = float(val)
                            break
                        except (TypeError, ValueError):
                            pass

                horse_states[horse_id].update(
                    race_date=course_date,
                    position_pct=pos_pct,
                    won=won,
                    placed=placed,
                    cote=cote,
                    earnings=earnings,
                )

    elapsed = time.time() - t2
    logger.info(
        "Pass 3 terminee: %d features ecrits en %.1fs "
        "(%d sans horse_id, %d sans date)",
        n_written, elapsed, n_no_horse, n_no_date,
    )

    # Cleanup
    del slim_records
    del horse_states
    gc.collect()

    total_elapsed = time.time() - t0
    logger.info(
        "=== Recency Weight Builder termine: %d records, %.1fs total ===",
        n_written, total_elapsed,
    )
    logger.info("Output: %s", out_path)
    return n_written


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    setup_logging("recency_weight_builder")

    parser = argparse.ArgumentParser(
        description=(
            "Compute recency-weighted features from partants_master.jsonl. "
            "Uses exponential decay weighting: weight = exp(-0.01 * days_since_race)."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=INPUT_PARTANTS,
        help="Path to partants_master.jsonl (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help="Output directory for recency_weight_features.jsonl (default: %(default)s)",
    )
    args = parser.parse_args()

    if not args.input.is_file():
        import logging
        logging.getLogger(__name__).error(
            "Input file not found: %s", args.input
        )
        sys.exit(1)

    build_recency_weight_features(
        input_path=args.input,
        output_dir=args.output,
        logger=__import__("logging").getLogger(__name__),
    )


if __name__ == "__main__":
    main()
