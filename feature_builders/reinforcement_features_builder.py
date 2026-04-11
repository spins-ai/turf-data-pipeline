#!/usr/bin/env python3
"""
feature_builders.reinforcement_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features for the Value Hunter RL and meta-selector modules -- features
that model the state/action/reward framework for reinforcement learning
applied to betting.

Temporal integrity: for any partant at date D, only races with date < D
contribute to RL state, action history, and reward estimates -- no future
leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - reinforcement_features.jsonl  in builder_outputs/reinforcement_features/

Features per partant (10):
  - rl_cumulative_bankroll_trend   : simulated cumulative bankroll change over
                                     time (betting 1 EUR on every favourite,
                                     rolling sum of returns minus costs)
  - rl_reward_moving_avg           : rolling average return of "always bet
                                     favourite" strategy over last 100 races
  - rl_exploration_score           : 1/log(times_seen_similar_context + 2) --
                                     how much has this context been explored
  - rl_context_state_encoding      : numeric encoding of (discipline,
                                     distance_band, field_size_bucket) -- state
  - rl_best_action_history         : which betting strategy has worked best in
                                     this context? 0=skip, 1=fav, 2=outsider,
                                     3=place
  - rl_regret_estimate             : difference between best_action_return and
                                     favourite_return in this context
  - rl_epsilon_greedy_signal       : based on exploration_score, should we
                                     explore (1) or exploit (0)?
  - rl_time_discount_factor        : exponential decay based on distance in
                                     time from most recent observation
  - rl_state_visit_count           : how many times this exact state has been
                                     visited before
  - rl_value_function_estimate     : estimated expected value of being in this
                                     state and betting optimally

Usage:
    python feature_builders/reinforcement_features_builder.py
    python feature_builders/reinforcement_features_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/reinforcement_features")

_LOG_EVERY = 500_000

# RL parameters
_REWARD_WINDOW = 100       # rolling window for reward moving average
_EPSILON_THRESHOLD = 0.5   # exploration_score above this -> explore
_TIME_DECAY_LAMBDA = 0.002 # exponential decay constant (per day)


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN guard
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_date_days(date_str: str) -> Optional[int]:
    """Convert YYYY-MM-DD to an integer day count for gap calculations."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


# ===========================================================================
# STATE ENCODING
# ===========================================================================

# Discipline encoding: maps discipline string -> int
_DISCIPLINE_MAP = {
    "plat": 1,
    "attele": 2, "attelé": 2, "trot attele": 2, "trot attelé": 2,
    "monte": 3, "monté": 3, "trot monte": 3, "trot monté": 3,
    "obstacle": 4, "haies": 4, "steeple": 5, "cross": 6,
}


def _encode_discipline(disc: Any) -> int:
    """Map discipline string to a numeric code. 0 = unknown."""
    if not disc:
        return 0
    key = str(disc).lower().strip()
    return _DISCIPLINE_MAP.get(key, 0)


def _distance_band(dist: Any) -> int:
    """Map distance (metres) to a band index.

    0 = unknown, 1 = sprint (<1400), 2 = mile (1400-1799),
    3 = intermediate (1800-2399), 4 = staying (2400-3199), 5 = long (3200+)
    """
    try:
        d = int(dist)
    except (TypeError, ValueError):
        return 0
    if d <= 0:
        return 0
    if d < 1400:
        return 1
    if d < 1800:
        return 2
    if d < 2400:
        return 3
    if d < 3200:
        return 4
    return 5


def _field_size_bucket(nb: Any) -> int:
    """Map number of runners to a bucket.

    0 = unknown, 1 = small (<8), 2 = medium (8-11),
    3 = large (12-15), 4 = xlarge (16+)
    """
    try:
        n = int(nb)
    except (TypeError, ValueError):
        return 0
    if n <= 0:
        return 0
    if n < 8:
        return 1
    if n < 12:
        return 2
    if n < 16:
        return 3
    return 4


def _encode_context(discipline: Any, distance: Any, nb_partants: Any) -> int:
    """Encode (discipline, distance_band, field_size_bucket) as a single int.

    Format: disc * 100 + dist_band * 10 + field_bucket
    Produces values in [0, 659] range.
    """
    d = _encode_discipline(discipline)
    b = _distance_band(distance)
    f = _field_size_bucket(nb_partants)
    return d * 100 + b * 10 + f


# ===========================================================================
# PER-CONTEXT STATE (keyed by context_encoding)
# ===========================================================================


class _ContextState:
    """Track RL state for one context (discipline x distance_band x field_size)."""

    __slots__ = (
        "visit_count",
        "action_returns",   # {action_id: [total_return, count]}
        "action_counts",    # {action_id: count}
    )

    def __init__(self) -> None:
        self.visit_count: int = 0
        # action_id: 0=skip, 1=fav, 2=outsider, 3=place
        # total_return = cumulative net return, count = number of bets
        self.action_returns: dict[int, list[float]] = {
            0: [0.0, 0],  # skip: always 0 return
            1: [0.0, 0],  # fav
            2: [0.0, 0],  # outsider
            3: [0.0, 0],  # place
        }
        self.action_counts: dict[int, int] = {0: 0, 1: 0, 2: 0, 3: 0}

    def best_action(self) -> int:
        """Return action with highest average return. Default to 0 (skip)."""
        best_a = 0
        best_avg = 0.0
        for a in range(4):
            total, cnt = self.action_returns[a]
            if cnt > 0:
                avg = total / cnt
                if avg > best_avg:
                    best_avg = avg
                    best_a = a
        return best_a

    def avg_return(self, action: int) -> float:
        """Average return for a specific action."""
        total, cnt = self.action_returns[action]
        if cnt == 0:
            return 0.0
        return total / cnt

    def best_action_avg_return(self) -> float:
        """Average return of the best action."""
        ba = self.best_action()
        return self.avg_return(ba)


# ===========================================================================
# GLOBAL BANKROLL STATE
# ===========================================================================


class _GlobalBankrollState:
    """Track the simulated bankroll from betting 1 EUR on every favourite."""

    __slots__ = ("cumulative_return", "recent_returns", "_window_sum", "_window_count")

    def __init__(self) -> None:
        self.cumulative_return: float = 0.0
        # Circular buffer for last _REWARD_WINDOW race returns
        self.recent_returns: list[float] = []
        self._window_sum: float = 0.0
        self._window_count: int = 0

    def add_race_return(self, net_return: float) -> None:
        """Record the net return from one race's favourite bet."""
        self.cumulative_return += net_return

        self.recent_returns.append(net_return)
        self._window_sum += net_return
        self._window_count += 1

        if self._window_count > _REWARD_WINDOW:
            oldest = self.recent_returns.pop(0)
            self._window_sum -= oldest
            self._window_count -= 1

    def moving_avg(self) -> Optional[float]:
        """Rolling average return over last _REWARD_WINDOW races."""
        if self._window_count == 0:
            return None
        return self._window_sum / self._window_count


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_reinforcement_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build reinforcement learning features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Reinforcement Features Builder (memory-optimised) ===")
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
    context_states: dict[int, _ContextState] = defaultdict(_ContextState)
    bankroll = _GlobalBankrollState()
    latest_date_days: Optional[int] = None  # track most recent date for time discount

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "rl_cumulative_bankroll_trend",
        "rl_reward_moving_avg",
        "rl_exploration_score",
        "rl_context_state_encoding",
        "rl_best_action_history",
        "rl_regret_estimate",
        "rl_epsilon_greedy_signal",
        "rl_time_discount_factor",
        "rl_state_visit_count",
        "rl_value_function_estimate",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

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

            current_date_days = _parse_date_days(course_date_str)

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # Identify the favourite in this race (lowest cote_finale)
            fav_idx: Optional[int] = None
            fav_cote: Optional[float] = None
            outsider_idx: Optional[int] = None
            outsider_cote: float = 0.0

            cotes: list[tuple[int, float]] = []
            for ridx, rec in enumerate(course_records):
                c = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("rapport_final"))
                if c is not None and c > 1.0:
                    cotes.append((ridx, c))

            if cotes:
                cotes.sort(key=lambda x: x[1])
                fav_idx, fav_cote = cotes[0]
                # Outsider = highest odds horse
                outsider_idx, outsider_cote = cotes[-1]

            # -- Snapshot pre-race state and emit features --
            post_updates: list[dict[str, Any]] = []

            for ridx, rec in enumerate(course_records):
                partant_uid = rec.get("partant_uid")
                course_uid_rec = rec.get("course_uid")
                date_iso = rec.get("date_reunion_iso")

                # Context encoding
                discipline = rec.get("discipline") or rec.get("type_course")
                distance = rec.get("distance") or rec.get("distance_metres")
                nb_partants = rec.get("nombre_partants")
                ctx_code = _encode_context(discipline, distance, nb_partants)

                ctx = context_states[ctx_code]

                # --- Compute features from pre-race state ---

                # 1. rl_cumulative_bankroll_trend
                rl_bankroll = round(bankroll.cumulative_return, 4)

                # 2. rl_reward_moving_avg
                rl_reward_avg = bankroll.moving_avg()
                if rl_reward_avg is not None:
                    rl_reward_avg = round(rl_reward_avg, 6)

                # 3. rl_exploration_score: 1/log(visits + 2)
                rl_exploration = round(1.0 / math.log(ctx.visit_count + 2), 6)

                # 4. rl_context_state_encoding
                rl_ctx_encoding = ctx_code

                # 5. rl_best_action_history
                rl_best_action = ctx.best_action()

                # 6. rl_regret_estimate: best_action_return - fav_return
                best_ret = ctx.best_action_avg_return()
                fav_ret = ctx.avg_return(1)  # action 1 = favourite
                rl_regret = round(best_ret - fav_ret, 6) if ctx.visit_count > 0 else None

                # 7. rl_epsilon_greedy_signal
                rl_epsilon = 1 if rl_exploration > _EPSILON_THRESHOLD else 0

                # 8. rl_time_discount_factor
                rl_time_discount = None
                if current_date_days is not None and latest_date_days is not None:
                    days_gap = max(0, current_date_days - latest_date_days)
                    rl_time_discount = round(math.exp(-_TIME_DECAY_LAMBDA * days_gap), 6)

                # 9. rl_state_visit_count
                rl_visit_count = ctx.visit_count

                # 10. rl_value_function_estimate
                rl_value = round(best_ret, 6) if ctx.visit_count > 0 else None

                # Emit record
                out_rec = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_rec,
                    "date_reunion_iso": date_iso,
                    "rl_cumulative_bankroll_trend": rl_bankroll,
                    "rl_reward_moving_avg": rl_reward_avg,
                    "rl_exploration_score": rl_exploration,
                    "rl_context_state_encoding": rl_ctx_encoding,
                    "rl_best_action_history": rl_best_action,
                    "rl_regret_estimate": rl_regret,
                    "rl_epsilon_greedy_signal": rl_epsilon,
                    "rl_time_discount_factor": rl_time_discount,
                    "rl_state_visit_count": rl_visit_count,
                    "rl_value_function_estimate": rl_value,
                }

                for k in feature_keys:
                    if out_rec.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # Collect info for post-race update
                is_gagnant = bool(rec.get("is_gagnant"))
                is_place = bool(rec.get("is_place"))
                cote_val = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("rapport_final"))
                rapport_place = _safe_float(rec.get("rapport_place"))

                post_updates.append({
                    "ridx": ridx,
                    "ctx_code": ctx_code,
                    "is_gagnant": is_gagnant,
                    "is_place": is_place,
                    "cote": cote_val,
                    "rapport_place": rapport_place,
                })

            # -- Post-race updates (after all features emitted) --

            # Compute returns for each action for this race
            # Action 1 (fav): bet 1 EUR on favourite
            fav_return: float = -1.0  # default: lose stake
            if fav_idx is not None and fav_cote is not None:
                fav_rec = post_updates[fav_idx]
                if fav_rec["is_gagnant"]:
                    fav_return = fav_cote - 1.0  # net profit
                # else: -1.0 (lost stake)

            # Update global bankroll with fav bet return
            bankroll.add_race_return(fav_return)

            # Action 2 (outsider): bet 1 EUR on outsider
            outsider_return: float = -1.0
            if outsider_idx is not None:
                out_rec_data = post_updates[outsider_idx]
                if out_rec_data["is_gagnant"] and out_rec_data["cote"] is not None:
                    outsider_return = out_rec_data["cote"] - 1.0

            # Action 3 (place): bet 1 EUR on favourite for place
            place_return: float = -1.0
            if fav_idx is not None:
                fav_place_rec = post_updates[fav_idx]
                if fav_place_rec["is_place"]:
                    rp = fav_place_rec["rapport_place"]
                    if rp is not None and rp > 0:
                        place_return = rp - 1.0
                    else:
                        # Place but no odds known: assume small return
                        place_return = 0.5

            # Action 0 (skip): always 0
            skip_return: float = 0.0

            action_returns = {
                0: skip_return,
                1: fav_return,
                2: outsider_return,
                3: place_return,
            }

            # Update all context states that appeared in this race
            seen_contexts: set[int] = set()
            for pu in post_updates:
                ctx_code = pu["ctx_code"]
                if ctx_code not in seen_contexts:
                    seen_contexts.add(ctx_code)
                    ctx = context_states[ctx_code]
                    ctx.visit_count += 1
                    for a in range(4):
                        ret = action_returns[a]
                        ctx.action_returns[a][0] += ret
                        ctx.action_returns[a][1] += 1
                        ctx.action_counts[a] += 1

            # Update latest date
            if current_date_days is not None:
                latest_date_days = current_date_days

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Reinforcement features build termine: %d features en %.1fs (contexts: %d)",
        n_written, elapsed, len(context_states),
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
        description="Construction des features reinforcement learning a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/reinforcement_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("reinforcement_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "reinforcement_features.jsonl"
    build_reinforcement_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
