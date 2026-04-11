#!/usr/bin/env python3
"""
feature_builders.hippodrome_stats_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Hippodrome-level statistics and characteristics, computed per partant
using temporal integrity: for any partant at date D, only races with
date < D contribute to hippodrome stats -- no future leakage.

Strategy: index + sort + seek (same pattern as seasonality_builder).
  Phase 1: read lightweight index (date, course_uid, num_pmu, byte_offset).
  Phase 2: sort chronologically.
  Phase 3: per-course, read records, snapshot pre-race stats -> write,
           then update hippodrome state.

Also tracks per-horse the set of hippodromes visited, to compute
hs_hippo_home_advantage (returning vs newcomer win rates).

Features (8):
  hs_hippo_avg_field_size  : average field size at this hippodrome (historically)
  hs_hippo_favorite_wr     : historical win rate of the favorite at this hippodrome
  hs_hippo_upset_rate      : rate of non-top3-odds horses winning (upset frequency)
  hs_hippo_avg_winning_odds: average cote_finale of winners at this hippodrome
  hs_hippo_total_races     : total number of races held at this hippodrome (popularity)
  hs_hippo_draw_importance : variance of draw-position win rates (high = draw matters)
  hs_hippo_avg_allocation  : average race allocation at this hippodrome (quality level)
  hs_hippo_home_advantage  : rate at which horses who've raced here before outperform
                             newcomers (returning_wr - newcomer_wr)

Produces:
  hippodrome_stats.jsonl  in OUTPUT_DIR

Usage:
    python feature_builders/hippodrome_stats_builder.py
    python feature_builders/hippodrome_stats_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
    python feature_builders/hippodrome_stats_builder.py --output-dir D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/hippodrome_stats
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
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/hippodrome_stats")

_LOG_EVERY = 500_000

# Cap on the size of the winning_odds and allocations lists to bound memory.
# Once the cap is reached, new values replace a random slot (reservoir sampling).
_LIST_CAP = 2_000

# Minimum draw occurrences to include a draw slot in variance computation.
_MIN_DRAW_OBS = 3

# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_mean(values: list) -> Optional[float]:
    """Return mean of a list, None if empty."""
    if not values:
        return None
    return sum(values) / len(values)


def _variance_of_rates(draw_wins: dict, draw_total: dict) -> Optional[float]:
    """Compute variance of per-draw win rates, only for draws with >= _MIN_DRAW_OBS starts.

    High variance means draw position strongly affects outcomes.
    """
    rates = []
    for draw, total in draw_total.items():
        if total >= _MIN_DRAW_OBS:
            rates.append(draw_wins.get(draw, 0) / total)
    if len(rates) < 2:
        return None
    mean_r = sum(rates) / len(rates)
    variance = sum((r - mean_r) ** 2 for r in rates) / len(rates)
    return round(variance, 6)


def _bounded_append(lst: list, value: float, cap: int, rng_state: list) -> None:
    """Append value to lst, respecting cap using reservoir sampling.

    rng_state is a mutable list [seed_int] so caller can pass state across calls.
    """
    if len(lst) < cap:
        lst.append(value)
    else:
        # Reservoir sampling: replace a random existing element
        rng_state[0] = (rng_state[0] * 1664525 + 1013904223) & 0xFFFFFFFF
        idx = rng_state[0] % cap
        lst[idx] = value


# ===========================================================================
# HIPPODROME STATE
# ===========================================================================


class _HippoState:
    """Accumulates statistics for a single hippodrome.

    All fields are updated AFTER features are computed (temporal integrity).
    """

    __slots__ = (
        "total_races",
        "total_starters",
        "fav_wins",
        "fav_total",
        "upset_wins",
        "upset_total",
        "winning_odds",
        "allocations",
        "draw_wins",
        "draw_total",
        "returning_wins",
        "returning_total",
        "newcomer_wins",
        "newcomer_total",
        "_rng_odds",
        "_rng_alloc",
    )

    def __init__(self) -> None:
        self.total_races: int = 0
        self.total_starters: int = 0
        self.fav_wins: int = 0
        self.fav_total: int = 0
        self.upset_wins: int = 0
        self.upset_total: int = 0
        self.winning_odds: list = []
        self.allocations: list = []
        self.draw_wins: dict = {}    # draw_pos -> wins
        self.draw_total: dict = {}   # draw_pos -> starts
        self.returning_wins: int = 0
        self.returning_total: int = 0
        self.newcomer_wins: int = 0
        self.newcomer_total: int = 0
        self._rng_odds: list = [42]
        self._rng_alloc: list = [137]

    def snapshot(self) -> dict:
        """Return feature dict from current (pre-race) state."""
        # hs_hippo_avg_field_size
        avg_field = (
            round(self.total_starters / self.total_races, 2)
            if self.total_races > 0
            else None
        )

        # hs_hippo_favorite_wr
        fav_wr = (
            round(self.fav_wins / self.fav_total, 4)
            if self.fav_total > 0
            else None
        )

        # hs_hippo_upset_rate
        upset_rate = (
            round(self.upset_wins / self.upset_total, 4)
            if self.upset_total > 0
            else None
        )

        # hs_hippo_avg_winning_odds
        avg_winning_odds = _safe_mean(self.winning_odds)
        if avg_winning_odds is not None:
            avg_winning_odds = round(avg_winning_odds, 2)

        # hs_hippo_total_races
        total_races = self.total_races if self.total_races > 0 else None

        # hs_hippo_draw_importance
        draw_importance = _variance_of_rates(self.draw_wins, self.draw_total)

        # hs_hippo_avg_allocation
        avg_allocation = _safe_mean(self.allocations)
        if avg_allocation is not None:
            avg_allocation = round(avg_allocation, 2)

        # hs_hippo_home_advantage
        home_advantage: Optional[float] = None
        ret_wr = (
            self.returning_wins / self.returning_total
            if self.returning_total > 0
            else None
        )
        new_wr = (
            self.newcomer_wins / self.newcomer_total
            if self.newcomer_total > 0
            else None
        )
        if ret_wr is not None and new_wr is not None:
            home_advantage = round(ret_wr - new_wr, 4)

        return {
            "hs_hippo_avg_field_size": avg_field,
            "hs_hippo_favorite_wr": fav_wr,
            "hs_hippo_upset_rate": upset_rate,
            "hs_hippo_avg_winning_odds": avg_winning_odds,
            "hs_hippo_total_races": total_races,
            "hs_hippo_draw_importance": draw_importance,
            "hs_hippo_avg_allocation": avg_allocation,
            "hs_hippo_home_advantage": home_advantage,
        }

    def update(
        self,
        starters: int,
        fav_uid: Optional[str],
        winner_uid: Optional[str],
        winner_odds: Optional[float],
        upset: bool,
        allocation: Optional[float],
        runner_draws: list,       # list of (draw_pos, is_winner)
        returning_runners: list,  # list of (is_returning, is_winner)
    ) -> None:
        """Update hippodrome state with results of a completed race."""
        self.total_races += 1
        self.total_starters += starters

        if fav_uid is not None:
            self.fav_total += 1
            if fav_uid == winner_uid:
                self.fav_wins += 1

        if winner_uid is not None:
            self.upset_total += 1
            if upset:
                self.upset_wins += 1

        if winner_odds is not None and winner_odds > 0:
            _bounded_append(self.winning_odds, winner_odds, _LIST_CAP, self._rng_odds)

        if allocation is not None and allocation > 0:
            _bounded_append(self.allocations, allocation, _LIST_CAP, self._rng_alloc)

        for draw_pos, is_winner in runner_draws:
            if draw_pos is None:
                continue
            self.draw_total[draw_pos] = self.draw_total.get(draw_pos, 0) + 1
            if is_winner:
                self.draw_wins[draw_pos] = self.draw_wins.get(draw_pos, 0) + 1

        for is_returning, is_winner in returning_runners:
            if is_returning:
                self.returning_total += 1
                if is_winner:
                    self.returning_wins += 1
            else:
                self.newcomer_total += 1
                if is_winner:
                    self.newcomer_wins += 1


# ===========================================================================
# PARSING HELPERS
# ===========================================================================


def _parse_position(pos) -> Optional[int]:
    """Return integer position_arrivee, or None if DNF/NR."""
    if pos is None:
        return None
    try:
        p = int(pos)
        return p if p > 0 else None
    except (ValueError, TypeError):
        return None


def _parse_float(val) -> Optional[float]:
    """Parse a value to float, None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _parse_int(val) -> Optional[int]:
    """Parse a value to int, None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _horse_id(rec: dict) -> Optional[str]:
    """Return a stable horse identifier from a partant record."""
    hid = rec.get("horse_id") or rec.get("cheval_id")
    if hid:
        return str(hid)
    nom = rec.get("nom_cheval")
    if nom:
        return str(nom).strip().upper()
    return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_hippodrome_stats(input_path: Path, output_path: Path, logger) -> int:
    """Build hippodrome-level stat features from partants_master.jsonl.

    Temporal integrity: index + sort + seek pattern.
      Phase 1: lightweight index (date_str, course_uid, num_pmu, byte_offset).
      Phase 2: sort chronologically.
      Phase 3: per-course, snapshot pre-race features -> write, then update state.

    Returns total number of feature records written.
    """
    logger.info("=== Hippodrome Stats Builder ===")
    logger.info("Input : %s", input_path)
    logger.info("Output: %s", output_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Build lightweight index
    # ------------------------------------------------------------------
    index: list[tuple[str, str, int, int]] = []  # (date_str, course_uid, num_pmu, offset)
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
                logger.info("  [Phase 1] Indexed %d records...", n_read)

            date_str = rec.get("date_reunion_iso") or ""
            course_uid = rec.get("course_uid") or ""
            num_pmu = _parse_int(rec.get("num_pmu")) or 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 done: %d records indexed in %.1fs",
        len(index), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Phase 2 done: sorted in %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3: Process course by course, stream output
    # ------------------------------------------------------------------
    t2 = time.time()

    # hippodrome_normalise -> _HippoState
    hippo_states: dict[str, _HippoState] = defaultdict(_HippoState)

    # horse_id -> set of hippodromes visited
    horse_hippos: dict[str, set] = defaultdict(set)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    n_processed = 0
    total = len(index)

    # Fill-rate counters
    fill_counts = {
        "hs_hippo_avg_field_size": 0,
        "hs_hippo_favorite_wr": 0,
        "hs_hippo_upset_rate": 0,
        "hs_hippo_avg_winning_odds": 0,
        "hs_hippo_total_races": 0,
        "hs_hippo_draw_importance": 0,
        "hs_hippo_avg_allocation": 0,
        "hs_hippo_home_advantage": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date = index[i][0]
            course_idxs: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date
            ):
                course_idxs.append(i)
                i += 1

            if not course_idxs:
                continue

            # Read full records for this course
            course_recs = [_read_at(index[ci][3]) for ci in course_idxs]

            # Identify hippodrome for this course (use first record)
            hippo_name = (
                course_recs[0].get("hippodrome_normalise")
                or course_recs[0].get("hippodrome")
                or ""
            ).strip().upper()

            hippo_st = hippo_states[hippo_name] if hippo_name else None

            # ----------------------------------------------------------
            # Determine favorite for this race (lowest cote_finale among
            # runners with a valid odds; ties broken by num_pmu).
            # Only non-NR runners are considered.
            # ----------------------------------------------------------
            fav_uid: Optional[str] = None
            fav_odds: Optional[float] = None

            valid_runners = []
            for rec in course_recs:
                pos = _parse_position(rec.get("position_arrivee"))
                cote = _parse_float(rec.get("cote_finale"))
                uid = rec.get("partant_uid")
                if cote is not None and cote > 0:
                    valid_runners.append((cote, rec.get("num_pmu") or 99, uid, pos))

            if valid_runners:
                valid_runners.sort(key=lambda x: (x[0], x[1]))
                fav_odds = valid_runners[0][0]
                fav_uid = valid_runners[0][2]

            # Identify top-3 odds uids (for upset determination)
            top3_uids: set = set()
            for cote, _, uid, _ in valid_runners[:3]:
                if uid:
                    top3_uids.add(uid)

            # Identify actual winner
            winner_uid: Optional[str] = None
            winner_odds: Optional[float] = None
            for rec in course_recs:
                pos = _parse_position(rec.get("position_arrivee"))
                if pos == 1:
                    winner_uid = rec.get("partant_uid")
                    winner_odds = _parse_float(rec.get("cote_finale"))
                    break

            # Is winner an upset? (winner not in top-3 odds)
            upset = (
                winner_uid is not None
                and winner_uid not in top3_uids
                and len(top3_uids) > 0
            )

            # Allocation (use first non-null across runners)
            race_allocation: Optional[float] = None
            for rec in course_recs:
                alloc = _parse_float(rec.get("allocation"))
                if alloc is not None and alloc > 0:
                    race_allocation = alloc
                    break

            # Number of starters
            starters = len(course_recs)

            # Per-runner draw data for draw_importance
            runner_draws = []
            for rec in course_recs:
                draw_pos = _parse_int(rec.get("num_pmu"))  # gate/draw proxy
                pos = _parse_position(rec.get("position_arrivee"))
                is_winner = (pos == 1)
                runner_draws.append((draw_pos, is_winner))

            # Per-runner returning/newcomer data
            runner_returning = []
            for rec in course_recs:
                hid = _horse_id(rec)
                pos = _parse_position(rec.get("position_arrivee"))
                is_winner = (pos == 1)
                if hid and hippo_name:
                    is_returning = hippo_name in horse_hippos[hid]
                    runner_returning.append((is_returning, is_winner))

            # ----------------------------------------------------------
            # SNAPSHOT: compute features BEFORE updating state
            # ----------------------------------------------------------
            for rec in course_recs:
                uid = rec.get("partant_uid")
                feat: dict = {"partant_uid": uid}

                if hippo_st is not None:
                    feat.update(hippo_st.snapshot())
                else:
                    feat.update({
                        "hs_hippo_avg_field_size": None,
                        "hs_hippo_favorite_wr": None,
                        "hs_hippo_upset_rate": None,
                        "hs_hippo_avg_winning_odds": None,
                        "hs_hippo_total_races": None,
                        "hs_hippo_draw_importance": None,
                        "hs_hippo_avg_allocation": None,
                        "hs_hippo_home_advantage": None,
                    })

                # Update fill counters
                for k in fill_counts:
                    if feat.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(feat, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # ----------------------------------------------------------
            # UPDATE STATE after all partants' features have been written
            # ----------------------------------------------------------
            if hippo_st is not None and hippo_name:
                hippo_st.update(
                    starters=starters,
                    fav_uid=fav_uid,
                    winner_uid=winner_uid,
                    winner_odds=winner_odds,
                    upset=upset,
                    allocation=race_allocation,
                    runner_draws=runner_draws,
                    returning_runners=runner_returning,
                )

                # Mark each horse as having visited this hippodrome
                for rec in course_recs:
                    hid = _horse_id(rec)
                    if hid:
                        horse_hippos[hid].add(hippo_name)

            n_processed += len(course_recs)
            if n_processed % _LOG_EVERY < len(course_recs):
                logger.info(
                    "  [Phase 3] Processed %d / %d records, %d hippos tracked...",
                    n_processed, total, len(hippo_states),
                )
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Build complete: %d feature records written in %.1fs",
        n_written, elapsed,
    )
    logger.info("Hippodromes tracked: %d", len(hippo_states))
    logger.info("Horses tracked: %d", len(horse_hippos))

    # Fill-rate summary
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0.0
        logger.info("  %-35s %d / %d  (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute hippodrome-level stat features from partants_master.jsonl"
    )
    parser.add_argument(
        "--input", type=str, default=str(INPUT_PARTANTS),
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Output directory for hippodrome_stats.jsonl",
    )
    args = parser.parse_args()

    logger = setup_logging("hippodrome_stats_builder")

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "hippodrome_stats.jsonl"

    build_hippodrome_stats(input_path, output_path, logger)


if __name__ == "__main__":
    main()
