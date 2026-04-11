#!/usr/bin/env python3
"""
feature_builders.career_phase_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Classifies each horse's career phase and produces related features that
capture where the horse sits on its performance arc.

Temporal integrity: for any partant at date D, only races strictly before D
(or earlier in the same chronological sort) contribute to the horse's
accumulated state — no future leakage.

The file is processed in two passes:
  Pass 1  – stream + sort all records chronologically.
  Pass 2  – iterate in order; for each record read state BEFORE updating it.

Produces:
  - career_phase_features.jsonl   in output/career_phase/

Features per partant (8):
  cp_career_phase       : int  0=debut(1-5), 1=developing(6-20), 2=peak(21-60), 3=veteran(61+)
  cp_races_at_phase     : int  number of races so far in the current phase
  cp_phase_win_rate     : float win rate accumulated in the current phase (None if 0 races in phase)
  cp_improving_phase    : int  1 if current-phase win rate > previous-phase win rate, else 0 (None if no prev phase)
  cp_career_arc         : float slope of position_pct over full career (positive = getting worse)
  cp_peak_performance_pct: float best 3-race rolling win rate / overall career win rate (None if no career wins)
  cp_debut_quality      : float mean position_pct over first 3 races (low = impressive)
  cp_longevity_bonus    : int  1 if horse has >60 career races AND career win rate >10%, else 0

Usage:
    python feature_builders/career_phase_builder.py
    python feature_builders/career_phase_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
    python feature_builders/career_phase_builder.py --output D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/career_phase
"""

from __future__ import annotations

import argparse
import gc
import json
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/career_phase")

# Fallback candidates (relative to project root) if the fixed path is absent
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
_OUTPUT_FALLBACK = _PROJECT_ROOT / "output" / "career_phase"

_LOG_EVERY = 500_000

# Career phase thresholds (races completed BEFORE this race)
# Phase 0 = debut  : 0-4 prior races  (i.e. total_races in [0, 4])
# Phase 1 = developing: 5-20
# Phase 2 = peak   : 21-60
# Phase 3 = veteran: 61+
_PHASE_THRESHOLDS = [5, 21, 61]   # lower bounds of phases 1, 2, 3


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _career_phase(total_races: int) -> int:
    """Map number of career races (BEFORE current race) to phase index."""
    if total_races < 5:
        return 0   # debut
    elif total_races < 21:
        return 1   # developing
    elif total_races < 61:
        return 2   # peak
    else:
        return 3   # veteran


def _position_pct(position: Any, nb_partants: Any) -> Optional[float]:
    """Normalise finishing position to [0, 1] where 0 = winner, 1 = last.

    Returns None if data are unavailable or invalid.
    """
    pos = _safe_int(position)
    nb = _safe_int(nb_partants)
    if pos is None or nb is None or nb < 2 or pos < 1 or pos > nb:
        return None
    return round((pos - 1) / (nb - 1), 6)


def _linear_slope(values: list[float]) -> Optional[float]:
    """Least-squares slope of values indexed 0, 1, 2, …

    Returns None if fewer than 2 data points.
    """
    n = len(values)
    if n < 2:
        return None
    xs = list(range(n))
    mx = (n - 1) / 2.0
    my = sum(values) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, values))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None
    return round(num / den, 6)


def _best_rolling3_win_rate(position_pcts: list[float]) -> Optional[float]:
    """Best 3-race rolling win rate over a sequence of position_pcts.

    A 'win' in this context means position_pct == 0 (i.e. position 1).
    Returns None if fewer than 3 data points.
    """
    n = len(position_pcts)
    if n < 3:
        return None
    best = 0.0
    for i in range(n - 2):
        window = position_pcts[i: i + 3]
        wins = sum(1 for p in window if p == 0.0)
        wr = wins / 3.0
        if wr > best:
            best = wr
    return round(best, 4)


def _resolve_input(cli_arg: Optional[str]) -> Path:
    if cli_arg:
        p = Path(cli_arg)
        if p.exists():
            return p
        raise FileNotFoundError(f"Input file not found: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"No input file found among default candidates. Pass --input explicitly.\n"
        f"Tried: {_INPUT_CANDIDATES}"
    )


def _resolve_output(cli_arg: Optional[str]) -> Path:
    if cli_arg:
        return Path(cli_arg)
    if OUTPUT_DIR.parent.exists():
        return OUTPUT_DIR
    return _OUTPUT_FALLBACK


# ===========================================================================
# HORSE STATE
# ===========================================================================


class HorseState:
    """Mutable per-horse accumulator — updated AFTER features are read."""

    __slots__ = (
        "total_races",
        "phase_wins",
        "phase_total",
        "all_position_pcts",
        "first3_positions",
        "rolling_wins_3",
    )

    def __init__(self) -> None:
        self.total_races: int = 0
        # per_phase_wins / per_phase_total indexed by phase 0-3
        self.phase_wins: dict[int, int] = defaultdict(int)
        self.phase_total: dict[int, int] = defaultdict(int)
        # bounded list of position_pcts (keep max 200 to cap RAM)
        self.all_position_pcts: list[float] = []
        # first 3 finishing position_pcts
        self.first3_positions: list[float] = []
        # last 3 wins (1 if won, 0 otherwise) for rolling peak calculation
        self.rolling_wins_3: deque = deque(maxlen=3)

    def get_phase_win_rate(self, phase: int) -> Optional[float]:
        total = self.phase_total[phase]
        if total == 0:
            return None
        return round(self.phase_wins[phase] / total, 4)

    def get_previous_phase_win_rate(self, current_phase: int) -> Optional[float]:
        if current_phase == 0:
            return None
        prev = current_phase - 1
        return self.get_phase_win_rate(prev)

    def career_win_rate(self) -> Optional[float]:
        total = self.total_races
        if total == 0:
            return None
        wins = sum(self.phase_wins.values())
        return round(wins / total, 4)

    def update(self, phase: int, pos_pct: Optional[float], is_win: bool) -> None:
        self.total_races += 1
        self.phase_total[phase] += 1
        if is_win:
            self.phase_wins[phase] += 1
        if pos_pct is not None:
            if len(self.all_position_pcts) < 200:
                self.all_position_pcts.append(pos_pct)
            if len(self.first3_positions) < 3:
                self.first3_positions.append(pos_pct)
        self.rolling_wins_3.append(1 if is_win else 0)


# ===========================================================================
# MAIN BUILDER
# ===========================================================================


def build_career_phase_features(input_path: Path, output_dir: Path) -> None:
    logger = setup_logging("career_phase_builder")
    logger.info("Input : %s", input_path)
    logger.info("Output: %s", output_dir)

    # ── Pass 1: load + sort ──────────────────────────────────────────
    logger.info("Pass 1: loading records...")
    t0 = time.time()
    records: list[dict[str, Any]] = []

    with open(input_path, "r", encoding="utf-8") as fh:
        for i, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
            if i % _LOG_EVERY == 0:
                logger.info("  loaded %d records…", i)

    logger.info("  loaded %d records in %.1fs", len(records), time.time() - t0)

    logger.info("Pass 1: sorting chronologically…")
    records.sort(
        key=lambda r: (
            str(r.get("date_reunion_iso", "") or "")[:10],
            str(r.get("course_uid", "") or ""),
            _safe_int(r.get("num_pmu")) or 0,
        )
    )
    logger.info("  sorted %d records.", len(records))

    # ── Pass 2: compute features ─────────────────────────────────────
    logger.info("Pass 2: computing career-phase features…")

    horse_states: dict[str, HorseState] = {}
    results: list[dict[str, Any]] = []
    t1 = time.time()

    for idx, rec in enumerate(records):
        if idx % _LOG_EVERY == 0 and idx > 0:
            logger.info("  processed %d / %d records…", idx, len(records))

        # ── Identify horse ───────────────────────────────────────────
        partant_uid = rec.get("partant_uid") or ""
        horse_id = (
            rec.get("horse_id")
            or rec.get("nom_cheval")
            or ""
        )
        # Use partant_uid as primary key; fall back to horse identifier
        horse_key = str(horse_id).upper().strip() if horse_id else str(partant_uid)
        if not horse_key:
            horse_key = f"__unknown_{idx}"

        # ── Parse race outcome ───────────────────────────────────────
        position = rec.get("position_arrivee")
        nb_partants = rec.get("nombre_partants")
        pos_pct = _position_pct(position, nb_partants)
        is_win = (_safe_int(position) == 1)

        # ── Get or create state (state reflects races BEFORE this one) ──
        if horse_key not in horse_states:
            horse_states[horse_key] = HorseState()
        state = horse_states[horse_key]

        # ── Read features from CURRENT state (before update) ────────
        total_before = state.total_races
        phase = _career_phase(total_before)

        # 1. cp_career_phase
        cp_career_phase = phase

        # 2. cp_races_at_phase
        cp_races_at_phase = state.phase_total[phase]

        # 3. cp_phase_win_rate
        cp_phase_win_rate = state.get_phase_win_rate(phase)

        # 4. cp_improving_phase
        prev_wr = state.get_previous_phase_win_rate(phase)
        curr_wr = cp_phase_win_rate
        if prev_wr is None:
            cp_improving_phase = None
        else:
            if curr_wr is None:
                cp_improving_phase = None
            else:
                cp_improving_phase = 1 if curr_wr > prev_wr else 0

        # 5. cp_career_arc — slope of position_pct over entire career
        if len(state.all_position_pcts) >= 2:
            cp_career_arc = _linear_slope(state.all_position_pcts)
        else:
            cp_career_arc = None

        # 6. cp_peak_performance_pct — best 3-race rolling win rate / overall career win rate
        career_wr = state.career_win_rate()
        best_rolling = _best_rolling3_win_rate(state.all_position_pcts)
        if best_rolling is None or career_wr is None or career_wr == 0.0:
            cp_peak_performance_pct = None
        else:
            cp_peak_performance_pct = round(best_rolling / career_wr, 4)

        # 7. cp_debut_quality — mean position_pct over first 3 races
        if state.first3_positions:
            cp_debut_quality = round(sum(state.first3_positions) / len(state.first3_positions), 4)
        else:
            cp_debut_quality = None

        # 8. cp_longevity_bonus — >60 career races AND win rate >10%
        if total_before > 60 and career_wr is not None and career_wr > 0.10:
            cp_longevity_bonus = 1
        else:
            cp_longevity_bonus = 0

        # ── Emit feature record ──────────────────────────────────────
        results.append(
            {
                "partant_uid": partant_uid,
                "course_uid": rec.get("course_uid"),
                "horse_key": horse_key,
                "cp_career_phase": cp_career_phase,
                "cp_races_at_phase": cp_races_at_phase,
                "cp_phase_win_rate": cp_phase_win_rate,
                "cp_improving_phase": cp_improving_phase,
                "cp_career_arc": cp_career_arc,
                "cp_peak_performance_pct": cp_peak_performance_pct,
                "cp_debut_quality": cp_debut_quality,
                "cp_longevity_bonus": cp_longevity_bonus,
            }
        )

        # ── Update state AFTER reading features ──────────────────────
        state.update(phase, pos_pct, is_win)

    logger.info("  computed %d feature records in %.1fs", len(results), time.time() - t1)

    # ── Save ─────────────────────────────────────────────────────────
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "career_phase_features.jsonl"

    logger.info("Saving to %s …", output_path)
    with open(output_path, "w", encoding="utf-8") as fh:
        for row in results:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    logger.info("Saved %d records → %s", len(results), output_path)

    # ── Stats summary ────────────────────────────────────────────────
    phase_counts: dict[int, int] = defaultdict(int)
    n_longevity = 0
    n_improving = 0
    n_improving_total = 0
    for row in results:
        phase_counts[row["cp_career_phase"]] += 1
        if row["cp_longevity_bonus"] == 1:
            n_longevity += 1
        if row["cp_improving_phase"] is not None:
            n_improving_total += 1
            if row["cp_improving_phase"] == 1:
                n_improving += 1

    logger.info("Phase distribution: %s", dict(sorted(phase_counts.items())))
    logger.info(
        "Longevity bonus: %d / %d (%.1f%%)",
        n_longevity,
        len(results),
        100.0 * n_longevity / len(results) if results else 0.0,
    )
    if n_improving_total > 0:
        logger.info(
            "Improving phase: %d / %d (%.1f%%)",
            n_improving,
            n_improving_total,
            100.0 * n_improving / n_improving_total,
        )

    # Free memory
    del records, results, horse_states
    gc.collect()
    logger.info("Done.")


# ===========================================================================
# CLI
# ===========================================================================


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build career-phase features for each partant."
    )
    parser.add_argument(
        "--input",
        metavar="PATH",
        default=None,
        help="Path to partants_master.jsonl (default: auto-detected).",
    )
    parser.add_argument(
        "--output",
        metavar="DIR",
        default=None,
        help="Output directory (default: auto-detected).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    input_path = _resolve_input(args.input)
    output_dir = _resolve_output(args.output)
    build_career_phase_features(input_path, output_dir)


if __name__ == "__main__":
    main()
