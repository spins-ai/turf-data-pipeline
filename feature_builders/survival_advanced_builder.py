#!/usr/bin/env python3
"""
feature_builders.survival_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Time-to-event survival features for the survival_model module.

Reads partants_master.jsonl in streaming mode, builds a lightweight
index + chronological sort + seek architecture, and computes per-horse
survival statistics (time to next win, career longevity).

Temporal integrity: for any partant at date D, features are computed
from the horse's state BEFORE this race is processed -- no future leakage.

Produces:
  - survival_advanced_features.jsonl  in builder_outputs/survival_advanced/

Features per partant (10):
  - srv_races_since_last_win       : number of races since horse's last win
  - srv_days_since_last_win        : calendar days since last win
  - srv_races_since_last_place     : number of races since last place (top 3)
  - srv_career_win_interval_avg    : average number of races between wins
  - srv_career_win_interval_std    : std dev of races between wins (consistency)
  - srv_hazard_rate_win            : estimated P(win this race) from career pattern
  - srv_hazard_rate_place          : estimated P(place this race) from career pattern
  - srv_career_phase_score         : career lifecycle position (0=debut, 0.5=peak, 1=declining)
  - srv_time_to_next_win_estimate  : predicted races until next win = 1/hazard_rate
  - srv_career_races_remaining_est : estimated races remaining based on age & breed

Usage:
    python feature_builders/survival_advanced_builder.py
    python feature_builders/survival_advanced_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/survival_advanced")

# Progress / GC
_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# Breed-typical career spans (age range in years)
_TROT_RETIREMENT_AGE = 11   # midpoint of 10-12
_GALOP_RETIREMENT_AGE = 6   # midpoint of 5-7
_DEFAULT_RETIREMENT_AGE = 9
# Average races per year (rough estimate for remaining-races calc)
_RACES_PER_YEAR = 10

# Minimum hazard rate floor (avoid division by zero / extreme estimates)
_HAZARD_FLOOR = 0.005


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _days_between(d1: datetime, d2: datetime) -> int:
    """Absolute number of days between two datetimes."""
    return abs((d2 - d1).days)


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ===========================================================================
# PER-HORSE STATE TRACKER
# ===========================================================================


class _HorseState:
    """Lightweight per-horse survival state tracker."""

    __slots__ = (
        "total_races",
        "total_wins",
        "total_places",
        "races_since_last_win",
        "races_since_last_place",
        "last_win_date_days",
        "win_intervals",
        "first_race_date_days",
        "best_position_date_days",
        "recent_positions",
    )

    def __init__(self) -> None:
        self.total_races: int = 0
        self.total_wins: int = 0
        self.total_places: int = 0
        self.races_since_last_win: int = 0
        self.races_since_last_place: int = 0
        # Stored as epoch-days for compact representation; None until first win
        self.last_win_date_days: Optional[int] = None
        # List of race-count gaps between consecutive wins
        self.win_intervals: list[int] = []
        # Epoch-days of first race (for age / career-phase calc)
        self.first_race_date_days: Optional[int] = None
        # Epoch-days of the date when best recent performance was achieved
        self.best_position_date_days: Optional[int] = None
        # Recent finishing positions for career-phase assessment
        self.recent_positions: deque = deque(maxlen=10)


_EPOCH = datetime(1970, 1, 1)


def _to_epoch_days(dt: datetime) -> int:
    return (dt - _EPOCH).days


# ===========================================================================
# FEATURE COMPUTATION (snapshot before update)
# ===========================================================================


def _compute_features(
    state: _HorseState,
    race_date: Optional[datetime],
    age: Optional[int],
    discipline: str,
) -> dict[str, Any]:
    """Compute the 10 survival features from current (pre-race) horse state."""

    feats: dict[str, Any] = {}

    # --- srv_races_since_last_win ---
    if state.total_wins > 0:
        feats["srv_races_since_last_win"] = state.races_since_last_win
    else:
        # Never won: use total races as proxy
        feats["srv_races_since_last_win"] = state.total_races if state.total_races > 0 else None

    # --- srv_days_since_last_win ---
    if state.last_win_date_days is not None and race_date is not None:
        race_days = _to_epoch_days(race_date)
        feats["srv_days_since_last_win"] = race_days - state.last_win_date_days
    else:
        feats["srv_days_since_last_win"] = None

    # --- srv_races_since_last_place ---
    if state.total_places > 0:
        feats["srv_races_since_last_place"] = state.races_since_last_place
    else:
        feats["srv_races_since_last_place"] = state.total_races if state.total_races > 0 else None

    # --- srv_career_win_interval_avg / std ---
    if len(state.win_intervals) >= 1:
        intervals = state.win_intervals
        avg_iv = sum(intervals) / len(intervals)
        feats["srv_career_win_interval_avg"] = round(avg_iv, 2)

        if len(intervals) >= 2:
            variance = sum((x - avg_iv) ** 2 for x in intervals) / len(intervals)
            feats["srv_career_win_interval_std"] = round(math.sqrt(variance), 2)
        else:
            feats["srv_career_win_interval_std"] = None
    else:
        feats["srv_career_win_interval_avg"] = None
        feats["srv_career_win_interval_std"] = None

    # --- srv_hazard_rate_win ---
    # Base rate = wins / total, adjusted downward by current losing streak
    if state.total_races > 0:
        base_wr = state.total_wins / state.total_races
        # Streak adjustment: longer drought -> lower hazard
        # Factor = 1 / (1 + races_since_last_win / total_races)
        streak_factor = 1.0 / (1.0 + state.races_since_last_win / max(state.total_races, 1))
        hazard_win = max(base_wr * streak_factor, _HAZARD_FLOOR) if base_wr > 0 else _HAZARD_FLOOR
        feats["srv_hazard_rate_win"] = round(hazard_win, 6)
    else:
        feats["srv_hazard_rate_win"] = None

    # --- srv_hazard_rate_place ---
    if state.total_races > 0:
        base_pr = state.total_places / state.total_races
        streak_factor_p = 1.0 / (1.0 + state.races_since_last_place / max(state.total_races, 1))
        hazard_place = max(base_pr * streak_factor_p, _HAZARD_FLOOR) if base_pr > 0 else _HAZARD_FLOOR
        feats["srv_hazard_rate_place"] = round(hazard_place, 6)
    else:
        feats["srv_hazard_rate_place"] = None

    # --- srv_career_phase_score ---
    # 0 = debut, 0.5 = peak, 1.0 = declining
    if state.total_races == 0:
        feats["srv_career_phase_score"] = 0.0  # debut
    elif state.total_races > 0 and race_date is not None:
        # Two signals: age-based phase + performance trend
        # Age component
        age_phase = None
        if age is not None:
            disc_upper = discipline.strip().upper() if discipline else ""
            if "TROT" in disc_upper:
                retirement = _TROT_RETIREMENT_AGE
                debut_age = 3
            elif "GALOP" in disc_upper or "PLAT" in disc_upper or "OBSTACLE" in disc_upper:
                retirement = _GALOP_RETIREMENT_AGE
                debut_age = 2
            else:
                retirement = _DEFAULT_RETIREMENT_AGE
                debut_age = 3

            career_span = max(retirement - debut_age, 1)
            age_position = min(max((age - debut_age) / career_span, 0.0), 1.0)
            age_phase = age_position

        # Performance component: compare recent avg position to career avg
        perf_phase = None
        if len(state.recent_positions) >= 3:
            recent_avg = sum(state.recent_positions) / len(state.recent_positions)
            # Lower position = better. If recent avg is worse than career-mean
            # position, horse may be declining.
            # Simplification: recent positions > 3 suggests declining
            if recent_avg <= 2.0:
                perf_phase = 0.4  # still near peak
            elif recent_avg <= 4.0:
                perf_phase = 0.5  # peak/plateau
            elif recent_avg <= 6.0:
                perf_phase = 0.7  # starting to decline
            else:
                perf_phase = 0.9  # declining

        if age_phase is not None and perf_phase is not None:
            feats["srv_career_phase_score"] = round(0.5 * age_phase + 0.5 * perf_phase, 4)
        elif age_phase is not None:
            feats["srv_career_phase_score"] = round(age_phase, 4)
        elif perf_phase is not None:
            feats["srv_career_phase_score"] = round(perf_phase, 4)
        else:
            # Fallback: use race count as rough proxy
            feats["srv_career_phase_score"] = round(min(state.total_races / 80.0, 1.0), 4)
    else:
        feats["srv_career_phase_score"] = None

    # --- srv_time_to_next_win_estimate ---
    h_win = feats.get("srv_hazard_rate_win")
    if h_win is not None and h_win > 0:
        feats["srv_time_to_next_win_estimate"] = round(1.0 / h_win, 2)
    else:
        feats["srv_time_to_next_win_estimate"] = None

    # --- srv_career_races_remaining_est ---
    if age is not None:
        disc_upper = discipline.strip().upper() if discipline else ""
        if "TROT" in disc_upper:
            retirement = _TROT_RETIREMENT_AGE
        elif "GALOP" in disc_upper or "PLAT" in disc_upper or "OBSTACLE" in disc_upper:
            retirement = _GALOP_RETIREMENT_AGE
        else:
            retirement = _DEFAULT_RETIREMENT_AGE

        years_left = max(retirement - age, 0)
        feats["srv_career_races_remaining_est"] = years_left * _RACES_PER_YEAR
    else:
        feats["srv_career_races_remaining_est"] = None

    return feats


# ===========================================================================
# STATE UPDATE (after snapshot)
# ===========================================================================


def _update_state(
    state: _HorseState,
    is_winner: bool,
    is_placed: bool,
    position: Optional[int],
    race_date: Optional[datetime],
) -> None:
    """Update per-horse state AFTER features have been snapshotted."""

    # Track first race date
    if race_date is not None and state.first_race_date_days is None:
        state.first_race_date_days = _to_epoch_days(race_date)

    state.total_races += 1

    if is_winner:
        # Record interval since previous win (in races)
        if state.total_wins > 0:
            state.win_intervals.append(state.races_since_last_win)
        else:
            # First win: interval from career start
            state.win_intervals.append(state.total_races - 1)
        state.total_wins += 1
        state.races_since_last_win = 0
        if race_date is not None:
            state.last_win_date_days = _to_epoch_days(race_date)
    else:
        state.races_since_last_win += 1

    if is_placed:
        state.total_places += 1
        state.races_since_last_place = 0
    else:
        state.races_since_last_place += 1

    # Track recent positions
    if position is not None and position > 0:
        state.recent_positions.append(position)

    # Track best-position date for career-phase
    if position is not None and position <= 3 and race_date is not None:
        state.best_position_date_days = _to_epoch_days(race_date)


# ===========================================================================
# MAIN BUILD (index + sort + seek)
# ===========================================================================


def build_survival_features(input_path: Path, output_path: Path, logger) -> int:
    """Build survival features from partants_master.jsonl.

    Architecture:
      1. Read minimal sort keys + byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Seek-read records from disk, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Survival Advanced Builder (index + seek) ===")
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
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "srv_races_since_last_win",
        "srv_days_since_last_win",
        "srv_races_since_last_place",
        "srv_career_win_interval_avg",
        "srv_career_win_interval_std",
        "srv_hazard_rate_win",
        "srv_hazard_rate_place",
        "srv_career_phase_score",
        "srv_time_to_next_win_estimate",
        "srv_career_races_remaining_est",
    ]
    fill_counts = {name: 0 for name in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(file_offset: int) -> dict:
            fin.seek(file_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            position = rec.get("place_officielle") or rec.get("place_arrivee")
            pos_int = _safe_int(position)

            is_gagnant = bool(rec.get("is_gagnant"))
            # Placed = top 3
            is_placed = pos_int is not None and 1 <= pos_int <= 3

            age_val = rec.get("age") or rec.get("age_cheval")
            age_int = _safe_int(age_val)

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            if isinstance(discipline, str):
                discipline = discipline.strip().upper()

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "cheval": rec.get("nom_cheval"),
                "gagnant": is_gagnant,
                "placed": is_placed,
                "position": pos_int,
                "age": age_int,
                "discipline": discipline,
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

            # Read this course's records from disk via seek
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            race_date = _parse_date(course_date_str)

            # -- Snapshot pre-race features (temporal integrity) --
            post_updates: list[tuple[str, bool, bool, Optional[int], Optional[datetime]]] = []

            for rec in course_group:
                cheval = rec["cheval"]

                out_rec: dict[str, Any] = {
                    "partant_uid": rec["uid"],
                    "course_uid": rec["course"],
                    "date_reunion_iso": rec["date"],
                }

                if cheval:
                    st = horse_state[cheval]
                    feats = _compute_features(st, race_date, rec["age"], rec["discipline"])
                    out_rec.update(feats)

                    for fname in feature_names:
                        if feats.get(fname) is not None:
                            fill_counts[fname] += 1
                else:
                    # No horse identifier -- output nulls
                    for fname in feature_names:
                        out_rec[fname] = None

                fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Queue state update
                post_updates.append((
                    cheval,
                    rec["gagnant"],
                    rec["placed"],
                    rec["position"],
                    race_date,
                ))

            # -- Update states AFTER snapshot --
            for cheval, is_winner, is_placed, position, rd in post_updates:
                if not cheval:
                    continue
                _update_state(horse_state[cheval], is_winner, is_placed, position, rd)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)

            # Periodic garbage collection
            if n_processed % _GC_EVERY < len(course_group):
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Survival build termine: %d features en %.1fs (chevaux uniques: %d)",
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
        description="Construction des features survie avancees a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/survival_advanced/)",
    )
    args = parser.parse_args()

    logger = setup_logging("survival_advanced_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "survival_advanced_features.jsonl"
    build_survival_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
