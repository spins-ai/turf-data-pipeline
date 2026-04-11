#!/usr/bin/env python3
"""
feature_builders.survival_hazard_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Survival-analysis / time-to-event features for horse racing data.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant survival/hazard features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the feature values -- no future leakage (snapshot-before-update).

Produces:
  - survival_hazard.jsonl   in output/survival_hazard/

Features per partant (14):
  - days_since_last_race        : ecart_precedent or computed from dates
  - career_duration_days        : days from first race to current race
  - races_per_month             : career intensity (nb_courses / months elapsed)
  - avg_days_between_wins       : mean inter-win interval (None if <2 wins)
  - hazard_win_proxy            : P(win next | days since last win), decaying proxy
  - retirement_risk             : nb_courses_carriere / age_in_years (declining = slowing)
  - inter_race_interval_trend   : slope of recent intervals vs older (positive = slowing)
  - days_since_last_win         : days since most recent victory
  - days_since_last_place       : days since most recent top-3 finish
  - censoring_indicator         : likelihood this is the horse's last race (0..1)
  - career_stage                : 1=early(<10), 2=developing(10-30), 3=peak(30-60), 4=veteran(60+)
  - avg_field_position          : cumulative running average of position_arrivee
  - win_rate_decay              : win rate last 10 races minus career win rate
  - time_weighted_performance   : exponentially weighted recent place rate

Memory-optimised:
  - Per-horse state uses __slots__ and bounded deques
  - gc.collect() every 500K records
  - Atomic .tmp -> rename output

Usage:
    python feature_builders/survival_hazard_builder.py
    python feature_builders/survival_hazard_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/survival_hazard")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

# Progress log every N records
_LOG_EVERY = 500_000

# Rolling window for recent performance
_RECENT_WINDOW = 10

# Exponential decay half-life (in races) for time-weighted performance
_DECAY_HALFLIFE = 8.0
_DECAY_LAMBDA = math.log(2) / _DECAY_HALFLIFE

# Max deque sizes to bound memory
_MAX_RACE_DATES = 60
_MAX_WIN_DATES = 30
_MAX_INTERVALS = 30


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


def _parse_date(date_str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) else None
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


# ===========================================================================
# PER-HORSE STATE TRACKER
# ===========================================================================


class _HorseState:
    """Tracks per-horse temporal state for survival features.

    Uses __slots__ and bounded deques to keep memory manageable
    across hundreds of thousands of horses.
    """

    __slots__ = (
        "first_race_date",
        "last_race_date",
        "last_win_date",
        "last_place_date",
        "total_races",
        "total_wins",
        "total_places",
        "race_dates",
        "win_dates",
        "inter_race_intervals",
        "recent_results",
        "position_sum",
        "position_count",
        "recent_place_flags",
    )

    def __init__(self) -> None:
        self.first_race_date: Optional[datetime] = None
        self.last_race_date: Optional[datetime] = None
        self.last_win_date: Optional[datetime] = None
        self.last_place_date: Optional[datetime] = None
        self.total_races: int = 0
        self.total_wins: int = 0
        self.total_places: int = 0  # top-3 finishes
        self.race_dates: deque = deque(maxlen=_MAX_RACE_DATES)
        self.win_dates: deque = deque(maxlen=_MAX_WIN_DATES)
        self.inter_race_intervals: deque = deque(maxlen=_MAX_INTERVALS)
        # Recent results: list of (is_win: bool) for last N races
        self.recent_results: deque = deque(maxlen=_RECENT_WINDOW)
        # Running average of position
        self.position_sum: float = 0.0
        self.position_count: int = 0
        # Recent place flags for time-weighted performance
        self.recent_place_flags: deque = deque(maxlen=_RECENT_WINDOW * 3)


# ===========================================================================
# FEATURE COMPUTATION (snapshot before update)
# ===========================================================================


def _compute_features(
    state: _HorseState,
    race_date: Optional[datetime],
    ecart_precedent: Optional[float],
    nb_courses_carriere: Optional[int],
    age: Optional[float],
) -> dict[str, Any]:
    """Compute survival/hazard features from current horse state.

    This is called BEFORE updating state with the current race result.
    """
    feats: dict[str, Any] = {}

    # ---------------------------------------------------------------
    # 1. Days since last race
    # ---------------------------------------------------------------
    days_since_last = None
    if ecart_precedent is not None and ecart_precedent > 0:
        days_since_last = round(ecart_precedent, 1)
    elif race_date and state.last_race_date:
        delta = (race_date - state.last_race_date).days
        if delta >= 0:
            days_since_last = float(delta)
    feats["days_since_last_race"] = days_since_last

    # ---------------------------------------------------------------
    # 2. Career duration in days
    # ---------------------------------------------------------------
    career_days = None
    if race_date and state.first_race_date:
        career_days = (race_date - state.first_race_date).days
    feats["career_duration_days"] = career_days

    # ---------------------------------------------------------------
    # 3. Races per month (career intensity)
    # ---------------------------------------------------------------
    races_per_month = None
    if career_days is not None and career_days > 30 and state.total_races >= 2:
        months_elapsed = career_days / 30.44  # avg days per month
        races_per_month = round(state.total_races / months_elapsed, 3)
    feats["races_per_month"] = races_per_month

    # ---------------------------------------------------------------
    # 4. Average days between wins
    # ---------------------------------------------------------------
    avg_days_between_wins = None
    if len(state.win_dates) >= 2:
        win_list = sorted(state.win_dates)
        intervals = []
        for j in range(1, len(win_list)):
            intervals.append((win_list[j] - win_list[j - 1]).days)
        if intervals:
            avg_days_between_wins = round(sum(intervals) / len(intervals), 1)
    feats["avg_days_between_wins"] = avg_days_between_wins

    # ---------------------------------------------------------------
    # 5. Hazard win proxy: decaying probability based on time since last win
    #    P(win) ~ base_rate * exp(-lambda * days_since_last_win)
    #    Horses that won recently have higher hazard.
    # ---------------------------------------------------------------
    hazard_win_proxy = None
    if race_date and state.last_win_date and state.total_races > 0:
        days_since_win = (race_date - state.last_win_date).days
        if days_since_win >= 0:
            base_rate = state.total_wins / state.total_races if state.total_races else 0.0
            # Decay with half-life of ~90 days
            decay = math.exp(-0.0077 * days_since_win)  # ln(2)/90 ~ 0.0077
            hazard_win_proxy = round(base_rate * decay, 5)
    feats["hazard_win_proxy"] = hazard_win_proxy

    # ---------------------------------------------------------------
    # 6. Retirement risk: nb_courses / age_in_years
    # ---------------------------------------------------------------
    retirement_risk = None
    if nb_courses_carriere is not None and age is not None and age > 0:
        retirement_risk = round(nb_courses_carriere / age, 3)
    feats["retirement_risk"] = retirement_risk

    # ---------------------------------------------------------------
    # 7. Inter-race interval trend
    #    Compare mean of recent half vs older half of intervals.
    #    Positive = horse is slowing down (gaps growing).
    # ---------------------------------------------------------------
    inter_race_trend = None
    n_intervals = len(state.inter_race_intervals)
    if n_intervals >= 4:
        intervals_list = list(state.inter_race_intervals)
        mid = n_intervals // 2
        older_mean = sum(intervals_list[:mid]) / mid
        recent_mean = sum(intervals_list[mid:]) / (n_intervals - mid)
        if older_mean > 0:
            inter_race_trend = round((recent_mean - older_mean) / older_mean, 4)
    feats["inter_race_interval_trend"] = inter_race_trend

    # ---------------------------------------------------------------
    # 8. Days since last win
    # ---------------------------------------------------------------
    days_since_last_win = None
    if race_date and state.last_win_date:
        d = (race_date - state.last_win_date).days
        if d >= 0:
            days_since_last_win = d
    feats["days_since_last_win"] = days_since_last_win

    # ---------------------------------------------------------------
    # 9. Days since last place (top 3)
    # ---------------------------------------------------------------
    days_since_last_place = None
    if race_date and state.last_place_date:
        d = (race_date - state.last_place_date).days
        if d >= 0:
            days_since_last_place = d
    feats["days_since_last_place"] = days_since_last_place

    # ---------------------------------------------------------------
    # 10. Censoring indicator: likelihood this is the horse's last race
    #     Combines age (high = more likely), frequency decline, career length.
    #     Score 0..1 where higher = more likely to be last race.
    # ---------------------------------------------------------------
    censoring_indicator = None
    if state.total_races >= 3:
        score = 0.0
        # Age factor: horses >10 years old increasingly likely to retire
        if age is not None and age > 0:
            if age >= 12:
                score += 0.4
            elif age >= 10:
                score += 0.25
            elif age >= 8:
                score += 0.1
        # Frequency decline factor
        if inter_race_trend is not None and inter_race_trend > 0.3:
            score += min(0.3, inter_race_trend * 0.3)
        # Long career factor
        if state.total_races >= 80:
            score += 0.2
        elif state.total_races >= 60:
            score += 0.1
        censoring_indicator = round(min(1.0, score), 3)
    feats["censoring_indicator"] = censoring_indicator

    # ---------------------------------------------------------------
    # 11. Career stage: 1=early, 2=developing, 3=peak, 4=veteran
    # ---------------------------------------------------------------
    career_stage = None
    n = state.total_races
    if n > 0:
        if n < 10:
            career_stage = 1
        elif n < 30:
            career_stage = 2
        elif n < 60:
            career_stage = 3
        else:
            career_stage = 4
    feats["career_stage"] = career_stage

    # ---------------------------------------------------------------
    # 12. Average field position (cumulative running average)
    # ---------------------------------------------------------------
    avg_field_position = None
    if state.position_count > 0:
        avg_field_position = round(state.position_sum / state.position_count, 2)
    feats["avg_field_position"] = avg_field_position

    # ---------------------------------------------------------------
    # 13. Win rate decay: win rate in last 10 races vs career rate
    # ---------------------------------------------------------------
    win_rate_decay = None
    if len(state.recent_results) >= 5 and state.total_races >= 10:
        recent_wins = sum(1 for r in state.recent_results if r)
        recent_wr = recent_wins / len(state.recent_results)
        career_wr = state.total_wins / state.total_races
        win_rate_decay = round(recent_wr - career_wr, 4)
    feats["win_rate_decay"] = win_rate_decay

    # ---------------------------------------------------------------
    # 14. Time-weighted performance: exponentially weighted place rate
    #     More recent races weighted higher. place = top 3.
    # ---------------------------------------------------------------
    time_weighted_perf = None
    n_recent = len(state.recent_place_flags)
    if n_recent >= 3:
        weighted_sum = 0.0
        weight_total = 0.0
        for idx, is_place in enumerate(state.recent_place_flags):
            # idx=0 is oldest, idx=n-1 is most recent
            w = math.exp(_DECAY_LAMBDA * idx)
            weighted_sum += w * (1.0 if is_place else 0.0)
            weight_total += w
        if weight_total > 0:
            time_weighted_perf = round(weighted_sum / weight_total, 4)
    feats["time_weighted_performance"] = time_weighted_perf

    return feats


def _update_state(
    state: _HorseState,
    race_date: Optional[datetime],
    is_gagnant: bool,
    is_place: bool,
    position: Optional[int],
) -> None:
    """Update horse state AFTER computing features for the current race."""
    # Track inter-race interval
    if race_date and state.last_race_date:
        interval = (race_date - state.last_race_date).days
        if interval >= 0:
            state.inter_race_intervals.append(interval)

    # Update dates
    if race_date:
        if state.first_race_date is None:
            state.first_race_date = race_date
        state.last_race_date = race_date
        state.race_dates.append(race_date)

    # Update wins
    state.total_races += 1
    if is_gagnant:
        state.total_wins += 1
        if race_date:
            state.last_win_date = race_date
            state.win_dates.append(race_date)

    # Update places (top 3)
    if is_place:
        state.total_places += 1
        if race_date:
            state.last_place_date = race_date

    # Position tracking
    if position is not None and position > 0:
        state.position_sum += position
        state.position_count += 1

    # Recent results
    state.recent_results.append(is_gagnant)
    state.recent_place_flags.append(is_place)


# ===========================================================================
# MAIN BUILD (memory-optimised: index + seek + streaming output)
# ===========================================================================


def build_survival_hazard_features(input_path: Path, output_path: Path, logger) -> int:
    """Build survival/hazard features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Survival Hazard Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
    index: list[tuple[str, str, int, int]] = []
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
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "days_since_last_race",
        "career_duration_days",
        "races_per_month",
        "avg_days_between_wins",
        "hazard_win_proxy",
        "retirement_risk",
        "inter_race_interval_trend",
        "days_since_last_win",
        "days_since_last_place",
        "censoring_indicator",
        "career_stage",
        "avg_field_position",
        "win_rate_decay",
        "time_weighted_performance",
    ]
    fill_counts = {name: 0 for name in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
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

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]
            race_date = _parse_date(course_date_str)

            # -- Snapshot: compute features BEFORE updating state --
            features_batch: list[tuple[dict, dict, str, Optional[datetime], bool, bool, Optional[int]]] = []

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                if not horse_id:
                    # Still write a record with Nones
                    out_rec = {"partant_uid": rec.get("partant_uid")}
                    for name in feature_names:
                        out_rec[name] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                    n_written += 1
                    continue

                state = horse_states[horse_id]

                # Extract fields
                ecart = _safe_float(rec.get("ecart_precedent"))
                nb_courses = _safe_int(rec.get("nb_courses_carriere"))
                age = _safe_float(rec.get("age"))
                is_gagnant = bool(rec.get("is_gagnant"))
                is_place = bool(rec.get("is_place"))
                position = _safe_int(rec.get("position_arrivee"))

                # Compute features (snapshot before update)
                feats = _compute_features(state, race_date, ecart, nb_courses, age)

                # Build output record
                out_rec = {"partant_uid": rec.get("partant_uid")}
                for name in feature_names:
                    val = feats.get(name)
                    out_rec[name] = val
                    if val is not None:
                        fill_counts[name] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Store update info for post-snapshot phase
                features_batch.append(
                    (rec, feats, horse_id, race_date, is_gagnant, is_place, position)
                )

            # -- Update states AFTER all features are computed for this course --
            for rec, feats, horse_id, rd, is_g, is_p, pos in features_batch:
                _update_state(horse_states[horse_id], rd, is_g, is_p, pos)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Survival hazard build termine: %d features en %.1fs (chevaux uniques: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %-35s: %d/%d (%.1f%%)", k, v, n_written, pct)

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
        description="Construction des features survival/hazard a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/survival_hazard/)",
    )
    args = parser.parse_args()

    logger = setup_logging("survival_hazard_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "survival_hazard.jsonl"
    build_survival_hazard_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
