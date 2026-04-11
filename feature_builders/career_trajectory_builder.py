#!/usr/bin/env python3
"""
feature_builders.career_trajectory_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Career trajectory features -- how a horse's career is evolving over time.

Temporal integrity: for any partant at date D, only races with date < D
contribute to trajectory metrics -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - career_trajectory_features.jsonl  in builder_outputs/career_trajectory/

Features per partant (10):
  - ctr_career_win_rate_trend    : slope of win_rate over last 20 races
  - ctr_career_class_trend       : slope of field_strength over last 10 races
  - ctr_earnings_acceleration    : gains last 5 / gains previous 5
  - ctr_peak_performance_gap     : days since best speed figure or best position
  - ctr_consistency_window       : std dev of positions in last 10 races
  - ctr_distance_exploration     : distinct distance categories in last 10
  - ctr_hippodrome_diversity     : distinct hippodromes in last 10
  - ctr_improving_flag           : 1 if avg_pos_last5 < avg_pos_last10
  - ctr_class_drop_flag          : 1 if current field_strength < avg of last 5
  - ctr_win_recency              : 1/(days_since_last_win + 1)

Usage:
    python feature_builders/career_trajectory_builder.py
    python feature_builders/career_trajectory_builder.py --input path/to/partants_master.jsonl
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
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/career_trajectory")

_LOG_EVERY = 500_000
_WINDOW_20 = 20
_WINDOW_10 = 10
_WINDOW_5 = 5


# ===========================================================================
# DISTANCE CATEGORY HELPER
# ===========================================================================

def _distance_category(dist_m) -> Optional[str]:
    """Map distance in metres to a category string."""
    if dist_m is None:
        return None
    try:
        d = int(dist_m)
    except (ValueError, TypeError):
        return None
    if d <= 0:
        return None
    if d < 1300:
        return "sprint"
    if d < 1800:
        return "mile"
    if d < 2200:
        return "intermediate"
    if d < 2800:
        return "staying"
    return "long"


def _parse_date_days(date_str: str) -> Optional[int]:
    """Convert YYYY-MM-DD to an integer day count for gap calculations."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
        # Approximate days since epoch for gap calculations
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


def _slope(values: list[float]) -> Optional[float]:
    """Compute simple linear regression slope over indexed values.

    x = 0, 1, 2, ..., n-1 (chronological order).
    Returns None if fewer than 3 values.
    """
    n = len(values)
    if n < 3:
        return None
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if den == 0:
        return None
    return round(num / den, 6)


def _safe_stdev(values: list[float]) -> Optional[float]:
    """Population standard deviation. None if fewer than 2 values."""
    n = len(values)
    if n < 2:
        return None
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / n
    return round(math.sqrt(var), 4)


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseState:
    """Track rolling career state for one horse."""

    __slots__ = (
        "positions",       # deque(maxlen=20): last 20 finishing positions
        "field_strengths", # deque(maxlen=20): last 20 field_strength values
        "gains",           # deque(maxlen=20): last 20 gains values
        "dates",           # deque(maxlen=20): last 20 date-as-days values
        "speed_figures",   # deque(maxlen=20): last 20 speed figures
        "win_flags",       # deque(maxlen=20): last 20 win flags (0/1)
        "last_win_date",   # int or None: date-as-days of last win
        "distances",       # deque(maxlen=10): last 10 distance categories
        "hippodromes",     # deque(maxlen=10): last 10 hippodromes
        "best_speed",      # float or None: best speed figure ever
        "best_speed_date", # int or None: date of best speed figure
        "best_pos",        # int or None: best (lowest) position ever
        "best_pos_date",   # int or None: date of best position
    )

    def __init__(self) -> None:
        self.positions: deque = deque(maxlen=_WINDOW_20)
        self.field_strengths: deque = deque(maxlen=_WINDOW_20)
        self.gains: deque = deque(maxlen=_WINDOW_20)
        self.dates: deque = deque(maxlen=_WINDOW_20)
        self.speed_figures: deque = deque(maxlen=_WINDOW_20)
        self.win_flags: deque = deque(maxlen=_WINDOW_20)
        self.last_win_date: Optional[int] = None
        self.distances: deque = deque(maxlen=_WINDOW_10)
        self.hippodromes: deque = deque(maxlen=_WINDOW_10)
        self.best_speed: Optional[float] = None
        self.best_speed_date: Optional[int] = None
        self.best_pos: Optional[int] = None
        self.best_pos_date: Optional[int] = None


# ===========================================================================
# FEATURE COMPUTATION (from snapshot before update)
# ===========================================================================


def _compute_features(
    hs: _HorseState,
    current_field_strength: Optional[float],
    current_date_days: Optional[int],
) -> dict[str, Any]:
    """Compute all 10 trajectory features from the horse's pre-race state."""
    feats: dict[str, Any] = {}

    # 1. ctr_career_win_rate_trend: slope of cumulative win_rate over last 20
    wf = list(hs.win_flags)
    if len(wf) >= 3:
        cumulative_wr = []
        wins_so_far = 0
        for idx, w in enumerate(wf, 1):
            wins_so_far += w
            cumulative_wr.append(wins_so_far / idx)
        feats["ctr_career_win_rate_trend"] = _slope(cumulative_wr)
    else:
        feats["ctr_career_win_rate_trend"] = None

    # 2. ctr_career_class_trend: slope of field_strength over last 10
    fs_list = [v for v in list(hs.field_strengths)[-_WINDOW_10:] if v is not None]
    feats["ctr_career_class_trend"] = _slope(fs_list) if len(fs_list) >= 3 else None

    # 3. ctr_earnings_acceleration: gains last 5 / gains previous 5
    gains_list = [v for v in list(hs.gains) if v is not None]
    if len(gains_list) >= 10:
        prev5 = sum(gains_list[-10:-5])
        last5 = sum(gains_list[-5:])
        if prev5 > 0:
            feats["ctr_earnings_acceleration"] = round(last5 / prev5, 4)
        else:
            feats["ctr_earnings_acceleration"] = None
    else:
        feats["ctr_earnings_acceleration"] = None

    # 4. ctr_peak_performance_gap: days since best speed figure or best position
    if current_date_days is not None:
        best_date = None
        if hs.best_speed_date is not None:
            best_date = hs.best_speed_date
        if hs.best_pos_date is not None:
            if best_date is None or hs.best_pos_date > best_date:
                best_date = hs.best_pos_date
        if best_date is not None:
            feats["ctr_peak_performance_gap"] = max(0, current_date_days - best_date)
        else:
            feats["ctr_peak_performance_gap"] = None
    else:
        feats["ctr_peak_performance_gap"] = None

    # 5. ctr_consistency_window: std dev of positions in last 10
    pos_list = [v for v in list(hs.positions)[-_WINDOW_10:] if v is not None]
    feats["ctr_consistency_window"] = _safe_stdev(pos_list) if len(pos_list) >= 2 else None

    # 6. ctr_distance_exploration: distinct distance categories in last 10
    dist_set = set(v for v in hs.distances if v is not None)
    feats["ctr_distance_exploration"] = len(dist_set) if dist_set else None

    # 7. ctr_hippodrome_diversity: distinct hippodromes in last 10
    hippo_set = set(v for v in hs.hippodromes if v is not None and v != "")
    feats["ctr_hippodrome_diversity"] = len(hippo_set) if hippo_set else None

    # 8. ctr_improving_flag: 1 if avg_pos_last5 < avg_pos_last10
    pos_all = [v for v in list(hs.positions) if v is not None]
    if len(pos_all) >= 10:
        avg5 = sum(pos_all[-5:]) / 5
        avg10 = sum(pos_all[-10:]) / 10
        feats["ctr_improving_flag"] = 1 if avg5 < avg10 else 0
    elif len(pos_all) >= 5:
        # Not enough for last10 comparison
        feats["ctr_improving_flag"] = None
    else:
        feats["ctr_improving_flag"] = None

    # 9. ctr_class_drop_flag: 1 if current field_strength < avg of last 5
    fs_recent = [v for v in list(hs.field_strengths) if v is not None]
    if current_field_strength is not None and len(fs_recent) >= 5:
        avg_fs5 = sum(fs_recent[-5:]) / 5
        feats["ctr_class_drop_flag"] = 1 if current_field_strength < avg_fs5 else 0
    else:
        feats["ctr_class_drop_flag"] = None

    # 10. ctr_win_recency: 1/(days_since_last_win + 1)
    if current_date_days is not None and hs.last_win_date is not None:
        days_gap = max(0, current_date_days - hs.last_win_date)
        feats["ctr_win_recency"] = round(1.0 / (days_gap + 1), 6)
    else:
        feats["ctr_win_recency"] = None

    return feats


# ===========================================================================
# UPDATE HORSE STATE (post-race)
# ===========================================================================


def _update_state(
    hs: _HorseState,
    position: Optional[int],
    field_strength: Optional[float],
    gains: Optional[float],
    date_days: Optional[int],
    speed_figure: Optional[float],
    is_winner: bool,
    distance_cat: Optional[str],
    hippodrome: Optional[str],
) -> None:
    """Update the horse's rolling state after a race."""
    hs.positions.append(position)
    hs.field_strengths.append(field_strength)
    hs.gains.append(gains)
    hs.dates.append(date_days)
    hs.speed_figures.append(speed_figure)
    hs.win_flags.append(1 if is_winner else 0)

    hs.distances.append(distance_cat)
    hs.hippodromes.append(hippodrome)

    if is_winner and date_days is not None:
        hs.last_win_date = date_days

    # Update best speed figure
    if speed_figure is not None:
        if hs.best_speed is None or speed_figure > hs.best_speed:
            hs.best_speed = speed_figure
            hs.best_speed_date = date_days

    # Update best position (lowest = best)
    if position is not None and position > 0:
        if hs.best_pos is None or position < hs.best_pos:
            hs.best_pos = position
            hs.best_pos_date = date_days


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_career_trajectory_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build career trajectory features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Career Trajectory Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
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
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "ctr_career_win_rate_trend",
        "ctr_career_class_trend",
        "ctr_earnings_acceleration",
        "ctr_peak_performance_gap",
        "ctr_consistency_window",
        "ctr_distance_exploration",
        "ctr_hippodrome_diversity",
        "ctr_improving_flag",
        "ctr_class_drop_flag",
        "ctr_win_recency",
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

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                cheval = rec.get("nom_cheval")
                if not cheval:
                    # Still emit a record with Nones
                    out_rec = {
                        "partant_uid": rec.get("partant_uid"),
                        "course_uid": rec.get("course_uid"),
                        "date_reunion_iso": rec.get("date_reunion_iso"),
                    }
                    for k in feature_keys:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[cheval]

                # Extract current race fields
                field_strength = None
                fs_raw = rec.get("field_strength") or rec.get("force_champ")
                if fs_raw is not None:
                    try:
                        field_strength = float(fs_raw)
                    except (ValueError, TypeError):
                        pass

                # Compute features from pre-race state
                feats = _compute_features(hs, field_strength, current_date_days)

                out_rec = {
                    "partant_uid": rec.get("partant_uid"),
                    "course_uid": rec.get("course_uid"),
                    "date_reunion_iso": rec.get("date_reunion_iso"),
                }
                for k in feature_keys:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # Prepare deferred state update
                position = None
                pos_raw = rec.get("place_arrivee") or rec.get("position_arrivee")
                if pos_raw is not None:
                    try:
                        position = int(pos_raw)
                    except (ValueError, TypeError):
                        pass

                gains_val = None
                g_raw = rec.get("gains_course") or rec.get("gains")
                if g_raw is not None:
                    try:
                        gains_val = float(g_raw)
                    except (ValueError, TypeError):
                        pass

                speed_fig = None
                sf_raw = rec.get("speed_figure") or rec.get("vitesse_moyenne")
                if sf_raw is not None:
                    try:
                        speed_fig = float(sf_raw)
                    except (ValueError, TypeError):
                        pass

                is_winner = bool(rec.get("is_gagnant"))

                dist_raw = rec.get("distance") or rec.get("distance_metres")
                dist_cat = _distance_category(dist_raw)

                hippo = rec.get("hippodrome_normalise", "") or ""

                post_updates.append((
                    cheval, position, field_strength, gains_val,
                    current_date_days, speed_fig, is_winner, dist_cat, hippo,
                ))

            # -- Update horse states after race (no leakage) --
            for (
                cheval, position, fs, gains_val,
                date_days, speed_fig, is_winner, dist_cat, hippo,
            ) in post_updates:
                _update_state(
                    horse_state[cheval],
                    position, fs, gains_val,
                    date_days, speed_fig, is_winner, dist_cat, hippo,
                )

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Career trajectory build termine: %d features en %.1fs (chevaux suivis: %d)",
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
        description="Construction des features trajectoire carriere a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/career_trajectory/)",
    )
    args = parser.parse_args()

    logger = setup_logging("career_trajectory_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "career_trajectory_features.jsonl"
    build_career_trajectory_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
