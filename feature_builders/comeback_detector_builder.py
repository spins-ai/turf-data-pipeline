#!/usr/bin/env python3
"""
feature_builders.comeback_detector_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features for the "retour_forme_hidden" module -- detecting horses about
to return to form after a decline.

Reads partants_master.jsonl in streaming mode (index + chronological sort
+ seek).  Tracks per-horse career peaks and recent troughs to identify
comeback signals.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the state -- no future leakage.  Snapshot is taken BEFORE
the state is updated with the current race result.

Produces:
  - comeback_detector_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/comeback_detector/

Features per partant (10):
  - cmb_peak_position_career       : best ever finishing position
  - cmb_peak_to_current_gap        : current avg position (last 3) minus career best
  - cmb_days_since_peak            : days since best performance
  - cmb_losing_streak_length       : current consecutive non-place streak
  - cmb_has_won_here_before        : 1 if horse has won at this hippodrome
  - cmb_has_won_distance_before    : 1 if horse has won at this distance (+/- 200m)
  - cmb_class_drop_from_peak       : peak field_strength minus current field_strength
  - cmb_rest_after_bad_run         : days since last race when last 3 were poor
  - cmb_equipment_change_signal    : 1 if oeilleres or deferre changed from last race
  - cmb_comeback_composite         : weighted score combining signals

Usage:
    python feature_builders/comeback_detector_builder.py
    python feature_builders/comeback_detector_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/comeback_detector")

# Progress / gc every N records
_LOG_EVERY = 500_000

# "Placed" threshold (top 3 = in the money)
_PLACE_THRESHOLD = 3

# Distance tolerance for "won at this distance" (+/- metres)
_DISTANCE_TOLERANCE = 200

# Poor-run threshold: avg position > this in last 3 = "bad form"
_POOR_RUN_THRESHOLD = 6


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _date_to_ordinal(date_str: str) -> Optional[int]:
    """Convert ISO date string (YYYY-MM-DD) to ordinal (days since epoch).

    Returns None on failure.  Using ordinal avoids datetime object overhead.
    """
    if not date_str or len(date_str) < 10:
        return None
    try:
        y = int(date_str[:4])
        m = int(date_str[5:7])
        d = int(date_str[8:10])
        # Simplified ordinal: good enough for day-difference calculations
        # (exact Gregorian ordinal)
        from datetime import date
        return date(y, m, d).toordinal()
    except (ValueError, TypeError):
        return None


def _distance_band(dist_m: Optional[int]) -> Optional[int]:
    """Round distance to nearest 200m band for win tracking.

    E.g. 2100 -> 2100, 2150 -> 2200, 1875 -> 1800.
    Returns centre of band.
    """
    if dist_m is None or dist_m <= 0:
        return None
    return round(dist_m / _DISTANCE_TOLERANCE) * _DISTANCE_TOLERANCE


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Lightweight per-horse career tracker.

    Uses __slots__ to minimise memory across ~200K+ horses.
    """

    __slots__ = (
        "best_position",
        "best_field_strength",
        "best_date",
        "recent_positions",
        "recent_field_strengths",
        "hippo_wins",
        "distance_wins",
        "last_oeilleres",
        "last_deferre",
        "last_date",
        "current_non_place_streak",
    )

    def __init__(self) -> None:
        self.best_position: Optional[int] = None
        self.best_field_strength: Optional[float] = None
        self.best_date: Optional[int] = None           # ordinal
        self.recent_positions: deque = deque(maxlen=5)
        self.recent_field_strengths: deque = deque(maxlen=5)
        self.hippo_wins: set = set()                    # hippodromes where horse won
        self.distance_wins: set = set()                 # distance bands where horse won
        self.last_oeilleres: Optional[str] = None
        self.last_deferre: Optional[str] = None
        self.last_date: Optional[int] = None            # ordinal
        self.current_non_place_streak: int = 0


# ===========================================================================
# FEATURE COMPUTATION (snapshot BEFORE update)
# ===========================================================================


def _compute_features(
    state: _HorseState,
    current_date_ord: Optional[int],
    hippo: str,
    distance_m: Optional[int],
    field_strength: Optional[float],
) -> dict[str, Any]:
    """Compute 10 comeback features from horse state snapshot.

    All values are based on state BEFORE this race (temporal integrity).
    """
    feats: dict[str, Any] = {}

    # 1. cmb_peak_position_career
    feats["cmb_peak_position_career"] = state.best_position

    # 2. cmb_peak_to_current_gap: avg of last 3 positions minus career best
    if state.best_position is not None and len(state.recent_positions) >= 1:
        last_3 = list(state.recent_positions)[-3:] if len(state.recent_positions) >= 3 else list(state.recent_positions)
        avg_recent = sum(last_3) / len(last_3)
        feats["cmb_peak_to_current_gap"] = round(avg_recent - state.best_position, 2)
    else:
        feats["cmb_peak_to_current_gap"] = None

    # 3. cmb_days_since_peak
    if state.best_date is not None and current_date_ord is not None:
        feats["cmb_days_since_peak"] = current_date_ord - state.best_date
    else:
        feats["cmb_days_since_peak"] = None

    # 4. cmb_losing_streak_length
    feats["cmb_losing_streak_length"] = state.current_non_place_streak if len(state.recent_positions) > 0 else None

    # 5. cmb_has_won_here_before
    if hippo:
        feats["cmb_has_won_here_before"] = 1 if hippo in state.hippo_wins else 0
    else:
        feats["cmb_has_won_here_before"] = None

    # 6. cmb_has_won_distance_before
    dist_band = _distance_band(distance_m)
    if dist_band is not None:
        feats["cmb_has_won_distance_before"] = 1 if dist_band in state.distance_wins else 0
    else:
        feats["cmb_has_won_distance_before"] = None

    # 7. cmb_class_drop_from_peak
    if state.best_field_strength is not None and field_strength is not None:
        feats["cmb_class_drop_from_peak"] = round(state.best_field_strength - field_strength, 4)
    else:
        feats["cmb_class_drop_from_peak"] = None

    # 8. cmb_rest_after_bad_run: days since last race IF last 3 were poor
    if (
        state.last_date is not None
        and current_date_ord is not None
        and len(state.recent_positions) >= 3
    ):
        last_3 = list(state.recent_positions)[-3:]
        avg_last_3 = sum(last_3) / len(last_3)
        if avg_last_3 > _POOR_RUN_THRESHOLD:
            feats["cmb_rest_after_bad_run"] = current_date_ord - state.last_date
        else:
            feats["cmb_rest_after_bad_run"] = None
    else:
        feats["cmb_rest_after_bad_run"] = None

    # 9. cmb_equipment_change_signal
    # Check if oeilleres or deferre changed from last race
    feats["cmb_equipment_change_signal"] = None  # default, overridden below

    # 10. cmb_comeback_composite (computed after all signals ready)
    feats["cmb_comeback_composite"] = None

    return feats


def _equipment_change_signal(
    state: _HorseState,
    current_oeilleres: Optional[str],
    current_deferre: Optional[str],
) -> Optional[int]:
    """Return 1 if oeilleres or deferre changed from last race, 0 if same, None if no history."""
    if state.last_oeilleres is None and state.last_deferre is None:
        return None
    changed = False
    if state.last_oeilleres is not None and current_oeilleres is not None:
        if state.last_oeilleres != current_oeilleres:
            changed = True
    if state.last_deferre is not None and current_deferre is not None:
        if state.last_deferre != current_deferre:
            changed = True
    return 1 if changed else 0


def _comeback_composite(feats: dict[str, Any]) -> Optional[float]:
    """Weighted composite score combining key comeback signals.

    Formula: class_drop * rest * has_won_here (with additive bonuses).

    Returns a score in [0, ~10+] range, or None if key inputs missing.
    """
    class_drop = feats.get("cmb_class_drop_from_peak")
    rest = feats.get("cmb_rest_after_bad_run")
    won_here = feats.get("cmb_has_won_here_before")
    won_dist = feats.get("cmb_has_won_distance_before")
    equip = feats.get("cmb_equipment_change_signal")

    # Need at least class_drop to compute anything meaningful
    if class_drop is None:
        return None

    score = 0.0

    # Class drop contributes directly (bigger drop = bigger comeback potential)
    if class_drop > 0:
        score += min(class_drop, 5.0)  # cap at 5 points

    # Rest factor: long rest after bad run = trainer preparing
    if rest is not None and rest > 14:
        score += min(rest / 30.0, 2.0)  # up to 2 points for ~60 day rest

    # Course knowledge
    if won_here == 1:
        score += 1.5
    if won_dist == 1:
        score += 1.0

    # Equipment change = trainer trying something new
    if equip == 1:
        score += 0.5

    return round(score, 4) if score > 0 else None


# ===========================================================================
# STATE UPDATE (after snapshot)
# ===========================================================================


def _update_state(
    state: _HorseState,
    position: Optional[int],
    is_winner: bool,
    date_ord: Optional[int],
    hippo: str,
    distance_m: Optional[int],
    field_strength: Optional[float],
    oeilleres: Optional[str],
    deferre: Optional[str],
) -> None:
    """Update horse state after this race."""
    if position is not None:
        state.recent_positions.append(position)

        # Update peak position (lower is better)
        if state.best_position is None or position < state.best_position:
            state.best_position = position
            state.best_date = date_ord

        # Non-place streak
        if position <= _PLACE_THRESHOLD:
            state.current_non_place_streak = 0
        else:
            state.current_non_place_streak += 1

    if field_strength is not None:
        state.recent_field_strengths.append(field_strength)
        if state.best_field_strength is None or field_strength > state.best_field_strength:
            state.best_field_strength = field_strength

    if is_winner:
        if hippo:
            state.hippo_wins.add(hippo)
        dist_band = _distance_band(distance_m)
        if dist_band is not None:
            state.distance_wins.add(dist_band)

    if oeilleres is not None:
        state.last_oeilleres = oeilleres
    if deferre is not None:
        state.last_deferre = deferre
    if date_ord is not None:
        state.last_date = date_ord


# ===========================================================================
# MAIN BUILD (index + sort + seek)
# ===========================================================================


def build_comeback_features(input_path: Path, output_path: Path, logger) -> int:
    """Build comeback detector features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process per-horse,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Comeback Detector Builder (index + sort + seek) ===")
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

    # -- Phase 3: Process record by record, streaming output --
    t2 = time.time()
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "cmb_peak_position_career",
        "cmb_peak_to_current_gap",
        "cmb_days_since_peak",
        "cmb_losing_streak_length",
        "cmb_has_won_here_before",
        "cmb_has_won_distance_before",
        "cmb_class_drop_from_peak",
        "cmb_rest_after_bad_run",
        "cmb_equipment_change_signal",
        "cmb_comeback_composite",
    ]
    fill_counts = {k: 0 for k in feature_names}

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

            date_ord = _date_to_ordinal(course_date_str)

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Snapshot BEFORE update (temporal integrity) --
            snapshots: list[dict[str, Any]] = []

            for rec in course_records:
                cheval = rec.get("nom_cheval") or ""
                partant_uid = rec.get("partant_uid") or ""
                course_uid_rec = rec.get("course_uid") or ""
                date_iso = rec.get("date_reunion_iso") or ""
                hippo = rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
                distance_m = _safe_int(rec.get("distance") or rec.get("distance_metres"))
                field_strength = _safe_float(rec.get("field_strength"))
                oeilleres = rec.get("oeilleres")
                deferre = rec.get("deferre")

                if not cheval:
                    # No horse name => emit empty features
                    out_rec = {
                        "partant_uid": partant_uid,
                        "course_uid": course_uid_rec,
                        "date_reunion_iso": date_iso,
                    }
                    for fn in feature_names:
                        out_rec[fn] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                    n_written += 1
                    snapshots.append(None)
                    continue

                state = horse_states[cheval]

                # Compute features from PRE-RACE state
                feats = _compute_features(state, date_ord, hippo, distance_m, field_strength)

                # Equipment change signal (needs current record info)
                equip_sig = _equipment_change_signal(
                    state,
                    str(oeilleres).strip() if oeilleres is not None else None,
                    str(deferre).strip() if deferre is not None else None,
                )
                feats["cmb_equipment_change_signal"] = equip_sig

                # Composite score
                feats["cmb_comeback_composite"] = _comeback_composite(feats)

                # Write output
                out_rec = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_rec,
                    "date_reunion_iso": date_iso,
                }
                for fn in feature_names:
                    val = feats.get(fn)
                    out_rec[fn] = val
                    if val is not None:
                        fill_counts[fn] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Store info needed for state update
                snapshots.append(rec)

            # -- Update states AFTER all snapshots for this course --
            for rec in snapshots:
                if rec is None:
                    continue

                cheval = rec.get("nom_cheval") or ""
                if not cheval:
                    continue

                state = horse_states[cheval]
                position = _safe_int(rec.get("place") or rec.get("place_officielle"))
                is_winner = bool(rec.get("is_gagnant"))
                hippo = rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
                distance_m = _safe_int(rec.get("distance") or rec.get("distance_metres"))
                field_strength = _safe_float(rec.get("field_strength"))
                oeilleres = rec.get("oeilleres")
                deferre = rec.get("deferre")

                _update_state(
                    state,
                    position,
                    is_winner,
                    date_ord,
                    hippo,
                    distance_m,
                    field_strength,
                    str(oeilleres).strip() if oeilleres is not None else None,
                    str(deferre).strip() if deferre is not None else None,
                )

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Comeback detector build termine: %d features en %.1fs (chevaux uniques: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
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
        description="Comeback detector: features retour de forme cache"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/comeback_detector/)",
    )
    args = parser.parse_args()

    logger = setup_logging("comeback_detector_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "comeback_detector_features.jsonl"
    build_comeback_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
