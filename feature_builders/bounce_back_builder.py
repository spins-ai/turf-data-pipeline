#!/usr/bin/env python3
"""
feature_builders.bounce_back_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features capturing how horses recover after bad performances, whether they
tend to bounce back or continue declining.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant bounce-back features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - bounce_back_features.jsonl   in output/bounce_back/

Features per partant (14):
  - bb_last_position            : position in previous race (raw)
  - bb_last_surprise_factor     : last race position vs implied probability rank
                                  (positive = finished worse than odds suggested)
  - bb_last_underperformed      : 1 if horse finished worse than odds suggested, 0 otherwise
  - bb_bounce_back_rate         : % of times horse improved position after a bad race
  - bb_decline_rate             : % of times horse got worse after a bad race
  - bb_streak_direction         : 1=improving, -1=declining, 0=oscillating (last 5 races)
  - bb_post_incident_avg_pos    : avg position in race after having an incident
  - bb_post_disq_avg_pos        : avg next-race position after disqualification
  - bb_recovery_speed           : races needed after a bad run to return to average
  - bb_form_reversal_prob       : probability of alternating good/bad based on history
  - bb_position_volatility      : std dev of last 5 positions
  - bb_beaten_lengths_trend     : slope of position-in-field ratio over last 5 races
  - bb_last_was_best_worst      : 1=last was best in recent 5, -1=worst, 0=neither
  - bb_class_drop_after_bad     : 1 if field size decreased after a bad race (class drop proxy)

Usage:
    python feature_builders/bounce_back_builder.py
    python feature_builders/bounce_back_builder.py --input data_master/partants_master.jsonl
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/bounce_back")

_LOG_EVERY = 500_000

# A "bad race" threshold: position in top 60% of field is OK, worse = bad
_BAD_RACE_PERCENTILE = 0.6


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
# HELPERS
# ===========================================================================


def _safe_int(val) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        v = int(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _stddev(values: list[float]) -> Optional[float]:
    """Standard deviation of a list. None if < 2 values."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / (n - 1)
    return math.sqrt(var)


def _slope(values: list[float]) -> Optional[float]:
    """Simple linear regression slope. None if < 2 values."""
    n = len(values)
    if n < 2:
        return None
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    return num / den


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseBounceState:
    """Track bounce-back related data for one horse."""

    __slots__ = (
        "last_positions",          # deque of (position, nb_partants) tuples, maxlen=20
        "last_position_ratios",    # deque of position/nb_partants ratios, maxlen=20
        "last_was_bad",            # bool: was the last race a bad one?
        "last_implied_rank",       # int: rank based on implied probability last race
        "last_actual_pos",         # int: actual finishing position last race
        "last_nb_partants",        # int: field size of last race
        "last_had_incident",       # bool: did horse have incident last race?
        "last_was_disqualified",   # bool: was horse disqualified last race?
        "bounce_back_count",       # int: times improved after bad race
        "decline_after_bad_count", # int: times got worse after bad race
        "bad_race_count",          # int: total bad races (denominator)
        "post_incident_positions", # list of positions in races AFTER an incident
        "post_disq_positions",     # list of positions in races AFTER a disqualification
        "recovery_distances",      # list of int: races needed to recover after bad runs
        "alternation_count",       # int: times direction changed (good->bad or bad->good)
        "total_transitions",       # int: total transitions counted for reversal prob
        "avg_position_sum",        # float: running sum of all positions
        "avg_position_count",      # int: total races counted for average
        "_in_recovery",            # bool: currently in recovery mode
        "_recovery_counter",       # int: races since last bad race during recovery
    )

    def __init__(self) -> None:
        self.last_positions: deque = deque(maxlen=20)
        self.last_position_ratios: deque = deque(maxlen=20)
        self.last_was_bad: bool = False
        self.last_implied_rank: Optional[int] = None
        self.last_actual_pos: Optional[int] = None
        self.last_nb_partants: Optional[int] = None
        self.last_had_incident: bool = False
        self.last_was_disqualified: bool = False
        self.bounce_back_count: int = 0
        self.decline_after_bad_count: int = 0
        self.bad_race_count: int = 0
        self.post_incident_positions: list = []
        self.post_disq_positions: list = []
        self.recovery_distances: list = []
        self.alternation_count: int = 0
        self.total_transitions: int = 0
        self.avg_position_sum: float = 0.0
        self.avg_position_count: int = 0
        self._in_recovery: bool = False
        self._recovery_counter: int = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_bounce_back_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build 14 bounce-back features from partants_master.jsonl."""
    logger.info("=== Bounce Back Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        position = _safe_int(rec.get("position_arrivee"))
        cote = _safe_float(rec.get("cote_finale"))
        proba = _safe_float(rec.get("proba_implicite"))
        nb_partants = _safe_int(rec.get("nombre_partants"))

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("horse_id") or rec.get("nom_cheval"),
            "position": position,
            "cote": cote,
            "proba": proba,
            "nb_partants": nb_partants,
            "incident": bool(rec.get("incident")),
            "disq": bool(rec.get("is_disqualifie")),
            "gagnant": bool(rec.get("is_gagnant")),
            "hippo": (rec.get("hippodrome_normalise") or "").lower().strip(),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_state: dict[str, _HorseBounceState] = defaultdict(_HorseBounceState)
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

        # Compute implied probability ranks for this course
        # (rank by cote ascending = rank 1 is favourite)
        runners_with_cote = [
            (idx, r["cote"])
            for idx, r in enumerate(course_group)
            if r["cote"] is not None and r["cote"] > 0
        ]
        runners_with_cote.sort(key=lambda x: x[1])  # lowest cote = rank 1
        implied_rank_map: dict[int, int] = {}
        for rank, (idx, _) in enumerate(runners_with_cote, 1):
            implied_rank_map[idx] = rank

        # Field size for this course (use actual or count)
        course_nb = None
        for r in course_group:
            if r["nb_partants"] is not None:
                course_nb = r["nb_partants"]
                break
        if course_nb is None:
            course_nb = len(course_group)

        # -- Snapshot pre-race features, then prepare updates --
        post_updates: list[tuple] = []

        for idx, rec in enumerate(course_group):
            cheval = rec["cheval"]

            if not cheval:
                results.append({
                    "partant_uid": rec["uid"],
                    "bb_last_position": None,
                    "bb_last_surprise_factor": None,
                    "bb_last_underperformed": None,
                    "bb_bounce_back_rate": None,
                    "bb_decline_rate": None,
                    "bb_streak_direction": None,
                    "bb_post_incident_avg_pos": None,
                    "bb_post_disq_avg_pos": None,
                    "bb_recovery_speed": None,
                    "bb_form_reversal_prob": None,
                    "bb_position_volatility": None,
                    "bb_beaten_lengths_trend": None,
                    "bb_last_was_best_worst": None,
                    "bb_class_drop_after_bad": None,
                })
                post_updates.append(None)
                continue

            st = horse_state[cheval]

            # --- Feature 1: last race position ---
            bb_last_position = st.last_actual_pos

            # --- Feature 2: last surprise factor ---
            bb_last_surprise = None
            if st.last_actual_pos is not None and st.last_implied_rank is not None:
                # positive = finished worse than odds rank suggested
                bb_last_surprise = st.last_actual_pos - st.last_implied_rank

            # --- Feature 3: did horse underperform last race? ---
            bb_last_underperformed = None
            if bb_last_surprise is not None:
                bb_last_underperformed = 1 if bb_last_surprise > 0 else 0

            # --- Feature 4: bounce-back rate ---
            bb_bounce_back_rate = None
            if st.bad_race_count >= 2:
                bb_bounce_back_rate = round(
                    st.bounce_back_count / st.bad_race_count, 4
                )

            # --- Feature 5: decline rate ---
            bb_decline_rate = None
            if st.bad_race_count >= 2:
                bb_decline_rate = round(
                    st.decline_after_bad_count / st.bad_race_count, 4
                )

            # --- Feature 6: streak direction ---
            bb_streak_direction = None
            recent_pos = list(st.last_positions)[-5:]  # last 5 (pos, nb)
            if len(recent_pos) >= 3:
                # Use position ratios for direction
                recent_ratios = list(st.last_position_ratios)[-5:]
                if len(recent_ratios) >= 3:
                    sl = _slope(recent_ratios)
                    if sl is not None:
                        if sl < -0.02:
                            bb_streak_direction = 1   # improving (ratio decreasing)
                        elif sl > 0.02:
                            bb_streak_direction = -1  # declining (ratio increasing)
                        else:
                            bb_streak_direction = 0   # oscillating

            # --- Feature 7: post-incident avg position ---
            bb_post_incident_avg = None
            if len(st.post_incident_positions) >= 1:
                bb_post_incident_avg = round(
                    sum(st.post_incident_positions) / len(st.post_incident_positions), 2
                )

            # --- Feature 8: post-disqualification avg position ---
            bb_post_disq_avg = None
            if len(st.post_disq_positions) >= 1:
                bb_post_disq_avg = round(
                    sum(st.post_disq_positions) / len(st.post_disq_positions), 2
                )

            # --- Feature 9: recovery speed ---
            bb_recovery_speed = None
            if len(st.recovery_distances) >= 1:
                bb_recovery_speed = round(
                    sum(st.recovery_distances) / len(st.recovery_distances), 2
                )

            # --- Feature 10: form reversal probability ---
            bb_form_reversal = None
            if st.total_transitions >= 3:
                bb_form_reversal = round(
                    st.alternation_count / st.total_transitions, 4
                )

            # --- Feature 11: position volatility (std dev last 5) ---
            bb_pos_volatility = None
            recent_raw_pos = [p for p, _ in list(st.last_positions)[-5:]]
            if len(recent_raw_pos) >= 2:
                bb_pos_volatility = round(_stddev(recent_raw_pos), 3)

            # --- Feature 12: beaten lengths trend ---
            bb_beaten_trend = None
            recent_ratios_list = list(st.last_position_ratios)[-5:]
            if len(recent_ratios_list) >= 3:
                sl = _slope(recent_ratios_list)
                if sl is not None:
                    bb_beaten_trend = round(sl, 4)

            # --- Feature 13: last race was best/worst in recent 5 ---
            bb_last_best_worst = None
            if len(recent_raw_pos) >= 2 and st.last_actual_pos is not None:
                best = min(recent_raw_pos)
                worst = max(recent_raw_pos)
                if st.last_actual_pos == best and best != worst:
                    bb_last_best_worst = 1
                elif st.last_actual_pos == worst and best != worst:
                    bb_last_best_worst = -1
                else:
                    bb_last_best_worst = 0

            # --- Feature 14: class drop after bad race ---
            bb_class_drop = None
            if st.last_was_bad and st.last_nb_partants is not None and course_nb is not None:
                # Smaller field = lower class (proxy)
                if course_nb < st.last_nb_partants:
                    bb_class_drop = 1
                else:
                    bb_class_drop = 0

            results.append({
                "partant_uid": rec["uid"],
                "bb_last_position": bb_last_position,
                "bb_last_surprise_factor": bb_last_surprise,
                "bb_last_underperformed": bb_last_underperformed,
                "bb_bounce_back_rate": bb_bounce_back_rate,
                "bb_decline_rate": bb_decline_rate,
                "bb_streak_direction": bb_streak_direction,
                "bb_post_incident_avg_pos": bb_post_incident_avg,
                "bb_post_disq_avg_pos": bb_post_disq_avg,
                "bb_recovery_speed": bb_recovery_speed,
                "bb_form_reversal_prob": bb_form_reversal,
                "bb_position_volatility": bb_pos_volatility,
                "bb_beaten_lengths_trend": bb_beaten_trend,
                "bb_last_was_best_worst": bb_last_best_worst,
                "bb_class_drop_after_bad": bb_class_drop,
            })

            # Prepare update data
            implied_rank = implied_rank_map.get(idx)
            post_updates.append((
                cheval, rec["position"], rec["nb_partants"],
                rec["incident"], rec["disq"], implied_rank, course_nb,
            ))

        # -- Update state after race (post-race, no leakage) --
        for update in post_updates:
            if update is None:
                continue

            cheval, position, nb_partants, had_incident, was_disq, implied_rank, c_nb = update
            st = horse_state[cheval]

            if position is not None:
                effective_nb = nb_partants if nb_partants is not None else c_nb
                ratio = position / effective_nb if effective_nb and effective_nb > 0 else None

                # Track post-incident / post-disqualification positions
                if st.last_had_incident:
                    st.post_incident_positions.append(position)
                if st.last_was_disqualified:
                    st.post_disq_positions.append(position)

                # Determine if this race is "bad"
                is_bad = False
                if effective_nb is not None and effective_nb > 0:
                    is_bad = position > max(1, int(effective_nb * _BAD_RACE_PERCENTILE))

                # Bounce-back / decline tracking
                if st.last_was_bad and st.last_actual_pos is not None:
                    if position < st.last_actual_pos:
                        st.bounce_back_count += 1
                    elif position > st.last_actual_pos:
                        st.decline_after_bad_count += 1

                # Alternation tracking (good->bad or bad->good)
                if st.last_actual_pos is not None:
                    prev_was_bad = st.last_was_bad
                    if prev_was_bad != is_bad:
                        st.alternation_count += 1
                    st.total_transitions += 1

                # Recovery tracking
                if is_bad and not st._in_recovery:
                    st._in_recovery = True
                    st._recovery_counter = 0
                elif st._in_recovery:
                    st._recovery_counter += 1
                    # Check if recovered: position at or better than running average
                    running_avg = (
                        st.avg_position_sum / st.avg_position_count
                        if st.avg_position_count > 0 else None
                    )
                    if running_avg is not None and position <= running_avg:
                        st.recovery_distances.append(st._recovery_counter)
                        st._in_recovery = False
                    elif st._recovery_counter >= 10:
                        # Cap recovery at 10 races
                        st.recovery_distances.append(10)
                        st._in_recovery = False

                # Update bad race count
                if is_bad:
                    st.bad_race_count += 1

                # Update running average
                st.avg_position_sum += position
                st.avg_position_count += 1

                # Update position history
                st.last_positions.append((position, effective_nb))
                if ratio is not None:
                    st.last_position_ratios.append(ratio)

                # Update last-race state
                st.last_actual_pos = position
                st.last_nb_partants = effective_nb
                st.last_was_bad = is_bad
            else:
                # No position (DNF etc.) -- still update flags
                st.last_actual_pos = None
                st.last_nb_partants = nb_partants if nb_partants is not None else c_nb

            st.last_implied_rank = implied_rank
            st.last_had_incident = had_incident
            st.last_was_disqualified = was_disq

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY < len(course_group):
            logger.info("  Traite %d / %d records...", n_processed, total)
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Bounce back build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_state),
    )
    return results


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
        description="Construction des bounce-back features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/bounce_back/)",
    )
    args = parser.parse_args()

    logger = setup_logging("bounce_back_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_bounce_back_features(input_path, logger)

    # Save with .tmp then rename
    out_path = output_dir / "bounce_back_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")
    save_jsonl(results, tmp_path, logger)
    tmp_path.replace(out_path)
    logger.info("Fichier final: %s", out_path)

    # Summary fill rates
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
