#!/usr/bin/env python3
"""
feature_builders.claiming_class_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Detects class levels from race allocation (prize money) and creates
class-related interaction features per partant.

Two-pass algorithm:
  Pass 1 – collect all allocation values to compute global percentile
            breakpoints (deciles 0-9).
  Pass 2 – stream partants_master.jsonl chronologically, compute features
            BEFORE updating per-horse state (no future leakage).

Features (8) per partant:
  cc_class_level            : race class based on allocation (0-9 deciles)
  cc_horse_avg_class        : rolling average class level horse has raced at
  cc_class_step             : current_class - last_race_class (>0 = stepping up)
  cc_horse_best_class_win   : highest class level horse has won at
  cc_class_outperform       : 1 if horse has won at class > current_class
  cc_horse_class_win_rate_here : win rate at this exact class level
  cc_class_range            : max_class - min_class horse has raced at (versatility)
  cc_class_momentum         : average class_step over last 5 races (>0 = moving up)

Temporal integrity: for any partant at date D, only races with date < D
contribute to the horse state — no future leakage.

Usage:
    python feature_builders/claiming_class_builder.py
    python feature_builders/claiming_class_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
    python feature_builders/claiming_class_builder.py --input ... --output-dir D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/claiming_class
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

INPUT_PARTANTS = Path(
    "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"
)
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/claiming_class"
)

# Number of class buckets (deciles 0-9)
N_CLASSES = 10

# Rolling window for momentum calculation
MOMENTUM_WINDOW = 5

# Max history per horse
CLASS_HISTORY_MAXLEN = 20

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger) -> Any:
    """Yield parsed dicts from a JSONL file one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning(
                        "Ligne JSON invalide ignoree (erreur %d)", errors
                    )
    logger.info(
        "Lecture terminee: %d records, %d erreurs JSON", count, errors
    )


# ===========================================================================
# PERCENTILE BREAKPOINTS
# ===========================================================================


def _compute_percentile_breakpoints(
    values: list[float], n_buckets: int
) -> list[float]:
    """Return (n_buckets - 1) breakpoints that divide `values` into equal-size
    buckets.  Values are sorted; breakpoints are the upper edges of each bucket
    except the last.

    Example with n_buckets=10: returns 9 breakpoints (p10, p20, …, p90).
    """
    if not values:
        return []
    sorted_vals = sorted(values)
    m = len(sorted_vals)
    breakpoints: list[float] = []
    for i in range(1, n_buckets):
        # Percentile index using nearest-rank
        idx = max(0, math.ceil(m * i / n_buckets) - 1)
        breakpoints.append(sorted_vals[idx])
    return breakpoints


def _value_to_class(value: float, breakpoints: list[float]) -> int:
    """Map a value to a class bucket [0, n_classes-1] using breakpoints."""
    for i, bp in enumerate(breakpoints):
        if value <= bp:
            return i
    return len(breakpoints)  # highest class


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Tracks all class-level history for a single horse.

    All attributes are updated AFTER the race result is known, so reads
    taken before the update reflect only past races.
    """

    __slots__ = (
        "class_history",   # deque[int] of class levels raced (capped at 20)
        "per_class_wins",  # dict[int, int]  wins at each class level
        "per_class_total", # dict[int, int]  starts at each class level
        "best_class_won",  # int: highest class level won (-1 = never won)
        "_step_history",   # deque[int] of class_step values (for momentum)
        "_last_class",     # Optional[int]: class of most recent race
    )

    def __init__(self) -> None:
        self.class_history: deque[int] = deque(maxlen=CLASS_HISTORY_MAXLEN)
        self.per_class_wins: dict[int, int] = defaultdict(int)
        self.per_class_total: dict[int, int] = defaultdict(int)
        self.best_class_won: int = -1
        self._step_history: deque[int] = deque(maxlen=MOMENTUM_WINDOW)
        self._last_class: Optional[int] = None

    # ------------------------------------------------------------------
    # Read-before-update helpers (called BEFORE this race is recorded)
    # ------------------------------------------------------------------

    def avg_class(self) -> Optional[float]:
        """Rolling average class from past races."""
        if not self.class_history:
            return None
        return sum(self.class_history) / len(self.class_history)

    def class_step(self, current_class: int) -> Optional[int]:
        """current_class - last_race_class.  None if no prior race."""
        if self._last_class is None:
            return None
        return current_class - self._last_class

    def class_win_rate_here(self, current_class: int) -> Optional[float]:
        """Win rate at exactly this class level (past races only)."""
        total = self.per_class_total[current_class]
        if total == 0:
            return None
        return self.per_class_wins[current_class] / total

    def class_range(self) -> Optional[int]:
        """max_class - min_class across all past races."""
        if not self.class_history:
            return None
        return max(self.class_history) - min(self.class_history)

    def class_momentum(self) -> Optional[float]:
        """Average class_step over last MOMENTUM_WINDOW races."""
        if not self._step_history:
            return None
        return sum(self._step_history) / len(self._step_history)

    def outperforms(self, current_class: int) -> int:
        """1 if horse has won at a class level strictly above current_class."""
        return int(self.best_class_won > current_class)

    # ------------------------------------------------------------------
    # Update (called AFTER race features are emitted)
    # ------------------------------------------------------------------

    def update(self, current_class: int, won: bool) -> None:
        """Record the outcome of a completed race."""
        step = self.class_step(current_class)
        if step is not None:
            self._step_history.append(step)

        self.class_history.append(current_class)
        self.per_class_total[current_class] += 1

        if won:
            self.per_class_wins[current_class] += 1
            if current_class > self.best_class_won:
                self.best_class_won = current_class

        self._last_class = current_class


# ===========================================================================
# PASS 1: COLLECT ALLOCATIONS
# ===========================================================================


def _collect_allocations(input_path: Path, logger) -> list[float]:
    """First pass: read all allocation values (ignoring nulls)."""
    logger.info("=== Pass 1: collecte des allocations ===")
    t0 = time.time()
    allocs: list[float] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 – lu %d records...", n_read)

        raw = rec.get("allocation") or rec.get("prize_money") or rec.get("dotation")
        if raw is None:
            continue
        try:
            v = float(raw)
            if v > 0:
                allocs.append(v)
        except (ValueError, TypeError):
            pass

    logger.info(
        "Pass 1 terminee: %d records, %d allocations valides, %.1fs",
        n_read,
        len(allocs),
        time.time() - t0,
    )
    return allocs


# ===========================================================================
# PASS 2: STREAMING BUILD
# ===========================================================================


def _build_features(
    input_path: Path,
    breakpoints: list[float],
    logger,
) -> list[dict[str, Any]]:
    """Second pass: stream records chronologically and emit features."""
    logger.info("=== Pass 2: construction des features ===")
    logger.info("Breakpoints de classe (deciles): %s", breakpoints)
    t0 = time.time()

    # -- Read slim records --
    slim_records: list[dict[str, Any]] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 2 lecture – %d records...", n_read)

        raw_alloc = (
            rec.get("allocation") or rec.get("prize_money") or rec.get("dotation")
        )
        try:
            alloc_val: Optional[float] = float(raw_alloc) if raw_alloc is not None else None
        except (ValueError, TypeError):
            alloc_val = None

        # Determine class level
        if alloc_val is not None and alloc_val > 0 and breakpoints:
            class_level: Optional[int] = _value_to_class(alloc_val, breakpoints)
        else:
            class_level = None

        # Winner detection: position 1 or explicit flag
        pos = rec.get("position_arrivee")
        is_gagnant = rec.get("is_gagnant")
        if is_gagnant is not None:
            won = bool(is_gagnant)
        elif pos is not None:
            try:
                won = int(pos) == 1
            except (ValueError, TypeError):
                won = False
        else:
            won = False

        # Horse identifier
        horse_id = (
            rec.get("horse_id")
            or rec.get("cheval_id")
            or rec.get("nom_cheval")
        )

        slim_records.append(
            {
                "partant_uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course_uid": rec.get("course_uid", ""),
                "num_pmu": rec.get("num_pmu", 0) or 0,
                "horse_id": horse_id,
                "class_level": class_level,
                "won": won,
            }
        )

    logger.info(
        "Pass 2 lecture terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -- Sort chronologically --
    t1 = time.time()
    slim_records.sort(
        key=lambda r: (r["date"], r["course_uid"], r["num_pmu"])
    )
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Process course by course (temporal: emit then update) --
    t2 = time.time()
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    i = 0
    while i < total:
        # Collect all partants in this race
        course_uid = slim_records[i]["course_uid"]
        course_date = slim_records[i]["date"]
        course_group: list[dict[str, Any]] = []

        while (
            i < total
            and slim_records[i]["course_uid"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # Deferred updates: collect (horse_id, class_level, won) for post-emit update
        deferred: list[tuple[str, Optional[int], bool]] = []

        for rec in course_group:
            h: Optional[str] = rec["horse_id"]
            cl: Optional[int] = rec["class_level"]
            won: bool = rec["won"]

            state = horse_states[h] if h else None

            # --- Compute features BEFORE update ---

            # cc_class_level: class level of this race
            cc_class_level: Optional[int] = cl

            if state is not None and cl is not None:
                # cc_horse_avg_class
                cc_horse_avg_class: Optional[float] = state.avg_class()
                if cc_horse_avg_class is not None:
                    cc_horse_avg_class = round(cc_horse_avg_class, 4)

                # cc_class_step
                cc_class_step: Optional[int] = state.class_step(cl)

                # cc_horse_best_class_win: -1 means never won → set to None for clarity
                raw_best = state.best_class_won
                cc_horse_best_class_win: Optional[int] = (
                    raw_best if raw_best >= 0 else None
                )

                # cc_class_outperform
                cc_class_outperform: int = state.outperforms(cl)

                # cc_horse_class_win_rate_here
                cc_horse_class_win_rate_here: Optional[float] = (
                    state.class_win_rate_here(cl)
                )
                if cc_horse_class_win_rate_here is not None:
                    cc_horse_class_win_rate_here = round(
                        cc_horse_class_win_rate_here, 4
                    )

                # cc_class_range
                cc_class_range: Optional[int] = state.class_range()

                # cc_class_momentum
                cc_class_momentum: Optional[float] = state.class_momentum()
                if cc_class_momentum is not None:
                    cc_class_momentum = round(cc_class_momentum, 4)

            else:
                # No horse id or no class level available
                cc_horse_avg_class = None
                cc_class_step = None
                cc_horse_best_class_win = None
                cc_class_outperform = 0 if cl is not None else None
                cc_horse_class_win_rate_here = None
                cc_class_range = None
                cc_class_momentum = None

            results.append(
                {
                    "partant_uid": rec["partant_uid"],
                    "cc_class_level": cc_class_level,
                    "cc_horse_avg_class": cc_horse_avg_class,
                    "cc_class_step": cc_class_step,
                    "cc_horse_best_class_win": cc_horse_best_class_win,
                    "cc_class_outperform": cc_class_outperform,
                    "cc_horse_class_win_rate_here": cc_horse_class_win_rate_here,
                    "cc_class_range": cc_class_range,
                    "cc_class_momentum": cc_class_momentum,
                }
            )

            deferred.append((h, cl, won))

        # --- Update state AFTER emitting features (no leakage) ---
        for h, cl, won in deferred:
            if h is None or cl is None:
                continue
            horse_states[h].update(cl, won)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info(
                "  Traite %d / %d records...", n_processed, total
            )

    elapsed = time.time() - t0
    logger.info(
        "Pass 2 terminee: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        len(horse_states),
    )

    return results


# ===========================================================================
# TOP-LEVEL BUILD
# ===========================================================================


def build_claiming_class_features(
    input_path: Path,
    logger,
) -> list[dict[str, Any]]:
    """Full two-pass pipeline: percentile breakpoints then feature streaming."""
    logger.info("=== Claiming Class Features Builder ===")
    logger.info("Input: %s", input_path)

    # Pass 1 – allocation percentiles
    alloc_values = _collect_allocations(input_path, logger)
    gc.collect()

    if not alloc_values:
        logger.warning(
            "Aucune valeur d'allocation trouvee — cc_class_level sera None partout"
        )
        breakpoints: list[float] = []
    else:
        breakpoints = _compute_percentile_breakpoints(alloc_values, N_CLASSES)
        logger.info(
            "Breakpoints calcules sur %d valeurs: %s",
            len(alloc_values),
            [round(b, 0) for b in breakpoints],
        )
        del alloc_values
        gc.collect()

    # Pass 2 – features
    results = _build_features(input_path, breakpoints, logger)
    gc.collect()

    return results


# ===========================================================================
# SAVE
# ===========================================================================


def _save_jsonl(records: list[dict], path: Path, logger) -> None:
    """Write records to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info("Fichier ecrit: %s (%d lignes)", path, len(records))


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features claiming-class a partir de "
            "partants_master.jsonl (deux passes)."
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: voir OUTPUT_DIR dans le script)",
    )
    args = parser.parse_args()

    logger = setup_logging("claiming_class_builder")

    # Resolve input
    input_path: Path
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            logger.error("Fichier introuvable: %s", input_path)
            sys.exit(1)
    elif INPUT_PARTANTS.exists():
        input_path = INPUT_PARTANTS
    else:
        logger.error(
            "Fichier d'entree introuvable. Utilisez --input ou verifiez: %s",
            INPUT_PARTANTS,
        )
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    t_total = time.time()
    results = build_claiming_class_features(input_path, logger)

    # Write output
    out_path = output_dir / "claiming_class_features.jsonl"
    _save_jsonl(results, out_path, logger)

    # Fill-rate report
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total = len(results)
        logger.info("=== Fill rates (cc_* features) ===")
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info(
                "  %s: %d/%d (%.1f%%)", k, filled, total, 100.0 * filled / total
            )

    logger.info(
        "=== Done — %d records en %.1fs ===",
        len(results),
        time.time() - t_total,
    )


if __name__ == "__main__":
    main()
