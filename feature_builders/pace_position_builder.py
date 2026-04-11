#!/usr/bin/env python3
"""
feature_builders.pace_position_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Infer running position / pace style from finishing positions and field context.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant pace/position features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the features — no future leakage. Features are emitted
BEFORE the state is updated with the current race outcome.

Produces:
  - pace_position.jsonl   in builder_outputs/pace_position/

Features per partant (8):
  - pp_avg_early_vs_final      : avg (num_pmu - position_arrivee); positive means
                                 horse tends to pass others (comes from behind and
                                 finishes better than draw)
  - pp_front_runner_score      : fraction of races where horse finished in top 30%
                                 of field (leader/front-runner profile)
  - pp_closer_score            : fraction of races where position improved by >3
                                 spots from draw number to finishing position
  - pp_consistent_finisher     : 1 - std_dev(position_pct over last 10 races);
                                 high value = predictable finisher
  - pp_position_improvement_avg: average improvement (num_pmu - position_arrivee)
                                 across all career races
  - pp_last5_avg_position_pct  : average (position_arrivee / nombre_partants) over
                                 last 5 races; lower = better
  - pp_best_position_recent    : best (lowest) finishing position in last 5 races
  - pp_win_from_back           : fraction of wins where num_pmu > median draw in the
                                 race (winning from an unfavourable draw)

Usage:
    python feature_builders/pace_position_builder.py
    python feature_builders/pace_position_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/pace_position_builder.py --output-dir /path/to/out/
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
from statistics import median
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pace_position")

_LOG_EVERY = 500_000

# Window sizes
_WINDOW_LAST5 = 5
_WINDOW_CONSISTENCY = 10

# Front-runner threshold: top 30% of field
_FRONT_RUNNER_PCT = 0.30

# Closer improvement threshold: must beat draw by more than this many spots
_CLOSER_IMPROVEMENT = 3

# ===========================================================================
# STATISTICS HELPERS
# ===========================================================================


def _std_population(values: list[float]) -> Optional[float]:
    """Population standard deviation. Returns None if fewer than 2 values."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(variance)


# ===========================================================================
# HORSE STATE
# ===========================================================================


class _PaceState:
    """Per-horse running state for pace/position feature accumulation.

    positions deque stores tuples of (position_arrivee, num_pmu, nombre_partants)
    for up to the last 20 races. Older races are dropped automatically.

    Separate counters track wins_from_back and total_wins / total_races
    across the full career (not just the rolling window) so that
    pp_win_from_back has a reliable denominator.
    """

    __slots__ = (
        "positions",
        "total_races",
        "total_wins",
        "wins_from_back",
    )

    def __init__(self) -> None:
        # Each entry: (position_arrivee: int, num_pmu: int, nombre_partants: int)
        self.positions: deque[tuple[int, int, int]] = deque(maxlen=20)
        self.total_races: int = 0
        self.total_wins: int = 0
        self.wins_from_back: int = 0


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


def _safe_int(value: Any) -> Optional[int]:
    """Parse int safely, returning None on failure."""
    if value is None:
        return None
    try:
        v = int(value)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# FEATURE COMPUTATION (from frozen state snapshot)
# ===========================================================================


def _compute_features(state: _PaceState) -> dict[str, Optional[float]]:
    """Compute pace/position features from a pre-race state snapshot.

    All computations rely only on data already stored in `state`, which
    reflects races prior to the current one (temporal integrity).
    """
    positions = list(state.positions)  # oldest → newest
    n = len(positions)

    # ── pp_avg_early_vs_final ──────────────────────────────────────────────
    # Average (num_pmu - position_arrivee): positive = passes others on average
    if n > 0:
        diffs = [num - pos for pos, num, _ in positions]
        pp_avg_early_vs_final: Optional[float] = round(sum(diffs) / n, 4)
    else:
        pp_avg_early_vs_final = None

    # ── pp_front_runner_score ─────────────────────────────────────────────
    # Fraction of races where horse finished in top 30% of field
    if n > 0:
        front_count = 0
        for pos, _, nb_starters in positions:
            if nb_starters and nb_starters > 0:
                pct = pos / nb_starters
                if pct <= _FRONT_RUNNER_PCT:
                    front_count += 1
        pp_front_runner_score: Optional[float] = round(front_count / n, 4)
    else:
        pp_front_runner_score = None

    # ── pp_closer_score ───────────────────────────────────────────────────
    # Fraction of races where horse improved by >3 spots (num_pmu - position_arrivee > 3)
    if n > 0:
        closer_count = sum(
            1 for pos, num, _ in positions if (num - pos) > _CLOSER_IMPROVEMENT
        )
        pp_closer_score: Optional[float] = round(closer_count / n, 4)
    else:
        pp_closer_score = None

    # ── pp_consistent_finisher ────────────────────────────────────────────
    # 1 - std_dev(position_pct) over last 10 races; high = predictable
    last10 = positions[-_WINDOW_CONSISTENCY:]
    if len(last10) >= 2:
        pct_list = [
            pos / nb for pos, _, nb in last10 if nb and nb > 0
        ]
        if len(pct_list) >= 2:
            std = _std_population(pct_list)
            pp_consistent_finisher: Optional[float] = (
                round(max(0.0, 1.0 - std), 4) if std is not None else None
            )
        else:
            pp_consistent_finisher = None
    else:
        pp_consistent_finisher = None

    # ── pp_position_improvement_avg ───────────────────────────────────────
    # Average improvement from draw to finish across all career races in window
    if n > 0:
        improvements = [num - pos for pos, num, _ in positions]
        pp_position_improvement_avg: Optional[float] = round(
            sum(improvements) / n, 4
        )
    else:
        pp_position_improvement_avg = None

    # ── pp_last5_avg_position_pct ─────────────────────────────────────────
    # Average (position_arrivee / nombre_partants) over last 5 races
    last5 = positions[-_WINDOW_LAST5:]
    valid5 = [pos / nb for pos, _, nb in last5 if nb and nb > 0]
    if valid5:
        pp_last5_avg_position_pct: Optional[float] = round(
            sum(valid5) / len(valid5), 4
        )
    else:
        pp_last5_avg_position_pct = None

    # ── pp_best_position_recent ───────────────────────────────────────────
    # Best (lowest) finishing position in last 5 races
    if last5:
        pp_best_position_recent: Optional[int] = min(pos for pos, _, _ in last5)
    else:
        pp_best_position_recent = None

    # ── pp_win_from_back ─────────────────────────────────────────────────
    # Fraction of wins where num_pmu > median draw in race
    # Uses career-level wins_from_back / total_wins counters
    if state.total_wins > 0:
        pp_win_from_back: Optional[float] = round(
            state.wins_from_back / state.total_wins, 4
        )
    else:
        pp_win_from_back = None

    return {
        "pp_avg_early_vs_final": pp_avg_early_vs_final,
        "pp_front_runner_score": pp_front_runner_score,
        "pp_closer_score": pp_closer_score,
        "pp_consistent_finisher": pp_consistent_finisher,
        "pp_position_improvement_avg": pp_position_improvement_avg,
        "pp_last5_avg_position_pct": pp_last5_avg_position_pct,
        "pp_best_position_recent": pp_best_position_recent,
        "pp_win_from_back": pp_win_from_back,
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_pace_position_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build pace/position features from partants_master.jsonl.

    Three-phase approach:
      1. Read minimal fields into memory (streaming, one pass).
      2. Sort chronologically by (date, course_uid, num_pmu).
      3. Process race by race (grouped by course_uid):
         a. Emit features from pre-race state snapshot (strict temporal integrity).
         b. Compute race-level context (e.g. median draw) needed for pp_win_from_back.
         c. Update horse state with this race's outcome.

    Memory budget (approx):
      - Slim records: ~16M * ~120 bytes = ~1.9 GB
      - State dicts: ~390K horses * ~400 bytes (deque 20 tuples) = ~160 MB
    """
    logger.info("=== Pace Position Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        pos = _safe_int(rec.get("position_arrivee"))
        num = _safe_int(rec.get("num_pmu"))
        nb_starters = _safe_int(rec.get("nombre_partants"))

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", "") or "",
            "course": rec.get("course_uid", "") or "",
            "num": num,
            "cheval": rec.get("nom_cheval") or rec.get("horse_id"),
            "position": pos,
            "nb_starters": nb_starters,
            # Whether this is a winning race (to track wins_from_back)
            "is_win": pos == 1,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"] or 0))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process race by race ──
    t2 = time.time()
    horse_state: dict[str, _PaceState] = defaultdict(_PaceState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    i = 0
    while i < total:
        # Collect all partants in this race (same course_uid + date)
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        race_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            race_group.append(slim_records[i])
            i += 1

        if not race_group:
            continue

        # Compute median draw number in this race (for pp_win_from_back)
        race_draw_numbers = [
            r["num"] for r in race_group if r["num"] is not None
        ]
        race_median_draw: Optional[float] = (
            median(race_draw_numbers) if race_draw_numbers else None
        )

        # ── Emit features (pre-update snapshot) ──
        for rec in race_group:
            cheval = rec["cheval"]
            if cheval:
                state = horse_state[cheval]
            else:
                state = _PaceState()  # anonymous horse gets empty state

            feats = _compute_features(state)
            feats["partant_uid"] = rec["uid"]
            results.append(feats)

        # ── Update state with this race's outcomes ──
        for rec in race_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            pos = rec["position"]
            num = rec["num"]
            nb_starters = rec["nb_starters"]

            state = horse_state[cheval]
            state.total_races += 1

            # Only record if we have valid position + draw + field size
            if (
                pos is not None
                and num is not None
                and nb_starters is not None
                and nb_starters > 0
            ):
                state.positions.append((pos, num, nb_starters))

                if rec["is_win"]:
                    state.total_wins += 1
                    # Win from back: horse's draw > median draw in this race
                    if race_median_draw is not None and num > race_median_draw:
                        state.wins_from_back += 1
            elif rec["is_win"]:
                # Count win even without full positional data
                state.total_wins += 1

        n_processed += len(race_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Pace position build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        len(horse_state),
    )

    # Free memory
    del slim_records
    del horse_state
    gc.collect()

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path (CLI override → candidates in order)."""
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features de pace/position a partir de partants_master"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pace_position/)",
    )
    args = parser.parse_args()

    logger = setup_logging("pace_position_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_pace_position_features(input_path, logger)

    # Save
    out_path = output_dir / "pace_position.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100.0 * v / total)


if __name__ == "__main__":
    main()
