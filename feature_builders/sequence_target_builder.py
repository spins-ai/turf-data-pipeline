#!/usr/bin/env python3
"""
feature_builders.sequence_target_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Target-related sequence features for multi-target prediction tasks.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant target sequence features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - sequence_target.jsonl   in builder_outputs/sequence_target/

Features per partant (10):
  - st_horse_win_rate       : horse's historical win rate (wins / total races)
  - st_horse_place_rate     : horse's rate of finishing in top 3
  - st_horse_show_rate      : horse's rate of finishing in top 5
  - st_horse_exacta_rate    : horse's rate of finishing in top 2
  - st_win_rate_trend       : win rate in last 10 races minus career win rate
                              (positive = improving, negative = declining)
  - st_place_streak         : current consecutive top-3 finishes (resets on miss)
  - st_races_since_win      : number of races since last win (None if never won)
  - st_avg_beaten_lengths   : average of (position_arrivee / nombre_partants)
                              over all career races; lower = better
  - st_never_won            : 1 if horse has 0 career wins (maiden), 0 otherwise
  - st_win_frequency        : 1 / max(avg_races_between_wins, 1)
                              -- how often the horse wins on average

Usage:
    python feature_builders/sequence_target_builder.py
    python feature_builders/sequence_target_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/sequence_target_builder.py --output-dir /path/to/output
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/sequence_target")

# Window for recent-race trend computation
RECENT_WINDOW = 10

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Accumulates per-horse target sequence statistics.

    All attributes represent state BEFORE the current race is processed,
    ensuring strict temporal integrity (no future leakage).
    """

    __slots__ = (
        "wins",
        "places",
        "shows",
        "exactas",
        "total",
        "recent_wins",       # deque(maxlen=10): 1 if win else 0, for last 10 races
        "recent_places",     # deque(maxlen=10): 1 if top-3 else 0, for last 10 races
        "last_win_race_num", # race sequence number of the most recent win (1-based)
        "current_place_streak",  # consecutive top-3 finishes ending at last race
        "positions",         # deque(maxlen=20): (pos, nb_partants) tuples
        "win_race_numbers",  # list of race sequence numbers when wins occurred
    )

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.shows: int = 0
        self.exactas: int = 0
        self.total: int = 0
        self.recent_wins: deque = deque(maxlen=RECENT_WINDOW)
        self.recent_places: deque = deque(maxlen=RECENT_WINDOW)
        self.last_win_race_num: Optional[int] = None
        self.current_place_streak: int = 0
        self.positions: deque = deque(maxlen=20)
        self.win_race_numbers: list = []


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file one line at a time (streaming)."""
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
# FEATURE COMPUTATION HELPERS
# ===========================================================================


def _safe_int(val: Any, min_val: Optional[int] = None) -> Optional[int]:
    """Parse an integer safely; return None on failure."""
    if val is None:
        return None
    try:
        result = int(val)
        if min_val is not None and result < min_val:
            return None
        return result
    except (ValueError, TypeError):
        return None


def _rate(num: int, den: int) -> Optional[float]:
    """Return num/den rounded to 6 dp, or None if den==0."""
    if den == 0:
        return None
    return round(num / den, 6)


def _compute_win_frequency(state: _HorseState) -> Optional[float]:
    """Compute 1 / avg_races_between_wins.

    If the horse has 0 or 1 career wins, frequency cannot be estimated from
    the gap sequence; we fall back to a simple career win rate.
    """
    if state.wins == 0:
        return 0.0
    if state.wins == 1:
        # Only one win: avg gap = total races so far
        avg_gap = max(state.total, 1)
        return round(1.0 / avg_gap, 6)

    # Compute gaps between consecutive wins (using race sequence numbers)
    nums = state.win_race_numbers
    gaps = [nums[i + 1] - nums[i] for i in range(len(nums) - 1)]
    avg_gap = sum(gaps) / len(gaps)
    return round(1.0 / max(avg_gap, 1.0), 6)


def _compute_avg_beaten_lengths(state: _HorseState) -> Optional[float]:
    """Average of (position / nb_partants) over stored recent positions."""
    valid = [
        (pos / nb) for pos, nb in state.positions
        if nb > 0 and pos > 0
    ]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 6)


def _compute_win_rate_trend(state: _HorseState) -> Optional[float]:
    """Recent win rate (last <=10 races) minus career win rate.

    Positive = improving, negative = declining.
    Requires at least 1 career race and at least 1 recent race.
    """
    if state.total == 0:
        return None
    career_wr = state.wins / state.total
    recent = list(state.recent_wins)
    if not recent:
        return None
    recent_wr = sum(recent) / len(recent)
    return round(recent_wr - career_wr, 6)


def _snapshot_features(state: _HorseState) -> dict[str, Any]:
    """Compute the 10 target features from the current (pre-race) state."""
    total = state.total

    # -- rates --
    win_rate = _rate(state.wins, total)
    place_rate = _rate(state.places, total)
    show_rate = _rate(state.shows, total)
    exacta_rate = _rate(state.exactas, total)

    # -- trend --
    win_rate_trend = _compute_win_rate_trend(state)

    # -- place streak --
    place_streak = state.current_place_streak

    # -- races since win --
    if state.last_win_race_num is None:
        races_since_win = None
    else:
        races_since_win = total - state.last_win_race_num

    # -- avg beaten lengths --
    avg_beaten = _compute_avg_beaten_lengths(state)

    # -- maiden indicator --
    never_won = 1 if (total > 0 and state.wins == 0) else (0 if total > 0 else None)

    # -- win frequency --
    win_freq = _compute_win_frequency(state) if total > 0 else None

    return {
        "st_horse_win_rate": win_rate,
        "st_horse_place_rate": place_rate,
        "st_horse_show_rate": show_rate,
        "st_horse_exacta_rate": exacta_rate,
        "st_win_rate_trend": win_rate_trend,
        "st_place_streak": place_streak if total > 0 else None,
        "st_races_since_win": races_since_win,
        "st_avg_beaten_lengths": avg_beaten,
        "st_never_won": never_won,
        "st_win_frequency": win_freq,
    }


def _update_state(state: _HorseState, pos: Optional[int], nb_partants: Optional[int]) -> None:
    """Update horse state with this race's outcome.

    pos: finishing position (1 = win); None if DNF / unknown.
    nb_partants: number of starters in the race.
    """
    state.total += 1
    race_num = state.total  # 1-based sequence number for this horse

    # Determine outcome flags
    is_win = pos is not None and pos == 1
    is_exacta = pos is not None and pos <= 2
    is_place = pos is not None and pos <= 3
    is_show = pos is not None and pos <= 5

    if is_win:
        state.wins += 1
        state.last_win_race_num = race_num
        state.win_race_numbers.append(race_num)

    if is_exacta:
        state.exactas += 1

    if is_place:
        state.places += 1

    if is_show:
        state.shows += 1

    # Recent windows
    state.recent_wins.append(1 if is_win else 0)
    state.recent_places.append(1 if is_place else 0)

    # Place streak: increment if top-3, reset otherwise
    if is_place:
        state.current_place_streak += 1
    else:
        state.current_place_streak = 0

    # Beaten lengths position buffer (store normalized rank)
    if pos is not None and pos > 0 and nb_partants is not None and nb_partants > 0:
        state.positions.append((pos, nb_partants))


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_sequence_target_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build sequence-target features from partants_master.jsonl.

    Two-phase approach:
      1. Read minimal fields into memory, sort chronologically.
      2. Process records grouped by date (strict temporal integrity):
         features are emitted BEFORE updating state (no future leakage).

    Memory budget (example at 16 M records):
      - Slim records : ~16M * ~110 bytes  ~= ~1.8 GB
      - Horse states : ~390K horses * ~500 bytes ~= ~195 MB
    """
    logger.info("=== Sequence Target Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        pos = _safe_int(rec.get("position_arrivee"), min_val=0)
        # pos == 0 typically means DNF; treat as None for outcome computation
        if pos is not None and pos == 0:
            pos = None

        nb_partants = _safe_int(rec.get("nombre_partants"), min_val=1)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": _safe_int(rec.get("num_pmu")) or 0,
            "cheval": rec.get("horse_id") or rec.get("nom_cheval"),
            "position": pos,
            "nb_partants": nb_partants,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process grouped by date (strict temporal integrity) ──
    t2 = time.time()
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total_records = len(slim_records)

    i = 0
    while i < total_records:
        current_date = slim_records[i]["date"]
        date_group: list[dict] = []

        # Collect all records sharing this date
        while i < total_records and slim_records[i]["date"] == current_date:
            date_group.append(slim_records[i])
            i += 1

        # ── Emit pre-race snapshot (features BEFORE update) ──
        for rec in date_group:
            cheval = rec["cheval"]
            uid = rec["uid"]

            if not cheval:
                # Cannot track state without a horse identifier
                results.append({
                    "partant_uid": uid,
                    "st_horse_win_rate": None,
                    "st_horse_place_rate": None,
                    "st_horse_show_rate": None,
                    "st_horse_exacta_rate": None,
                    "st_win_rate_trend": None,
                    "st_place_streak": None,
                    "st_races_since_win": None,
                    "st_avg_beaten_lengths": None,
                    "st_never_won": None,
                    "st_win_frequency": None,
                })
                continue

            state = horse_states[cheval]
            feats = _snapshot_features(state)
            feats["partant_uid"] = uid
            results.append(feats)

        # ── Update states with this date's race outcomes ──
        for rec in date_group:
            cheval = rec["cheval"]
            if not cheval:
                continue
            _update_state(horse_states[cheval], rec["position"], rec["nb_partants"])

        n_processed += len(date_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total_records)

    elapsed = time.time() - t0
    logger.info(
        "Sequence target build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        len(horse_states),
    )

    # Free heavy structures before returning
    del slim_records
    del horse_states
    gc.collect()

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path from CLI arg or default candidates."""
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
        description="Construction des features sequence-target a partir de partants_master"
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
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("sequence_target_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_sequence_target_features(input_path, logger)

    # Save
    out_path = output_dir / "sequence_target.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100.0 * v / total)

    logger.info("Done.")


if __name__ == "__main__":
    main()
