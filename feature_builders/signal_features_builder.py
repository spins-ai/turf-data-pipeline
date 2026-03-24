#!/usr/bin/env python3
"""
feature_builders.signal_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
5 features capturing actionable signals from jockey/trainer/equipment changes.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant signal features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the signal computation -- no future leakage.

Produces:
  - signal_features.jsonl   in output/signal_features/

Features per partant:
  - jockey_upgrade          : 1 if current jockey has higher win rate than previous jockey
  - trainer_change_recent   : 1 if trainer changed in last 90 days
  - class_drop_after_win    : 1 if horse won last race and is now in lower class
  - returning_from_break    : 1 if >90 days since last race
  - equipment_change        : 1 if any equipment field differs from last run

Usage:
    python feature_builders/signal_features_builder.py
    python feature_builders/signal_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
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
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "signal_features"

# Threshold for "returning from break"
BREAK_DAYS = 90

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# HELPERS
# ===========================================================================


def _days_between(date_a: str, date_b: str) -> Optional[int]:
    """Return number of days between two ISO date strings (YYYY-MM-DD).

    Returns None if either date is missing or malformed.
    """
    if not date_a or not date_b or len(date_a) < 10 or len(date_b) < 10:
        return None
    try:
        ya, ma, da_d = int(date_a[:4]), int(date_a[5:7]), int(date_a[8:10])
        yb, mb, db_d = int(date_b[:4]), int(date_b[5:7]), int(date_b[8:10])
        # Simple Julian day number difference (accurate enough for day gaps)
        jdn_a = _jdn(ya, ma, da_d)
        jdn_b = _jdn(yb, mb, db_d)
        return abs(jdn_a - jdn_b)
    except (ValueError, IndexError):
        return None


def _jdn(year: int, month: int, day: int) -> int:
    """Julian Day Number for a given Gregorian date."""
    a = (14 - month) // 12
    y = year + 4800 - a
    m = month + 12 * a - 3
    return day + (153 * m + 2) // 5 + 365 * y + y // 4 - y // 100 + y // 400 - 32045


def _equip_tuple(rec: dict) -> tuple:
    """Return a hashable equipment state from a record."""
    oeil = (rec.get("oeilleres") or "").upper().strip()
    defe = (rec.get("deferre") or "").upper().strip()
    return (oeil, defe)


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
# PER-ENTITY STATE
# ===========================================================================


class _JockeyStats:
    """Track jockey win rate."""

    __slots__ = ("wins", "runs")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0

    @property
    def win_rate(self) -> float:
        return self.wins / self.runs if self.runs > 0 else 0.0


class _HorseHistory:
    """Track per-horse state for signal detection."""

    __slots__ = (
        "last_date", "last_jockey", "last_trainer", "last_equip",
        "last_won", "last_allocation", "trainer_change_date",
    )

    def __init__(self) -> None:
        self.last_date: Optional[str] = None
        self.last_jockey: Optional[str] = None
        self.last_trainer: Optional[str] = None
        self.last_equip: Optional[tuple] = None
        self.last_won: bool = False
        self.last_allocation: Optional[float] = None
        self.trainer_change_date: Optional[str] = None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_signal_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build signal features from partants_master.jsonl.

    Two-pass approach:
      1. Read minimal fields, sort chronologically.
      2. Process sequentially, accumulating per-horse and per-jockey state.
    """
    logger.info("=== Signal Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Parse allocation from course-level fields embedded in partant
        allocation = rec.get("allocation_totale")
        if allocation is None:
            # Try alternative field names
            allocation = rec.get("cnd_cond_allocation")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "horse_id": rec.get("horse_id"),
            "jockey": rec.get("jockey_driver"),
            "entraineur": rec.get("entraineur"),
            "gagnant": bool(rec.get("is_gagnant")),
            "equip": _equip_tuple(rec),
            "allocation": allocation,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process sequentially --
    t2 = time.time()
    jockey_stats: dict[str, _JockeyStats] = defaultdict(_JockeyStats)
    horse_state: dict[str, _HorseHistory] = defaultdict(_HorseHistory)
    results: list[dict[str, Any]] = []
    n_enriched = 0

    for idx, rec in enumerate(slim_records):
        cheval = rec["horse_id"] or rec["cheval"]
        jockey = rec["jockey"]
        entraineur = rec["entraineur"]
        date_iso = rec["date"][:10] if rec["date"] else ""

        if not cheval:
            results.append({
                "partant_uid": rec["uid"],
                "jockey_upgrade": None,
                "trainer_change_recent": None,
                "class_drop_after_win": None,
                "returning_from_break": None,
                "equipment_change": None,
            })
            continue

        state = horse_state[cheval]
        has_history = state.last_date is not None

        if not has_history:
            # First run for this horse
            jockey_upgrade = None
            trainer_change_recent = None
            class_drop_after_win = None
            returning_from_break = None
            equipment_change = None
        else:
            n_enriched += 1

            # 1. jockey_upgrade: current jockey has higher win rate than previous jockey
            if jockey and state.last_jockey and jockey != state.last_jockey:
                curr_wr = jockey_stats[jockey].win_rate
                prev_wr = jockey_stats[state.last_jockey].win_rate
                jockey_upgrade = 1 if curr_wr > prev_wr else 0
            elif jockey and state.last_jockey and jockey == state.last_jockey:
                jockey_upgrade = 0
            else:
                jockey_upgrade = None

            # 2. trainer_change_recent: trainer changed in last 90 days
            if entraineur and state.last_trainer:
                if entraineur != state.last_trainer:
                    # Trainer changed on this run -- mark the change date
                    trainer_change_recent = 1
                elif state.trainer_change_date is not None:
                    # Check if prior trainer change was within 90 days
                    gap = _days_between(date_iso, state.trainer_change_date)
                    trainer_change_recent = 1 if (gap is not None and gap <= BREAK_DAYS) else 0
                else:
                    trainer_change_recent = 0
            else:
                trainer_change_recent = None

            # 3. class_drop_after_win: won last race AND now in lower class
            if state.last_won and rec["allocation"] is not None and state.last_allocation is not None:
                try:
                    class_drop_after_win = 1 if float(rec["allocation"]) < float(state.last_allocation) else 0
                except (TypeError, ValueError):
                    class_drop_after_win = None
            elif state.last_won:
                class_drop_after_win = None  # can't determine class
            else:
                class_drop_after_win = 0

            # 4. returning_from_break: >90 days since last race
            gap_days = _days_between(date_iso, state.last_date)
            if gap_days is not None:
                returning_from_break = 1 if gap_days > BREAK_DAYS else 0
            else:
                returning_from_break = None

            # 5. equipment_change: any equipment field differs from last run
            if state.last_equip is not None:
                equipment_change = 1 if rec["equip"] != state.last_equip else 0
            else:
                equipment_change = None

        results.append({
            "partant_uid": rec["uid"],
            "jockey_upgrade": jockey_upgrade,
            "trainer_change_recent": trainer_change_recent,
            "class_drop_after_win": class_drop_after_win,
            "returning_from_break": returning_from_break,
            "equipment_change": equipment_change,
        })

        # -- Update state after emitting features (no leakage) --
        # Update jockey stats
        if jockey:
            jockey_stats[jockey].runs += 1
            if rec["gagnant"]:
                jockey_stats[jockey].wins += 1

        # Update horse state
        # Trainer change tracking
        if entraineur and state.last_trainer and entraineur != state.last_trainer:
            state.trainer_change_date = date_iso
        # Don't clear trainer_change_date if same trainer -- it persists

        state.last_date = date_iso
        state.last_jockey = jockey
        state.last_trainer = entraineur
        state.last_equip = rec["equip"]
        state.last_won = rec["gagnant"]
        state.last_allocation = rec["allocation"]

        if (idx + 1) % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", idx + 1, len(slim_records))

    elapsed = time.time() - t0
    logger.info(
        "Signal build termine: %d features en %.1fs (chevaux: %d, jockeys: %d, enrichis: %d)",
        len(results), elapsed, len(horse_state), len(jockey_stats), n_enriched,
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
        description="Construction des features signal a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/signal_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("signal_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_signal_features(input_path, logger)

    # Save
    out_path = output_dir / "signal_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
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
