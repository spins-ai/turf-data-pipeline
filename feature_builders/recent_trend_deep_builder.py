#!/usr/bin/env python3
"""
feature_builders.recent_trend_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Detailed recent form / trend analysis using exponentially weighted moving
averages (EWMA) computed per horse.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant recent-trend features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - recent_trend_deep.jsonl   in OUTPUT_DIR

Features per partant (8):
  - rtd_ewma_position      : EWMA of position_pct (alpha=0.3); lower = improving
  - rtd_ewma_win           : EWMA of binary win indicator (alpha=0.3); higher = winning form
  - rtd_form_acceleration  : EWMA(last 3 pos_pct) - EWMA(last 10 pos_pct); negative = improving
  - rtd_win_drought        : races since last win, capped at 50
  - rtd_last3_avg_pos_pct  : simple average of position_pct over last 3 races
  - rtd_last3_vs_last10    : last3_avg - last10_avg; negative = improving
  - rtd_hot_streak         : consecutive top-3 finishes (current streak)
  - rtd_cold_streak        : consecutive finishes outside top 50% of field

Usage:
    python feature_builders/recent_trend_deep_builder.py
    python feature_builders/recent_trend_deep_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/recent_trend_deep_builder.py --output-dir /path/to/output
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/recent_trend_deep")

_LOG_EVERY = 500_000

# EWMA smoothing factor
_ALPHA = 0.3

# Deque maxlen -- we keep 20 races of history but only use up to 10 for features
_HISTORY_LEN = 20

# Drought cap
_DROUGHT_CAP = 50


# ===========================================================================
# HELPERS
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


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if not math.isnan(v) else None
    except (ValueError, TypeError):
        return None


def _ewma_of_list(values: list[float], alpha: float) -> Optional[float]:
    """Compute EWMA of a list (oldest-first). Returns None if empty."""
    if not values:
        return None
    result = values[0]
    for v in values[1:]:
        result = alpha * v + (1.0 - alpha) * result
    return result


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Tracks rolling state for one horse.

    positions   : deque of position_pct values (oldest → newest)
    wins        : deque of 0/1 win indicators
    ewma_pos    : current EWMA of position_pct
    ewma_win    : current EWMA of win indicator
    races_since_win : races since the last win (updated each race)
    current_top3_streak  : consecutive top-3 finishes (current streak)
    current_cold_streak  : consecutive finishes outside top 50% of field
    """

    __slots__ = (
        "positions",
        "wins",
        "ewma_pos",
        "ewma_win",
        "races_since_win",
        "current_top3_streak",
        "current_cold_streak",
    )

    def __init__(self) -> None:
        self.positions: deque[float] = deque(maxlen=_HISTORY_LEN)
        self.wins: deque[int] = deque(maxlen=_HISTORY_LEN)
        self.ewma_pos: Optional[float] = None
        self.ewma_win: Optional[float] = None
        self.races_since_win: int = 0
        self.current_top3_streak: int = 0
        self.current_cold_streak: int = 0

    # ------------------------------------------------------------------
    # SNAPSHOT  (call BEFORE update -- temporal integrity)
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return feature values based on history so far (pre-race)."""
        n = len(self.positions)

        # rtd_ewma_position & rtd_ewma_win
        ewma_pos = self.ewma_pos
        ewma_win = self.ewma_win

        # rtd_form_acceleration: EWMA(last 3) - EWMA(last 10)
        form_acceleration: Optional[float] = None
        if n >= 1:
            last3 = list(self.positions)[-3:]
            last10 = list(self.positions)[-10:]
            ewma3 = _ewma_of_list(last3, _ALPHA)
            ewma10 = _ewma_of_list(last10, _ALPHA)
            if ewma3 is not None and ewma10 is not None:
                form_acceleration = round(ewma3 - ewma10, 6)

        # rtd_win_drought
        win_drought: Optional[int] = None
        if n >= 1 or self.races_since_win > 0:
            win_drought = min(self.races_since_win, _DROUGHT_CAP)

        # rtd_last3_avg_pos_pct
        last3_avg: Optional[float] = None
        if n >= 1:
            last3_vals = list(self.positions)[-3:]
            last3_avg = round(sum(last3_vals) / len(last3_vals), 6)

        # rtd_last3_vs_last10
        last3_vs_last10: Optional[float] = None
        if n >= 1:
            last10_vals = list(self.positions)[-10:]
            last10_avg = sum(last10_vals) / len(last10_vals)
            if last3_avg is not None:
                last3_vs_last10 = round(last3_avg - last10_avg, 6)

        return {
            "rtd_ewma_position": round(ewma_pos, 6) if ewma_pos is not None else None,
            "rtd_ewma_win": round(ewma_win, 6) if ewma_win is not None else None,
            "rtd_form_acceleration": form_acceleration,
            "rtd_win_drought": win_drought,
            "rtd_last3_avg_pos_pct": last3_avg,
            "rtd_last3_vs_last10": last3_vs_last10,
            "rtd_hot_streak": self.current_top3_streak if n >= 1 else None,
            "rtd_cold_streak": self.current_cold_streak if n >= 1 else None,
        }

    # ------------------------------------------------------------------
    # UPDATE  (call AFTER snapshot -- post-race)
    # ------------------------------------------------------------------

    def update(self, position_pct: Optional[float], won: bool) -> None:
        """Incorporate a completed race result into state."""
        if position_pct is None:
            # No valid position info: update drought only
            if not won:
                self.races_since_win += 1
            else:
                self.races_since_win = 0
            return

        # Append to deques
        self.positions.append(position_pct)
        win_flag = 1 if won else 0
        self.wins.append(win_flag)

        # Update EWMA
        if self.ewma_pos is None:
            self.ewma_pos = position_pct
        else:
            self.ewma_pos = _ALPHA * position_pct + (1.0 - _ALPHA) * self.ewma_pos

        if self.ewma_win is None:
            self.ewma_win = float(win_flag)
        else:
            self.ewma_win = _ALPHA * win_flag + (1.0 - _ALPHA) * self.ewma_win

        # Win drought
        if won:
            self.races_since_win = 0
        else:
            self.races_since_win += 1

        # Hot streak (consecutive top-3 finishes = position_pct <= 3/N)
        # position_pct is position / nombre_partants; top-3 means pct <= 3/N
        # We approximate top-3 as position_pct <= 0.15 for large fields or
        # explicitly check if the stored value corresponds to positions 1, 2, 3.
        # Since we only have position_pct, we use a threshold of 0.33 (top third)
        # to robustly capture "top 3 out of >= 9 runners".  For small fields (<9)
        # we use position_pct <= 3/N; however N is not stored in the deque.
        # We use position_pct <= 0.33 as a pragmatic proxy for "top 3" which is
        # exact for 9+ runner fields and slightly generous for smaller ones.
        IS_TOP3_THRESHOLD = 0.34  # position_pct <= 1/3 ≈ top 3 out of 9+
        if position_pct <= IS_TOP3_THRESHOLD:
            self.current_top3_streak += 1
            self.current_cold_streak = 0
        else:
            self.current_top3_streak = 0

        # Cold streak (finishes outside top 50% = position_pct > 0.5)
        if position_pct > 0.50:
            self.current_cold_streak += 1
        else:
            self.current_cold_streak = 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_recent_trend_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build recent trend deep features from partants_master.jsonl."""
    logger.info("=== Recent Trend Deep Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1 : Read minimal fields into memory
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Horse identity: prefer horse_id, fall back to nom_cheval
        horse_id = rec.get("horse_id") or rec.get("nom_cheval")

        position = _safe_int(rec.get("position_arrivee"))
        nombre_partants = _safe_int(rec.get("nombre_partants"))

        # Compute position_pct = position / nombre_partants (in ]0,1])
        position_pct: Optional[float] = None
        if (
            position is not None
            and nombre_partants is not None
            and nombre_partants > 0
            and position > 0
        ):
            position_pct = position / nombre_partants

        # Win flag: prefer is_gagnant, else derive from position == 1
        is_gagnant_raw = rec.get("is_gagnant")
        if is_gagnant_raw is not None:
            won = bool(is_gagnant_raw)
        else:
            won = position == 1

        slim_records.append(
            {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "horse_id": horse_id,
                "position_pct": position_pct,
                "won": won,
            }
        )

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2 : Sort chronologically (date, course, num_pmu)
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3 : Process course by course, snapshot BEFORE update
    # ------------------------------------------------------------------
    t2 = time.time()

    horse_states: dict[Any, _HorseState] = defaultdict(_HorseState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    i = 0
    while i < total:
        # Collect all partants of the same course
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

        # -- Snapshot features BEFORE this race (temporal integrity) --
        for rec in course_group:
            horse_id = rec["horse_id"]
            state = horse_states[horse_id] if horse_id else None

            if state is not None:
                snap = state.snapshot()
            else:
                snap = {
                    "rtd_ewma_position": None,
                    "rtd_ewma_win": None,
                    "rtd_form_acceleration": None,
                    "rtd_win_drought": None,
                    "rtd_last3_avg_pos_pct": None,
                    "rtd_last3_vs_last10": None,
                    "rtd_hot_streak": None,
                    "rtd_cold_streak": None,
                }

            snap["partant_uid"] = rec["uid"]
            results.append(snap)

        # -- Update states AFTER snapshotting --
        for rec in course_group:
            horse_id = rec["horse_id"]
            if horse_id is not None:
                horse_states[horse_id].update(rec["position_pct"], rec["won"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Recent Trend Deep build termine: %d features en %.1fs (%d chevaux uniques)",
        len(results),
        elapsed,
        len(horse_states),
    )

    # Free large intermediate structures
    del slim_records, horse_states
    gc.collect()

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _save_jsonl(records: list[dict], path: Path, logger) -> None:
    """Write records to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info("Sauvegarde: %d records -> %s", len(records), path)


def _find_input(cli_path: Optional[str], logger) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")

    # Fallback candidates
    candidates = [
        INPUT_PARTANTS,
        Path(__file__).resolve().parent.parent / "data_master" / "partants_master.jsonl",
        Path(__file__).resolve().parent.parent / "data_master" / "partants_master_enrichi.jsonl",
    ]
    for c in candidates:
        if c.exists():
            logger.info("Fichier d'entree auto-detecte: %s", c)
            return c

    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve. Essayez --input. Chemins testes: "
        + ", ".join(str(c) for c in candidates)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features recent trend deep a partir de partants_master"
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
        help="Repertoire de sortie (defaut: OUTPUT_DIR)",
    )
    args = parser.parse_args()

    logger = setup_logging("recent_trend_deep_builder")

    input_path = _find_input(args.input, logger)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_recent_trend_features(input_path, logger)

    out_path = output_dir / "recent_trend_deep.jsonl"
    _save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info(
                "  %-32s %d/%d (%.1f%%)",
                k,
                v,
                total_count,
                100.0 * v / total_count if total_count else 0.0,
            )

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
