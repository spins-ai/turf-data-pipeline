#!/usr/bin/env python3
"""
feature_builders.odds_market_timing_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Odds market timing features -- when and how the market moves for each horse.

Reads partants_master.jsonl in streaming mode, indexes + sorts
chronologically, then processes course-by-course with snapshot-before-
update semantics to avoid future leakage.

Temporal integrity: for any partant at date D, only races with date < D
contribute to horse-level steam/drift statistics -- no future leakage.

Produces:
  - odds_market_timing.jsonl   in builder_outputs/odds_market_timing/

Features per partant (8):
  - omt_late_money_indicator   : (cote_reference - cote_finale) / cote_reference
                                 positive = late money came in
  - omt_abs_market_shift       : absolute value of late_money_indicator
  - omt_is_steamer             : 1 if cote_finale < cote_reference * 0.8 (significant tightening)
  - omt_is_drifter             : 1 if cote_finale > cote_reference * 1.25 (significant weakening)
  - omt_horse_steam_rate       : fraction of past races where horse was a steamer
  - omt_horse_drift_rate       : fraction of past races where horse was a drifter
  - omt_steam_success_rate     : horse's win rate when steaming (from past)
  - omt_drift_success_rate     : horse's win rate when drifting (from past)

Usage:
    python feature_builders/odds_market_timing_builder.py
    python feature_builders/odds_market_timing_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/odds_market_timing")

_LOG_EVERY = 500_000

# Steamer/drifter thresholds
_STEAMER_THRESHOLD = 0.80   # cote_finale < cote_reference * 0.80
_DRIFTER_THRESHOLD = 1.25   # cote_finale > cote_reference * 1.25


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


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f and f > 0 else None  # NaN check + positive
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseTimingState:
    """Track steam/drift history per horse."""

    __slots__ = ("steam_count", "drift_count", "steam_wins", "drift_wins", "total_races")

    def __init__(self) -> None:
        self.steam_count: int = 0
        self.drift_count: int = 0
        self.steam_wins: int = 0
        self.drift_wins: int = 0
        self.total_races: int = 0

    def snapshot(self) -> dict[str, Any]:
        """Return historical steam/drift features BEFORE this race is counted."""
        feats: dict[str, Any] = {
            "omt_horse_steam_rate": None,
            "omt_horse_drift_rate": None,
            "omt_steam_success_rate": None,
            "omt_drift_success_rate": None,
        }

        if self.total_races >= 1:
            feats["omt_horse_steam_rate"] = round(self.steam_count / self.total_races, 4)
            feats["omt_horse_drift_rate"] = round(self.drift_count / self.total_races, 4)

        if self.steam_count >= 1:
            feats["omt_steam_success_rate"] = round(self.steam_wins / self.steam_count, 4)

        if self.drift_count >= 1:
            feats["omt_drift_success_rate"] = round(self.drift_wins / self.drift_count, 4)

        return feats

    def update(self, is_steamer: bool, is_drifter: bool, is_winner: bool) -> None:
        """Update state AFTER snapshot."""
        self.total_races += 1
        if is_steamer:
            self.steam_count += 1
            if is_winner:
                self.steam_wins += 1
        if is_drifter:
            self.drift_count += 1
            if is_winner:
                self.drift_wins += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_odds_market_timing(input_path: Path, output_path: Path, logger) -> int:
    """Build odds market timing features -- two-phase index+sort then course-by-course."""
    logger.info("=== Odds Market Timing Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Read minimal fields into slim records
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "cote_finale": _safe_float(rec.get("cote_finale")),
            "cote_reference": _safe_float(rec.get("cote_reference")),
            "is_gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)
    gc.collect()

    # ------------------------------------------------------------------
    # Phase 3: Course-by-course processing
    # ------------------------------------------------------------------
    logger.info("Phase 3: traitement course par course...")
    t2 = time.time()

    horse_states: dict[str, _HorseTimingState] = defaultdict(_HorseTimingState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    fill_counts = {
        "omt_late_money_indicator": 0,
        "omt_abs_market_shift": 0,
        "omt_is_steamer": 0,
        "omt_is_drifter": 0,
        "omt_horse_steam_rate": 0,
        "omt_horse_drift_rate": 0,
        "omt_steam_success_rate": 0,
        "omt_drift_success_rate": 0,
    }

    i = 0
    total = len(slim_records)

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
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

            # ----- Snapshot BEFORE update, emit features -----
            post_updates: list[dict] = []

            for rec in course_group:
                h = rec["cheval"]
                cf = rec["cote_finale"]
                cr = rec["cote_reference"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                # --- Current-race features (from this race's odds) ---

                # 1. omt_late_money_indicator
                if cf is not None and cr is not None and cr > 0:
                    lmi = round((cr - cf) / cr, 4)
                    features["omt_late_money_indicator"] = lmi
                    fill_counts["omt_late_money_indicator"] += 1
                else:
                    lmi = None
                    features["omt_late_money_indicator"] = None

                # 2. omt_abs_market_shift
                if lmi is not None:
                    features["omt_abs_market_shift"] = round(abs(lmi), 4)
                    fill_counts["omt_abs_market_shift"] += 1
                else:
                    features["omt_abs_market_shift"] = None

                # 3. omt_is_steamer
                if cf is not None and cr is not None and cr > 0:
                    is_steamer = int(cf < cr * _STEAMER_THRESHOLD)
                    features["omt_is_steamer"] = is_steamer
                    fill_counts["omt_is_steamer"] += 1
                else:
                    is_steamer = 0
                    features["omt_is_steamer"] = None

                # 4. omt_is_drifter
                if cf is not None and cr is not None and cr > 0:
                    is_drifter = int(cf > cr * _DRIFTER_THRESHOLD)
                    features["omt_is_drifter"] = is_drifter
                    fill_counts["omt_is_drifter"] += 1
                else:
                    is_drifter = 0
                    features["omt_is_drifter"] = None

                # --- Historical features (snapshot BEFORE update) ---
                if h:
                    state = horse_states[h]
                    hist = state.snapshot()
                    for k, v in hist.items():
                        features[k] = v
                        if v is not None:
                            fill_counts[k] += 1
                else:
                    features["omt_horse_steam_rate"] = None
                    features["omt_horse_drift_rate"] = None
                    features["omt_steam_success_rate"] = None
                    features["omt_drift_success_rate"] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update info
                post_updates.append({
                    "cheval": h,
                    "is_steamer": bool(is_steamer),
                    "is_drifter": bool(is_drifter),
                    "is_gagnant": rec["is_gagnant"],
                })

            # ----- Update global state AFTER snapshot -----
            for upd in post_updates:
                h = upd["cheval"]
                if h:
                    horse_states[h].update(
                        is_steamer=upd["is_steamer"],
                        is_drifter=upd["is_drifter"],
                        is_winner=upd["is_gagnant"],
                    )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Odds market timing build termine: %d features en %.1fs "
        "(chevaux uniques: %d)",
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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features odds market timing a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/odds_market_timing/)",
    )
    args = parser.parse_args()

    logger = setup_logging("odds_market_timing_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "odds_market_timing.jsonl"
    build_odds_market_timing(input_path, out_path, logger)


if __name__ == "__main__":
    main()
