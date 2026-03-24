#!/usr/bin/env python3
"""
feature_builders.lag_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Lag features: recent race history per horse.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant lag-based features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the lag features -- no future leakage.

Produces:
  - lag_features.jsonl   in output/lag_features/

Features per partant:
  - lag_position_1       : position course N-1 du meme cheval
  - lag_position_2       : position course N-2
  - lag_position_3       : position course N-3
  - lag_cote_1           : cote finale course N-1
  - lag_days_since_last  : jours depuis derniere course

Usage:
    python feature_builders/lag_features_builder.py
    python feature_builders/lag_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "lag_features"

# Progress log every N records
_LOG_EVERY = 500_000


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


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime, return None on failure."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


class _HorseHistory:
    """Stores last 3 positions, last cote, and last race date for a horse."""

    __slots__ = ("positions", "cotes", "last_date")

    def __init__(self) -> None:
        self.positions: list[Optional[int]] = []  # most recent first
        self.cotes: list[Optional[float]] = []    # most recent first
        self.last_date: Optional[datetime] = None


def build_lag_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build lag features from partants_master.jsonl.

    Single-pass approach: read all records with minimal fields,
    sort chronologically, then process sequentially.
    """
    logger.info("=== Lag Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Extract cote: try multiple field names
        cote = rec.get("cote_probable") or rec.get("rapport_final") or rec.get("cote_finale")
        if cote is not None:
            try:
                cote = float(cote)
            except (ValueError, TypeError):
                cote = None

        position = rec.get("position_arrivee")
        if position is not None:
            try:
                position = int(position)
            except (ValueError, TypeError):
                position = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "position": position,
            "cote": cote,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process sequentially, building lag features --
    t2 = time.time()
    horse_history: dict[str, _HorseHistory] = defaultdict(_HorseHistory)
    results: list[dict[str, Any]] = []
    n_processed = 0

    for rec in slim_records:
        n_processed += 1
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, len(slim_records))

        cheval = rec["cheval"]
        date_str = rec["date"]
        race_date = _parse_date(date_str)

        if not cheval:
            results.append({
                "partant_uid": rec["uid"],
                "lag_position_1": None,
                "lag_position_2": None,
                "lag_position_3": None,
                "lag_cote_1": None,
                "lag_days_since_last": None,
            })
            continue

        hist = horse_history[cheval]

        # -- Snapshot pre-race features (temporal integrity) --
        lag_pos_1 = hist.positions[0] if len(hist.positions) >= 1 else None
        lag_pos_2 = hist.positions[1] if len(hist.positions) >= 2 else None
        lag_pos_3 = hist.positions[2] if len(hist.positions) >= 3 else None
        lag_cote_1 = hist.cotes[0] if len(hist.cotes) >= 1 else None

        lag_days: Optional[int] = None
        if hist.last_date is not None and race_date is not None:
            delta = (race_date - hist.last_date).days
            if delta >= 0:
                lag_days = delta

        results.append({
            "partant_uid": rec["uid"],
            "lag_position_1": lag_pos_1,
            "lag_position_2": lag_pos_2,
            "lag_position_3": lag_pos_3,
            "lag_cote_1": round(lag_cote_1, 2) if lag_cote_1 is not None else None,
            "lag_days_since_last": lag_days,
        })

        # -- Update history after emitting features (no leakage) --
        position = rec["position"]
        cote = rec["cote"]

        # Prepend to history (most recent first), keep max 3
        hist.positions.insert(0, position)
        if len(hist.positions) > 3:
            hist.positions.pop()

        hist.cotes.insert(0, cote)
        if len(hist.cotes) > 3:
            hist.cotes.pop()

        if race_date is not None:
            hist.last_date = race_date

    elapsed = time.time() - t0
    logger.info(
        "Lag features build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_history),
    )

    return results


# ===========================================================================
# SAUVEGARDE & CLI
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
        description="Construction des lag features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/lag_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("lag_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_lag_features(input_path, logger)

    # Save
    out_path = output_dir / "lag_features.jsonl"
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
