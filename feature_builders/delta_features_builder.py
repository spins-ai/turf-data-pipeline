#!/usr/bin/env python3
"""
feature_builders.delta_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Consecutive race deltas per horse.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant delta-based features comparing
consecutive races for the same horse.

Temporal integrity: for any partant at date D, only the horse's most recent
race with date < D contributes to the delta features -- no future leakage.

Produces:
  - delta_features.jsonl   in output/delta_features/

Features per partant:
  - delta_cote          : cote_finale - previous cote_finale
  - delta_poids         : poids_porte_kg - previous poids_porte_kg
  - delta_distance      : distance - previous distance
  - delta_reduction_km  : reduction_km_ms - previous reduction_km_ms
  - same_hippodrome     : 1 if same hippodrome as last race, else 0
  - same_jockey         : 1 if same jockey as last race, else 0
  - same_discipline     : 1 if same discipline as last race, else 0
  - days_between        : days since previous race

Usage:
    python feature_builders/delta_features_builder.py
    python feature_builders/delta_features_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "delta_features"

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


def _safe_float(val) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# HORSE PREVIOUS-RACE STATE
# ===========================================================================


class _HorsePrev:
    """Stores the previous race context for a horse."""

    __slots__ = ("cote", "poids", "distance", "reduction_km",
                 "hippodrome", "jockey", "discipline", "date")

    def __init__(self) -> None:
        self.cote: Optional[float] = None
        self.poids: Optional[float] = None
        self.distance: Optional[float] = None
        self.reduction_km: Optional[float] = None
        self.hippodrome: Optional[str] = None
        self.jockey: Optional[str] = None
        self.discipline: Optional[str] = None
        self.date: Optional[datetime] = None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_delta_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build delta features from partants_master.jsonl.

    Single-pass approach: read all records with minimal fields,
    sort chronologically, then process sequentially.
    """
    logger.info("=== Delta Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
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
            "cote": _safe_float(
                rec.get("cote_probable") or rec.get("rapport_final") or rec.get("cote_finale")
            ),
            "poids": _safe_float(rec.get("poids_porte_kg")),
            "distance": _safe_float(rec.get("distance")),
            "reduction_km": _safe_float(rec.get("reduction_km_ms")),
            "hippodrome": rec.get("hippodrome_code") or rec.get("hippodrome"),
            "jockey": rec.get("jockey_nom") or rec.get("nom_jockey"),
            "discipline": rec.get("discipline"),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process sequentially, building delta features --
    t2 = time.time()
    horse_prev: dict[str, _HorsePrev] = defaultdict(_HorsePrev)
    results: list[dict[str, Any]] = []
    n_processed = 0

    _null_row = {
        "delta_cote": None,
        "delta_poids": None,
        "delta_distance": None,
        "delta_reduction_km": None,
        "same_hippodrome": None,
        "same_jockey": None,
        "same_discipline": None,
        "days_between": None,
    }

    for rec in slim_records:
        n_processed += 1
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, len(slim_records))

        cheval = rec["cheval"]
        race_date = _parse_date(rec["date"])

        if not cheval:
            results.append({"partant_uid": rec["uid"], **_null_row})
            continue

        prev = horse_prev[cheval]

        # -- Snapshot pre-race features (temporal integrity) --
        # Only emit deltas if there was a previous race
        has_prev = prev.date is not None

        if has_prev:
            delta_cote = None
            if rec["cote"] is not None and prev.cote is not None:
                delta_cote = round(rec["cote"] - prev.cote, 2)

            delta_poids = None
            if rec["poids"] is not None and prev.poids is not None:
                delta_poids = round(rec["poids"] - prev.poids, 2)

            delta_distance = None
            if rec["distance"] is not None and prev.distance is not None:
                delta_distance = round(rec["distance"] - prev.distance, 2)

            delta_reduction_km = None
            if rec["reduction_km"] is not None and prev.reduction_km is not None:
                delta_reduction_km = round(rec["reduction_km"] - prev.reduction_km, 2)

            same_hippo = None
            if rec["hippodrome"] and prev.hippodrome:
                same_hippo = 1 if rec["hippodrome"] == prev.hippodrome else 0

            same_jock = None
            if rec["jockey"] and prev.jockey:
                same_jock = 1 if rec["jockey"] == prev.jockey else 0

            same_disc = None
            if rec["discipline"] and prev.discipline:
                same_disc = 1 if rec["discipline"] == prev.discipline else 0

            days_bet: Optional[int] = None
            if race_date is not None and prev.date is not None:
                delta_days = (race_date - prev.date).days
                if delta_days >= 0:
                    days_bet = delta_days

            results.append({
                "partant_uid": rec["uid"],
                "delta_cote": delta_cote,
                "delta_poids": delta_poids,
                "delta_distance": delta_distance,
                "delta_reduction_km": delta_reduction_km,
                "same_hippodrome": same_hippo,
                "same_jockey": same_jock,
                "same_discipline": same_disc,
                "days_between": days_bet,
            })
        else:
            results.append({"partant_uid": rec["uid"], **_null_row})

        # -- Update previous-race state after emitting features (no leakage) --
        prev.cote = rec["cote"]
        prev.poids = rec["poids"]
        prev.distance = rec["distance"]
        prev.reduction_km = rec["reduction_km"]
        prev.hippodrome = rec["hippodrome"]
        prev.jockey = rec["jockey"]
        prev.discipline = rec["discipline"]
        if race_date is not None:
            prev.date = race_date

    elapsed = time.time() - t0
    logger.info(
        "Delta features build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_prev),
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
        description="Construction des delta features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/delta_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("delta_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_delta_features(input_path, logger)

    # Save
    out_path = output_dir / "delta_features.jsonl"
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
