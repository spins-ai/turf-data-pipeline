#!/usr/bin/env python3
"""
feature_builders.first_time_events_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
5 features capturing first-time events for a horse.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant first-time event features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the history -- no future leakage.

Produces:
  - first_time_events.jsonl   in output/first_time_events/

Features per partant:
  - first_time_psf            : 1 if horse has never raced on PSF before
  - first_time_distance_cat   : 1 if horse has never raced at this distance category
  - first_time_hippodrome     : 1 if horse has never raced at this hippodrome
  - first_time_oeilleres      : 1 if horse has equipment change (oeilleres or deferre)
  - nb_firsts_count           : count of how many "firsts" this run has (0-4)

Usage:
    python feature_builders/first_time_events_builder.py
    python feature_builders/first_time_events_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "first_time_events"

# Distance category thresholds (metres) -- same as distance_preference_builder
CAT_SPRINT = 0       # < 1300m
CAT_MILE = 1         # 1300-1899m
CAT_INTERMEDIATE = 2  # 1900-2499m
CAT_STAYING = 3       # >= 2500m

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# HELPERS
# ===========================================================================


def _distance_category(distance_m: Optional[float]) -> Optional[int]:
    """Map distance in metres to a category code."""
    if distance_m is None:
        return None
    try:
        d = float(distance_m)
    except (TypeError, ValueError):
        return None
    if d < 1300:
        return CAT_SPRINT
    if d < 1900:
        return CAT_MILE
    if d < 2500:
        return CAT_INTERMEDIATE
    return CAT_STAYING


def _is_psf(rec: dict) -> bool:
    """Check if the race is on PSF (piste en sable fibre)."""
    # met_is_psf is a direct boolean flag
    if rec.get("met_is_psf"):
        return True
    # Also check type_piste for PSF-like surface
    tp = (rec.get("type_piste") or "").upper().strip()
    if "PSF" in tp or "SABLE" in tp:
        return True
    return False


def _equip_key(rec: dict) -> tuple:
    """Return a hashable equipment state (oeilleres, deferre)."""
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
# PER-HORSE STATE
# ===========================================================================


class _HorseHistory:
    """Lightweight per-horse accumulator for first-time checks."""

    __slots__ = ("hippodromes", "distance_cats", "has_psf", "last_equip")

    def __init__(self) -> None:
        self.hippodromes: set[str] = set()
        self.distance_cats: set[int] = set()
        self.has_psf: bool = False
        self.last_equip: Optional[tuple] = None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_first_time_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build first-time event features from partants_master.jsonl.

    Single-pass approach: read minimal fields, sort chronologically,
    then process sequentially accumulating per-horse state.
    """
    logger.info("=== First Time Events Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
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
            "horse_id": rec.get("horse_id"),
            "hippo": (rec.get("hippodrome_normalise") or "").upper().strip(),
            "distance": rec.get("distance"),
            "is_psf": _is_psf(rec),
            "equip": _equip_key(rec),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process sequentially --
    t2 = time.time()
    horse_state: dict[str, _HorseHistory] = defaultdict(_HorseHistory)
    results: list[dict[str, Any]] = []
    n_enriched = 0

    for idx, rec in enumerate(slim_records):
        cheval = rec["horse_id"] or rec["cheval"]

        if not cheval:
            results.append({
                "partant_uid": rec["uid"],
                "first_time_psf": None,
                "first_time_distance_cat": None,
                "first_time_hippodrome": None,
                "first_time_oeilleres": None,
                "nb_firsts_count": None,
            })
            continue

        state = horse_state[cheval]
        hippo = rec["hippo"]
        dist_cat = _distance_category(rec["distance"])
        is_psf = rec["is_psf"]
        equip = rec["equip"]

        # Determine if horse has any prior history at all
        has_history = bool(state.hippodromes or state.distance_cats or state.has_psf
                          or state.last_equip is not None)

        if not has_history:
            # First ever run for this horse -- all firsts are trivially true
            # but we mark None since there is no prior to compare against
            ft_psf = None
            ft_dist_cat = None
            ft_hippo = None
            ft_oeil = None
            nb_firsts = None
        else:
            n_enriched += 1
            # First time on PSF?
            if is_psf:
                ft_psf = 1 if not state.has_psf else 0
            else:
                ft_psf = 0

            # First time at this distance category?
            if dist_cat is not None:
                ft_dist_cat = 1 if dist_cat not in state.distance_cats else 0
            else:
                ft_dist_cat = None

            # First time at this hippodrome?
            if hippo:
                ft_hippo = 1 if hippo not in state.hippodromes else 0
            else:
                ft_hippo = None

            # Equipment change from last run?
            if state.last_equip is not None:
                ft_oeil = 1 if equip != state.last_equip else 0
            else:
                ft_oeil = None

            # Count of firsts (only count non-None True values)
            firsts = [v for v in (ft_psf, ft_dist_cat, ft_hippo, ft_oeil)
                      if v is not None]
            nb_firsts = sum(firsts) if firsts else 0

        results.append({
            "partant_uid": rec["uid"],
            "first_time_psf": ft_psf,
            "first_time_distance_cat": ft_dist_cat,
            "first_time_hippodrome": ft_hippo,
            "first_time_oeilleres": ft_oeil,
            "nb_firsts_count": nb_firsts,
        })

        # -- Update state after emitting features (no leakage) --
        if hippo:
            state.hippodromes.add(hippo)
        if dist_cat is not None:
            state.distance_cats.add(dist_cat)
        if is_psf:
            state.has_psf = True
        state.last_equip = equip

        if (idx + 1) % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", idx + 1, len(slim_records))

    elapsed = time.time() - t0
    logger.info(
        "First-time build termine: %d features en %.1fs (chevaux: %d, enrichis: %d)",
        len(results), elapsed, len(horse_state), n_enriched,
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
        description="Construction des features first-time events a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/first_time_events/)",
    )
    args = parser.parse_args()

    logger = setup_logging("first_time_events_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_first_time_features(input_path, logger)

    # Save
    out_path = output_dir / "first_time_events.jsonl"
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
