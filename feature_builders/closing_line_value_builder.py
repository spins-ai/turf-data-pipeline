#!/usr/bin/env python3
"""
feature_builders.closing_line_value_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Closing-line value and expected-value features derived from odds movements.

Reads partants_master.jsonl in streaming mode, computes value-betting
signals from opening (cote_reference) and closing (cote_finale) odds.

Temporal integrity: expected_value_brute uses only past race outcomes
(strict date < current race) to estimate win probability for similar
Elo + class profiles.

Produces:
  - closing_line_value.jsonl   in output/closing_line_value/

Features per partant:
  - closing_line_value    : (1/cote_ref - 1/cote_fin) * cote_fin
                            Positive = smart money came in (horse shortened)
  - expected_value_brute  : (estimated_prob * cote_fin) - 1
                            Where estimated_prob = historical win rate of
                            horses with similar elo+class profile
  - cote_movement_pct     : (cote_fin - cote_ref) / cote_ref * 100
  - is_value_bet          : 1 if expected_value_brute > 0

Usage:
    python feature_builders/closing_line_value_builder.py
    python feature_builders/closing_line_value_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import os
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "closing_line_value"

# Elo bucketing for estimated_prob (bucket width)
ELO_BUCKET_SIZE = 100
# Class bucketing granularity (group allocation ranges)
CLASS_BUCKET_SIZE = 10_000

# Progress log every N records
_LOG_EVERY = 500_000

# ===========================================================================
# PROFILE BUCKETING
# ===========================================================================


def _elo_bucket(elo_val: float | None) -> int:
    """Round Elo to nearest bucket centre."""
    if elo_val is None:
        return 0
    return int(round(elo_val / ELO_BUCKET_SIZE)) * ELO_BUCKET_SIZE


def _class_bucket(allocation: float | None) -> int:
    """Round allocation value to nearest class bucket."""
    if allocation is None:
        return 0
    return int(round(allocation / CLASS_BUCKET_SIZE)) * CLASS_BUCKET_SIZE


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


def _sort_key(rec: dict) -> tuple:
    """Sort key: date, course_uid, num_pmu for determinism."""
    return (
        rec.get("date", ""),
        rec.get("course", ""),
        rec.get("num", 0) or 0,
    )


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_closing_line_value_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build closing-line value features from partants_master.jsonl.

    Two-phase approach:
      1. Read minimal fields into memory, sort chronologically.
      2. Process in order, accumulating historical win-rate stats per
         (elo_bucket, class_bucket) profile for expected_value_brute.

    Temporal integrity: for any partant at date D, only races with
    date < D contribute to the estimated probability.
    """
    logger.info("=== Closing Line Value Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
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
            "cote_ref": rec.get("cote_reference"),
            "cote_fin": rec.get("cote_finale"),
            "elo_combined": rec.get("elo_combined"),
            "allocation": rec.get("allocation"),
            "gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=_sort_key)
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process record by record ──
    t2 = time.time()

    # Historical win-rate accumulator: (elo_bucket, class_bucket) -> {wins, total}
    profile_stats: dict[tuple[int, int], dict[str, int]] = defaultdict(
        lambda: {"wins": 0, "total": 0}
    )

    results: list[dict[str, Any]] = []
    n_processed = 0
    prev_date = ""

    # Group by date to ensure we only use strictly past data
    # All records with the same date get the same profile snapshot
    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants for this date
        current_date = slim_records[i]["date"]
        date_group: list[dict] = []

        while i < total and slim_records[i]["date"] == current_date:
            date_group.append(slim_records[i])
            i += 1

        # ── Emit features for all partants of this date ──
        # profile_stats contains only data from dates < current_date
        for rec in date_group:
            uid = rec["uid"]
            cote_ref = rec["cote_ref"]
            cote_fin = rec["cote_fin"]

            # -- closing_line_value --
            clv = None
            if (
                cote_ref is not None
                and cote_fin is not None
                and cote_ref > 0
                and cote_fin > 0
            ):
                clv = round((1.0 / cote_ref - 1.0 / cote_fin) * cote_fin, 6)

            # -- cote_movement_pct --
            cote_mvt = None
            if cote_ref is not None and cote_fin is not None and cote_ref > 0:
                cote_mvt = round(
                    (cote_fin - cote_ref) / cote_ref * 100.0, 4
                )

            # -- expected_value_brute --
            elo_b = _elo_bucket(rec.get("elo_combined"))
            cls_b = _class_bucket(rec.get("allocation"))
            profile_key = (elo_b, cls_b)

            ev_brute = None
            is_vb = None
            stats = profile_stats.get(profile_key)
            if stats is not None and stats["total"] >= 5 and cote_fin is not None and cote_fin > 0:
                estimated_prob = stats["wins"] / stats["total"]
                ev_brute = round(estimated_prob * cote_fin - 1.0, 6)
                is_vb = 1 if ev_brute > 0 else 0

            results.append({
                "partant_uid": uid,
                "closing_line_value": clv,
                "expected_value_brute": ev_brute,
                "cote_movement_pct": cote_mvt,
                "is_value_bet": is_vb,
            })

        # ── Update profile stats with this date's outcomes ──
        for rec in date_group:
            elo_b = _elo_bucket(rec.get("elo_combined"))
            cls_b = _class_bucket(rec.get("allocation"))
            profile_key = (elo_b, cls_b)
            profile_stats[profile_key]["total"] += 1
            if rec["gagnant"]:
                profile_stats[profile_key]["wins"] += 1

        n_processed += len(date_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "CLV build termine: %d features en %.1fs (profils uniques: %d)",
        len(results),
        elapsed,
        len(profile_stats),
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
        description="Construction des features Closing Line Value a partir de partants_master"
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
        help="Repertoire de sortie (defaut: output/closing_line_value/)",
    )
    args = parser.parse_args()

    logger = setup_logging("closing_line_value_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_closing_line_value_features(input_path, logger)

    # Save
    out_path = output_dir / "closing_line_value.jsonl"
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
