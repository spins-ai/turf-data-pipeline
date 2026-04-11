#!/usr/bin/env python3
"""
feature_builders.race_competitiveness_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race competitiveness features.

Reads partants_master.jsonl in streaming mode, groups by course,
and computes per-course competitiveness metrics.

Temporal integrity: these features are derived from field composition
and pre-race data (known before the race), no future leakage.
For elo_spread, uses spd_class_rating as proxy for Elo rating.

Produces:
  - race_competitiveness.jsonl   in output/race_competitiveness/

Features per partant:
  - competitiveness_score   : composite (elo_spread x field_size x allocation)
  - elo_spread              : max_elo - min_elo dans le champ
  - cote_concentration      : top-3 cotes / sum cotes (champ ferme vs ouvert)
  - nb_serious_contenders   : nb chevaux avec cote < 10

Usage:
    python feature_builders/race_competitiveness_builder.py
    python feature_builders/race_competitiveness_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "race_competitiveness"

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_race_competitiveness_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build race competitiveness features from partants_master.jsonl."""
    logger.info("=== Race Competitiveness Builder ===")
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
            "cote": _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference")),
            "elo": _safe_float(rec.get("spd_class_rating")),
            "nb_partants": _safe_int(rec.get("nombre_partants")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Group by course and compute --
    t2 = time.time()
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        # Collect field-level stats
        elos: list[float] = [r["elo"] for r in course_group if r["elo"] is not None]
        cotes: list[float] = [r["cote"] for r in course_group if r["cote"] is not None and r["cote"] > 0]
        field_size = len(course_group)

        # Elo spread
        elo_spread: Optional[float] = None
        if len(elos) >= 2:
            elo_spread = round(max(elos) - min(elos), 4)

        # Cote concentration: sum of top-3 lowest cotes / sum of all cotes
        cote_concentration: Optional[float] = None
        if len(cotes) >= 3:
            sorted_cotes = sorted(cotes)
            # Top-3 favorites = lowest odds = highest implied prob
            # Their implied probs dominate if field is concentrated
            top3_implied = sum(1.0 / c for c in sorted_cotes[:3])
            total_implied = sum(1.0 / c for c in sorted_cotes)
            if total_implied > 0:
                cote_concentration = round(top3_implied / total_implied, 4)

        # Nb serious contenders: cote < 10
        nb_serious: Optional[int] = None
        if cotes:
            nb_serious = sum(1 for c in cotes if c < 10.0)

        # Competitiveness score: composite
        competitiveness: Optional[float] = None
        if elo_spread is not None and field_size > 0:
            # Normalize: elo_spread (higher = less competitive), field_size (higher = more competitive)
            # Score = field_size / (1 + elo_spread) -- higher = more competitive (tight field, many runners)
            competitiveness = round(field_size / (1.0 + elo_spread), 4)

        for rec in course_group:
            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "competitiveness_score": competitiveness,
                "elo_spread": elo_spread,
                "cote_concentration": cote_concentration,
                "nb_serious_contenders": nb_serious,
            }
            results.append(features)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Race competitiveness build termine: %d features en %.1fs",
        len(results), elapsed,
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
        description="Construction des features race competitiveness a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/race_competitiveness/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_competitiveness_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_race_competitiveness_features(input_path, logger)

    # Save
    out_path = output_dir / "race_competitiveness.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
