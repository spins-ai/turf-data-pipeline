#!/usr/bin/env python3
"""
feature_builders.odds_implied_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Odds-implied probability features.

Reads partants_master.jsonl in streaming mode, groups by course,
and computes per-partant odds-implied features.

Temporal integrity: these features are derived from race-day odds
(known before the race starts), no future leakage.

Produces:
  - odds_implied.jsonl   in output/odds_implied/

Features per partant:
  - implied_prob_normalized : (1/cote) / sum(1/cotes du champ) -- vraie probabilite
  - overround_share         : part de ce cheval dans le overround
  - fair_odds               : cote sans overround
  - odds_value_gap          : fair_odds - cote_finale (positif = value)

Usage:
    python feature_builders/odds_implied_builder.py
    python feature_builders/odds_implied_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "odds_implied"

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
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_odds_implied_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build odds-implied features from partants_master.jsonl."""
    logger.info("=== Odds Implied Builder ===")
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

        # Compute sum of implied probs for the field
        implied_probs: list[Optional[float]] = []
        for rec in course_group:
            cote = rec["cote"]
            if cote is not None and cote > 0:
                implied_probs.append(1.0 / cote)
            else:
                implied_probs.append(None)

        sum_implied = sum(p for p in implied_probs if p is not None)
        overround = sum_implied - 1.0 if sum_implied > 0 else 0.0
        nb_with_odds = sum(1 for p in implied_probs if p is not None)

        for idx, rec in enumerate(course_group):
            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "implied_prob_normalized": None,
                "overround_share": None,
                "fair_odds": None,
                "odds_value_gap": None,
            }

            ip = implied_probs[idx]
            cote = rec["cote"]

            if ip is not None and sum_implied > 0:
                # Normalized implied probability
                norm_prob = round(ip / sum_implied, 6)
                features["implied_prob_normalized"] = norm_prob

                # Overround share: this horse's contribution to the overround
                if overround > 0:
                    # overround_share = (ip - norm_prob) / overround
                    # Simpler: this horse's share of the total overround
                    features["overround_share"] = round((ip - norm_prob) / overround, 6) if overround > 0.001 else 0.0

                # Fair odds (without overround) = 1 / normalized_prob
                if norm_prob > 0:
                    fair = round(1.0 / norm_prob, 4)
                    features["fair_odds"] = fair

                    # Value gap: fair_odds - actual_odds (positive = value bet)
                    if cote is not None and cote > 0:
                        features["odds_value_gap"] = round(fair - cote, 4)

            results.append(features)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Odds implied build termine: %d features en %.1fs",
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
        description="Construction des features odds implied a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/odds_implied/)",
    )
    args = parser.parse_args()

    logger = setup_logging("odds_implied_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_odds_implied_features(input_path, logger)

    # Save
    out_path = output_dir / "odds_implied.jsonl"
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
