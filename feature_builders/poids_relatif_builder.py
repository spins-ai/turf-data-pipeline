#!/usr/bin/env python3
"""
feature_builders.poids_relatif_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Relative weight features within a race.

Temporal integrity: features are intra-race (no horse history needed),
computed from the race's own field -- no future leakage.

Produces:
  - poids_relatif.jsonl  in output/poids_relatif/

Features per partant:
  - poids_vs_field_avg         : poids / average poids of the field
  - poids_handicap_advantage   : field_avg_poids - this_horse_poids (positive = advantage)
  - is_top_weight              : 1 if highest weight in the field
  - is_bottom_weight           : 1 if lowest weight in the field

Usage:
    python feature_builders/poids_relatif_builder.py
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "poids_relatif"

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
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
        return v if v == v and v > 0 else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_poids_relatif_features(input_path: Path, logger) -> list[dict[str, Any]]:
    logger.info("=== Poids Relatif Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # Phase 1: read and group by course
    course_records: dict[str, list[dict]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        course_key = rec.get("course_uid", "")
        course_records[course_key].append({
            "uid": rec.get("partant_uid"),
            "poids": _safe_float(rec.get("poids_porte_kg")),
        })

    logger.info("Phase 1 terminee: %d records, %d courses en %.1fs",
                n_read, len(course_records), time.time() - t0)

    # Phase 2: compute per course
    t1 = time.time()
    results: list[dict[str, Any]] = []

    for course_uid, runners in course_records.items():
        # Collect valid weights
        weights = [r["poids"] for r in runners if r["poids"] is not None]

        if not weights:
            for r in runners:
                results.append({
                    "partant_uid": r["uid"],
                    "poids_vs_field_avg": None,
                    "poids_handicap_advantage": None,
                    "is_top_weight": None,
                    "is_bottom_weight": None,
                })
            continue

        avg_w = sum(weights) / len(weights)
        max_w = max(weights)
        min_w = min(weights)

        for r in runners:
            poids = r["poids"]
            feats: dict[str, Any] = {"partant_uid": r["uid"]}

            if poids is None:
                feats["poids_vs_field_avg"] = None
                feats["poids_handicap_advantage"] = None
                feats["is_top_weight"] = None
                feats["is_bottom_weight"] = None
            else:
                feats["poids_vs_field_avg"] = round(poids / avg_w, 4) if avg_w > 0 else None
                feats["poids_handicap_advantage"] = round(avg_w - poids, 2)
                feats["is_top_weight"] = int(abs(poids - max_w) < 0.01)
                feats["is_bottom_weight"] = int(abs(poids - min_w) < 0.01)

            results.append(feats)

    elapsed = time.time() - t0
    logger.info(
        "Poids relatif build termine: %d features en %.1fs (courses: %d)",
        len(results), elapsed, len(course_records),
    )
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
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
        description="Construction des features poids relatif a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("poids_relatif_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_poids_relatif_features(input_path, logger)

    out_path = output_dir / "poids_relatif.jsonl"
    save_jsonl(results, out_path, logger)

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
