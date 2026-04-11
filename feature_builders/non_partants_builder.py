#!/usr/bin/env python3
"""
feature_builders.non_partants_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Non-partant (scratched horse) features per race.

Computes field reduction metrics by comparing declared runners (statut != "partant")
against the final field size.

Temporal integrity: features are race-level (no horse history needed), computed
from the race's own data only.

Produces:
  - non_partants.jsonl  in output/non_partants/

Features per partant:
  - nb_non_partants       : number of horses declared non-partant in the race
  - field_reduction_pct   : nb_non_partants / (nombre_partants + nb_non_partants)
  - non_partant_impact    : 1 if > 20% of field is non-partant

Usage:
    python feature_builders/non_partants_builder.py
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "non_partants"

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


def build_non_partants_features(input_path: Path, logger) -> list[dict[str, Any]]:
    logger.info("=== Non Partants Builder ===")
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
            "statut": (rec.get("statut") or "").strip().lower(),
            "nb_partants": _safe_int(rec.get("nombre_partants")),
        })

    logger.info("Phase 1 terminee: %d records, %d courses en %.1fs",
                n_read, len(course_records), time.time() - t0)

    # Phase 2: compute per course, emit per partant
    t1 = time.time()
    results: list[dict[str, Any]] = []

    for course_uid, runners in course_records.items():
        # Count non-partants in this course group
        nb_np = sum(1 for r in runners if r["statut"] == "non-partant" or r["statut"] == "non_partant")

        # Get nombre_partants from actual partants
        nb_partants = None
        for r in runners:
            if r["nb_partants"] is not None:
                nb_partants = r["nb_partants"]
                break

        # If no explicit nombre_partants, count actual partants
        if nb_partants is None:
            nb_partants = sum(1 for r in runners if r["statut"] == "partant")

        total_declared = nb_partants + nb_np
        field_reduction = round(nb_np / total_declared, 4) if total_declared > 0 else None
        impact = int(field_reduction > 0.20) if field_reduction is not None else None

        for r in runners:
            results.append({
                "partant_uid": r["uid"],
                "nb_non_partants": nb_np,
                "field_reduction_pct": field_reduction,
                "non_partant_impact": impact,
            })

    elapsed = time.time() - t0
    logger.info(
        "Non partants build termine: %d features en %.1fs (courses: %d)",
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
        description="Construction des features non-partants a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("non_partants_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_non_partants_features(input_path, logger)

    out_path = output_dir / "non_partants.jsonl"
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
