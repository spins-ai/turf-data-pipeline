#!/usr/bin/env python3
"""
feature_builders.career_milestone_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Career milestone features -- tracking key career milestones and achievements.

Single-pass streaming: features are derived from career statistics already
present in the current record (nb_victoires_carriere, nb_courses_carriere,
nb_places_carriere, gains_carriere). No temporal tracking needed.

Produces:
  - career_milestone.jsonl  in builder_outputs/career_milestone/

Features per partant (8):
  - cms_is_maiden              : 1 if nb_victoires_carriere == 0 (never won)
  - cms_is_first_10_starts     : 1 if nb_courses_carriere <= 10
  - cms_win_rate_career        : nb_victoires / max(nb_courses, 1)
  - cms_place_rate_career      : nb_places_carriere / max(nb_courses, 1)
  - cms_wins_to_starts_ratio_log : log(nb_victoires + 1) / log(nb_courses + 2)
  - cms_is_graded_performer    : 1 if gains_carriere > 200000
  - cms_earnings_per_win       : gains_carriere / max(nb_victoires, 1)
  - cms_career_profit_index    : (gains - nb_courses * 500) / max(nb_courses * 500, 1)

Usage:
    python feature_builders/career_milestone_builder.py
    python feature_builders/career_milestone_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/career_milestone")

_LOG_EVERY = 500_000

_FEATURE_KEYS = [
    "cms_is_maiden",
    "cms_is_first_10_starts",
    "cms_win_rate_career",
    "cms_place_rate_career",
    "cms_wins_to_starts_ratio_log",
    "cms_is_graded_performer",
    "cms_earnings_per_win",
    "cms_career_profit_index",
]


# ===========================================================================
# HELPERS
# ===========================================================================


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
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _compute_features(
    nb_victoires: Optional[int],
    nb_courses: Optional[int],
    nb_places: Optional[int],
    gains_carriere: Optional[float],
) -> dict[str, Any]:
    """Compute the 8 career milestone features from record-level career stats."""
    feats: dict[str, Any] = {k: None for k in _FEATURE_KEYS}

    # 1. cms_is_maiden: 1 if nb_victoires_carriere == 0
    if nb_victoires is not None:
        feats["cms_is_maiden"] = 1 if nb_victoires == 0 else 0

    # 2. cms_is_first_10_starts: 1 if nb_courses_carriere <= 10
    if nb_courses is not None:
        feats["cms_is_first_10_starts"] = 1 if nb_courses <= 10 else 0

    # 3. cms_win_rate_career: nb_victoires / max(nb_courses, 1)
    if nb_victoires is not None and nb_courses is not None:
        feats["cms_win_rate_career"] = round(
            nb_victoires / max(nb_courses, 1), 4
        )

    # 4. cms_place_rate_career: nb_places / max(nb_courses, 1)
    if nb_places is not None and nb_courses is not None:
        feats["cms_place_rate_career"] = round(
            nb_places / max(nb_courses, 1), 4
        )

    # 5. cms_wins_to_starts_ratio_log: log(nb_victoires + 1) / log(nb_courses + 2)
    if nb_victoires is not None and nb_courses is not None:
        feats["cms_wins_to_starts_ratio_log"] = round(
            math.log(nb_victoires + 1) / math.log(nb_courses + 2), 6
        )

    # 6. cms_is_graded_performer: 1 if gains_carriere > 200000
    if gains_carriere is not None:
        feats["cms_is_graded_performer"] = 1 if gains_carriere > 200_000 else 0

    # 7. cms_earnings_per_win: gains_carriere / max(nb_victoires, 1)
    if gains_carriere is not None and nb_victoires is not None:
        feats["cms_earnings_per_win"] = round(
            gains_carriere / max(nb_victoires, 1), 2
        )

    # 8. cms_career_profit_index: (gains - nb_courses * 500) / max(nb_courses * 500, 1)
    if gains_carriere is not None and nb_courses is not None:
        cost = nb_courses * 500
        feats["cms_career_profit_index"] = round(
            (gains_carriere - cost) / max(cost, 1), 4
        )

    return feats


# ===========================================================================
# MAIN BUILD (single-pass streaming)
# ===========================================================================


def build_career_milestone_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build career milestone features from partants_master.jsonl.

    Single-pass streaming: reads each record, extracts career stats,
    computes features, and writes directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Career Milestone Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    fill_counts = {k: 0 for k in _FEATURE_KEYS}
    n_read = 0
    n_written = 0
    n_errors = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Lu %d records...", n_read)
                gc.collect()

            # Extract career stats from the record
            nb_victoires = _safe_int(
                rec.get("nb_victoires_carriere") or rec.get("nbVictoiresCarriere")
            )
            nb_courses = _safe_int(
                rec.get("nb_courses_carriere") or rec.get("nbCoursesCarriere")
            )
            nb_places = _safe_int(
                rec.get("nb_places_carriere") or rec.get("nbPlacesCarriere")
            )
            gains_carriere = _safe_float(
                rec.get("gains_carriere") or rec.get("gainsCarriere")
            )

            # Compute features
            feats = _compute_features(nb_victoires, nb_courses, nb_places, gains_carriere)

            # Build output record
            out_rec: dict[str, Any] = {
                "partant_uid": rec.get("partant_uid"),
            }
            for k in _FEATURE_KEYS:
                v = feats[k]
                out_rec[k] = v
                if v is not None:
                    fill_counts[k] += 1

            fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info("Lecture terminee: %d records, %d erreurs JSON", n_read, n_errors)
    logger.info(
        "Career milestone build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features career milestone a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/career_milestone/)",
    )
    args = parser.parse_args()

    logger = setup_logging("career_milestone_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "career_milestone.jsonl"
    build_career_milestone_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
