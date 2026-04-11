#!/usr/bin/env python3
"""
feature_builders.race_predictability_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race predictability features -- measuring how predictable the race outcome
is likely to be based on pre-race odds distribution and field composition.

Index + chronological sort + seek architecture:
  Phase 1 - Build lightweight index (sort keys + byte offsets) from JSONL,
            plus per-course aggregation of cotes, win rates, gains.
  Phase 2 - Sort the index chronologically, then compute per-partant features
            streaming to disk.

Temporal integrity: all features are derived from pre-race data (odds,
field composition, career stats known before the race). No future leakage.

Produces:
  - race_predictability.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_predictability/

Features per partant (8):
  - rpr_odds_gini              : Gini coefficient of implied probabilities
  - rpr_top_horse_gap          : probability gap between 1st and 2nd favorite
  - rpr_nb_realistic_contenders: horses with implied prob > (1/nombre_partants)
  - rpr_field_form_homogeneity : 1 - cv(win_rates) across field
  - rpr_is_open_race           : 1 if no horse has >25% implied probability
  - rpr_is_one_horse_race      : 1 if one horse has >40% implied probability
  - rpr_field_class_cv         : coefficient of variation of gains_carriere
  - rpr_predictability_composite: weighted combo (gini, top_gap, contenders)

Usage:
    python feature_builders/race_predictability_builder.py
    python feature_builders/race_predictability_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
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
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_predictability"
)

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# Feature names
_FEATURE_NAMES = [
    "rpr_odds_gini",
    "rpr_top_horse_gap",
    "rpr_nb_realistic_contenders",
    "rpr_field_form_homogeneity",
    "rpr_is_open_race",
    "rpr_is_one_horse_race",
    "rpr_field_class_cv",
    "rpr_predictability_composite",
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    try:
        f = float(v)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _gini(values: list[float]) -> float:
    """Compute Gini coefficient for a list of non-negative values.

    Returns 0 (perfect equality) to 1 (maximum inequality).
    """
    n = len(values)
    if n < 2:
        return 0.0
    sorted_vals = sorted(values)
    total = sum(sorted_vals)
    if total <= 0:
        return 0.0
    cumulative = 0.0
    gini_sum = 0.0
    for i, v in enumerate(sorted_vals):
        cumulative += v
        gini_sum += cumulative - v / 2.0
    gini_sum = 2.0 * gini_sum / (n * total) - 1.0
    return max(0.0, min(1.0, gini_sum))


def _coefficient_of_variation(values: list[float]) -> Optional[float]:
    """Compute coefficient of variation (std / mean). Returns None if not computable."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    if mean <= 0:
        return None
    variance = sum((v - mean) ** 2 for v in values) / n
    std = variance ** 0.5
    return std / mean


# ===========================================================================
# MAIN BUILD (two-pass: index + seek)
# ===========================================================================


def build_race_predictability_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build race predictability features using index + seek architecture.

    Returns the total number of feature records written.
    """
    logger.info("=== Race Predictability Builder (index + seek) ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Build lightweight index + per-course aggregation
    # ------------------------------------------------------------------
    # Index: (date, course_uid, num_pmu, byte_offset)
    index: list[tuple[str, str, int, int]] = []
    # Per-course aggregation: course_key -> {cotes, wins, gains, nb_courses}
    course_data: dict[str, dict[str, list]] = {}
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line_stripped = line.strip()
            if not line_stripped:
                continue
            try:
                rec = json.loads(line_stripped)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Phase 1 - Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

            # Aggregate per course
            course_key = f"{date_str}|{course_uid}"
            if course_key not in course_data:
                course_data[course_key] = {
                    "cotes": [],
                    "win_rates": [],
                    "gains": [],
                    "nb_courses": [],
                }

            cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference"))
            if cote is not None and cote > 0:
                course_data[course_key]["cotes"].append(cote)

            # Win rate proxy: victoires / nb_courses_carriere
            victoires = _safe_int(rec.get("nb_victoires_carriere"))
            nb_courses = _safe_int(rec.get("nb_courses_carriere"))
            if victoires is not None and nb_courses is not None and nb_courses > 0:
                course_data[course_key]["win_rates"].append(victoires / nb_courses)

            gains = _safe_float(rec.get("gains_carriere") or rec.get("gains_totaux"))
            if gains is not None:
                course_data[course_key]["gains"].append(gains)

            if nb_courses is not None:
                course_data[course_key]["nb_courses"].append(nb_courses)

    logger.info(
        "Phase 1 terminee: %d records indexes, %d courses en %.1fs",
        len(index), len(course_data), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 1b: Compute per-course features from aggregation
    # ------------------------------------------------------------------
    t1 = time.time()
    course_features: dict[str, dict[str, Any]] = {}

    for course_key, data in course_data.items():
        cotes = data["cotes"]
        win_rates = data["win_rates"]
        gains = data["gains"]

        nb_partants = len(cotes) if cotes else 0

        # Implied probabilities from odds: prob_i = (1/cote_i) / sum(1/cote_j)
        implied_probs: list[float] = []
        if cotes:
            raw_probs = [1.0 / c for c in cotes]
            total_prob = sum(raw_probs)
            if total_prob > 0:
                implied_probs = [p / total_prob for p in raw_probs]

        # 1. rpr_odds_gini: Gini coefficient of implied probabilities
        odds_gini: Optional[float] = None
        if len(implied_probs) >= 2:
            odds_gini = round(_gini(implied_probs), 4)

        # 2. rpr_top_horse_gap: gap between 1st and 2nd favorite
        top_horse_gap: Optional[float] = None
        if len(implied_probs) >= 2:
            sorted_probs = sorted(implied_probs, reverse=True)
            top_horse_gap = round(sorted_probs[0] - sorted_probs[1], 4)

        # 3. rpr_nb_realistic_contenders: horses with implied prob > fair share
        nb_realistic: Optional[int] = None
        if implied_probs and nb_partants > 0:
            fair_share = 1.0 / nb_partants
            nb_realistic = sum(1 for p in implied_probs if p > fair_share)

        # 4. rpr_field_form_homogeneity: 1 - cv(win_rates)
        form_homogeneity: Optional[float] = None
        if len(win_rates) >= 2:
            cv = _coefficient_of_variation(win_rates)
            if cv is not None:
                form_homogeneity = round(max(0.0, 1.0 - cv), 4)

        # 5. rpr_is_open_race: no horse has >25% implied probability
        is_open: Optional[int] = None
        if implied_probs:
            is_open = 1 if max(implied_probs) <= 0.25 else 0

        # 6. rpr_is_one_horse_race: one horse has >40% implied probability
        is_one_horse: Optional[int] = None
        if implied_probs:
            is_one_horse = 1 if max(implied_probs) > 0.40 else 0

        # 7. rpr_field_class_cv: coefficient of variation of gains_carriere
        field_class_cv: Optional[float] = None
        if len(gains) >= 2:
            cv_gains = _coefficient_of_variation(gains)
            if cv_gains is not None:
                field_class_cv = round(cv_gains, 4)

        # 8. rpr_predictability_composite: weighted combo
        predictability_composite: Optional[float] = None
        if odds_gini is not None and top_horse_gap is not None and nb_realistic is not None and nb_partants > 0:
            # Normalize nb_realistic: fewer contenders = more predictable
            contender_score = 1.0 - (nb_realistic / nb_partants)
            predictability_composite = round(
                0.40 * odds_gini + 0.35 * top_horse_gap + 0.25 * contender_score,
                4,
            )

        course_features[course_key] = {
            "rpr_odds_gini": odds_gini,
            "rpr_top_horse_gap": top_horse_gap,
            "rpr_nb_realistic_contenders": nb_realistic,
            "rpr_field_form_homogeneity": form_homogeneity,
            "rpr_is_open_race": is_open,
            "rpr_is_one_horse_race": is_one_horse,
            "rpr_field_class_cv": field_class_cv,
            "rpr_predictability_composite": predictability_composite,
        }

    # Free aggregation data
    del course_data
    gc.collect()
    logger.info("Phase 1b terminee: features calculees pour %d courses en %.1fs",
                len(course_features), time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 2: Sort the lightweight index chronologically
    # ------------------------------------------------------------------
    t2 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Phase 2 - Tri chronologique en %.1fs", time.time() - t2)

    # ------------------------------------------------------------------
    # Phase 3: Stream output per partant using sorted index + course features
    # ------------------------------------------------------------------
    t3 = time.time()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {k: 0 for k in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for date_str, course_uid, num_pmu, offset in index:
            # Read the record from disk
            fin.seek(offset)
            line = fin.readline()
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            uid = rec.get("partant_uid")
            course_key = f"{date_str}|{course_uid}"

            feats = course_features.get(course_key, {})

            row = {
                "partant_uid": uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }
            for k in _FEATURE_NAMES:
                row[k] = feats.get(k)

            fout.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            n_written += 1

            for k in _FEATURE_NAMES:
                if row.get(k) is not None:
                    fill_counts[k] += 1

            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Phase 3 - Ecrit %d / %d records...", n_processed, total)

            if n_processed % _GC_EVERY == 0:
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Race predictability build termine: %d features en %.1fs",
        n_written, elapsed,
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features race predictability a partir de partants_master"
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
        help="Repertoire de sortie (defaut: builder_outputs/race_predictability/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_predictability_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "race_predictability.jsonl"
    build_race_predictability_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
