#!/usr/bin/env python3
"""
feature_builders.gap_analysis_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Gap analysis features -- measuring gaps between horses in various
dimensions within the same race.

Reads partants_master.jsonl in streaming mode, groups by course,
and computes per-partant gap metrics relative to the rest of the field.

Temporal integrity: all features are derived from pre-race data
(cote, gains, nb_courses, age, poids_porte) -- no future leakage.

Produces:
  - gap_analysis.jsonl   in builder_outputs/gap_analysis/

Features per partant (8):
  - gap_odds_to_next          : cote_finale - next_higher_cote (gap to next less-favored)
  - gap_odds_from_prev        : previous_lower_cote - cote_finale (gap from next more-favored)
  - gap_gains_to_leader       : max(gains in field) - horse's gains
  - gap_experience_to_leader  : max(nb_courses) - horse's nb_courses
  - gap_age_to_youngest       : horse's age - min(age in field)
  - gap_weight_to_lightest    : horse's poids_porte - min(poids_porte in field)
  - gap_relative_odds_gap     : (cote - min_cote) / (max_cote - min_cote) normalised odds position
  - gap_competitive_cluster   : count of horses within +/-20% of this horse's cote

Usage:
    python feature_builders/gap_analysis_builder.py
    python feature_builders/gap_analysis_builder.py --input path/to/partants_master.jsonl
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/gap_analysis")

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


def build_gap_analysis_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build gap analysis features from partants_master.jsonl.

    Two-pass approach:
      Pass 1: Read sort keys + file byte offsets into a lightweight index.
              Sort chronologically.
      Pass 2: Per course, build sorted field-level lists (cotes, gains,
              nb_courses, ages, weights) then compute per-partant gaps,
              streaming output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Gap Analysis Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Pass 1: Build lightweight index (date, course, num, byte_offset) --
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Pass 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # Sort chronologically
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Pass 2: Process course by course, streaming output --
    t2 = time.time()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "gap_odds_to_next": 0,
        "gap_odds_from_prev": 0,
        "gap_gains_to_leader": 0,
        "gap_experience_to_leader": 0,
        "gap_age_to_youngest": 0,
        "gap_weight_to_lightest": 0,
        "gap_relative_odds_gap": 0,
        "gap_competitive_cluster": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            return {
                "uid": rec.get("partant_uid"),
                "cote": _safe_float(rec.get("cote_finale"))
                        or _safe_float(rec.get("cote_reference")),
                "gains": _safe_float(rec.get("gains_carriere"))
                         or _safe_float(rec.get("gainsParticipant_carriere")),
                "nb_courses": _safe_int(rec.get("nb_courses_carriere")),
                "age": _safe_int(rec.get("age")),
                "poids": _safe_float(rec.get("poids_porte"))
                         or _safe_float(rec.get("poidsConditionMonte")),
            }

        i = 0
        while i < total:
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_offsets: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_offsets.append(index[i][3])
                i += 1

            if not course_offsets:
                continue

            # Read slim records for this course
            course_group = [
                _extract_slim(_read_record_at(off))
                for off in course_offsets
            ]

            # -- Build sorted field-level lists --
            cotes = sorted(
                [r["cote"] for r in course_group
                 if r["cote"] is not None and r["cote"] > 0]
            )
            gains_list = [r["gains"] for r in course_group if r["gains"] is not None]
            nb_courses_list = [r["nb_courses"] for r in course_group if r["nb_courses"] is not None]
            ages_list = [r["age"] for r in course_group if r["age"] is not None]
            poids_list = [r["poids"] for r in course_group if r["poids"] is not None and r["poids"] > 0]

            # Pre-compute field-level aggregates
            max_gains = max(gains_list) if gains_list else None
            max_nb_courses = max(nb_courses_list) if nb_courses_list else None
            min_age = min(ages_list) if ages_list else None
            min_poids = min(poids_list) if poids_list else None
            min_cote = cotes[0] if cotes else None
            max_cote = cotes[-1] if cotes else None
            cote_range = (max_cote - min_cote) if (min_cote is not None and max_cote is not None) else None

            # -- Compute per-partant features --
            for rec in course_group:
                features: dict[str, Any] = {"partant_uid": rec["uid"]}
                cote = rec["cote"]
                gains = rec["gains"]
                nb_c = rec["nb_courses"]
                age = rec["age"]
                poids = rec["poids"]

                # 1. gap_odds_to_next: cote - next_higher_cote
                if cote is not None and cote > 0 and len(cotes) >= 2:
                    # Find next higher cote in sorted list
                    next_higher = None
                    for c in cotes:
                        if c > cote:
                            next_higher = c
                            break
                    if next_higher is not None:
                        features["gap_odds_to_next"] = round(next_higher - cote, 4)
                        fill_counts["gap_odds_to_next"] += 1
                    else:
                        features["gap_odds_to_next"] = None  # horse is highest cote
                else:
                    features["gap_odds_to_next"] = None

                # 2. gap_odds_from_prev: cote - previous_lower_cote
                if cote is not None and cote > 0 and len(cotes) >= 2:
                    prev_lower = None
                    for c in reversed(cotes):
                        if c < cote:
                            prev_lower = c
                            break
                    if prev_lower is not None:
                        features["gap_odds_from_prev"] = round(cote - prev_lower, 4)
                        fill_counts["gap_odds_from_prev"] += 1
                    else:
                        features["gap_odds_from_prev"] = None  # horse is lowest cote
                else:
                    features["gap_odds_from_prev"] = None

                # 3. gap_gains_to_leader
                if gains is not None and max_gains is not None:
                    features["gap_gains_to_leader"] = round(max_gains - gains, 4)
                    fill_counts["gap_gains_to_leader"] += 1
                else:
                    features["gap_gains_to_leader"] = None

                # 4. gap_experience_to_leader
                if nb_c is not None and max_nb_courses is not None:
                    features["gap_experience_to_leader"] = max_nb_courses - nb_c
                    fill_counts["gap_experience_to_leader"] += 1
                else:
                    features["gap_experience_to_leader"] = None

                # 5. gap_age_to_youngest
                if age is not None and min_age is not None:
                    features["gap_age_to_youngest"] = age - min_age
                    fill_counts["gap_age_to_youngest"] += 1
                else:
                    features["gap_age_to_youngest"] = None

                # 6. gap_weight_to_lightest
                if poids is not None and poids > 0 and min_poids is not None:
                    features["gap_weight_to_lightest"] = round(poids - min_poids, 2)
                    fill_counts["gap_weight_to_lightest"] += 1
                else:
                    features["gap_weight_to_lightest"] = None

                # 7. gap_relative_odds_gap: (cote - min_cote) / (max_cote - min_cote)
                if (cote is not None and cote > 0
                        and cote_range is not None and cote_range > 0):
                    features["gap_relative_odds_gap"] = round(
                        (cote - min_cote) / cote_range, 4
                    )
                    fill_counts["gap_relative_odds_gap"] += 1
                else:
                    features["gap_relative_odds_gap"] = None

                # 8. gap_competitive_cluster: horses within +/-20% of this cote
                if cote is not None and cote > 0 and cotes:
                    lo = cote * 0.8
                    hi = cote * 1.2
                    cluster_count = sum(1 for c in cotes if lo <= c <= hi)
                    features["gap_competitive_cluster"] = cluster_count
                    fill_counts["gap_competitive_cluster"] += 1
                else:
                    features["gap_competitive_cluster"] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Gap analysis build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)",
            k, v, n_written, 100 * v / n_written if n_written else 0,
        )

    return n_written


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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features gap analysis a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/gap_analysis/)",
    )
    args = parser.parse_args()

    logger = setup_logging("gap_analysis_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "gap_analysis.jsonl"
    build_gap_analysis_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
