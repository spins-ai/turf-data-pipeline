#!/usr/bin/env python3
"""
feature_builders.class_change_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
11 features from class transitions and context changes (allocation, distance,
discipline, hippodrome, surface).

Temporal integrity: for any partant at date D, only races with date < D are used.

Usage:
    python feature_builders/class_change_features.py
    python feature_builders/class_change_features.py --input output/02_liste_courses/partants_normalises.jsonl
    python feature_builders/class_change_features.py --courses output/02_liste_courses/courses_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Optional

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
COURSES_DEFAULT = os.path.join("output", "02_liste_courses", "courses_master.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "class_change_features")
LOG_DIR = os.path.join("logs")

# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("class_change_features")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    os.makedirs(LOG_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(LOG_DIR, "class_change_features.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

# ===========================================================================
# HELPERS
# ===========================================================================

def _safe_percentile_rank(value: float, history_values: list[float]) -> Optional[float]:
    if not history_values:
        return None
    below = sum(1 for v in history_values if v < value)
    equal = sum(1 for v in history_values if v == value)
    return round((below + 0.5 * equal) / len(history_values), 4)

# ===========================================================================
# LOAD
# ===========================================================================

def load_jsonl(path: str, logger: logging.Logger) -> list:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    logger.info("Charge %d enregistrements depuis %s", len(records), path)
    return records


def load_json_or_jsonl(path: str, logger: logging.Logger) -> list:
    if path.endswith(".jsonl"):
        return load_jsonl(path, logger)
    jsonl_path = path.replace(".json", ".jsonl")
    if os.path.exists(jsonl_path):
        return load_jsonl(jsonl_path, logger)
    if os.path.exists(path):
        logger.info("Chargement JSON: %s", path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("  %d entrees chargees", len(data))
        return data
    logger.warning("Fichier introuvable: %s", path)
    return []

# ===========================================================================
# BUILDER
# ===========================================================================

def build_class_change_features(partants: list, courses: list, logger: logging.Logger = None) -> list:
    """Build 11 class-change and context-transition features."""
    if logger is None:
        logger = logging.getLogger(__name__)

    # Build course lookup for allocation and surface
    course_lookup: dict[str, dict] = {}
    for c in courses:
        cuid = c.get("course_uid")
        if cuid:
            course_lookup[cuid] = c
    logger.info("Course lookup: %d courses", len(course_lookup))

    # Sort chronologically
    sorted_p = sorted(
        partants,
        key=lambda p: (
            str(p.get("date_reunion_iso", "") or ""),
            str(p.get("course_uid", "") or ""),
            p.get("num_pmu", 0) or 0,
        ),
    )

    # Accumulate per-horse history
    horse_history: dict[str, list[dict]] = defaultdict(list)
    enriched = 0
    results = []

    for idx, p in enumerate(sorted_p):
        cheval = (p.get("nom_cheval") or "").upper().strip()
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        course_uid = p.get("course_uid", "")
        distance = p.get("distance")
        discipline = (p.get("discipline") or "").upper().strip()
        hippo = (p.get("hippodrome_normalise") or "").upper().strip()

        # Get course-level info
        course_info = course_lookup.get(course_uid, {})
        allocation = course_info.get("allocation_totale")
        surface = (course_info.get("type_piste") or "").upper().strip()

        # Get PAST races for this horse (strictly < current date)
        past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso] if cheval else []

        feat = {}

        # Allocation-based features
        allocation_diff = None
        allocation_ratio = None
        allocation_rank = None
        is_class_up = None
        is_class_down = None

        if past and allocation is not None:
            enriched += 1
            last = past[-1]
            last_alloc = last.get("allocation")

            if last_alloc is not None and last_alloc > 0:
                allocation_diff = round(allocation - last_alloc, 2)
                allocation_ratio = round(allocation / last_alloc, 4)
                is_class_up = 1 if allocation > last_alloc else 0
                is_class_down = 1 if allocation < last_alloc else 0

            past_allocs = [r["allocation"] for r in past if r["allocation"] is not None]
            if past_allocs:
                allocation_rank = _safe_percentile_rank(allocation, past_allocs)

        feat["allocation_diff_vs_last"] = allocation_diff
        feat["allocation_ratio_vs_last"] = allocation_ratio
        feat["allocation_rank_career"] = allocation_rank
        feat["is_class_up"] = is_class_up
        feat["is_class_down"] = is_class_down

        # Distance-based features
        distance_diff = None
        distance_diff_abs = None
        if past and distance is not None:
            last_dist = past[-1].get("distance")
            if last_dist is not None:
                distance_diff = distance - last_dist
                distance_diff_abs = abs(distance_diff)

        feat["distance_diff_vs_last"] = distance_diff
        feat["distance_diff_abs"] = distance_diff_abs

        # Discipline change
        discipline_change = None
        if past and discipline:
            last_disc = past[-1].get("discipline", "")
            if last_disc:
                discipline_change = 1 if discipline != last_disc else 0
        feat["discipline_change"] = discipline_change

        # Hippodrome change
        hippo_change = None
        if past and hippo:
            last_hippo = past[-1].get("hippo", "")
            if last_hippo:
                hippo_change = 1 if hippo != last_hippo else 0
        feat["hippo_change"] = hippo_change

        # Surface change
        surface_change = None
        if past and surface:
            last_surface = past[-1].get("surface", "")
            if last_surface:
                surface_change = 1 if surface != last_surface else 0
        feat["surface_change"] = surface_change

        # Count class changes in last 5 races
        nb_class_changes_5 = None
        if len(past) >= 2:
            recent = past[-5:]
            changes = 0
            for i in range(1, len(recent)):
                a_prev = recent[i - 1].get("allocation")
                a_curr = recent[i].get("allocation")
                if a_prev is not None and a_curr is not None and a_prev != a_curr:
                    changes += 1
            nb_class_changes_5 = changes
        feat["nb_class_changes_5"] = nb_class_changes_5

        p.update(feat)
        results.append(p)

        # Append current race to history
        if cheval:
            horse_history[cheval].append({
                "date": date_iso,
                "allocation": allocation,
                "distance": distance,
                "discipline": discipline,
                "hippo": hippo,
                "surface": surface,
            })

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(sorted_p), enriched)

    logger.info("Features class_change: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="11 class change features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--courses", default=COURSES_DEFAULT, help="Courses JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("class_change_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    courses = load_json_or_jsonl(args.courses, logger)
    results = build_class_change_features(partants, courses, logger)

    out_path = os.path.join(args.output_dir, "class_change_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
