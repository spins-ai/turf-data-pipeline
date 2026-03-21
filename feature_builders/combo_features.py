#!/usr/bin/env python3
"""
feature_builders.combo_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
13 features from jockey-trainer-horse combinations and entity+context pairs.

Temporal integrity: for any partant at date D, only races with date < D
are used (no future leakage).

Usage:
    python feature_builders/combo_features.py
    python feature_builders/combo_features.py --input output/02_liste_courses/partants_normalises.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "combo_features")

# ===========================================================================
# HELPERS
# ===========================================================================

def _safe_rate(count: int, total: int) -> Optional[float]:
    if total == 0:
        return None
    return round(count / total, 4)


def _distance_category(dist) -> Optional[str]:
    if dist is None:
        return None
    try:
        dist = int(dist)
    except (ValueError, TypeError):
        return None
    if dist < 1400:
        return "sprint"
    elif dist < 1800:
        return "mile"
    elif dist < 2400:
        return "intermediate"
    else:
        return "staying"

# ===========================================================================
# LOAD
# ===========================================================================

# ===========================================================================
# BUILDER
# ===========================================================================

def build_combo_features(partants: list, logger: logging.Logger = None) -> list:
    """Build 13 jockey-trainer-horse combination features."""
    if logger is None:
        logger = logging.getLogger(__name__)

    # Sort chronologically for temporal integrity
    sorted_p = sorted(
        partants,
        key=lambda p: (
            str(p.get("date_reunion_iso", "") or ""),
            str(p.get("course_uid", "") or ""),
            p.get("num_pmu", 0) or 0,
        ),
    )

    # Accumulate histories for each combo key
    jt_history: dict[str, list[dict]] = defaultdict(list)
    jh_history: dict[str, list[dict]] = defaultdict(list)
    th_history: dict[str, list[dict]] = defaultdict(list)
    j_hippo_history: dict[str, list[dict]] = defaultdict(list)
    t_hippo_history: dict[str, list[dict]] = defaultdict(list)
    j_dist_history: dict[str, list[dict]] = defaultdict(list)
    t_dist_history: dict[str, list[dict]] = defaultdict(list)

    # Track last jockey per horse
    horse_last_jockey: dict[str, str] = {}

    enriched = 0
    results = []

    for idx, p in enumerate(sorted_p):
        cheval = (p.get("nom_cheval") or "").upper().strip()
        jockey = (p.get("jockey_driver") or "").upper().strip()
        trainer = (p.get("entraineur") or "").upper().strip()
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        hippo = (p.get("hippodrome_normalise") or "").upper().strip()
        dist = p.get("distance")
        dist_cat = _distance_category(dist)

        is_gagnant = bool(p.get("is_gagnant"))
        is_place = bool(p.get("is_place"))

        # Build combo keys
        jt_key = f"{jockey}||{trainer}" if jockey and trainer else None
        jh_key = f"{jockey}||{cheval}" if jockey and cheval else None
        th_key = f"{trainer}||{cheval}" if trainer and cheval else None
        j_hippo_key = f"{jockey}||{hippo}" if jockey and hippo else None
        t_hippo_key = f"{trainer}||{hippo}" if trainer and hippo else None
        j_dist_key = f"{jockey}||{dist_cat}" if jockey and dist_cat else None
        t_dist_key = f"{trainer}||{dist_cat}" if trainer and dist_cat else None

        # Retrieve PAST records (strictly < current date)
        def _past(history, key):
            if key is None:
                return []
            return [r for r in history.get(key, []) if r["date"] < date_iso]

        jt_past = _past(jt_history, jt_key)
        jh_past = _past(jh_history, jh_key)
        th_past = _past(th_history, th_key)
        j_hippo_past = _past(j_hippo_history, j_hippo_key)
        t_hippo_past = _past(t_hippo_history, t_hippo_key)
        j_dist_past = _past(j_dist_history, j_dist_key)
        t_dist_past = _past(t_dist_history, t_dist_key)

        # Compute features
        jt_nb = len(jt_past)
        jt_wins = sum(1 for r in jt_past if r["gagnant"])
        jt_places = sum(1 for r in jt_past if r["place"])

        jh_nb = len(jh_past)
        jh_wins = sum(1 for r in jh_past if r["gagnant"])

        th_nb = len(th_past)
        th_wins = sum(1 for r in th_past if r["gagnant"])

        j_hippo_wins = sum(1 for r in j_hippo_past if r["gagnant"])
        t_hippo_wins = sum(1 for r in t_hippo_past if r["gagnant"])
        j_dist_wins = sum(1 for r in j_dist_past if r["gagnant"])
        t_dist_wins = sum(1 for r in t_dist_past if r["gagnant"])

        # Jockey change
        last_jockey = horse_last_jockey.get(cheval)
        jockey_change = None
        if last_jockey is not None and jockey:
            jockey_change = 1 if last_jockey != jockey else 0

        has_any_past = jt_nb > 0 or jh_nb > 0 or th_nb > 0
        if has_any_past:
            enriched += 1

        feat = {
            "jockey_trainer_nb_courses": jt_nb if jt_key else None,
            "jockey_trainer_taux_victoire": _safe_rate(jt_wins, jt_nb),
            "jockey_trainer_taux_place": _safe_rate(jt_places, jt_nb),
            "jockey_cheval_nb_courses": jh_nb if jh_key else None,
            "jockey_cheval_taux_victoire": _safe_rate(jh_wins, jh_nb),
            "trainer_cheval_taux_victoire": _safe_rate(th_wins, th_nb),
            "jockey_hippo_taux_victoire": _safe_rate(j_hippo_wins, len(j_hippo_past)),
            "trainer_hippo_taux_victoire": _safe_rate(t_hippo_wins, len(t_hippo_past)),
            "jockey_distance_taux_victoire": _safe_rate(j_dist_wins, len(j_dist_past)),
            "trainer_distance_taux_victoire": _safe_rate(t_dist_wins, len(t_dist_past)),
            "is_new_jockey": 1 if (jh_key and jh_nb == 0) else (0 if jh_key else None),
            "is_new_trainer": 1 if (th_key and th_nb == 0) else (0 if th_key else None),
            "jockey_change": jockey_change,
        }

        p.update(feat)
        results.append(p)

        # Append current race to histories
        record = {"date": date_iso, "gagnant": is_gagnant, "place": is_place}

        if jt_key:
            jt_history[jt_key].append(record)
        if jh_key:
            jh_history[jh_key].append(record)
        if th_key:
            th_history[th_key].append(record)
        if j_hippo_key:
            j_hippo_history[j_hippo_key].append(record)
        if t_hippo_key:
            t_hippo_history[t_hippo_key].append(record)
        if j_dist_key:
            j_dist_history[j_dist_key].append(record)
        if t_dist_key:
            t_dist_history[t_dist_key].append(record)

        if cheval and jockey:
            horse_last_jockey[cheval] = jockey

        if (idx + 1) % 100000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(sorted_p), enriched)

    logger.info("Features combo: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="13 jockey-trainer-horse combo features")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("combo_features")
    logger.info("=" * 70)
    logger.info("combo_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_combo_features(partants, logger)

    out_path = os.path.join(args.output_dir, "combo_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
