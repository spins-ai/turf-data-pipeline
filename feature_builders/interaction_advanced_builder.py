#!/usr/bin/env python3
"""
feature_builders.interaction_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
6 advanced multiplicative interaction features between existing features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant interaction features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to entity-level statistics -- no future leakage.
The interaction terms themselves are computed from pre-race snapshots only.

Produces:
  - interaction_advanced.jsonl   in output/interaction_advanced/

Features per partant:
  - elo_x_cote                : elo_cheval * (1 / cote_finale)
  - forme_x_distance_pref     : momentum_3 * dist_pref_advantage
  - jockey_x_hippo_specialist : jockey_hippo_win_rate * horse_hippo_win_rate
  - age_x_distance            : age * distance_category_encoded
  - fatigue_x_repos           : fatigue_30j * jours_repos
  - field_size_x_draw         : nombre_partants * draw_position_normalized

Usage:
    python feature_builders/interaction_advanced_builder.py
    python feature_builders/interaction_advanced_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "interaction_advanced"

# Minimum observations to emit a win rate
_MIN_OBS = 3

# Progress log every N records
_LOG_EVERY = 500_000

# Distance category encoding (ordered by typical distance)
_DIST_CAT_MAP = {
    "sprint": 1,
    "mile": 2,
    "intermediate": 3,
    "staying": 4,
    "long": 5,
}

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


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    """Convert value to float or return None."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _safe_mul(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Multiply two optional floats; None if either is missing."""
    if a is None or b is None:
        return None
    result = a * b
    return round(result, 6) if math.isfinite(result) else None


def _encode_distance_category(cat: Optional[str]) -> Optional[float]:
    """Encode distance category as ordinal."""
    if not cat:
        return None
    return float(_DIST_CAT_MAP.get(str(cat).lower().strip(), 3))


def _classify_distance(distance_m) -> Optional[str]:
    """Classify distance in metres into a category."""
    d = _safe_float(distance_m)
    if d is None:
        return None
    if d < 1200:
        return "sprint"
    if d < 1600:
        return "mile"
    if d < 2100:
        return "intermediate"
    if d < 2800:
        return "staying"
    return "long"


def _win_rate(wins: int, total: int) -> Optional[float]:
    """Compute win rate with minimum observation threshold."""
    if total < _MIN_OBS:
        return None
    return round(wins / total, 4)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_interaction_advanced_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build advanced interaction features from partants_master.jsonl.

    Two-phase approach:
      1. Read minimal fields into memory and sort chronologically.
      2. Process course-by-course, computing interaction features from
         pre-race snapshots of entity-level statistics.

    Entity-level statistics tracked (for jockey/horse hippo specialisation):
      - jockey wins/runs per hippodrome
      - horse wins/runs per hippodrome
    """
    logger.info("=== Interaction Advanced Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Extract fields we need
        distance = _safe_float(rec.get("distance"))
        dist_cat_raw = rec.get("distance_category") or _classify_distance(distance)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            # Entity identifiers
            "cheval": rec.get("nom_cheval"),
            "jockey": rec.get("jockey_driver"),
            "hippo": rec.get("hippodrome"),
            # Result (for post-race stat updates)
            "gagnant": bool(rec.get("is_gagnant")),
            # Pre-existing features (may come from enriched file)
            "elo_cheval": _safe_float(rec.get("elo_cheval")),
            "cote_finale": _safe_float(rec.get("cote_finale") or rec.get("cote_probable")),
            "momentum_3": _safe_float(rec.get("momentum_3")),
            "dist_pref_advantage": _safe_float(rec.get("dist_pref_advantage")),
            "age": _safe_float(rec.get("age")),
            "distance_category": dist_cat_raw,
            "fatigue_30j": _safe_float(rec.get("fatigue_30j") or rec.get("nb_courses_30j")),
            "jours_repos": _safe_float(rec.get("jours_repos") or rec.get("repos_jours")),
            "nombre_partants": _safe_float(rec.get("nombre_partants") or rec.get("nb_partants")),
            "num_place": _safe_float(rec.get("num_pmu")),
            "nb_partants_course": _safe_float(rec.get("nombre_partants") or rec.get("nb_partants")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()

    # Entity-level trackers for hippo specialisation
    # key: (jockey, hippo) -> [wins, total]
    jockey_hippo: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
    # key: (horse, hippo) -> [wins, total]
    horse_hippo: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])

    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while i < total and slim_records[i]["course"] == course_uid and slim_records[i]["date"] == course_date:
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        n_runners = len(course_group)

        # -- Compute features for each partant (pre-race snapshot) --
        for rec in course_group:
            uid = rec["uid"]
            cheval = rec["cheval"]
            jockey = rec["jockey"]
            hippo = rec["hippo"]

            # 1. elo_x_cote: elo_cheval * (1 / cote_finale)
            inv_cote = None
            if rec["cote_finale"] is not None and rec["cote_finale"] > 0:
                inv_cote = 1.0 / rec["cote_finale"]
            elo_x_cote = _safe_mul(rec["elo_cheval"], inv_cote)

            # 2. forme_x_distance_pref: momentum_3 * dist_pref_advantage
            forme_x_distance_pref = _safe_mul(rec["momentum_3"], rec["dist_pref_advantage"])

            # 3. jockey_x_hippo_specialist: jockey_hippo_win_rate * horse_hippo_win_rate
            j_hippo_wr = None
            h_hippo_wr = None
            if jockey and hippo:
                stats = jockey_hippo[(jockey, hippo)]
                j_hippo_wr = _win_rate(stats[0], stats[1])
            if cheval and hippo:
                stats = horse_hippo[(cheval, hippo)]
                h_hippo_wr = _win_rate(stats[0], stats[1])
            jockey_x_hippo = _safe_mul(j_hippo_wr, h_hippo_wr)

            # 4. age_x_distance: age * distance_category_encoded
            dist_cat_enc = _encode_distance_category(rec["distance_category"])
            age_x_distance = _safe_mul(rec["age"], dist_cat_enc)

            # 5. fatigue_x_repos: fatigue_30j * jours_repos
            fatigue_x_repos = _safe_mul(rec["fatigue_30j"], rec["jours_repos"])

            # 6. field_size_x_draw: nombre_partants * draw_position_normalized
            draw_norm = None
            if rec["num_place"] is not None and rec["nb_partants_course"] is not None and rec["nb_partants_course"] > 0:
                draw_norm = rec["num_place"] / rec["nb_partants_course"]
            field_size_x_draw = _safe_mul(rec["nb_partants_course"], draw_norm)

            results.append({
                "partant_uid": uid,
                "elo_x_cote": elo_x_cote,
                "forme_x_distance_pref": forme_x_distance_pref,
                "jockey_x_hippo_specialist": jockey_x_hippo,
                "age_x_distance": age_x_distance,
                "fatigue_x_repos": fatigue_x_repos,
                "field_size_x_draw": field_size_x_draw,
            })

        # -- Post-race: update entity stats --
        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]
            hippo = rec["hippo"]
            won = rec["gagnant"]

            if jockey and hippo:
                jockey_hippo[(jockey, hippo)][1] += 1
                if won:
                    jockey_hippo[(jockey, hippo)][0] += 1

            if cheval and hippo:
                horse_hippo[(cheval, hippo)][1] += 1
                if won:
                    horse_hippo[(cheval, hippo)][0] += 1

        n_processed += n_runners
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Interaction advanced build termine: %d features en %.1fs",
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
        description="Construction des features d'interaction avancees a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/interaction_advanced/)",
    )
    args = parser.parse_args()

    logger = setup_logging("interaction_advanced_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_interaction_advanced_features(input_path, logger)

    # Save
    out_path = output_dir / "interaction_advanced.jsonl"
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
