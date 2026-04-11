#!/usr/bin/env python3
"""
feature_builders.anomaly_detection_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Anomaly / outlier detection features for outsider-detection modules
(anomalie_detector, retour_forme_hidden, GAN-turf, ZURI outsider engine).

Two-pass architecture:
  Pass 1 - collect all records into memory (slim), group by course_uid,
           compute per-field distributions (mean/std of cote, age,
           nb_courses, gains, speed, poids).
  Pass 2 - iterate partants, compute z-scores and outlier flags using
           field-level stats from pass 1.

Temporal integrity: all features are field-relative (within the same race),
so no future leakage.

Produces:
  - anomaly_detection_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/anomaly_detection/

Features per partant (12):
  - ano_cote_zscore_field        : z-score of cote within the field
  - ano_age_outlier              : 1 if age > 2 std from field mean
  - ano_experience_outlier       : 1 if nb_courses > 2 std from field mean
  - ano_gains_outlier            : 1 if gains_carriere > 2 std from field mean
  - ano_form_reversal            : 1 if good recent form but high cote rank
  - ano_market_disagree          : |proba_implicite - historical_win_rate|
  - ano_class_jump               : normalized class rating vs field avg
  - ano_speed_outlier            : 1 if speed_figure > 1.5 std from field mean
  - ano_comeback_signal          : 1 if losing streak but has raced at this hippo
  - ano_distance_outlier         : 1 if distance differs > 500m from horse avg
  - ano_weight_outlier           : 1 if poids > 2 std from field mean
  - ano_composite_anomaly_score  : sum of individual outlier flags (0-7)

Usage:
    python feature_builders/anomaly_detection_builder.py
    python feature_builders/anomaly_detection_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
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
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/anomaly_detection"
)

# Progress / GC
_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# Outlier thresholds
_Z_THRESHOLD = 2.0       # for age, experience, gains, weight
_Z_SPEED_THRESHOLD = 1.5  # for speed outlier
_DISTANCE_DIFF_M = 500    # for distance outlier


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _mean_std(values: list[float]) -> tuple[Optional[float], Optional[float]]:
    """Compute mean and population std of a list. Returns (None, None) if empty."""
    if not values:
        return None, None
    n = len(values)
    m = sum(values) / n
    if n < 2:
        return m, 0.0
    variance = sum((x - m) ** 2 for x in values) / n
    return m, math.sqrt(variance)


def _zscore(value: Optional[float], mean: Optional[float], std: Optional[float]) -> Optional[float]:
    """Compute z-score. Returns None if inputs are missing or std is 0."""
    if value is None or mean is None or std is None:
        return None
    if std < 1e-9:
        return 0.0
    return round((value - mean) / std, 4)


def _is_outlier(value: Optional[float], mean: Optional[float], std: Optional[float],
                threshold: float = _Z_THRESHOLD) -> Optional[int]:
    """Return 1 if |z-score| > threshold, 0 otherwise. None if data missing."""
    z = _zscore(value, mean, std)
    if z is None:
        return None
    return 1 if abs(z) > threshold else 0


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
# FIELD STATS (per course_uid)
# ===========================================================================


class FieldStats:
    """Aggregated statistics for one course field."""
    __slots__ = (
        "cote_mean", "cote_std", "cote_median",
        "age_mean", "age_std",
        "exp_mean", "exp_std",
        "gains_mean", "gains_std",
        "speed_mean", "speed_std",
        "poids_mean", "poids_std",
        "cote_ranks",  # dict partant_uid -> rank (1=lowest cote=favourite)
        "nb_partants",
    )

    def __init__(self):
        for s in self.__slots__:
            setattr(self, s, None)


def _compute_field_stats(runners: list[dict]) -> FieldStats:
    """Compute field-level distribution stats from a list of slim runner dicts."""
    fs = FieldStats()
    fs.nb_partants = len(runners)

    # Collect values
    cotes = [r["cote"] for r in runners if r["cote"] is not None]
    ages = [r["age"] for r in runners if r["age"] is not None]
    exps = [r["nb_courses"] for r in runners if r["nb_courses"] is not None]
    gains = [r["gains"] for r in runners if r["gains"] is not None]
    speeds = [r["speed"] for r in runners if r["speed"] is not None]
    poids_list = [r["poids"] for r in runners if r["poids"] is not None]

    fs.cote_mean, fs.cote_std = _mean_std(cotes)
    fs.age_mean, fs.age_std = _mean_std(ages)
    fs.exp_mean, fs.exp_std = _mean_std(exps)
    fs.gains_mean, fs.gains_std = _mean_std(gains)
    fs.speed_mean, fs.speed_std = _mean_std(speeds)
    fs.poids_mean, fs.poids_std = _mean_std(poids_list)

    # Median cote
    if cotes:
        s = sorted(cotes)
        mid = len(s) // 2
        fs.cote_median = s[mid] if len(s) % 2 == 1 else (s[mid - 1] + s[mid]) / 2.0
    else:
        fs.cote_median = None

    # Cote rank (1 = lowest cote = favourite)
    fs.cote_ranks = {}
    if cotes:
        indexed = [(r["uid"], r["cote"]) for r in runners if r["cote"] is not None]
        indexed.sort(key=lambda x: x[1])
        for rank, (uid, _) in enumerate(indexed, start=1):
            fs.cote_ranks[uid] = rank

    return fs


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_anomaly_detection_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build anomaly detection features in two passes."""
    logger.info("=== Anomaly Detection Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # PASS 1: Read all records into slim dicts, group by course_uid
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 - Lu %d records...", n_read)

        cote = _safe_float(rec.get("cote_finale") or rec.get("rapport_final"))
        age = _safe_int(rec.get("age"))
        nb_courses = _safe_int(rec.get("nb_courses_carriere"))
        gains = _safe_float(rec.get("gains_carriere_euros") or rec.get("gains_carriere"))
        speed = _safe_float(rec.get("speed_figure") or rec.get("spd_speed_figure"))
        poids = _safe_float(rec.get("poids_porte_kg") or rec.get("poids"))
        distance = _safe_float(rec.get("distance") or rec.get("distance_metres"))
        nb_victoires = _safe_int(rec.get("nb_victoires_carriere"))

        # For form reversal: seq_serie_places (recent placing streak)
        seq_places = _safe_int(rec.get("seq_serie_places"))
        # For comeback signal: seq_serie_non_places (losing streak)
        seq_non_places = _safe_int(rec.get("seq_serie_non_places"))
        # For class jump
        class_rating = _safe_float(rec.get("spd_class_rating"))
        field_strength = _safe_float(rec.get("spd_field_strength_avg"))
        # Hippodrome for comeback signal
        hippodrome = rec.get("hippodrome") or rec.get("nom_hippodrome") or ""

        slim = {
            "uid": rec.get("partant_uid"),
            "course_uid": rec.get("course_uid", ""),
            "date": rec.get("date_reunion_iso", ""),
            "cote": cote,
            "age": age,
            "nb_courses": nb_courses,
            "gains": gains,
            "speed": speed,
            "poids": poids,
            "distance": distance,
            "nb_victoires": nb_victoires,
            "seq_places": seq_places,
            "seq_non_places": seq_non_places,
            "class_rating": class_rating,
            "field_strength": field_strength,
            "hippodrome": hippodrome,
        }
        slim_records.append(slim)

        if n_read % _GC_EVERY == 0:
            gc.collect()

    logger.info(
        "Pass 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0
    )
    gc.collect()

    # ------------------------------------------------------------------
    # Group by course_uid and compute field stats
    # ------------------------------------------------------------------
    t1 = time.time()
    course_groups: dict[str, list[dict]] = defaultdict(list)
    for s in slim_records:
        if s["course_uid"]:
            course_groups[s["course_uid"]].append(s)

    logger.info("Courses uniques: %d", len(course_groups))

    # Compute field stats per course
    course_stats: dict[str, FieldStats] = {}
    for cuid, runners in course_groups.items():
        course_stats[cuid] = _compute_field_stats(runners)

    logger.info("Field stats calculees en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Build horse-level context for comeback signal and distance outlier:
    #   - horse_hippos: set of hippodromes where horse has raced
    #   - horse_distances: list of distances horse has raced at
    # We build these from ALL records (no temporal concern since it is
    # field-relative, not predictive of future).
    # ------------------------------------------------------------------
    t2 = time.time()
    horse_hippos: dict[str, set[str]] = defaultdict(set)
    horse_distances: dict[str, list[float]] = defaultdict(list)

    for s in slim_records:
        h = s.get("uid")
        nom = h  # partant_uid is per-race; we need horse identity
        # Extract horse name from partant_uid if possible (format: date_hippo_courseN_numX)
        # Actually, we need a horse identifier. Use nom_cheval if available, else fall back.
        # Since we only have slim records, we use the hippodrome/distance for the same
        # partant_uid grouping. For a simpler approach, just check if the CURRENT horse
        # hippodrome appears in the field at all (sufficient for outsider signal).
        hippo = s.get("hippodrome", "")
        dist = s.get("distance")
        if h and hippo:
            horse_hippos[h].add(hippo)
        if h and dist is not None:
            horse_distances[h].append(dist)

    logger.info("Horse context built in %.1fs", time.time() - t2)

    # ------------------------------------------------------------------
    # PASS 2: Compute anomaly features per partant
    # ------------------------------------------------------------------
    t3 = time.time()
    results: list[dict[str, Any]] = []
    n_processed = 0

    for slim in slim_records:
        uid = slim["uid"]
        cuid = slim["course_uid"]
        fs = course_stats.get(cuid)

        if fs is None:
            # No field stats (orphan record) - emit nulls
            results.append({
                "partant_uid": uid,
                "course_uid": cuid,
                "date_reunion_iso": slim["date"],
                "ano_cote_zscore_field": None,
                "ano_age_outlier": None,
                "ano_experience_outlier": None,
                "ano_gains_outlier": None,
                "ano_form_reversal": None,
                "ano_market_disagree": None,
                "ano_class_jump": None,
                "ano_speed_outlier": None,
                "ano_comeback_signal": None,
                "ano_distance_outlier": None,
                "ano_weight_outlier": None,
                "ano_composite_anomaly_score": None,
            })
            n_processed += 1
            continue

        # --- ano_cote_zscore_field ---
        cote_z = _zscore(slim["cote"], fs.cote_mean, fs.cote_std)

        # --- ano_age_outlier ---
        age_out = _is_outlier(slim["age"], fs.age_mean, fs.age_std, _Z_THRESHOLD)

        # --- ano_experience_outlier ---
        exp_out = _is_outlier(slim["nb_courses"], fs.exp_mean, fs.exp_std, _Z_THRESHOLD)

        # --- ano_gains_outlier ---
        gains_out = _is_outlier(slim["gains"], fs.gains_mean, fs.gains_std, _Z_THRESHOLD)

        # --- ano_form_reversal ---
        # Good recent form (seq_serie_places > 0) but high cote rank in field
        form_rev = None
        seq_pl = slim["seq_places"]
        if seq_pl is not None and fs.cote_ranks and uid in fs.cote_ranks:
            n_ranked = len(fs.cote_ranks)
            cote_rank = fs.cote_ranks[uid]
            # "high cote rank" = in bottom half of market confidence (high rank number)
            if n_ranked >= 2:
                if seq_pl > 0 and cote_rank > n_ranked / 2:
                    form_rev = 1
                else:
                    form_rev = 0

        # --- ano_market_disagree ---
        mkt_disagree = None
        if slim["cote"] is not None and slim["cote"] > 0:
            proba_implicite = 1.0 / slim["cote"]
            nb_c = slim["nb_courses"]
            nb_v = slim["nb_victoires"]
            if nb_c is not None and nb_c > 0 and nb_v is not None:
                hist_wr = nb_v / nb_c
                mkt_disagree = round(abs(proba_implicite - hist_wr), 4)

        # --- ano_class_jump ---
        class_jump = None
        cr = slim["class_rating"]
        fsa = slim["field_strength"]
        if cr is not None and fsa is not None and fsa > 0:
            class_jump = round((cr - fsa) / fsa, 4)

        # --- ano_speed_outlier ---
        speed_out = _is_outlier(slim["speed"], fs.speed_mean, fs.speed_std, _Z_SPEED_THRESHOLD)

        # --- ano_comeback_signal ---
        # Losing streak (seq_non_places >= 3) but has raced at this hippodrome before
        comeback = None
        seq_np = slim["seq_non_places"]
        hippo = slim["hippodrome"]
        if seq_np is not None and hippo:
            hippos_set = horse_hippos.get(uid, set())
            if seq_np >= 3 and hippo in hippos_set and len(hippos_set) > 1:
                comeback = 1
            elif seq_np >= 3:
                comeback = 0
            else:
                comeback = 0

        # --- ano_distance_outlier ---
        dist_out = None
        cur_dist = slim["distance"]
        if cur_dist is not None and uid in horse_distances:
            dists = horse_distances[uid]
            if len(dists) >= 2:
                avg_dist = sum(dists) / len(dists)
                if abs(cur_dist - avg_dist) > _DISTANCE_DIFF_M:
                    dist_out = 1
                else:
                    dist_out = 0

        # --- ano_weight_outlier ---
        weight_out = _is_outlier(slim["poids"], fs.poids_mean, fs.poids_std, _Z_THRESHOLD)

        # --- ano_composite_anomaly_score ---
        flags = [age_out, exp_out, gains_out, form_rev, speed_out, comeback, weight_out]
        non_null_flags = [f for f in flags if f is not None]
        composite = sum(non_null_flags) if non_null_flags else None

        results.append({
            "partant_uid": uid,
            "course_uid": cuid,
            "date_reunion_iso": slim["date"],
            "ano_cote_zscore_field": cote_z,
            "ano_age_outlier": age_out,
            "ano_experience_outlier": exp_out,
            "ano_gains_outlier": gains_out,
            "ano_form_reversal": form_rev,
            "ano_market_disagree": mkt_disagree,
            "ano_class_jump": class_jump,
            "ano_speed_outlier": speed_out,
            "ano_comeback_signal": comeback,
            "ano_distance_outlier": dist_out,
            "ano_weight_outlier": weight_out,
            "ano_composite_anomaly_score": composite,
        })

        n_processed += 1
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Pass 2 - Traite %d / %d records...", n_processed, len(slim_records))
        if n_processed % _GC_EVERY == 0:
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Anomaly detection build termine: %d features en %.1fs (courses: %d)",
        len(results),
        elapsed,
        len(course_stats),
    )

    return results


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
        description="Construction des features anomaly detection a partir de partants_master"
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
        help="Repertoire de sortie (defaut: builder_outputs/anomaly_detection/)",
    )
    args = parser.parse_args()

    logger = setup_logging("anomaly_detection_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_anomaly_detection_features(input_path, logger)

    # Save
    out_path = output_dir / "anomaly_detection_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rates
    if results:
        feature_keys = [
            k for k in results[0]
            if k not in ("partant_uid", "course_uid", "date_reunion_iso")
        ]
        filled = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)

    gc.collect()
    logger.info("Termine.")


if __name__ == "__main__":
    main()
