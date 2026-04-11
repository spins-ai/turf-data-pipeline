#!/usr/bin/env python3
"""
feature_builders.race_result_prediction_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race result prediction proxy features -- features that help predict
different aspects of race outcomes by blending market odds, career
statistics and within-field relative rankings.

Temporal integrity: all features are derived from pre-race data
(career stats and odds known before the race), no future leakage.

Produces:
  - race_result_prediction.jsonl   in builder_outputs/race_result_prediction/

Features per partant (10):
  - rrp_implied_win_prob           : 1 / cote_finale (raw implied probability)
  - rrp_implied_place_prob         : min(3/nombre_partants, 1) * 1/cote_finale * 3
  - rrp_horse_class_percentile     : gains_carriere rank percentile within field (0-1)
  - rrp_horse_experience_percentile: nb_courses_carriere rank percentile within field
  - rrp_horse_wr_percentile        : win rate rank percentile within field
  - rrp_win_prob_adjusted          : implied_prob * (1 + (wr - 1/cote) * 0.5)
  - rrp_field_predictability       : 1 / (std of implied probs in field)
  - rrp_horse_value_rank           : rank by (wr * cote - 1) within field (1 = best)
  - rrp_place_rate_career          : nb_places_carriere / max(nb_courses, 1)
  - rrp_exacta_proxy               : (1/cote * place_rate)

Memory-optimised version:
  - Two-pass: Pass 1 = per-course aggregation, Pass 2 = per-partant
  - gc.collect() called every 500K records
  - .tmp then atomic rename
  - open(..., newline="\\n")

Usage:
    python feature_builders/race_result_prediction_builder.py
    python feature_builders/race_result_prediction_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_result_prediction")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _percentile_rank(value: float, sorted_values: list[float]) -> float:
    """Return the percentile rank of *value* within *sorted_values* (0-1).

    Uses the fraction of values that are strictly less than *value*.
    """
    n = len(sorted_values)
    if n <= 1:
        return 0.5
    below = sum(1 for v in sorted_values if v < value)
    return below / (n - 1)


# ===========================================================================
# MAIN BUILD (two-pass, streaming output)
# ===========================================================================


def build_race_result_prediction_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build race result prediction proxy features.

    Pass 1 -- read the file and build per-course aggregation lists.
    Pass 2 -- iterate over partants again, compute per-partant features
              using the course-level context built in pass 1, and stream
              output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Race Result Prediction Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1: per-course aggregation
    # ------------------------------------------------------------------
    # course_uid -> { cotes, gains, courses, wins, places, num_pmus }
    course_agg: dict[str, dict[str, list]] = defaultdict(
        lambda: {
            "cotes": [],
            "gains": [],
            "courses": [],
            "wins": [],
            "places": [],
            "num_pmus": [],
        }
    )

    n_read = 0
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Pass 1 -- lu %d records...", n_read)
                gc.collect()

            course_uid = rec.get("course_uid", "")
            if not course_uid:
                continue

            agg = course_agg[course_uid]
            agg["cotes"].append(_safe_float(rec.get("cote_finale")))
            agg["gains"].append(_safe_float(rec.get("gains_carriere_euros")))
            agg["courses"].append(_safe_int(rec.get("nb_courses_carriere")))
            agg["wins"].append(_safe_int(rec.get("nb_victoires_carriere")))
            agg["places"].append(_safe_int(rec.get("nb_places_carriere")))
            agg["num_pmus"].append(_safe_int(rec.get("num_pmu")))

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        n_read, len(course_agg), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Pre-compute per-course sorted lists for percentile calculations
    # ------------------------------------------------------------------
    t1 = time.time()

    # course_uid -> { sorted_gains, sorted_courses, sorted_wr, implied_probs_std }
    course_ctx: dict[str, dict[str, Any]] = {}

    for cuid, agg in course_agg.items():
        ctx: dict[str, Any] = {}

        # Sorted gains (non-None)
        gains_clean = [g for g in agg["gains"] if g is not None]
        ctx["sorted_gains"] = sorted(gains_clean)

        # Sorted nb_courses (non-None)
        courses_clean = [c for c in agg["courses"] if c is not None]
        ctx["sorted_courses"] = sorted(float(c) for c in courses_clean)

        # Win rates per partant in field
        win_rates: list[Optional[float]] = []
        for w, c in zip(agg["wins"], agg["courses"]):
            if c is not None and c > 0 and w is not None:
                win_rates.append(w / c)
            else:
                win_rates.append(None)
        ctx["win_rates"] = win_rates
        wr_clean = [wr for wr in win_rates if wr is not None]
        ctx["sorted_wr"] = sorted(wr_clean)

        # Implied probabilities and their std
        implied_probs: list[Optional[float]] = []
        for cote in agg["cotes"]:
            if cote is not None and cote > 0:
                implied_probs.append(1.0 / cote)
            else:
                implied_probs.append(None)
        ctx["implied_probs"] = implied_probs

        ip_clean = [ip for ip in implied_probs if ip is not None]
        if len(ip_clean) >= 2:
            mean_ip = sum(ip_clean) / len(ip_clean)
            variance = sum((x - mean_ip) ** 2 for x in ip_clean) / len(ip_clean)
            std_ip = math.sqrt(variance) if variance > 0 else 0.0
            ctx["implied_probs_std"] = std_ip
        else:
            ctx["implied_probs_std"] = None

        # Value scores for ranking: (wr * cote - 1) -- higher = better value
        value_scores: list[Optional[float]] = []
        for wr, cote in zip(win_rates, agg["cotes"]):
            if wr is not None and cote is not None and cote > 0:
                value_scores.append(wr * cote - 1.0)
            else:
                value_scores.append(None)
        ctx["value_scores"] = value_scores

        # Pre-compute value rank (1 = best value, descending)
        indexed_vs = [
            (idx, vs) for idx, vs in enumerate(value_scores) if vs is not None
        ]
        indexed_vs.sort(key=lambda x: x[1], reverse=True)
        value_ranks: dict[int, int] = {}
        for rank, (idx, _) in enumerate(indexed_vs, start=1):
            value_ranks[idx] = rank
        ctx["value_ranks"] = value_ranks

        # nombre_partants from field size
        ctx["nb_partants"] = len(agg["cotes"])

        course_ctx[cuid] = ctx

    logger.info("Pre-calcul contexte courses en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Pass 2: per-partant features (streaming output)
    # ------------------------------------------------------------------
    t2 = time.time()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    feature_names = [
        "rrp_implied_win_prob",
        "rrp_implied_place_prob",
        "rrp_horse_class_percentile",
        "rrp_horse_experience_percentile",
        "rrp_horse_wr_percentile",
        "rrp_win_prob_adjusted",
        "rrp_field_predictability",
        "rrp_horse_value_rank",
        "rrp_place_rate_career",
        "rrp_exacta_proxy",
    ]
    fill_counts = {k: 0 for k in feature_names}
    n_written = 0

    # Track position within each course to map partant -> index in agg lists
    course_counters: dict[str, int] = defaultdict(int)

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        n_pass2 = 0
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_pass2 += 1
            if n_pass2 % _LOG_EVERY == 0:
                logger.info("  Pass 2 -- traite %d records...", n_pass2)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid", "")

            features: dict[str, Any] = {"partant_uid": partant_uid}

            ctx = course_ctx.get(course_uid)
            if ctx is None:
                # No course context -- all None
                for fn in feature_names:
                    features[fn] = None
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1
                continue

            idx = course_counters[course_uid]
            course_counters[course_uid] += 1

            nb_partants = ctx["nb_partants"]

            # Retrieve per-partant raw values from agg lists
            cote = _safe_float(rec.get("cote_finale"))
            gains = _safe_float(rec.get("gains_carriere_euros"))
            nb_courses = _safe_int(rec.get("nb_courses_carriere"))
            nb_wins = _safe_int(rec.get("nb_victoires_carriere"))
            nb_places = _safe_int(rec.get("nb_places_carriere"))

            # Win rate for this horse
            wr: Optional[float] = None
            if nb_courses is not None and nb_courses > 0 and nb_wins is not None:
                wr = nb_wins / nb_courses

            # 1. rrp_implied_win_prob: 1 / cote_finale
            implied_win_prob: Optional[float] = None
            if cote is not None and cote > 0:
                implied_win_prob = round(1.0 / cote, 6)
            features["rrp_implied_win_prob"] = implied_win_prob

            # 2. rrp_implied_place_prob: min(3/nombre_partants, 1) * 1/cote * 3
            implied_place_prob: Optional[float] = None
            if cote is not None and cote > 0 and nb_partants > 0:
                place_factor = min(3.0 / nb_partants, 1.0)
                implied_place_prob = round(place_factor * (1.0 / cote) * 3.0, 6)
            features["rrp_implied_place_prob"] = implied_place_prob

            # 3. rrp_horse_class_percentile: gains_carriere rank percentile within field
            horse_class_pct: Optional[float] = None
            if gains is not None and ctx["sorted_gains"]:
                horse_class_pct = round(
                    _percentile_rank(gains, ctx["sorted_gains"]), 4
                )
            features["rrp_horse_class_percentile"] = horse_class_pct

            # 4. rrp_horse_experience_percentile: nb_courses rank percentile
            horse_exp_pct: Optional[float] = None
            if nb_courses is not None and ctx["sorted_courses"]:
                horse_exp_pct = round(
                    _percentile_rank(float(nb_courses), ctx["sorted_courses"]), 4
                )
            features["rrp_horse_experience_percentile"] = horse_exp_pct

            # 5. rrp_horse_wr_percentile: win rate rank percentile
            horse_wr_pct: Optional[float] = None
            if wr is not None and ctx["sorted_wr"]:
                horse_wr_pct = round(
                    _percentile_rank(wr, ctx["sorted_wr"]), 4
                )
            features["rrp_horse_wr_percentile"] = horse_wr_pct

            # 6. rrp_win_prob_adjusted: implied_prob * (1 + (wr - 1/cote) * 0.5)
            win_prob_adj: Optional[float] = None
            if implied_win_prob is not None and wr is not None and cote is not None and cote > 0:
                adjustment = 1.0 + (wr - 1.0 / cote) * 0.5
                win_prob_adj = round(implied_win_prob * adjustment, 6)
            features["rrp_win_prob_adjusted"] = win_prob_adj

            # 7. rrp_field_predictability: 1 / std(implied probs)
            field_pred: Optional[float] = None
            std_ip = ctx["implied_probs_std"]
            if std_ip is not None and std_ip > 0:
                field_pred = round(1.0 / std_ip, 4)
            features["rrp_field_predictability"] = field_pred

            # 8. rrp_horse_value_rank: rank by (wr * cote - 1) within field
            value_rank: Optional[int] = None
            if idx in ctx["value_ranks"]:
                value_rank = ctx["value_ranks"][idx]
            features["rrp_horse_value_rank"] = value_rank

            # 9. rrp_place_rate_career: nb_places_carriere / max(nb_courses, 1)
            place_rate: Optional[float] = None
            if nb_places is not None and nb_courses is not None:
                place_rate = round(nb_places / max(nb_courses, 1), 4)
            features["rrp_place_rate_career"] = place_rate

            # 10. rrp_exacta_proxy: (1/cote * place_rate)
            exacta_proxy: Optional[float] = None
            if implied_win_prob is not None and place_rate is not None:
                exacta_proxy = round(implied_win_prob * place_rate, 6)
            features["rrp_exacta_proxy"] = exacta_proxy

            # Count fill rates
            for fn in feature_names:
                if features.get(fn) is not None:
                    fill_counts[fn] += 1

            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Race result prediction build termine: %d features en %.1fs (courses: %d)",
        n_written, elapsed, len(course_ctx),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)", k, v, n_written,
            100 * v / n_written if n_written else 0,
        )

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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features race result prediction a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/race_result_prediction/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_result_prediction_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "race_result_prediction.jsonl"
    build_race_result_prediction_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
