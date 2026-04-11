#!/usr/bin/env python3
"""
feature_builders.win_probability_ensemble_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Win probability ensemble features -- combining multiple signals
(market odds, form) into blended probability estimates per partant.

Two-pass streaming:
  Pass 1: read all partants, group by course_uid, build race-level
           lists of {num_pmu, cote_finale, win_rate}.
  Pass 2: re-read partants, compute per-partant features using
           precomputed race-level aggregates.

  - .tmp then atomic rename, gc.collect() every 500K records

Temporal integrity: uses only pre-race data (cote_finale, career
win rate) -- no future leakage.

Produces:
  - win_probability_ensemble.jsonl  in builder_outputs/win_probability_ensemble/

Features per partant (10):
  - wpe_market_prob         : 1 / cote_finale (raw market probability)
  - wpe_form_prob           : nb_victoires / max(nb_courses, 1) (form-based probability)
  - wpe_blend_prob          : 0.6 * market_prob + 0.4 * form_prob
  - wpe_rank_by_blend       : rank within race by blend_prob (1 = highest prob)
  - wpe_prob_vs_field_avg   : blend_prob - (1 / nombre_partants) -- above/below fair share
  - wpe_prob_concentration  : max(blend_prob) / sum(blend_prob) in race -- does one horse dominate?
  - wpe_horse_edge          : blend_prob - market_prob (positive = undervalued by blend model)
  - wpe_log_odds            : log(cote_finale) (log scale for regression models)
  - wpe_prob_entropy        : -sum(p * log(p)) for all blend probs in race (race uncertainty)
  - wpe_is_top3_prob        : 1 if horse is in top 3 by blend_prob

Usage:
    python feature_builders/win_probability_ensemble_builder.py
    python feature_builders/win_probability_ensemble_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/win_probability_ensemble")

_LOG_EVERY = 500_000

# Blend weights
_W_MARKET = 0.6
_W_FORM = 0.4


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
# MAIN BUILD (two-pass)
# ===========================================================================


def build_win_probability_ensemble_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build win probability ensemble features from partants_master.jsonl.

    Two-pass streaming:
      Pass 1 -- group by course_uid, collect per-runner market prob and
                form prob; precompute race-level aggregates.
      Pass 2 -- re-read records, compute per-partant features using the
                precomputed race-level stats.

    Writes to a .tmp file then atomically renames on success.

    Returns the total number of feature records written.
    """
    logger.info("=== Win Probability Ensemble Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # PASS 1: Per-course aggregation
    # ------------------------------------------------------------------
    logger.info("--- Pass 1: per-course aggregation ---")

    # course_uid -> list of {num_pmu, market_prob, form_prob}
    course_data: dict[str, list[dict[str, Any]]] = defaultdict(list)
    # Also store nb_partants per course for fair-share calculation
    course_nb_partants: dict[str, Optional[int]] = {}

    n_read = 0
    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1: lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid", "")
        num_pmu = rec.get("num_pmu", 0) or 0

        # Market probability: 1 / cote_finale
        cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference"))
        market_prob: Optional[float] = None
        if cote is not None and cote > 0:
            market_prob = 1.0 / cote

        # Form probability: nb_victoires / max(nb_courses, 1)
        nb_vict = _safe_int(rec.get("nb_victoires_carriere")) or 0
        nb_courses = _safe_int(rec.get("nb_courses_carriere")) or 0
        form_prob = nb_vict / max(nb_courses, 1) if nb_courses >= 0 else 0.0

        course_data[course_uid].append({
            "num": num_pmu,
            "market_prob": market_prob,
            "form_prob": form_prob,
            "cote": cote,
        })

        # Store nombre_partants (take first non-null per course)
        if course_uid not in course_nb_partants:
            course_nb_partants[course_uid] = _safe_int(rec.get("nombre_partants"))

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        n_read, len(course_data), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Precompute race-level stats from Pass 1 data
    # ------------------------------------------------------------------
    t1 = time.time()
    logger.info("--- Precomputing race-level stats ---")

    # course_uid -> {blend_probs: [...], sum_blend, max_blend, entropy,
    #                concentration, ranks: {num: rank}, top3: set}
    course_stats: dict[str, dict[str, Any]] = {}

    for cuid, runners in course_data.items():
        # Compute blend_prob for each runner
        blend_probs: list[Optional[float]] = []
        for r in runners:
            mp = r["market_prob"]
            fp = r["form_prob"]
            if mp is not None:
                bp = _W_MARKET * mp + _W_FORM * fp
            else:
                # Fallback: form_prob only (scaled so it stays comparable)
                bp = fp if fp > 0 else None
            blend_probs.append(bp)

        # Non-null blend probs
        valid_blends = [(i, bp) for i, bp in enumerate(blend_probs) if bp is not None and bp > 0]

        sum_blend = sum(bp for _, bp in valid_blends) if valid_blends else 0.0
        max_blend = max((bp for _, bp in valid_blends), default=0.0)

        # Concentration: max / sum
        concentration: Optional[float] = None
        if sum_blend > 0:
            concentration = round(max_blend / sum_blend, 6)

        # Entropy: -sum(p * log(p)) where p = blend / sum_blend (normalized)
        entropy: Optional[float] = None
        if sum_blend > 0 and len(valid_blends) >= 2:
            ent = 0.0
            for _, bp in valid_blends:
                p_norm = bp / sum_blend
                if p_norm > 0:
                    ent -= p_norm * math.log(p_norm)
            entropy = round(ent, 6)

        # Rank by blend_prob (1 = highest)
        # Sort by blend desc, assign ranks
        sorted_by_blend = sorted(valid_blends, key=lambda x: x[1], reverse=True)
        rank_map: dict[int, int] = {}  # runner index -> rank
        for rank_pos, (idx, _) in enumerate(sorted_by_blend, start=1):
            rank_map[idx] = rank_pos

        # Top 3 set (runner indices)
        top3_indices: set[int] = set()
        for idx, _ in sorted_by_blend[:3]:
            top3_indices.add(idx)

        course_stats[cuid] = {
            "blend_probs": blend_probs,
            "sum_blend": sum_blend,
            "concentration": concentration,
            "entropy": entropy,
            "rank_map": rank_map,
            "top3_indices": top3_indices,
        }

    logger.info("Race-level stats precomputed in %.1fs", time.time() - t1)

    # Free course_data (no longer needed)
    del course_data
    gc.collect()

    # ------------------------------------------------------------------
    # PASS 2: Per-partant feature computation
    # ------------------------------------------------------------------
    t2 = time.time()
    logger.info("--- Pass 2: per-partant features ---")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0

    fill_counts = {
        "wpe_market_prob": 0,
        "wpe_form_prob": 0,
        "wpe_blend_prob": 0,
        "wpe_rank_by_blend": 0,
        "wpe_prob_vs_field_avg": 0,
        "wpe_prob_concentration": 0,
        "wpe_horse_edge": 0,
        "wpe_log_odds": 0,
        "wpe_prob_entropy": 0,
        "wpe_is_top3_prob": 0,
    }

    # We need to track which runner index we are at per course
    course_runner_idx: dict[str, int] = defaultdict(int)

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Pass 2: traite %d records...", n_processed)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid", "")

            # Runner index within this course (same order as Pass 1)
            runner_idx = course_runner_idx[course_uid]
            course_runner_idx[course_uid] += 1

            stats = course_stats.get(course_uid)
            if stats is None:
                # Should not happen, but defensive
                features: dict[str, Any] = {
                    "partant_uid": partant_uid,
                    "wpe_market_prob": None,
                    "wpe_form_prob": None,
                    "wpe_blend_prob": None,
                    "wpe_rank_by_blend": None,
                    "wpe_prob_vs_field_avg": None,
                    "wpe_prob_concentration": None,
                    "wpe_horse_edge": None,
                    "wpe_log_odds": None,
                    "wpe_prob_entropy": None,
                    "wpe_is_top3_prob": None,
                }
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1
                continue

            # --- Per-partant raw signals ---
            cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference"))
            market_prob: Optional[float] = None
            if cote is not None and cote > 0:
                market_prob = round(1.0 / cote, 6)

            nb_vict = _safe_int(rec.get("nb_victoires_carriere")) or 0
            nb_courses = _safe_int(rec.get("nb_courses_carriere")) or 0
            form_prob = round(nb_vict / max(nb_courses, 1), 6) if nb_courses >= 0 else 0.0

            # Blend prob from precomputed list
            blend_prob: Optional[float] = None
            bp_list = stats["blend_probs"]
            if runner_idx < len(bp_list):
                bp_raw = bp_list[runner_idx]
                if bp_raw is not None:
                    blend_prob = round(bp_raw, 6)

            # Rank by blend
            rank_by_blend: Optional[int] = stats["rank_map"].get(runner_idx)

            # Prob vs field average: blend_prob - (1 / nombre_partants)
            prob_vs_field: Optional[float] = None
            nb_partants = _safe_int(rec.get("nombre_partants")) or course_nb_partants.get(course_uid)
            if blend_prob is not None and nb_partants is not None and nb_partants > 0:
                fair_share = 1.0 / nb_partants
                prob_vs_field = round(blend_prob - fair_share, 6)

            # Concentration (race-level, same for all runners in race)
            prob_concentration = stats["concentration"]

            # Horse edge: blend_prob - market_prob
            horse_edge: Optional[float] = None
            if blend_prob is not None and market_prob is not None:
                horse_edge = round(blend_prob - market_prob, 6)

            # Log odds
            log_odds: Optional[float] = None
            if cote is not None and cote > 0:
                log_odds = round(math.log(cote), 6)

            # Entropy (race-level)
            prob_entropy = stats["entropy"]

            # Is top 3 prob
            is_top3: Optional[int] = None
            if runner_idx in stats["top3_indices"]:
                is_top3 = 1
            elif rank_by_blend is not None:
                is_top3 = 0

            # --- Build feature dict ---
            features = {
                "partant_uid": partant_uid,
                "wpe_market_prob": market_prob,
                "wpe_form_prob": form_prob,
                "wpe_blend_prob": blend_prob,
                "wpe_rank_by_blend": rank_by_blend,
                "wpe_prob_vs_field_avg": prob_vs_field,
                "wpe_prob_concentration": prob_concentration,
                "wpe_horse_edge": horse_edge,
                "wpe_log_odds": log_odds,
                "wpe_prob_entropy": prob_entropy,
                "wpe_is_top3_prob": is_top3,
            }

            # Track fill rates
            for k in fill_counts:
                if features.get(k) is not None:
                    fill_counts[k] += 1

            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Win probability ensemble build termine: %d features en %.1fs",
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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features win probability ensemble a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/win_probability_ensemble/)",
    )
    args = parser.parse_args()

    logger = setup_logging("win_probability_ensemble_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "win_probability_ensemble.jsonl"
    build_win_probability_ensemble_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
