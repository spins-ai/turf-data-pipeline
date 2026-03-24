#!/usr/bin/env python3
"""
feature_builders.bayesian_rating_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Bayesian shrinkage ratings for horses, jockeys, and trainers.

New entities with few races are pulled toward the population average,
while experienced entities keep their actual performance.

Formula:
    bayes_rate = (global_avg * prior_weight + entity_sum) / (prior_weight + entity_count)

Prior weights: horse=10, jockey=20, trainer=15.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant Bayesian features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the rating -- no future leakage.

Produces:
  - bayesian_ratings.jsonl   in output/bayesian_ratings/

Features per partant (8):
  - bayes_horse_win_rate     : Bayesian win rate for the horse (shrunk toward global avg)
  - bayes_horse_place_rate   : Bayesian place rate (top 3)
  - bayes_jockey_win_rate    : Bayesian win rate for jockey
  - bayes_jockey_roi         : Bayesian ROI for jockey (shrunk toward -15% global avg)
  - bayes_trainer_win_rate   : Bayesian win rate for trainer
  - bayes_trainer_strike_rate: Bayesian place rate for trainer
  - bayes_combo_jt_win       : Jockey-trainer combination win rate
  - bayes_confidence         : 1 - (prior_weight / (prior_weight + nb_courses)),
                               measure of how much we trust the estimate

Usage:
    python feature_builders/bayesian_rating_builder.py
    python feature_builders/bayesian_rating_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "bayesian_ratings"

# Prior weights per entity type
PRIOR_WEIGHT_HORSE = 10
PRIOR_WEIGHT_JOCKEY = 20
PRIOR_WEIGHT_TRAINER = 15
PRIOR_WEIGHT_COMBO = 15  # jockey-trainer combo

# Global average priors
GLOBAL_WIN_RATE = 0.10       # ~10% average win rate
GLOBAL_PLACE_RATE = 0.30     # ~30% top-3 finish rate
GLOBAL_ROI = -0.15           # -15% average ROI

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# ENTITY ACCUMULATORS
# ===========================================================================


class _WinPlaceAccum:
    """Accumulate wins, places and total starts for an entity."""

    __slots__ = ("wins", "places", "starts")

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.starts: int = 0


class _ROIAccum:
    """Accumulate ROI numerator (net gains) and starts."""

    __slots__ = ("net_gain", "starts")

    def __init__(self) -> None:
        self.net_gain: float = 0.0
        self.starts: int = 0


# ===========================================================================
# BAYESIAN COMPUTATION
# ===========================================================================


def _bayes_rate(global_avg: float, prior_weight: int,
                entity_sum: float, entity_count: int) -> float:
    """Compute Bayesian shrinkage estimate.

    bayes_rate = (global_avg * prior_weight + entity_sum) / (prior_weight + entity_count)
    """
    return (global_avg * prior_weight + entity_sum) / (prior_weight + entity_count)


def _confidence(prior_weight: int, nb_courses: int) -> float:
    """How much we trust the entity's own data vs the prior.

    Returns 1 - (prior_weight / (prior_weight + nb_courses)).
    0 = pure prior, approaching 1 = mostly raw data.
    """
    return 1.0 - (prior_weight / (prior_weight + nb_courses))


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
# MAIN BUILD
# ===========================================================================


def build_bayesian_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build Bayesian shrinkage rating features from partants_master.jsonl.

    Single-pass approach: read minimal fields, sort chronologically,
    then process record-by-record accumulating stats per entity.
    For each partant, emit the Bayesian rating computed from all
    prior races (strict temporal integrity: date < current date).
    """
    logger.info("=== Bayesian Rating Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Extract rapport_simple (odds) for ROI calculation
        rapport = rec.get("rapport_simple_gagnant")
        if rapport is not None:
            try:
                rapport = float(rapport)
            except (ValueError, TypeError):
                rapport = None

        position = rec.get("position_arrivee")
        if position is not None:
            try:
                position = int(position)
            except (ValueError, TypeError):
                position = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "jockey": rec.get("jockey_driver"),
            "entraineur": rec.get("entraineur"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": position,
            "rapport": rapport,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process record by record with temporal grouping --
    t2 = time.time()

    # Accumulators per entity
    horse_acc: dict[str, _WinPlaceAccum] = defaultdict(_WinPlaceAccum)
    jockey_acc: dict[str, _WinPlaceAccum] = defaultdict(_WinPlaceAccum)
    jockey_roi: dict[str, _ROIAccum] = defaultdict(_ROIAccum)
    trainer_acc: dict[str, _WinPlaceAccum] = defaultdict(_WinPlaceAccum)
    combo_acc: dict[str, _WinPlaceAccum] = defaultdict(_WinPlaceAccum)

    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by (date, course) for batch update after emitting features
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (i < total
               and slim_records[i]["course"] == course_uid
               and slim_records[i]["date"] == course_date):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # -- Emit pre-race Bayesian features (no leakage) --
        for rec in course_group:
            h = rec["cheval"]
            j = rec["jockey"]
            t = rec["entraineur"]
            combo_key = f"{j}||{t}" if j and t else None

            # Horse features
            if h and horse_acc[h].starts > 0:
                ha = horse_acc[h]
                b_horse_win = _bayes_rate(GLOBAL_WIN_RATE, PRIOR_WEIGHT_HORSE,
                                          ha.wins, ha.starts)
                b_horse_place = _bayes_rate(GLOBAL_PLACE_RATE, PRIOR_WEIGHT_HORSE,
                                            ha.places, ha.starts)
                horse_n = ha.starts
            else:
                b_horse_win = GLOBAL_WIN_RATE
                b_horse_place = GLOBAL_PLACE_RATE
                horse_n = 0

            # Jockey features
            if j and jockey_acc[j].starts > 0:
                ja = jockey_acc[j]
                b_jockey_win = _bayes_rate(GLOBAL_WIN_RATE, PRIOR_WEIGHT_JOCKEY,
                                           ja.wins, ja.starts)
                jr = jockey_roi[j]
                avg_roi = jr.net_gain / jr.starts if jr.starts > 0 else GLOBAL_ROI
                b_jockey_roi = _bayes_rate(GLOBAL_ROI, PRIOR_WEIGHT_JOCKEY,
                                           jr.net_gain, jr.starts)
                jockey_n = ja.starts
            else:
                b_jockey_win = GLOBAL_WIN_RATE
                b_jockey_roi = GLOBAL_ROI
                jockey_n = 0

            # Trainer features
            if t and trainer_acc[t].starts > 0:
                ta = trainer_acc[t]
                b_trainer_win = _bayes_rate(GLOBAL_WIN_RATE, PRIOR_WEIGHT_TRAINER,
                                            ta.wins, ta.starts)
                b_trainer_strike = _bayes_rate(GLOBAL_PLACE_RATE, PRIOR_WEIGHT_TRAINER,
                                               ta.places, ta.starts)
                trainer_n = ta.starts
            else:
                b_trainer_win = GLOBAL_WIN_RATE
                b_trainer_strike = GLOBAL_PLACE_RATE
                trainer_n = 0

            # Jockey-trainer combo
            if combo_key and combo_acc[combo_key].starts > 0:
                ca = combo_acc[combo_key]
                b_combo_jt = _bayes_rate(GLOBAL_WIN_RATE, PRIOR_WEIGHT_COMBO,
                                         ca.wins, ca.starts)
            else:
                b_combo_jt = GLOBAL_WIN_RATE

            # Confidence: use horse prior weight and horse race count
            conf = _confidence(PRIOR_WEIGHT_HORSE, horse_n)

            results.append({
                "partant_uid": rec["uid"],
                "bayes_horse_win_rate": round(b_horse_win, 5),
                "bayes_horse_place_rate": round(b_horse_place, 5),
                "bayes_jockey_win_rate": round(b_jockey_win, 5),
                "bayes_jockey_roi": round(b_jockey_roi, 5),
                "bayes_trainer_win_rate": round(b_trainer_win, 5),
                "bayes_trainer_strike_rate": round(b_trainer_strike, 5),
                "bayes_combo_jt_win": round(b_combo_jt, 5),
                "bayes_confidence": round(conf, 5),
            })

        # -- Update accumulators AFTER emitting features (temporal integrity) --
        for rec in course_group:
            h = rec["cheval"]
            j = rec["jockey"]
            t = rec["entraineur"]
            is_win = rec["gagnant"]
            pos = rec["position"]
            is_place = pos is not None and 1 <= pos <= 3
            rapport = rec["rapport"]

            # Horse accumulator
            if h:
                horse_acc[h].starts += 1
                if is_win:
                    horse_acc[h].wins += 1
                if is_place:
                    horse_acc[h].places += 1

            # Jockey accumulator
            if j:
                jockey_acc[j].starts += 1
                if is_win:
                    jockey_acc[j].wins += 1
                if is_place:
                    jockey_acc[j].places += 1
                # ROI: net gain = (rapport - 1) if won, else -1
                jockey_roi[j].starts += 1
                if is_win and rapport is not None and rapport > 0:
                    jockey_roi[j].net_gain += (rapport - 1.0)
                else:
                    jockey_roi[j].net_gain -= 1.0

            # Trainer accumulator
            if t:
                trainer_acc[t].starts += 1
                if is_win:
                    trainer_acc[t].wins += 1
                if is_place:
                    trainer_acc[t].places += 1

            # Combo accumulator
            combo_key = f"{j}||{t}" if j and t else None
            if combo_key:
                combo_acc[combo_key].starts += 1
                if is_win:
                    combo_acc[combo_key].wins += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Bayesian build termine: %d features en %.1fs "
        "(chevaux: %d, jockeys: %d, entraineurs: %d, combos: %d)",
        len(results), elapsed,
        len(horse_acc), len(jockey_acc), len(trainer_acc), len(combo_acc),
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
        description="Construction des features Bayesian shrinkage a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/bayesian_ratings/)",
    )
    args = parser.parse_args()

    logger = setup_logging("bayesian_rating_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_bayesian_features(input_path, logger)

    # Save
    out_path = output_dir / "bayesian_ratings.jsonl"
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
