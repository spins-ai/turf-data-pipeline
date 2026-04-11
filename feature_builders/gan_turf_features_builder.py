#!/usr/bin/env python3
"""
feature_builders.gan_turf_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features for the GAN-Turf module -- features that help detect
"fake-looking" races (highly predictable) vs "real chaos" (genuinely
unpredictable outsider wins).  GANs need discriminator features.

Architecture (two passes, no temporal dependency):
  Pass 1 : Stream partants_master.jsonl, collect minimal fields per
           course_uid into a dict-of-lists (win_rate, implied_prob,
           class_rating, age, nb_courses, cote).
  Pass 2 : For each course compute field-level stats, then emit
           one record per partant with 8 features.

Produces:
  - gan_turf_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/gan_turf_features/

Features per partant (8):
  - gan_race_predictability  : how predictable was this race based on
                               field composition (low entropy = predictable)
  - gan_field_homogeneity    : 1 - std(win_rates) / mean(win_rates)
                               (more similar horses = harder to discriminate)
  - gan_favorite_dominance   : implied_prob(fav1) - implied_prob(fav2)
  - gan_class_variance       : variance of class ratings in field
  - gan_age_variance         : variance of ages in field
  - gan_experience_skew      : skewness of career races in field
                               (right skew = few very experienced)
  - gan_odds_kurtosis        : kurtosis of odds distribution
                               (fat tails = more outsiders)
  - gan_synthetic_difficulty : composite = field_homogeneity
                               * log(nombre_partants) * (1 - favorite_dominance)

Usage:
    python feature_builders/gan_turf_features_builder.py
    python feature_builders/gan_turf_features_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/gan_turf_features")

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
        return v if v == v else None  # reject NaN
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _variance(values: list[float]) -> Optional[float]:
    """Population variance. Returns None if fewer than 2 values."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    return sum((x - mean) ** 2 for x in values) / len(values)


def _stdev(values: list[float]) -> Optional[float]:
    """Population standard deviation."""
    var = _variance(values)
    if var is None:
        return None
    return math.sqrt(var)


def _skewness(values: list[float]) -> Optional[float]:
    """Sample skewness (Fisher). Returns None if fewer than 3 values."""
    n = len(values)
    if n < 3:
        return None
    mean = sum(values) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in values) / (n - 1))
    if sd == 0:
        return 0.0
    m3 = sum((x - mean) ** 3 for x in values) / n
    return round(m3 / (sd ** 3), 6)


def _kurtosis(values: list[float]) -> Optional[float]:
    """Excess kurtosis (Fisher). Returns None if fewer than 4 values."""
    n = len(values)
    if n < 4:
        return None
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / n
    if var == 0:
        return 0.0
    m4 = sum((x - mean) ** 4 for x in values) / n
    return round(m4 / (var ** 2) - 3.0, 6)


def _entropy(probs: list[float]) -> Optional[float]:
    """Shannon entropy of a probability distribution. Higher = less predictable."""
    if not probs:
        return None
    total = sum(probs)
    if total <= 0:
        return None
    normed = [p / total for p in probs if p > 0]
    return round(-sum(p * math.log(p) for p in normed), 6)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_gan_turf_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build GAN-Turf discriminator features.

    Returns the number of records written.
    """
    logger.info("=== GAN-Turf Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1 : Read minimal fields, group by course_uid
    # ------------------------------------------------------------------
    course_data: dict[str, list[dict]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        course_uid = rec.get("course_uid", "")
        if not course_uid:
            continue

        # Win rate from career counters
        nb_c = _safe_int(rec.get("nb_courses_carriere") or rec.get("nb_courses"))
        nb_v = _safe_int(rec.get("nb_victoires_carriere") or rec.get("nb_victoires"))
        win_rate: Optional[float] = None
        if nb_c is not None and nb_c > 0 and nb_v is not None:
            win_rate = nb_v / nb_c

        # Implied probability from cote
        cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference"))
        implied_prob: Optional[float] = None
        if cote is not None and cote > 0:
            implied_prob = 1.0 / cote

        # Class rating (handicap or allocation)
        class_rating = _safe_float(
            rec.get("handicap_valeur")
            or rec.get("handicap_poids")
            or rec.get("allocation")
        )

        slim = {
            "partant_uid": rec.get("partant_uid"),
            "course_uid": course_uid,
            "date": rec.get("date_reunion_iso", ""),
            "win_rate": win_rate,
            "implied_prob": implied_prob,
            "class_rating": class_rating,
            "age": _safe_int(rec.get("age")),
            "nb_courses": nb_c,
            "cote": _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference")),
        }
        course_data[course_uid].append(slim)

    logger.info(
        "Pass 1 terminee: %d records lus, %d courses uniques en %.1fs",
        n_read, len(course_data), time.time() - t0,
    )
    gc.collect()

    # ------------------------------------------------------------------
    # Pass 2 : Compute features per course, emit per partant
    # ------------------------------------------------------------------
    t1 = time.time()
    n_written = 0

    feature_keys = [
        "gan_race_predictability",
        "gan_field_homogeneity",
        "gan_favorite_dominance",
        "gan_class_variance",
        "gan_age_variance",
        "gan_experience_skew",
        "gan_odds_kurtosis",
        "gan_synthetic_difficulty",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    tmp_out = output_path.with_suffix(".tmp")

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for course_uid, runners in course_data.items():
            nb_partants = len(runners)

            # -- Collect field-level value lists --
            win_rates = [r["win_rate"] for r in runners if r["win_rate"] is not None]
            implied_probs = [r["implied_prob"] for r in runners if r["implied_prob"] is not None and r["implied_prob"] > 0]
            class_ratings = [r["class_rating"] for r in runners if r["class_rating"] is not None]
            ages = [float(r["age"]) for r in runners if r["age"] is not None]
            nb_courses_list = [float(r["nb_courses"]) for r in runners if r["nb_courses"] is not None]
            cotes = [r["cote"] for r in runners if r["cote"] is not None and r["cote"] > 0]

            # -- gan_race_predictability: Shannon entropy of implied probabilities --
            # Lower entropy = more predictable
            # Normalize: divide by max possible entropy (uniform) to get 0..1
            race_predictability: Optional[float] = None
            if implied_probs and len(implied_probs) >= 2:
                raw_entropy = _entropy(implied_probs)
                max_entropy = math.log(len(implied_probs))
                if raw_entropy is not None and max_entropy > 0:
                    # Invert: 1 = perfectly predictable, 0 = maximum chaos
                    race_predictability = round(1.0 - raw_entropy / max_entropy, 6)

            # -- gan_field_homogeneity: 1 - std(win_rates) / mean(win_rates) --
            field_homogeneity: Optional[float] = None
            if win_rates and len(win_rates) >= 2:
                mean_wr = sum(win_rates) / len(win_rates)
                std_wr = _stdev(win_rates)
                if mean_wr > 0 and std_wr is not None:
                    raw = 1.0 - std_wr / mean_wr
                    field_homogeneity = round(max(-1.0, min(1.0, raw)), 6)

            # -- gan_favorite_dominance: implied_prob(fav1) - implied_prob(fav2) --
            favorite_dominance: Optional[float] = None
            if implied_probs and len(implied_probs) >= 2:
                sorted_probs = sorted(implied_probs, reverse=True)
                favorite_dominance = round(sorted_probs[0] - sorted_probs[1], 6)

            # -- gan_class_variance --
            class_variance: Optional[float] = None
            if class_ratings and len(class_ratings) >= 2:
                var = _variance(class_ratings)
                if var is not None:
                    class_variance = round(var, 4)

            # -- gan_age_variance --
            age_variance: Optional[float] = None
            if ages and len(ages) >= 2:
                var = _variance(ages)
                if var is not None:
                    age_variance = round(var, 4)

            # -- gan_experience_skew --
            experience_skew: Optional[float] = None
            if nb_courses_list and len(nb_courses_list) >= 3:
                experience_skew = _skewness(nb_courses_list)

            # -- gan_odds_kurtosis --
            odds_kurtosis: Optional[float] = None
            if cotes and len(cotes) >= 4:
                odds_kurtosis = _kurtosis(cotes)

            # -- gan_synthetic_difficulty --
            # composite = field_homogeneity * log(nombre_partants) * (1 - favorite_dominance)
            synthetic_difficulty: Optional[float] = None
            if (
                field_homogeneity is not None
                and favorite_dominance is not None
                and nb_partants >= 2
            ):
                synthetic_difficulty = round(
                    field_homogeneity * math.log(nb_partants) * (1.0 - favorite_dominance),
                    6,
                )

            # -- Emit per-partant records --
            for r in runners:
                out_rec: dict[str, Any] = {
                    "partant_uid": r["partant_uid"],
                    "course_uid": r["course_uid"],
                    "date_reunion_iso": r["date"],
                    "gan_race_predictability": race_predictability,
                    "gan_field_homogeneity": field_homogeneity,
                    "gan_favorite_dominance": favorite_dominance,
                    "gan_class_variance": class_variance,
                    "gan_age_variance": age_variance,
                    "gan_experience_skew": experience_skew,
                    "gan_odds_kurtosis": odds_kurtosis,
                    "gan_synthetic_difficulty": synthetic_difficulty,
                }

                # Track fill rates
                for k in feature_keys:
                    if out_rec.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

            # GC every 500K written records
            if n_written % _LOG_EVERY < len(runners):
                logger.info("  Ecrit %d records...", n_written)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "GAN-Turf build termine: %d features en %.1fs (%d courses)",
        n_written, elapsed, len(course_data),
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
        description="Construction des features GAN-Turf (discriminator) a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/gan_turf_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("gan_turf_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "gan_turf_features.jsonl"
    build_gan_turf_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
