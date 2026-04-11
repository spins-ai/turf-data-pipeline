#!/usr/bin/env python3
"""
feature_builders.cluster_segmentation_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Rule-based horse type segmentation features (no sklearn needed).

Single-pass streaming over partants_master.jsonl.  Each record is
classified into categorical buckets based on distance, experience,
earnings, odds, age, field size, and weight.  A composite profile
hash and competitive index are also emitted.

Temporal integrity: all features are derived from the current record's
own fields (career counters already available at race time) -- no
future leakage.

Produces:
  - cluster_segmentation.jsonl  in builder_outputs/cluster_segmentation/

Features per partant (10):
  - cls_horse_type           : 0=sprinter 1=miler 2=middle 3=stayer
  - cls_experience_level     : 0=debutant 1=novice 2=experienced 3=veteran
  - cls_class_bucket         : 0..4 quintile of gains_carriere_euros
  - cls_odds_bucket          : 0=heavy_fav 1=fav 2=contender 3=outsider 4=longshot
  - cls_age_category         : 0=juvenile 1=classic 2=peak 3=mature 4=veteran
  - cls_field_size_category  : 0=small 1=medium 2=large 3=massive
  - cls_weight_category      : 0=light 1=normal 2=heavy 3=top_weight
  - cls_horse_profile_hash   : combined code 0..59 from (discipline, distance_type, age_cat)
  - cls_is_class_dropper     : 1 if gains > 80K and field > 12
  - cls_competitive_index    : (win_rate) * log(gains+1) / (age+1)

Usage:
    python feature_builders/cluster_segmentation_builder.py
    python feature_builders/cluster_segmentation_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/cluster_segmentation")

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
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# CLASSIFICATION FUNCTIONS
# ===========================================================================


def _horse_type(distance: Optional[float]) -> Optional[int]:
    """0=sprinter (<1400), 1=miler (1400-1800), 2=middle (1800-2400), 3=stayer (>2400)."""
    if distance is None:
        return None
    if distance < 1400:
        return 0
    if distance < 1800:
        return 1
    if distance <= 2400:
        return 2
    return 3


def _experience_level(nb_courses: Optional[int]) -> Optional[int]:
    """0=debutant (<3), 1=novice (3-10), 2=experienced (10-30), 3=veteran (>30)."""
    if nb_courses is None:
        return None
    if nb_courses < 3:
        return 0
    if nb_courses <= 10:
        return 1
    if nb_courses <= 30:
        return 2
    return 3


def _class_bucket(gains: Optional[float]) -> Optional[int]:
    """0=<5K, 1=5-20K, 2=20-80K, 3=80-200K, 4=>200K."""
    if gains is None:
        return None
    if gains < 5_000:
        return 0
    if gains < 20_000:
        return 1
    if gains < 80_000:
        return 2
    if gains < 200_000:
        return 3
    return 4


def _odds_bucket(cote: Optional[float]) -> Optional[int]:
    """0=heavy_fav (<3), 1=fav (3-6), 2=contender (6-12), 3=outsider (12-25), 4=longshot (>25)."""
    if cote is None:
        return None
    if cote < 3:
        return 0
    if cote < 6:
        return 1
    if cote < 12:
        return 2
    if cote < 25:
        return 3
    return 4


def _age_category(age: Optional[int]) -> Optional[int]:
    """0=juvenile (2), 1=classic (3), 2=peak (4-5), 3=mature (6-7), 4=veteran (8+)."""
    if age is None:
        return None
    if age <= 2:
        return 0
    if age == 3:
        return 1
    if age <= 5:
        return 2
    if age <= 7:
        return 3
    return 4


def _field_size_category(nb_partants: Optional[int]) -> Optional[int]:
    """0=small (<8), 1=medium (8-12), 2=large (12-16), 3=massive (>16)."""
    if nb_partants is None:
        return None
    if nb_partants < 8:
        return 0
    if nb_partants <= 12:
        return 1
    if nb_partants <= 16:
        return 2
    return 3


def _weight_category(poids: Optional[float]) -> Optional[int]:
    """0=light (<54), 1=normal (54-58), 2=heavy (58-62), 3=top_weight (>62)."""
    if poids is None:
        return None
    if poids < 54:
        return 0
    if poids <= 58:
        return 1
    if poids <= 62:
        return 2
    return 3


# Discipline mapping for profile hash
_DISCIPLINE_MAP = {
    "plat": 0,
    "trot attele": 1,
    "trot attelé": 1,
    "trot monte": 2,
    "trot monté": 2,
    "obstacle": 3,
}


def _profile_hash(
    discipline: Optional[str],
    distance_type: Optional[int],
    age_cat: Optional[int],
) -> Optional[int]:
    """Combined profile code 0..59 from (discipline[0..3], distance_type[0..3], age_cat[0..4]).

    Formula: discipline_idx * 20 + distance_type * 5 + age_cat
    Max = 3*20 + 3*5 + 4 = 79, but practical range is 0..59 with common values.
    """
    if distance_type is None or age_cat is None:
        return None
    disc_idx = 0
    if discipline is not None:
        disc_norm = discipline.strip().lower()
        disc_idx = _DISCIPLINE_MAP.get(disc_norm, 0)
    return disc_idx * 20 + distance_type * 5 + age_cat


def _is_class_dropper(
    gains: Optional[float], nb_partants: Optional[int]
) -> Optional[int]:
    """1 if gains > 80K and field > 12 (proxy for class dropper)."""
    if gains is None or nb_partants is None:
        return None
    return 1 if gains > 80_000 and nb_partants > 12 else 0


def _competitive_index(
    nb_victoires: Optional[int],
    nb_courses: Optional[int],
    gains: Optional[float],
    age: Optional[int],
) -> Optional[float]:
    """Composite = (nb_victoires/nb_courses) * log(gains+1) / (age+1)."""
    if nb_courses is None or nb_courses <= 0:
        return None
    if nb_victoires is None or gains is None or age is None:
        return None
    win_rate = nb_victoires / nb_courses
    score = win_rate * math.log(gains + 1) / (age + 1)
    return round(score, 6)


# ===========================================================================
# FEATURE KEYS
# ===========================================================================

FEATURE_KEYS = [
    "cls_horse_type",
    "cls_experience_level",
    "cls_class_bucket",
    "cls_odds_bucket",
    "cls_age_category",
    "cls_field_size_category",
    "cls_weight_category",
    "cls_horse_profile_hash",
    "cls_is_class_dropper",
    "cls_competitive_index",
]


# ===========================================================================
# MAIN BUILD (single-pass streaming)
# ===========================================================================


def build_cluster_segmentation_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build cluster segmentation features from partants_master.jsonl.

    Single-pass streaming: read each record, classify, write immediately.
    Returns the total number of feature records written.
    """
    logger.info("=== Cluster Segmentation Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_read = 0
    n_written = 0
    n_errors = 0
    fill_counts = {k: 0 for k in FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_read)
                gc.collect()

            # Extract fields
            distance = _safe_float(rec.get("distance"))
            nb_courses = _safe_int(rec.get("nb_courses_carriere"))
            gains = _safe_float(rec.get("gains_carriere_euros"))
            cote = _safe_float(rec.get("cote_finale"))
            age = _safe_int(rec.get("age"))
            nb_partants = _safe_int(rec.get("nombre_partants"))
            poids = _safe_float(rec.get("poids_porte_kg"))
            discipline = rec.get("discipline")
            nb_victoires = _safe_int(rec.get("nb_victoires_carriere"))

            # Compute features
            dist_type = _horse_type(distance)
            age_cat = _age_category(age)

            feats: dict[str, Any] = {
                "partant_uid": rec.get("partant_uid"),
                "cls_horse_type": dist_type,
                "cls_experience_level": _experience_level(nb_courses),
                "cls_class_bucket": _class_bucket(gains),
                "cls_odds_bucket": _odds_bucket(cote),
                "cls_age_category": age_cat,
                "cls_field_size_category": _field_size_category(nb_partants),
                "cls_weight_category": _weight_category(poids),
                "cls_horse_profile_hash": _profile_hash(discipline, dist_type, age_cat),
                "cls_is_class_dropper": _is_class_dropper(gains, nb_partants),
                "cls_competitive_index": _competitive_index(
                    nb_victoires, nb_courses, gains, age
                ),
            }

            # Track fill rates
            for k in FEATURE_KEYS:
                if feats.get(k) is not None:
                    fill_counts[k] += 1

            fout.write(json.dumps(feats, ensure_ascii=False) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Cluster segmentation build termine: %d features en %.1fs (%d erreurs JSON)",
        n_written, elapsed, n_errors,
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
        description="Construction des features cluster segmentation a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/cluster_segmentation/)",
    )
    args = parser.parse_args()

    logger = setup_logging("cluster_segmentation_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "cluster_segmentation.jsonl"
    build_cluster_segmentation_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
