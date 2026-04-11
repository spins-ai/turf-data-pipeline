#!/usr/bin/env python3
"""
feature_builders.ensemble_meta_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Meta-features for stacking / blending / meta-model modules.

These are features ABOUT the features -- data quality indicators, prediction
difficulty metrics, and context signals that help a meta-model weight
different base models appropriately.

Reads partants_master.jsonl in a single streaming pass and emits 12
per-partant meta-features.

Produces:
  - ensemble_meta_features.jsonl

Features per partant (12):
  - ens_data_completeness      : count of non-null fields out of 30 key
                                  predictive fields / 30
  - ens_odds_available         : 1 if cote_finale is not null
  - ens_speed_available        : 1 if spd_speed_figure is not null
  - ens_pedigree_available     : 1 if ped_has_pedigree is True
  - ens_history_depth          : min(seq_nb_courses_historique, 20) / 20
  - ens_field_size_bucket      : small=0 (<10), medium=1 (10-16), large=2 (>16)
  - ens_discipline_code        : trot_attele=0, trot_monte=1, galop_plat=2,
                                  galop_obstacle=3, other=4
  - ens_is_quinte              : 1 if cnd_cond_is_quinte
  - ens_date_recency           : (current_date - race_date).days
  - ens_market_liquidity       : 1 if rap_nb_gagnants_simple > 1000
  - ens_prediction_difficulty  : nombre_partants * (1 - market_concentration)
  - ens_context_hash           : numeric encoding of (discipline, distance
                                  bucket, hippodrome_frequency)

Usage:
    python feature_builders/ensemble_meta_features_builder.py
    python feature_builders/ensemble_meta_features_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/ensemble_meta_features")

# Progress log / gc every N records
_LOG_EVERY = 500_000

# Current date for ens_date_recency
_TODAY = datetime.now()

# 30 key predictive fields for data_completeness
_KEY_FIELDS = [
    "cote_finale",
    "proba_implicite",
    "spd_speed_figure",
    "spd_class_rating",
    "spd_field_strength_avg",
    "elo_combined",
    "nb_courses_carriere",
    "nb_victoires_carriere",
    "nb_places_carriere",
    "gains_carriere_euros",
    "age",
    "distance",
    "nombre_partants",
    "poids_porte_kg",
    "seq_serie_places",
    "seq_nb_victoires_recent_5",
    "seq_nb_places_recent_5",
    "seq_nb_courses_historique",
    "reduction_km_ms",
    "temps_ms",
    "handicap_valeur",
    "met_impact_meteo_score",
    "ped_stamina_index",
    "ped_speed_index",
    "num_pmu",
    "position_arrivee",
    "rap_market_concentration",
    "spd_class_vs_field",
    "spd_bias_interieur",
    "gnn_duo_jockey_entraineur_win_rate",
]

# Discipline encoding
_DISCIPLINE_MAP = {
    "TROT_ATTELE": 0,
    "ATTELE": 0,
    "TROT ATTELE": 0,
    "TROT_MONTE": 1,
    "MONTE": 1,
    "TROT MONTE": 1,
    "PLAT": 2,
    "GALOP_PLAT": 2,
    "GALOP PLAT": 2,
    "OBSTACLE": 3,
    "GALOP_OBSTACLE": 3,
    "GALOP OBSTACLE": 3,
    "HAIES": 3,
    "STEEPLE": 3,
    "STEEPLE-CHASE": 3,
    "CROSS": 3,
}
_DISCIPLINE_OTHER = 4

# Distance buckets for context hash
_DIST_BUCKETS = [
    (0, 1400, 0),
    (1400, 1800, 1),
    (1800, 2200, 2),
    (2200, 2600, 3),
    (2600, 99999, 4),
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string, return None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _is_non_null(value: Any) -> bool:
    """Check if a value is non-null and non-empty for completeness purposes."""
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert to float, return None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert to int, return None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _discipline_code(raw: Optional[str]) -> int:
    """Map discipline string to numeric code."""
    if not raw:
        return _DISCIPLINE_OTHER
    key = raw.strip().upper()
    return _DISCIPLINE_MAP.get(key, _DISCIPLINE_OTHER)


def _distance_bucket(dist: Optional[float]) -> int:
    """Return distance bucket index (0-4)."""
    if dist is None or dist <= 0:
        return 2  # default: medium distance
    for lo, hi, idx in _DIST_BUCKETS:
        if lo <= dist < hi:
            return idx
    return 4


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
# HIPPODROME FREQUENCY COUNTER (first pass or inline)
# ===========================================================================


def _count_hippodrome_freq(input_path: Path, logger) -> dict[str, int]:
    """Quick first pass to count hippodrome occurrences for context hash."""
    logger.info("Pre-pass: comptage frequences hippodromes...")
    t0 = time.time()
    freq: dict[str, int] = defaultdict(int)
    n = 0
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                hippo = rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
                hippo = hippo.strip().upper()
                if hippo:
                    freq[hippo] += 1
                n += 1
                if n % _LOG_EVERY == 0:
                    logger.info("  Pre-pass: %d records...", n)
            except json.JSONDecodeError:
                pass
    logger.info("Pre-pass termine: %d hippodromes en %.1fs", len(freq), time.time() - t0)
    return dict(freq)


def _hippo_freq_bucket(count: int, total_hippos: int) -> int:
    """Bucket hippodrome by frequency: 0=rare, 1=medium, 2=frequent."""
    if total_hippos == 0:
        return 1
    # Top 20% of hippodromes = frequent, bottom 30% = rare
    if count < 500:
        return 0  # rare
    elif count < 5000:
        return 1  # medium
    else:
        return 2  # frequent


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_ensemble_meta_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build ensemble meta-features in a single streaming pass."""
    logger.info("=== Ensemble Meta-Features Builder ===")
    t0 = time.time()

    # Pre-pass for hippodrome frequencies
    hippo_freq = _count_hippodrome_freq(input_path, logger)

    # -- Main pass: stream and write --
    logger.info("Pass principal: lecture en streaming: %s", input_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0

    # Fill rate counters
    feature_names = [
        "ens_data_completeness",
        "ens_odds_available",
        "ens_speed_available",
        "ens_pedigree_available",
        "ens_history_depth",
        "ens_field_size_bucket",
        "ens_discipline_code",
        "ens_is_quinte",
        "ens_date_recency",
        "ens_market_liquidity",
        "ens_prediction_difficulty",
        "ens_context_hash",
    ]
    fill_counts = {name: 0 for name in feature_names}

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_processed += 1

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid", "")
            date_str = rec.get("date_reunion_iso", "")

            if not partant_uid:
                continue

            # -----------------------------------------------------------
            # 1. ens_data_completeness: non-null count / 30
            # -----------------------------------------------------------
            non_null_count = sum(1 for f in _KEY_FIELDS if _is_non_null(rec.get(f)))
            ens_data_completeness = round(non_null_count / 30.0, 4)

            # -----------------------------------------------------------
            # 2. ens_odds_available
            # -----------------------------------------------------------
            cote = rec.get("cote_finale")
            ens_odds_available = 1 if _is_non_null(cote) else 0

            # -----------------------------------------------------------
            # 3. ens_speed_available
            # -----------------------------------------------------------
            spd = rec.get("spd_speed_figure")
            ens_speed_available = 1 if _is_non_null(spd) else 0

            # -----------------------------------------------------------
            # 4. ens_pedigree_available
            # -----------------------------------------------------------
            ped = rec.get("ped_has_pedigree")
            ens_pedigree_available = 1 if ped is True else 0

            # -----------------------------------------------------------
            # 5. ens_history_depth
            # -----------------------------------------------------------
            hist_raw = _safe_int(rec.get("seq_nb_courses_historique"))
            if hist_raw is not None:
                ens_history_depth = round(min(hist_raw, 20) / 20.0, 4)
            else:
                ens_history_depth = None

            # -----------------------------------------------------------
            # 6. ens_field_size_bucket
            # -----------------------------------------------------------
            nb_partants = _safe_int(rec.get("nombre_partants"))
            if nb_partants is not None and nb_partants > 0:
                if nb_partants < 10:
                    ens_field_size_bucket = 0  # small
                elif nb_partants <= 16:
                    ens_field_size_bucket = 1  # medium
                else:
                    ens_field_size_bucket = 2  # large
            else:
                ens_field_size_bucket = None

            # -----------------------------------------------------------
            # 7. ens_discipline_code
            # -----------------------------------------------------------
            discipline_raw = rec.get("discipline") or rec.get("type_course") or ""
            ens_discipline_code = _discipline_code(discipline_raw)

            # -----------------------------------------------------------
            # 8. ens_is_quinte
            # -----------------------------------------------------------
            is_quinte = rec.get("cnd_cond_is_quinte")
            ens_is_quinte = 1 if is_quinte else 0

            # -----------------------------------------------------------
            # 9. ens_date_recency
            # -----------------------------------------------------------
            race_date = _parse_date(date_str)
            if race_date is not None:
                ens_date_recency = (_TODAY - race_date).days
            else:
                ens_date_recency = None

            # -----------------------------------------------------------
            # 10. ens_market_liquidity
            # -----------------------------------------------------------
            nb_gagnants = _safe_int(rec.get("rap_nb_gagnants_simple"))
            if nb_gagnants is not None:
                ens_market_liquidity = 1 if nb_gagnants > 1000 else 0
            else:
                ens_market_liquidity = None

            # -----------------------------------------------------------
            # 11. ens_prediction_difficulty
            # -----------------------------------------------------------
            mkt_conc = _safe_float(rec.get("rap_market_concentration"))
            if nb_partants is not None and nb_partants > 0 and mkt_conc is not None:
                ens_prediction_difficulty = round(
                    nb_partants * (1.0 - mkt_conc), 4
                )
            else:
                ens_prediction_difficulty = None

            # -----------------------------------------------------------
            # 12. ens_context_hash
            # -----------------------------------------------------------
            dist_val = _safe_float(rec.get("distance"))
            dist_bucket = _distance_bucket(dist_val)
            hippo_raw = (
                rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
            )
            hippo_raw = hippo_raw.strip().upper()
            hippo_count = hippo_freq.get(hippo_raw, 0)
            hippo_bucket = _hippo_freq_bucket(hippo_count, len(hippo_freq))
            # Encode: discipline * 15 + dist_bucket * 3 + hippo_bucket
            ens_context_hash = ens_discipline_code * 15 + dist_bucket * 3 + hippo_bucket

            # -----------------------------------------------------------
            # Build output record
            # -----------------------------------------------------------
            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
                "ens_data_completeness": ens_data_completeness,
                "ens_odds_available": ens_odds_available,
                "ens_speed_available": ens_speed_available,
                "ens_pedigree_available": ens_pedigree_available,
                "ens_history_depth": ens_history_depth,
                "ens_field_size_bucket": ens_field_size_bucket,
                "ens_discipline_code": ens_discipline_code,
                "ens_is_quinte": ens_is_quinte,
                "ens_date_recency": ens_date_recency,
                "ens_market_liquidity": ens_market_liquidity,
                "ens_prediction_difficulty": ens_prediction_difficulty,
                "ens_context_hash": ens_context_hash,
            }

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_written += 1

            # Fill rate tracking
            for fname in feature_names:
                if _is_non_null(out.get(fname)):
                    fill_counts[fname] += 1

            # Periodic logging and gc
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traite %d records, ecrit %d...", n_processed, n_written)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Ensemble meta-features build termine: %d features en %.1fs",
        n_written, elapsed,
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
        description="Construction des meta-features ensemble a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/ensemble_meta_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("ensemble_meta_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "ensemble_meta_features.jsonl"
    build_ensemble_meta_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
