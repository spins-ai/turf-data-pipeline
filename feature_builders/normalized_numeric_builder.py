#!/usr/bin/env python3
"""
feature_builders.normalized_numeric_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Normalized / standardized versions of key numeric fields.

Two-pass streaming builder:
  Pass 1 -- scan the full file to compute global statistics
            (min, max, mean, std, 99th-percentile) for each field.
  Pass 2 -- stream again, apply normalization, emit one row per partant.

No temporal leakage risk: all statistics are population-level descriptives
computed from the static fields available before a race starts (cote, gains,
nb_courses, poids, distance, allocation, age, nombre_partants).

Produces:
  - normalized_numeric.jsonl   in output/normalized_numeric/

Features (10):
  - nn_cote_minmax         : min-max normalized cote_finale (0-1, clipped to
                             99th-percentile cap before scaling)
  - nn_gains_minmax        : min-max normalized gains_carriere (0-1)
  - nn_experience_minmax   : min-max normalized nb_courses (0-1)
  - nn_weight_zscore       : z-score of poids_porte
  - nn_distance_zscore     : z-score of distance
  - nn_allocation_zscore   : z-score of allocation
  - nn_age_minmax          : min-max normalized age (fixed range 2-12)
  - nn_cote_zscore         : z-score of cote_finale
  - nn_gains_zscore        : z-score of gains_carriere
  - nn_field_size_minmax   : min-max normalized nombre_partants

Usage:
    python feature_builders/normalized_numeric_builder.py
    python feature_builders/normalized_numeric_builder.py \\
        --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path(
    "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"
)
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/normalized_numeric"
)

_LOG_EVERY = 500_000

# Fixed age normalization range (domain knowledge: horses race between 2-12 yo)
_AGE_MIN = 2.0
_AGE_MAX = 12.0

# Percentile cap for cote (cap outlier odds before min-max scaling)
_COTE_PERCENTILE_CAP = 0.99

# Fallback input candidates (relative to project root, used when main path absent)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

# Fields tracked in pass 1
# key -> list of raw field names to try (first non-None wins)
_FIELD_ALIASES: Dict[str, List[str]] = {
    "cote": [
        "cote_finale",
        "cote_probable",
        "rapport_final",
        "rapport_pmu",
    ],
    "gains": [
        "gains_carriere_euros",
        "gains_carriere",
        "gains_total",
        "gains_prix_euros",
        "gainsCarriere",
        "gains",
    ],
    "nb_courses": [
        "nb_courses_carriere",
        "nb_courses",
        "nombreCourses",
    ],
    "poids": [
        "poids_porte_kg",
        "poids_porte",
        "poids",
    ],
    "distance": [
        "distance",
        "distanceCourse",
        "distance_metres",
    ],
    "allocation": [
        "allocation",
        "dotation",
        "montant_prix",
    ],
    "age": [
        "age",
        "age_chevaux",
    ],
    "nombre_partants": [
        "nombre_partants",
        "nb_partants",
        "nombrePartants",
        "partants",
    ],
}


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    """Convert value to float, return None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _extract(rec: dict, field_key: str) -> Optional[float]:
    """Extract the first non-None float for a given field key using aliases."""
    for alias in _FIELD_ALIASES[field_key]:
        v = _safe_float(rec.get(alias))
        if v is not None:
            return v
    return None


def _percentile_sorted(sorted_vals: List[float], p: float) -> float:
    """Compute percentile p (0-1) from a pre-sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return float("nan")
    if n == 1:
        return sorted_vals[0]
    pos = p * (n - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


# ===========================================================================
# ONLINE STATS ACCUMULATOR (Welford's algorithm for mean/var)
# ===========================================================================


class _Stats:
    """Accumulate count, min, max, running mean and variance (Welford)."""

    __slots__ = ("count", "min", "max", "_mean", "_M2", "_values_for_pct")

    def __init__(self, store_values: bool = False) -> None:
        self.count: int = 0
        self.min: float = float("inf")
        self.max: float = float("-inf")
        self._mean: float = 0.0
        self._M2: float = 0.0
        # Only kept when we need percentile computation
        self._values_for_pct: Optional[List[float]] = [] if store_values else None

    def update(self, x: float) -> None:
        self.count += 1
        if x < self.min:
            self.min = x
        if x > self.max:
            self.max = x
        # Welford online update
        delta = x - self._mean
        self._mean += delta / self.count
        delta2 = x - self._mean
        self._M2 += delta * delta2
        if self._values_for_pct is not None:
            self._values_for_pct.append(x)

    @property
    def mean(self) -> float:
        return self._mean if self.count > 0 else float("nan")

    @property
    def std(self) -> float:
        if self.count < 2:
            return float("nan")
        return math.sqrt(self._M2 / (self.count - 1))

    def percentile(self, p: float) -> float:
        """Compute percentile (requires store_values=True)."""
        if self._values_for_pct is None:
            raise RuntimeError("store_values must be True to compute percentile")
        if not self._values_for_pct:
            return float("nan")
        sorted_v = sorted(self._values_for_pct)
        return _percentile_sorted(sorted_v, p)

    def summary(self) -> str:
        return (
            f"n={self.count}, min={self.min:.4g}, max={self.max:.4g}, "
            f"mean={self.mean:.4g}, std={self.std:.4g}"
        )


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger, pass_label: str = ""):
    """Yield dicts from a JSONL file one line at a time."""
    count = 0
    errors = 0
    label = f"[{pass_label}] " if pass_label else ""
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("%sLigne JSON invalide ignoree (erreur %d)", label, errors)
    logger.info(
        "%sLecture terminee: %d records, %d erreurs JSON", label, count, errors
    )


# ===========================================================================
# PASS 1 -- COLLECT GLOBAL STATISTICS
# ===========================================================================


def _pass1_collect_stats(
    input_path: Path, logger
) -> Dict[str, _Stats]:
    """First pass: compute global statistics for all numeric fields."""
    logger.info("=== Passe 1 : collecte des statistiques globales ===")
    t0 = time.time()

    stats: Dict[str, _Stats] = {
        # cote needs percentile computation
        "cote": _Stats(store_values=True),
        "gains": _Stats(store_values=False),
        "nb_courses": _Stats(store_values=False),
        "poids": _Stats(store_values=False),
        "distance": _Stats(store_values=False),
        "allocation": _Stats(store_values=False),
        "age": _Stats(store_values=False),
        "nombre_partants": _Stats(store_values=False),
    }

    n = 0
    for rec in _iter_jsonl(input_path, logger, pass_label="Passe1"):
        n += 1
        if n % _LOG_EVERY == 0:
            logger.info("  Passe 1 : %d records traites...", n)

        for field_key, stat in stats.items():
            v = _extract(rec, field_key)
            if v is not None:
                # Basic sanity filters
                if field_key == "cote" and v <= 0:
                    continue
                if field_key == "age" and (v < 0 or v > 30):
                    continue
                if field_key == "poids" and (v <= 0 or v > 200):
                    continue
                if field_key == "distance" and (v <= 0 or v > 10000):
                    continue
                if field_key == "nombre_partants" and (v <= 0 or v > 50):
                    continue
                if field_key == "nb_courses" and v < 0:
                    continue
                if field_key == "gains" and v < 0:
                    continue
                stat.update(v)

    elapsed = time.time() - t0
    logger.info("Passe 1 terminee : %d records en %.1fs", n, elapsed)

    for field_key, stat in stats.items():
        logger.info("  %s : %s", field_key, stat.summary())

    return stats


# ===========================================================================
# DERIVED NORMALIZATION PARAMETERS
# ===========================================================================


class _NormParams:
    """Pre-computed normalization parameters from pass-1 stats."""

    def __init__(self, stats: Dict[str, _Stats]) -> None:
        s = stats

        # --- cote ---
        cote_s = s["cote"]
        self.cote_mean: float = cote_s.mean
        self.cote_std: float = cote_s.std
        # cap cote at 99th percentile for min-max
        if cote_s.count > 0:
            self.cote_cap = cote_s.percentile(_COTE_PERCENTILE_CAP)
        else:
            self.cote_cap = cote_s.max
        self.cote_min: float = cote_s.min
        # min-max range uses [cote_min, cote_cap]
        self.cote_mm_range: float = self.cote_cap - self.cote_min

        # --- gains ---
        gains_s = s["gains"]
        self.gains_mean: float = gains_s.mean
        self.gains_std: float = gains_s.std
        self.gains_min: float = gains_s.min
        self.gains_max: float = gains_s.max
        self.gains_mm_range: float = self.gains_max - self.gains_min

        # --- nb_courses ---
        exp_s = s["nb_courses"]
        self.exp_min: float = exp_s.min
        self.exp_max: float = exp_s.max
        self.exp_mm_range: float = self.exp_max - self.exp_min

        # --- poids ---
        poids_s = s["poids"]
        self.poids_mean: float = poids_s.mean
        self.poids_std: float = poids_s.std

        # --- distance ---
        dist_s = s["distance"]
        self.dist_mean: float = dist_s.mean
        self.dist_std: float = dist_s.std

        # --- allocation ---
        alloc_s = s["allocation"]
        self.alloc_mean: float = alloc_s.mean
        self.alloc_std: float = alloc_s.std

        # --- age (fixed domain range 2-12) ---
        self.age_min: float = _AGE_MIN
        self.age_max: float = _AGE_MAX
        self.age_mm_range: float = _AGE_MAX - _AGE_MIN

        # --- nombre_partants ---
        np_s = s["nombre_partants"]
        self.np_min: float = np_s.min
        self.np_max: float = np_s.max
        self.np_mm_range: float = self.np_max - self.np_min

    # --- Normalization helpers ---

    @staticmethod
    def _minmax(x: float, mn: float, rng: float) -> Optional[float]:
        """Min-max scale, capped to [0, 1]."""
        if rng == 0 or math.isnan(rng):
            return None
        v = (x - mn) / rng
        return round(max(0.0, min(1.0, v)), 6)

    @staticmethod
    def _zscore(x: float, mean: float, std: float) -> Optional[float]:
        """Z-score normalization."""
        if std == 0 or math.isnan(std) or math.isnan(mean):
            return None
        return round((x - mean) / std, 6)

    # --- Feature computation ---

    def nn_cote_minmax(self, cote: Optional[float]) -> Optional[float]:
        if cote is None or cote <= 0:
            return None
        capped = min(cote, self.cote_cap)
        return self._minmax(capped, self.cote_min, self.cote_mm_range)

    def nn_gains_minmax(self, gains: Optional[float]) -> Optional[float]:
        if gains is None:
            return None
        return self._minmax(gains, self.gains_min, self.gains_mm_range)

    def nn_experience_minmax(self, nb: Optional[float]) -> Optional[float]:
        if nb is None:
            return None
        return self._minmax(nb, self.exp_min, self.exp_mm_range)

    def nn_weight_zscore(self, poids: Optional[float]) -> Optional[float]:
        if poids is None:
            return None
        return self._zscore(poids, self.poids_mean, self.poids_std)

    def nn_distance_zscore(self, dist: Optional[float]) -> Optional[float]:
        if dist is None:
            return None
        return self._zscore(dist, self.dist_mean, self.dist_std)

    def nn_allocation_zscore(self, alloc: Optional[float]) -> Optional[float]:
        if alloc is None:
            return None
        return self._zscore(alloc, self.alloc_mean, self.alloc_std)

    def nn_age_minmax(self, age: Optional[float]) -> Optional[float]:
        if age is None:
            return None
        return self._minmax(age, self.age_min, self.age_mm_range)

    def nn_cote_zscore(self, cote: Optional[float]) -> Optional[float]:
        if cote is None or cote <= 0:
            return None
        return self._zscore(cote, self.cote_mean, self.cote_std)

    def nn_gains_zscore(self, gains: Optional[float]) -> Optional[float]:
        if gains is None:
            return None
        return self._zscore(gains, self.gains_mean, self.gains_std)

    def nn_field_size_minmax(self, np_val: Optional[float]) -> Optional[float]:
        if np_val is None:
            return None
        return self._minmax(np_val, self.np_min, self.np_mm_range)


# ===========================================================================
# PASS 2 -- APPLY NORMALIZATION
# ===========================================================================


def _pass2_apply(
    input_path: Path,
    params: _NormParams,
    logger,
) -> List[Dict[str, Any]]:
    """Second pass: stream records and apply normalization."""
    logger.info("=== Passe 2 : application de la normalisation ===")
    t0 = time.time()

    results: List[Dict[str, Any]] = []
    n = 0

    for rec in _iter_jsonl(input_path, logger, pass_label="Passe2"):
        n += 1
        if n % _LOG_EVERY == 0:
            logger.info("  Passe 2 : %d records traites...", n)

        partant_uid = rec.get("partant_uid")
        course_uid = rec.get("course_uid")
        date_reunion = rec.get("date_reunion_iso")

        cote = _extract(rec, "cote")
        gains = _extract(rec, "gains")
        nb = _extract(rec, "nb_courses")
        poids = _extract(rec, "poids")
        dist = _extract(rec, "distance")
        alloc = _extract(rec, "allocation")
        age = _extract(rec, "age")
        np_val = _extract(rec, "nombre_partants")

        results.append({
            "partant_uid": partant_uid,
            "course_uid": course_uid,
            "date_reunion_iso": date_reunion,
            "nn_cote_minmax": params.nn_cote_minmax(cote),
            "nn_gains_minmax": params.nn_gains_minmax(gains),
            "nn_experience_minmax": params.nn_experience_minmax(nb),
            "nn_weight_zscore": params.nn_weight_zscore(poids),
            "nn_distance_zscore": params.nn_distance_zscore(dist),
            "nn_allocation_zscore": params.nn_allocation_zscore(alloc),
            "nn_age_minmax": params.nn_age_minmax(age),
            "nn_cote_zscore": params.nn_cote_zscore(cote),
            "nn_gains_zscore": params.nn_gains_zscore(gains),
            "nn_field_size_minmax": params.nn_field_size_minmax(np_val),
        })

    elapsed = time.time() - t0
    logger.info("Passe 2 terminee : %d records en %.1fs", n, elapsed)
    return results


# ===========================================================================
# ORCHESTRATOR
# ===========================================================================


def build_normalized_numeric(
    input_path: Path, logger
) -> List[Dict[str, Any]]:
    """Full two-pass build: statistics then normalization."""
    logger.info("=== Normalized Numeric Builder ===")
    logger.info("Fichier source : %s", input_path)
    t_total = time.time()

    # Pass 1
    stats = _pass1_collect_stats(input_path, logger)

    # Derive normalization parameters
    params = _NormParams(stats)
    logger.info(
        "Params cote : min=%.4g, cap(p99)=%.4g, mean=%.4g, std=%.4g",
        params.cote_min, params.cote_cap, params.cote_mean, params.cote_std,
    )
    logger.info(
        "Params gains : min=%.4g, max=%.4g, mean=%.4g, std=%.4g",
        params.gains_min, params.gains_max, params.gains_mean, params.gains_std,
    )
    logger.info(
        "Params distance : mean=%.4g, std=%.4g",
        params.dist_mean, params.dist_std,
    )

    # Free pass-1 value lists before pass 2
    del stats
    gc.collect()

    # Pass 2
    results = _pass2_apply(input_path, params, logger)

    total_elapsed = time.time() - t_total
    logger.info(
        "Build total termine : %d features en %.1fs", len(results), total_elapsed
    )
    return results


# ===========================================================================
# INPUT RESOLUTION
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable : {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve parmi : "
        + str([str(c) for c in _INPUT_CANDIDATES])
    )


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features numeriques normalisees "
            "a partir de partants_master (two-pass, streaming)"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut : auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut : D:/turf-data-pipeline/...)",
    )
    args = parser.parse_args()

    logger = setup_logging("normalized_numeric_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_normalized_numeric(input_path, logger)

    out_path = output_dir / "normalized_numeric.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rate summary
    if results:
        feature_keys = [
            k for k in results[0]
            if k not in ("partant_uid", "course_uid", "date_reunion_iso")
        ]
        filled = {k: 0 for k in feature_keys}
        for row in results:
            for k in feature_keys:
                if row.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s : %d/%d (%.1f%%)", k, v, total, 100.0 * v / total)


if __name__ == "__main__":
    main()
