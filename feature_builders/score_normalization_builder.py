#!/usr/bin/env python3
"""
feature_builders.score_normalization_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Score normalization features -- normalizing various raw metrics into
comparable scales (z-score, min-max, rank percentile, ratio).

Two-pass approach:
  Pass 1: compute global distributions (running sums, counts, min, max)
           for each raw field across the entire dataset.
  Pass 2: normalize each record using the global statistics and stream
           output to disk.

Produces:
  - score_normalization.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/score_normalization/

Features per partant (10):
  - snm_gains_zscore       : (gains_carriere - global_mean) / global_std
  - snm_age_zscore         : (age - global_mean) / global_std
  - snm_cote_log_zscore    : (log(cote) - global_mean_log) / global_std_log
  - snm_nb_courses_zscore  : (nb_courses - global_mean) / global_std
  - snm_poids_zscore       : (poids_porte - global_mean) / global_std
  - snm_gains_minmax       : (gains - global_min) / (global_max - global_min)
  - snm_cote_rank_pct      : rank of cote_finale / total records (global percentile)
  - snm_wins_per_100       : (nb_victoires / nb_courses) * 100 (win rate per 100 starts)
  - snm_distance_zscore    : (distance - global_mean) / global_std
  - snm_field_size_zscore  : (nombre_partants - global_mean) / global_std

Usage:
    python feature_builders/score_normalization_builder.py
    python feature_builders/score_normalization_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/score_normalization")

# Progress / gc every N records
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
        return v if math.isfinite(v) else None
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


# ===========================================================================
# RUNNING STATISTICS ACCUMULATOR
# ===========================================================================


class _RunningStats:
    """Welford's online algorithm for mean / variance + min / max tracking."""

    __slots__ = ("n", "mean", "m2", "min_val", "max_val")

    def __init__(self) -> None:
        self.n: int = 0
        self.mean: float = 0.0
        self.m2: float = 0.0
        self.min_val: float = float("inf")
        self.max_val: float = float("-inf")

    def update(self, x: float) -> None:
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2
        if x < self.min_val:
            self.min_val = x
        if x > self.max_val:
            self.max_val = x

    @property
    def std(self) -> float:
        if self.n < 2:
            return 0.0
        return math.sqrt(self.m2 / self.n)

    @property
    def range(self) -> float:
        if self.n == 0:
            return 0.0
        return self.max_val - self.min_val


# ===========================================================================
# MAIN BUILD (two-pass: global stats then normalize)
# ===========================================================================


def build_score_normalization_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build score normalization features from partants_master.jsonl.

    Two-pass approach:
      Pass 1: stream all records to compute global distributions
              (mean, std, min, max for each raw field; collect cote values
              for rank percentile).
      Pass 2: re-read records, normalize each one using global stats,
              stream output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Score Normalization Builder (two-pass) ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # -- Pass 1: Compute global distributions --
    logger.info("Pass 1: computing global distributions...")

    stats_gains = _RunningStats()
    stats_age = _RunningStats()
    stats_log_cote = _RunningStats()
    stats_nb_courses = _RunningStats()
    stats_poids = _RunningStats()
    stats_distance = _RunningStats()
    stats_field_size = _RunningStats()

    # For cote rank percentile: collect all cote values (sorted later)
    all_cotes: list[float] = []

    n_pass1 = 0
    errors_pass1 = 0

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                errors_pass1 += 1
                if errors_pass1 <= 10:
                    logger.warning("Pass 1: ligne JSON invalide (erreur %d)", errors_pass1)
                continue

            n_pass1 += 1
            if n_pass1 % _LOG_EVERY == 0:
                logger.info("  Pass 1: %d records...", n_pass1)
                gc.collect()

            # gains_carriere
            g = _safe_float(rec.get("gains_carriere_euros"))
            if g is not None:
                stats_gains.update(g)

            # age
            a = _safe_float(rec.get("age"))
            if a is not None:
                stats_age.update(a)

            # log(cote)
            cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference"))
            if cote is not None and cote > 0:
                stats_log_cote.update(math.log(cote))
                all_cotes.append(cote)

            # nb_courses
            nc = _safe_float(rec.get("nombre_courses"))
            if nc is not None:
                stats_nb_courses.update(nc)

            # poids_porte
            p = _safe_float(rec.get("poids_porte"))
            if p is not None:
                stats_poids.update(p)

            # distance
            d = _safe_float(rec.get("distance"))
            if d is not None:
                stats_distance.update(d)

            # nombre_partants (field size)
            fs = _safe_float(rec.get("nombre_partants"))
            if fs is not None:
                stats_field_size.update(fs)

    logger.info(
        "Pass 1 terminee: %d records, %d erreurs JSON en %.1fs",
        n_pass1, errors_pass1, time.time() - t0,
    )

    # Sort cotes for rank percentile lookup
    t_sort = time.time()
    all_cotes.sort()
    total_cotes = len(all_cotes)
    logger.info(
        "Cotes triees pour percentile: %d valeurs en %.1fs",
        total_cotes, time.time() - t_sort,
    )

    # Log global stats summary
    logger.info("Global stats summary:")
    logger.info("  gains:      n=%d mean=%.2f std=%.2f", stats_gains.n, stats_gains.mean, stats_gains.std)
    logger.info("  age:        n=%d mean=%.2f std=%.2f", stats_age.n, stats_age.mean, stats_age.std)
    logger.info("  log_cote:   n=%d mean=%.4f std=%.4f", stats_log_cote.n, stats_log_cote.mean, stats_log_cote.std)
    logger.info("  nb_courses: n=%d mean=%.2f std=%.2f", stats_nb_courses.n, stats_nb_courses.mean, stats_nb_courses.std)
    logger.info("  poids:      n=%d mean=%.2f std=%.2f", stats_poids.n, stats_poids.mean, stats_poids.std)
    logger.info("  distance:   n=%d mean=%.2f std=%.2f", stats_distance.n, stats_distance.mean, stats_distance.std)
    logger.info("  field_size: n=%d mean=%.2f std=%.2f", stats_field_size.n, stats_field_size.mean, stats_field_size.std)

    gc.collect()

    # -- Pass 2: Normalize each record and write output --
    logger.info("Pass 2: normalizing records...")
    t2 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    feature_names = [
        "snm_gains_zscore",
        "snm_age_zscore",
        "snm_cote_log_zscore",
        "snm_nb_courses_zscore",
        "snm_poids_zscore",
        "snm_gains_minmax",
        "snm_cote_rank_pct",
        "snm_wins_per_100",
        "snm_distance_zscore",
        "snm_field_size_zscore",
    ]
    fill_counts = {k: 0 for k in feature_names}

    n_pass2 = 0
    n_written = 0
    errors_pass2 = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                errors_pass2 += 1
                continue

            n_pass2 += 1
            if n_pass2 % _LOG_EVERY == 0:
                logger.info("  Pass 2: %d records...", n_pass2)
                gc.collect()

            partant_uid = rec.get("partant_uid") or ""

            # Extract raw values
            gains = _safe_float(rec.get("gains_carriere_euros"))
            age = _safe_float(rec.get("age"))
            cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("cote_reference"))
            nb_courses = _safe_float(rec.get("nombre_courses"))
            poids = _safe_float(rec.get("poids_porte"))
            nb_victoires = _safe_float(rec.get("nombre_victoires"))
            distance = _safe_float(rec.get("distance"))
            field_size = _safe_float(rec.get("nombre_partants"))

            feats: dict[str, Any] = {"partant_uid": partant_uid}

            # 1. snm_gains_zscore
            if gains is not None and stats_gains.std > 0:
                feats["snm_gains_zscore"] = round((gains - stats_gains.mean) / stats_gains.std, 4)
            else:
                feats["snm_gains_zscore"] = None

            # 2. snm_age_zscore
            if age is not None and stats_age.std > 0:
                feats["snm_age_zscore"] = round((age - stats_age.mean) / stats_age.std, 4)
            else:
                feats["snm_age_zscore"] = None

            # 3. snm_cote_log_zscore
            if cote is not None and cote > 0 and stats_log_cote.std > 0:
                log_cote = math.log(cote)
                feats["snm_cote_log_zscore"] = round(
                    (log_cote - stats_log_cote.mean) / stats_log_cote.std, 4
                )
            else:
                feats["snm_cote_log_zscore"] = None

            # 4. snm_nb_courses_zscore
            if nb_courses is not None and stats_nb_courses.std > 0:
                feats["snm_nb_courses_zscore"] = round(
                    (nb_courses - stats_nb_courses.mean) / stats_nb_courses.std, 4
                )
            else:
                feats["snm_nb_courses_zscore"] = None

            # 5. snm_poids_zscore
            if poids is not None and stats_poids.std > 0:
                feats["snm_poids_zscore"] = round(
                    (poids - stats_poids.mean) / stats_poids.std, 4
                )
            else:
                feats["snm_poids_zscore"] = None

            # 6. snm_gains_minmax
            if gains is not None and stats_gains.range > 0:
                feats["snm_gains_minmax"] = round(
                    (gains - stats_gains.min_val) / stats_gains.range, 6
                )
            else:
                feats["snm_gains_minmax"] = None

            # 7. snm_cote_rank_pct (global percentile via bisect on sorted cotes)
            if cote is not None and cote > 0 and total_cotes > 0:
                # Binary search for rank position
                lo, hi = 0, total_cotes
                while lo < hi:
                    mid = (lo + hi) // 2
                    if all_cotes[mid] < cote:
                        lo = mid + 1
                    else:
                        hi = mid
                feats["snm_cote_rank_pct"] = round(lo / total_cotes, 6)
            else:
                feats["snm_cote_rank_pct"] = None

            # 8. snm_wins_per_100
            if nb_victoires is not None and nb_courses is not None and nb_courses > 0:
                feats["snm_wins_per_100"] = round((nb_victoires / nb_courses) * 100, 2)
            else:
                feats["snm_wins_per_100"] = None

            # 9. snm_distance_zscore
            if distance is not None and stats_distance.std > 0:
                feats["snm_distance_zscore"] = round(
                    (distance - stats_distance.mean) / stats_distance.std, 4
                )
            else:
                feats["snm_distance_zscore"] = None

            # 10. snm_field_size_zscore
            if field_size is not None and stats_field_size.std > 0:
                feats["snm_field_size_zscore"] = round(
                    (field_size - stats_field_size.mean) / stats_field_size.std, 4
                )
            else:
                feats["snm_field_size_zscore"] = None

            # Track fill rates
            for fn in feature_names:
                if feats.get(fn) is not None:
                    fill_counts[fn] += 1

            fout.write(json.dumps(feats, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Score normalization build termine: %d features en %.1fs",
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Score normalization: z-scores, min-max, rank percentiles"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/score_normalization/)",
    )
    args = parser.parse_args()

    logger = setup_logging("score_normalization_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "score_normalization.jsonl"
    build_score_normalization_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
