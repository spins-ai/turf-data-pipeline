#!/usr/bin/env python3
"""
feature_builders.bayesian_uncertainty_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Bayesian uncertainty and confidence features for the Bayesian Neural Network
module -- measuring epistemic/aleatoric uncertainty and prediction confidence.

Reads partants_master.jsonl via index + chronological sort + seek architecture.

Temporal integrity: for any partant at date D, only races with date < D
contribute to computed features -- no future leakage.

Produces:
  - bayesian_uncertainty_features.jsonl  in builder_outputs/bayesian_uncertainty/

Features per partant (10):
  - bay_horse_sample_size        : number of past races (more data = less uncertainty)
  - bay_horse_confidence         : 1 - 1/(sqrt(sample_size) + 1)
  - bay_prior_strength           : how much to trust prior vs evidence = min(1, 10/sample_size)
  - bay_posterior_win_rate       : Bayesian posterior win rate with discipline prior
  - bay_posterior_place_rate     : Bayesian posterior place rate with discipline prior
  - bay_credible_interval_width  : approximate 95% CI width = 4*sqrt(p*(1-p)/n)
  - bay_epistemic_uncertainty    : uncertainty from lack of data = 1/sqrt(n+1)
  - bay_aleatoric_uncertainty    : inherent randomness = position_std / mean_position (CV)
  - bay_information_gain         : bits gained = log2(1/prior) - log2(1/posterior)
  - bay_prediction_entropy       : binary entropy = -p*log(p) - (1-p)*log(1-p)

Usage:
    python feature_builders/bayesian_uncertainty_builder.py
    python feature_builders/bayesian_uncertainty_builder.py --input path/to/partants_master.jsonl
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
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/bayesian_uncertainty")

# Bayesian prior pseudo-count (controls shrinkage strength)
PRIOR_PSEUDO_COUNT = 10

# Progress / GC frequency
_LOG_EVERY = 500_000

# Epsilon to avoid log(0)
_EPS = 1e-10

# ===========================================================================
# FEATURE NAMES
# ===========================================================================

_FEATURE_NAMES = [
    "bay_horse_sample_size",
    "bay_horse_confidence",
    "bay_prior_strength",
    "bay_posterior_win_rate",
    "bay_posterior_place_rate",
    "bay_credible_interval_width",
    "bay_epistemic_uncertainty",
    "bay_aleatoric_uncertainty",
    "bay_information_gain",
    "bay_prediction_entropy",
]


# ===========================================================================
# HORSE STATE TRACKER
# ===========================================================================


class _HorseState:
    """Per-horse state for Bayesian features.

    Tracks wins, places, total races, and positions (for std calculation).
    Uses online Welford algorithm for numerically stable variance.
    """

    __slots__ = ("wins", "places", "total", "pos_mean", "pos_m2")

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.total: int = 0
        # Welford online variance for positions
        self.pos_mean: float = 0.0
        self.pos_m2: float = 0.0

    def update(self, is_winner: bool, is_place: bool, position: Optional[int]) -> None:
        """Update state AFTER computing features (post-race)."""
        self.total += 1
        if is_winner:
            self.wins += 1
        if is_place:
            self.places += 1
        if position is not None and position > 0:
            # Welford online update
            delta = position - self.pos_mean
            self.pos_mean += delta / self.total
            delta2 = position - self.pos_mean
            self.pos_m2 += delta * delta2

    def position_std(self) -> Optional[float]:
        """Population std of finishing positions."""
        if self.total < 2:
            return None
        var = self.pos_m2 / self.total
        if var < 0:
            return None
        return math.sqrt(var)


# ===========================================================================
# GLOBAL PRIOR TRACKER (per discipline)
# ===========================================================================


class _DisciplinePrior:
    """Tracks global win/place rates per discipline for Bayesian priors."""

    __slots__ = ("wins", "places", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.total: int = 0

    def win_rate(self) -> float:
        if self.total == 0:
            return 0.08  # fallback ~1/12
        return self.wins / self.total

    def place_rate(self) -> float:
        if self.total == 0:
            return 0.25  # fallback ~3/12
        return self.places / self.total


# ===========================================================================
# MATH HELPERS
# ===========================================================================


def _binary_entropy(p: float) -> Optional[float]:
    """Binary entropy: -p*log2(p) - (1-p)*log2(1-p). Returns None if degenerate."""
    if p <= _EPS or p >= 1.0 - _EPS:
        return 0.0
    return -(p * math.log2(p) + (1 - p) * math.log2(1 - p))


def _information_gain(prior_p: float, posterior_p: float) -> Optional[float]:
    """Bits of information gained: log2(1/prior) - log2(1/posterior)."""
    if prior_p <= _EPS or posterior_p <= _EPS:
        return None
    return math.log2(1.0 / prior_p) - math.log2(1.0 / posterior_p)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_bayesian_uncertainty_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build Bayesian uncertainty features from partants_master.jsonl.

    Architecture: index + chronological sort + seek.
      1. Read only sort keys + byte offsets into memory.
      2. Sort chronologically.
      3. Process course by course, seek to read full records, stream output.

    Returns the total number of feature records written.
    """
    logger.info("=== Bayesian Uncertainty Builder (index + sort + seek) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)
    disc_prior: dict[str, _DisciplinePrior] = defaultdict(_DisciplinePrior)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {name: 0 for name in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            position = rec.get("place_officielle") or rec.get("position") or None
            if position is not None:
                try:
                    position = int(position)
                except (ValueError, TypeError):
                    position = None

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            is_place = bool(rec.get("is_place"))
            if not is_place and position is not None:
                is_place = position <= 3

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "cheval": rec.get("nom_cheval"),
                "gagnant": bool(rec.get("is_gagnant")),
                "place": is_place,
                "position": position,
                "discipline": discipline,
            }

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read this course's records from disk
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                discipline = rec["discipline"]

                features: dict[str, Any] = {
                    "partant_uid": rec["uid"],
                    "course_uid": rec["course"],
                    "date_reunion_iso": rec["date"],
                }

                if not cheval:
                    # No horse name -> all None
                    for name in _FEATURE_NAMES:
                        features[name] = None
                    fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[cheval]
                dp = disc_prior.get(discipline)
                n = hs.total

                # -- bay_horse_sample_size --
                features["bay_horse_sample_size"] = n
                if n > 0:
                    fill_counts["bay_horse_sample_size"] += 1

                # -- bay_horse_confidence --
                confidence = round(1.0 - 1.0 / (math.sqrt(n) + 1.0), 4) if n > 0 else 0.0
                features["bay_horse_confidence"] = confidence
                fill_counts["bay_horse_confidence"] += 1

                # -- bay_prior_strength --
                prior_str = round(min(1.0, PRIOR_PSEUDO_COUNT / n), 4) if n > 0 else 1.0
                features["bay_prior_strength"] = prior_str
                fill_counts["bay_prior_strength"] += 1

                # Get discipline priors
                prior_win = dp.win_rate() if dp else 0.08
                prior_place = dp.place_rate() if dp else 0.25

                # -- bay_posterior_win_rate --
                posterior_win = round(
                    (prior_win * PRIOR_PSEUDO_COUNT + hs.wins) / (PRIOR_PSEUDO_COUNT + n),
                    4,
                )
                features["bay_posterior_win_rate"] = posterior_win
                fill_counts["bay_posterior_win_rate"] += 1

                # -- bay_posterior_place_rate --
                posterior_place = round(
                    (prior_place * PRIOR_PSEUDO_COUNT + hs.places) / (PRIOR_PSEUDO_COUNT + n),
                    4,
                )
                features["bay_posterior_place_rate"] = posterior_place
                fill_counts["bay_posterior_place_rate"] += 1

                # -- bay_credible_interval_width --
                if n > 0:
                    p = posterior_win
                    ci_width = round(4.0 * math.sqrt(p * (1.0 - p) / n), 4)
                    features["bay_credible_interval_width"] = ci_width
                    fill_counts["bay_credible_interval_width"] += 1
                else:
                    features["bay_credible_interval_width"] = None

                # -- bay_epistemic_uncertainty --
                epistemic = round(1.0 / math.sqrt(n + 1), 4)
                features["bay_epistemic_uncertainty"] = epistemic
                fill_counts["bay_epistemic_uncertainty"] += 1

                # -- bay_aleatoric_uncertainty --
                pos_std = hs.position_std()
                if pos_std is not None and hs.pos_mean > _EPS:
                    aleatoric = round(pos_std / hs.pos_mean, 4)
                    features["bay_aleatoric_uncertainty"] = aleatoric
                    fill_counts["bay_aleatoric_uncertainty"] += 1
                else:
                    features["bay_aleatoric_uncertainty"] = None

                # -- bay_information_gain --
                info_gain = _information_gain(prior_win, posterior_win)
                if info_gain is not None:
                    features["bay_information_gain"] = round(info_gain, 4)
                    fill_counts["bay_information_gain"] += 1
                else:
                    features["bay_information_gain"] = None

                # -- bay_prediction_entropy --
                entropy = _binary_entropy(posterior_win)
                if entropy is not None:
                    features["bay_prediction_entropy"] = round(entropy, 4)
                    fill_counts["bay_prediction_entropy"] += 1
                else:
                    features["bay_prediction_entropy"] = None

                # Write record
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER computing features (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                discipline = rec["discipline"]

                if cheval:
                    hs = horse_state[cheval]
                    hs.update(rec["gagnant"], rec["place"], rec["position"])

                if discipline:
                    dp = disc_prior[discipline]
                    dp.total += 1
                    if rec["gagnant"]:
                        dp.wins += 1
                    if rec["place"]:
                        dp.places += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Bayesian uncertainty build termine: %d features en %.1fs "
        "(chevaux uniques: %d, disciplines: %d)",
        n_written, elapsed, len(horse_state), len(disc_prior),
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
        description="Construction des features d'incertitude bayesienne a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/bayesian_uncertainty/)",
    )
    args = parser.parse_args()

    logger = setup_logging("bayesian_uncertainty_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "bayesian_uncertainty_features.jsonl"
    build_bayesian_uncertainty_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
