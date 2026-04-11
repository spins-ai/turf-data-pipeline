#!/usr/bin/env python3
"""
feature_builders.age_distance_pref_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Age-specific distance preference features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant age-distance preference features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - age_distance_pref_features.jsonl  in
    D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/age_distance_pref/

Distance buckets:
  sprint  : < 1200 m
  mile    : 1200 - 1600 m
  middle  : 1600 - 2000 m
  staying : 2000 - 2400 m
  marathon: > 2400 m

Age groups: 2, 3, 4, 5+ (any horse aged 5 or more maps to "5+")

Features per partant (8):
  - adp_age_distance_win_rate    : population win rate for (age_group, dist_bucket)
  - adp_horse_optimal_distance   : distance bucket where horse has best historical win rate
  - adp_distance_match_score     : 1.0 if current dist == optimal, decreasing with gap
  - adp_age_improving_distance   : 1 if horse's wins increase at longer distances as it ages
  - adp_short_vs_long_ratio      : horse win rate short (<1600m) / win rate long (>=1600m)
  - adp_age_group_avg_distance   : population average racing distance for this age group
  - adp_distance_versatility     : 1 - coefficient-of-variation of win rates across buckets
  - adp_age_distance_edge        : horse win rate at this dist - population avg for (age, dist)

Temporal pattern: index + sort + seek (seek-based streaming).
  Phase 1 — build a lightweight index (date, course_uid, num_pmu, byte_offset).
  Phase 2 — sort the index chronologically.
  Phase 3 — seek-read records course by course:
              snapshot features BEFORE updating state,
              then update state with race result.

Usage:
    python feature_builders/age_distance_pref_builder.py
    python feature_builders/age_distance_pref_builder.py \\
        --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

_DEFAULT_INPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
_DEFAULT_OUTPUT = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/age_distance_pref/"
    "age_distance_pref_features.jsonl"
)

_LOG_EVERY = 500_000

# Distance bucket boundaries (metres)
_BUCKETS: list[tuple[str, Optional[float], Optional[float]]] = [
    ("sprint",   None,   1200.0),   # < 1200
    ("mile",     1200.0, 1600.0),   # 1200 <= d < 1600
    ("middle",   1600.0, 2000.0),   # 1600 <= d < 2000
    ("staying",  2000.0, 2400.0),   # 2000 <= d < 2400
    ("marathon", 2400.0, None),     # >= 2400
]

_BUCKET_NAMES: list[str] = [b[0] for b in _BUCKETS]

# Ordered bucket sequence for "distance gap" score computation
_BUCKET_ORDER: dict[str, int] = {b: i for i, b in enumerate(_BUCKET_NAMES)}

# Age group thresholds
_MAX_NAMED_AGE = 4  # ages 2, 3, 4 get their own group; 5+ is the last group

# Minimum races to report a stable statistic
_MIN_RUNS_HORSE = 3
_MIN_RUNS_POP   = 10


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None   # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _dist_bucket(distance: Optional[float]) -> Optional[str]:
    """Classify a distance (metres) into a named bucket."""
    if distance is None or distance <= 0:
        return None
    for name, lo, hi in _BUCKETS:
        if lo is None and distance < hi:
            return name
        if hi is None and distance >= lo:
            return name
        if lo is not None and hi is not None and lo <= distance < hi:
            return name
    return None


def _age_group(age: Optional[int]) -> Optional[str]:
    """Normalise raw age to age group label."""
    if age is None or age < 2:
        return None
    if age >= 5:
        return "5+"
    return str(age)


def _bucket_gap(b1: Optional[str], b2: Optional[str]) -> int:
    """Absolute distance (in bucket steps) between two bucket names."""
    if b1 is None or b2 is None:
        return 99
    return abs(_BUCKET_ORDER.get(b1, -99) - _BUCKET_ORDER.get(b2, -99))


def _match_score(current_bucket: Optional[str], optimal_bucket: Optional[str]) -> Optional[float]:
    """
    1.0  if current == optimal
    0.75 if adjacent (1 step apart)
    0.5  if 2 steps apart
    0.25 if 3 steps apart
    0.0  if 4 steps apart (maximum)
    None if either is unknown
    """
    gap = _bucket_gap(current_bucket, optimal_bucket)
    if gap == 99:
        return None
    score = max(0.0, 1.0 - gap * 0.25)
    return round(score, 4)


# ===========================================================================
# STATE
# ===========================================================================


class _PopStats:
    """Population-level statistics: (age_group, dist_bucket) -> {wins, total}."""

    def __init__(self) -> None:
        # key: (age_group_str, bucket_str) -> [wins, total]
        self._data: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        # age_group -> [sum_distance, count]
        self._age_dist: dict[str, list[float]] = defaultdict(lambda: [0.0, 0])

    def win_rate(self, age_group: Optional[str], bucket: Optional[str]) -> Optional[float]:
        if age_group is None or bucket is None:
            return None
        cell = self._data.get((age_group, bucket))
        if cell is None or cell[1] < _MIN_RUNS_POP:
            return None
        return round(cell[0] / cell[1], 4)

    def avg_distance(self, age_group: Optional[str]) -> Optional[float]:
        if age_group is None:
            return None
        agg = self._age_dist.get(age_group)
        if agg is None or agg[1] == 0:
            return None
        return round(agg[0] / agg[1], 1)

    def update(
        self,
        age_group: Optional[str],
        bucket: Optional[str],
        distance: Optional[float],
        is_winner: bool,
    ) -> None:
        if age_group is None:
            return
        if bucket is not None:
            cell = self._data[(age_group, bucket)]
            cell[1] += 1
            if is_winner:
                cell[0] += 1
        if distance is not None and distance > 0:
            agg = self._age_dist[age_group]
            agg[0] += distance
            agg[1] += 1


class _HorseStats:
    """Per-horse statistics: dist_bucket -> {wins, total} + age-indexed history."""

    def __init__(self) -> None:
        # bucket -> [wins, total]
        self._bucket: dict[str, list[int]] = {b: [0, 0] for b in _BUCKET_NAMES}
        # age_group -> bucket -> wins  (for stamina maturation feature)
        self._age_bucket_wins: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self._age_bucket_runs: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )

    # ------------------------------------------------------------------ #
    # Feature computation (snapshot BEFORE update)                        #
    # ------------------------------------------------------------------ #

    def optimal_bucket(self) -> Optional[str]:
        """Bucket with the best win rate (min _MIN_RUNS_HORSE)."""
        best_bucket = None
        best_wr = -1.0
        for name, cell in self._bucket.items():
            if cell[1] < _MIN_RUNS_HORSE:
                continue
            wr = cell[0] / cell[1]
            if wr > best_wr:
                best_wr = wr
                best_bucket = name
        return best_bucket

    def win_rate_bucket(self, bucket: Optional[str]) -> Optional[float]:
        if bucket is None:
            return None
        cell = self._bucket.get(bucket)
        if cell is None or cell[1] < _MIN_RUNS_HORSE:
            return None
        return round(cell[0] / cell[1], 4)

    def short_vs_long_ratio(self) -> Optional[float]:
        """Win rate for short distances (<1600m: sprint+mile) / win rate for long (>=1600m)."""
        short_wins  = sum(self._bucket[b][0] for b in ("sprint", "mile"))
        short_runs  = sum(self._bucket[b][1] for b in ("sprint", "mile"))
        long_wins   = sum(self._bucket[b][0] for b in ("middle", "staying", "marathon"))
        long_runs   = sum(self._bucket[b][1] for b in ("middle", "staying", "marathon"))

        if short_runs < _MIN_RUNS_HORSE or long_runs < _MIN_RUNS_HORSE:
            return None
        wr_short = short_wins / short_runs
        wr_long  = long_wins  / long_runs
        if wr_long == 0:
            return None   # avoid division by zero / infinite ratio
        return round(wr_short / wr_long, 4)

    def distance_versatility(self) -> Optional[float]:
        """1 - CoV of win rates across buckets with sufficient data.

        CoV = std / mean of win rates.
        Returns None if fewer than 2 buckets have enough data.
        """
        rates = []
        for name in _BUCKET_NAMES:
            cell = self._bucket[name]
            if cell[1] >= _MIN_RUNS_HORSE:
                rates.append(cell[0] / cell[1])

        if len(rates) < 2:
            return None

        mean = sum(rates) / len(rates)
        if mean == 0:
            return None

        variance = sum((r - mean) ** 2 for r in rates) / len(rates)
        std = math.sqrt(variance)
        cov = std / mean
        return round(max(0.0, 1.0 - cov), 4)

    def age_improving_distance(self) -> Optional[int]:
        """
        1 if the horse's win rate at longer distances improves as it ages.

        Algorithm: compare win rate at short (<= mile) vs long (>= middle)
        for younger age groups (2-3) vs older (4-5+). If older wins more
        at longer distances (proportionally), return 1 else 0.
        Needs data in at least 2 age groups.
        """
        young_groups = [g for g in ("2", "3") if g in self._age_bucket_wins]
        old_groups   = [g for g in ("4", "5+") if g in self._age_bucket_wins]

        if not young_groups or not old_groups:
            return None

        def _long_ratio(groups: list[str]) -> Optional[float]:
            long_wins = sum(
                self._age_bucket_wins[g].get(b, 0)
                for g in groups
                for b in ("middle", "staying", "marathon")
            )
            long_runs = sum(
                self._age_bucket_runs[g].get(b, 0)
                for g in groups
                for b in ("middle", "staying", "marathon")
            )
            all_wins = sum(
                self._age_bucket_wins[g].get(b, 0)
                for g in groups
                for b in _BUCKET_NAMES
            )
            all_runs = sum(
                self._age_bucket_runs[g].get(b, 0)
                for g in groups
                for b in _BUCKET_NAMES
            )
            if long_runs < 2 or all_runs < 3 or all_wins == 0:
                return None
            return (long_wins / long_runs) / (all_wins / all_runs)

        young_lr = _long_ratio(young_groups)
        old_lr   = _long_ratio(old_groups)

        if young_lr is None or old_lr is None:
            return None
        return 1 if old_lr > young_lr else 0

    # ------------------------------------------------------------------ #
    # State update (AFTER snapshot)                                        #
    # ------------------------------------------------------------------ #

    def update(
        self,
        bucket: Optional[str],
        age_group: Optional[str],
        is_winner: bool,
    ) -> None:
        if bucket is None:
            return
        cell = self._bucket[bucket]
        cell[1] += 1
        if is_winner:
            cell[0] += 1
        if age_group is not None:
            self._age_bucket_runs[age_group][bucket] += 1
            if is_winner:
                self._age_bucket_wins[age_group][bucket] += 1


# ===========================================================================
# MAIN BUILD (index + sort + seek pattern, streaming output)
# ===========================================================================


def build_age_distance_pref_features(
    input_path: Path,
    output_path: Path,
    logger,
) -> int:
    """Build age-distance preference features from partants_master.jsonl.

    Returns the total number of feature records written.
    """
    logger.info("=== Age Distance Preference Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1 : Build lightweight index (sort_key, byte_offset)
    # ------------------------------------------------------------------
    index: list[tuple[str, str, int, int]] = []  # (date, course_uid, num_pmu, offset)
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
                gc.collect()

            date_str   = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu    = _safe_int(rec.get("num_pmu")) or 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2 : Sort the lightweight index chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3 : Process course by course, streaming output
    # ------------------------------------------------------------------
    pop_stats: _PopStats = _PopStats()
    horse_stats: dict[str, _HorseStats] = defaultdict(_HorseStats)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written   = 0
    total       = len(index)

    feature_keys = [
        "adp_age_distance_win_rate",
        "adp_horse_optimal_distance",
        "adp_distance_match_score",
        "adp_age_improving_distance",
        "adp_short_vs_long_ratio",
        "adp_age_group_avg_distance",
        "adp_distance_versatility",
        "adp_age_distance_edge",
    ]
    fill_counts: dict[str, int] = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract(rec: dict) -> dict:
            """Pull only the fields we need from a full record."""
            raw_age  = _safe_int(rec.get("age"))
            raw_dist = _safe_float(rec.get("distance"))

            horse_id = (
                rec.get("horse_id")
                or rec.get("nom_cheval")
                or ""
            )
            if isinstance(horse_id, str):
                horse_id = horse_id.strip()

            return {
                "uid":        rec.get("partant_uid"),
                "date":       rec.get("date_reunion_iso", ""),
                "course":     rec.get("course_uid", ""),
                "num":        rec.get("num_pmu", 0) or 0,
                "horse_id":   horse_id,
                "age":        raw_age,
                "age_group":  _age_group(raw_age),
                "distance":   raw_dist,
                "bucket":     _dist_bucket(raw_dist),
                "is_winner":  bool(rec.get("is_gagnant")),
                "position":   _safe_int(rec.get("position_arrivee")),
            }

        i = 0
        while i < total:
            # Collect all index entries for the current course
            course_uid      = index[i][1]
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

            # Seek-read only this course's records
            course_group = [_extract(_read_at(index[ci][3])) for ci in course_indices]

            # ---- Snapshot pre-race features (BEFORE update) ----
            for rec in course_group:
                horse_id  = rec["horse_id"]
                bucket    = rec["bucket"]
                age_group = rec["age_group"]

                # 1. adp_age_distance_win_rate  (population stat)
                pop_wr = pop_stats.win_rate(age_group, bucket)

                # 2-3. adp_horse_optimal_distance + adp_distance_match_score
                hs = horse_stats[horse_id] if horse_id else None
                optimal_bucket = hs.optimal_bucket() if hs else None
                match_score    = _match_score(bucket, optimal_bucket)

                # 4. adp_age_improving_distance
                age_improving = hs.age_improving_distance() if hs else None

                # 5. adp_short_vs_long_ratio
                sv_l_ratio = hs.short_vs_long_ratio() if hs else None

                # 6. adp_age_group_avg_distance  (population baseline)
                pop_avg_dist = pop_stats.avg_distance(age_group)

                # 7. adp_distance_versatility
                versatility = hs.distance_versatility() if hs else None

                # 8. adp_age_distance_edge  (horse wr at bucket - population wr)
                horse_wr_bucket = hs.win_rate_bucket(bucket) if hs else None
                if horse_wr_bucket is not None and pop_wr is not None:
                    age_dist_edge: Optional[float] = round(horse_wr_bucket - pop_wr, 4)
                else:
                    age_dist_edge = None

                features: dict[str, Any] = {
                    "partant_uid":                rec["uid"],
                    "adp_age_distance_win_rate":  pop_wr,
                    "adp_horse_optimal_distance":  optimal_bucket,
                    "adp_distance_match_score":    match_score,
                    "adp_age_improving_distance":  age_improving,
                    "adp_short_vs_long_ratio":     sv_l_ratio,
                    "adp_age_group_avg_distance":  pop_avg_dist,
                    "adp_distance_versatility":    versatility,
                    "adp_age_distance_edge":       age_dist_edge,
                }

                # Track fill rates
                for k in feature_keys:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # ---- Update state AFTER snapshot ----
            for rec in course_group:
                horse_id  = rec["horse_id"]
                bucket    = rec["bucket"]
                age_group = rec["age_group"]
                distance  = rec["distance"]
                is_winner = rec["is_winner"]

                pop_stats.update(age_group, bucket, distance, is_winner)
                if horse_id:
                    horse_stats[horse_id].update(bucket, age_group, is_winner)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace (no partial files on crash)
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Age distance pref build termine: %d features en %.1fs "
        "(chevaux uniques: %d, pop cells: %d)",
        n_written, elapsed, len(horse_stats), len(pop_stats._data),
    )

    # Fill-rate summary
    logger.info("=== Fill rates ===")
    for k in feature_keys:
        v = fill_counts[k]
        pct = 100.0 * v / n_written if n_written else 0.0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _resolve_input(cli_path: Optional[str]) -> Path:
    """Resolve the input file path from CLI argument or default."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if _DEFAULT_INPUT.exists():
        return _DEFAULT_INPUT
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {_DEFAULT_INPUT}\n"
        "Utilisez --input pour specifier un chemin alternatif."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features age-distance preference "
            "a partir de partants_master.jsonl"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help=(
            "Chemin vers partants_master.jsonl "
            f"(defaut: {_DEFAULT_INPUT})"
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=(
            "Chemin de sortie complet du fichier JSONL "
            f"(defaut: {_DEFAULT_OUTPUT})"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("age_distance_pref_builder")

    input_path  = _resolve_input(args.input)
    output_path = Path(args.output) if args.output else _DEFAULT_OUTPUT

    logger.info("Input  : %s", input_path)
    logger.info("Output : %s", output_path)

    build_age_distance_pref_features(input_path, output_path, logger)


if __name__ == "__main__":
    main()
