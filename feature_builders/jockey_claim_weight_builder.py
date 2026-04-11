#!/usr/bin/env python3
"""
feature_builders.jockey_claim_weight_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Jockey weight claim and weight management features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant jockey weight features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage. Snapshot BEFORE update.

Produces:
  - jockey_claim_weight_features.jsonl   in builder_outputs/jockey_claim_weight/

Features per partant (8):
  - jcw_weight_claim            : weight advantage from jockey claim
                                  (poids_condition or poids_porte - avg weight in race)
  - jcw_jockey_avg_weight       : jockey's average carried weight across history
  - jcw_weight_vs_jockey_avg    : current weight - jockey's historical average weight
  - jcw_jockey_light_win_rate   : jockey's win rate when carrying below-average weight
  - jcw_jockey_heavy_win_rate   : jockey's win rate when carrying above-average weight
  - jcw_optimal_weight_zone     : 1 if weight is within jockey's best-performing weight
                                  range (bottom quartile of weight for wins)
  - jcw_weight_trend            : slope of jockey's last 10 carried weights (positive =
                                  increasingly heavy, negative = increasingly light)
  - jcw_handicap_weight_pct     : poids_porte / max weight in race (within-field pct)

State per jockey:
  - weights         : bounded deque of last 50 carried weights (for slope + avg)
  - wins_light      : int -- wins when carrying below jockey's own running avg
  - total_light     : int -- starts when carrying below jockey's own running avg
  - wins_heavy      : int -- wins when carrying above jockey's own running avg
  - total_heavy     : int -- starts when carrying above jockey's own running avg
  - running_avg     : Welford running mean of weight
  - running_count   : int -- total races observed
  - win_weights     : list of carried weights on winning races (for quartile)

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records
  - Write to .tmp then atomic rename

Key fields: nom_jockey (or jockey/driver), poids_porte_kg or poids_porte,
            date_reunion_iso, position_arrivee, partant_uid, course_uid, num_pmu.

Usage:
    python feature_builders/jockey_claim_weight_builder.py
    python feature_builders/jockey_claim_weight_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/jockey_claim_weight")

# Progress log every N records
_LOG_EVERY = 500_000

# Max weight history stored per jockey (for slope and avg)
_MAX_WEIGHT_HISTORY = 50

# Window for weight trend (slope over last N rides)
_TREND_WINDOW = 10

# Minimum rides to produce a reliable stat
_MIN_RIDES = 5
_MIN_LIGHT_HEAVY = 3


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v and v > 0 else None
    except (ValueError, TypeError):
        return None


def _norm_jockey(name: Optional[str]) -> Optional[str]:
    """Normalise jockey name: strip + upper."""
    if not name or not isinstance(name, str):
        return None
    n = name.strip().upper()
    return n if n else None


def _slope(values: list[float]) -> Optional[float]:
    """Ordinary-least-squares slope of a sequence (index 0..n-1 as x).

    Returns None when fewer than 2 points.
    """
    n = len(values)
    if n < 2:
        return None
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def _quartile_threshold(values: list[float], q: float = 0.25) -> Optional[float]:
    """Return the q-th quantile of a list (simple linear interpolation)."""
    if not values:
        return None
    sv = sorted(values)
    pos = q * (len(sv) - 1)
    lo = int(pos)
    hi = lo + 1
    if hi >= len(sv):
        return sv[lo]
    frac = pos - lo
    return sv[lo] + frac * (sv[hi] - sv[lo])


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _JockeyWeightState:
    """Tracks jockey weight history for claim/management features.

    State:
      weights        : bounded deque of last _MAX_WEIGHT_HISTORY carried weights
      running_sum    : float -- sum of all weights seen (for running avg)
      running_count  : int   -- total races where weight was known
      wins_light     : int   -- wins when weight < running avg at race time
      total_light    : int   -- starts when weight < running avg at race time
      wins_heavy     : int   -- wins when weight >= running avg at race time
      total_heavy    : int   -- starts when weight >= running avg at race time
      win_weights    : list  -- weights carried in winning races (for quartile zone)
    """

    __slots__ = (
        "weights",
        "running_sum",
        "running_count",
        "wins_light",
        "total_light",
        "wins_heavy",
        "total_heavy",
        "win_weights",
    )

    def __init__(self) -> None:
        self.weights: deque[float] = deque(maxlen=_MAX_WEIGHT_HISTORY)
        self.running_sum: float = 0.0
        self.running_count: int = 0
        self.wins_light: int = 0
        self.total_light: int = 0
        self.wins_heavy: int = 0
        self.total_heavy: int = 0
        self.win_weights: list[float] = []

    # ------------------------------------------------------------------
    # Read-only snapshot (uses only data already in state -- no leakage)
    # ------------------------------------------------------------------

    def snapshot(
        self,
        current_weight: Optional[float],
        field_max_weight: Optional[float],
        field_avg_weight: Optional[float],
        poids_condition: Optional[float],
    ) -> dict[str, Any]:
        """Compute all 8 features using past data only."""

        feats: dict[str, Any] = {
            "jcw_weight_claim": None,
            "jcw_jockey_avg_weight": None,
            "jcw_weight_vs_jockey_avg": None,
            "jcw_jockey_light_win_rate": None,
            "jcw_jockey_heavy_win_rate": None,
            "jcw_optimal_weight_zone": None,
            "jcw_weight_trend": None,
            "jcw_handicap_weight_pct": None,
        }

        # ---- jcw_weight_claim -----------------------------------------------
        # Priority: poids_condition (explicit claim allowance) if available.
        # Fallback: current weight vs average field weight (negative = advantage).
        if poids_condition is not None and poids_condition > 0:
            # positive = extra weight (overweight); negative = claim (allowance)
            feats["jcw_weight_claim"] = round(poids_condition, 2)
        elif current_weight is not None and field_avg_weight is not None:
            feats["jcw_weight_claim"] = round(current_weight - field_avg_weight, 2)

        # ---- jcw_jockey_avg_weight ------------------------------------------
        if self.running_count >= _MIN_RIDES:
            avg_w = self.running_sum / self.running_count
            feats["jcw_jockey_avg_weight"] = round(avg_w, 2)

            # ---- jcw_weight_vs_jockey_avg -----------------------------------
            if current_weight is not None:
                feats["jcw_weight_vs_jockey_avg"] = round(current_weight - avg_w, 2)

            # ---- jcw_jockey_light_win_rate / heavy_win_rate -----------------
            if self.total_light >= _MIN_LIGHT_HEAVY:
                feats["jcw_jockey_light_win_rate"] = round(
                    self.wins_light / self.total_light, 4
                )
            if self.total_heavy >= _MIN_LIGHT_HEAVY:
                feats["jcw_jockey_heavy_win_rate"] = round(
                    self.wins_heavy / self.total_heavy, 4
                )

        # ---- jcw_optimal_weight_zone ----------------------------------------
        # 1 if current weight is within the bottom quartile of jockey's winning weights
        # (i.e. they perform best at lighter weights in that zone)
        if current_weight is not None and len(self.win_weights) >= _MIN_LIGHT_HEAVY:
            q25 = _quartile_threshold(self.win_weights, 0.25)
            q75 = _quartile_threshold(self.win_weights, 0.75)
            if q25 is not None and q75 is not None:
                feats["jcw_optimal_weight_zone"] = (
                    1 if q25 <= current_weight <= q75 else 0
                )

        # ---- jcw_weight_trend -----------------------------------------------
        if len(self.weights) >= 2:
            window = list(self.weights)[-_TREND_WINDOW:]
            feats["jcw_weight_trend"] = _slope(window)

        # ---- jcw_handicap_weight_pct ----------------------------------------
        if current_weight is not None and field_max_weight is not None and field_max_weight > 0:
            feats["jcw_handicap_weight_pct"] = round(current_weight / field_max_weight, 4)

        return feats

    # ------------------------------------------------------------------
    # Mutating update (called AFTER snapshot -- temporal integrity)
    # ------------------------------------------------------------------

    def update(self, weight: Optional[float], is_winner: bool) -> None:
        """Update state with result of this race."""
        if weight is None:
            return

        # Determine light/heavy threshold = running avg BEFORE this race
        if self.running_count >= _MIN_RIDES:
            avg_w = self.running_sum / self.running_count
            if weight < avg_w:
                self.total_light += 1
                if is_winner:
                    self.wins_light += 1
            else:
                self.total_heavy += 1
                if is_winner:
                    self.wins_heavy += 1
        else:
            # Not enough history yet -- still count light/heavy once we can
            # compare, so just track raw counts for future use
            pass

        self.weights.append(weight)
        self.running_sum += weight
        self.running_count += 1

        if is_winner:
            self.win_weights.append(weight)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_jockey_claim_weight_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build jockey claim-weight features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Also computes race-level weight aggregates (max, avg) for handicap_weight_pct
    and weight_claim fallback -- these are derived from the current course group only
    (intra-race knowledge, available at race time).

    Returns the total number of feature records written.
    """
    logger.info("=== Jockey Claim Weight Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date, course_uid, num_pmu, byte_offset) --
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

    # -- Phase 2: Sort lightweight index chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, stream output to disk --
    t2 = time.time()
    jockey_states: dict[str, _JockeyWeightState] = defaultdict(_JockeyWeightState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "jcw_weight_claim",
        "jcw_jockey_avg_weight",
        "jcw_weight_vs_jockey_avg",
        "jcw_jockey_light_win_rate",
        "jcw_jockey_heavy_win_rate",
        "jcw_optimal_weight_zone",
        "jcw_weight_trend",
        "jcw_handicap_weight_pct",
    ]
    fill_counts = {k: 0 for k in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(offset: int) -> dict:
            """Seek to byte offset and read one JSONL record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect index entries for this course
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

            # Read all records for this course from disk
            course_records: list[dict] = [
                _read_at(index[ci][3]) for ci in course_indices
            ]

            # -- Race-level weight aggregates (intra-race, no leakage) --
            # These use all runners in the same race (known at race time).
            course_weights: list[float] = []
            for rec in course_records:
                w = _safe_float(rec.get("poids_porte_kg")) or _safe_float(
                    rec.get("poids_porte")
                )
                if w is not None:
                    course_weights.append(w)

            field_max_weight: Optional[float] = max(course_weights) if course_weights else None
            field_avg_weight: Optional[float] = (
                sum(course_weights) / len(course_weights) if course_weights else None
            )

            # -- Snapshot pre-race stats & emit features --
            post_updates: list[tuple[Optional[str], Optional[float], bool]] = []

            for rec in course_records:
                jockey = _norm_jockey(
                    rec.get("nom_jockey") or rec.get("jockey") or rec.get("driver")
                )
                partant_uid = rec.get("partant_uid")
                is_winner = bool(rec.get("is_gagnant"))

                current_weight = _safe_float(rec.get("poids_porte_kg")) or _safe_float(
                    rec.get("poids_porte")
                )
                # poids_condition: claimed allowance (e.g. -2 kg apprentice claim).
                # Some schemas store this directly; others don't expose it.
                poids_condition = _safe_float(
                    rec.get("poids_condition") or rec.get("allocation_poids")
                )

                features: dict[str, Any] = {"partant_uid": partant_uid}

                if jockey:
                    st = jockey_states[jockey]
                    snap = st.snapshot(
                        current_weight, field_max_weight, field_avg_weight, poids_condition
                    )
                    features.update(snap)

                    for fname in feature_names:
                        if features.get(fname) is not None:
                            fill_counts[fname] += 1
                else:
                    for fname in feature_names:
                        features[fname] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Queue post-race update
                post_updates.append((jockey, current_weight, is_winner))

            # -- Update states AFTER snapshotting (no leakage) --
            for jockey, weight, is_winner in post_updates:
                if jockey:
                    jockey_states[jockey].update(weight, is_winner)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Jockey claim weight build termine: %d features en %.1fs (jockeys: %d)",
        n_written, elapsed, len(jockey_states),
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


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features jockey claim weight a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_claim_weight_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "jockey_claim_weight_features.jsonl"
    build_jockey_claim_weight_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
