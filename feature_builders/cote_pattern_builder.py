#!/usr/bin/env python3
"""
feature_builders.cote_pattern_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Odds pattern features -- how a horse's odds evolve over time and
in different contexts (hippodrome, distance band).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically using an index + seek architecture, and computes
per-partant odds-pattern features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the odds stats -- no future leakage.

Produces:
  - cote_pattern_features.jsonl

Features per partant (10):
  - cpt_avg_cote_last5         : average odds over last 5 races
  - cpt_cote_trend             : slope of odds over last 5 races
                                 (negative = shortening = market gaining confidence)
  - cpt_cote_volatility        : std dev of odds over last 5 races
  - cpt_cote_vs_career_avg     : current cote / career average cote
                                 (>1 = less favored than usual)
  - cpt_best_odds_career       : minimum cote ever (shortest price = peak confidence)
  - cpt_cote_range_career      : max_cote - min_cote over career
  - cpt_cote_at_this_hippo     : average cote when racing at this hippodrome
  - cpt_cote_at_this_distance  : average cote at this distance band
  - cpt_cote_improvement       : 1 if current cote < avg_last_5 (market improving view)
  - cpt_cote_x_form            : cote * seq_serie_places interaction

Usage:
    python feature_builders/cote_pattern_builder.py
    python feature_builders/cote_pattern_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/cote_pattern")
OUTPUT_FILENAME = "cote_pattern_features.jsonl"

_LOG_EVERY = 500_000

# Distance band boundaries (metres)
_DISTANCE_BANDS = [
    (0, 1200, "sprint"),
    (1201, 1600, "mile"),
    (1601, 2200, "inter"),
    (2201, 3000, "stayer"),
    (3001, 99999, "marathon"),
]


def _distance_band(distance: Optional[int]) -> str:
    """Map distance in metres to a band label."""
    if distance is None:
        return "unknown"
    for lo, hi, label in _DISTANCE_BANDS:
        if lo <= distance <= hi:
            return label
    return "unknown"


# ===========================================================================
# PER-HORSE STATE TRACKER
# ===========================================================================


class _HorseOddsState:
    """Tracks odds history for a single horse.

    Kept lightweight with __slots__ to minimise per-instance memory.
    """

    __slots__ = (
        "cotes",                  # deque(maxlen=20) of recent cotes
        "career_sum", "career_count",  # running sum + count for career avg
        "min_cote", "max_cote",
        "per_hippo_sum", "per_hippo_count",
        "per_distance_sum", "per_distance_count",
    )

    def __init__(self) -> None:
        self.cotes: deque = deque(maxlen=20)
        self.career_sum: float = 0.0
        self.career_count: int = 0
        self.min_cote: Optional[float] = None
        self.max_cote: Optional[float] = None
        # Per-hippodrome: dict[str, [sum, count]]
        self.per_hippo_sum: dict[str, float] = defaultdict(float)
        self.per_hippo_count: dict[str, int] = defaultdict(int)
        # Per-distance band: dict[str, [sum, count]]
        self.per_distance_sum: dict[str, float] = defaultdict(float)
        self.per_distance_count: dict[str, int] = defaultdict(int)

    def career_avg(self) -> Optional[float]:
        if self.career_count == 0:
            return None
        return self.career_sum / self.career_count

    def hippo_avg(self, hippo: str) -> Optional[float]:
        c = self.per_hippo_count.get(hippo, 0)
        if c == 0:
            return None
        return self.per_hippo_sum[hippo] / c

    def distance_avg(self, band: str) -> Optional[float]:
        c = self.per_distance_count.get(band, 0)
        if c == 0:
            return None
        return self.per_distance_sum[band] / c

    def last_n(self, n: int) -> list[float]:
        """Return last n cotes (oldest first)."""
        items = list(self.cotes)
        return items[-n:] if len(items) >= n else items

    def update(self, cote: float, hippo: str, dist_band: str) -> None:
        """Update state AFTER feature snapshot."""
        self.cotes.append(cote)
        self.career_sum += cote
        self.career_count += 1
        if self.min_cote is None or cote < self.min_cote:
            self.min_cote = cote
        if self.max_cote is None or cote > self.max_cote:
            self.max_cote = cote
        if hippo:
            self.per_hippo_sum[hippo] += cote
            self.per_hippo_count[hippo] += 1
        if dist_band and dist_band != "unknown":
            self.per_distance_sum[dist_band] += cote
            self.per_distance_count[dist_band] += 1


# ===========================================================================
# FEATURE COMPUTATION HELPERS
# ===========================================================================


def _slope(values: list[float]) -> Optional[float]:
    """Compute slope of a simple linear regression on values (index as x).

    Positive slope = odds increasing (drifting out).
    Negative slope = odds shortening (market confidence growing).
    """
    n = len(values)
    if n < 2:
        return None
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if den == 0:
        return None
    return round(num / den, 6)


def _std_dev(values: list[float]) -> Optional[float]:
    """Population standard deviation."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    return round(math.sqrt(var), 6)


# ===========================================================================
# MAIN BUILD (index + seek + streaming output)
# ===========================================================================


def build_cote_pattern_features(input_path: Path, output_path: Path, logger) -> int:
    """Build odds-pattern features from partants_master.jsonl.

    Architecture:
      1. Build a lightweight index: (date, course_uid, num_pmu, byte_offset)
      2. Sort chronologically
      3. Seek to each record, snapshot features BEFORE update, write output

    Returns the total number of feature records written.
    """
    logger.info("=== Cote Pattern Builder (index + seek) ===")
    logger.info("Input: %s", input_path)
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
            line_s = line.strip()
            if not line_s:
                continue
            try:
                rec = json.loads(line_s)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexing %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1: %d records indexed in %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Phase 2: sorted in %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseOddsState] = defaultdict(_HorseOddsState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    total = len(index)

    fill_counts: dict[str, int] = {
        "cpt_avg_cote_last5": 0,
        "cpt_cote_trend": 0,
        "cpt_cote_volatility": 0,
        "cpt_cote_vs_career_avg": 0,
        "cpt_best_odds_career": 0,
        "cpt_cote_range_career": 0,
        "cpt_cote_at_this_hippo": 0,
        "cpt_cote_at_this_distance": 0,
        "cpt_cote_improvement": 0,
        "cpt_cote_x_form": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read records from disk for this course
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_at(index[ci][3])
                course_records.append(rec)

            # -- Deferred updates to apply AFTER snapshotting all features --
            deferred_updates: list[tuple[str, float, str, str]] = []

            for rec in course_records:
                cheval = rec.get("nom_cheval") or ""
                uid = rec.get("partant_uid")
                course_uid_val = rec.get("course_uid", "")
                date_iso = rec.get("date_reunion_iso", "")
                hippo = rec.get("hippodrome_normalise", "") or ""

                # Parse distance
                raw_dist = rec.get("distance")
                dist_int: Optional[int] = None
                if raw_dist is not None:
                    try:
                        dist_int = int(raw_dist)
                    except (ValueError, TypeError):
                        pass
                dist_band = _distance_band(dist_int)

                # Parse current cote
                cote_raw = rec.get("cote_finale") or rec.get("cote_reference") or rec.get("rapport_final")
                cote: Optional[float] = None
                if cote_raw is not None:
                    try:
                        cote = float(cote_raw)
                        if cote <= 0:
                            cote = None
                    except (ValueError, TypeError):
                        pass

                # Parse seq_serie_places for interaction feature
                serie_raw = rec.get("seq_serie_places")
                serie: Optional[float] = None
                if serie_raw is not None:
                    try:
                        serie = float(serie_raw)
                    except (ValueError, TypeError):
                        pass

                # === SNAPSHOT features BEFORE update ===
                feat: dict[str, Any] = {
                    "partant_uid": uid,
                    "course_uid": course_uid_val,
                    "date_reunion_iso": date_iso,
                }

                if cheval:
                    st = horse_state[cheval]
                    last5 = st.last_n(5)

                    # cpt_avg_cote_last5
                    if len(last5) >= 1:
                        avg5 = sum(last5) / len(last5)
                        feat["cpt_avg_cote_last5"] = round(avg5, 4)
                        fill_counts["cpt_avg_cote_last5"] += 1
                    else:
                        avg5 = None
                        feat["cpt_avg_cote_last5"] = None

                    # cpt_cote_trend (slope over last 5)
                    if len(last5) >= 2:
                        feat["cpt_cote_trend"] = _slope(last5)
                        if feat["cpt_cote_trend"] is not None:
                            fill_counts["cpt_cote_trend"] += 1
                    else:
                        feat["cpt_cote_trend"] = None

                    # cpt_cote_volatility (std dev last 5)
                    if len(last5) >= 2:
                        feat["cpt_cote_volatility"] = _std_dev(last5)
                        if feat["cpt_cote_volatility"] is not None:
                            fill_counts["cpt_cote_volatility"] += 1
                    else:
                        feat["cpt_cote_volatility"] = None

                    # cpt_cote_vs_career_avg
                    c_avg = st.career_avg()
                    if cote is not None and c_avg is not None and c_avg > 0:
                        feat["cpt_cote_vs_career_avg"] = round(cote / c_avg, 4)
                        fill_counts["cpt_cote_vs_career_avg"] += 1
                    else:
                        feat["cpt_cote_vs_career_avg"] = None

                    # cpt_best_odds_career
                    feat["cpt_best_odds_career"] = st.min_cote
                    if st.min_cote is not None:
                        fill_counts["cpt_best_odds_career"] += 1

                    # cpt_cote_range_career
                    if st.min_cote is not None and st.max_cote is not None:
                        feat["cpt_cote_range_career"] = round(st.max_cote - st.min_cote, 4)
                        fill_counts["cpt_cote_range_career"] += 1
                    else:
                        feat["cpt_cote_range_career"] = None

                    # cpt_cote_at_this_hippo
                    if hippo:
                        h_avg = st.hippo_avg(hippo)
                        feat["cpt_cote_at_this_hippo"] = round(h_avg, 4) if h_avg is not None else None
                        if h_avg is not None:
                            fill_counts["cpt_cote_at_this_hippo"] += 1
                    else:
                        feat["cpt_cote_at_this_hippo"] = None

                    # cpt_cote_at_this_distance
                    d_avg = st.distance_avg(dist_band)
                    feat["cpt_cote_at_this_distance"] = round(d_avg, 4) if d_avg is not None else None
                    if d_avg is not None:
                        fill_counts["cpt_cote_at_this_distance"] += 1

                    # cpt_cote_improvement: 1 if current cote < avg_last_5
                    if cote is not None and avg5 is not None:
                        feat["cpt_cote_improvement"] = 1 if cote < avg5 else 0
                        fill_counts["cpt_cote_improvement"] += 1
                    else:
                        feat["cpt_cote_improvement"] = None

                    # cpt_cote_x_form: cote * seq_serie_places
                    if cote is not None and serie is not None:
                        feat["cpt_cote_x_form"] = round(cote * serie, 4)
                        fill_counts["cpt_cote_x_form"] += 1
                    else:
                        feat["cpt_cote_x_form"] = None

                else:
                    # No horse name -- all None
                    for k in fill_counts:
                        feat[k] = None

                fout.write(json.dumps(feat, ensure_ascii=False) + "\n")
                n_written += 1

                # Queue deferred update
                if cheval and cote is not None:
                    deferred_updates.append((cheval, cote, hippo, dist_band))

            # -- Apply updates AFTER all features for this course are snapshotted --
            for cheval, cote_val, hippo_val, band_val in deferred_updates:
                horse_state[cheval].update(cote_val, hippo_val, band_val)

            # Periodic GC
            if n_written % _LOG_EVERY < len(course_records):
                logger.info("  Processed %d / %d records...", n_written, total)
                gc.collect()

    # Atomic rename
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Cote pattern build done: %d features in %.1fs (horses tracked: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Fill rates
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
        raise FileNotFoundError(f"Input file not found: {p}")
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Input file not found: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Cote pattern features from partants_master.jsonl"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help=f"Path to partants_master.jsonl (default: {INPUT_DEFAULT})",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("cote_pattern_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_cote_pattern_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
