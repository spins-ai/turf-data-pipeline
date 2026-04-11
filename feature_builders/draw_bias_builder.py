#!/usr/bin/env python3
"""
feature_builders.draw_bias_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Analyzes historical draw (post position) bias at each hippodrome and distance.

Reads partants_master.jsonl in streaming mode with an index+sort+seek approach,
processes all records chronologically, and computes per-partant draw-bias
features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the draw statistics -- no future leakage.

Produces:
  - draw_bias.jsonl   in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/draw_bias/

Features per partant (8):
  - db_draw_win_rate         : historical win rate for this draw position (num_pmu)
                               at this hippodrome
  - db_draw_place_rate       : historical place rate (top 3) for this draw at
                               this hippodrome
  - db_draw_vs_avg           : db_draw_win_rate - average win rate across all
                               draws at this hippodrome
  - db_inner_draw            : 1 if num_pmu <= 4 (inner draw), else 0
  - db_outer_draw            : 1 if num_pmu > nombre_partants * 0.7, else 0
  - db_draw_position_pct     : num_pmu / nombre_partants (normalized 0-1)
  - db_best_draw_in_race     : 1 if this horse has the historically best-performing
                               draw in the race (by db_draw_win_rate)
  - db_hippo_distance_draw_wr: win rate for this draw at this specific
                               hippodrome + distance-bucket combo

State:
  - dict[(hippodrome, draw_position)] -> {wins, places, total}
  - dict[(hippodrome, distance_bucket, draw_position)] -> {wins, total}

Key fields: num_pmu, hippodrome, distance, nombre_partants, position_arrivee,
            date_reunion_iso, partant_uid, course_uid.

Usage:
    python feature_builders/draw_bias_builder.py
    python feature_builders/draw_bias_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
    python feature_builders/draw_bias_builder.py --output-dir D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/draw_bias
"""

from __future__ import annotations

import argparse
import gc
import json
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/draw_bias")

# Fallback candidates when the primary path is absent
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# Distance buckets (metres): <1200, 1200-1599, 1600-1999, 2000-2399, 2400+
_DIST_BREAKPOINTS = [1200, 1600, 2000, 2400]


# ===========================================================================
# HELPERS
# ===========================================================================


def _distance_bucket(distance: Any) -> Optional[str]:
    """Convert raw distance value to a coarse bucket string, or None."""
    try:
        d = int(distance)
    except (TypeError, ValueError):
        return None
    if d <= 0:
        return None
    if d < _DIST_BREAKPOINTS[0]:
        return "lt1200"
    for i in range(1, len(_DIST_BREAKPOINTS)):
        if d < _DIST_BREAKPOINTS[i]:
            lo = _DIST_BREAKPOINTS[i - 1]
            hi = _DIST_BREAKPOINTS[i]
            return f"{lo}-{hi}"
    return "ge2400"


def _is_placed(position: Any) -> bool:
    """Return True if position_arrivee indicates a top-3 finish."""
    try:
        return int(position) <= 3
    except (TypeError, ValueError):
        return False


def _is_winner(position: Any) -> bool:
    """Return True if position_arrivee == 1."""
    try:
        return int(position) == 1
    except (TypeError, ValueError):
        return False


# ===========================================================================
# STATE CONTAINERS
# ===========================================================================


class _DrawStats:
    """Wins / places / total counter for one (hippodrome, draw) key.

    Uses __slots__ to reduce per-object memory overhead when millions of
    distinct keys exist.
    """

    __slots__ = ("wins", "places", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.total: int = 0

    def win_rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return round(self.wins / self.total, 6)

    def place_rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return round(self.places / self.total, 6)


class _HippoDrawState:
    """All draw stats for a single hippodrome.

    Keeps a per-draw _DrawStats dict and a dirty flag so that the average
    win rate across draws is only recomputed when the state has changed.
    """

    __slots__ = ("draws", "_avg_wr_cache", "_dirty")

    def __init__(self) -> None:
        self.draws: dict[int, _DrawStats] = defaultdict(_DrawStats)
        self._avg_wr_cache: Optional[float] = None
        self._dirty: bool = False

    def avg_win_rate(self) -> Optional[float]:
        """Average win rate across all draw positions seen at this hippodrome."""
        if not self._dirty and self._avg_wr_cache is not None:
            return self._avg_wr_cache
        rates = [s.win_rate() for s in self.draws.values() if s.win_rate() is not None]
        if not rates:
            self._avg_wr_cache = None
        else:
            self._avg_wr_cache = round(sum(rates) / len(rates), 6)
        self._dirty = False
        return self._avg_wr_cache

    def mark_dirty(self) -> None:
        self._dirty = True


class _HippoDistDrawStats:
    """Wins / total counter for one (hippodrome, distance_bucket, draw) key."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def win_rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return round(self.wins / self.total, 6)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_draw_bias_features(input_path: Path, output_path: Path, logger) -> int:
    """Build draw-bias features from partants_master.jsonl.

    Memory-optimised approach (index + sort + seek):
      1. Read only sort keys + file byte offsets into memory (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process race by race,
         emit features BEFORE updating state (temporal integrity), then
         update state.

    Returns the total number of feature records written.
    """
    logger.info("=== Draw Bias Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # -----------------------------------------------------------------------
    # Phase 1: Build lightweight index (date_str, course_uid, num_pmu, offset)
    # -----------------------------------------------------------------------
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
                logger.info("  Indexed %d records...", n_read)

            date_str = rec.get("date_reunion_iso") or ""
            course_uid = rec.get("course_uid") or ""
            try:
                num_pmu = int(rec.get("num_pmu") or 0)
            except (TypeError, ValueError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 complete: %d records indexed in %.1fs",
        len(index), time.time() - t0,
    )

    # -----------------------------------------------------------------------
    # Phase 2: Sort chronologically (date, then course_uid, then num_pmu)
    # -----------------------------------------------------------------------
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Chronological sort done in %.1fs", time.time() - t1)

    # -----------------------------------------------------------------------
    # Phase 3: Stream through races, compute features before updating state
    # -----------------------------------------------------------------------

    # State: hippodrome -> _HippoDrawState
    hippo_draw: dict[str, _HippoDrawState] = defaultdict(_HippoDrawState)

    # State: (hippodrome, distance_bucket, draw_position) -> _HippoDistDrawStats
    hippo_dist_draw: dict[tuple[str, str, int], _HippoDistDrawStats] = defaultdict(
        _HippoDistDrawStats
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts: dict[str, int] = {
        "db_draw_win_rate": 0,
        "db_draw_place_rate": 0,
        "db_draw_vs_avg": 0,
        "db_inner_draw": 0,
        "db_outer_draw": 0,
        "db_draw_position_pct": 0,
        "db_best_draw_in_race": 0,
        "db_hippo_distance_draw_wr": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(off: int) -> dict:
            fin.seek(off)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect all index entries belonging to this course_uid
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

            # Read full records for this race from disk
            race_records: list[dict] = [_read_at(index[ci][3]) for ci in course_indices]

            # -------------------------------------------------------------------
            # Extract per-runner minimal fields
            # -------------------------------------------------------------------
            runners: list[dict[str, Any]] = []
            for rec in race_records:
                try:
                    draw = int(rec.get("num_pmu") or 0)
                except (TypeError, ValueError):
                    draw = 0

                try:
                    nb_partants = int(rec.get("nombre_partants") or 0)
                except (TypeError, ValueError):
                    nb_partants = 0

                hippo = (
                    rec.get("hippodrome_normalise")
                    or rec.get("hippodrome")
                    or ""
                ).strip()
                dist_bucket = _distance_bucket(rec.get("distance"))
                pos = rec.get("position_arrivee")

                runners.append({
                    "uid": rec.get("partant_uid"),
                    "draw": draw,
                    "nb_partants": nb_partants,
                    "hippo": hippo,
                    "dist_bucket": dist_bucket,
                    "winner": _is_winner(pos),
                    "placed": _is_placed(pos),
                })

            # -------------------------------------------------------------------
            # Step A: Snapshot pre-race features (BEFORE state update)
            # -------------------------------------------------------------------

            # Pre-compute db_draw_win_rate for each runner (needed for best_draw)
            runner_draw_wrs: list[Optional[float]] = []
            for r in runners:
                hippo = r["hippo"]
                draw = r["draw"]
                if hippo and draw:
                    wr = hippo_draw[hippo].draws[draw].win_rate()
                else:
                    wr = None
                runner_draw_wrs.append(wr)

            # Best win rate seen in this race
            non_none_wrs = [wr for wr in runner_draw_wrs if wr is not None]
            best_wr_in_race: Optional[float] = max(non_none_wrs) if non_none_wrs else None

            for idx_r, r in enumerate(runners):
                uid = r["uid"]
                draw = r["draw"]
                nb_partants = r["nb_partants"]
                hippo = r["hippo"]
                dist_bucket = r["dist_bucket"]

                feat: dict[str, Any] = {"partant_uid": uid}

                # db_draw_win_rate  &  db_draw_place_rate  &  db_draw_vs_avg
                if hippo and draw:
                    dstats = hippo_draw[hippo].draws[draw]

                    wr = dstats.win_rate()
                    feat["db_draw_win_rate"] = wr
                    if wr is not None:
                        fill_counts["db_draw_win_rate"] += 1

                    pr = dstats.place_rate()
                    feat["db_draw_place_rate"] = pr
                    if pr is not None:
                        fill_counts["db_draw_place_rate"] += 1

                    avg_wr = hippo_draw[hippo].avg_win_rate()
                    if wr is not None and avg_wr is not None:
                        feat["db_draw_vs_avg"] = round(wr - avg_wr, 6)
                        fill_counts["db_draw_vs_avg"] += 1
                    else:
                        feat["db_draw_vs_avg"] = None
                else:
                    feat["db_draw_win_rate"] = None
                    feat["db_draw_place_rate"] = None
                    feat["db_draw_vs_avg"] = None

                # db_inner_draw
                if draw > 0:
                    feat["db_inner_draw"] = 1 if draw <= 4 else 0
                    fill_counts["db_inner_draw"] += 1
                else:
                    feat["db_inner_draw"] = None

                # db_outer_draw
                if draw > 0 and nb_partants > 0:
                    feat["db_outer_draw"] = 1 if draw > nb_partants * 0.7 else 0
                    fill_counts["db_outer_draw"] += 1
                else:
                    feat["db_outer_draw"] = None

                # db_draw_position_pct
                if draw > 0 and nb_partants > 0:
                    feat["db_draw_position_pct"] = round(draw / nb_partants, 6)
                    fill_counts["db_draw_position_pct"] += 1
                else:
                    feat["db_draw_position_pct"] = None

                # db_best_draw_in_race
                runner_wr = runner_draw_wrs[idx_r]
                if best_wr_in_race is not None and runner_wr is not None:
                    feat["db_best_draw_in_race"] = 1 if runner_wr >= best_wr_in_race else 0
                    fill_counts["db_best_draw_in_race"] += 1
                else:
                    feat["db_best_draw_in_race"] = None

                # db_hippo_distance_draw_wr
                if hippo and dist_bucket and draw:
                    hdd_key = (hippo, dist_bucket, draw)
                    hdd_wr = hippo_dist_draw[hdd_key].win_rate()
                    feat["db_hippo_distance_draw_wr"] = hdd_wr
                    if hdd_wr is not None:
                        fill_counts["db_hippo_distance_draw_wr"] += 1
                else:
                    feat["db_hippo_distance_draw_wr"] = None

                fout.write(json.dumps(feat, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -------------------------------------------------------------------
            # Step B: Update state from race results (after emitting features)
            # -------------------------------------------------------------------
            for r in runners:
                hippo = r["hippo"]
                draw = r["draw"]
                dist_bucket = r["dist_bucket"]

                if not hippo or not draw:
                    continue

                hs = hippo_draw[hippo]
                dstats = hs.draws[draw]
                dstats.total += 1
                if r["winner"]:
                    dstats.wins += 1
                if r["placed"]:
                    dstats.places += 1
                hs.mark_dirty()

                if dist_bucket:
                    hdd_key = (hippo, dist_bucket, draw)
                    hdd = hippo_dist_draw[hdd_key]
                    hdd.total += 1
                    if r["winner"]:
                        hdd.wins += 1

            n_processed += len(runners)
            if n_processed % _LOG_EVERY < len(runners):
                logger.info(
                    "  Processed %d / %d records...", n_processed, total
                )
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Draw bias build complete: %d feature rows in %.1fs "
        "(hippodromes: %d, hippo+dist+draw keys: %d)",
        n_written, elapsed, len(hippo_draw), len(hippo_dist_draw),
    )

    # Fill-rate summary
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0.0
        logger.info("  %-35s %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _resolve_input(cli_path: Optional[str]) -> Path:
    """Return a valid input path or raise FileNotFoundError."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"File not found: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "No partants_master.jsonl found. Tried: "
        + ", ".join(str(c) for c in _INPUT_CANDIDATES)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build draw-bias features from partants_master.jsonl"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Path to partants_master.jsonl (auto-detected if omitted)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=(
            "Output directory "
            "(default: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/draw_bias)"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("draw_bias_builder")

    input_path = _resolve_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "draw_bias.jsonl"
    build_draw_bias_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
