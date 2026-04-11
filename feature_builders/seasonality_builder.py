#!/usr/bin/env python3
"""
feature_builders.seasonality_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Seasonal performance features for horses, hippodromes, and disciplines.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant seasonality features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the seasonal stats -- no future leakage.

Produces:
  - seasonality.jsonl   in output/seasonality/

Features per partant:
  - horse_season_win_rate      : horse's win rate in current season (spring/summer/autumn/winter)
  - horse_best_season          : which season horse performs best in (1=spring..4=winter)
  - season_match_score         : 1.0 if current=best, 0.5 adjacent, 0.0 opposite
  - hippo_season_bias          : this hippodrome's win-rate deviation in current season
  - discipline_seasonal_trend  : avg field size growth rate for this discipline in current season

Memory-optimised version:
  - Phase 1 reads only minimal tuples (not full dicts) for sorting
  - Phase 2 streams output to disk instead of accumulating in a list
  - DisciplineSeasonFieldSize uses aggregated counters, not raw lists
  - gc.collect() called every 500K records

Usage:
    python feature_builders/seasonality_builder.py
    python feature_builders/seasonality_builder.py --input data_master/partants_master.jsonl
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
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "seasonality"

# Progress log every N records
_LOG_EVERY = 500_000

# Season definitions: month -> season code (1=spring, 2=summer, 3=autumn, 4=winter)
_MONTH_TO_SEASON = {
    1: 4, 2: 4, 3: 1, 4: 1, 5: 1,
    6: 2, 7: 2, 8: 2,
    9: 3, 10: 3, 11: 3,
    12: 4,
}

# Season adjacency: two seasons are adjacent if they differ by 1 (mod 4)
_SEASON_NAMES = {1: "spring", 2: "summer", 3: "autumn", 4: "winter"}


def _seasons_adjacent(s1: int, s2: int) -> bool:
    """Return True if seasons are adjacent (differ by 1 on circular ring)."""
    diff = abs(s1 - s2)
    return diff == 1 or diff == 3  # 3 means wrapping (winter/spring)


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


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKERS (memory-optimised)
# ===========================================================================


class _SeasonStats:
    """Tracks wins/total per season for an entity.

    Uses fixed-size arrays (4 seasons) instead of dicts to save memory.
    Index 0=season1(spring), 1=season2(summer), 2=season3(autumn), 3=season4(winter).
    """

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        # Fixed arrays: index = season_code - 1
        self.wins = [0, 0, 0, 0]
        self.total = [0, 0, 0, 0]

    def win_rate_for_season(self, season: int) -> Optional[float]:
        idx = season - 1
        t = self.total[idx]
        if t == 0:
            return None
        return round(self.wins[idx] / t, 4)

    def overall_win_rate(self) -> Optional[float]:
        total_all = sum(self.total)
        if total_all == 0:
            return None
        return sum(self.wins) / total_all

    def best_season(self) -> Optional[int]:
        """Return season with highest win rate (min 2 races)."""
        best_s = None
        best_wr = -1.0
        for i in range(4):
            t = self.total[i]
            if t < 2:
                continue
            wr = self.wins[i] / t
            if wr > best_wr:
                best_wr = wr
                best_s = i + 1  # season codes are 1-based
        return best_s


class _DisciplineSeasonFieldSize:
    """Tracks field sizes per (discipline, season) using aggregated counters.

    Instead of storing every (year, field_size) tuple (unbounded), we keep
    per-year aggregates: sum and count, which is O(years) not O(records).
    """

    __slots__ = ("data",)

    def __init__(self) -> None:
        # (discipline, season) -> {year: [sum_field_size, count]}
        self.data: dict[tuple[str, int], dict[int, list]] = defaultdict(dict)

    def add(self, discipline: str, season: int, year: int, field_size: int) -> None:
        year_agg = self.data[(discipline, season)]
        if year in year_agg:
            year_agg[year][0] += field_size
            year_agg[year][1] += 1
        else:
            year_agg[year] = [field_size, 1]

    def trend(self, discipline: str, season: int, current_year: int) -> Optional[float]:
        """Return growth rate of avg field size: positive = growing, negative = shrinking.

        Compares recent 2 years to older data. Returns None if not enough data.
        """
        year_agg = self.data.get((discipline, season))
        if not year_agg:
            return None

        total_entries = sum(v[1] for v in year_agg.values())
        if total_entries < 5:
            return None

        recent_sum = 0
        recent_count = 0
        older_sum = 0
        older_count = 0

        for yr, (s, c) in year_agg.items():
            if yr >= current_year - 1:
                recent_sum += s
                recent_count += c
            else:
                older_sum += s
                older_count += c

        if not recent_count or not older_count:
            return None

        avg_recent = recent_sum / recent_count
        avg_older = older_sum / older_count

        if avg_older == 0:
            return None

        return round((avg_recent - avg_older) / avg_older, 4)


# ===========================================================================
# MAIN BUILD (memory-optimised: chunked sort + streaming output)
# ===========================================================================


def build_seasonality_features(input_path: Path, output_path: Path, logger) -> int:
    """Build seasonality features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Seasonality Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
    # Each entry is a tuple: (date_str, course_uid, num_pmu, byte_offset)
    # stored as raw data to minimise memory per record.
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

    # -- Phase 2: Sort the lightweight index --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_season: dict[str, _SeasonStats] = defaultdict(_SeasonStats)
    hippo_season: dict[str, _SeasonStats] = defaultdict(_SeasonStats)
    disc_field = _DisciplineSeasonFieldSize()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "horse_season_win_rate": 0,
        "horse_best_season": 0,
        "season_match_score": 0,
        "hippo_season_bias": 0,
        "discipline_seasonal_trend": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            nb_partants = rec.get("nombre_partants") or 0
            try:
                nb_partants = int(nb_partants)
            except (ValueError, TypeError):
                nb_partants = 0

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "cheval": rec.get("nom_cheval"),
                "gagnant": bool(rec.get("is_gagnant")),
                "hippo": rec.get("hippodrome_normalise", ""),
                "discipline": discipline,
                "nb_partants": nb_partants,
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

            # Read only this course's records from disk
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            race_date = _parse_date(course_date_str)
            if race_date:
                current_season = _MONTH_TO_SEASON[race_date.month]
                current_year = race_date.year
            else:
                current_season = None
                current_year = None

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                hippo = rec["hippo"]
                discipline = rec["discipline"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if cheval and current_season is not None:
                    hs = horse_season[cheval]
                    wr = hs.win_rate_for_season(current_season)
                    features["horse_season_win_rate"] = wr
                    if wr is not None:
                        fill_counts["horse_season_win_rate"] += 1

                    best_s = hs.best_season()
                    features["horse_best_season"] = best_s
                    if best_s is not None:
                        fill_counts["horse_best_season"] += 1

                    if best_s is not None:
                        if best_s == current_season:
                            features["season_match_score"] = 1.0
                        elif _seasons_adjacent(best_s, current_season):
                            features["season_match_score"] = 0.5
                        else:
                            features["season_match_score"] = 0.0
                        fill_counts["season_match_score"] += 1
                    else:
                        features["season_match_score"] = None
                else:
                    features["horse_season_win_rate"] = None
                    features["horse_best_season"] = None
                    features["season_match_score"] = None

                # Hippodrome season bias: hippo win rate in this season vs overall
                if hippo and current_season is not None:
                    hs_hippo = hippo_season[hippo]
                    season_wr = hs_hippo.win_rate_for_season(current_season)
                    overall_wr = hs_hippo.overall_win_rate()
                    if season_wr is not None and overall_wr is not None and overall_wr > 0:
                        features["hippo_season_bias"] = round(season_wr - overall_wr, 4)
                        fill_counts["hippo_season_bias"] += 1
                    else:
                        features["hippo_season_bias"] = None
                else:
                    features["hippo_season_bias"] = None

                # Discipline seasonal trend
                if discipline and current_season is not None and current_year is not None:
                    trend_val = disc_field.trend(discipline, current_season, current_year)
                    features["discipline_seasonal_trend"] = trend_val
                    if trend_val is not None:
                        fill_counts["discipline_seasonal_trend"] += 1
                else:
                    features["discipline_seasonal_trend"] = None

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after race --
            course_field_tracked = False

            for rec in course_group:
                cheval = rec["cheval"]
                hippo = rec["hippo"]
                discipline = rec["discipline"]

                if current_season is None:
                    continue

                if cheval:
                    hs = horse_season[cheval]
                    hs.total[current_season - 1] += 1
                    if rec["gagnant"]:
                        hs.wins[current_season - 1] += 1

                if hippo:
                    hs_hippo = hippo_season[hippo]
                    hs_hippo.total[current_season - 1] += 1
                    if rec["gagnant"]:
                        hs_hippo.wins[current_season - 1] += 1

                # Field size: once per course
                if not course_field_tracked and discipline and current_year is not None:
                    nb = rec["nb_partants"]
                    if nb > 0:
                        disc_field.add(discipline, current_season, current_year, nb)
                        course_field_tracked = True

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                # Periodic garbage collection to keep memory in check
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Seasonality build termine: %d features en %.1fs (chevaux: %d, hippos: %d)",
        n_written, elapsed, len(horse_season), len(hippo_season),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written if n_written else 0)

    return n_written


# ===========================================================================
# SAUVEGARDE & CLI
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
        description="Construction des features saisonnalite a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/seasonality/)",
    )
    args = parser.parse_args()

    logger = setup_logging("seasonality_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "seasonality.jsonl"
    build_seasonality_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
