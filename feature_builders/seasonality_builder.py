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

Usage:
    python feature_builders/seasonality_builder.py
    python feature_builders/seasonality_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

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
# STATE TRACKERS
# ===========================================================================


class _SeasonStats:
    """Tracks wins/total per season for an entity."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        # season_code -> count
        self.wins: dict[int, int] = defaultdict(int)
        self.total: dict[int, int] = defaultdict(int)

    def win_rate_for_season(self, season: int) -> Optional[float]:
        t = self.total.get(season, 0)
        if t == 0:
            return None
        return round(self.wins.get(season, 0) / t, 4)

    def overall_win_rate(self) -> Optional[float]:
        total_all = sum(self.total.values())
        if total_all == 0:
            return None
        return sum(self.wins.values()) / total_all

    def best_season(self) -> Optional[int]:
        """Return season with highest win rate (min 2 races)."""
        best_s = None
        best_wr = -1.0
        for s, t in self.total.items():
            if t < 2:
                continue
            wr = self.wins.get(s, 0) / t
            if wr > best_wr:
                best_wr = wr
                best_s = s
        return best_s


class _DisciplineSeasonFieldSize:
    """Tracks field sizes per (discipline, season, year) for trend calculation."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        # (discipline, season) -> list of (year, field_size)
        self.data: dict[tuple[str, int], list[tuple[int, int]]] = defaultdict(list)

    def add(self, discipline: str, season: int, year: int, field_size: int) -> None:
        self.data[(discipline, season)].append((year, field_size))

    def trend(self, discipline: str, season: int, current_year: int) -> Optional[float]:
        """Return growth rate of avg field size: positive = growing, negative = shrinking.

        Compares recent 2 years to older data. Returns None if not enough data.
        """
        entries = self.data.get((discipline, season))
        if not entries or len(entries) < 5:
            return None

        recent = [fs for yr, fs in entries if yr >= current_year - 1]
        older = [fs for yr, fs in entries if yr < current_year - 1]

        if not recent or not older:
            return None

        avg_recent = sum(recent) / len(recent)
        avg_older = sum(older) / len(older)

        if avg_older == 0:
            return None

        return round((avg_recent - avg_older) / avg_older, 4)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_seasonality_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build seasonality features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory.
      2. Sort chronologically for strict temporal ordering.
      3. Process record by record, snapshotting seasonal stats before updating.

    Temporal integrity: features reflect only races strictly before the
    current record's date -- no same-day leakage within a course group.
    """
    logger.info("=== Seasonality Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        nb_partants = rec.get("nombre_partants") or 0
        try:
            nb_partants = int(nb_partants)
        except (ValueError, TypeError):
            nb_partants = 0

        discipline = rec.get("discipline") or rec.get("type_course") or ""
        discipline = discipline.strip().upper()

        slim = {
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
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_season: dict[str, _SeasonStats] = defaultdict(_SeasonStats)
    hippo_season: dict[str, _SeasonStats] = defaultdict(_SeasonStats)
    disc_field: _DisciplineSeasonFieldSize = _DisciplineSeasonFieldSize()

    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants for this course
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        race_date = _parse_date(course_date_str)
        if race_date:
            current_season = _MONTH_TO_SEASON[race_date.month]
            current_year = race_date.year
        else:
            current_season = None
            current_year = None

        # -- Snapshot pre-race stats for all partants (temporal integrity) --
        pre_race_features: list[dict[str, Any]] = []

        for rec in course_group:
            cheval = rec["cheval"]
            hippo = rec["hippo"]
            discipline = rec["discipline"]

            features: dict[str, Any] = {"partant_uid": rec["uid"]}

            if cheval and current_season is not None:
                hs = horse_season[cheval]
                features["horse_season_win_rate"] = hs.win_rate_for_season(current_season)

                best_s = hs.best_season()
                features["horse_best_season"] = best_s

                if best_s is not None:
                    if best_s == current_season:
                        features["season_match_score"] = 1.0
                    elif _seasons_adjacent(best_s, current_season):
                        features["season_match_score"] = 0.5
                    else:
                        features["season_match_score"] = 0.0
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
                else:
                    features["hippo_season_bias"] = None
            else:
                features["hippo_season_bias"] = None

            # Discipline seasonal trend
            if discipline and current_season is not None and current_year is not None:
                features["discipline_seasonal_trend"] = disc_field.trend(
                    discipline, current_season, current_year
                )
            else:
                features["discipline_seasonal_trend"] = None

            pre_race_features.append(features)

        # Emit features (pre-race snapshot -- no leakage)
        results.extend(pre_race_features)

        # -- Update states after race --
        # Track which courses we already counted for disc_field (one entry per course)
        course_field_tracked = False

        for rec in course_group:
            cheval = rec["cheval"]
            hippo = rec["hippo"]
            discipline = rec["discipline"]

            if current_season is None:
                continue

            if cheval:
                hs = horse_season[cheval]
                hs.total[current_season] += 1
                if rec["gagnant"]:
                    hs.wins[current_season] += 1

            if hippo:
                hs_hippo = hippo_season[hippo]
                hs_hippo.total[current_season] += 1
                if rec["gagnant"]:
                    hs_hippo.wins[current_season] += 1

            # Field size: once per course
            if not course_field_tracked and discipline and current_year is not None:
                nb = rec["nb_partants"]
                if nb > 0:
                    disc_field.add(discipline, current_season, current_year, nb)
                    course_field_tracked = True

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Seasonality build termine: %d features en %.1fs (chevaux: %d, hippos: %d)",
        len(results), elapsed, len(horse_season), len(hippo_season),
    )

    return results


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

    results = build_seasonality_features(input_path, logger)

    # Save
    out_path = output_dir / "seasonality.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
