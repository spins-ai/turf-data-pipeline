#!/usr/bin/env python3
"""
feature_builders.trainer_seasonal_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer seasonal performance features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant trainer-seasonal features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - trainer_seasonal.jsonl  in builder_outputs/trainer_seasonal/

Features per partant:
  - trs_trainer_month_wr       : trainer's win rate in this calendar month
  - trs_trainer_season_wr      : trainer's win rate in this season
  - trs_trainer_best_month     : month where trainer performs best (1-12)
  - trs_is_trainer_best_month  : 1 if current month matches trainer's best
  - trs_trainer_winter_specialist : 1 if winter wr > 1.5x overall wr (min 10)
  - trs_trainer_summer_specialist : same for summer
  - trs_trainer_year_form      : trainer's win rate in current calendar year
  - trs_trainer_improving      : 1 if current year's wr > previous year's wr

Usage:
    python feature_builders/trainer_seasonal_builder.py
    python feature_builders/trainer_seasonal_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_seasonal")

_LOG_EVERY = 500_000

# Season definitions: month -> season name
_MONTH_TO_SEASON = {
    1: "winter", 2: "winter",
    3: "spring", 4: "spring", 5: "spring",
    6: "summer", 7: "summer", 8: "summer",
    9: "autumn", 10: "autumn", 11: "autumn",
    12: "winter",
}


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_month_year(date_str: str) -> tuple[Optional[int], Optional[int]]:
    """Extract month and year from ISO date string. Returns (month, year) or (None, None)."""
    if not date_str or len(date_str) < 7:
        return None, None
    try:
        year = int(date_str[:4])
        month = int(date_str[5:7])
        if 1 <= month <= 12 and 1900 <= year <= 2100:
            return month, year
    except (ValueError, TypeError):
        pass
    return None, None


# ===========================================================================
# PER-TRAINER STATE
# ===========================================================================


class _TrainerSeasonalState:
    """Per-trainer accumulated seasonal state.

    Tracks:
      - month_stats: {month(1-12) -> [wins, total]}
      - season_stats: {season_name -> [wins, total]}
      - year_stats: {year -> [wins, total]}
      - overall: [wins, total]
    """

    __slots__ = ("month_stats", "season_stats", "year_stats", "overall")

    def __init__(self) -> None:
        self.month_stats: dict[int, list[int]] = defaultdict(lambda: [0, 0])
        self.season_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.year_stats: dict[int, list[int]] = defaultdict(lambda: [0, 0])
        self.overall: list[int] = [0, 0]  # [wins, total]

    def snapshot(self, month: Optional[int], year: Optional[int]) -> dict[str, Any]:
        """Compute features using only past data (strict temporal). BEFORE update."""
        feats: dict[str, Any] = {
            "trs_trainer_month_wr": None,
            "trs_trainer_season_wr": None,
            "trs_trainer_best_month": None,
            "trs_is_trainer_best_month": None,
            "trs_trainer_winter_specialist": None,
            "trs_trainer_summer_specialist": None,
            "trs_trainer_year_form": None,
            "trs_trainer_improving": None,
        }

        if month is None or year is None:
            return feats

        # -- month win rate --
        ms = self.month_stats.get(month)
        if ms and ms[1] > 0:
            feats["trs_trainer_month_wr"] = round(ms[0] / ms[1], 4)

        # -- season win rate --
        season = _MONTH_TO_SEASON[month]
        ss = self.season_stats.get(season)
        if ss and ss[1] > 0:
            feats["trs_trainer_season_wr"] = round(ss[0] / ss[1], 4)

        # -- best month (min 3 races in a month) --
        best_month = None
        best_wr = -1.0
        for m, (w, t) in self.month_stats.items():
            if t >= 3:
                wr = w / t
                if wr > best_wr:
                    best_wr = wr
                    best_month = m
        feats["trs_trainer_best_month"] = best_month
        if best_month is not None:
            feats["trs_is_trainer_best_month"] = 1 if month == best_month else 0

        # -- specialist flags (min 10 races in that season) --
        overall_total = self.overall[1]
        overall_wr = (self.overall[0] / overall_total) if overall_total > 0 else 0.0

        for season_name, feat_key in [
            ("winter", "trs_trainer_winter_specialist"),
            ("summer", "trs_trainer_summer_specialist"),
        ]:
            ss_spec = self.season_stats.get(season_name)
            if ss_spec and ss_spec[1] >= 10 and overall_wr > 0:
                season_wr = ss_spec[0] / ss_spec[1]
                feats[feat_key] = 1 if season_wr > 1.5 * overall_wr else 0

        # -- year form --
        ys = self.year_stats.get(year)
        if ys and ys[1] > 0:
            feats["trs_trainer_year_form"] = round(ys[0] / ys[1], 4)

        # -- improving: current year wr > previous year wr --
        prev_year = year - 1
        ys_prev = self.year_stats.get(prev_year)
        if ys and ys[1] > 0 and ys_prev and ys_prev[1] > 0:
            curr_wr = ys[0] / ys[1]
            prev_wr = ys_prev[0] / ys_prev[1]
            feats["trs_trainer_improving"] = 1 if curr_wr > prev_wr else 0

        return feats

    def update(self, month: int, year: int, is_winner: bool) -> None:
        """Update state with a new race result (post-race)."""
        season = _MONTH_TO_SEASON[month]
        win_int = int(is_winner)

        self.month_stats[month][0] += win_int
        self.month_stats[month][1] += 1

        self.season_stats[season][0] += win_int
        self.season_stats[season][1] += 1

        self.year_stats[year][0] += win_int
        self.year_stats[year][1] += 1

        self.overall[0] += win_int
        self.overall[1] += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek-based)
# ===========================================================================


def build_trainer_seasonal_features(input_path: Path, output_path: Path, logger) -> int:
    """Build trainer seasonal features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Trainer Seasonal Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
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
    trainer_states: dict[str, _TrainerSeasonalState] = defaultdict(_TrainerSeasonalState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "trs_trainer_month_wr": 0,
        "trs_trainer_season_wr": 0,
        "trs_trainer_best_month": 0,
        "trs_is_trainer_best_month": 0,
        "trs_trainer_winter_specialist": 0,
        "trs_trainer_summer_specialist": 0,
        "trs_trainer_year_form": 0,
        "trs_trainer_improving": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", "") or "",
                "course": rec.get("course_uid", "") or "",
                "num": rec.get("num_pmu", 0) or 0,
                "entraineur": (rec.get("entraineur") or "").strip(),
                "is_gagnant": bool(rec.get("is_gagnant")),
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

            # Parse month/year from course date
            month, year = _parse_month_year(course_date_str)

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            for rec in course_group:
                entraineur = rec["entraineur"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if entraineur and month is not None and year is not None:
                    state = trainer_states[entraineur]
                    snap = state.snapshot(month, year)
                    features.update(snap)
                    for k, v in snap.items():
                        if v is not None:
                            fill_counts[k] += 1
                else:
                    features.update({
                        "trs_trainer_month_wr": None,
                        "trs_trainer_season_wr": None,
                        "trs_trainer_best_month": None,
                        "trs_is_trainer_best_month": None,
                        "trs_trainer_winter_specialist": None,
                        "trs_trainer_summer_specialist": None,
                        "trs_trainer_year_form": None,
                        "trs_trainer_improving": None,
                    })

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after snapshotting (post-race) --
            for rec in course_group:
                entraineur = rec["entraineur"]
                if entraineur and month is not None and year is not None:
                    trainer_states[entraineur].update(month, year, rec["is_gagnant"])

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Trainer seasonal build termine: %d features en %.1fs (entraineurs uniques: %d)",
        n_written, elapsed, len(trainer_states),
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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features trainer seasonal a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/trainer_seasonal/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_seasonal_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "trainer_seasonal.jsonl"
    build_trainer_seasonal_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
