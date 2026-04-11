#!/usr/bin/env python3
"""
feature_builders.seasonal_horse_form_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Seasonal horse form features -- tracking how each horse performs in
different seasons and calendar months.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - seasonal_horse_form.jsonl  in builder_outputs/seasonal_horse_form/

Features per partant (8):
  - shf_horse_month_wr         : horse's win rate in this calendar month historically
  - shf_horse_season_wr        : horse's win rate in this season (spring/summer/autumn/winter)
  - shf_horse_best_month       : month where horse has best win rate (1-12)
  - shf_is_best_month          : 1 if current month == horse's best month
  - shf_horse_winter_specialist: 1 if winter wr > 1.5x overall wr and min 3 winter runs
  - shf_horse_summer_specialist: same for summer
  - shf_season_runs_count      : how many times horse has run in this season before
  - shf_seasonal_position_avg  : horse's average position in this season historically

State per horse:
  month_stats  {month -> [wins, total, position_sum]}
  season_stats {season -> [wins, total]}
  overall      {wins, total}

Snapshot BEFORE update (strict temporal integrity).

Usage:
    python feature_builders/seasonal_horse_form_builder.py
    python feature_builders/seasonal_horse_form_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/seasonal_horse_form")

_LOG_EVERY = 500_000

# Season definitions: month -> season name
_MONTH_TO_SEASON = {
    12: "winter", 1: "winter", 2: "winter",
    3: "spring", 4: "spring", 5: "spring",
    6: "summer", 7: "summer", 8: "summer",
    9: "autumn", 10: "autumn", 11: "autumn",
}


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


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseSeasonalState:
    """Per-horse seasonal state tracker.

    month_stats:  {month(1-12) -> [wins, total, position_sum]}
    season_stats: {season_str  -> [wins, total]}
    overall:      [wins, total]
    """

    __slots__ = ("month_stats", "season_stats", "overall")

    def __init__(self) -> None:
        # month_stats: dict[int, list[int, int, float]]
        #   index 0 = wins, 1 = total, 2 = position_sum
        self.month_stats: dict[int, list] = {}
        # season_stats: dict[str, list[int, int]]
        #   index 0 = wins, 1 = total
        self.season_stats: dict[str, list] = {}
        # overall: [wins, total]
        self.overall: list[int] = [0, 0]

    def snapshot(self, month: int, season: str) -> dict[str, Any]:
        """Return feature dict BEFORE updating state."""
        feats: dict[str, Any] = {}

        # --- shf_horse_month_wr ---
        ms = self.month_stats.get(month)
        if ms is not None and ms[1] > 0:
            feats["shf_horse_month_wr"] = round(ms[0] / ms[1], 4)
        else:
            feats["shf_horse_month_wr"] = None

        # --- shf_horse_season_wr ---
        ss = self.season_stats.get(season)
        if ss is not None and ss[1] > 0:
            feats["shf_horse_season_wr"] = round(ss[0] / ss[1], 4)
        else:
            feats["shf_horse_season_wr"] = None

        # --- shf_horse_best_month ---
        best_month = None
        best_wr = -1.0
        for m, stats in self.month_stats.items():
            if stats[1] < 2:
                continue
            wr = stats[0] / stats[1]
            if wr > best_wr:
                best_wr = wr
                best_month = m
        feats["shf_horse_best_month"] = best_month

        # --- shf_is_best_month ---
        if best_month is not None:
            feats["shf_is_best_month"] = 1 if month == best_month else 0
        else:
            feats["shf_is_best_month"] = None

        # --- shf_horse_winter_specialist ---
        overall_wr = (self.overall[0] / self.overall[1]) if self.overall[1] > 0 else None
        winter_ss = self.season_stats.get("winter")
        if (
            winter_ss is not None
            and winter_ss[1] >= 3
            and overall_wr is not None
            and overall_wr > 0
        ):
            winter_wr = winter_ss[0] / winter_ss[1]
            feats["shf_horse_winter_specialist"] = 1 if winter_wr > 1.5 * overall_wr else 0
        else:
            feats["shf_horse_winter_specialist"] = None

        # --- shf_horse_summer_specialist ---
        summer_ss = self.season_stats.get("summer")
        if (
            summer_ss is not None
            and summer_ss[1] >= 3
            and overall_wr is not None
            and overall_wr > 0
        ):
            summer_wr = summer_ss[0] / summer_ss[1]
            feats["shf_horse_summer_specialist"] = 1 if summer_wr > 1.5 * overall_wr else 0
        else:
            feats["shf_horse_summer_specialist"] = None

        # --- shf_season_runs_count ---
        if ss is not None:
            feats["shf_season_runs_count"] = ss[1]
        else:
            feats["shf_season_runs_count"] = 0

        # --- shf_seasonal_position_avg ---
        if ms is not None and ms[1] > 0 and ms[2] > 0:
            feats["shf_seasonal_position_avg"] = round(ms[2] / ms[1], 4)
        else:
            feats["shf_seasonal_position_avg"] = None

        return feats

    def update(self, month: int, season: str, is_winner: bool, position: Optional[int]) -> None:
        """Update state AFTER snapshot."""
        # month_stats
        if month not in self.month_stats:
            self.month_stats[month] = [0, 0, 0.0]
        ms = self.month_stats[month]
        ms[1] += 1
        if is_winner:
            ms[0] += 1
        if position is not None and position > 0:
            ms[2] += position

        # season_stats
        if season not in self.season_stats:
            self.season_stats[season] = [0, 0]
        ss = self.season_stats[season]
        ss[1] += 1
        if is_winner:
            ss[0] += 1

        # overall
        self.overall[1] += 1
        if is_winner:
            self.overall[0] += 1


# ===========================================================================
# MAIN BUILD (index + sort + seek-based streaming output)
# ===========================================================================


def build_seasonal_horse_form_features(input_path: Path, output_path: Path, logger) -> int:
    """Build seasonal horse form features from partants_master.jsonl.

    Memory-optimised approach:
      Phase 1: Read only sort keys + file byte offsets (lightweight index).
      Phase 2: Sort chronologically.
      Phase 3: Seek-based re-read, snapshot BEFORE update, stream output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Seasonal Horse Form Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date_str, course_uid, num_pmu, byte_offset) --
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

    # -- Phase 3: Seek-based processing, streaming output --
    t2 = time.time()
    horse_states: dict[str, _HorseSeasonalState] = defaultdict(_HorseSeasonalState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "shf_horse_month_wr": 0,
        "shf_horse_season_wr": 0,
        "shf_horse_best_month": 0,
        "shf_is_best_month": 0,
        "shf_horse_winter_specialist": 0,
        "shf_horse_summer_specialist": 0,
        "shf_season_runs_count": 0,
        "shf_seasonal_position_avg": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

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

            # Parse date for month/season
            month: Optional[int] = None
            season: Optional[str] = None
            if course_date_str and len(course_date_str) >= 7:
                try:
                    month = int(course_date_str[5:7])
                    season = _MONTH_TO_SEASON.get(month)
                except (ValueError, TypeError):
                    pass

            # Read records from disk for this course
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                is_gagnant = bool(rec.get("is_gagnant"))
                position = _safe_int(rec.get("place_officielle") or rec.get("position"))
                course_records.append({
                    "uid": rec.get("partant_uid"),
                    "horse_id": horse_id,
                    "is_gagnant": is_gagnant,
                    "position": position,
                })

            # -- Snapshot BEFORE update (temporal integrity) --
            for rec in course_records:
                hid = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if hid and month is not None and season is not None:
                    feats = horse_states[hid].snapshot(month, season)
                    features.update(feats)
                    for k in fill_counts:
                        if feats.get(k) is not None:
                            fill_counts[k] += 1
                else:
                    features["shf_horse_month_wr"] = None
                    features["shf_horse_season_wr"] = None
                    features["shf_horse_best_month"] = None
                    features["shf_is_best_month"] = None
                    features["shf_horse_winter_specialist"] = None
                    features["shf_horse_summer_specialist"] = None
                    features["shf_season_runs_count"] = None
                    features["shf_seasonal_position_avg"] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER race --
            if month is not None and season is not None:
                for rec in course_records:
                    hid = rec["horse_id"]
                    if hid:
                        horse_states[hid].update(
                            month, season, rec["is_gagnant"], rec["position"],
                        )

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Seasonal horse form build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_states),
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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de forme saisonniere a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/seasonal_horse_form/)",
    )
    args = parser.parse_args()

    logger = setup_logging("seasonal_horse_form_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "seasonal_horse_form.jsonl"
    build_seasonal_horse_form_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
