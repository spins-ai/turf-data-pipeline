#!/usr/bin/env python3
"""
feature_builders.concept_drift_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Concept drift detection features -- detect when the distribution of racing
data changes over time (new hippodromes, rule changes, market evolution).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically using index + seek, and computes per-partant concept drift
features via global rolling statistics.

Temporal integrity: for any partant at date D, only races with date < D
contribute to drift statistics -- no future leakage.  State is snapshotted
BEFORE being updated with the current race.

Produces:
  - concept_drift_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/concept_drift_features/

Features per partant (8):
  - cdf_global_favorite_winrate_trend  : rolling favorite win rate over last 1000 races
  - cdf_global_avg_field_size_trend    : rolling avg field size over last 1000 races
  - cdf_global_avg_cote_winner_trend   : rolling avg winning odds over last 1000 races
  - cdf_hippo_new_flag                 : 1 if hippodrome not seen in last 500 races
  - cdf_discipline_proportion_shift    : proportion of discipline in last 500 vs 500-1000
  - cdf_market_efficiency_trend        : rolling |1/cote_winner - 1/nb_partants| over 500
  - cdf_days_since_data_start          : days from first record to current race
  - cdf_record_density                 : number of races in last 30 days

Usage:
    python feature_builders/concept_drift_features_builder.py
    python feature_builders/concept_drift_features_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/concept_drift_features")

_GC_EVERY = 500_000
_LOG_EVERY = 500_000

# Rolling window sizes (in races, not records)
_GLOBAL_WINDOW = 1000
_RECENT_WINDOW = 500
_DENSITY_DAYS = 30


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _safe_float(val, default=None) -> Optional[float]:
    """Convert a value to float safely."""
    if val is None:
        return default
    try:
        f = float(val)
        return f if f == f else default  # NaN check
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    """Convert a value to int safely."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _ConceptDriftState:
    """Global rolling state for concept drift features.

    Tracks per-race (not per-partant) statistics in deques.
    Each race contributes one entry to the global deques.
    """

    def __init__(self) -> None:
        # Global deque of per-race tuples: (is_favorite_win, field_size, cote_winner, discipline, hippodrome)
        self.global_races: deque = deque(maxlen=_GLOBAL_WINDOW)

        # Set of hippodromes seen in last 500 races (maintained from global_races)
        # We recompute from deque tail on demand -- no separate set needed

        # Date tracking
        self.first_date: Optional[datetime] = None

        # Race dates deque for density calculation (stores datetime objects)
        self.race_dates: deque = deque(maxlen=10_000)  # generous buffer for 30-day lookback

    def snapshot_features(self, hippodrome: str, discipline: str,
                          race_date: Optional[datetime]) -> dict[str, Any]:
        """Snapshot all 8 concept drift features BEFORE updating state."""
        features: dict[str, Any] = {}
        n = len(self.global_races)

        # --- cdf_global_favorite_winrate_trend ---
        if n >= 20:
            fav_wins = sum(1 for r in self.global_races if r[0])
            features["cdf_global_favorite_winrate_trend"] = round(fav_wins / n, 6)
        else:
            features["cdf_global_favorite_winrate_trend"] = None

        # --- cdf_global_avg_field_size_trend ---
        if n >= 20:
            field_sizes = [r[1] for r in self.global_races if r[1] and r[1] > 0]
            if field_sizes:
                features["cdf_global_avg_field_size_trend"] = round(
                    sum(field_sizes) / len(field_sizes), 4
                )
            else:
                features["cdf_global_avg_field_size_trend"] = None
        else:
            features["cdf_global_avg_field_size_trend"] = None

        # --- cdf_global_avg_cote_winner_trend ---
        if n >= 20:
            cotes = [r[2] for r in self.global_races if r[2] is not None and r[2] > 0]
            if cotes:
                features["cdf_global_avg_cote_winner_trend"] = round(
                    sum(cotes) / len(cotes), 4
                )
            else:
                features["cdf_global_avg_cote_winner_trend"] = None
        else:
            features["cdf_global_avg_cote_winner_trend"] = None

        # --- cdf_hippo_new_flag ---
        # Check if hippodrome appears in last 500 races
        if hippodrome and n > 0:
            recent_slice = list(self.global_races)[-_RECENT_WINDOW:]
            recent_hippos = {r[4] for r in recent_slice if r[4]}
            features["cdf_hippo_new_flag"] = 1 if hippodrome not in recent_hippos else 0
        else:
            features["cdf_hippo_new_flag"] = None

        # --- cdf_discipline_proportion_shift ---
        if discipline and n >= _RECENT_WINDOW:
            races_list = list(self.global_races)
            # Last 500 races
            recent = races_list[-_RECENT_WINDOW:]
            recent_count = sum(1 for r in recent if r[3] == discipline)
            recent_prop = recent_count / len(recent)

            # 500-1000 races ago
            older_end = len(races_list) - _RECENT_WINDOW
            older_start = max(0, older_end - _RECENT_WINDOW)
            older = races_list[older_start:older_end]
            if older:
                older_count = sum(1 for r in older if r[3] == discipline)
                older_prop = older_count / len(older)
                features["cdf_discipline_proportion_shift"] = round(
                    recent_prop - older_prop, 6
                )
            else:
                features["cdf_discipline_proportion_shift"] = None
        else:
            features["cdf_discipline_proportion_shift"] = None

        # --- cdf_market_efficiency_trend ---
        # rolling |1/cote_winner - 1/nb_partants| over last 500 races
        if n >= 20:
            recent_slice = list(self.global_races)[-_RECENT_WINDOW:]
            efficiency_vals = []
            for r in recent_slice:
                cote_w = r[2]
                field_s = r[1]
                if cote_w and cote_w > 0 and field_s and field_s > 0:
                    efficiency_vals.append(abs(1.0 / cote_w - 1.0 / field_s))
            if efficiency_vals:
                features["cdf_market_efficiency_trend"] = round(
                    sum(efficiency_vals) / len(efficiency_vals), 6
                )
            else:
                features["cdf_market_efficiency_trend"] = None
        else:
            features["cdf_market_efficiency_trend"] = None

        # --- cdf_days_since_data_start ---
        if race_date and self.first_date:
            delta = (race_date - self.first_date).days
            features["cdf_days_since_data_start"] = max(delta, 0)
        else:
            features["cdf_days_since_data_start"] = None

        # --- cdf_record_density ---
        # Number of races in last 30 days
        if race_date and self.race_dates:
            cutoff = race_date - timedelta(days=_DENSITY_DAYS)
            count = sum(1 for d in self.race_dates if d >= cutoff)
            features["cdf_record_density"] = count
        else:
            features["cdf_record_density"] = None

        return features

    def update(self, is_favorite_win: bool, field_size: int,
               cote_winner: Optional[float], discipline: str,
               hippodrome: str, race_date: Optional[datetime]) -> None:
        """Update global state AFTER snapshotting features for this race."""
        self.global_races.append((
            is_favorite_win,
            field_size,
            cote_winner,
            discipline,
            hippodrome,
        ))

        if race_date:
            if self.first_date is None:
                self.first_date = race_date
            self.race_dates.append(race_date)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_concept_drift_features(input_path: Path, output_path: Path, logger) -> int:
    """Build concept drift features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         snapshot BEFORE update, and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Concept Drift Features Builder ===")
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
    state = _ConceptDriftState()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts = {
        "cdf_global_favorite_winrate_trend": 0,
        "cdf_global_avg_field_size_trend": 0,
        "cdf_global_avg_cote_winner_trend": 0,
        "cdf_hippo_new_flag": 0,
        "cdf_discipline_proportion_shift": 0,
        "cdf_market_efficiency_trend": 0,
        "cdf_days_since_data_start": 0,
        "cdf_record_density": 0,
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

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            race_date = _parse_date(course_date_str)

            # Extract course-level info for state update
            nb_partants = len(course_records)
            discipline = ""
            hippodrome = ""
            cote_winner: Optional[float] = None
            is_favorite_win = False

            # Find winner, favorite, discipline, hippodrome
            min_cote = float("inf")
            favorite_uid = None
            winner_uid = None

            for rec in course_records:
                if not discipline:
                    d = rec.get("discipline") or rec.get("type_course") or ""
                    discipline = d.strip().upper()
                if not hippodrome:
                    hippodrome = (rec.get("hippodrome_normalise") or "").strip()

                cote = _safe_float(rec.get("cote_finale") or rec.get("cote_reference"))
                if cote is not None and cote > 0 and cote < min_cote:
                    min_cote = cote
                    favorite_uid = rec.get("partant_uid")

                if rec.get("is_gagnant"):
                    winner_uid = rec.get("partant_uid")
                    cote_winner = _safe_float(
                        rec.get("cote_finale") or rec.get("cote_reference")
                    )

            if favorite_uid and winner_uid:
                is_favorite_win = (favorite_uid == winner_uid)

            field_size = _safe_int(
                course_records[0].get("nombre_partants"), default=nb_partants
            )

            # -- Snapshot features for all partants (temporal integrity) --
            feat_snapshot = state.snapshot_features(hippodrome, discipline, race_date)

            for rec in course_records:
                partant_uid = rec.get("partant_uid")
                course_uid_rec = rec.get("course_uid", "")
                date_iso = rec.get("date_reunion_iso", "")

                out_rec = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_rec,
                    "date_reunion_iso": date_iso,
                }
                out_rec.update(feat_snapshot)

                # Track fill rates
                for feat_key in fill_counts:
                    if out_rec.get(feat_key) is not None:
                        fill_counts[feat_key] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update state AFTER snapshotting --
            state.update(
                is_favorite_win=is_favorite_win,
                field_size=field_size,
                cote_winner=cote_winner,
                discipline=discipline,
                hippodrome=hippodrome,
                race_date=race_date,
            )

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)

            if n_processed % _GC_EVERY < len(course_records):
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Concept drift build termine: %d features en %.1fs",
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
        description="Construction des features concept drift a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/concept_drift_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("concept_drift_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "concept_drift_features.jsonl"
    build_concept_drift_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
