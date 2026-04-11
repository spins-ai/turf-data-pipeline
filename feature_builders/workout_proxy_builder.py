#!/usr/bin/env python3
"""
feature_builders.workout_proxy_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Proxy features for horse fitness and workout readiness.

We don't have actual workout data but can approximate fitness from
race spacing and frequency patterns.

Temporal integrity: for any partant at date D, only races with date < D
contribute to features -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - workout_proxy_features.jsonl  in builder_outputs/workout_proxy/

Features per partant (8):
  - wkp_rest_days              : days since last race
  - wkp_rest_optimal           : 1 if rest days is between 14-45 (optimal recovery zone)
  - wkp_rest_too_short         : 1 if rest < 7 days (potential fatigue)
  - wkp_rest_too_long          : 1 if rest > 90 days (fitness concern)
  - wkp_race_frequency_30d     : number of races in last 30 days
  - wkp_race_frequency_90d     : number of races in last 90 days
  - wkp_fitness_score          : combination of optimal rest + recent race frequency
  - wkp_freshness_vs_fitness   : rest_days / (race_frequency_90d + 1)

Per-horse state: deque of last 20 race dates.

Usage:
    python feature_builders/workout_proxy_builder.py
    python feature_builders/workout_proxy_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/workout_proxy")

_LOG_EVERY = 500_000
_DEQUE_MAX = 20


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseState:
    """Track last 20 race dates for one horse."""

    __slots__ = ("race_dates",)

    def __init__(self) -> None:
        self.race_dates: deque = deque(maxlen=_DEQUE_MAX)

    def snapshot(self, current_date: date) -> dict[str, Any]:
        """Compute all 8 workout proxy features from pre-race state."""
        feats: dict[str, Any] = {
            "wkp_rest_days": None,
            "wkp_rest_optimal": None,
            "wkp_rest_too_short": None,
            "wkp_rest_too_long": None,
            "wkp_race_frequency_30d": None,
            "wkp_race_frequency_90d": None,
            "wkp_fitness_score": None,
            "wkp_freshness_vs_fitness": None,
        }

        if not self.race_dates:
            return feats

        # Rest days since last race
        last_date = self.race_dates[-1]
        rest_days = (current_date - last_date).days
        if rest_days < 0:
            # Same-day or data anomaly -- treat as no prior info
            return feats

        feats["wkp_rest_days"] = rest_days
        feats["wkp_rest_optimal"] = int(14 <= rest_days <= 45)
        feats["wkp_rest_too_short"] = int(rest_days < 7)
        feats["wkp_rest_too_long"] = int(rest_days > 90)

        # Race frequencies
        cutoff_30 = current_date - timedelta(days=30)
        cutoff_90 = current_date - timedelta(days=90)
        freq_30 = 0
        freq_90 = 0
        for d in self.race_dates:
            if d >= cutoff_30:
                freq_30 += 1
                freq_90 += 1
            elif d >= cutoff_90:
                freq_90 += 1

        feats["wkp_race_frequency_30d"] = freq_30
        feats["wkp_race_frequency_90d"] = freq_90

        # Fitness score: optimal rest bonus + frequency signal
        # Range roughly 0-3: optimal_rest(0-1) + freq_30 normalized + freq_90 normalized
        optimal_bonus = 1.0 if 14 <= rest_days <= 45 else 0.0
        freq_30_score = min(freq_30 / 3.0, 1.0)  # cap at 3 races/month
        freq_90_score = min(freq_90 / 8.0, 1.0)   # cap at 8 races/quarter
        fitness = round(optimal_bonus + freq_30_score + freq_90_score, 4)
        feats["wkp_fitness_score"] = fitness

        # Freshness vs fitness balance
        feats["wkp_freshness_vs_fitness"] = round(rest_days / (freq_90 + 1), 4)

        return feats

    def update(self, race_date: date) -> None:
        """Record a new race date (post-race)."""
        self.race_dates.append(race_date)


# ===========================================================================
# DATE PARSING
# ===========================================================================


def _parse_date(val: Any) -> Optional[date]:
    """Parse an ISO date string to a date object."""
    if not val or not isinstance(val, str):
        return None
    try:
        parts = val[:10].split("-")
        if len(parts) == 3:
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError):
        pass
    return None


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_workout_proxy_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build workout proxy features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Workout Proxy Builder (memory-optimised) ===")
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

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "wkp_rest_days",
        "wkp_rest_optimal",
        "wkp_rest_too_short",
        "wkp_rest_too_long",
        "wkp_race_frequency_30d",
        "wkp_race_frequency_90d",
        "wkp_fitness_score",
        "wkp_freshness_vs_fitness",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
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

            # Parse the race date once for this course
            race_date = _parse_date(course_date_str)

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple[str, date]] = []

            for rec in course_records:
                cheval = rec.get("nom_cheval")
                partant_uid = rec.get("partant_uid")
                course_uid_val = rec.get("course_uid")
                date_val = rec.get("date_reunion_iso")

                out_rec: dict[str, Any] = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_val,
                    "date_reunion_iso": date_val,
                }

                if not cheval or race_date is None:
                    # No horse id or unparseable date -- emit Nones
                    for k in feature_keys:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[cheval]

                # Compute features from pre-race state (BEFORE update)
                feats = hs.snapshot(race_date)

                for k in feature_keys:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # Defer state update
                post_updates.append((cheval, race_date))

            # -- Update horse states after race (no leakage) --
            for cheval, rd in post_updates:
                horse_state[cheval].update(rd)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Workout proxy build termine: %d features en %.1fs (chevaux suivis: %d)",
        n_written, elapsed, len(horse_state),
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
        description="Construction des features workout proxy a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/workout_proxy/)",
    )
    args = parser.parse_args()

    logger = setup_logging("workout_proxy_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "workout_proxy_features.jsonl"
    build_workout_proxy_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
