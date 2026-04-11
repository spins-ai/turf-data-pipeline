#!/usr/bin/env python3
"""
feature_builders.career_peak_detector_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Detects whether a horse is at or near its career peak performance.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant career-peak features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the state -- no future leakage. Snapshot BEFORE update.

Produces:
  - career_peak_detector.jsonl   in builder_outputs/career_peak_detector/

Features per partant (10):
  - cpd_career_win_rate       : total wins / total races so far
  - cpd_recent_win_rate_10    : win rate in last 10 races
  - cpd_form_vs_career        : recent_win_rate_10 - career_win_rate
  - cpd_is_peak_form          : 1 if recent > 1.5x career AND min 5 races
  - cpd_is_declining          : 1 if recent < 0.5x career AND min 10 races
  - cpd_best_odds_career      : best (lowest) cote_finale ever seen
  - cpd_odds_vs_best          : current cote / best_odds (>1 = worse than peak)
  - cpd_win_rate_ewma         : exponentially weighted moving average of wins (alpha=0.1)
  - cpd_peak_age_match        : 1 if horse is age 4-6 (typical peak age)
  - cpd_career_stage          : 0=early(<10), 1=developing(10-30), 2=prime(30-70), 3=veteran(>70)

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records
  - Write to .tmp then atomic rename

Usage:
    python feature_builders/career_peak_detector_builder.py
    python feature_builders/career_peak_detector_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/career_peak_detector")

# Progress log every N records
_LOG_EVERY = 500_000

# EWMA smoothing factor
_EWMA_ALPHA = 0.1


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseState:
    """Tracks career state for a single horse."""

    __slots__ = (
        "total_wins", "total_races", "recent_results",
        "best_odds", "ewma_win_rate", "odds_history",
    )

    def __init__(self) -> None:
        self.total_wins: int = 0
        self.total_races: int = 0
        self.recent_results: deque = deque(maxlen=10)  # 1=win, 0=loss
        self.best_odds: Optional[float] = None
        self.ewma_win_rate: float = 0.0
        self.odds_history: deque = deque(maxlen=5)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_career_peak_features(input_path: Path, output_path: Path, logger) -> int:
    """Build career peak detector features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Career Peak Detector Builder ===")
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
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "cpd_career_win_rate",
        "cpd_recent_win_rate_10",
        "cpd_form_vs_career",
        "cpd_is_peak_form",
        "cpd_is_declining",
        "cpd_best_odds_career",
        "cpd_odds_vs_best",
        "cpd_win_rate_ewma",
        "cpd_peak_age_match",
        "cpd_career_stage",
    ]
    fill_counts = {k: 0 for k in feature_names}

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

            # Read only this course's records from disk
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                course_records.append(rec)

            # -- Snapshot pre-race stats & emit features (temporal integrity) --
            post_updates: list[tuple[str, bool, Optional[float]]] = []

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                partant_uid = rec.get("partant_uid")
                is_winner = bool(rec.get("is_gagnant"))

                # Parse cote_finale
                cote_raw = rec.get("cote_finale")
                cote: Optional[float] = None
                if cote_raw is not None:
                    try:
                        cote = float(cote_raw)
                        if cote <= 0:
                            cote = None
                    except (ValueError, TypeError):
                        cote = None

                # Parse age
                age_raw = rec.get("age")
                age: Optional[int] = None
                if age_raw is not None:
                    try:
                        age = int(age_raw)
                    except (ValueError, TypeError):
                        age = None

                features: dict[str, Any] = {"partant_uid": partant_uid}

                if horse_id:
                    st = horse_state[horse_id]

                    # --- Feature 1: cpd_career_win_rate ---
                    if st.total_races > 0:
                        career_wr = round(st.total_wins / st.total_races, 4)
                        features["cpd_career_win_rate"] = career_wr
                        fill_counts["cpd_career_win_rate"] += 1
                    else:
                        career_wr = None
                        features["cpd_career_win_rate"] = None

                    # --- Feature 2: cpd_recent_win_rate_10 ---
                    if len(st.recent_results) > 0:
                        recent_wr = round(sum(st.recent_results) / len(st.recent_results), 4)
                        features["cpd_recent_win_rate_10"] = recent_wr
                        fill_counts["cpd_recent_win_rate_10"] += 1
                    else:
                        recent_wr = None
                        features["cpd_recent_win_rate_10"] = None

                    # --- Feature 3: cpd_form_vs_career ---
                    if recent_wr is not None and career_wr is not None:
                        features["cpd_form_vs_career"] = round(recent_wr - career_wr, 4)
                        fill_counts["cpd_form_vs_career"] += 1
                    else:
                        features["cpd_form_vs_career"] = None

                    # --- Feature 4: cpd_is_peak_form ---
                    if (
                        recent_wr is not None
                        and career_wr is not None
                        and career_wr > 0
                        and st.total_races >= 5
                    ):
                        features["cpd_is_peak_form"] = 1 if recent_wr > 1.5 * career_wr else 0
                        fill_counts["cpd_is_peak_form"] += 1
                    else:
                        features["cpd_is_peak_form"] = None

                    # --- Feature 5: cpd_is_declining ---
                    if (
                        recent_wr is not None
                        and career_wr is not None
                        and career_wr > 0
                        and st.total_races >= 10
                    ):
                        features["cpd_is_declining"] = 1 if recent_wr < 0.5 * career_wr else 0
                        fill_counts["cpd_is_declining"] += 1
                    else:
                        features["cpd_is_declining"] = None

                    # --- Feature 6: cpd_best_odds_career ---
                    if st.best_odds is not None:
                        features["cpd_best_odds_career"] = st.best_odds
                        fill_counts["cpd_best_odds_career"] += 1
                    else:
                        features["cpd_best_odds_career"] = None

                    # --- Feature 7: cpd_odds_vs_best ---
                    if cote is not None and st.best_odds is not None and st.best_odds > 0:
                        features["cpd_odds_vs_best"] = round(cote / st.best_odds, 4)
                        fill_counts["cpd_odds_vs_best"] += 1
                    else:
                        features["cpd_odds_vs_best"] = None

                    # --- Feature 8: cpd_win_rate_ewma ---
                    if st.total_races > 0:
                        features["cpd_win_rate_ewma"] = round(st.ewma_win_rate, 4)
                        fill_counts["cpd_win_rate_ewma"] += 1
                    else:
                        features["cpd_win_rate_ewma"] = None

                    # --- Feature 9: cpd_peak_age_match ---
                    if age is not None:
                        features["cpd_peak_age_match"] = 1 if 4 <= age <= 6 else 0
                        fill_counts["cpd_peak_age_match"] += 1
                    else:
                        features["cpd_peak_age_match"] = None

                    # --- Feature 10: cpd_career_stage ---
                    if st.total_races > 0:
                        n = st.total_races
                        if n < 10:
                            features["cpd_career_stage"] = 0
                        elif n < 30:
                            features["cpd_career_stage"] = 1
                        elif n <= 70:
                            features["cpd_career_stage"] = 2
                        else:
                            features["cpd_career_stage"] = 3
                        fill_counts["cpd_career_stage"] += 1
                    else:
                        features["cpd_career_stage"] = None

                else:
                    # No horse_id: all features null
                    for fname in feature_names:
                        features[fname] = None

                # Write to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Queue post-race update
                post_updates.append((horse_id, is_winner, cote))

            # -- Update states AFTER race (no leakage) --
            for horse_id, is_winner, cote in post_updates:
                if not horse_id:
                    continue
                st = horse_state[horse_id]

                # Update totals
                st.total_races += 1
                if is_winner:
                    st.total_wins += 1

                # Update recent results deque
                st.recent_results.append(1 if is_winner else 0)

                # Update best odds
                if cote is not None:
                    if st.best_odds is None or cote < st.best_odds:
                        st.best_odds = cote

                # Update odds history
                if cote is not None:
                    st.odds_history.append(cote)

                # Update EWMA: ewma = alpha * new_value + (1-alpha) * old_ewma
                win_val = 1.0 if is_winner else 0.0
                st.ewma_win_rate = _EWMA_ALPHA * win_val + (1.0 - _EWMA_ALPHA) * st.ewma_win_rate

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Career peak build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
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
        description="Construction des features career peak detector a partir de partants_master"
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

    logger = setup_logging("career_peak_detector_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "career_peak_detector.jsonl"
    build_career_peak_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
