#!/usr/bin/env python3
"""
feature_builders.trainer_jockey_affinity_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer-jockey affinity features -- measuring how well specific
trainer-jockey pairs work together.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant trainer-jockey affinity features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - trainer_jockey_affinity.jsonl  in builder_outputs/trainer_jockey_affinity/

Features per partant (10):
  - tja_pair_wr              : trainer+jockey pair win rate
  - tja_pair_place_rate      : trainer+jockey pair place rate
  - tja_pair_runs            : number of runs for this pair
  - tja_pair_wr_vs_trainer_avg : pair_wr - trainer's overall wr (positive = jockey adds value)
  - tja_pair_wr_vs_jockey_avg : pair_wr - jockey's overall wr (positive = trainer adds value)
  - tja_synergy_score        : (pair_wr - trainer_wr) + (pair_wr - jockey_wr) -- total synergy
  - tja_pair_recent_form     : win rate of pair in last 10 rides together
  - tja_is_preferred_pair    : 1 if pair has 20+ rides together (established partnership)
  - tja_pair_roi_score       : pair_wr * avg_odds_when_paired (proxy for profitability)
  - tja_pair_first_time      : 1 if trainer and jockey have never worked together

Usage:
    python feature_builders/trainer_jockey_affinity_builder.py
    python feature_builders/trainer_jockey_affinity_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_jockey_affinity")

_LOG_EVERY = 500_000

_FEATURE_KEYS = [
    "tja_pair_wr",
    "tja_pair_place_rate",
    "tja_pair_runs",
    "tja_pair_wr_vs_trainer_avg",
    "tja_pair_wr_vs_jockey_avg",
    "tja_synergy_score",
    "tja_pair_recent_form",
    "tja_is_preferred_pair",
    "tja_pair_roi_score",
    "tja_pair_first_time",
]

# Minimum rides for reliable rate calculations
_MIN_RIDES_FOR_RATE = 3
# Threshold for "preferred pair" (established partnership)
_PREFERRED_PAIR_THRESHOLD = 20


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _norm_name(name: Optional[str]) -> Optional[str]:
    """Normalise a trainer/jockey name for consistent keying."""
    if not name or not isinstance(name, str):
        return None
    n = name.strip().upper()
    return n if n else None


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class _OverallStats:
    """Tracks overall wins/total for a trainer or jockey."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def win_rate(self) -> Optional[float]:
        if self.total < _MIN_RIDES_FOR_RATE:
            return None
        return self.wins / self.total


class _PairState:
    """Tracks per (trainer, jockey) pair statistics."""

    __slots__ = ("wins", "places", "total", "recent_results", "odds_sum")

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.total: int = 0
        self.recent_results: deque = deque(maxlen=10)
        self.odds_sum: float = 0.0

    def pair_wr(self) -> Optional[float]:
        if self.total < _MIN_RIDES_FOR_RATE:
            return None
        return self.wins / self.total

    def pair_place_rate(self) -> Optional[float]:
        if self.total < _MIN_RIDES_FOR_RATE:
            return None
        return self.places / self.total

    def recent_form(self) -> Optional[float]:
        if not self.recent_results:
            return None
        return sum(self.recent_results) / len(self.recent_results)

    def avg_odds(self) -> Optional[float]:
        if self.total == 0:
            return None
        return self.odds_sum / self.total

    def snapshot(
        self,
        trainer_stats: _OverallStats,
        jockey_stats: _OverallStats,
    ) -> dict[str, Any]:
        """Compute affinity features BEFORE updating with current race."""
        feats: dict[str, Any] = {k: None for k in _FEATURE_KEYS}

        # tja_pair_first_time: 1 if pair has never raced together
        feats["tja_pair_first_time"] = 1 if self.total == 0 else 0

        if self.total == 0:
            return feats

        # tja_pair_runs
        feats["tja_pair_runs"] = self.total

        # tja_is_preferred_pair
        feats["tja_is_preferred_pair"] = 1 if self.total >= _PREFERRED_PAIR_THRESHOLD else 0

        # Rates (only if enough rides)
        p_wr = self.pair_wr()
        p_pr = self.pair_place_rate()

        if p_wr is not None:
            feats["tja_pair_wr"] = round(p_wr, 4)
        if p_pr is not None:
            feats["tja_pair_place_rate"] = round(p_pr, 4)

        # Deltas vs trainer / jockey averages
        trainer_wr = trainer_stats.win_rate()
        jockey_wr = jockey_stats.win_rate()

        if p_wr is not None and trainer_wr is not None:
            feats["tja_pair_wr_vs_trainer_avg"] = round(p_wr - trainer_wr, 4)

        if p_wr is not None and jockey_wr is not None:
            feats["tja_pair_wr_vs_jockey_avg"] = round(p_wr - jockey_wr, 4)

        # Synergy score
        if p_wr is not None and trainer_wr is not None and jockey_wr is not None:
            synergy = (p_wr - trainer_wr) + (p_wr - jockey_wr)
            feats["tja_synergy_score"] = round(synergy, 4)

        # Recent form (last 10 rides together)
        rf = self.recent_form()
        if rf is not None:
            feats["tja_pair_recent_form"] = round(rf, 4)

        # ROI score: pair_wr * avg_odds
        avg_o = self.avg_odds()
        if p_wr is not None and avg_o is not None:
            feats["tja_pair_roi_score"] = round(p_wr * avg_o, 4)

        return feats

    def update(self, is_win: bool, is_place: bool, odds: Optional[float]) -> None:
        """Update pair state AFTER snapshotting."""
        self.total += 1
        if is_win:
            self.wins += 1
        if is_place:
            self.places += 1
        self.recent_results.append(1 if is_win else 0)
        if odds is not None:
            self.odds_sum += odds


# ===========================================================================
# MAIN BUILD (index + sort + seek-based streaming output)
# ===========================================================================


def build_trainer_jockey_affinity_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build trainer-jockey affinity features from partants_master.jsonl.

    Two-phase approach:
      1. Build lightweight index (sort_key, byte_offset).
      2. Sort index chronologically, then seek-read records from disk,
         process course by course, stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Trainer-Jockey Affinity Builder ===")
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

    # -- Phase 2: Sort the lightweight index --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    trainer_stats: dict[str, _OverallStats] = defaultdict(_OverallStats)
    jockey_stats: dict[str, _OverallStats] = defaultdict(_OverallStats)
    pair_states: dict[tuple[str, str], _PairState] = defaultdict(_PairState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    fill_counts = {k: 0 for k in _FEATURE_KEYS}
    n_processed = 0
    n_written = 0
    total = len(index)

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(byte_offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(byte_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("rapport_final"))
            position = rec.get("position_arrivee")
            try:
                position = int(position) if position is not None else None
            except (ValueError, TypeError):
                position = None

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "entraineur": _norm_name(rec.get("entraineur")),
                "jockey": _norm_name(rec.get("jockey_driver")),
                "is_gagnant": bool(rec.get("is_gagnant")),
                "position": position,
                "cote": cote,
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

            # -- Snapshot pre-race features (temporal integrity) --
            for rec in course_group:
                entraineur = rec["entraineur"]
                jockey = rec["jockey"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if entraineur and jockey:
                    pair_key = (entraineur, jockey)
                    pair_st = pair_states[pair_key]
                    snap = pair_st.snapshot(
                        trainer_stats[entraineur],
                        jockey_stats[jockey],
                    )
                    for k in _FEATURE_KEYS:
                        v = snap[k]
                        features[k] = v
                        if v is not None:
                            fill_counts[k] += 1
                else:
                    for k in _FEATURE_KEYS:
                        features[k] = None

                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

            # -- Update states AFTER snapshotting (post-race) --
            for rec in course_group:
                entraineur = rec["entraineur"]
                jockey = rec["jockey"]
                is_win = rec["is_gagnant"]
                position = rec["position"]
                cote = rec["cote"]

                is_place = position is not None and 1 <= position <= 3

                if entraineur:
                    trainer_stats[entraineur].total += 1
                    if is_win:
                        trainer_stats[entraineur].wins += 1

                if jockey:
                    jockey_stats[jockey].total += 1
                    if is_win:
                        jockey_stats[jockey].wins += 1

                if entraineur and jockey:
                    pair_states[(entraineur, jockey)].update(is_win, is_place, cote)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Trainer-jockey affinity build termine: %d features en %.1fs "
        "(entraineurs: %d, jockeys: %d, paires: %d)",
        n_written, elapsed,
        len(trainer_stats), len(jockey_stats), len(pair_states),
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
        description="Construction des features trainer-jockey affinity a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/trainer_jockey_affinity/)",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_jockey_affinity_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "trainer_jockey_affinity.jsonl"
    build_trainer_jockey_affinity_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
