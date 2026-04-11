#!/usr/bin/env python3
"""
feature_builders.position_distribution_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features modeling the full distribution of a horse's finishing positions,
not just averages.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant position distribution features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the stats -- no future leakage.

Produces:
  - position_distribution_features.jsonl

Features per partant (10):
  - pds_median_position        : median of last 10 positions
  - pds_mode_position          : most frequent position in last 10
  - pds_position_skew          : skewness of position distribution (last 20)
  - pds_position_kurtosis      : kurtosis of position distribution (last 20)
  - pds_win_probability_empirical : wins / total career races
  - pds_top3_probability       : top-3 finishes / total career races
  - pds_dnf_rate               : proportion of non-finishes in career
  - pds_position_entropy       : Shannon entropy of position distribution (last 20)
  - pds_position_iqr           : interquartile range Q3-Q1 (last 20)
  - pds_best_quartile_freq     : proportion finishing in top 25% of field (career)

Memory-optimised approach:
  - Phase 1: index with byte offsets (not full dicts)
  - Phase 2: chronological sort
  - Phase 3: seek-based streaming output
  - gc.collect() every 500K records

Usage:
    python feature_builders/position_distribution_builder.py
    python feature_builders/position_distribution_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/position_distribution")

_LOG_EVERY = 500_000


# ===========================================================================
# MATH HELPERS
# ===========================================================================


def _median(vals: list[int]) -> Optional[float]:
    """Median of a sorted list of ints."""
    n = len(vals)
    if n == 0:
        return None
    s = sorted(vals)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0


def _mode(vals: list[int]) -> Optional[int]:
    """Most frequent value. Ties broken by lowest value."""
    if not vals:
        return None
    c = Counter(vals)
    max_count = max(c.values())
    # Among all values with max_count, pick the lowest
    return min(v for v, cnt in c.items() if cnt == max_count)


def _skewness(vals: list[int]) -> Optional[float]:
    """Sample skewness (Fisher). Needs >= 3 values."""
    n = len(vals)
    if n < 3:
        return None
    mean = sum(vals) / n
    m2 = sum((x - mean) ** 2 for x in vals) / n
    m3 = sum((x - mean) ** 3 for x in vals) / n
    if m2 == 0:
        return 0.0
    return round(m3 / (m2 ** 1.5), 4)


def _kurtosis(vals: list[int]) -> Optional[float]:
    """Excess kurtosis. Needs >= 4 values."""
    n = len(vals)
    if n < 4:
        return None
    mean = sum(vals) / n
    m2 = sum((x - mean) ** 2 for x in vals) / n
    m4 = sum((x - mean) ** 4 for x in vals) / n
    if m2 == 0:
        return 0.0
    return round(m4 / (m2 ** 2) - 3.0, 4)


def _entropy(vals: list[int]) -> Optional[float]:
    """Shannon entropy of the empirical distribution."""
    if not vals:
        return None
    n = len(vals)
    c = Counter(vals)
    ent = 0.0
    for count in c.values():
        p = count / n
        if p > 0:
            ent -= p * math.log2(p)
    return round(ent, 4)


def _iqr(vals: list[int]) -> Optional[float]:
    """Interquartile range Q3 - Q1."""
    n = len(vals)
    if n < 4:
        return None
    s = sorted(vals)
    q1_idx = n // 4
    q3_idx = (3 * n) // 4
    return float(s[q3_idx] - s[q1_idx])


def _parse_date(date_str: str) -> Optional[str]:
    """Validate and return ISO date prefix. Returns None on failure."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        # Quick validation
        int(date_str[:4])
        return date_str[:10]
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorsePositionState:
    """Tracks position history and career stats for one horse."""

    __slots__ = ("positions", "career_wins", "career_places", "career_dnf",
                 "career_total", "career_best_quartile")

    def __init__(self) -> None:
        self.positions: deque = deque(maxlen=20)
        self.career_wins: int = 0
        self.career_places: int = 0     # top 3
        self.career_dnf: int = 0        # non-finishes
        self.career_total: int = 0
        self.career_best_quartile: int = 0  # finished in top 25% of field

    def snapshot_features(self) -> dict[str, Any]:
        """Compute all 10 features from current state (BEFORE update)."""
        pos_list = list(self.positions)
        last10 = pos_list[-10:] if len(pos_list) >= 10 else pos_list

        # Features from last 10
        pds_median = round(_median(last10), 2) if _median(last10) is not None else None
        pds_mode = _mode(last10)

        # Features from full deque (up to 20)
        pds_skew = _skewness(pos_list)
        pds_kurt = _kurtosis(pos_list)
        pds_ent = _entropy(pos_list)
        pds_iqr_val = _iqr(pos_list)

        # Career features
        ct = self.career_total
        if ct > 0:
            pds_win_prob = round(self.career_wins / ct, 4)
            pds_top3_prob = round(self.career_places / ct, 4)
            pds_dnf = round(self.career_dnf / ct, 4)
            pds_best_q = round(self.career_best_quartile / ct, 4)
        else:
            pds_win_prob = None
            pds_top3_prob = None
            pds_dnf = None
            pds_best_q = None

        return {
            "pds_median_position": pds_median,
            "pds_mode_position": pds_mode,
            "pds_position_skew": pds_skew,
            "pds_position_kurtosis": pds_kurt,
            "pds_win_probability_empirical": pds_win_prob,
            "pds_top3_probability": pds_top3_prob,
            "pds_dnf_rate": pds_dnf,
            "pds_position_entropy": pds_ent,
            "pds_position_iqr": pds_iqr_val,
            "pds_best_quartile_freq": pds_best_q,
        }

    def update(self, position: int, nb_partants: int, is_winner: bool) -> None:
        """Update state AFTER snapshotting features."""
        is_dnf = (position <= 0 or position > nb_partants * 2) if nb_partants > 0 else (position <= 0)

        self.career_total += 1

        if is_dnf:
            self.career_dnf += 1
            # Still record position=0 for distribution tracking
            self.positions.append(position if position > 0 else 0)
        else:
            self.positions.append(position)
            if is_winner or position == 1:
                self.career_wins += 1
            if position <= 3:
                self.career_places += 1
            # Top 25% of field
            if nb_partants > 0 and position <= max(1, nb_partants // 4):
                self.career_best_quartile += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_position_distribution_features(input_path: Path, output_path: Path, logger) -> int:
    """Build position distribution features from partants_master.jsonl.

    Memory-optimised: index + sort + seek.
    Returns the total number of feature records written.
    """
    logger.info("=== Position Distribution Builder (memory-optimised) ===")
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
            line_s = line.strip()
            if not line_s:
                continue
            try:
                rec = json.loads(line_s)
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
    horse_states: dict[str, _HorsePositionState] = defaultdict(_HorsePositionState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "pds_median_position", "pds_mode_position", "pds_position_skew",
        "pds_position_kurtosis", "pds_win_probability_empirical",
        "pds_top3_probability", "pds_dnf_rate", "pds_position_entropy",
        "pds_position_iqr", "pds_best_quartile_freq",
    ]
    fill_counts = {name: 0 for name in feature_names}

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
            course_records = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                course_records.append(rec)

            # Determine field size for this race
            nb_partants_race = 0
            for rec in course_records:
                nb = rec.get("nombre_partants") or 0
                try:
                    nb = int(nb)
                except (ValueError, TypeError):
                    nb = 0
                if nb > nb_partants_race:
                    nb_partants_race = nb
            # Fallback: count records if nombre_partants missing
            if nb_partants_race == 0:
                nb_partants_race = len(course_records)

            # -- Snapshot BEFORE update (temporal integrity) --
            snapshots = []
            for rec in course_records:
                cheval = rec.get("nom_cheval") or ""
                partant_uid = rec.get("partant_uid") or ""
                date_iso = rec.get("date_reunion_iso", "") or ""
                course_id = rec.get("course_uid", "") or ""

                out_rec = {
                    "partant_uid": partant_uid,
                    "course_uid": course_id,
                    "date_reunion_iso": date_iso[:10] if len(date_iso) >= 10 else date_iso,
                }

                if cheval and horse_states[cheval].career_total > 0:
                    feats = horse_states[cheval].snapshot_features()
                    out_rec.update(feats)
                    for fname in feature_names:
                        if feats.get(fname) is not None:
                            fill_counts[fname] += 1
                else:
                    for fname in feature_names:
                        out_rec[fname] = None

                fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare update info
                position = rec.get("place_arrivee") or rec.get("cl") or 0
                try:
                    position = int(position)
                except (ValueError, TypeError):
                    position = 0
                is_winner = bool(rec.get("is_gagnant"))

                snapshots.append((cheval, position, nb_partants_race, is_winner))

            # -- Update states AFTER snapshotting --
            for cheval, position, nb_p, is_win in snapshots:
                if cheval:
                    horse_states[cheval].update(position, nb_p, is_win)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Position distribution build termine: %d features en %.1fs (chevaux uniques: %d)",
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
    if INPUT_PATH.exists():
        return INPUT_PATH
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PATH}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de distribution de positions"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("position_distribution_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "position_distribution_features.jsonl"
    build_position_distribution_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
