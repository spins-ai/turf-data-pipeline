#!/usr/bin/env python3
"""
feature_builders.multi_race_sequence_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Multi-race sequence features that encode patterns across consecutive races
for sequence models (LSTM, GRU, Temporal Fusion Transformer).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant sequence features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the sequence state -- no future leakage.  State is
snapshotted BEFORE the current race updates it.

Produces:
  - multi_race_sequence.jsonl   in builder_outputs/multi_race_sequence/

Features per partant (10):
  - mrs_position_sequence_5      : comma-separated last 5 positions (string for embedding)
  - mrs_win_sequence_5           : binary string of last 5 wins (e.g. "01001")
  - mrs_distance_change_pattern  : sequence of distance changes (+/-/=) last 3
  - mrs_discipline_consistency   : fraction of last 5 races in same discipline as current
  - mrs_avg_position_last_3      : mean of last 3 positions
  - mrs_avg_position_last_5      : mean of last 5 positions
  - mrs_position_variance_5      : variance of last 5 positions (consistency)
  - mrs_form_momentum            : weighted mean of last 5 positions (recent=5x, oldest=1x)
  - mrs_class_trajectory         : trend in nombre_partants over last 3 races
  - mrs_earnings_velocity        : change in gains_carriere over last 3 races / 3

Memory-optimised version:
  - Phase 1 reads only minimal tuples (not full dicts) for sorting
  - Phase 2 streams output to disk via seek-based processing
  - gc.collect() called every 500K records

Usage:
    python feature_builders/multi_race_sequence_builder.py
    python feature_builders/multi_race_sequence_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/multi_race_sequence")

# Progress log every N records
_LOG_EVERY = 500_000

# Sequence lengths
_SEQ_LEN_5 = 5
_SEQ_LEN_3 = 3

# Momentum weights: most recent race weighted 5x, oldest 1x
_MOMENTUM_WEIGHTS = [1, 2, 3, 4, 5]


# ===========================================================================
# HORSE STATE (deque-based, bounded)
# ===========================================================================


class _HorseState:
    """Rolling state per horse using bounded deques."""

    __slots__ = ("positions", "wins", "distances", "disciplines",
                 "field_sizes", "gains_snapshots")

    def __init__(self) -> None:
        self.positions: deque[int] = deque(maxlen=_SEQ_LEN_5)
        self.wins: deque[int] = deque(maxlen=_SEQ_LEN_5)           # 1=win, 0=not
        self.distances: deque[float] = deque(maxlen=_SEQ_LEN_5)
        self.disciplines: deque[str] = deque(maxlen=_SEQ_LEN_5)
        self.field_sizes: deque[int] = deque(maxlen=_SEQ_LEN_5)
        self.gains_snapshots: deque[float] = deque(maxlen=_SEQ_LEN_5)


# ===========================================================================
# FEATURE COMPUTATION (from snapshot)
# ===========================================================================


def _compute_features(
    state: _HorseState,
    current_discipline: str,
    partant_uid: str,
) -> dict[str, Any]:
    """Compute all 10 features from the CURRENT (pre-update) state."""

    features: dict[str, Any] = {"partant_uid": partant_uid}
    pos = state.positions
    wins = state.wins
    dists = state.distances
    discs = state.disciplines
    fsizes = state.field_sizes
    gains = state.gains_snapshots

    n_pos = len(pos)
    n_wins = len(wins)
    n_dists = len(dists)
    n_discs = len(discs)
    n_fs = len(fsizes)
    n_gains = len(gains)

    # 1. mrs_position_sequence_5 -- comma-separated last 5 positions
    if n_pos > 0:
        features["mrs_position_sequence_5"] = ",".join(str(p) for p in pos)
    else:
        features["mrs_position_sequence_5"] = None

    # 2. mrs_win_sequence_5 -- binary string last 5
    if n_wins > 0:
        features["mrs_win_sequence_5"] = "".join(str(w) for w in wins)
    else:
        features["mrs_win_sequence_5"] = None

    # 3. mrs_distance_change_pattern -- +/-/= for last 3 distance transitions
    if n_dists >= 2:
        changes = []
        dlist = list(dists)
        start = max(0, len(dlist) - 4)  # need up to 4 distances for 3 transitions
        for j in range(start, len(dlist) - 1):
            diff = dlist[j + 1] - dlist[j]
            if diff > 0:
                changes.append("+")
            elif diff < 0:
                changes.append("-")
            else:
                changes.append("=")
        # Keep only last 3 changes
        features["mrs_distance_change_pattern"] = "".join(changes[-_SEQ_LEN_3:])
    else:
        features["mrs_distance_change_pattern"] = None

    # 4. mrs_discipline_consistency -- fraction of last 5 in same discipline
    if n_discs > 0 and current_discipline:
        same = sum(1 for d in discs if d == current_discipline)
        features["mrs_discipline_consistency"] = round(same / n_discs, 4)
    else:
        features["mrs_discipline_consistency"] = None

    # 5. mrs_avg_position_last_3
    if n_pos >= 3:
        last3 = list(pos)[-3:]
        features["mrs_avg_position_last_3"] = round(sum(last3) / 3, 4)
    else:
        features["mrs_avg_position_last_3"] = None

    # 6. mrs_avg_position_last_5
    if n_pos >= 5:
        features["mrs_avg_position_last_5"] = round(sum(pos) / 5, 4)
    else:
        features["mrs_avg_position_last_5"] = None

    # 7. mrs_position_variance_5
    if n_pos >= 5:
        mean_p = sum(pos) / 5
        var_p = sum((p - mean_p) ** 2 for p in pos) / 5
        features["mrs_position_variance_5"] = round(var_p, 4)
    else:
        features["mrs_position_variance_5"] = None

    # 8. mrs_form_momentum -- weighted mean (most recent = 5x, oldest = 1x)
    if n_pos >= 5:
        plist = list(pos)
        weighted = sum(w * p for w, p in zip(_MOMENTUM_WEIGHTS, plist))
        total_w = sum(_MOMENTUM_WEIGHTS)
        features["mrs_form_momentum"] = round(weighted / total_w, 4)
    else:
        features["mrs_form_momentum"] = None

    # 9. mrs_class_trajectory -- trend in nombre_partants over last 3
    if n_fs >= 3:
        last3_fs = list(fsizes)[-3:]
        # Simple slope: (last - first) / 2
        features["mrs_class_trajectory"] = round((last3_fs[2] - last3_fs[0]) / 2, 4)
    else:
        features["mrs_class_trajectory"] = None

    # 10. mrs_earnings_velocity -- (current_gains - gains_3_ago) / 3
    if n_gains >= 3:
        glist = list(gains)
        features["mrs_earnings_velocity"] = round((glist[-1] - glist[-3]) / 3, 4)
    else:
        features["mrs_earnings_velocity"] = None

    return features


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_multi_race_sequence_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build multi-race sequence features from partants_master.jsonl.

    Two-phase approach:
      1. Read sort keys + byte offsets (lightweight index).
      2. Sort chronologically, then seek-based processing with streaming output.

    Returns the total number of feature records written.
    """
    logger.info("=== Multi-Race Sequence Builder ===")
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

    # -- Phase 3: Seek-based processing, streaming output --
    t2 = time.time()
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "mrs_position_sequence_5",
        "mrs_win_sequence_5",
        "mrs_distance_change_pattern",
        "mrs_discipline_consistency",
        "mrs_avg_position_last_3",
        "mrs_avg_position_last_5",
        "mrs_position_variance_5",
        "mrs_form_momentum",
        "mrs_class_trajectory",
        "mrs_earnings_velocity",
    ]
    fill_counts = {k: 0 for k in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect all entries for this course
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

            # Read records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Snapshot BEFORE update: compute features for each partant --
            for rec in course_records:
                partant_uid = rec.get("partant_uid")
                horse_id = rec.get("horse_id") or rec.get("nom_cheval") or ""
                if not horse_id or not partant_uid:
                    continue

                discipline = (rec.get("discipline") or "").strip().upper()
                state = horse_states[horse_id]

                features = _compute_features(state, discipline, partant_uid)

                # Count fills
                for k in feature_names:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER snapshot --
            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval") or ""
                if not horse_id:
                    continue

                state = horse_states[horse_id]

                # Position
                pos_raw = rec.get("position_arrivee")
                try:
                    pos = int(pos_raw)
                except (ValueError, TypeError):
                    pos = None
                if pos is not None and pos > 0:
                    state.positions.append(pos)

                # Win
                is_win = bool(rec.get("is_gagnant"))
                state.wins.append(1 if is_win else 0)

                # Distance
                dist_raw = rec.get("distance")
                try:
                    dist = float(dist_raw)
                except (ValueError, TypeError):
                    dist = None
                if dist is not None and dist > 0:
                    state.distances.append(dist)

                # Discipline
                discipline = (rec.get("discipline") or "").strip().upper()
                if discipline:
                    state.disciplines.append(discipline)

                # Field size
                nb_raw = rec.get("nombre_partants")
                try:
                    nb = int(nb_raw)
                except (ValueError, TypeError):
                    nb = None
                if nb is not None and nb > 0:
                    state.field_sizes.append(nb)

                # Gains snapshot
                gains_raw = rec.get("gains_carriere_euros")
                try:
                    gains_val = float(gains_raw)
                except (ValueError, TypeError):
                    gains_val = None
                if gains_val is not None:
                    state.gains_snapshots.append(gains_val)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Multi-race sequence build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Fill rates
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
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features multi-race sequence a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/multi_race_sequence/)",
    )
    args = parser.parse_args()

    logger = setup_logging("multi_race_sequence_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "multi_race_sequence.jsonl"
    build_multi_race_sequence_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
