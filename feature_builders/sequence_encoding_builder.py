#!/usr/bin/env python3
"""
feature_builders.sequence_encoding_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sequence-encoded features designed for LSTM, GRU, and Temporal Fusion
Transformer models.  These models need the actual ordered sequence of
past performances -- not just rolling averages.

Temporal integrity: for any partant at date D, features are computed from
the horse's state BEFORE the current race is added -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - sequence_encoding_features.jsonl  in builder_outputs/sequence_encoding/

Features per partant (14):
  - seq_enc_pos_1 .. seq_enc_pos_5   : last 5 finishing positions (most
        recent first), normalised 0-1 by dividing by nombre_partants.
  - seq_enc_speed_1 .. seq_enc_speed_3 : last 3 speed figures (most recent
        first).
  - seq_enc_rest_1, seq_enc_rest_2   : days rest before last 2 races
        (log-scaled).
  - seq_enc_cote_1, seq_enc_cote_2   : last 2 odds (log-scaled).
  - seq_enc_win_streak_binary        : last 5 races as binary win vector
        encoded as integer (e.g. 10100 = 20, 11000 = 24).
  - seq_enc_trend_slope              : linear regression slope of last 5
        normalised positions (negative = improving).

Usage:
    python feature_builders/sequence_encoding_builder.py
    python feature_builders/sequence_encoding_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/sequence_encoding"
)

_LOG_EVERY = 500_000
_HISTORY_MAXLEN = 10  # rolling window of past races


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date_days(date_str: str) -> Optional[int]:
    """Convert YYYY-MM-DD to an approximate integer day count."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


def _slope(values: list[float]) -> Optional[float]:
    """Simple linear regression slope.  x = 0..n-1 (chronological order).

    Returns None if fewer than 3 values.
    """
    n = len(values)
    if n < 3:
        return None
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if den == 0:
        return None
    return round(num / den, 6)


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseState:
    """Track rolling sequence state for one horse.

    Each entry in ``history`` is a tuple:
        (position, speed_figure, days_since_prev, cote, nombre_partants, is_winner)

    ``last_date_days`` is kept outside the deque to compute the gap for the
    *next* race.
    """

    __slots__ = ("history", "last_date_days")

    def __init__(self) -> None:
        self.history: deque = deque(maxlen=_HISTORY_MAXLEN)
        self.last_date_days: Optional[int] = None


# ===========================================================================
# FEATURE COMPUTATION (from snapshot before update)
# ===========================================================================

# Feature key order for fill-rate tracking
FEATURE_KEYS = [
    "seq_enc_pos_1", "seq_enc_pos_2", "seq_enc_pos_3",
    "seq_enc_pos_4", "seq_enc_pos_5",
    "seq_enc_speed_1", "seq_enc_speed_2", "seq_enc_speed_3",
    "seq_enc_rest_1", "seq_enc_rest_2",
    "seq_enc_cote_1", "seq_enc_cote_2",
    "seq_enc_win_streak_binary",
    "seq_enc_trend_slope",
]


def _compute_features(hs: _HorseState) -> dict[str, Any]:
    """Compute all 14 sequence-encoded features from the horse's pre-race state."""
    feats: dict[str, Any] = {k: None for k in FEATURE_KEYS}

    hist = list(hs.history)  # oldest first
    n = len(hist)
    if n == 0:
        return feats

    # Reverse so index 0 = most recent race
    recent = list(reversed(hist))

    # -- seq_enc_pos_1 .. seq_enc_pos_5 --
    # Each tuple: (position, speed_figure, days_since_prev, cote, nombre_partants, is_winner)
    for k in range(min(5, n)):
        pos = recent[k][0]
        nb_part = recent[k][4]
        if pos is not None and nb_part is not None and nb_part > 0:
            feats[f"seq_enc_pos_{k + 1}"] = round(pos / nb_part, 4)

    # -- seq_enc_speed_1 .. seq_enc_speed_3 --
    for k in range(min(3, n)):
        spd = recent[k][1]
        if spd is not None:
            feats[f"seq_enc_speed_{k + 1}"] = round(spd, 4)

    # -- seq_enc_rest_1, seq_enc_rest_2 --
    for k in range(min(2, n)):
        rest = recent[k][2]
        if rest is not None and rest >= 0:
            feats[f"seq_enc_rest_{k + 1}"] = round(math.log1p(rest), 4)

    # -- seq_enc_cote_1, seq_enc_cote_2 --
    for k in range(min(2, n)):
        cote = recent[k][3]
        if cote is not None and cote > 0:
            feats[f"seq_enc_cote_{k + 1}"] = round(math.log1p(cote), 4)

    # -- seq_enc_win_streak_binary --
    # Encode last 5 races as binary integer (most recent = highest bit)
    if n >= 1:
        bits = 0
        for k in range(min(5, n)):
            if recent[k][5]:  # is_winner
                bits |= 1 << (4 - k)
        feats["seq_enc_win_streak_binary"] = bits

    # -- seq_enc_trend_slope --
    # Slope of normalised positions over last 5 races (chronological order)
    norm_positions: list[float] = []
    # We need chronological order for slope, so take from hist (oldest first)
    window = hist[-5:] if n >= 5 else hist
    for entry in window:
        pos = entry[0]
        nb_part = entry[4]
        if pos is not None and nb_part is not None and nb_part > 0:
            norm_positions.append(pos / nb_part)

    feats["seq_enc_trend_slope"] = _slope(norm_positions)

    return feats


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_sequence_encoding_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build sequence-encoded features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Sequence Encoding Builder (memory-optimised) ===")
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

    fill_counts = {k: 0 for k in FEATURE_KEYS}

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

            current_date_days = _parse_date_days(course_date_str)

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                cheval = rec.get("nom_cheval")

                if not cheval:
                    # Still emit a record with Nones
                    out_rec = {
                        "partant_uid": rec.get("partant_uid"),
                        "course_uid": rec.get("course_uid"),
                        "date_reunion_iso": rec.get("date_reunion_iso"),
                    }
                    for k in FEATURE_KEYS:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[cheval]

                # Compute features from pre-race state (BEFORE updating)
                feats = _compute_features(hs)

                out_rec = {
                    "partant_uid": rec.get("partant_uid"),
                    "course_uid": rec.get("course_uid"),
                    "date_reunion_iso": rec.get("date_reunion_iso"),
                }
                for k in FEATURE_KEYS:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # -- Prepare deferred state update --
                position = None
                pos_raw = rec.get("place_arrivee") or rec.get("position_arrivee")
                if pos_raw is not None:
                    try:
                        position = int(pos_raw)
                    except (ValueError, TypeError):
                        pass

                speed_figure = None
                sf_raw = rec.get("speed_figure") or rec.get("vitesse_moyenne")
                if sf_raw is not None:
                    try:
                        speed_figure = float(sf_raw)
                    except (ValueError, TypeError):
                        pass

                # Days since previous race for this horse
                days_since_prev = None
                if current_date_days is not None and hs.last_date_days is not None:
                    days_since_prev = max(0, current_date_days - hs.last_date_days)

                cote = None
                cote_raw = rec.get("cote_finale") or rec.get("rapport_final")
                if cote_raw is not None:
                    try:
                        cote = float(cote_raw)
                        if cote <= 0:
                            cote = None
                    except (ValueError, TypeError):
                        pass

                nombre_partants = None
                np_raw = rec.get("nombre_partants") or rec.get("nb_partants")
                if np_raw is not None:
                    try:
                        nombre_partants = int(np_raw)
                        if nombre_partants <= 0:
                            nombre_partants = None
                    except (ValueError, TypeError):
                        pass

                is_winner = bool(rec.get("is_gagnant"))

                post_updates.append((
                    cheval, position, speed_figure, days_since_prev,
                    cote, nombre_partants, is_winner, current_date_days,
                ))

            # -- Update horse states after race (no leakage) --
            for (
                cheval, position, speed_figure, days_since_prev,
                cote, nombre_partants, is_winner, date_days,
            ) in post_updates:
                hs = horse_state[cheval]
                hs.history.append((
                    position, speed_figure, days_since_prev,
                    cote, nombre_partants, is_winner,
                ))
                if date_days is not None:
                    hs.last_date_days = date_days

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Sequence encoding build termine: %d features en %.1fs (chevaux suivis: %d)",
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
        description="Construction des features sequence-encoding a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/sequence_encoding/)",
    )
    args = parser.parse_args()

    logger = setup_logging("sequence_encoding_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "sequence_encoding_features.jsonl"
    build_sequence_encoding_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
