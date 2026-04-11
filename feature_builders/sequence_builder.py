#!/usr/bin/env python3
"""
feature_builders.sequence_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sequence features for LSTM/GRU deep learning models.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and builds per-horse sequences of their last 10 races.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the sequence -- no future leakage.

Produces:
  - sequence_features.jsonl   in output/sequence_features/

Features per partant:
  - seq_positions_10   : list of last 10 positions (padded with -1)
  - seq_cotes_10       : list of last 10 cotes finales (padded with -1)
  - seq_distances_10   : list of last 10 distances (padded with -1)
  - seq_jours_entre_10 : list of last 10 inter-race gaps in days (padded with -1)
  - seq_is_winner_10   : list of last 10 win flags 0/1 (padded with -1)
  - seq_length         : actual number of past races (0-10)

All list features are stored as JSON arrays in the JSONL output, ready for
direct consumption by sequence models (LSTM, GRU, Transformer encoders).

Usage:
    python feature_builders/sequence_builder.py
    python feature_builders/sequence_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict, deque
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
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/sequence")

SEQ_LENGTH = 10
PAD_VALUE = -1

# Progress log every N records
_LOG_EVERY = 500_000

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
    """Parse an ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return None


def _pad_sequence(seq: list, length: int, pad_value: int = PAD_VALUE) -> list:
    """Left-pad a sequence to the desired length with pad_value."""
    if len(seq) >= length:
        return seq[-length:]
    return [pad_value] * (length - len(seq)) + seq


# ===========================================================================
# HORSE HISTORY TRACKER
# ===========================================================================


class _HorseHistory:
    """Tracks a rolling window of the last N races for a horse."""

    __slots__ = ("positions", "cotes", "distances", "dates", "is_winner")

    def __init__(self) -> None:
        self.positions: deque[int] = deque(maxlen=SEQ_LENGTH)
        self.cotes: deque[float] = deque(maxlen=SEQ_LENGTH)
        self.distances: deque[int] = deque(maxlen=SEQ_LENGTH)
        self.dates: deque[datetime] = deque(maxlen=SEQ_LENGTH)
        self.is_winner: deque[int] = deque(maxlen=SEQ_LENGTH)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_sequence_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build sequence features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory.
      2. Sort chronologically.
      3. Process record by record, snapshotting each horse's history
         before updating it (temporal integrity).

    Memory budget:
      - Slim records: ~16M records * ~180 bytes = ~2.9 GB
      - Horse histories: ~200K horses * ~500 bytes = ~100 MB
      - Output accumulator: written in streaming mode
    """
    logger.info("=== Sequence Builder (LSTM/GRU features) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "position": rec.get("position_arrivee"),
            "cote": rec.get("cote_finale"),
            "distance": rec.get("distance"),
            "gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process record by record --
    t2 = time.time()
    horse_history: dict[str, _HorseHistory] = defaultdict(_HorseHistory)
    results: list[dict[str, Any]] = []
    n_processed = 0

    for rec in slim_records:
        cheval = rec["cheval"]
        date_dt = _parse_date(rec["date"])

        if not cheval:
            results.append({
                "partant_uid": rec["uid"],
                "seq_positions_10": [PAD_VALUE] * SEQ_LENGTH,
                "seq_cotes_10": [PAD_VALUE] * SEQ_LENGTH,
                "seq_distances_10": [PAD_VALUE] * SEQ_LENGTH,
                "seq_jours_entre_10": [PAD_VALUE] * SEQ_LENGTH,
                "seq_is_winner_10": [PAD_VALUE] * SEQ_LENGTH,
                "seq_length": 0,
            })
            n_processed += 1
            continue

        hist = horse_history[cheval]
        n_past = len(hist.positions)

        # -- Snapshot pre-race sequences (temporal integrity) --
        positions_list = list(hist.positions)
        cotes_list = list(hist.cotes)
        distances_list = list(hist.distances)
        is_winner_list = list(hist.is_winner)

        # Compute inter-race gaps from stored dates
        jours_entre_list: list[float] = []
        dates_list = list(hist.dates)
        for idx in range(len(dates_list)):
            if idx == 0:
                # Gap before first known race: unknown
                jours_entre_list.append(PAD_VALUE)
            else:
                delta = (dates_list[idx] - dates_list[idx - 1]).days
                jours_entre_list.append(delta)

        # If we have a current date and at least one past race, add the
        # gap between the last past race and *this* race as context
        # (but we need the sequence to represent the past races themselves,
        # so the gaps are between consecutive past races).

        results.append({
            "partant_uid": rec["uid"],
            "seq_positions_10": _pad_sequence(positions_list, SEQ_LENGTH),
            "seq_cotes_10": _pad_sequence(cotes_list, SEQ_LENGTH),
            "seq_distances_10": _pad_sequence(distances_list, SEQ_LENGTH),
            "seq_jours_entre_10": _pad_sequence(jours_entre_list, SEQ_LENGTH),
            "seq_is_winner_10": _pad_sequence(is_winner_list, SEQ_LENGTH),
            "seq_length": n_past,
        })

        # -- Update history after snapshot (no leakage) --
        position = rec["position"]
        try:
            position = int(position) if position is not None else 0
        except (ValueError, TypeError):
            position = 0

        cote = rec["cote"]
        try:
            cote = float(cote) if cote is not None else 0.0
        except (ValueError, TypeError):
            cote = 0.0

        distance = rec["distance"]
        try:
            distance = int(distance) if distance is not None else 0
        except (ValueError, TypeError):
            distance = 0

        hist.positions.append(position)
        hist.cotes.append(cote)
        hist.distances.append(distance)
        hist.is_winner.append(1 if rec["gagnant"] else 0)
        if date_dt:
            hist.dates.append(date_dt)

        n_processed += 1
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, len(slim_records))

    elapsed = time.time() - t0
    logger.info(
        "Sequence build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results), elapsed, len(horse_history),
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
        description="Construction des features sequence (LSTM/GRU) a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/sequence_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("sequence_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_sequence_features(input_path, logger)

    # Save
    out_path = output_dir / "sequence_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                val = r.get(k)
                if val is not None:
                    # For list features, check if not all padding
                    if isinstance(val, list):
                        if any(v != PAD_VALUE for v in val):
                            filled[k] += 1
                    elif val != 0:
                        filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
