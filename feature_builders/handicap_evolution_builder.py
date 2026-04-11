#!/usr/bin/env python3
"""
feature_builders.handicap_evolution_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Handicap value evolution features per horse over time.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the evolution metrics -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - handicap_evolution_features.jsonl  in builder_outputs/handicap_evolution/

Features per partant (10):
  - hev_handicap_value            : handicap_valeur as float (current)
  - hev_handicap_delta            : change in handicap_valeur from last race
  - hev_handicap_trend_3          : average delta over last 3 races
  - hev_handicap_max              : maximum handicap_valeur seen for this horse
  - hev_handicap_vs_max           : current / max (how close to peak form)
  - hev_weight_carried_trend      : trend of poids_porte_kg over last 3 races
  - hev_handicap_win_rate_bracket : win rate at similar handicap value (+/-2)
  - hev_is_handicap_rise          : 1 if handicap went up from last race
  - hev_races_since_handicap_change : races since last handicap change
  - hev_surcharge_trend           : trend in surcharge_decharge over last 3 races

Usage:
    python feature_builders/handicap_evolution_builder.py
    python feature_builders/handicap_evolution_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/handicap_evolution")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v
    except (ValueError, TypeError):
        return None


def _bracket_key(handicap_val: float) -> int:
    """Round handicap value to nearest even integer for +/-2 bracket grouping."""
    return round(handicap_val / 2.0) * 2


def _trend_from_deltas(deltas: list[float], n: int = 3) -> Optional[float]:
    """Average of last n deltas. None if fewer than n values."""
    recent = deltas[-n:] if len(deltas) >= n else None
    if recent is None:
        return None
    return round(sum(recent) / len(recent), 4)


def _weight_trend(values: list[float], n: int = 3) -> Optional[float]:
    """Simple slope of last n weight values.

    Returns average change per step. None if fewer than 2 values in window.
    """
    recent = values[-n:] if len(values) >= n else list(values)
    if len(recent) < 2:
        return None
    total_change = recent[-1] - recent[0]
    return round(total_change / (len(recent) - 1), 4)


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseState:
    """Track rolling handicap state for one horse."""

    __slots__ = (
        "last_handicap",      # float or None: handicap_valeur of previous race
        "handicap_history",   # deque(maxlen=5): last 5 handicap_valeur values
        "max_handicap",       # float or None: best (max) handicap seen
        "weight_history",     # deque(maxlen=5): last 5 poids_porte_kg values
        "surcharge_history",  # deque(maxlen=5): last 5 surcharge_decharge values
        "bracket_wins",       # dict[int, int]: bracket_key -> win count
        "bracket_total",      # dict[int, int]: bracket_key -> total count
        "races_since_change", # int: races since last handicap change
        "handicap_deltas",    # deque(maxlen=5): last 5 handicap deltas
    )

    def __init__(self) -> None:
        self.last_handicap: Optional[float] = None
        self.handicap_history: deque = deque(maxlen=5)
        self.max_handicap: Optional[float] = None
        self.weight_history: deque = deque(maxlen=5)
        self.surcharge_history: deque = deque(maxlen=5)
        self.bracket_wins: dict[int, int] = defaultdict(int)
        self.bracket_total: dict[int, int] = defaultdict(int)
        self.races_since_change: int = 0
        self.handicap_deltas: deque = deque(maxlen=5)


# ===========================================================================
# FEATURE COMPUTATION (from snapshot BEFORE update)
# ===========================================================================


def _compute_features(
    hs: _HorseState,
    current_handicap: Optional[float],
    current_weight: Optional[float],
    current_surcharge: Optional[float],
) -> dict[str, Any]:
    """Compute all 10 handicap evolution features from pre-race state."""
    feats: dict[str, Any] = {}

    # 1. hev_handicap_value: current handicap_valeur
    feats["hev_handicap_value"] = round(current_handicap, 2) if current_handicap is not None else None

    # 2. hev_handicap_delta: change from last race
    if current_handicap is not None and hs.last_handicap is not None:
        feats["hev_handicap_delta"] = round(current_handicap - hs.last_handicap, 2)
    else:
        feats["hev_handicap_delta"] = None

    # 3. hev_handicap_trend_3: average delta over last 3 races
    deltas = list(hs.handicap_deltas)
    # Include current delta if computable
    if current_handicap is not None and hs.last_handicap is not None:
        deltas_with_current = deltas + [current_handicap - hs.last_handicap]
    else:
        deltas_with_current = deltas
    feats["hev_handicap_trend_3"] = _trend_from_deltas(deltas_with_current, 3)

    # 4. hev_handicap_max: maximum handicap seen (pre-race state)
    feats["hev_handicap_max"] = round(hs.max_handicap, 2) if hs.max_handicap is not None else None

    # 5. hev_handicap_vs_max: current / max
    if current_handicap is not None and hs.max_handicap is not None and hs.max_handicap > 0:
        feats["hev_handicap_vs_max"] = round(current_handicap / hs.max_handicap, 4)
    else:
        feats["hev_handicap_vs_max"] = None

    # 6. hev_weight_carried_trend: trend of poids_porte_kg over last 3
    weight_vals = [v for v in hs.weight_history if v is not None]
    if current_weight is not None:
        weight_vals_with_current = weight_vals + [current_weight]
    else:
        weight_vals_with_current = weight_vals
    feats["hev_weight_carried_trend"] = _weight_trend(weight_vals_with_current, 3)

    # 7. hev_handicap_win_rate_bracket: win rate at similar handicap (+/-2)
    if current_handicap is not None:
        bk = _bracket_key(current_handicap)
        total = hs.bracket_total.get(bk, 0)
        if total >= 3:
            wins = hs.bracket_wins.get(bk, 0)
            feats["hev_handicap_win_rate_bracket"] = round(wins / total, 4)
        else:
            feats["hev_handicap_win_rate_bracket"] = None
    else:
        feats["hev_handicap_win_rate_bracket"] = None

    # 8. hev_is_handicap_rise: 1 if handicap went up from last race
    if current_handicap is not None and hs.last_handicap is not None:
        feats["hev_is_handicap_rise"] = 1 if current_handicap > hs.last_handicap else 0
    else:
        feats["hev_is_handicap_rise"] = None

    # 9. hev_races_since_handicap_change: races since last change
    if hs.last_handicap is not None:
        feats["hev_races_since_handicap_change"] = hs.races_since_change
    else:
        feats["hev_races_since_handicap_change"] = None

    # 10. hev_surcharge_trend: trend in surcharge_decharge over last 3
    surcharge_vals = [v for v in hs.surcharge_history if v is not None]
    if current_surcharge is not None:
        surcharge_vals_with_current = surcharge_vals + [current_surcharge]
    else:
        surcharge_vals_with_current = surcharge_vals
    feats["hev_surcharge_trend"] = _weight_trend(surcharge_vals_with_current, 3)

    return feats


# ===========================================================================
# UPDATE HORSE STATE (post-race, AFTER feature extraction)
# ===========================================================================


def _update_state(
    hs: _HorseState,
    handicap_val: Optional[float],
    weight: Optional[float],
    surcharge: Optional[float],
    is_winner: bool,
) -> None:
    """Update the horse's rolling state after a race."""
    # Track handicap delta before updating last_handicap
    if handicap_val is not None and hs.last_handicap is not None:
        delta = handicap_val - hs.last_handicap
        hs.handicap_deltas.append(delta)

        # Track races since handicap change
        if abs(delta) > 0.01:
            hs.races_since_change = 0
        else:
            hs.races_since_change += 1
    elif handicap_val is not None and hs.last_handicap is None:
        # First race with handicap -- reset counter
        hs.races_since_change = 0
    else:
        hs.races_since_change += 1

    # Update handicap history and max
    if handicap_val is not None:
        hs.last_handicap = handicap_val
        hs.handicap_history.append(handicap_val)
        if hs.max_handicap is None or handicap_val > hs.max_handicap:
            hs.max_handicap = handicap_val

        # Update bracket win/total stats
        bk = _bracket_key(handicap_val)
        hs.bracket_total[bk] += 1
        if is_winner:
            hs.bracket_wins[bk] += 1

    # Update weight history
    if weight is not None:
        hs.weight_history.append(weight)

    # Update surcharge history
    if surcharge is not None:
        hs.surcharge_history.append(surcharge)


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_handicap_evolution_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build handicap evolution features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Handicap Evolution Builder (memory-optimised) ===")
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
        "hev_handicap_value",
        "hev_handicap_delta",
        "hev_handicap_trend_3",
        "hev_handicap_max",
        "hev_handicap_vs_max",
        "hev_weight_carried_trend",
        "hev_handicap_win_rate_bracket",
        "hev_is_handicap_rise",
        "hev_races_since_handicap_change",
        "hev_surcharge_trend",
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

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                if not horse_id:
                    # Still emit a record with Nones
                    out_rec = {
                        "partant_uid": rec.get("partant_uid"),
                        "course_uid": rec.get("course_uid"),
                        "date_reunion_iso": rec.get("date_reunion_iso"),
                    }
                    for k in feature_keys:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[horse_id]

                # Extract current race fields
                current_handicap = _safe_float(rec.get("handicap_valeur"))
                current_weight = _safe_float(rec.get("poids_porte_kg"))
                current_surcharge = _safe_float(rec.get("surcharge_decharge_kg"))

                # Compute features from pre-race state
                feats = _compute_features(hs, current_handicap, current_weight, current_surcharge)

                out_rec = {
                    "partant_uid": rec.get("partant_uid"),
                    "course_uid": rec.get("course_uid"),
                    "date_reunion_iso": rec.get("date_reunion_iso"),
                }
                for k in feature_keys:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # Prepare deferred state update
                is_winner = bool(rec.get("is_gagnant"))

                post_updates.append((
                    horse_id, current_handicap, current_weight,
                    current_surcharge, is_winner,
                ))

            # -- Update horse states after race (no leakage) --
            for (
                horse_id, handicap_val, weight,
                surcharge, is_winner,
            ) in post_updates:
                _update_state(
                    horse_state[horse_id],
                    handicap_val, weight, surcharge, is_winner,
                )

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Handicap evolution build termine: %d features en %.1fs (chevaux suivis: %d)",
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
        description="Construction des features evolution handicap a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/handicap_evolution/)",
    )
    args = parser.parse_args()

    logger = setup_logging("handicap_evolution_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "handicap_evolution_features.jsonl"
    build_handicap_evolution_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
