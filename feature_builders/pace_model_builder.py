#!/usr/bin/env python3
"""
feature_builders.pace_model_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pace / running-style features for race simulation modules and prediction models.

Reads partants_master.jsonl in index+seek streaming mode, processes all
records chronologically, and computes per-partant pace features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the pace statistics -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - pace_model_features.jsonl  in builder_outputs/pace_model/

Features per partant (10):
  - pace_early_speed_index       : ratio of wins at short (<2000m) vs long distances
  - pace_horse_style_score       : -1 (closer) to +1 (front runner) based on
                                   weighted recent positions relative to field
                                   size and distance
  - pace_field_pace_pressure     : count of likely front-runners in field /
                                   nombre_partants
  - pace_estimated_position_early: predicted early position from num_pmu
                                   weighted by horse style
  - pace_collapse_risk           : probability of pace collapse = pressure *
                                   distance / 2000
  - pace_closer_advantage        : 1 - collapse_risk when horse is a closer
  - pace_distance_speed_ratio    : reduction_km_ms / distance (normalised speed)
  - pace_horse_speed_consistency : std dev of horse's last 5 reduction_km values
  - pace_speed_vs_field          : horse avg speed - field avg speed at this
                                   distance band
  - pace_tactical_advantage      : combination of style matching conditions
                                   (closer in high-pace field = advantage,
                                   front runner in low-pace = advantage)

Usage:
    python feature_builders/pace_model_builder.py
    python feature_builders/pace_model_builder.py --input path/to/partants_master.jsonl
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
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pace_model"
)

_LOG_EVERY = 500_000
_DIST_SHORT_THRESHOLD = 2000  # metres
_STYLE_WINDOW = 10
_SPEED_WINDOW = 10


# ===========================================================================
# DISTANCE BAND HELPER
# ===========================================================================


def _distance_band(dist_m) -> Optional[int]:
    """Round distance to nearest 400m band (e.g. 2400, 2800).

    Returns None if distance is not usable.
    """
    if dist_m is None:
        return None
    try:
        d = int(dist_m)
    except (ValueError, TypeError):
        return None
    if d <= 0:
        return None
    return round(d / 400) * 400


def _safe_stdev(values: list[float]) -> Optional[float]:
    """Population standard deviation. None if fewer than 2 values."""
    n = len(values)
    if n < 2:
        return None
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / n
    return round(math.sqrt(var), 4)


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseState:
    """Track rolling pace state for one horse."""

    __slots__ = (
        "recent_positions",  # deque(maxlen=10) of (position, nombre_partants, distance)
        "recent_speeds",     # deque(maxlen=10) of reduction_km_ms float
        "wins_short",        # count of wins at distance < 2000
        "wins_long",         # count of wins at distance >= 2000
        "total_short",       # total races at distance < 2000
        "total_long",        # total races at distance >= 2000
    )

    def __init__(self) -> None:
        self.recent_positions: deque = deque(maxlen=_STYLE_WINDOW)
        self.recent_speeds: deque = deque(maxlen=_SPEED_WINDOW)
        self.wins_short: int = 0
        self.wins_long: int = 0
        self.total_short: int = 0
        self.total_long: int = 0


# ===========================================================================
# PER-DISTANCE-BAND STATE (for field speed comparison)
# ===========================================================================


class _BandStats:
    """Aggregate speed stats for a distance band."""

    __slots__ = ("sum_speeds", "count")

    def __init__(self) -> None:
        self.sum_speeds: float = 0.0
        self.count: int = 0

    def avg(self) -> Optional[float]:
        if self.count == 0:
            return None
        return self.sum_speeds / self.count


# ===========================================================================
# FEATURE COMPUTATION (from snapshot before update)
# ===========================================================================


def _compute_style_score(positions: deque) -> Optional[float]:
    """Compute horse running style score from -1 (closer) to +1 (front runner).

    Horses that finish near the front of the field at short distances get
    positive scores (front runners).  Horses that finish from behind at
    long distances get negative scores (closers).

    Positions are weighted: most recent races weigh more.
    """
    if len(positions) < 2:
        return None

    score = 0.0
    weight_sum = 0.0

    for idx, (pos, field_size, dist) in enumerate(positions):
        if pos is None or field_size is None or field_size <= 0:
            continue
        # Weight: more recent = higher weight (exponential decay)
        w = 1.0 + idx * 0.3  # older entries have higher idx in deque (FIFO)
        # Actually deque appends to right, so index 0 = oldest
        # Reverse: weight = len - idx
        w = len(positions) - idx

        # Relative position: 0 (led) to 1 (last)
        rel_pos = (pos - 1) / max(field_size - 1, 1)

        # Distance factor: short races where horse was in front = front runner signal
        dist_factor = 1.0
        if dist is not None:
            if dist < _DIST_SHORT_THRESHOLD:
                dist_factor = 1.2  # amplify short-distance signal
            else:
                dist_factor = 0.8  # amplify long-distance closer signal

        # Front runners: low relative position = positive contribution
        # Closers: high relative position at long distances = negative
        contribution = (0.5 - rel_pos) * 2.0 * dist_factor
        score += contribution * w
        weight_sum += w

    if weight_sum == 0:
        return None

    raw = score / weight_sum
    # Clamp to [-1, +1]
    return round(max(-1.0, min(1.0, raw)), 4)


def _compute_features(
    hs: _HorseState,
    num_pmu: int,
    distance: Optional[int],
    reduction_km_ms: Optional[float],
    nombre_partants: Optional[int],
    course_style_scores: list[Optional[float]],
    band: Optional[int],
    band_stats: dict[int, _BandStats],
) -> dict[str, Any]:
    """Compute all 10 pace features from the horse's pre-race state."""
    feats: dict[str, Any] = {}

    # 1. pace_early_speed_index: ratio of wins at short vs long
    total_races = hs.total_short + hs.total_long
    if total_races >= 3:
        short_wr = hs.wins_short / max(hs.total_short, 1)
        long_wr = hs.wins_long / max(hs.total_long, 1)
        # Index > 1 means better at short = front runner tendency
        if long_wr > 0:
            feats["pace_early_speed_index"] = round(short_wr / long_wr, 4)
        elif short_wr > 0:
            feats["pace_early_speed_index"] = round(short_wr * 10, 4)  # cap proxy
        else:
            feats["pace_early_speed_index"] = 1.0  # neutral
    else:
        feats["pace_early_speed_index"] = None

    # 2. pace_horse_style_score: -1 (closer) to +1 (front runner)
    style = _compute_style_score(hs.recent_positions)
    feats["pace_horse_style_score"] = style

    # 3. pace_field_pace_pressure: fraction of likely front runners in field
    if course_style_scores and nombre_partants and nombre_partants > 0:
        front_runners = sum(
            1 for s in course_style_scores if s is not None and s > 0
        )
        feats["pace_field_pace_pressure"] = round(
            front_runners / nombre_partants, 4
        )
    else:
        feats["pace_field_pace_pressure"] = None

    # 4. pace_estimated_position_early: predicted early position
    if style is not None and nombre_partants and nombre_partants > 0:
        # Front runners (positive style) expected toward front
        # Base estimate from num_pmu, adjusted by style
        base = min(num_pmu, nombre_partants) if num_pmu else nombre_partants // 2
        adjustment = -style * (nombre_partants / 3.0)
        est = max(1.0, min(float(nombre_partants), base + adjustment))
        feats["pace_estimated_position_early"] = round(est, 2)
    else:
        feats["pace_estimated_position_early"] = None

    # 5. pace_collapse_risk: pressure * distance / 2000
    pressure = feats.get("pace_field_pace_pressure")
    if pressure is not None and distance is not None and distance > 0:
        feats["pace_collapse_risk"] = round(
            pressure * distance / 2000.0, 4
        )
    else:
        feats["pace_collapse_risk"] = None

    # 6. pace_closer_advantage: benefit to closers from potential collapse
    collapse_risk = feats.get("pace_collapse_risk")
    if collapse_risk is not None and style is not None and style < 0:
        feats["pace_closer_advantage"] = round(1.0 - collapse_risk, 4)
    else:
        feats["pace_closer_advantage"] = None

    # 7. pace_distance_speed_ratio: reduction_km_ms / distance
    if reduction_km_ms is not None and distance is not None and distance > 0:
        feats["pace_distance_speed_ratio"] = round(
            reduction_km_ms / distance, 6
        )
    else:
        feats["pace_distance_speed_ratio"] = None

    # 8. pace_horse_speed_consistency: std dev of last 5 speeds
    speed_vals = [v for v in list(hs.recent_speeds)[-5:] if v is not None]
    feats["pace_horse_speed_consistency"] = _safe_stdev(speed_vals)

    # 9. pace_speed_vs_field: horse avg speed - field avg at distance band
    if band is not None and band in band_stats:
        field_avg = band_stats[band].avg()
        horse_speeds = [v for v in hs.recent_speeds if v is not None]
        if horse_speeds and field_avg is not None:
            horse_avg = sum(horse_speeds) / len(horse_speeds)
            feats["pace_speed_vs_field"] = round(horse_avg - field_avg, 4)
        else:
            feats["pace_speed_vs_field"] = None
    else:
        feats["pace_speed_vs_field"] = None

    # 10. pace_tactical_advantage: style matching conditions
    if style is not None and pressure is not None:
        # Closer in high-pace field = advantage
        # Front runner in low-pace field = advantage
        if style < 0 and pressure > 0.4:
            # Closer benefits from fast pace
            feats["pace_tactical_advantage"] = round(
                abs(style) * pressure, 4
            )
        elif style > 0 and pressure < 0.3:
            # Front runner benefits from slow pace (few competitors up front)
            feats["pace_tactical_advantage"] = round(
                style * (1.0 - pressure), 4
            )
        else:
            feats["pace_tactical_advantage"] = 0.0
    else:
        feats["pace_tactical_advantage"] = None

    return feats


# ===========================================================================
# STATE UPDATE (post-race, deferred to avoid leakage)
# ===========================================================================


def _update_state(
    hs: _HorseState,
    position: Optional[int],
    nombre_partants: Optional[int],
    distance: Optional[int],
    reduction_km_ms: Optional[float],
    is_winner: bool,
    band: Optional[int],
    band_stats: dict[int, _BandStats],
) -> None:
    """Update horse state and global band stats after a race."""
    # Update recent positions
    hs.recent_positions.append((position, nombre_partants, distance))

    # Update recent speeds
    if reduction_km_ms is not None:
        hs.recent_speeds.append(reduction_km_ms)

    # Update distance-split win counts
    if distance is not None:
        if distance < _DIST_SHORT_THRESHOLD:
            hs.total_short += 1
            if is_winner:
                hs.wins_short += 1
        else:
            hs.total_long += 1
            if is_winner:
                hs.wins_long += 1

    # Update global distance-band speed stats
    if band is not None and reduction_km_ms is not None:
        bs = band_stats[band]
        bs.sum_speeds += reduction_km_ms
        bs.count += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_pace_model_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build pace model features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Pace Model Builder (memory-optimised) ===")
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
    band_stats: dict[int, _BandStats] = defaultdict(_BandStats)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "pace_early_speed_index",
        "pace_horse_style_score",
        "pace_field_pace_pressure",
        "pace_estimated_position_early",
        "pace_collapse_risk",
        "pace_closer_advantage",
        "pace_distance_speed_ratio",
        "pace_horse_speed_consistency",
        "pace_speed_vs_field",
        "pace_tactical_advantage",
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
            course_records = [
                _read_record_at(index[ci][3]) for ci in course_indices
            ]

            # Extract nombre_partants from field
            nombre_partants = None
            for rec in course_records:
                np_val = rec.get("nombre_partants")
                if np_val is not None:
                    try:
                        nombre_partants = int(np_val)
                    except (ValueError, TypeError):
                        pass
                    break
            if nombre_partants is None:
                nombre_partants = len(course_records)

            # -- Pre-compute style scores for all runners (for field pressure) --
            course_style_scores: list[Optional[float]] = []
            course_horses: list[Optional[str]] = []
            for rec in course_records:
                cheval = rec.get("nom_cheval")
                course_horses.append(cheval)
                if cheval and cheval in horse_state:
                    course_style_scores.append(
                        _compute_style_score(horse_state[cheval].recent_positions)
                    )
                else:
                    course_style_scores.append(None)

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec_idx, rec in enumerate(course_records):
                cheval = rec.get("nom_cheval")

                # Parse fields
                num_pmu_val = rec.get("num_pmu", 0) or 0
                try:
                    num_pmu_val = int(num_pmu_val)
                except (ValueError, TypeError):
                    num_pmu_val = 0

                distance = None
                dist_raw = rec.get("distance") or rec.get("distance_metres")
                if dist_raw is not None:
                    try:
                        distance = int(dist_raw)
                    except (ValueError, TypeError):
                        pass

                reduction_km_ms = None
                rkm_raw = rec.get("reduction_km_ms") or rec.get("reduction_km")
                if rkm_raw is not None:
                    try:
                        reduction_km_ms = float(rkm_raw)
                    except (ValueError, TypeError):
                        pass

                band = _distance_band(distance)

                if cheval and cheval in horse_state:
                    hs = horse_state[cheval]
                else:
                    hs = _HorseState()  # empty state for first-time horses

                feats = _compute_features(
                    hs,
                    num_pmu_val,
                    distance,
                    reduction_km_ms,
                    nombre_partants,
                    course_style_scores,
                    band,
                    band_stats,
                )

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
                position = None
                pos_raw = rec.get("place_arrivee") or rec.get("position_arrivee")
                if pos_raw is not None:
                    try:
                        position = int(pos_raw)
                    except (ValueError, TypeError):
                        pass

                is_winner = bool(rec.get("is_gagnant"))

                post_updates.append((
                    cheval, position, nombre_partants,
                    distance, reduction_km_ms, is_winner, band,
                ))

            # -- Update horse states after race (no leakage) --
            for (
                cheval, position, np_val,
                dist, rkm, is_winner, band_val,
            ) in post_updates:
                if not cheval:
                    continue
                _update_state(
                    horse_state[cheval],
                    position, np_val, dist, rkm, is_winner,
                    band_val, band_stats,
                )

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Pace model build termine: %d features en %.1fs (chevaux suivis: %d)",
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
        description="Construction des features pace model a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/pace_model/)",
    )
    args = parser.parse_args()

    logger = setup_logging("pace_model_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "pace_model_features.jsonl"
    build_pace_model_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
