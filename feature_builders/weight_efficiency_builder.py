#!/usr/bin/env python3
"""
feature_builders.weight_efficiency_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Weight carrying efficiency and optimal weight pattern features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically (course-by-course with snapshot-before-update), and
computes per-partant weight efficiency features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the historical weight statistics -- no future leakage.

Produces:
  - weight_efficiency.jsonl   in builder_outputs/weight_efficiency/

Features per partant (16):
  - we_weight_per_meter           : poids_porte_kg / distance (g/m effort ratio)
  - we_surcharge_normalized       : surcharge_decharge_kg / poids_base_kg (relative adjustment)
  - we_avg_weight_winning         : horse's historical avg weight when winning
  - we_avg_weight_losing          : horse's historical avg weight when losing
  - we_weight_win_delta           : avg_weight_winning - avg_weight_losing (optimal direction)
  - we_optimal_weight_center      : midpoint of weight range at best (top-3) performances
  - we_weight_from_optimal        : current weight - optimal_weight_center (deviation)
  - we_weight_change_last         : poids_porte_kg - last race weight (delta)
  - we_winrate_light              : horse win rate when weight < 54 kg
  - we_winrate_medium             : horse win rate when weight 54-58 kg
  - we_winrate_heavy              : horse win rate when weight > 58 kg
  - we_jockey_wr_light            : jockey win rate at weight < 54 kg
  - we_jockey_wr_heavy            : jockey win rate at weight > 58 kg
  - we_weight_vs_field_avg        : poids_porte_kg / field average weight (relative)
  - we_weight_trend               : slope of recent weight assignments (rising = +1, falling = -1)
  - we_handicap_change_last       : handicap_valeur - last race handicap (delta)

Derived interaction (computed inline, not stored separately):
  - we_weight_distance_difficulty : (weight * distance) / 100000 (combined load index)
  - we_weight_vs_usual            : current weight / horse avg weight (more or less than usual)

Usage:
    python feature_builders/weight_efficiency_builder.py
    python feature_builders/weight_efficiency_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/weight_efficiency")

# Progress log every N records
_LOG_EVERY = 500_000

# Weight bucket boundaries (kg)
_LIGHT_THRESHOLD = 54.0
_HEAVY_THRESHOLD = 58.0

# Minimum sample size for reliable win rate
_MIN_SAMPLE = 3


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v and v > 0 else None  # reject NaN and non-positive
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_rate(wins: int, total: int, ndigits: int = 4) -> Optional[float]:
    """Win rate with minimum-sample guard."""
    if total < _MIN_SAMPLE:
        return None
    return round(wins / total, ndigits)


def _weight_bucket(weight: float) -> str:
    """Classify weight into light/medium/heavy bucket."""
    if weight < _LIGHT_THRESHOLD:
        return "light"
    elif weight <= _HEAVY_THRESHOLD:
        return "medium"
    else:
        return "heavy"


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


# ===========================================================================
# PER-HORSE STATE (bounded with deque maxlen=30)
# ===========================================================================


class _HorseWeightState:
    """Track weight history for a single horse, bounded to last 30 races."""

    __slots__ = (
        "weights",           # deque of poids_porte_kg values
        "handicaps",         # deque of handicap_valeur values
        "win_weights",       # deque of weights when winning
        "lose_weights",      # deque of weights when losing
        "top3_weights",      # deque of weights at positions 1-3
        "bucket_wins",       # {bucket: wins}
        "bucket_total",      # {bucket: total}
    )

    def __init__(self) -> None:
        self.weights: deque = deque(maxlen=30)
        self.handicaps: deque = deque(maxlen=30)
        self.win_weights: deque = deque(maxlen=30)
        self.lose_weights: deque = deque(maxlen=30)
        self.top3_weights: deque = deque(maxlen=30)
        self.bucket_wins: dict[str, int] = defaultdict(int)
        self.bucket_total: dict[str, int] = defaultdict(int)

    def snapshot(self, current_weight: Optional[float],
                 current_handicap: Optional[float]) -> dict[str, Any]:
        """Compute features from historical data (before this race)."""
        feats: dict[str, Any] = {}

        # -- Weight change from last race --
        if current_weight is not None and len(self.weights) >= 1:
            feats["we_weight_change_last"] = round(current_weight - self.weights[-1], 2)
        else:
            feats["we_weight_change_last"] = None

        # -- Handicap change from last race --
        if current_handicap is not None and len(self.handicaps) >= 1:
            feats["we_handicap_change_last"] = round(current_handicap - self.handicaps[-1], 2)
        else:
            feats["we_handicap_change_last"] = None

        # -- Avg weight when winning vs losing --
        if len(self.win_weights) >= 1:
            feats["we_avg_weight_winning"] = round(
                sum(self.win_weights) / len(self.win_weights), 2
            )
        else:
            feats["we_avg_weight_winning"] = None

        if len(self.lose_weights) >= 1:
            feats["we_avg_weight_losing"] = round(
                sum(self.lose_weights) / len(self.lose_weights), 2
            )
        else:
            feats["we_avg_weight_losing"] = None

        # -- Win-lose weight delta --
        if feats["we_avg_weight_winning"] is not None and feats["we_avg_weight_losing"] is not None:
            feats["we_weight_win_delta"] = round(
                feats["we_avg_weight_winning"] - feats["we_avg_weight_losing"], 2
            )
        else:
            feats["we_weight_win_delta"] = None

        # -- Optimal weight center (midpoint of top-3 performance weights) --
        if len(self.top3_weights) >= 2:
            feats["we_optimal_weight_center"] = round(
                sum(self.top3_weights) / len(self.top3_weights), 2
            )
        else:
            feats["we_optimal_weight_center"] = None

        # -- Deviation from optimal --
        if current_weight is not None and feats["we_optimal_weight_center"] is not None:
            feats["we_weight_from_optimal"] = round(
                current_weight - feats["we_optimal_weight_center"], 2
            )
        else:
            feats["we_weight_from_optimal"] = None

        # -- Win rate per weight bucket --
        feats["we_winrate_light"] = _safe_rate(
            self.bucket_wins.get("light", 0),
            self.bucket_total.get("light", 0),
        )
        feats["we_winrate_medium"] = _safe_rate(
            self.bucket_wins.get("medium", 0),
            self.bucket_total.get("medium", 0),
        )
        feats["we_winrate_heavy"] = _safe_rate(
            self.bucket_wins.get("heavy", 0),
            self.bucket_total.get("heavy", 0),
        )

        # -- Weight trend: linear slope over recent weights --
        if len(self.weights) >= 3:
            n = len(self.weights)
            xs = list(range(n))
            ys = list(self.weights)
            x_mean = sum(xs) / n
            y_mean = sum(ys) / n
            num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
            den = sum((x - x_mean) ** 2 for x in xs)
            if den > 0:
                slope = num / den
                if slope > 0.1:
                    feats["we_weight_trend"] = 1    # rising class
                elif slope < -0.1:
                    feats["we_weight_trend"] = -1   # dropping class
                else:
                    feats["we_weight_trend"] = 0    # stable
            else:
                feats["we_weight_trend"] = 0
        else:
            feats["we_weight_trend"] = None

        # -- Weight vs usual (current / career avg) --
        if current_weight is not None and len(self.weights) >= 1:
            avg_w = sum(self.weights) / len(self.weights)
            if avg_w > 0:
                feats["we_weight_vs_usual"] = round(current_weight / avg_w, 4)
            else:
                feats["we_weight_vs_usual"] = None
        else:
            feats["we_weight_vs_usual"] = None

        return feats

    def update(self, weight: Optional[float], handicap: Optional[float],
               is_winner: bool, position: Optional[int]) -> None:
        """Update state after race result is known."""
        if weight is not None:
            self.weights.append(weight)
            bucket = _weight_bucket(weight)
            self.bucket_total[bucket] += 1
            if is_winner:
                self.win_weights.append(weight)
                self.bucket_wins[bucket] += 1
            else:
                self.lose_weights.append(weight)
            # Top-3 performances
            if position is not None and 1 <= position <= 3:
                self.top3_weights.append(weight)

        if handicap is not None:
            self.handicaps.append(handicap)


# ===========================================================================
# PER-JOCKEY WEIGHT STATE (bounded)
# ===========================================================================


class _JockeyWeightState:
    """Track jockey performance by weight bucket."""

    __slots__ = ("bucket_wins", "bucket_total")

    def __init__(self) -> None:
        self.bucket_wins: dict[str, int] = defaultdict(int)
        self.bucket_total: dict[str, int] = defaultdict(int)

    def snapshot(self) -> dict[str, Any]:
        """Jockey win rates at light and heavy weight ranges."""
        feats: dict[str, Any] = {}
        feats["we_jockey_wr_light"] = _safe_rate(
            self.bucket_wins.get("light", 0),
            self.bucket_total.get("light", 0),
        )
        feats["we_jockey_wr_heavy"] = _safe_rate(
            self.bucket_wins.get("heavy", 0),
            self.bucket_total.get("heavy", 0),
        )
        return feats

    def update(self, weight: Optional[float], is_winner: bool) -> None:
        if weight is not None:
            bucket = _weight_bucket(weight)
            self.bucket_total[bucket] += 1
            if is_winner:
                self.bucket_wins[bucket] += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort + seek-based processing)
# ===========================================================================


def build_weight_efficiency_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build weight efficiency features from partants_master.jsonl.

    Two-phase approach:
      1. Read sort keys + file byte offsets into memory (lightweight index).
      2. Sort chronologically.
      3. Seek-based course-by-course processing, streaming output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Weight Efficiency Builder ===")
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

    horse_states: dict[str, _HorseWeightState] = defaultdict(_HorseWeightState)
    jockey_states: dict[str, _JockeyWeightState] = defaultdict(_JockeyWeightState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    # Fill rate tracking
    _FEATURE_KEYS = [
        "we_weight_per_meter",
        "we_surcharge_normalized",
        "we_avg_weight_winning",
        "we_avg_weight_losing",
        "we_weight_win_delta",
        "we_optimal_weight_center",
        "we_weight_from_optimal",
        "we_weight_change_last",
        "we_winrate_light",
        "we_winrate_medium",
        "we_winrate_heavy",
        "we_jockey_wr_light",
        "we_jockey_wr_heavy",
        "we_weight_vs_field_avg",
        "we_weight_trend",
        "we_handicap_change_last",
        "we_weight_distance_difficulty",
        "we_weight_vs_usual",
    ]
    fill_counts = {k: 0 for k in _FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval") or "",
                "jockey": rec.get("jockey_driver") or "",
                "poids": _safe_float(rec.get("poids_porte_kg")),
                "poids_base": _safe_float(rec.get("poids_base_kg")),
                "surcharge": _safe_float(rec.get("surcharge_decharge_kg")),
                "handicap": _safe_float(rec.get("handicap_valeur")),
                "distance": _safe_float(rec.get("distance")),
                "gagnant": bool(rec.get("is_gagnant")),
                "position": _safe_int(rec.get("position_arrivee")),
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
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Compute field average weight for this race --
            field_weights = [r["poids"] for r in course_group if r["poids"] is not None]
            field_avg_weight = (
                sum(field_weights) / len(field_weights) if field_weights else None
            )

            # -- Snapshot pre-race stats and emit features --
            post_updates: list[dict] = []

            for rec in course_group:
                horse_id = rec["horse_id"]
                jockey = rec["jockey"]
                poids = rec["poids"]
                poids_base = rec["poids_base"]
                surcharge = rec["surcharge"]
                handicap = rec["handicap"]
                distance = rec["distance"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                # 1. Weight per meter
                if poids is not None and distance is not None and distance > 0:
                    features["we_weight_per_meter"] = round(poids / distance, 6)
                    fill_counts["we_weight_per_meter"] += 1
                else:
                    features["we_weight_per_meter"] = None

                # 2. Surcharge normalized
                if surcharge is not None and poids_base is not None and poids_base > 0:
                    features["we_surcharge_normalized"] = round(surcharge / poids_base, 4)
                    fill_counts["we_surcharge_normalized"] += 1
                else:
                    features["we_surcharge_normalized"] = None

                # 3-11, 15, 18. Horse historical features (snapshot before update)
                if horse_id:
                    h_state = horse_states[horse_id]
                    h_feats = h_state.snapshot(poids, handicap)
                    features.update(h_feats)
                    for k, v in h_feats.items():
                        if v is not None and k in fill_counts:
                            fill_counts[k] += 1
                else:
                    features["we_avg_weight_winning"] = None
                    features["we_avg_weight_losing"] = None
                    features["we_weight_win_delta"] = None
                    features["we_optimal_weight_center"] = None
                    features["we_weight_from_optimal"] = None
                    features["we_weight_change_last"] = None
                    features["we_winrate_light"] = None
                    features["we_winrate_medium"] = None
                    features["we_winrate_heavy"] = None
                    features["we_weight_trend"] = None
                    features["we_handicap_change_last"] = None
                    features["we_weight_vs_usual"] = None

                # 12-13. Jockey weight range performance (snapshot before update)
                if jockey:
                    j_state = jockey_states[jockey]
                    j_feats = j_state.snapshot()
                    features.update(j_feats)
                    for k, v in j_feats.items():
                        if v is not None and k in fill_counts:
                            fill_counts[k] += 1
                else:
                    features["we_jockey_wr_light"] = None
                    features["we_jockey_wr_heavy"] = None

                # 14. Weight relative to field average
                if poids is not None and field_avg_weight is not None and field_avg_weight > 0:
                    features["we_weight_vs_field_avg"] = round(poids / field_avg_weight, 4)
                    fill_counts["we_weight_vs_field_avg"] += 1
                else:
                    features["we_weight_vs_field_avg"] = None

                # 17. Weight-distance interaction (combined load index)
                if poids is not None and distance is not None and distance > 0:
                    features["we_weight_distance_difficulty"] = round(
                        poids * distance / 100000.0, 4
                    )
                    fill_counts["we_weight_distance_difficulty"] += 1
                else:
                    features["we_weight_distance_difficulty"] = None

                # Stream to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update
                post_updates.append(rec)

            # -- Update states after race (post-race, no leakage) --
            for rec in post_updates:
                horse_id = rec["horse_id"]
                jockey = rec["jockey"]
                poids = rec["poids"]
                handicap = rec["handicap"]
                is_winner = rec["gagnant"]
                position = rec["position"]

                if horse_id:
                    horse_states[horse_id].update(poids, handicap, is_winner, position)

                if jockey:
                    jockey_states[jockey].update(poids, is_winner)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Weight efficiency build termine: %d features en %.1fs "
        "(chevaux uniques: %d, jockeys uniques: %d)",
        n_written, elapsed, len(horse_states), len(jockey_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)",
            k, v, n_written, 100 * v / n_written if n_written else 0,
        )

    return n_written


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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features weight efficiency a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/weight_efficiency/)",
    )
    args = parser.parse_args()

    logger = setup_logging("weight_efficiency_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "weight_efficiency.jsonl"
    build_weight_efficiency_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
