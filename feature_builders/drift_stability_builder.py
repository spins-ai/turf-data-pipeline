#!/usr/bin/env python3
"""
feature_builders.drift_stability_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Drift / stability features for model decay detection and concept drift.

Measures how stable a horse's performance and the racing environment are
over time.  Useful for detecting when a model's assumptions no longer hold
(e.g. a horse changed class, a hippodrome changed surface, the market
became more/less efficient).

Temporal integrity: for any partant at date D, only races with date < D
contribute to the rolling statistics -- no future leakage.

Produces:
  - drift_stability_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/drift_stability/

Features per partant (10):
  - drf_horse_position_volatility    : rolling std dev of positions (last 10)
  - drf_horse_cote_volatility        : rolling std dev of odds (last 10)
  - drf_horse_performance_stability  : 1 - CV of positions (high = stable)
  - drf_horse_class_drift            : avg class last 5 vs last 5-10
  - drf_hippo_result_stability       : rolling std dev of winning odds (last 50)
  - drf_hippo_favorite_reliability   : proportion of favorites winning (last 50)
  - drf_discipline_trend             : trend in avg winning odds for discipline
  - drf_market_efficiency_trend      : trend of market error at this hippo
  - drf_horse_improving_confidence   : bayesian confidence horse is improving
  - drf_seasonal_adjustment          : deviation from horse's seasonal avg

Usage:
    python feature_builders/drift_stability_builder.py
    python feature_builders/drift_stability_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/drift_stability")

# Progress log every N records
_LOG_EVERY = 500_000

# Season: month -> season code (1=spring, 2=summer, 3=autumn, 4=winter)
_MONTH_TO_SEASON = {
    1: 4, 2: 4, 3: 1, 4: 1, 5: 1,
    6: 2, 7: 2, 8: 2,
    9: 3, 10: 3, 11: 3,
    12: 4,
}

# ===========================================================================
# MATH HELPERS
# ===========================================================================


def _std_dev(values: list | deque) -> Optional[float]:
    """Population standard deviation. Returns None if < 2 values."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    return round(math.sqrt(variance), 4)


def _mean(values: list | deque) -> Optional[float]:
    """Mean. Returns None if empty."""
    if not values:
        return None
    return sum(values) / len(values)


def _coefficient_of_variation(values: list | deque) -> Optional[float]:
    """CV = std / mean. Returns None if < 2 values or mean == 0."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    if mean == 0:
        return None
    variance = sum((x - mean) ** 2 for x in values) / n
    std = math.sqrt(variance)
    return std / abs(mean)


def _linear_slope(values: list | deque) -> Optional[float]:
    """Simple linear regression slope over index 0..n-1. Returns None if < 3 values."""
    n = len(values)
    if n < 3:
        return None
    # x = 0, 1, ..., n-1
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
    return num / den


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class _HorseState:
    """Rolling per-horse state for drift features."""

    __slots__ = ("positions", "cotes", "field_strengths", "season_positions")

    def __init__(self) -> None:
        self.positions: deque = deque(maxlen=20)
        self.cotes: deque = deque(maxlen=20)
        self.field_strengths: deque = deque(maxlen=20)
        # season code (1-4) -> list of positions
        self.season_positions: dict[int, list] = defaultdict(list)


class _HippoState:
    """Rolling per-hippodrome state."""

    __slots__ = ("winning_cotes", "favorite_wins", "favorite_runs", "_history_len")

    def __init__(self) -> None:
        self.winning_cotes: deque = deque(maxlen=50)
        self.favorite_wins: int = 0
        self.favorite_runs: int = 0
        # Track history to maintain sliding window for favorite stats
        self._history_len: deque = deque(maxlen=50)  # 1 if fav won, 0 if fav lost, -1 if no fav

    def add_race_result(self, winning_cote: Optional[float], had_favorite: bool, fav_won: bool) -> None:
        """Update hippodrome state with a race result."""
        if winning_cote is not None:
            self.winning_cotes.append(winning_cote)

        if had_favorite:
            # Evict oldest entry if window is full
            if len(self._history_len) == 50:
                oldest = self._history_len[0]
                if oldest == 1:
                    self.favorite_wins -= 1
                    self.favorite_runs -= 1
                elif oldest == 0:
                    self.favorite_runs -= 1
                # oldest == -1 means no favorite, nothing to adjust

            if fav_won:
                self._history_len.append(1)
                self.favorite_wins += 1
                self.favorite_runs += 1
            else:
                self._history_len.append(0)
                self.favorite_runs += 1
        else:
            if len(self._history_len) == 50:
                oldest = self._history_len[0]
                if oldest == 1:
                    self.favorite_wins -= 1
                    self.favorite_runs -= 1
                elif oldest == 0:
                    self.favorite_runs -= 1
            self._history_len.append(-1)


class _DisciplineState:
    """Rolling per-discipline state."""

    __slots__ = ("winning_cotes_recent", "market_errors")

    def __init__(self) -> None:
        self.winning_cotes_recent: deque = deque(maxlen=100)
        self.market_errors: deque = deque(maxlen=100)


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _compute_horse_features(
    horse: _HorseState,
    current_season: Optional[int],
) -> dict[str, Any]:
    """Snapshot horse drift features BEFORE updating state."""
    feats: dict[str, Any] = {}

    # -- drf_horse_position_volatility: std dev of last 10 positions --
    recent_pos = list(horse.positions)[-10:]
    feats["drf_horse_position_volatility"] = _std_dev(recent_pos)

    # -- drf_horse_cote_volatility: std dev of last 10 odds --
    recent_cotes = list(horse.cotes)[-10:]
    feats["drf_horse_cote_volatility"] = _std_dev(recent_cotes)

    # -- drf_horse_performance_stability: 1 - CV of positions --
    cv = _coefficient_of_variation(recent_pos)
    if cv is not None:
        feats["drf_horse_performance_stability"] = round(max(0.0, 1.0 - cv), 4)
    else:
        feats["drf_horse_performance_stability"] = None

    # -- drf_horse_class_drift: avg class last 5 vs 5-10 --
    # "class" approximated by field_strengths (higher = stronger field = higher class)
    fs_list = list(horse.field_strengths)
    if len(fs_list) >= 6:
        recent_5 = fs_list[-5:]
        older_5 = fs_list[-10:-5] if len(fs_list) >= 10 else fs_list[:-5]
        avg_recent = sum(recent_5) / len(recent_5)
        avg_older = sum(older_5) / len(older_5)
        feats["drf_horse_class_drift"] = round(avg_recent - avg_older, 4)
    else:
        feats["drf_horse_class_drift"] = None

    # -- drf_horse_improving_confidence: bayesian confidence horse is improving --
    all_pos = list(horse.positions)
    if len(all_pos) >= 5:
        slope = _linear_slope(all_pos)
        if slope is not None:
            n = len(all_pos)
            # Negative slope = improving (lower position = better)
            # Confidence scales with sample size: sigmoid-like with n
            confidence_scale = 1.0 - 1.0 / (1.0 + n / 10.0)
            # Transform slope into [-1, 1] range using tanh
            raw_signal = -math.tanh(slope)  # negative slope -> positive signal
            feats["drf_horse_improving_confidence"] = round(
                0.5 + 0.5 * raw_signal * confidence_scale, 4
            )
        else:
            feats["drf_horse_improving_confidence"] = None
    else:
        feats["drf_horse_improving_confidence"] = None

    # -- drf_seasonal_adjustment: deviation from horse's seasonal avg --
    if current_season is not None and horse.season_positions.get(current_season):
        season_pos = horse.season_positions[current_season]
        season_avg = sum(season_pos) / len(season_pos)
        # Overall average across all seasons
        all_season_pos = []
        for s_pos in horse.season_positions.values():
            all_season_pos.extend(s_pos)
        if len(all_season_pos) >= 3:
            overall_avg = sum(all_season_pos) / len(all_season_pos)
            if overall_avg > 0:
                feats["drf_seasonal_adjustment"] = round(
                    (season_avg - overall_avg) / overall_avg, 4
                )
            else:
                feats["drf_seasonal_adjustment"] = None
        else:
            feats["drf_seasonal_adjustment"] = None
    else:
        feats["drf_seasonal_adjustment"] = None

    return feats


def _compute_hippo_features(hippo: _HippoState) -> dict[str, Any]:
    """Snapshot hippodrome drift features."""
    feats: dict[str, Any] = {}

    # -- drf_hippo_result_stability: std dev of winning odds --
    feats["drf_hippo_result_stability"] = _std_dev(hippo.winning_cotes)

    # -- drf_hippo_favorite_reliability: proportion of favorites winning --
    if hippo.favorite_runs >= 5:
        feats["drf_hippo_favorite_reliability"] = round(
            hippo.favorite_wins / hippo.favorite_runs, 4
        )
    else:
        feats["drf_hippo_favorite_reliability"] = None

    return feats


def _compute_discipline_features(disc: _DisciplineState) -> dict[str, Any]:
    """Snapshot discipline drift features."""
    feats: dict[str, Any] = {}

    # -- drf_discipline_trend: trend in avg winning odds --
    if len(disc.winning_cotes_recent) >= 10:
        feats["drf_discipline_trend"] = round(
            _linear_slope(disc.winning_cotes_recent) or 0.0, 6
        )
    else:
        feats["drf_discipline_trend"] = None

    # -- drf_market_efficiency_trend: trend of market errors --
    if len(disc.market_errors) >= 10:
        feats["drf_market_efficiency_trend"] = round(
            _linear_slope(disc.market_errors) or 0.0, 6
        )
    else:
        feats["drf_market_efficiency_trend"] = None

    return feats


# ===========================================================================
# MAIN BUILD (index + seek + streaming output)
# ===========================================================================


def build_drift_stability_features(input_path: Path, output_path: Path, logger) -> int:
    """Build drift/stability features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Drift/Stability Builder (memory-optimised) ===")
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
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)
    hippo_states: dict[str, _HippoState] = defaultdict(_HippoState)
    disc_states: dict[str, _DisciplineState] = defaultdict(_DisciplineState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "drf_horse_position_volatility",
        "drf_horse_cote_volatility",
        "drf_horse_performance_stability",
        "drf_horse_class_drift",
        "drf_hippo_result_stability",
        "drf_hippo_favorite_reliability",
        "drf_discipline_trend",
        "drf_market_efficiency_trend",
        "drf_horse_improving_confidence",
        "drf_seasonal_adjustment",
    ]
    fill_counts = {name: 0 for name in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract fields needed for drift features."""
            position = rec.get("place_officielle") or rec.get("place_arrivee")
            try:
                position = int(position) if position is not None else None
            except (ValueError, TypeError):
                position = None

            cote_finale = rec.get("cote_finale") or rec.get("cote_probable")
            try:
                cote_finale = float(cote_finale) if cote_finale is not None else None
            except (ValueError, TypeError):
                cote_finale = None

            nombre_partants = rec.get("nombre_partants") or 0
            try:
                nombre_partants = int(nombre_partants)
            except (ValueError, TypeError):
                nombre_partants = 0

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            return {
                "partant_uid": rec.get("partant_uid"),
                "course_uid": rec.get("course_uid", ""),
                "date": rec.get("date_reunion_iso", ""),
                "nom_cheval": rec.get("nom_cheval"),
                "hippodrome": rec.get("hippodrome_normalise", ""),
                "discipline": discipline,
                "position": position,
                "cote_finale": cote_finale,
                "is_gagnant": bool(rec.get("is_gagnant")),
                "nombre_partants": nombre_partants,
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

            # Read this course's records from disk
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            race_date = _parse_date(course_date_str)
            current_season = _MONTH_TO_SEASON[race_date.month] if race_date else None

            # Determine field strength proxy: inverse avg cote of field
            cotes_in_race = [r["cote_finale"] for r in course_group if r["cote_finale"] is not None and r["cote_finale"] > 0]
            if cotes_in_race:
                field_strength_proxy = round(sum(1.0 / c for c in cotes_in_race) / len(cotes_in_race), 6)
            else:
                field_strength_proxy = None

            # Determine race-level facts for hippo update
            hippo_name = course_group[0]["hippodrome"] if course_group else ""
            discipline = course_group[0]["discipline"] if course_group else ""

            winning_cote = None
            had_favorite = False
            fav_won = False
            for r in course_group:
                if r["cote_finale"] is not None and r["cote_finale"] < 5.0:
                    had_favorite = True
                if r["is_gagnant"] and r["cote_finale"] is not None:
                    winning_cote = r["cote_finale"]
                    if r["cote_finale"] < 5.0:
                        fav_won = True

            # -- Snapshot BEFORE update: compute features for each partant --
            for rec in course_group:
                cheval = rec["nom_cheval"]
                hippo = rec["hippodrome"]
                disc = rec["discipline"]

                features: dict[str, Any] = {
                    "partant_uid": rec["partant_uid"],
                    "course_uid": rec["course_uid"],
                    "date_reunion_iso": rec["date"],
                }

                # Horse features
                if cheval:
                    horse = horse_states[cheval]
                    h_feats = _compute_horse_features(horse, current_season)
                    features.update(h_feats)
                else:
                    for name in feature_names[:4]:
                        features[name] = None
                    features["drf_horse_improving_confidence"] = None
                    features["drf_seasonal_adjustment"] = None

                # Hippodrome features
                if hippo:
                    hippo_st = hippo_states[hippo]
                    hp_feats = _compute_hippo_features(hippo_st)
                    features.update(hp_feats)
                else:
                    features["drf_hippo_result_stability"] = None
                    features["drf_hippo_favorite_reliability"] = None

                # Discipline features
                if disc:
                    disc_st = disc_states[disc]
                    d_feats = _compute_discipline_features(disc_st)
                    features.update(d_feats)
                else:
                    features["drf_discipline_trend"] = None
                    features["drf_market_efficiency_trend"] = None

                # Track fill rates
                for name in feature_names:
                    if features.get(name) is not None:
                        fill_counts[name] += 1

                # Write record
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER snapshot --
            for rec in course_group:
                cheval = rec["nom_cheval"]
                position = rec["position"]
                cote = rec["cote_finale"]

                if cheval:
                    horse = horse_states[cheval]
                    if position is not None:
                        horse.positions.append(position)
                        if current_season is not None:
                            horse.season_positions[current_season].append(position)
                    if cote is not None:
                        horse.cotes.append(cote)
                    if field_strength_proxy is not None:
                        horse.field_strengths.append(field_strength_proxy)

            # Update hippo state once per race
            if hippo_name:
                hippo_states[hippo_name].add_race_result(winning_cote, had_favorite, fav_won)

            # Update discipline state once per race
            if discipline:
                disc_st = disc_states[discipline]
                if winning_cote is not None:
                    disc_st.winning_cotes_recent.append(winning_cote)
                    # Market error: how far was the winning cote from "fair" odds?
                    # Fair odds for winner = nombre_partants (uniform). Error = |cote - nb_partants|
                    nb = course_group[0]["nombre_partants"]
                    if nb > 0:
                        market_error = abs(winning_cote - nb)
                        disc_st.market_errors.append(market_error)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Drift/Stability build termine: %d features en %.1fs "
        "(chevaux: %d, hippos: %d, disciplines: %d)",
        n_written, elapsed,
        len(horse_states), len(hippo_states), len(disc_states),
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
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features drift/stabilite a partir de partants_master"
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

    logger = setup_logging("drift_stability_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "drift_stability_features.jsonl"
    build_drift_stability_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
