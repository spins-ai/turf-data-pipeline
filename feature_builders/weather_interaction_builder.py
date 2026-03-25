#!/usr/bin/env python3
"""
feature_builders.weather_interaction_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross weather/meteo data with horse performance history.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant weather-interaction features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the weather statistics -- no future leakage.

Produces:
  - weather_interaction.jsonl   in output/weather_interaction/

Features per partant (6):
  - horse_rain_win_rate       : horse's win rate when met_impact_meteo_score > 0.5
  - horse_dry_win_rate        : horse's win rate when met_impact_meteo_score <= 0.5
  - rain_advantage            : rain_win_rate - dry_win_rate (positive = prefers rain)
  - terrain_lourd_specialist  : horse's win rate on 'lourd' terrain
  - wind_sensitivity          : std of positions vs wind speed (high = sensitive)
  - temperature_optimum       : distance from horse's best-performing temperature range

Usage:
    python feature_builders/weather_interaction_builder.py
    python feature_builders/weather_interaction_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
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
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "weather_interaction"

_LOG_EVERY = 500_000
_MIN_OBS = 2  # Minimum observations for a rate to be non-None

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


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Safely convert to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# TERRAIN NORMALISATION
# ===========================================================================

_LOURD_KEYWORDS = {"lourd", "heavy", "tres lourd", "très lourd"}


def _is_terrain_lourd(rec: dict) -> bool:
    """Check if the terrain is 'lourd' from multiple fields."""
    for field in ("cnd_cond_type_terrain", "met_terrain_predit", "penetrometre", "terrain"):
        val = rec.get(field)
        if val and isinstance(val, str):
            if val.strip().lower() in _LOURD_KEYWORDS:
                return True
    return False


def _classify_weather(rec: dict) -> Optional[str]:
    """Classify weather as 'rain' or 'dry' using met_impact_meteo_score.

    Returns 'rain' if score > 0.5, 'dry' if score <= 0.5, None if unknown.
    """
    score = _safe_float(rec.get("met_impact_meteo_score"))
    if score is not None:
        return "rain" if score > 0.5 else "dry"
    return None


def _extract_wind(rec: dict) -> Optional[float]:
    """Extract wind speed from record, trying multiple fields."""
    for field in ("reu_vent_vitesse", "meteo_wind_speed_kmh", "vent_vitesse"):
        val = _safe_float(rec.get(field))
        if val is not None:
            return val
    return None


def _extract_temperature(rec: dict) -> Optional[float]:
    """Extract temperature from record, trying multiple fields."""
    for field in (
        "reu_temperature", "meteo_temperature_c", "temperature",
        "met_temperature", "cnd_temperature", "temperature_2m",
        "meteo_temp", "temp_celsius",
    ):
        val = _safe_float(rec.get(field))
        if val is not None and -30 < val < 55:  # sanity bounds
            return val
    return None


# ===========================================================================
# PER-HORSE WEATHER STATE
# ===========================================================================


class _HorseWeatherState:
    """Accumulates per-horse weather performance stats."""

    __slots__ = (
        "rain_wins", "rain_runs",
        "dry_wins", "dry_runs",
        "lourd_wins", "lourd_runs",
        "wind_positions",       # list of (wind_speed, position)
        "temp_performances",    # list of (temperature, position)
    )

    def __init__(self) -> None:
        self.rain_wins: int = 0
        self.rain_runs: int = 0
        self.dry_wins: int = 0
        self.dry_runs: int = 0
        self.lourd_wins: int = 0
        self.lourd_runs: int = 0
        self.wind_positions: list[tuple[float, int]] = []
        self.temp_performances: list[tuple[float, int]] = []

    def snapshot_rain_wr(self) -> Optional[float]:
        if self.rain_runs < _MIN_OBS:
            return None
        return round(self.rain_wins / self.rain_runs, 4)

    def snapshot_dry_wr(self) -> Optional[float]:
        if self.dry_runs < _MIN_OBS:
            return None
        return round(self.dry_wins / self.dry_runs, 4)

    def snapshot_lourd_wr(self) -> Optional[float]:
        if self.lourd_runs < _MIN_OBS:
            return None
        return round(self.lourd_wins / self.lourd_runs, 4)

    def snapshot_wind_sensitivity(self) -> Optional[float]:
        """Std deviation of positions when there's wind data.

        Higher = more inconsistent under varying wind conditions.
        """
        if len(self.wind_positions) < _MIN_OBS:
            return None
        positions = [p for _, p in self.wind_positions]
        mean = sum(positions) / len(positions)
        variance = sum((p - mean) ** 2 for p in positions) / len(positions)
        return round(math.sqrt(variance), 4)

    def snapshot_temperature_optimum(self, current_temp: Optional[float]) -> Optional[float]:
        """Distance from horse's best-performing temperature range.

        Uses a weighted approach: temperatures from top-3 finishes get
        weight 3, top-5 get weight 2, others get weight 1.
        Falls back to all-performance average if insufficient top finishes.
        Returns absolute distance from that optimum.
        """
        if current_temp is None:
            return None
        if len(self.temp_performances) < _MIN_OBS:
            return None
        # Weighted average: better finishes get more weight
        total_w = 0.0
        weighted_sum = 0.0
        for t, pos in self.temp_performances:
            if pos <= 3:
                w = 3.0
            elif pos <= 5:
                w = 2.0
            else:
                w = 1.0
            weighted_sum += t * w
            total_w += w
        if total_w == 0:
            return None
        best_temp = weighted_sum / total_w
        return round(abs(current_temp - best_temp), 2)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_weather_interaction_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build weather interaction features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory.
      2. Sort chronologically.
      3. Process record-by-record, snapshotting pre-race then updating.
    """
    logger.info("=== Weather Interaction Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
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
            "gagnant": bool(rec.get("is_gagnant")),
            "position": _safe_int(rec.get("position_arrivee")),
            "weather": _classify_weather(rec),
            "is_lourd": _is_terrain_lourd(rec),
            "wind": _extract_wind(rec),
            "temperature": _extract_temperature(rec),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process record by record --
    t2 = time.time()
    horse_states: dict[str, _HorseWeatherState] = defaultdict(_HorseWeatherState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (i < total
               and slim_records[i]["course"] == course_uid
               and slim_records[i]["date"] == course_date):
            course_group.append(slim_records[i])
            i += 1

        # -- Snapshot pre-race features for all partants --
        for rec in course_group:
            cheval = rec["cheval"]

            if not cheval:
                results.append({
                    "partant_uid": rec["uid"],
                    "horse_rain_win_rate": None,
                    "horse_dry_win_rate": None,
                    "rain_advantage": None,
                    "terrain_lourd_specialist": None,
                    "wind_sensitivity": None,
                    "temperature_optimum": None,
                })
                continue

            state = horse_states[cheval]

            rain_wr = state.snapshot_rain_wr()
            dry_wr = state.snapshot_dry_wr()

            if rain_wr is not None and dry_wr is not None:
                rain_adv = round(rain_wr - dry_wr, 4)
            else:
                rain_adv = None

            lourd_wr = state.snapshot_lourd_wr()
            wind_sens = state.snapshot_wind_sensitivity()
            temp_opt = state.snapshot_temperature_optimum(rec["temperature"])

            results.append({
                "partant_uid": rec["uid"],
                "horse_rain_win_rate": rain_wr,
                "horse_dry_win_rate": dry_wr,
                "rain_advantage": rain_adv,
                "terrain_lourd_specialist": lourd_wr,
                "wind_sensitivity": wind_sens,
                "temperature_optimum": temp_opt,
            })

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            state = horse_states[cheval]
            is_win = rec["gagnant"]
            pos = rec["position"]

            # Weather classification
            weather = rec["weather"]
            if weather == "rain":
                state.rain_runs += 1
                if is_win:
                    state.rain_wins += 1
            elif weather == "dry":
                state.dry_runs += 1
                if is_win:
                    state.dry_wins += 1

            # Terrain lourd
            if rec["is_lourd"]:
                state.lourd_runs += 1
                if is_win:
                    state.lourd_wins += 1

            # Wind sensitivity (need both wind and position)
            wind = rec["wind"]
            if wind is not None and pos is not None:
                state.wind_positions.append((wind, pos))

            # Temperature performance (need both temp and position)
            temp = rec["temperature"]
            if temp is not None and pos is not None:
                state.temp_performances.append((temp, pos))

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Weather interaction build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_states),
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
        description="Construction des features meteo x performance a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/weather_interaction/)",
    )
    args = parser.parse_args()

    logger = setup_logging("weather_interaction_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_weather_interaction_features(input_path, logger)

    # Save
    out_path = output_dir / "weather_interaction.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
