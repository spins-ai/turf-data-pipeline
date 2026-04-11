#!/usr/bin/env python3
"""
feature_builders.hippodrome_weather_combo_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Hippodrome x weather combination features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - hippodrome_weather_combo.jsonl  in output/hippodrome_weather_combo/

Features per partant (8):
  - hwc_hippo_weather_wr        : horse's win rate at this hippo in similar weather
                                  (met_impact_meteo_score bracket: low<=3, mid 3-6, high>6)
  - hwc_hippo_weather_runs      : number of runs in this hippo+weather combo
  - hwc_hippo_surface_wr        : horse's win rate at this hippo on this surface type
  - hwc_weather_specialist      : 1 if horse wins >2x better in current weather bracket
                                  vs other brackets (min 3 races each)
  - hwc_hippo_first_time_weather: 1 if horse has never raced at this hippo in this
                                  weather bracket
  - hwc_global_weather_wr       : overall win rate for all horses in this weather bracket
                                  (market baseline)
  - hwc_weather_advantage       : horse's weather wr - global weather wr
                                  (positive = above average in these conditions)
  - hwc_hippo_season_combo_wr   : horse's win rate at this hippo in this month/season

Usage:
    python feature_builders/hippodrome_weather_combo_builder.py
    python feature_builders/hippodrome_weather_combo_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/hippodrome_weather_combo")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _weather_bracket(score: Optional[float]) -> Optional[str]:
    """Classify met_impact_meteo_score into low/mid/high."""
    if score is None:
        return None
    if score <= 3:
        return "low"
    elif score <= 6:
        return "mid"
    else:
        return "high"


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _month_from_date(date_str: str) -> Optional[int]:
    """Extract month (1-12) from ISO date string."""
    if not date_str or len(date_str) < 7:
        return None
    try:
        return int(date_str[5:7])
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseState:
    """Per-horse hippodrome x weather/surface/season interaction state.

    Tracks:
      - (hippo, weather_bracket) -> [wins, total]
      - (hippo, surface)         -> [wins, total]
      - (hippo, month)           -> [wins, total]
    """

    __slots__ = ("hippo_weather", "hippo_surface", "hippo_month")

    def __init__(self) -> None:
        # key -> [wins, total]
        self.hippo_weather: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.hippo_surface: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.hippo_month: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0, 0])

    def weather_wr_for_bracket(self, bracket: str) -> tuple[int, int]:
        """Return total (wins, total) across ALL hippos for a given weather bracket."""
        wins = 0
        total = 0
        for (_, wb), stats in self.hippo_weather.items():
            if wb == bracket:
                wins += stats[0]
                total += stats[1]
        return wins, total

    def weather_wr_for_other_brackets(self, bracket: str) -> tuple[int, int]:
        """Return total (wins, total) for all brackets OTHER than the given one."""
        wins = 0
        total = 0
        for (_, wb), stats in self.hippo_weather.items():
            if wb != bracket:
                wins += stats[0]
                total += stats[1]
        return wins, total


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_hippodrome_weather_combo_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build hippodrome x weather combo features from partants_master.jsonl.

    Two-phase approach:
      Phase 1: index + sort chronologically (lightweight tuples).
      Phase 2: seek-based course-by-course processing, streaming output.

    Returns total number of feature records written.
    """
    logger.info("=== Hippodrome Weather Combo Builder ===")
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

    # -- Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 2: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseState] = {}
    # Global weather bracket stats: bracket -> [wins, total]
    global_weather: dict[str, list[int]] = defaultdict(lambda: [0, 0])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "hwc_hippo_weather_wr": 0,
        "hwc_hippo_weather_runs": 0,
        "hwc_hippo_surface_wr": 0,
        "hwc_weather_specialist": 0,
        "hwc_hippo_first_time_weather": 0,
        "hwc_global_weather_wr": 0,
        "hwc_weather_advantage": 0,
        "hwc_hippo_season_combo_wr": 0,
    }

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

            # Extract race-level info
            month = _month_from_date(course_date_str)

            # -- Snapshot BEFORE update: compute features --
            for rec in course_records:
                horse_id = rec.get("horse_id")
                partant_uid = rec.get("partant_uid")

                hippo = rec.get("hippodrome_normalise")
                hippo_key = str(hippo).strip().lower() if hippo else None
                surface = rec.get("type_piste")
                surface_key = str(surface).strip().lower() if surface else None
                meteo_score = _safe_float(rec.get("met_impact_meteo_score"))
                w_bracket = _weather_bracket(meteo_score)

                features: dict[str, Any] = {"partant_uid": partant_uid}

                if horse_id and horse_id in horse_state and hippo_key:
                    hs = horse_state[horse_id]

                    # --- 1. hwc_hippo_weather_wr + hwc_hippo_weather_runs ---
                    if w_bracket:
                        hw_key = (hippo_key, w_bracket)
                        if hw_key in hs.hippo_weather:
                            hw_data = hs.hippo_weather[hw_key]
                            hw_total = hw_data[1]
                            hw_wins = hw_data[0]
                            features["hwc_hippo_weather_wr"] = round(hw_wins / hw_total, 4) if hw_total > 0 else None
                            features["hwc_hippo_weather_runs"] = hw_total
                            features["hwc_hippo_first_time_weather"] = 0
                            if features["hwc_hippo_weather_wr"] is not None:
                                fill_counts["hwc_hippo_weather_wr"] += 1
                            if hw_total > 0:
                                fill_counts["hwc_hippo_weather_runs"] += 1
                            fill_counts["hwc_hippo_first_time_weather"] += 1
                        else:
                            features["hwc_hippo_weather_wr"] = None
                            features["hwc_hippo_weather_runs"] = 0
                            features["hwc_hippo_first_time_weather"] = 1
                            fill_counts["hwc_hippo_weather_runs"] += 1
                            fill_counts["hwc_hippo_first_time_weather"] += 1

                        # --- 4. hwc_weather_specialist ---
                        cur_wins, cur_total = hs.weather_wr_for_bracket(w_bracket)
                        oth_wins, oth_total = hs.weather_wr_for_other_brackets(w_bracket)
                        if cur_total >= 3 and oth_total >= 3:
                            cur_wr = cur_wins / cur_total
                            oth_wr = oth_wins / oth_total
                            features["hwc_weather_specialist"] = 1 if (oth_wr > 0 and cur_wr / oth_wr > 2.0) else 0
                            fill_counts["hwc_weather_specialist"] += 1
                        else:
                            features["hwc_weather_specialist"] = None

                        # --- 6. hwc_global_weather_wr ---
                        gw = global_weather.get(w_bracket)
                        if gw and gw[1] > 0:
                            global_wr = round(gw[0] / gw[1], 4)
                            features["hwc_global_weather_wr"] = global_wr
                            fill_counts["hwc_global_weather_wr"] += 1

                            # --- 7. hwc_weather_advantage ---
                            horse_weather_wr = features.get("hwc_hippo_weather_wr")
                            if horse_weather_wr is not None:
                                features["hwc_weather_advantage"] = round(horse_weather_wr - global_wr, 4)
                                fill_counts["hwc_weather_advantage"] += 1
                            else:
                                # Use overall weather bracket wr for this horse
                                if cur_total > 0:
                                    hw_overall = round(cur_wins / cur_total, 4)
                                    features["hwc_weather_advantage"] = round(hw_overall - global_wr, 4)
                                    fill_counts["hwc_weather_advantage"] += 1
                                else:
                                    features["hwc_weather_advantage"] = None
                        else:
                            features["hwc_global_weather_wr"] = None
                            features["hwc_weather_advantage"] = None
                    else:
                        features["hwc_hippo_weather_wr"] = None
                        features["hwc_hippo_weather_runs"] = None
                        features["hwc_weather_specialist"] = None
                        features["hwc_hippo_first_time_weather"] = None
                        features["hwc_global_weather_wr"] = None
                        features["hwc_weather_advantage"] = None

                    # --- 3. hwc_hippo_surface_wr ---
                    if surface_key:
                        hs_key = (hippo_key, surface_key)
                        if hs_key in hs.hippo_surface:
                            hs_data = hs.hippo_surface[hs_key]
                            hs_total = hs_data[1]
                            hs_wins = hs_data[0]
                            features["hwc_hippo_surface_wr"] = round(hs_wins / hs_total, 4) if hs_total > 0 else None
                            if features["hwc_hippo_surface_wr"] is not None:
                                fill_counts["hwc_hippo_surface_wr"] += 1
                        else:
                            features["hwc_hippo_surface_wr"] = None
                    else:
                        features["hwc_hippo_surface_wr"] = None

                    # --- 8. hwc_hippo_season_combo_wr ---
                    if month is not None:
                        hm_key = (hippo_key, month)
                        if hm_key in hs.hippo_month:
                            hm_data = hs.hippo_month[hm_key]
                            hm_total = hm_data[1]
                            hm_wins = hm_data[0]
                            features["hwc_hippo_season_combo_wr"] = round(hm_wins / hm_total, 4) if hm_total > 0 else None
                            if features["hwc_hippo_season_combo_wr"] is not None:
                                fill_counts["hwc_hippo_season_combo_wr"] += 1
                        else:
                            features["hwc_hippo_season_combo_wr"] = None
                    else:
                        features["hwc_hippo_season_combo_wr"] = None

                elif horse_id and hippo_key:
                    # Horse exists but no prior history
                    features["hwc_hippo_weather_wr"] = None
                    features["hwc_hippo_weather_runs"] = 0
                    features["hwc_hippo_surface_wr"] = None
                    features["hwc_weather_specialist"] = None
                    features["hwc_hippo_first_time_weather"] = 1 if w_bracket else None
                    features["hwc_hippo_season_combo_wr"] = None

                    fill_counts["hwc_hippo_weather_runs"] += 1
                    if w_bracket:
                        fill_counts["hwc_hippo_first_time_weather"] += 1

                    # Global weather wr still available
                    if w_bracket:
                        gw = global_weather.get(w_bracket)
                        if gw and gw[1] > 0:
                            features["hwc_global_weather_wr"] = round(gw[0] / gw[1], 4)
                            fill_counts["hwc_global_weather_wr"] += 1
                            features["hwc_weather_advantage"] = None
                        else:
                            features["hwc_global_weather_wr"] = None
                            features["hwc_weather_advantage"] = None
                    else:
                        features["hwc_global_weather_wr"] = None
                        features["hwc_weather_advantage"] = None
                else:
                    # No horse_id or no hippo
                    features["hwc_hippo_weather_wr"] = None
                    features["hwc_hippo_weather_runs"] = None
                    features["hwc_hippo_surface_wr"] = None
                    features["hwc_weather_specialist"] = None
                    features["hwc_hippo_first_time_weather"] = None
                    features["hwc_global_weather_wr"] = None
                    features["hwc_weather_advantage"] = None
                    features["hwc_hippo_season_combo_wr"] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update state AFTER snapshot --
            for rec in course_records:
                horse_id = rec.get("horse_id")
                if not horse_id:
                    continue

                hippo = rec.get("hippodrome_normalise")
                hippo_key = str(hippo).strip().lower() if hippo else None
                if not hippo_key:
                    continue

                surface = rec.get("type_piste")
                surface_key = str(surface).strip().lower() if surface else None
                meteo_score = _safe_float(rec.get("met_impact_meteo_score"))
                w_bracket = _weather_bracket(meteo_score)
                is_win = bool(rec.get("is_gagnant"))

                if horse_id not in horse_state:
                    horse_state[horse_id] = _HorseState()
                hs = horse_state[horse_id]

                # Update hippo x weather
                if w_bracket:
                    hw = hs.hippo_weather[(hippo_key, w_bracket)]
                    hw[1] += 1
                    if is_win:
                        hw[0] += 1

                # Update hippo x surface
                if surface_key:
                    hsf = hs.hippo_surface[(hippo_key, surface_key)]
                    hsf[1] += 1
                    if is_win:
                        hsf[0] += 1

                # Update hippo x month
                if month is not None:
                    hm = hs.hippo_month[(hippo_key, month)]
                    hm[1] += 1
                    if is_win:
                        hm[0] += 1

                # Update global weather stats
                if w_bracket:
                    gw = global_weather[w_bracket]
                    gw[1] += 1
                    if is_win:
                        gw[0] += 1

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Hippodrome weather combo build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features hippodrome x weather combo"
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

    logger = setup_logging("hippodrome_weather_combo_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "hippodrome_weather_combo.jsonl"
    build_hippodrome_weather_combo_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
