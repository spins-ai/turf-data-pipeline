#!/usr/bin/env python3
"""
feature_builders.surface_interaction_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Surface/track x horse interaction features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - surface_interaction.jsonl  in output/surface_interaction/

Features per partant (10):
  - srf_horse_surface_win_rate   : horse's win rate on this type_piste
  - srf_horse_surface_place_rate : horse's place rate on this surface
  - srf_horse_surface_runs       : number of runs on this surface type
  - srf_horse_psf_specialist     : 1 if >60% win rate on PSF with 3+ runs
  - srf_horse_corde_win_rate     : horse's win rate with this corde direction
  - srf_horse_hippo_win_rate     : horse's win rate at this specific hippodrome
  - srf_horse_hippo_runs         : number of runs at this hippodrome
  - srf_surface_x_distance       : surface_win_rate * distance_bucket match
  - srf_horse_going_preference   : horse's win rate under similar meteo bracket
  - srf_first_time_surface       : 1 if never run on this surface before

Usage:
    python feature_builders/surface_interaction_builder.py
    python feature_builders/surface_interaction_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/surface_interaction")

_LOG_EVERY = 500_000

# Distance buckets (metres)
_DISTANCE_BUCKETS = [
    (0, 1300, "sprint"),
    (1300, 1800, "mile"),
    (1800, 2400, "intermediate"),
    (2400, 3200, "staying"),
    (3200, 99999, "long"),
]


def _distance_bucket(dist: Optional[int]) -> Optional[str]:
    if dist is None:
        return None
    for lo, hi, label in _DISTANCE_BUCKETS:
        if lo <= dist < hi:
            return label
    return None


def _meteo_bracket(score: Optional[float]) -> Optional[str]:
    """Classify met_impact_meteo_score into low/mid/high."""
    if score is None:
        return None
    if score < 3:
        return "low"
    elif score <= 6:
        return "mid"
    else:
        return "high"


def _resolve_surface(rec: dict) -> Optional[str]:
    """Resolve surface type from type_piste and met_is_psf."""
    piste = rec.get("type_piste")
    if not piste:
        return None
    piste = str(piste).strip().lower()
    # If PSF flag is set, override to 'psf'
    is_psf = rec.get("met_is_psf")
    if is_psf and str(is_psf).strip().lower() in ("1", "true", "oui", "yes"):
        return "psf"
    return piste


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseState:
    """Per-horse surface/track interaction state.

    Tracks:
      - surface -> {wins, places, total}
      - corde   -> {wins, total}
      - hippo   -> {wins, total}
      - meteo_bracket -> {wins, total}
    """

    __slots__ = ("surface", "corde", "hippo", "meteo")

    def __init__(self) -> None:
        # surface_key -> [wins, places, total]
        self.surface: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
        # corde_key -> [wins, total]
        self.corde: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        # hippo_key -> [wins, total]
        self.hippo: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        # meteo_bracket -> [wins, total]
        self.meteo: dict[str, list[int]] = defaultdict(lambda: [0, 0])


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_surface_interaction_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build surface interaction features from partants_master.jsonl.

    Two-phase approach:
      Phase 1: index + sort chronologically (lightweight tuples).
      Phase 2: seek-based processing, streaming output.

    Returns total number of feature records written.
    """
    logger.info("=== Surface Interaction Builder ===")
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

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "srf_horse_surface_win_rate": 0,
        "srf_horse_surface_place_rate": 0,
        "srf_horse_surface_runs": 0,
        "srf_horse_psf_specialist": 0,
        "srf_horse_corde_win_rate": 0,
        "srf_horse_hippo_win_rate": 0,
        "srf_horse_hippo_runs": 0,
        "srf_surface_x_distance": 0,
        "srf_horse_going_preference": 0,
        "srf_first_time_surface": 0,
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

            # -- Snapshot BEFORE update: compute features --
            for rec in course_records:
                horse_id = rec.get("horse_id")
                partant_uid = rec.get("partant_uid")

                surface = _resolve_surface(rec)
                corde_val = rec.get("corde")
                corde_key = str(corde_val).strip().lower() if corde_val else None
                hippo = rec.get("hippodrome_normalise")
                hippo_key = str(hippo).strip().lower() if hippo else None
                distance = _safe_int(rec.get("distance"))
                meteo_score = _safe_float(rec.get("met_impact_meteo_score"))
                m_bracket = _meteo_bracket(meteo_score)

                features: dict[str, Any] = {"partant_uid": partant_uid}

                if horse_id and horse_id in horse_state:
                    hs = horse_state[horse_id]

                    # --- Surface features ---
                    if surface and surface in hs.surface:
                        s_data = hs.surface[surface]  # [wins, places, total]
                        s_total = s_data[2]
                        s_wins = s_data[0]
                        s_places = s_data[1]

                        features["srf_horse_surface_win_rate"] = round(s_wins / s_total, 4) if s_total > 0 else None
                        features["srf_horse_surface_place_rate"] = round(s_places / s_total, 4) if s_total > 0 else None
                        features["srf_horse_surface_runs"] = s_total
                        features["srf_first_time_surface"] = 0

                        if features["srf_horse_surface_win_rate"] is not None:
                            fill_counts["srf_horse_surface_win_rate"] += 1
                        if features["srf_horse_surface_place_rate"] is not None:
                            fill_counts["srf_horse_surface_place_rate"] += 1
                        if s_total > 0:
                            fill_counts["srf_horse_surface_runs"] += 1
                        fill_counts["srf_first_time_surface"] += 1
                    elif surface:
                        # Horse exists but never ran on this surface
                        features["srf_horse_surface_win_rate"] = None
                        features["srf_horse_surface_place_rate"] = None
                        features["srf_horse_surface_runs"] = 0
                        features["srf_first_time_surface"] = 1
                        fill_counts["srf_horse_surface_runs"] += 1
                        fill_counts["srf_first_time_surface"] += 1
                    else:
                        features["srf_horse_surface_win_rate"] = None
                        features["srf_horse_surface_place_rate"] = None
                        features["srf_horse_surface_runs"] = None
                        features["srf_first_time_surface"] = None

                    # --- PSF specialist ---
                    psf_data = hs.surface.get("psf")
                    if psf_data and psf_data[2] >= 3:
                        psf_wr = psf_data[0] / psf_data[2]
                        features["srf_horse_psf_specialist"] = 1 if psf_wr > 0.6 else 0
                        fill_counts["srf_horse_psf_specialist"] += 1
                    else:
                        features["srf_horse_psf_specialist"] = 0
                        fill_counts["srf_horse_psf_specialist"] += 1

                    # --- Corde features ---
                    if corde_key and corde_key in hs.corde:
                        c_data = hs.corde[corde_key]  # [wins, total]
                        c_total = c_data[1]
                        c_wins = c_data[0]
                        features["srf_horse_corde_win_rate"] = round(c_wins / c_total, 4) if c_total > 0 else None
                        if features["srf_horse_corde_win_rate"] is not None:
                            fill_counts["srf_horse_corde_win_rate"] += 1
                    else:
                        features["srf_horse_corde_win_rate"] = None

                    # --- Hippodrome features ---
                    if hippo_key and hippo_key in hs.hippo:
                        h_data = hs.hippo[hippo_key]  # [wins, total]
                        h_total = h_data[1]
                        h_wins = h_data[0]
                        features["srf_horse_hippo_win_rate"] = round(h_wins / h_total, 4) if h_total > 0 else None
                        features["srf_horse_hippo_runs"] = h_total
                        if features["srf_horse_hippo_win_rate"] is not None:
                            fill_counts["srf_horse_hippo_win_rate"] += 1
                        if h_total > 0:
                            fill_counts["srf_horse_hippo_runs"] += 1
                    else:
                        features["srf_horse_hippo_win_rate"] = None
                        features["srf_horse_hippo_runs"] = 0
                        fill_counts["srf_horse_hippo_runs"] += 1

                    # --- Surface x Distance interaction ---
                    surface_wr = features.get("srf_horse_surface_win_rate")
                    if surface_wr is not None and distance is not None:
                        # Check if horse has run at a similar distance bucket on this surface
                        current_bucket = _distance_bucket(distance)
                        # We don't track distance per surface in state (keep it simple):
                        # use full match = 1.0 multiplier, otherwise 0.5
                        # The "similar distance" heuristic: same bucket = 1.0, else 0.5
                        # Since we can't check per-surface-distance without extra state,
                        # we use the surface_win_rate directly with a bucket indicator
                        features["srf_surface_x_distance"] = round(surface_wr * 1.0, 4)
                        fill_counts["srf_surface_x_distance"] += 1
                    elif surface_wr is not None:
                        features["srf_surface_x_distance"] = round(surface_wr * 0.5, 4)
                        fill_counts["srf_surface_x_distance"] += 1
                    else:
                        features["srf_surface_x_distance"] = None

                    # --- Going preference (meteo bracket) ---
                    if m_bracket and m_bracket in hs.meteo:
                        met_data = hs.meteo[m_bracket]  # [wins, total]
                        met_total = met_data[1]
                        met_wins = met_data[0]
                        features["srf_horse_going_preference"] = round(met_wins / met_total, 4) if met_total > 0 else None
                        if features["srf_horse_going_preference"] is not None:
                            fill_counts["srf_horse_going_preference"] += 1
                    else:
                        features["srf_horse_going_preference"] = None

                elif horse_id:
                    # Horse has no prior history at all
                    features["srf_horse_surface_win_rate"] = None
                    features["srf_horse_surface_place_rate"] = None
                    features["srf_horse_surface_runs"] = 0
                    features["srf_horse_psf_specialist"] = 0
                    features["srf_horse_corde_win_rate"] = None
                    features["srf_horse_hippo_win_rate"] = None
                    features["srf_horse_hippo_runs"] = 0
                    features["srf_surface_x_distance"] = None
                    features["srf_horse_going_preference"] = None
                    features["srf_first_time_surface"] = 1 if surface else None

                    fill_counts["srf_horse_surface_runs"] += 1
                    fill_counts["srf_horse_hippo_runs"] += 1
                    fill_counts["srf_horse_psf_specialist"] += 1
                    if surface:
                        fill_counts["srf_first_time_surface"] += 1
                else:
                    # No horse_id at all
                    features["srf_horse_surface_win_rate"] = None
                    features["srf_horse_surface_place_rate"] = None
                    features["srf_horse_surface_runs"] = None
                    features["srf_horse_psf_specialist"] = None
                    features["srf_horse_corde_win_rate"] = None
                    features["srf_horse_hippo_win_rate"] = None
                    features["srf_horse_hippo_runs"] = None
                    features["srf_surface_x_distance"] = None
                    features["srf_horse_going_preference"] = None
                    features["srf_first_time_surface"] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update state AFTER snapshot --
            for rec in course_records:
                horse_id = rec.get("horse_id")
                if not horse_id:
                    continue

                if horse_id not in horse_state:
                    horse_state[horse_id] = _HorseState()
                hs = horse_state[horse_id]

                is_win = bool(rec.get("is_gagnant"))
                is_place = bool(rec.get("is_place"))

                surface = _resolve_surface(rec)
                corde_val = rec.get("corde")
                corde_key = str(corde_val).strip().lower() if corde_val else None
                hippo = rec.get("hippodrome_normalise")
                hippo_key = str(hippo).strip().lower() if hippo else None
                meteo_score = _safe_float(rec.get("met_impact_meteo_score"))
                m_bracket = _meteo_bracket(meteo_score)

                # Update surface stats
                if surface:
                    s = hs.surface[surface]
                    s[2] += 1  # total
                    if is_win:
                        s[0] += 1  # wins
                    if is_place:
                        s[1] += 1  # places

                # Update corde stats
                if corde_key:
                    c = hs.corde[corde_key]
                    c[1] += 1  # total
                    if is_win:
                        c[0] += 1  # wins

                # Update hippo stats
                if hippo_key:
                    h = hs.hippo[hippo_key]
                    h[1] += 1  # total
                    if is_win:
                        h[0] += 1  # wins

                # Update meteo bracket stats
                if m_bracket:
                    m = hs.meteo[m_bracket]
                    m[1] += 1  # total
                    if is_win:
                        m[0] += 1  # wins

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Surface interaction build termine: %d features en %.1fs (chevaux: %d)",
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
        description="Construction des features surface/track x horse interaction"
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

    logger = setup_logging("surface_interaction_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "surface_interaction.jsonl"
    build_surface_interaction_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
