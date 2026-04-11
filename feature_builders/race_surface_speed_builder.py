#!/usr/bin/env python3
"""
feature_builders.race_surface_speed_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Race surface x speed interaction features.

Reads partants_master.jsonl in streaming mode, builds a lightweight
byte-offset index, sorts chronologically, then seeks back to produce
features -- keeping RAM usage proportional to the index, not the data.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - race_surface_speed.jsonl  in output/race_surface_speed/

Features per partant (8):
  - rss_horse_speed_on_surface      : average speed (reduction_km_ms) on current surface type
  - rss_horse_speed_off_surface     : average speed on other surfaces
  - rss_surface_speed_advantage     : speed_on - speed_off (positive = faster on this surface)
  - rss_horse_speed_on_turf         : average speed on herbe/turf
  - rss_horse_speed_on_aw           : average speed on all-weather (psf/sable)
  - rss_turf_aw_preference          : turf_speed - aw_speed (lower reduction = faster,
                                      so negative = prefers turf)
  - rss_speed_surface_combo_runs    : number of timed races on this surface
  - rss_is_surface_speed_proven     : 1 if horse has 3+ timed runs on this surface
                                      with above-average speed

Usage:
    python feature_builders/race_surface_speed_builder.py
    python feature_builders/race_surface_speed_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_surface_speed")

_LOG_EVERY = 500_000

# Surface classification
_TURF_KEYWORDS = {"herbe", "turf", "gazon"}
_AW_KEYWORDS = {"psf", "sable", "polytrack", "fibresand", "all-weather", "aw", "cendrée"}


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _classify_surface(rec: dict) -> Optional[str]:
    """Classify the race surface into 'turf', 'aw', or a normalised string.

    Uses type_piste, discipline, and libelle_hippo fields to determine
    the surface type.  Returns a lowercased surface key.
    """
    raw = (rec.get("type_piste") or rec.get("piste") or "").strip().lower()
    discipline = (rec.get("discipline") or "").strip().lower()

    # Try direct match
    if raw in _TURF_KEYWORDS or discipline in _TURF_KEYWORDS:
        return "turf"
    if raw in _AW_KEYWORDS or discipline in _AW_KEYWORDS:
        return "aw"

    # Partial match
    for kw in _TURF_KEYWORDS:
        if kw in raw or kw in discipline:
            return "turf"
    for kw in _AW_KEYWORDS:
        if kw in raw or kw in discipline:
            return "aw"

    # If discipline is plat/obstacle/haies it's typically turf in France
    if discipline in ("plat", "obstacle", "haies", "steeple-chase", "cross-country"):
        return "turf"

    # Return normalised raw if non-empty, else None
    return raw if raw else None


def _is_turf(surface: Optional[str]) -> bool:
    return surface == "turf"


def _is_aw(surface: Optional[str]) -> bool:
    return surface == "aw"


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseSurfaceSpeedState:
    """Per-horse accumulated state for surface x speed features.

    State:
      surface_speeds : {surface -> [speed_sum, count]}
      turf_speeds    : [sum, count]
      aw_speeds      : [sum, count]
      global_speed   : [sum, count]
    """

    __slots__ = ("surface_speeds", "turf_speeds", "aw_speeds", "global_speed")

    def __init__(self) -> None:
        self.surface_speeds: dict[str, list[float]] = defaultdict(lambda: [0.0, 0])
        self.turf_speeds: list[float] = [0.0, 0]
        self.aw_speeds: list[float] = [0.0, 0]
        self.global_speed: list[float] = [0.0, 0]

    def snapshot(self, surface: Optional[str]) -> dict[str, Any]:
        """Compute features using only past data (strict temporal)."""
        feats: dict[str, Any] = {
            "rss_horse_speed_on_surface": None,
            "rss_horse_speed_off_surface": None,
            "rss_surface_speed_advantage": None,
            "rss_horse_speed_on_turf": None,
            "rss_horse_speed_on_aw": None,
            "rss_turf_aw_preference": None,
            "rss_speed_surface_combo_runs": None,
            "rss_is_surface_speed_proven": None,
        }

        # -- Speed on current surface --
        speed_on = None
        combo_runs = 0
        if surface is not None:
            ss = self.surface_speeds.get(surface)
            if ss and ss[1] > 0:
                speed_on = ss[0] / ss[1]
                feats["rss_horse_speed_on_surface"] = round(speed_on, 4)
            combo_runs = ss[1] if ss else 0
            feats["rss_speed_surface_combo_runs"] = combo_runs

        # -- Speed off current surface --
        speed_off = None
        if surface is not None:
            off_sum = 0.0
            off_count = 0
            for s, (s_sum, s_cnt) in self.surface_speeds.items():
                if s != surface and s_cnt > 0:
                    off_sum += s_sum
                    off_count += s_cnt
            if off_count > 0:
                speed_off = off_sum / off_count
                feats["rss_horse_speed_off_surface"] = round(speed_off, 4)

        # -- Surface speed advantage --
        if speed_on is not None and speed_off is not None:
            feats["rss_surface_speed_advantage"] = round(speed_on - speed_off, 4)

        # -- Speed on turf --
        turf_speed = None
        if self.turf_speeds[1] > 0:
            turf_speed = self.turf_speeds[0] / self.turf_speeds[1]
            feats["rss_horse_speed_on_turf"] = round(turf_speed, 4)

        # -- Speed on AW --
        aw_speed = None
        if self.aw_speeds[1] > 0:
            aw_speed = self.aw_speeds[0] / self.aw_speeds[1]
            feats["rss_horse_speed_on_aw"] = round(aw_speed, 4)

        # -- Turf/AW preference --
        if turf_speed is not None and aw_speed is not None:
            feats["rss_turf_aw_preference"] = round(turf_speed - aw_speed, 4)

        # -- Is surface speed proven --
        # 1 if horse has 3+ timed runs on this surface with above-average speed
        if surface is not None and combo_runs >= 3 and speed_on is not None:
            global_avg = (self.global_speed[0] / self.global_speed[1]) if self.global_speed[1] > 0 else None
            if global_avg is not None:
                # Lower reduction = faster, so "above average speed" means
                # reduction <= global average (i.e. speed_on <= global_avg)
                feats["rss_is_surface_speed_proven"] = 1 if speed_on <= global_avg else 0
            else:
                feats["rss_is_surface_speed_proven"] = 0
        elif surface is not None:
            feats["rss_is_surface_speed_proven"] = 0

        return feats

    def update(self, surface: Optional[str], speed: Optional[float]) -> None:
        """Update state with a new race result (post-race)."""
        if speed is None or surface is None:
            return

        self.surface_speeds[surface][0] += speed
        self.surface_speeds[surface][1] += 1

        self.global_speed[0] += speed
        self.global_speed[1] += 1

        if _is_turf(surface):
            self.turf_speeds[0] += speed
            self.turf_speeds[1] += 1
        elif _is_aw(surface):
            self.aw_speeds[0] += speed
            self.aw_speeds[1] += 1


# ===========================================================================
# MAIN BUILD (index + sort + seek-based)
# ===========================================================================


def build_race_surface_speed_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build race surface x speed interaction features.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Seek back to disk to read full records, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Race Surface Speed Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date, course, num, offset) --
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
    horse_states: dict[str, _HorseSurfaceSpeedState] = defaultdict(_HorseSurfaceSpeedState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "rss_horse_speed_on_surface": 0,
        "rss_horse_speed_off_surface": 0,
        "rss_surface_speed_advantage": 0,
        "rss_horse_speed_on_turf": 0,
        "rss_horse_speed_on_aw": 0,
        "rss_turf_aw_preference": 0,
        "rss_speed_surface_combo_runs": 0,
        "rss_is_surface_speed_proven": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            surface = _classify_surface(rec)
            speed = _safe_float(rec.get("reduction_km_ms"))

            return {
                "uid": rec.get("partant_uid"),
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "surface": surface,
                "speed": speed,
                "is_gagnant": bool(rec.get("is_gagnant")),
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
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            # -- Snapshot pre-race features (BEFORE update) --
            for rec in course_group:
                horse_id = rec["horse_id"]
                surface = rec["surface"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if horse_id:
                    state = horse_states[horse_id]
                    snap = state.snapshot(surface)
                    features.update(snap)

                    # Update fill counts
                    for k in fill_counts:
                        if snap.get(k) is not None:
                            fill_counts[k] += 1
                else:
                    features.update({
                        "rss_horse_speed_on_surface": None,
                        "rss_horse_speed_off_surface": None,
                        "rss_surface_speed_advantage": None,
                        "rss_horse_speed_on_turf": None,
                        "rss_horse_speed_on_aw": None,
                        "rss_turf_aw_preference": None,
                        "rss_speed_surface_combo_runs": None,
                        "rss_is_surface_speed_proven": None,
                    })

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after race (post-race, no leakage) --
            for rec in course_group:
                horse_id = rec["horse_id"]
                if horse_id:
                    horse_states[horse_id].update(rec["surface"], rec["speed"])

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Race surface speed build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written if n_written else 0)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features race surface x speed a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("race_surface_speed_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "race_surface_speed.jsonl"
    build_race_surface_speed_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
