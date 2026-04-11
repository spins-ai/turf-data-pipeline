#!/usr/bin/env python3
"""
feature_builders.meteo_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Advanced weather/terrain features derived from partants_master fields.

Since meteo_historique.jsonl lacks proper join keys, this builder computes
terrain/surface features purely from existing partants_master columns:
type_piste, met_impact_meteo_score, met_is_psf, corde, distance.

Requires chronological processing for horse state tracking (terrain_change,
psf_advantage).

Features (8):
  - met_adv_terrain_class       : type_piste encoded as numeric
  - met_adv_meteo_impact_bin    : met_impact_meteo_score bucketed
  - met_adv_is_heavy_ground     : 1 if type_piste contains lourd/heavy
  - met_adv_corde_numeric       : corde encoded (gauche=0, droite=1)
  - met_adv_terrain_x_distance  : terrain_class * distance interaction
  - met_adv_psf_advantage       : 1 if PSF track and horse has PSF history
  - met_adv_terrain_change      : 1 if terrain differs from horse's last race
  - met_adv_distance_category   : distance bucket (sprint/mile/middle/staying)

Memory: ~2 GB max (horse history = dict of last terrain + psf flag)
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/meteo_advanced")
_LOG_EVERY = 500_000

# --- Terrain classification ---
_TERRAIN_MAP = {
    "cendree": 0, "cendrée": 0,
    "sable": 1, "sable fibre": 1, "sable fibré": 1,
    "herbe": 2, "gazon": 2, "turf": 2,
    "psf": 3, "polytrack": 3,
    "terre": 4,
    "machefer": 5,
}

# --- Corde mapping ---
_CORDE_MAP = {
    "gauche": 0, "g": 0, "left": 0,
    "droite": 1, "d": 1, "right": 1,
}


def _classify_terrain(type_piste: Optional[str]) -> Optional[int]:
    """Map type_piste string to numeric class."""
    if not type_piste:
        return None
    key = str(type_piste).strip().lower()
    # Direct match
    if key in _TERRAIN_MAP:
        return _TERRAIN_MAP[key]
    # Partial match
    for token, val in _TERRAIN_MAP.items():
        if token in key:
            return val
    return None


def _is_heavy(type_piste: Optional[str]) -> Optional[int]:
    """1 if terrain indicates heavy/lourd ground."""
    if not type_piste:
        return None
    key = str(type_piste).strip().lower()
    if "lourd" in key or "heavy" in key or "très souple" in key or "collant" in key:
        return 1
    return 0


def _classify_corde(corde: Optional[str]) -> Optional[int]:
    if not corde:
        return None
    key = str(corde).strip().lower()
    if key in _CORDE_MAP:
        return _CORDE_MAP[key]
    for token, val in _CORDE_MAP.items():
        if token in key:
            return val
    return None


def _meteo_impact_bin(score: Optional[float]) -> Optional[int]:
    """Bucket met_impact_meteo_score into categories.
    0 = neutral (0-0.2), 1 = slight (0.2-0.5), 2 = moderate (0.5-0.8), 3 = strong (>0.8)
    Negative scores mirror: -1 = slight neg, -2 = moderate neg, -3 = strong neg
    """
    if score is None:
        return None
    try:
        s = float(score)
    except (ValueError, TypeError):
        return None
    abs_s = abs(s)
    if abs_s <= 0.2:
        return 0
    sign = 1 if s > 0 else -1
    if abs_s <= 0.5:
        return 1 * sign
    if abs_s <= 0.8:
        return 2 * sign
    return 3 * sign


def _distance_category(dist: Optional[float]) -> Optional[int]:
    """0=sprint (<1600), 1=mile (1600-2000), 2=middle (2000-2800), 3=staying (>2800)."""
    if dist is None:
        return None
    try:
        d = float(dist)
    except (ValueError, TypeError):
        return None
    if d < 1600:
        return 0
    if d <= 2000:
        return 1
    if d <= 2800:
        return 2
    return 3


class _HorseTerrainState:
    __slots__ = ("last_terrain_class", "has_run_psf")

    def __init__(self):
        self.last_terrain_class: Optional[int] = None
        self.has_run_psf: bool = False


def build(input_path: Path, output_dir: Path, logger) -> None:
    t0 = time.time()
    logger.info("Phase 1: Indexation de %s", input_path)

    # Phase 1: Index with byte offsets
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
            num_pmu = 0
            try:
                num_pmu = int(rec.get("num_pmu", 0) or 0)
            except (ValueError, TypeError):
                pass
            index.append((date_str, course_uid, num_pmu, offset))

    logger.info("Phase 1: %d records indexes en %.1fs", len(index), time.time() - t0)

    # Phase 2: Sort chronologically
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # Phase 3: Process chronologically with horse state tracking
    t2 = time.time()
    horse_state: dict[str, _HorseTerrainState] = defaultdict(_HorseTerrainState)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "meteo_advanced_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    feature_names = [
        "met_adv_terrain_class", "met_adv_meteo_impact_bin",
        "met_adv_is_heavy_ground", "met_adv_corde_numeric",
        "met_adv_terrain_x_distance", "met_adv_psf_advantage",
        "met_adv_terrain_change", "met_adv_distance_category",
    ]
    fill = {k: 0 for k in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(off: int) -> dict:
            fin.seek(off)
            return json.loads(fin.readline())

        for date_str, course_uid, num_pmu, offset in index:
            rec = _read_at(offset)
            n_processed += 1

            if n_processed % _LOG_EVERY == 0:
                elapsed = time.time() - t2
                pct = n_processed / total * 100
                logger.info("  Phase 3: %d/%d (%.1f%%) en %.0fs", n_processed, total, pct, elapsed)
                gc.collect()

            partant_uid = rec.get("partant_uid", "")
            if not partant_uid:
                continue

            horse_id = rec.get("horse_id") or rec.get("nom_cheval", "")
            if not horse_id:
                continue

            type_piste = rec.get("type_piste")
            met_score = rec.get("met_impact_meteo_score")
            is_psf = rec.get("met_is_psf")
            corde = rec.get("corde")
            distance = rec.get("distance")

            # Parse distance as float
            dist_val = None
            if distance is not None:
                try:
                    dist_val = float(distance)
                except (ValueError, TypeError):
                    pass

            # Parse is_psf as bool
            psf_flag = False
            if is_psf is not None:
                try:
                    psf_flag = bool(int(is_psf))
                except (ValueError, TypeError):
                    if isinstance(is_psf, bool):
                        psf_flag = is_psf

            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }

            # --- Feature 1: terrain class ---
            terrain_cls = _classify_terrain(type_piste)
            out["met_adv_terrain_class"] = terrain_cls
            if terrain_cls is not None:
                fill["met_adv_terrain_class"] += 1

            # --- Feature 2: meteo impact bin ---
            impact_bin = _meteo_impact_bin(met_score)
            out["met_adv_meteo_impact_bin"] = impact_bin
            if impact_bin is not None:
                fill["met_adv_meteo_impact_bin"] += 1

            # --- Feature 3: heavy ground ---
            heavy = _is_heavy(type_piste)
            out["met_adv_is_heavy_ground"] = heavy
            if heavy is not None:
                fill["met_adv_is_heavy_ground"] += 1

            # --- Feature 4: corde numeric ---
            corde_num = _classify_corde(corde)
            out["met_adv_corde_numeric"] = corde_num
            if corde_num is not None:
                fill["met_adv_corde_numeric"] += 1

            # --- Feature 5: terrain x distance interaction ---
            if terrain_cls is not None and dist_val is not None:
                out["met_adv_terrain_x_distance"] = round(terrain_cls * dist_val, 1)
                fill["met_adv_terrain_x_distance"] += 1
            else:
                out["met_adv_terrain_x_distance"] = None

            # --- Feature 6: PSF advantage (horse state) ---
            hs = horse_state[horse_id]
            if psf_flag and hs.has_run_psf:
                out["met_adv_psf_advantage"] = 1
                fill["met_adv_psf_advantage"] += 1
            elif psf_flag and not hs.has_run_psf:
                out["met_adv_psf_advantage"] = 0
                fill["met_adv_psf_advantage"] += 1
            else:
                out["met_adv_psf_advantage"] = 0
                fill["met_adv_psf_advantage"] += 1

            # --- Feature 7: terrain change (horse state) ---
            if hs.last_terrain_class is not None and terrain_cls is not None:
                out["met_adv_terrain_change"] = 1 if terrain_cls != hs.last_terrain_class else 0
                fill["met_adv_terrain_change"] += 1
            elif terrain_cls is not None:
                # First race for horse, no change
                out["met_adv_terrain_change"] = 0
                fill["met_adv_terrain_change"] += 1
            else:
                out["met_adv_terrain_change"] = None

            # --- Feature 8: distance category ---
            dist_cat = _distance_category(dist_val)
            out["met_adv_distance_category"] = dist_cat
            if dist_cat is not None:
                fill["met_adv_distance_category"] += 1

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_written += 1

            # --- UPDATE horse state (after snapshot) ---
            if terrain_cls is not None:
                hs.last_terrain_class = terrain_cls
            if psf_flag:
                hs.has_run_psf = True

    # Rename tmp to final
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d features ecrites en %.1fs", n_written, elapsed)
    logger.info("Fill rates:")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %s: %.1f%%", k, pct)


def main():
    parser = argparse.ArgumentParser(description="Meteo advanced feature builder")
    parser.add_argument("--input", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("meteo_advanced_builder")

    if args.input:
        input_path = Path(args.input)
    else:
        input_path = None
        for p in INPUT_CANDIDATES:
            if p.exists():
                input_path = p
                break
        if input_path is None:
            logger.error("Aucun fichier partants_master trouve")
            sys.exit(1)

    logger.info("Input: %s", input_path)
    build(input_path, OUTPUT_DIR, logger)


if __name__ == "__main__":
    main()
