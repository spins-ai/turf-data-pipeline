#!/usr/bin/env python3
"""
feature_builders.log_transform_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Log transforms, power transforms, and inverse transforms for key skewed features.

Simple single-pass streaming builder -- no chronological ordering needed.
Each transform is applied independently per record.

Features (15):
  - log_cote_finale            : log(cote_finale + 1)
  - log_cote_reference         : log(cote_reference + 1)
  - log_gains_carriere         : log(gains_carriere_euros + 1)
  - log_gains_annee            : log(gains_annee_euros + 1)
  - log_nb_courses             : log(nb_courses_carriere + 1)
  - log_distance               : log(distance)
  - log_nombre_partants        : log(nombre_partants)
  - log_reduction_km           : log(reduction_km_ms + 1)
  - log_temps_ms               : log(temps_ms + 1)
  - log_rapport_simple_gagnant : log(rap_rapport_simple_gagnant + 1)
  - sqrt_cote_finale           : sqrt(cote_finale)
  - sqrt_nb_courses            : sqrt(nb_courses_carriere)
  - inv_cote                   : 1 / (cote_finale + 0.01)
  - inv_nombre_partants        : 1 / nombre_partants
  - cube_root_gains            : gains_carriere_euros^(1/3)

Usage:
    python feature_builders/log_transform_builder.py
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/log_transforms")
_LOG_EVERY = 500_000


def _sf(val) -> Optional[float]:
    """Safe float conversion."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if f != f else f  # NaN check
    except (TypeError, ValueError):
        return None


def build(logger) -> None:
    t0 = time.time()
    logger.info("=== Log Transform Builder ===")
    logger.info("Input: %s", INPUT_PARTANTS)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "log_transforms_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    feat_names = [
        "log_cote_finale", "log_cote_reference", "log_gains_carriere",
        "log_gains_annee", "log_nb_courses", "log_distance",
        "log_nombre_partants", "log_reduction_km", "log_temps_ms",
        "log_rapport_simple_gagnant", "sqrt_cote_finale", "sqrt_nb_courses",
        "inv_cote", "inv_nombre_partants", "cube_root_gains",
    ]
    fill = {k: 0 for k in feat_names}
    n_written = 0

    with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_written += 1
            if n_written % _LOG_EVERY == 0:
                logger.info("  %d records...", n_written)
                gc.collect()

            # Extract raw values
            cote_finale = _sf(rec.get("cote_finale"))
            cote_reference = _sf(rec.get("cote_reference"))
            gains_carriere = _sf(rec.get("gains_carriere_euros"))
            gains_annee = _sf(rec.get("gains_annee_euros"))
            nb_courses = _sf(rec.get("nb_courses_carriere"))
            distance = _sf(rec.get("distance"))
            nombre_partants = _sf(rec.get("nombre_partants"))
            reduction_km = _sf(rec.get("reduction_km_ms"))
            temps_ms = _sf(rec.get("temps_ms"))
            rapport_sg = _sf(rec.get("rap_rapport_simple_gagnant"))

            out: dict = {
                "partant_uid": rec.get("partant_uid", ""),
                "course_uid": rec.get("course_uid", ""),
                "date_reunion_iso": rec.get("date_reunion_iso", ""),
            }

            # --- log transforms: log(x + 1) ---
            if cote_finale is not None and cote_finale >= 0:
                out["log_cote_finale"] = round(math.log(cote_finale + 1), 6)
                fill["log_cote_finale"] += 1
            else:
                out["log_cote_finale"] = None

            if cote_reference is not None and cote_reference >= 0:
                out["log_cote_reference"] = round(math.log(cote_reference + 1), 6)
                fill["log_cote_reference"] += 1
            else:
                out["log_cote_reference"] = None

            if gains_carriere is not None and gains_carriere >= 0:
                out["log_gains_carriere"] = round(math.log(gains_carriere + 1), 6)
                fill["log_gains_carriere"] += 1
            else:
                out["log_gains_carriere"] = None

            if gains_annee is not None and gains_annee >= 0:
                out["log_gains_annee"] = round(math.log(gains_annee + 1), 6)
                fill["log_gains_annee"] += 1
            else:
                out["log_gains_annee"] = None

            if nb_courses is not None and nb_courses >= 0:
                out["log_nb_courses"] = round(math.log(nb_courses + 1), 6)
                fill["log_nb_courses"] += 1
            else:
                out["log_nb_courses"] = None

            # log(distance) -- no +1, distance is always > 0
            if distance is not None and distance > 0:
                out["log_distance"] = round(math.log(distance), 6)
                fill["log_distance"] += 1
            else:
                out["log_distance"] = None

            # log(nombre_partants) -- no +1, always >= 1
            if nombre_partants is not None and nombre_partants > 0:
                out["log_nombre_partants"] = round(math.log(nombre_partants), 6)
                fill["log_nombre_partants"] += 1
            else:
                out["log_nombre_partants"] = None

            if reduction_km is not None and reduction_km >= 0:
                out["log_reduction_km"] = round(math.log(reduction_km + 1), 6)
                fill["log_reduction_km"] += 1
            else:
                out["log_reduction_km"] = None

            if temps_ms is not None and temps_ms >= 0:
                out["log_temps_ms"] = round(math.log(temps_ms + 1), 6)
                fill["log_temps_ms"] += 1
            else:
                out["log_temps_ms"] = None

            if rapport_sg is not None and rapport_sg >= 0:
                out["log_rapport_simple_gagnant"] = round(math.log(rapport_sg + 1), 6)
                fill["log_rapport_simple_gagnant"] += 1
            else:
                out["log_rapport_simple_gagnant"] = None

            # --- sqrt transforms ---
            if cote_finale is not None and cote_finale >= 0:
                out["sqrt_cote_finale"] = round(math.sqrt(cote_finale), 6)
                fill["sqrt_cote_finale"] += 1
            else:
                out["sqrt_cote_finale"] = None

            if nb_courses is not None and nb_courses >= 0:
                out["sqrt_nb_courses"] = round(math.sqrt(nb_courses), 6)
                fill["sqrt_nb_courses"] += 1
            else:
                out["sqrt_nb_courses"] = None

            # --- inverse transforms ---
            if cote_finale is not None:
                out["inv_cote"] = round(1.0 / (cote_finale + 0.01), 6)
                fill["inv_cote"] += 1
            else:
                out["inv_cote"] = None

            if nombre_partants is not None and nombre_partants > 0:
                out["inv_nombre_partants"] = round(1.0 / nombre_partants, 6)
                fill["inv_nombre_partants"] += 1
            else:
                out["inv_nombre_partants"] = None

            # --- cube root ---
            if gains_carriere is not None and gains_carriere >= 0:
                out["cube_root_gains"] = round(gains_carriere ** (1.0 / 3.0), 6)
                fill["cube_root_gains"] += 1
            else:
                out["cube_root_gains"] = None

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

    # Rename tmp to final
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d records en %.1fs", n_written, elapsed)
    logger.info("=== Fill rates ===")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %-30s: %7d / %d (%.1f%%)", k, v, n_written, pct)


def main():
    parser = argparse.ArgumentParser(description="Log/power transform features builder")
    parser.parse_args()
    logger = setup_logging("log_transform_builder")
    build(logger)


if __name__ == "__main__":
    main()
