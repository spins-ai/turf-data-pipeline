#!/usr/bin/env python3
"""
feature_builders.pedigree_interaction_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pedigree-based interaction features that combine breeding data with race
conditions: stamina x distance, speed x sprint, inbreeding x age, etc.

Temporal integrity: all pedigree fields are static attributes of the horse
known before the race -- no future leakage.

Produces:
  - pedigree_interaction_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pedigree_interaction/

Features per partant (10):
  - ped_int_stamina_x_distance   : ped_stamina_index * distance / 3000
  - ped_int_speed_x_short        : ped_speed_index * (1 if distance < 1800 else 0)
  - ped_int_stamina_x_heavy      : ped_stamina_index * (1 if type_piste contains "lourd" else 0)
  - ped_int_inbreeding_x_age     : ped_inbreeding_score * age
  - ped_int_country_match         : 1 if pays_cheval matches typical country for discipline
  - ped_int_breed_x_discipline    : 1 if breed matches discipline (TF->trot, PS->galop, AQPS->obstacle)
  - ped_int_sire_stamina_x_dist  : ped_sire_stamina_flag * distance / 3000
  - ped_int_dam_speed_x_sprint   : ped_dam_sire_speed_flag * (1 if distance < 1800 else 0)
  - ped_int_lineage_adapted       : ped_lignee_adaptee_distance as int
  - ped_int_genetic_advantage     : (ped_stamina_index + ped_speed_index) / 2

Usage:
    python feature_builders/pedigree_interaction_builder.py
    python feature_builders/pedigree_interaction_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pedigree_interaction")

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# Sprint threshold (metres)
_SPRINT_THRESHOLD = 1800

# Feature keys
_FEATURE_KEYS = [
    "ped_int_stamina_x_distance",
    "ped_int_speed_x_short",
    "ped_int_stamina_x_heavy",
    "ped_int_inbreeding_x_age",
    "ped_int_country_match",
    "ped_int_breed_x_discipline",
    "ped_int_sire_stamina_x_dist",
    "ped_int_dam_speed_x_sprint",
    "ped_int_lineage_adapted",
    "ped_int_genetic_advantage",
]

# Breed <-> discipline mappings
# TF (Trotteur Francais) -> trot, PS (Pur-Sang) -> galop plat, AQPS -> obstacle
_BREED_DISCIPLINE = {
    "TF": "trot",
    "PS": "plat",
    "AQPS": "obstacle",
}

# Country <-> discipline typical mappings
# France dominates trot; UK/Ireland for obstacle; France/UK for plat
_COUNTRY_DISCIPLINE = {
    "trot": {"FRA", "FR", "FRANCE"},
    "plat": {"FRA", "FR", "FRANCE", "GBR", "GB", "IRE", "IRL", "USA", "US"},
    "obstacle": {"FRA", "FR", "FRANCE", "GBR", "GB", "IRE", "IRL"},
}


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert value to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _guess_discipline(rec: dict) -> str:
    """Guess the discipline from the record fields.

    Returns one of: 'trot', 'plat', 'obstacle', or '' if unknown.
    """
    # Try explicit discipline field
    disc = str(rec.get("discipline", "") or "").lower().strip()
    if "trot" in disc:
        return "trot"
    if "plat" in disc:
        return "plat"
    if "obstacle" in disc or "haie" in disc or "steeple" in disc:
        return "obstacle"

    # Try specialite
    spec = str(rec.get("specialite", "") or "").lower().strip()
    if "trot" in spec:
        return "trot"
    if "plat" in spec:
        return "plat"
    if "obstacle" in spec or "haie" in spec or "steeple" in spec:
        return "obstacle"

    return ""


def _is_heavy_track(rec: dict) -> bool:
    """Check if the track condition is heavy (lourd/tres lourd)."""
    for field in ("type_piste", "etat_terrain", "penetrometre_valeur"):
        val = str(rec.get(field, "") or "").lower()
        if "lourd" in val:
            return True
    return False


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _compute_features(rec: dict) -> dict[str, Any]:
    """Compute all 10 pedigree interaction features from a single record."""
    feats: dict[str, Any] = {}

    # Input fields
    stamina = _safe_float(rec.get("ped_stamina_index"))
    speed = _safe_float(rec.get("ped_speed_index"))
    inbreeding = _safe_float(rec.get("ped_inbreeding_score"))
    distance = _safe_float(rec.get("distance"))
    age = _safe_int(rec.get("age"))
    sire_stamina = _safe_float(rec.get("ped_sire_stamina_flag"))
    dam_speed = _safe_float(rec.get("ped_dam_sire_speed_flag"))
    lignee = rec.get("ped_lignee_adaptee_distance")
    breed = str(rec.get("race_cheval", "") or rec.get("breed", "") or "").upper().strip()
    pays = str(rec.get("pays_cheval", "") or rec.get("pays", "") or "").upper().strip()
    discipline = _guess_discipline(rec)
    is_heavy = _is_heavy_track(rec)
    is_sprint = distance is not None and distance < _SPRINT_THRESHOLD

    # 1. ped_int_stamina_x_distance
    if stamina is not None and distance is not None:
        feats["ped_int_stamina_x_distance"] = round(stamina * distance / 3000, 6)
    else:
        feats["ped_int_stamina_x_distance"] = None

    # 2. ped_int_speed_x_short
    if speed is not None:
        feats["ped_int_speed_x_short"] = round(speed, 6) if is_sprint else 0.0
    else:
        feats["ped_int_speed_x_short"] = None

    # 3. ped_int_stamina_x_heavy
    if stamina is not None:
        feats["ped_int_stamina_x_heavy"] = round(stamina, 6) if is_heavy else 0.0
    else:
        feats["ped_int_stamina_x_heavy"] = None

    # 4. ped_int_inbreeding_x_age
    if inbreeding is not None and age is not None:
        feats["ped_int_inbreeding_x_age"] = round(inbreeding * age, 6)
    else:
        feats["ped_int_inbreeding_x_age"] = None

    # 5. ped_int_country_match
    if pays and discipline:
        allowed = _COUNTRY_DISCIPLINE.get(discipline, set())
        feats["ped_int_country_match"] = 1 if pays in allowed else 0
    else:
        feats["ped_int_country_match"] = None

    # 6. ped_int_breed_x_discipline
    if breed and discipline:
        expected_disc = _BREED_DISCIPLINE.get(breed, "")
        feats["ped_int_breed_x_discipline"] = 1 if expected_disc == discipline else 0
    else:
        feats["ped_int_breed_x_discipline"] = None

    # 7. ped_int_sire_stamina_x_dist
    if sire_stamina is not None and distance is not None:
        feats["ped_int_sire_stamina_x_dist"] = round(sire_stamina * distance / 3000, 6)
    else:
        feats["ped_int_sire_stamina_x_dist"] = None

    # 8. ped_int_dam_speed_x_sprint
    if dam_speed is not None:
        feats["ped_int_dam_speed_x_sprint"] = round(dam_speed, 6) if is_sprint else 0.0
    else:
        feats["ped_int_dam_speed_x_sprint"] = None

    # 9. ped_int_lineage_adapted
    if lignee is not None:
        feats["ped_int_lineage_adapted"] = _safe_int(lignee)
    else:
        feats["ped_int_lineage_adapted"] = None

    # 10. ped_int_genetic_advantage
    if stamina is not None and speed is not None:
        feats["ped_int_genetic_advantage"] = round((stamina + speed) / 2, 6)
    else:
        feats["ped_int_genetic_advantage"] = None

    return feats


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build(input_path: Path, logger):
    """Single-pass streaming build of pedigree interaction features."""
    logger.info("=== Pedigree Interaction Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "pedigree_interaction_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    n_read = 0
    fill = {k: 0 for k in _FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  %d records traites...", n_read)
            if n_read % _GC_EVERY == 0:
                gc.collect()

            feats = _compute_features(rec)

            out = {
                "partant_uid": rec.get("partant_uid", ""),
                "course_uid": rec.get("course_uid", ""),
                "date_reunion_iso": rec.get("date_reunion_iso", ""),
            }
            out.update(feats)

            # Track fill rates
            for k in _FEATURE_KEYS:
                if out.get(k) is not None:
                    fill[k] += 1

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

    # Atomic rename
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Termine: %d records en %.1fs",
        n_read, elapsed,
    )
    logger.info("Output: %s", output_path)

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill.items():
        pct = v / n_read * 100 if n_read > 0 else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_read, pct)


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
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
        description="Pedigree interaction features (breeding x race conditions)"
    )
    parser.add_argument("--input", type=str, default=None,
                        help="Path to partants_master.jsonl")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Override output directory")
    args = parser.parse_args()

    logger = setup_logging("pedigree_interaction_builder")

    input_path = _find_input(args.input)

    if args.output_dir:
        global OUTPUT_DIR
        OUTPUT_DIR = Path(args.output_dir)

    build(input_path, logger)


if __name__ == "__main__":
    main()
