#!/usr/bin/env python3
"""
feature_builders.interaction_categorical_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Categorical interaction features -- encoding combinations of categorical
variables as single features for tree-based and embedding models.

All features are static (computed from the record itself, no temporal
tracking needed).  Pure categorical encoding from available columns.

Reads partants_master.jsonl in single-pass streaming mode.

Produces:
  - interaction_categorical.jsonl  in builder_outputs/interaction_categorical/

Features per partant (10):
  - ict_discipline_x_surface       : discipline * 10 + surface_encoded
  - ict_sex_x_age_group            : sex_code * 10 + age_bucket (0-4)
  - ict_discipline_x_distance_cat  : discipline_code * 4 + distance_category
  - ict_corde_x_draw_zone          : corde_encoded * 3 + draw_zone
  - ict_hippo_x_discipline         : hash(hippodrome) % 50 * 3 + discipline_code
  - ict_age_x_experience           : age_bucket * 4 + experience_level
  - ict_sex_x_discipline           : sex_code * 3 + discipline_code
  - ict_surface_x_weather          : surface_code * 3 + weather_bracket
  - ict_distance_x_weight          : distance_category * 4 + weight_category
  - ict_month_x_hippo_type         : month * 3 + hippo_size

Usage:
    python feature_builders/interaction_categorical_builder.py
    python feature_builders/interaction_categorical_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/interaction_categorical")

# Progress / GC frequency
_LOG_EVERY = 500_000

# Feature names (for fill-rate tracking)
_FEATURE_NAMES = [
    "ict_discipline_x_surface",
    "ict_sex_x_age_group",
    "ict_discipline_x_distance_cat",
    "ict_corde_x_draw_zone",
    "ict_hippo_x_discipline",
    "ict_age_x_experience",
    "ict_sex_x_discipline",
    "ict_surface_x_weather",
    "ict_distance_x_weight",
    "ict_month_x_hippo_type",
]

# ---------------------------------------------------------------------------
# Static dictionaries
# ---------------------------------------------------------------------------

# Hippodrome size classification (small/medium/large)
# Based on typical French racecourse sizes
_HIPPO_SIZE: dict[str, int] = {
    # Large hippodromes (0)
    "longchamp": 0, "chantilly": 0, "saint-cloud": 0, "deauville": 0,
    "auteuil": 0, "vincennes": 0, "enghien": 0, "maisons-laffitte": 0,
    "parislongchamp": 0, "paris-longchamp": 0,
    # Medium hippodromes (1)
    "lyon-parilly": 1, "lyon parilly": 1, "marseille-borely": 1,
    "toulouse": 1, "bordeaux": 1, "nantes": 1, "strasbourg": 1,
    "compiegne": 1, "fontainebleau": 1, "cagnes-sur-mer": 1,
    "cabourg": 1, "clairefontaine": 1, "vichy": 1, "le lion-d'angers": 1,
    "la teste-de-buch": 1, "la teste de buch": 1, "mont-de-marsan": 1,
    "angers": 1, "le mans": 1, "pau": 1, "craon": 1,
}
# Default = small (2) for any hippodrome not in the dict


# ===========================================================================
# HELPERS
# ===========================================================================


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
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _discipline_code(discipline: Optional[str]) -> Optional[int]:
    """Encode discipline: Plat=0, Obstacle=1, Trot=2."""
    if not discipline:
        return None
    d = discipline.strip().upper()
    if "PLAT" in d:
        return 0
    if "OBSTACLE" in d or "HAIE" in d or "STEEPLE" in d:
        return 1
    if "TROT" in d or "ATTELE" in d or "MONTE" in d:
        return 2
    return None


def _surface_code(type_piste: Optional[str], is_psf: Any) -> Optional[int]:
    """Encode surface: herbe=0, sable=1, psf=2, fibresable=3."""
    if is_psf:
        return 2
    if not type_piste:
        return None
    tp = type_piste.strip().lower()
    if "herbe" in tp or "gazon" in tp:
        return 0
    if "sable" in tp:
        return 1
    if "psf" in tp or "polytrack" in tp:
        return 2
    if "fibre" in tp:
        return 3
    return None


def _sex_code(sexe: Optional[str]) -> Optional[int]:
    """Encode sex: male=0, femelle=1, hongre=2."""
    if not sexe:
        return None
    s = sexe.strip().upper()
    if s in ("M", "MALE", "MALES"):
        return 0
    if s in ("F", "FEMELLE", "FEMELLES"):
        return 1
    if s in ("H", "HONGRE", "HONGRES"):
        return 2
    return None


def _age_bucket(age: Optional[int]) -> Optional[int]:
    """Encode age: 2=0, 3=1, 4=2, 5-6=3, 7+=4."""
    if age is None or age < 2:
        return None
    if age == 2:
        return 0
    if age == 3:
        return 1
    if age == 4:
        return 2
    if age <= 6:
        return 3
    return 4


def _distance_category(distance: Optional[float]) -> Optional[int]:
    """Encode distance: sprint(<1400)=0, mile(1400-1799)=1, mid(1800-2399)=2, route(2400+)=3."""
    if distance is None or distance <= 0:
        return None
    if distance < 1400:
        return 0
    if distance < 1800:
        return 1
    if distance < 2400:
        return 2
    return 3


def _draw_zone(corde: Optional[int], nb_partants: Optional[int]) -> Optional[int]:
    """Encode draw zone: inner=0, mid=1, outer=2."""
    if corde is None or nb_partants is None or nb_partants <= 0:
        return None
    ratio = corde / nb_partants
    if ratio <= 0.33:
        return 0  # inner
    if ratio <= 0.66:
        return 1  # mid
    return 2  # outer


def _experience_level(nb_courses: Optional[int]) -> Optional[int]:
    """Encode experience: debutant(0)=0, novice(1-9)=1, experienced(10-29)=2, veteran(30+)=3."""
    if nb_courses is None:
        return None
    if nb_courses == 0:
        return 0
    if nb_courses < 10:
        return 1
    if nb_courses < 30:
        return 2
    return 3


def _weather_bracket(met_impact: Optional[float]) -> Optional[int]:
    """Encode weather impact: low(<0.3)=0, mid(0.3-0.6)=1, high(>0.6)=2."""
    if met_impact is None:
        return None
    if met_impact < 0.3:
        return 0
    if met_impact <= 0.6:
        return 1
    return 2


def _weight_category(poids_kg: Optional[float]) -> Optional[int]:
    """Encode weight: light(<54)=0, normal(54-58)=1, heavy(58-62)=2, top(62+)=3."""
    if poids_kg is None or poids_kg <= 0:
        return None
    if poids_kg < 54:
        return 0
    if poids_kg < 58:
        return 1
    if poids_kg < 62:
        return 2
    return 3


def _hippo_size(hippo: Optional[str]) -> int:
    """Return hippodrome size: 0=large, 1=medium, 2=small (default)."""
    if not hippo:
        return 2
    return _HIPPO_SIZE.get(hippo.strip().lower(), 2)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_interaction_categorical(input_path: Path, output_path: Path, logger) -> int:
    """Build categorical interaction features from partants_master.jsonl."""
    logger.info("=== Interaction Categorical Builder ===")
    logger.info("Input : %s", input_path)
    logger.info("Output: %s", output_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_read = 0
    n_written = 0
    errors = 0
    fill_counts = {name: 0 for name in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Lu %d records...", n_read)
                gc.collect()

            # ---------------------------------------------------------------
            # Extract raw fields
            # ---------------------------------------------------------------
            uid = rec.get("partant_uid")
            discipline = rec.get("discipline")
            type_piste = rec.get("type_piste")
            is_psf = rec.get("met_is_psf")
            sexe = rec.get("sexe")
            age = _safe_int(rec.get("age"))
            distance = _safe_float(rec.get("distance"))
            corde = _safe_int(rec.get("corde"))
            num_pmu = _safe_int(rec.get("num_pmu"))
            nb_partants = _safe_int(rec.get("nombre_partants"))
            hippo = rec.get("hippodrome_normalise")
            met_impact = _safe_float(rec.get("met_impact_meteo_score"))
            poids_kg = _safe_float(rec.get("poids_porte_kg"))
            nb_courses = _safe_int(rec.get("nb_courses_carriere"))
            date_iso = rec.get("date_reunion_iso", "")

            # ---------------------------------------------------------------
            # Encode categorical components
            # ---------------------------------------------------------------
            disc_c = _discipline_code(discipline)
            surf_c = _surface_code(type_piste, is_psf)
            sex_c = _sex_code(sexe)
            age_b = _age_bucket(age)
            dist_cat = _distance_category(distance)
            dz = _draw_zone(corde, nb_partants)
            exp_lvl = _experience_level(nb_courses)
            weather_b = _weather_bracket(met_impact)
            weight_c = _weight_category(poids_kg)
            hippo_sz = _hippo_size(hippo)

            # Month from date
            month = None
            if date_iso and len(date_iso) >= 7:
                try:
                    month = int(date_iso[5:7])
                except (ValueError, IndexError):
                    month = None

            # Corde encoded (capped at 20 for compact encoding)
            corde_enc = min(corde, 20) if corde is not None and corde > 0 else None

            # ---------------------------------------------------------------
            # Build features
            # ---------------------------------------------------------------
            features: dict[str, Any] = {"partant_uid": uid}

            # 1. ict_discipline_x_surface
            if disc_c is not None and surf_c is not None:
                features["ict_discipline_x_surface"] = disc_c * 10 + surf_c
                fill_counts["ict_discipline_x_surface"] += 1
            else:
                features["ict_discipline_x_surface"] = None

            # 2. ict_sex_x_age_group
            if sex_c is not None and age_b is not None:
                features["ict_sex_x_age_group"] = sex_c * 10 + age_b
                fill_counts["ict_sex_x_age_group"] += 1
            else:
                features["ict_sex_x_age_group"] = None

            # 3. ict_discipline_x_distance_cat
            if disc_c is not None and dist_cat is not None:
                features["ict_discipline_x_distance_cat"] = disc_c * 4 + dist_cat
                fill_counts["ict_discipline_x_distance_cat"] += 1
            else:
                features["ict_discipline_x_distance_cat"] = None

            # 4. ict_corde_x_draw_zone
            if corde_enc is not None and dz is not None:
                features["ict_corde_x_draw_zone"] = corde_enc * 3 + dz
                fill_counts["ict_corde_x_draw_zone"] += 1
            else:
                features["ict_corde_x_draw_zone"] = None

            # 5. ict_hippo_x_discipline
            if hippo and disc_c is not None:
                h = hash(hippo.strip().lower()) % 50
                features["ict_hippo_x_discipline"] = h * 3 + disc_c
                fill_counts["ict_hippo_x_discipline"] += 1
            else:
                features["ict_hippo_x_discipline"] = None

            # 6. ict_age_x_experience
            if age_b is not None and exp_lvl is not None:
                features["ict_age_x_experience"] = age_b * 4 + exp_lvl
                fill_counts["ict_age_x_experience"] += 1
            else:
                features["ict_age_x_experience"] = None

            # 7. ict_sex_x_discipline
            if sex_c is not None and disc_c is not None:
                features["ict_sex_x_discipline"] = sex_c * 3 + disc_c
                fill_counts["ict_sex_x_discipline"] += 1
            else:
                features["ict_sex_x_discipline"] = None

            # 8. ict_surface_x_weather
            if surf_c is not None and weather_b is not None:
                features["ict_surface_x_weather"] = surf_c * 3 + weather_b
                fill_counts["ict_surface_x_weather"] += 1
            else:
                features["ict_surface_x_weather"] = None

            # 9. ict_distance_x_weight
            if dist_cat is not None and weight_c is not None:
                features["ict_distance_x_weight"] = dist_cat * 4 + weight_c
                fill_counts["ict_distance_x_weight"] += 1
            else:
                features["ict_distance_x_weight"] = None

            # 10. ict_month_x_hippo_type
            if month is not None:
                features["ict_month_x_hippo_type"] = month * 3 + hippo_sz
                fill_counts["ict_month_x_hippo_type"] += 1
            else:
                features["ict_month_x_hippo_type"] = None

            # Write record
            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info("Lecture terminee: %d records, %d erreurs JSON", n_read, errors)
    logger.info(
        "Interaction categorical build termine: %d features en %.1fs",
        n_written, elapsed,
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
        description="Construction des features d'interaction categorielle a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/interaction_categorical/)",
    )
    args = parser.parse_args()

    logger = setup_logging("interaction_categorical_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "interaction_categorical.jsonl"
    build_interaction_categorical(input_path, out_path, logger)


if __name__ == "__main__":
    main()
