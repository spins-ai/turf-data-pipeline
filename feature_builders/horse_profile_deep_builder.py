#!/usr/bin/env python3
"""
feature_builders.horse_profile_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep horse profiling features combining sex, age, breed, country
for all model types.

Simple single-pass streaming -- no chronological sort needed.
Each record is self-contained (no historical accumulator).

Produces:
  - horse_profile_deep_features.jsonl  in builder_outputs/horse_profile_deep/

Features per partant (12):
  - hpd_sex_encoded          : hongres=0, males=1, femelles=2
  - hpd_is_gelding           : 1 if hongres (gelding)
  - hpd_is_mare_pregnant     : 1 if jument_pleine
  - hpd_breed_encoded        : TROTTEUR_FRANCAIS=0, PUR_SANG=1, AQPS=2, other=3
  - hpd_is_french_bred       : 1 if pays_cheval contains France
  - hpd_age_peak_distance    : age adjusted for discipline peak (trot peaks later)
  - hpd_career_density       : nb_courses_carriere / age (races per year)
  - hpd_win_efficiency       : nb_victoires / (nb_courses + 1)
  - hpd_place_efficiency     : nb_places / (nb_courses + 1)
  - hpd_gains_efficiency     : gains_carriere / (nb_courses + 1)
  - hpd_is_inedit_numeric    : 1 if is_inedit (first time racing)
  - hpd_engagement_signal    : 1 if engagement is True (entry fee paid)

Usage:
    python feature_builders/horse_profile_deep_builder.py
    python feature_builders/horse_profile_deep_builder.py --input path/to/partants_master.jsonl
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
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/horse_profile_deep")

_LOG_EVERY = 500_000

# Sex encoding
_SEX_MAP = {
    "hongre": 0, "hongres": 0, "h": 0,
    "male": 1, "males": 1, "m": 1, "entier": 1, "entiers": 1,
    "femelle": 2, "femelles": 2, "f": 2, "jument": 2, "juments": 2,
}

# Breed encoding
_BREED_MAP = {
    "trotteur francais": 0, "trotteur_francais": 0, "tf": 0,
    "pur sang": 1, "pur_sang": 1, "ps": 1, "pur-sang": 1,
    "aqps": 2,
}

# Discipline peak ages (approximate)
_PEAK_AGE_TROT = 7.0
_PEAK_AGE_GALOP = 4.0


# ===========================================================================
# HELPERS
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _encode_sex(raw) -> Optional[int]:
    """Encode sex string to integer."""
    if not raw:
        return None
    return _SEX_MAP.get(str(raw).strip().lower())


def _encode_breed(raw) -> Optional[int]:
    """Encode breed string to integer. Unknown breeds -> 3."""
    if not raw:
        return None
    key = str(raw).strip().lower()
    return _BREED_MAP.get(key, 3)


def _is_french(pays_raw) -> Optional[int]:
    """Return 1 if country contains 'france', 0 otherwise, None if missing."""
    if not pays_raw:
        return None
    return 1 if "france" in str(pays_raw).strip().lower() else 0


def _age_peak_distance(age_val: Optional[float], discipline_raw) -> Optional[float]:
    """Age adjusted for discipline peak: negative = before peak, positive = past peak."""
    if age_val is None:
        return None
    disc = str(discipline_raw).strip().lower() if discipline_raw else ""
    if "trot" in disc:
        peak = _PEAK_AGE_TROT
    else:
        peak = _PEAK_AGE_GALOP
    return round(age_val - peak, 2)


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


_FEATURE_KEYS = [
    "hpd_sex_encoded",
    "hpd_is_gelding",
    "hpd_is_mare_pregnant",
    "hpd_breed_encoded",
    "hpd_is_french_bred",
    "hpd_age_peak_distance",
    "hpd_career_density",
    "hpd_win_efficiency",
    "hpd_place_efficiency",
    "hpd_gains_efficiency",
    "hpd_is_inedit_numeric",
    "hpd_engagement_signal",
]


def _compute_features(rec: dict) -> dict[str, Any]:
    """Compute all 12 features from a single record."""
    feats: dict[str, Any] = {k: None for k in _FEATURE_KEYS}

    # --- Sex ---
    sex_raw = rec.get("sexe") or rec.get("sex") or rec.get("sexe_cheval")
    sex_enc = _encode_sex(sex_raw)
    feats["hpd_sex_encoded"] = sex_enc
    feats["hpd_is_gelding"] = (1 if sex_enc == 0 else 0) if sex_enc is not None else None

    # --- Mare pregnant ---
    jp = rec.get("jument_pleine")
    if jp is not None:
        feats["hpd_is_mare_pregnant"] = 1 if jp else 0

    # --- Breed ---
    breed_raw = rec.get("race_cheval") or rec.get("race") or rec.get("breed")
    feats["hpd_breed_encoded"] = _encode_breed(breed_raw)

    # --- Country ---
    pays_raw = rec.get("pays_cheval") or rec.get("pays") or rec.get("nationalite")
    feats["hpd_is_french_bred"] = _is_french(pays_raw)

    # --- Age peak distance ---
    age = _safe_float(rec.get("age"))
    discipline = rec.get("discipline") or rec.get("specialite")
    feats["hpd_age_peak_distance"] = _age_peak_distance(age, discipline)

    # --- Career density ---
    nb_courses = _safe_float(rec.get("nb_courses_carriere") or rec.get("nb_courses"))
    if age is not None and age > 0 and nb_courses is not None:
        feats["hpd_career_density"] = round(nb_courses / age, 4)

    # --- Win efficiency ---
    nb_victoires = _safe_float(rec.get("nb_victoires_carriere") or rec.get("nb_victoires"))
    nb_c = _safe_float(rec.get("nb_courses_carriere") or rec.get("nb_courses"))
    if nb_victoires is not None and nb_c is not None:
        feats["hpd_win_efficiency"] = round(nb_victoires / (nb_c + 1), 4)

    # --- Place efficiency ---
    nb_places = _safe_float(rec.get("nb_places_carriere") or rec.get("nb_places"))
    if nb_places is not None and nb_c is not None:
        feats["hpd_place_efficiency"] = round(nb_places / (nb_c + 1), 4)

    # --- Gains efficiency ---
    gains = _safe_float(rec.get("gains_carriere") or rec.get("gains_total"))
    if gains is not None and nb_c is not None:
        feats["hpd_gains_efficiency"] = round(gains / (nb_c + 1), 2)

    # --- Is inedit ---
    is_inedit = rec.get("is_inedit")
    if is_inedit is not None:
        feats["hpd_is_inedit_numeric"] = 1 if is_inedit else 0

    # --- Engagement signal ---
    engagement = rec.get("engagement")
    if engagement is not None:
        feats["hpd_engagement_signal"] = 1 if engagement else 0

    return feats


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_horse_profile_deep_features(
    input_path: Path, output_path: Path, logger,
) -> int:
    """Build horse profile deep features from partants_master.jsonl.

    Single-pass streaming: read each record, compute features, write output.
    No sorting needed -- features are record-local.

    Returns the total number of feature records written.
    """
    logger.info("=== Horse Profile Deep Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_read = 0
    n_written = 0
    fill_counts = {k: 0 for k in _FEATURE_KEYS}

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_read += 1

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid")
            date_iso = rec.get("date_reunion_iso", "")

            if not partant_uid:
                continue

            feats = _compute_features(rec)

            out_rec: dict[str, Any] = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_iso,
            }
            out_rec.update(feats)

            fout.write(json.dumps(out_rec, ensure_ascii=False))
            fout.write("\n")
            n_written += 1

            for k in _FEATURE_KEYS:
                if feats[k] is not None:
                    fill_counts[k] += 1

            if n_read % _LOG_EVERY == 0:
                logger.info("  Traite %d records, ecrit %d...", n_read, n_written)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Horse Profile Deep build termine: %d features en %.1fs",
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
        description="Construction des features profil cheval profond a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/horse_profile_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("horse_profile_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "horse_profile_deep_features.jsonl"
    build_horse_profile_deep_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
