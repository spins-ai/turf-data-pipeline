#!/usr/bin/env python3
"""
feature_builders.phenotype_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse phenotype features -- physical characteristics and their racing
implications.

Single-pass streaming builder: each feature is derived from the current
record's static fields (no temporal state needed).

Produces:
  - phenotype_features.jsonl   in output/phenotype_features/

Features per partant (8):
  - phn_robe_category           : numeric encoding of robe colour
  - phn_is_grey                 : 1 if robe contains "gris" (often overbet)
  - phn_breed_speed_score       : breed speed potential (0-3)
  - phn_breed_stamina_score     : breed stamina potential (0-3)
  - phn_origin_quality          : racing-nation quality score (0-3)
  - phn_weight_for_age          : poids_porte_kg / (age + 0.01)
  - phn_sex_advantage           : 1 if male, 0 otherwise
  - phn_breed_x_discipline_match: how well breed matches discipline (0-3)

Usage:
    python feature_builders/phenotype_features_builder.py
    python feature_builders/phenotype_features_builder.py --input path/to/partants_master.jsonl
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/phenotype_features")

_LOG_EVERY = 500_000

# ===========================================================================
# LOOKUP TABLES
# ===========================================================================

# Robe -> numeric category
_ROBE_MAP: dict[str, int] = {
    "bai": 0,
    "alezan": 1,
    "gris": 2,
    "noir": 3,
    "bai brun": 4,
    "bai_brun": 4,
    "rouan": 5,
}
_ROBE_OTHER = 6

# Breed -> speed score (PS/PUR_SANG = fast flat, AQPS = mid, TROTTEUR = slow)
_BREED_SPEED: dict[str, int] = {
    "ps": 3, "pur_sang": 3, "pur sang": 3,
    "aqps": 2,
    "trotteur": 1, "trotteur francais": 1, "tf": 1,
}

# Breed -> stamina score (AQPS = high stamina, TROTTEUR = mid, PS = low)
_BREED_STAMINA: dict[str, int] = {
    "aqps": 3,
    "trotteur": 2, "trotteur francais": 2, "tf": 2,
    "ps": 1, "pur_sang": 1, "pur sang": 1,
}

# Country of birth -> racing-nation quality
_ORIGIN_QUALITY: dict[str, int] = {
    "fra": 2, "france": 2,
    "ire": 3, "irlande": 3, "gb": 3, "grande-bretagne": 3,
    "usa": 2, "etats-unis": 2,
    "ger": 1, "allemagne": 1,
    "ita": 1, "italie": 1,
}

# Breed x discipline match scores:
#   PS + PLAT = 3, AQPS + OBSTACLE = 3, TROTTEUR + TROT = 3, etc.
_BREED_DISCIPLINE_MATCH: dict[tuple[str, str], int] = {
    # PS / PUR_SANG
    ("ps", "plat"): 3,        ("ps", "attele"): 0,
    ("ps", "obstacle"): 1,    ("ps", "monte"): 0,
    ("ps", "haies"): 1,       ("ps", "steeple"): 1,
    ("ps", "cross"): 1,       ("ps", "trot"): 0,
    ("pur_sang", "plat"): 3,  ("pur_sang", "obstacle"): 1,
    ("pur_sang", "haies"): 1, ("pur_sang", "steeple"): 1,
    # AQPS
    ("aqps", "plat"): 1,      ("aqps", "obstacle"): 3,
    ("aqps", "haies"): 3,     ("aqps", "steeple"): 3,
    ("aqps", "cross"): 3,     ("aqps", "attele"): 0,
    ("aqps", "monte"): 0,     ("aqps", "trot"): 0,
    # TROTTEUR
    ("trotteur", "attele"): 3,  ("trotteur", "monte"): 3,
    ("trotteur", "trot"): 3,    ("trotteur", "plat"): 0,
    ("trotteur", "obstacle"): 0,
    ("tf", "attele"): 3,        ("tf", "monte"): 3,
    ("tf", "trot"): 3,          ("tf", "plat"): 0,
}


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
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _norm(s: Optional[str]) -> str:
    """Lowercase, strip, collapse whitespace."""
    if not s:
        return ""
    return s.strip().lower()


# ===========================================================================
# FEATURE COMPUTATION (per-record, no state)
# ===========================================================================


def _compute_features(rec: dict) -> dict[str, Any]:
    """Compute all 8 phenotype features from a single partant record."""
    uid = rec.get("partant_uid")

    # --- Raw fields ---
    robe_raw = _norm(rec.get("pgr_robe") or rec.get("robe"))
    sexe_raw = _norm(rec.get("pgr_sexe") or rec.get("sexe"))
    race_raw = _norm(rec.get("pgr_race") or rec.get("race"))
    pays_raw = _norm(rec.get("pgr_pays_naissance") or rec.get("pgr_pays_cheval") or rec.get("pays"))
    age = _safe_float(rec.get("age"))
    poids = _safe_float(rec.get("poids_porte_kg"))
    discipline = _norm(rec.get("discipline"))

    features: dict[str, Any] = {
        "partant_uid": uid,
        "phn_robe_category": None,
        "phn_is_grey": None,
        "phn_breed_speed_score": None,
        "phn_breed_stamina_score": None,
        "phn_origin_quality": None,
        "phn_weight_for_age": None,
        "phn_sex_advantage": None,
        "phn_breed_x_discipline_match": None,
    }

    # 1. phn_robe_category
    if robe_raw:
        features["phn_robe_category"] = _ROBE_MAP.get(robe_raw, _ROBE_OTHER)

    # 2. phn_is_grey
    if robe_raw:
        features["phn_is_grey"] = 1 if "gris" in robe_raw else 0

    # 3. phn_breed_speed_score
    if race_raw:
        features["phn_breed_speed_score"] = _BREED_SPEED.get(race_raw, 0)

    # 4. phn_breed_stamina_score
    if race_raw:
        features["phn_breed_stamina_score"] = _BREED_STAMINA.get(race_raw, 0)

    # 5. phn_origin_quality
    if pays_raw:
        features["phn_origin_quality"] = _ORIGIN_QUALITY.get(pays_raw, 0)

    # 6. phn_weight_for_age
    if poids is not None and poids > 0 and age is not None:
        features["phn_weight_for_age"] = round(poids / (age + 0.01), 4)

    # 7. phn_sex_advantage
    if sexe_raw:
        # Males: "m", "males", "hongre" (gelding counts as originally male)
        features["phn_sex_advantage"] = 1 if sexe_raw in ("m", "males", "male", "hongre", "h") else 0

    # 8. phn_breed_x_discipline_match
    if race_raw and discipline:
        features["phn_breed_x_discipline_match"] = _BREED_DISCIPLINE_MATCH.get(
            (race_raw, discipline), 0
        )

    return features


# ===========================================================================
# MAIN BUILD (single-pass streaming with .tmp + rename)
# ===========================================================================


def build_phenotype_features(input_path: Path, output_path: Path, logger) -> int:
    """Build phenotype features in a single streaming pass.

    Writes directly to a .tmp file, then atomically renames.
    Calls gc.collect() every _LOG_EVERY records.

    Returns the total number of feature records written.
    """
    logger.info("=== Phenotype Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    fill_counts: dict[str, int] = {
        "phn_robe_category": 0,
        "phn_is_grey": 0,
        "phn_breed_speed_score": 0,
        "phn_breed_stamina_score": 0,
        "phn_origin_quality": 0,
        "phn_weight_for_age": 0,
        "phn_sex_advantage": 0,
        "phn_breed_x_discipline_match": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            features = _compute_features(rec)
            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

            # Track fill rates
            for k in fill_counts:
                if features.get(k) is not None:
                    fill_counts[k] += 1

            if n_written % _LOG_EVERY == 0:
                logger.info("  Ecrit %d records...", n_written)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Phenotype features build termine: %d features en %.1fs",
        n_written, elapsed,
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


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features phenotype a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: INPUT_PARTANTS)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: OUTPUT_DIR)",
    )
    args = parser.parse_args()

    logger = setup_logging("phenotype_features_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "phenotype_features.jsonl"
    build_phenotype_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
