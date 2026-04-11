#!/usr/bin/env python3
"""
feature_builders.horse_name_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse name derived features (name length, character patterns, country/breed
encoding).  Surprisingly predictive in some racing models.

Temporal integrity: single-pass, all features derived from the current record
-- no temporal tracking needed.

Produces:
  - horse_name_features.jsonl  in builder_outputs/horse_name_features/

Features per partant (8):
  - hnf_name_length          : number of characters in nom_cheval
  - hnf_name_word_count      : number of words in nom_cheval (split by space)
  - hnf_has_apostrophe       : 1 if name contains apostrophe (common in Irish/French bloodlines)
  - hnf_starts_with_letter   : ord(first letter) - ord('A') (0-25, proxy for naming convention by year)
  - hnf_country_code         : encoded pays_cheval/pays_naissance (FRA=0, GB=1, IRE=2, USA=3, GER=4, ITA=5, other=6)
  - hnf_robe_encoded         : encode pgr_robe (BAI=0, ALEZAN=1, GRIS=2, NOIR=3, BAIBRN=4, other=5)
  - hnf_breed_encoded        : encode pgr_race (PS=0, TROTTEUR=1, AQPS=2, AR=3, AA=4, SF=5, other=6)
  - hnf_is_foreign           : 1 if pays_cheval is not FRA/FRANCE, 0 if French

Usage:
    python feature_builders/horse_name_features_builder.py
    python feature_builders/horse_name_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import os
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/horse_name_features")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# ===========================================================================
# ENCODING MAPS
# ===========================================================================

_COUNTRY_MAP: dict[str, int] = {
    "FRA": 0, "FRANCE": 0, "FR": 0,
    "GB": 1, "GBR": 1, "GRANDE-BRETAGNE": 1,
    "IRE": 2, "IRL": 2, "IRLANDE": 2,
    "USA": 3, "US": 3, "ETATS-UNIS": 3,
    "GER": 4, "DEU": 4, "ALL": 4, "ALLEMAGNE": 4,
    "ITA": 5, "ITY": 5, "ITALIE": 5,
}
_COUNTRY_OTHER = 6

_FRENCH_CODES = {"FRA", "FRANCE", "FR"}

_ROBE_MAP: dict[str, int] = {
    "BAI": 0, "B": 0,
    "ALEZAN": 1, "AL": 1,
    "GRIS": 2, "GR": 2,
    "NOIR": 3, "N": 3,
    "BAIBRN": 4, "BB": 4, "BAI BRUN": 4,
}
_ROBE_OTHER = 5

_BREED_MAP: dict[str, int] = {
    "PS": 0, "PUR-SANG": 0, "PURSANG": 0,
    "TROTTEUR": 1, "TF": 1, "TR": 1,
    "AQPS": 2,
    "AR": 3, "ARABE": 3,
    "AA": 4, "ANGLO-ARABE": 4,
    "SF": 5, "SELLE FRANCAIS": 5,
}
_BREED_OTHER = 6


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
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


def _norm_upper(val: Any) -> Optional[str]:
    """Normalise a string field to upper-case, return None if empty."""
    if val is None:
        return None
    s = str(val).strip().upper()
    return s if s else None


def _encode_country(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    return _COUNTRY_MAP.get(raw, _COUNTRY_OTHER)


def _encode_robe(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    return _ROBE_MAP.get(raw, _ROBE_OTHER)


def _encode_breed(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    return _BREED_MAP.get(raw, _BREED_OTHER)


def _is_foreign(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    return 0 if raw in _FRENCH_CODES else 1


def _compute_features(rec: dict) -> dict[str, Any]:
    """Compute all 8 horse-name features from a single record."""
    uid = rec.get("partant_uid")
    nom = rec.get("nom_cheval")
    nom_clean = nom.strip() if isinstance(nom, str) and nom.strip() else None

    # Country: try pays_cheval, then pays_naissance / pgr_pays_naissance
    pays_raw = _norm_upper(
        rec.get("pays_cheval")
        or rec.get("pays_naissance")
        or rec.get("pgr_pays_naissance")
    )
    robe_raw = _norm_upper(rec.get("pgr_robe"))
    breed_raw = _norm_upper(rec.get("pgr_race"))

    feats: dict[str, Any] = {"partant_uid": uid}

    # --- Name-based features ---
    if nom_clean:
        feats["hnf_name_length"] = len(nom_clean)
        feats["hnf_name_word_count"] = len(nom_clean.split())
        feats["hnf_has_apostrophe"] = int("'" in nom_clean)

        first_char = nom_clean[0].upper()
        if "A" <= first_char <= "Z":
            feats["hnf_starts_with_letter"] = ord(first_char) - ord("A")
        else:
            feats["hnf_starts_with_letter"] = None
    else:
        feats["hnf_name_length"] = None
        feats["hnf_name_word_count"] = None
        feats["hnf_has_apostrophe"] = None
        feats["hnf_starts_with_letter"] = None

    # --- Encoded categorical features ---
    feats["hnf_country_code"] = _encode_country(pays_raw)
    feats["hnf_robe_encoded"] = _encode_robe(robe_raw)
    feats["hnf_breed_encoded"] = _encode_breed(breed_raw)
    feats["hnf_is_foreign"] = _is_foreign(pays_raw)

    return feats


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_horse_name_features(input_path: Path, output_dir: Path, logger) -> int:
    """Stream-build horse name features, write to .tmp then rename."""
    logger.info("=== Horse Name Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "horse_name_features.jsonl"
    tmp_path = output_dir / "horse_name_features.jsonl.tmp"

    n_written = 0
    fill_counts: Optional[dict[str, int]] = None

    with open(tmp_path, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            feats = _compute_features(rec)
            fout.write(json.dumps(feats, ensure_ascii=False) + "\n")
            n_written += 1

            # Initialise fill-rate counters from first record
            if fill_counts is None:
                fill_counts = {k: 0 for k in feats if k != "partant_uid"}

            for k in fill_counts:
                if feats.get(k) is not None:
                    fill_counts[k] += 1

            if n_written % _LOG_EVERY == 0:
                logger.info("  Ecrit %d records...", n_written)
                gc.collect()

    # Atomic rename
    if tmp_path.exists():
        if out_path.exists():
            out_path.unlink()
        os.rename(str(tmp_path), str(out_path))

    elapsed = time.time() - t0
    logger.info(
        "Horse name features build termine: %d records en %.1fs",
        n_written, elapsed,
    )

    # Fill rates
    if fill_counts and n_written > 0:
        logger.info("=== Fill rates ===")
        for k, v in fill_counts.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
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
        description="Construction des features horse name a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("horse_name_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR

    build_horse_name_features(input_path, output_dir, logger)


if __name__ == "__main__":
    main()
