#!/usr/bin/env python3
"""
feature_builders.deep_learning_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features designed for deep learning models (LSTM, TabNet, TFT, etc.)
and advanced model-specific preparations.

Produces:
  - dl_features.jsonl   in output/dl_features/

Features per partant (12):
  - attention_cheval_norm    : normalised horse ID hash (0-1)
  - attention_jockey_norm    : normalised jockey ID hash (0-1)
  - attention_course_norm    : normalised course context hash (0-1)
  - tft_is_static_pedigree  : 1 if horse has pedigree data (static feature flag)
  - tft_is_static_hippo     : 1 if hippodrome is known (static feature flag)
  - tft_is_dynamic_form     : 1 (always; form is dynamic by nature)
  - tft_is_dynamic_odds     : 1 if odds data available
  - tabnet_group_form       : feature group ID for form features (0)
  - tabnet_group_pedigree   : feature group ID for pedigree features (1)
  - tabnet_group_odds       : feature group ID for odds features (2)
  - tabnet_group_context    : feature group ID for context features (3)
  - has_full_sequence       : 1 if horse has >=5 prior races for sequence models

Usage:
    python feature_builders/deep_learning_features_builder.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "dl_features"
_LOG_EVERY = 500_000


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
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


def _hash_norm(s: str) -> float:
    """Hash a string to a float in [0, 1)."""
    if not s:
        return 0.0
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16) / 0xFFFFFFFF


def build_dl_features(input_path: Path, logger) -> list[dict[str, Any]]:
    logger.info("=== Deep Learning Features Builder ===")
    t0 = time.time()

    # Track horse race counts for sequence readiness
    horse_race_count: dict[str, int] = defaultdict(int)
    results: list[dict[str, Any]] = []
    n = 0

    for rec in _iter_jsonl(input_path, logger):
        n += 1
        if n % _LOG_EVERY == 0:
            logger.info("  Traite %d records...", n)

        uid = rec.get("partant_uid")
        cheval = rec.get("nom_cheval") or ""
        jockey = rec.get("jockey") or rec.get("driver") or ""
        hippo = rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
        disc = rec.get("discipline") or ""
        date_str = rec.get("date_reunion_iso") or ""
        has_pedigree = bool(rec.get("pere") or rec.get("mere"))
        has_hippo = bool(hippo)
        has_odds = bool(rec.get("cote_finale") or rec.get("rapport_final"))

        # Course context = hippodrome + discipline + date
        course_ctx = f"{hippo}_{disc}_{date_str[:10]}"

        # Sequence readiness
        prior_races = horse_race_count[cheval] if cheval else 0
        has_full_seq = 1 if prior_races >= 5 else 0

        results.append({
            "partant_uid": uid,
            "attention_cheval_norm": round(_hash_norm(cheval), 6),
            "attention_jockey_norm": round(_hash_norm(jockey), 6),
            "attention_course_norm": round(_hash_norm(course_ctx), 6),
            "tft_is_static_pedigree": 1 if has_pedigree else 0,
            "tft_is_static_hippo": 1 if has_hippo else 0,
            "tft_is_dynamic_form": 1,
            "tft_is_dynamic_odds": 1 if has_odds else 0,
            "tabnet_group_form": 0,
            "tabnet_group_pedigree": 1,
            "tabnet_group_odds": 2,
            "tabnet_group_context": 3,
            "has_full_sequence": has_full_seq,
        })

        if cheval:
            horse_race_count[cheval] += 1

    dt = time.time() - t0
    logger.info("Terminé: %d features en %.1fs", len(results), dt)
    return results


def main():
    logger = setup_logging("dl_features_builder")
    parser = argparse.ArgumentParser(description="Deep Learning Features Builder")
    parser.add_argument("--input", type=str, help="Path to partants_master.jsonl")
    args = parser.parse_args()

    input_path = None
    if args.input:
        input_path = Path(args.input)
    else:
        for c in INPUT_CANDIDATES:
            if c.exists():
                input_path = c
                break

    if not input_path or not input_path.exists():
        logger.error("Aucun fichier partants_master.jsonl trouve.")
        sys.exit(1)

    results = build_dl_features(input_path, logger)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "dl_features.jsonl"
    save_jsonl(results, out_path, logger)
    logger.info("Sauvegarde: %s (%d records)", out_path, len(results))


if __name__ == "__main__":
    main()
