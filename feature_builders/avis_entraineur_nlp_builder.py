#!/usr/bin/env python3
"""
feature_builders.avis_entraineur_nlp_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trainer opinion (avis_entraineur) NLP features -- extracting signal
from trainer comments using simple keyword matching.

No temporal concerns: features are derived from the partant's own text
fields (known before the race).

Produces:
  - avis_entraineur_nlp.jsonl  in builder_outputs/avis_entraineur_nlp/

Features per partant (8):
  - aen_has_avis          : 1 if avis_entraineur is not null/empty
  - aen_avis_length       : character length of avis_entraineur
  - aen_positive_keywords : count of positive keywords found
  - aen_negative_keywords : count of negative keywords found
  - aen_sentiment_score   : (pos - neg) / (pos + neg + 1)
  - aen_has_comment       : 1 if commentaire_apres_course is not null/empty
  - aen_comment_length    : character length of commentaire_apres_course
  - aen_mentions_distance : 1 if avis mentions "distance" or "parcours"

Usage:
    python feature_builders/avis_entraineur_nlp_builder.py
    python feature_builders/avis_entraineur_nlp_builder.py --input path/to/partants_master.jsonl
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/avis_entraineur_nlp")

_LOG_EVERY = 500_000

# ===========================================================================
# KEYWORD LISTS
# ===========================================================================

_POSITIVE_KEYWORDS = [
    "bon", "bien", "forme", "confiance", "progresse", "devrait",
    "chance", "espere", "pret", "capable", "content", "satisfait",
]

_NEGATIVE_KEYWORDS = [
    "difficile", "doute", "inquiet", "manque", "pas pret",
    "probleme", "blessure", "meforme", "decu", "complique",
]

_DISTANCE_KEYWORDS = ["distance", "parcours"]


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


def _normalize_text(val: Any) -> Optional[str]:
    """Return lowercased stripped text, or None if empty/missing."""
    if not val or not isinstance(val, str):
        return None
    text = val.strip()
    return text.lower() if text else None


def _count_keywords(text_lower: str, keywords: list[str]) -> int:
    """Count how many keyword occurrences appear in lowered text."""
    count = 0
    for kw in keywords:
        count += text_lower.count(kw)
    return count


# ===========================================================================
# FEATURE EXTRACTION
# ===========================================================================


def _extract_features(rec: dict) -> dict[str, Any]:
    """Compute all 8 NLP features for one partant record."""
    uid = rec.get("partant_uid")

    avis_raw = rec.get("avis_entraineur")
    comment_raw = rec.get("commentaire_apres_course")

    avis = _normalize_text(avis_raw)
    comment = _normalize_text(comment_raw)

    # --- avis_entraineur features ---
    has_avis = 1 if avis is not None else 0
    avis_length = len(avis) if avis is not None else 0

    pos_count = 0
    neg_count = 0
    mentions_distance = 0

    if avis is not None:
        pos_count = _count_keywords(avis, _POSITIVE_KEYWORDS)
        neg_count = _count_keywords(avis, _NEGATIVE_KEYWORDS)
        mentions_distance = 1 if any(kw in avis for kw in _DISTANCE_KEYWORDS) else 0

    sentiment = round((pos_count - neg_count) / (pos_count + neg_count + 1), 4)

    # --- commentaire_apres_course features ---
    has_comment = 1 if comment is not None else 0
    comment_length = len(comment) if comment is not None else 0

    return {
        "partant_uid": uid,
        "aen_has_avis": has_avis,
        "aen_avis_length": avis_length,
        "aen_positive_keywords": pos_count,
        "aen_negative_keywords": neg_count,
        "aen_sentiment_score": sentiment,
        "aen_has_comment": has_comment,
        "aen_comment_length": comment_length,
        "aen_mentions_distance": mentions_distance,
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_avis_nlp_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build trainer opinion NLP features from partants_master.jsonl."""
    logger.info("=== Avis Entraineur NLP Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0
    n_with_avis = 0
    n_with_comment = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        feats = _extract_features(rec)
        results.append(feats)

        if feats["aen_has_avis"]:
            n_with_avis += 1
        if feats["aen_has_comment"]:
            n_with_comment += 1

    elapsed = time.time() - t0
    logger.info(
        "Avis NLP build termine: %d features en %.1fs "
        "(avis: %d, commentaires: %d)",
        len(results), elapsed, n_with_avis, n_with_comment,
    )

    return results


# ===========================================================================
# SAUVEGARDE & CLI
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
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features NLP avis entraineur a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/avis_entraineur_nlp/)",
    )
    args = parser.parse_args()

    logger = setup_logging("avis_entraineur_nlp_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_avis_nlp_features(input_path, logger)

    out_path = output_dir / "avis_entraineur_nlp.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rates
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
