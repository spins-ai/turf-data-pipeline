#!/usr/bin/env python3
"""
feature_builders.geny_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
10-15 features from Geny.com data (output/26).

Tipster consensus, comment sentiment scores, pronostic data.

Temporal integrity: Geny data matched by date + horse. Only pre-race
published pronostics are used.

Usage:
    python feature_builders/geny_builder.py
    python feature_builders/geny_builder.py --geny-data output/26_geny/geny.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

GENY_DEFAULT = os.path.join("output", "26_geny", "geny.jsonl")
PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "geny_features")

# ===========================================================================
# HELPERS
# ===========================================================================

# ===========================================================================
# SENTIMENT SCORING
# ===========================================================================

POSITIVE_WORDS = {
    "favori", "chance", "gagne", "confiance", "forme", "excellent", "solide",
    "regulier", "impressionnant", "bon", "serieux", "meilleur", "incontournable",
    "fiable", "redoutable", "capable", "fort", "performant",
}

NEGATIVE_WORDS = {
    "doute", "decevant", "risque", "recul", "mefiance", "faible", "incertain",
    "declin", "irregulier", "mediocre", "mauvais", "difficile", "limite",
    "dangereux", "piege", "eviter",
}


def _comment_score(text: str) -> Optional[float]:
    """Score a comment from -1 (very negative) to +1 (very positive)."""
    if not text:
        return None
    words = str(text).lower().split()
    pos = sum(1 for w in words if w in POSITIVE_WORDS)
    neg = sum(1 for w in words if w in NEGATIVE_WORDS)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 4)

# ===========================================================================
# INDEX GENY DATA
# ===========================================================================

def index_geny_data(geny_records: list, logger: logging.Logger) -> dict:
    """Index Geny data by (date, horse_name_norm)."""
    idx = {}
    for rec in geny_records:
        date = str(rec.get("date", "") or rec.get("date_reunion_iso", "") or "")[:10]
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        if date and horse:
            idx[(date, horse)] = rec
    logger.info("Index Geny: %d entrees", len(idx))
    return idx


def build_race_consensus(geny_records: list, logger: logging.Logger) -> dict:
    """Build per-race tipster consensus: how many tipsters picked each horse."""
    # Group by (date, course_id)
    race_tips: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for rec in geny_records:
        date = str(rec.get("date", "") or rec.get("date_reunion_iso", "") or "")[:10]
        course = rec.get("course_uid") or rec.get("id_course") or ""
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        if not (date and horse):
            continue
        key = f"{date}|{course}"

        # Count tipster selections
        nb_tips = rec.get("nb_tipsters") or rec.get("nb_selections") or 0
        try:
            nb_tips = int(nb_tips)
        except (ValueError, TypeError):
            nb_tips = 1 if rec.get("is_selection") else 0
        race_tips[key][horse] += max(nb_tips, 0)

    # Compute consensus rank per race
    consensus = {}
    for race_key, horses in race_tips.items():
        sorted_horses = sorted(horses.items(), key=lambda x: -x[1])
        total_tips = sum(v for _, v in sorted_horses) or 1
        for rank, (horse, tips) in enumerate(sorted_horses, 1):
            consensus[(race_key, horse)] = {
                "consensus_rank": rank,
                "consensus_tips": tips,
                "consensus_pct": round(tips / total_tips, 4),
            }
    logger.info("Consensus built for %d (race, horse) pairs", len(consensus))
    return consensus

# ===========================================================================
# BUILDER
# ===========================================================================

def build_geny_features(partants: list, geny_idx: dict, consensus: dict, logger: logging.Logger) -> list:
    """Build 10-15 features from Geny data."""

    enriched = 0
    for idx_i, p in enumerate(partants):
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        cheval = (p.get("nom_cheval") or "").upper().strip()
        course_uid = p.get("course_uid") or p.get("id_course") or ""

        geny = geny_idx.get((date_iso, cheval))

        feat = {}
        has_data = False

        if geny:
            has_data = True

            # --- Geny pronostic note ---
            note = geny.get("note") or geny.get("geny_note") or geny.get("rating")
            try:
                note = float(note) if note else None
            except (ValueError, TypeError):
                note = None
            feat["geny_note"] = note

            # --- Geny selection / tip ---
            is_sel = geny.get("selection") or geny.get("is_selection") or geny.get("pronostic")
            feat["geny_is_selection"] = bool(is_sel)

            # --- Number of tipsters selecting this horse ---
            nb_tips = geny.get("nb_tipsters") or geny.get("nb_selections")
            try:
                nb_tips = int(nb_tips) if nb_tips else None
            except (ValueError, TypeError):
                nb_tips = None
            feat["geny_nb_tipsters"] = nb_tips

            # --- Comment sentiment ---
            comment = geny.get("commentaire") or geny.get("comment") or geny.get("avis") or ""
            feat["geny_comment_score"] = _comment_score(comment)

            # --- Geny odds ---
            geny_cote = geny.get("cote") or geny.get("odds")
            try:
                geny_cote = float(geny_cote) if geny_cote else None
            except (ValueError, TypeError):
                geny_cote = None
            feat["geny_cote"] = geny_cote

            # --- Geny star rating (1-5 stars) ---
            stars = geny.get("etoiles") or geny.get("stars")
            try:
                stars = int(stars) if stars else None
            except (ValueError, TypeError):
                stars = None
            feat["geny_stars"] = stars

            # --- Geny form analysis ---
            forme = geny.get("forme") or geny.get("form")
            feat["geny_forme_label"] = forme

        # --- Consensus features (even if no direct geny record) ---
        race_key = f"{date_iso}|{course_uid}"
        cons = consensus.get((race_key, cheval))
        if cons:
            has_data = True
            feat["geny_consensus_rank"] = cons["consensus_rank"]
            feat["geny_consensus_tips"] = cons["consensus_tips"]
            feat["geny_consensus_pct"] = cons["consensus_pct"]
            feat["geny_is_top_pick"] = cons["consensus_rank"] == 1

        if has_data:
            enriched += 1
            p.update(feat)

        if (idx_i + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx_i + 1, len(partants), enriched)

    logger.info("Features Geny: %d/%d enrichis (%.1f%%)",
                enriched, len(partants), 100 * enriched / max(len(partants), 1))
    return partants

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Features from Geny data (output/26)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--geny-data", default=GENY_DEFAULT, help="Geny data JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging("geny_builder")
    logger.info("=" * 70)
    logger.info("geny_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    geny_data = load_json_or_jsonl(args.geny_data, logger)
    geny_idx = index_geny_data(geny_data, logger)
    consensus = build_race_consensus(geny_data, logger)

    results = build_geny_features(partants, geny_idx, consensus, logger)

    out_path = os.path.join(args.output_dir, "geny_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants", len(results))


if __name__ == "__main__":
    main()
