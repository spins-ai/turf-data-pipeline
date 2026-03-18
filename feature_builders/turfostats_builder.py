#!/usr/bin/env python3
"""
feature_builders.turfostats_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
10-15 features from TurfoStats data (output/25).

Key race index, racing style, distance affinity from TurfoStats analysis.

Temporal integrity: TurfoStats data matched by date + horse. Only pre-race
published statistics are used.

Usage:
    python feature_builders/turfostats_builder.py
    python feature_builders/turfostats_builder.py --ts-data output/25_turfostats/turfostats.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Optional

# ===========================================================================
# CONFIG
# ===========================================================================

TS_DEFAULT = os.path.join("output", "25_turfostats", "turfostats.jsonl")
PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "turfostats_features")
LOG_DIR = os.path.join("logs")

# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("turfostats_builder")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    os.makedirs(LOG_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(LOG_DIR, "turfostats_builder.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

# ===========================================================================
# HELPERS
# ===========================================================================

def load_json_or_jsonl(path: str, logger: logging.Logger) -> list:
    if path.endswith(".jsonl") and os.path.exists(path):
        records = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        logger.info("Charge %d depuis %s", len(records), path)
        return records
    jsonl_path = path.replace(".json", ".jsonl")
    if os.path.exists(jsonl_path):
        return load_json_or_jsonl(jsonl_path, logger)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("Charge %d depuis %s", len(data), path)
        return data
    logger.error("Fichier introuvable: %s", path)
    sys.exit(1)

# ===========================================================================
# INDEX TS DATA
# ===========================================================================

def index_ts_data(ts_records: list, logger: logging.Logger) -> dict:
    """Index TurfoStats data by (date, horse_name_norm)."""
    idx = {}
    for rec in ts_records:
        date = str(rec.get("date", "") or rec.get("date_reunion_iso", "") or "")[:10]
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        if date and horse:
            idx[(date, horse)] = rec
    logger.info("Index TurfoStats: %d entrees", len(idx))
    return idx

# ===========================================================================
# BUILDER
# ===========================================================================

def build_turfostats_features(partants: list, ts_idx: dict, logger: logging.Logger) -> list:
    """Build 10-15 features from TurfoStats data."""

    enriched = 0
    for idx_i, p in enumerate(partants):
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        cheval = (p.get("nom_cheval") or "").upper().strip()

        ts = ts_idx.get((date_iso, cheval))
        if not ts:
            continue

        enriched += 1
        feat = {}

        # --- Key Race Index (indice course cle) ---
        kcr = ts.get("indice_course_cle") or ts.get("key_race_index") or ts.get("kri")
        try:
            kcr = float(kcr) if kcr else None
        except (ValueError, TypeError):
            kcr = None
        feat["ts_key_race_index"] = kcr

        # --- Racing style ---
        style = ts.get("style_course") or ts.get("racing_style") or ts.get("style")
        feat["ts_racing_style"] = style
        if style:
            style_l = str(style).lower()
            feat["ts_is_front_runner"] = "devant" in style_l or "leader" in style_l or "front" in style_l
            feat["ts_is_closer"] = "arriere" in style_l or "finisseur" in style_l or "closer" in style_l
        else:
            feat["ts_is_front_runner"] = None
            feat["ts_is_closer"] = None

        # --- Distance affinity ---
        dist_affinity = ts.get("affinite_distance") or ts.get("distance_affinity")
        try:
            dist_affinity = float(dist_affinity) if dist_affinity else None
        except (ValueError, TypeError):
            dist_affinity = None
        feat["ts_distance_affinity"] = dist_affinity

        optimal_dist = ts.get("distance_optimale") or ts.get("optimal_distance")
        try:
            optimal_dist = int(optimal_dist) if optimal_dist else None
        except (ValueError, TypeError):
            optimal_dist = None
        feat["ts_optimal_distance"] = optimal_dist

        # Gap to optimal distance
        course_dist = p.get("distance") or p.get("rapport_distance_m")
        try:
            course_dist = int(course_dist) if course_dist else None
        except (ValueError, TypeError):
            course_dist = None
        if optimal_dist and course_dist:
            feat["ts_dist_gap_to_optimal"] = abs(course_dist - optimal_dist)
            feat["ts_dist_gap_pct"] = round(abs(course_dist - optimal_dist) / optimal_dist, 4)
        else:
            feat["ts_dist_gap_to_optimal"] = None
            feat["ts_dist_gap_pct"] = None

        # --- TurfoStats rating / note ---
        ts_note = ts.get("note") or ts.get("ts_note") or ts.get("rating")
        try:
            ts_note = float(ts_note) if ts_note else None
        except (ValueError, TypeError):
            ts_note = None
        feat["ts_note"] = ts_note

        # --- TurfoStats form indicator ---
        forme = ts.get("forme") or ts.get("form_indicator")
        try:
            forme = float(forme) if forme else None
        except (ValueError, TypeError):
            forme = None
        feat["ts_forme"] = forme

        # --- TurfoStats terrain preference ---
        terrain_pref = ts.get("terrain_preference") or ts.get("pref_terrain")
        feat["ts_terrain_pref"] = terrain_pref

        # Match terrain preference to current going
        going = p.get("meteo_terrain_category") or p.get("terrain_category")
        if terrain_pref and going:
            feat["ts_terrain_match"] = str(terrain_pref).lower() == str(going).lower()
        else:
            feat["ts_terrain_match"] = None

        # --- TurfoStats class level ---
        classe = ts.get("classe") or ts.get("class_level")
        try:
            classe = int(classe) if classe else None
        except (ValueError, TypeError):
            classe = None
        feat["ts_class_level"] = classe

        # --- TurfoStats regularity ---
        regularite = ts.get("regularite") or ts.get("consistency")
        try:
            regularite = float(regularite) if regularite else None
        except (ValueError, TypeError):
            regularite = None
        feat["ts_regularity"] = regularite

        p.update(feat)

        if (idx_i + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx_i + 1, len(partants), enriched)

    logger.info("Features TurfoStats: %d/%d enrichis (%.1f%%)",
                enriched, len(partants), 100 * enriched / max(len(partants), 1))
    return partants

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Features from TurfoStats data (output/25)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--ts-data", default=TS_DEFAULT, help="TurfoStats data JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("turfostats_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    ts_data = load_json_or_jsonl(args.ts_data, logger)
    ts_idx = index_ts_data(ts_data, logger)

    results = build_turfostats_features(partants, ts_idx, logger)

    out_path = os.path.join(args.output_dir, "turfostats_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants", len(results))


if __name__ == "__main__":
    main()
