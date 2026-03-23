#!/usr/bin/env python3
"""
feature_builders.pedigree_distance_aptitude
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Computes 6 advanced pedigree distance/terrain aptitude features:

1. sire_win_rate_distance   - father's win rate at this distance category
2. sire_win_rate_terrain    - father's win rate on this terrain type
3. dam_sire_win_rate        - maternal grandfather's overall win rate
4. inbreeding_coefficient   - overlap between sire and dam ancestry lines
5. stamina_index            - estimated stamina from avg winning distance of sire + dam_sire
6. speed_index              - estimated speed (inverse of stamina)

Data sources:
- pedigree_master.json  : ancestry data (pere, mere, pere_mere per horse)
- partants_master.jsonl : race results (distance, terrain, position, etc.)

Temporal integrity: pedigree data is static (genetic), and race-based
accumulations use strict point-in-time (date < current).

RAM budget: streams partants_master.jsonl line-by-line, builds compact
lookup dicts. Target < 3 GB peak for ~2M records.

Usage:
    python feature_builders/pedigree_distance_aptitude.py
    python feature_builders/pedigree_distance_aptitude.py --output-dir output/pedigree_dist_apt
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
from utils.logging_setup import setup_logging
from utils.math import safe_rate
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

PEDIGREE_MASTER = os.path.join("data_master", "pedigree_master.json")
PARTANTS_MASTER = os.path.join("data_master", "partants_master.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "pedigree_distance_aptitude")

# Distance category thresholds (meters)
_DISTANCE_CATEGORIES = [
    (0, 1300, "sprint"),
    (1300, 1900, "mile"),
    (1900, 2500, "intermediate"),
    (2500, 99999, "staying"),
]

# Terrain normalization mapping
_TERRAIN_MAP = {
    "bon": "bon",
    "souple": "souple",
    "lourd": "lourd",
    "tres_lourd": "lourd",       # merge tres_lourd into lourd
    "collant": "lourd",
    "leger": "bon",
    "assez_souple": "souple",
    "tres_souple": "souple",
}


# ===========================================================================
# HELPERS
# ===========================================================================

def _norm_name(name: object) -> Optional[str]:
    """Normalize an ancestor name to uppercase, or None."""
    if not name:
        return None
    n = str(name).strip().upper()
    return n if len(n) >= 2 else None


def _distance_category(distance_m: object) -> Optional[str]:
    """Classify distance into sprint / mile / intermediate / staying."""
    try:
        d = int(distance_m)
    except (TypeError, ValueError):
        return None
    for lo, hi, label in _DISTANCE_CATEGORIES:
        if lo <= d < hi:
            return label
    return None


def _normalize_terrain(partant: dict) -> Optional[str]:
    """Extract and normalize terrain from a partant record."""
    # Try met_terrain_predit first (most reliable), then cnd_cond_type_terrain
    raw = partant.get("met_terrain_predit") or partant.get("cnd_cond_type_terrain") or ""
    raw = str(raw).strip().lower()
    return _TERRAIN_MAP.get(raw)


def _get_classement(p: dict) -> Optional[int]:
    """Extract finishing position from a partant record."""
    for key in ("position_arrivee", "classement", "arrivee", "place"):
        v = p.get(key)
        if v is not None:
            try:
                return int(v)
            except (ValueError, TypeError):
                pass
    return None


# ===========================================================================
# PEDIGREE INDEX BUILDER
# ===========================================================================

def _build_pedigree_index(pedigree_path: str, logger: logging.Logger) -> dict:
    """Build horse_name_upper -> {pere, mere, pere_mere} from pedigree_master.

    Returns a dict of dicts.  RAM-efficient: only stores names.
    """
    logger.info("Loading pedigree_master from %s ...", pedigree_path)
    index: dict[str, dict[str, Optional[str]]] = {}

    # Try utf-8 first, fall back to latin-1
    for enc in ("utf-8", "latin-1"):
        try:
            with open(pedigree_path, "r", encoding=enc) as f:
                data = json.load(f)
            break
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    else:
        logger.warning("Cannot read pedigree_master at %s", pedigree_path)
        return index

    for rec in data:
        nom = _norm_name(rec.get("nom"))
        if not nom:
            continue
        pere = _norm_name(rec.get("pere"))
        mere = _norm_name(rec.get("mere"))
        pere_mere = _norm_name(rec.get("pere_mere"))
        if pere or mere or pere_mere:
            index[nom] = {
                "pere": pere,
                "mere": mere,
                "pere_mere": pere_mere,
            }

    # Free the raw list
    del data
    logger.info("Pedigree index: %d horses with ancestry data", len(index))
    return index


# ===========================================================================
# ACCUMULATOR TYPES  (compact, to keep RAM low)
# ===========================================================================

def _new_stats() -> dict:
    return {"total": 0, "wins": 0}


def _new_dist_stats() -> dict:
    """Per distance-category stats for a sire."""
    return defaultdict(_new_stats)


def _new_terrain_stats() -> dict:
    """Per terrain stats for a sire."""
    return defaultdict(_new_stats)


def _new_win_distances() -> list:
    """List of winning distances for stamina index computation."""
    return []


# ===========================================================================
# MAIN BUILDER
# ===========================================================================

def build_pedigree_distance_aptitude(
    pedigree_path: str,
    partants_path: str,
    logger: logging.Logger,
) -> list[dict]:
    """Build 6 pedigree distance/terrain aptitude features.

    Two-pass approach:
      Pass 1: stream partants_master.jsonl, accumulate per-sire stats
              by distance category and terrain, and per-dam_sire overall stats.
              Also collect winning distances for stamina index.
      Pass 2: stream again, emit features per partant using point-in-time
              accumulated stats.

    Actually we do a single-pass with point-in-time accumulation
    (same pattern as pedigree_advanced_builder): for each partant sorted
    chronologically, read accumulated stats THEN update accumulators.
    """
    # Load pedigree index (horse -> ancestry)
    ped_index = _build_pedigree_index(pedigree_path, logger)

    # ---------- Load and sort partants ----------
    logger.info("Loading partants from %s ...", partants_path)
    partants: list[dict] = []
    with open(partants_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                partants.append(json.loads(line))
    logger.info("Loaded %d partants", len(partants))

    partants.sort(key=lambda p: (
        p.get("date_reunion_iso", ""),
        p.get("course_uid", ""),
        p.get("num_pmu", 0),
    ))

    # ---------- Accumulators ----------
    # sire -> distance_cat -> {total, wins}
    sire_dist_stats: dict[str, dict[str, dict]] = defaultdict(_new_dist_stats)
    # sire -> terrain -> {total, wins}
    sire_terrain_stats: dict[str, dict[str, dict]] = defaultdict(_new_terrain_stats)
    # dam_sire -> {total, wins}
    dam_sire_stats: dict[str, dict] = defaultdict(_new_stats)
    # sire -> [winning distances]  (for stamina index)
    sire_win_dists: dict[str, list[int]] = defaultdict(list)
    # dam_sire -> [winning distances]
    dam_sire_win_dists: dict[str, list[int]] = defaultdict(list)

    # ---------- Single pass ----------
    results: list[dict] = []
    enriched = 0

    for idx, p in enumerate(partants):
        uid = p.get("partant_uid")
        cheval = _norm_name(p.get("nom_cheval"))

        # Resolve ancestry: first from partant record, then from pedigree_master
        pere = _norm_name(p.get("pere") or p.get("nom_pere") or p.get("sire"))
        mere = _norm_name(p.get("mere") or p.get("nom_mere") or p.get("dam"))
        pere_mere = _norm_name(
            p.get("pere_mere") or p.get("dam_sire") or p.get("broodmare_sire")
        )

        # Enrich from pedigree_master if missing
        if cheval and cheval in ped_index:
            ped = ped_index[cheval]
            if not pere:
                pere = ped.get("pere")
            if not mere:
                mere = ped.get("mere")
            if not pere_mere:
                pere_mere = ped.get("pere_mere")

        dist_cat = _distance_category(p.get("distance"))
        terrain = _normalize_terrain(p)
        distance_m = None
        try:
            distance_m = int(p.get("distance"))
        except (TypeError, ValueError):
            pass

        feat: dict[str, object] = {"partant_uid": uid}

        has_any = False

        # --- Feature 1: sire_win_rate_distance ---
        if pere and dist_cat:
            s = sire_dist_stats[pere].get(dist_cat)
            if s and s["total"] > 0:
                feat["sire_win_rate_distance"] = safe_rate(
                    s["wins"], s["total"], ndigits=4
                )
                feat["sire_nb_at_distance"] = s["total"]
                has_any = True

        # --- Feature 2: sire_win_rate_terrain ---
        if pere and terrain:
            s = sire_terrain_stats[pere].get(terrain)
            if s and s["total"] > 0:
                feat["sire_win_rate_terrain"] = safe_rate(
                    s["wins"], s["total"], ndigits=4
                )
                feat["sire_nb_on_terrain"] = s["total"]
                has_any = True

        # --- Feature 3: dam_sire_win_rate ---
        if pere_mere:
            s = dam_sire_stats[pere_mere]
            if s["total"] > 0:
                feat["dam_sire_win_rate"] = safe_rate(
                    s["wins"], s["total"], ndigits=4
                )
                feat["dam_sire_nb_offspring"] = s["total"]
                has_any = True

        # --- Feature 4: inbreeding_coefficient ---
        if cheval and cheval in ped_index:
            # Collect all ancestors from sire line and dam line
            sire_ancestors = set()
            dam_ancestors = set()

            if pere:
                sire_ancestors.add(pere)
                # sire's parents
                if pere in ped_index:
                    pp = ped_index[pere]
                    if pp.get("pere"):
                        sire_ancestors.add(pp["pere"])
                    if pp.get("mere"):
                        sire_ancestors.add(pp["mere"])
                    if pp.get("pere_mere"):
                        sire_ancestors.add(pp["pere_mere"])

            if mere:
                dam_ancestors.add(mere)
                if pere_mere:
                    dam_ancestors.add(pere_mere)
                # dam's parents
                if mere in ped_index:
                    mp = ped_index[mere]
                    if mp.get("pere"):
                        dam_ancestors.add(mp["pere"])
                    if mp.get("mere"):
                        dam_ancestors.add(mp["mere"])
                    if mp.get("pere_mere"):
                        dam_ancestors.add(mp["pere_mere"])

            common = sire_ancestors & dam_ancestors
            total_unique = len(sire_ancestors | dam_ancestors)
            if total_unique > 0:
                feat["inbreeding_coefficient"] = round(
                    len(common) / total_unique, 4
                )
                feat["inbreeding_common_ancestors"] = len(common)
                has_any = True
        elif pere or pere_mere:
            # Minimal inbreeding check (same as pedigree_advanced_builder)
            ancestors_seen: set[str] = set()
            common_count = 0
            for anc in [pere, mere, pere_mere]:
                if anc:
                    if anc in ancestors_seen:
                        common_count += 1
                    ancestors_seen.add(anc)
            if ancestors_seen:
                feat["inbreeding_coefficient"] = round(
                    common_count / len(ancestors_seen), 4
                )
                feat["inbreeding_common_ancestors"] = common_count
                has_any = True

        # --- Feature 5: stamina_index ---
        # Average winning distance of sire's offspring + dam_sire's offspring
        sire_avg_win_d = None
        ds_avg_win_d = None
        if pere and sire_win_dists.get(pere):
            dists = sire_win_dists[pere]
            sire_avg_win_d = sum(dists) / len(dists)
        if pere_mere and dam_sire_win_dists.get(pere_mere):
            dists = dam_sire_win_dists[pere_mere]
            ds_avg_win_d = sum(dists) / len(dists)

        if sire_avg_win_d is not None or ds_avg_win_d is not None:
            components = [d for d in [sire_avg_win_d, ds_avg_win_d] if d is not None]
            avg_win_dist = sum(components) / len(components)
            # Normalize: 1000m -> 0.0, 4000m -> 1.0
            stamina = max(0.0, min(1.0, (avg_win_dist - 1000) / 3000))
            feat["stamina_index"] = round(stamina, 4)
            feat["speed_index"] = round(1.0 - stamina, 4)
            feat["avg_winning_distance_ancestry"] = round(avg_win_dist, 0)
            has_any = True

        if has_any:
            enriched += 1

        results.append(feat)

        # ---------- Update accumulators ----------
        classement = _get_classement(p)
        is_win = classement == 1

        if pere:
            if dist_cat:
                bucket = sire_dist_stats[pere][dist_cat]
                bucket["total"] += 1
                if is_win:
                    bucket["wins"] += 1
            if terrain:
                bucket = sire_terrain_stats[pere][terrain]
                bucket["total"] += 1
                if is_win:
                    bucket["wins"] += 1
            if is_win and distance_m is not None:
                sire_win_dists[pere].append(distance_m)

        if pere_mere:
            dam_sire_stats[pere_mere]["total"] += 1
            if is_win:
                dam_sire_stats[pere_mere]["wins"] += 1
            if is_win and distance_m is not None:
                dam_sire_win_dists[pere_mere].append(distance_m)

        if (idx + 1) % 200000 == 0:
            logger.info(
                "  %d/%d processed, %d enriched, %d sires tracked",
                idx + 1, len(partants), enriched, len(sire_dist_stats),
            )

    logger.info(
        "pedigree_distance_aptitude: %d/%d enriched (%.1f%%)",
        enriched, len(partants), 100 * enriched / max(len(partants), 1),
    )
    return results


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pedigree distance/terrain aptitude features"
    )
    parser.add_argument("--pedigree", default=PEDIGREE_MASTER,
                        help="Path to pedigree_master.json")
    parser.add_argument("--partants", default=PARTANTS_MASTER,
                        help="Path to partants_master.jsonl")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    logger = setup_logging("pedigree_distance_aptitude")
    logger.info("=" * 70)
    logger.info("pedigree_distance_aptitude.py")
    logger.info("=" * 70)

    results = build_pedigree_distance_aptitude(
        args.pedigree, args.partants, logger
    )

    out_path = os.path.join(args.output_dir, "pedigree_distance_aptitude.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Done -- %d partants written to %s", len(results), out_path)


if __name__ == "__main__":
    main()
