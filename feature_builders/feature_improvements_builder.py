#!/usr/bin/env python3
"""
feature_builders.feature_improvements_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Data-driven feature improvements based on statistical analysis of the
features_matrix.jsonl (2.93M records, 411 columns).

Analysis performed on 1000-record sample revealed:
  - 88 features with 0% fill (completely empty)
  - 68 features that are constant (single value = zero variance)
  - 14 redundant pairs (|r| > 0.9)
  - 94 synergy pairs where A*B has higher target correlation than either alone
  - 190 ratio pairs with improved target correlation

This builder:
  1. Creates top 5 multiplicative synergy features
  2. Creates top 5 ratio features
  3. Flags/removes 0%-fill and constant features from records

Reads features_matrix.jsonl (streaming, <2GB RAM), outputs improved records to
output/feature_improvements/.

Usage:
    python feature_builders/feature_improvements_builder.py
    python feature_builders/feature_improvements_builder.py --input output/features/features_matrix.jsonl
"""

from __future__ import annotations

import argparse
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "output" / "features" / "features_matrix.jsonl",
    _PROJECT_ROOT / "output" / "features" / "features_matrix_clean.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "feature_improvements"

_LOG_EVERY = 500_000
_BATCH_SIZE = 50_000

# ===========================================================================
# FEATURES TO REMOVE (0% fill or constant value across 1000-record sample)
# ===========================================================================

ZERO_FILL_FEATURES = frozenset([
    "allocation_diff_vs_last", "allocation_rank_career",
    "allocation_ratio_vs_last", "allocation_x_forme",
    "avis_entraineur", "combo_jh_taux_vic", "combo_th_taux_vic",
    "commentaire_apres_course", "cote_finale", "cote_reference",
    "cote_x_nb_partants", "distance_change", "distance_change_pct",
    "eleveur", "ent_entraineur_nb_courses_total",
    "ent_jockey_nb_courses_total", "ent_mere_gains_moy_produit",
    "ent_mere_nb_produits", "ent_mere_taux_victoire",
    "ent_pere_gains_moy_produit", "ent_pere_nb_produits",
    "ent_pere_taux_victoire", "equip_deferre_added",
    "equip_deferre_change", "equip_deferre_removed",
    "equip_nb_courses_with_oeilleres", "equip_nb_oeilleres_changes_5",
    "equip_oeilleres_added", "equip_oeilleres_change",
    "equip_oeilleres_removed", "forme_x_cote", "forme_x_terrain",
    "gains_annee_euros", "gains_carriere_euros", "handicap_valeur",
    "is_class_down", "is_class_up", "is_discipline_change",
    "is_favori_x_forme", "is_hippodrome_change",
    "jockey_taux_x_cheval_taux", "jours_depuis_derniere",
    "mti_precip_x_terrain", "mti_temp_x_terrain", "mti_terrain_score",
    "mti_wind_category", "nb_places_2eme", "nb_places_3eme",
    "pays_entrainement", "pc_cote_mediane_course",
    "pc_cote_moyenne_course", "pc_deferre_prev", "pc_ecart_cote_moyenne",
    "pc_ecart_poids", "pc_handicap_valeur",
    "pc_nb_courses_sans_oeilleres", "pc_oeilleres_prev",
    "pc_poids_precedent", "pc_sectional_200m", "pc_sectional_400m",
    "pc_sectional_600m", "pc_sectional_rank",
    "ped_damsire_stamina_idx", "ped_sire_precocity_idx",
    "poids_avg_career", "poids_change_vs_avg", "poids_change_vs_last",
    "poids_is_heaviest", "poids_is_lightest", "poids_max_career",
    "poids_min_career", "proba_implicite", "profil_gains_annee_log",
    "profil_gains_carriere_log", "profil_gains_par_course",
    "rest_x_forme", "temps_avg_reduction_10", "temps_avg_reduction_5",
    "temps_best_reduction_10", "temps_best_reduction_5",
    "temps_reduction_trend", "temps_speed_consistency",
    "vb_cote_finale", "vb_log_proba", "vb_proba_implicite",
    "vb_proba_normalisee", "vb_rang_cote", "vb_rang_cote_pct",
])

CONSTANT_FEATURES = frozenset([
    "cnd_cond_is_international", "cnd_cond_is_quinte",
    "combo_jh_nb", "combo_th_nb", "engagement",
    "equip_deferre_code", "equip_deferre_type", "equip_has_deferre",
    "equip_has_oeilleres", "equip_oeilleres_code", "equip_oeilleres_type",
    "equip_poids_monte_change", "gnn_cheval_degree", "gnn_premier_hippo",
    "gnn_premier_jockey", "is_inedit", "jockey_driver_change",
    "jument_pleine", "met_impact_meteo_score", "met_is_psf",
    "pc_retrait_oeilleres", "poids_monte_change",
    "profil_engagement", "profil_is_inedit", "profil_jument_pleine",
    "seq_nb_courses_historique", "seq_nb_places_recent_5",
    "seq_nb_victoires_recent_5", "seq_serie_non_places",
    "seq_serie_places", "seq_serie_victoires", "source",
    "supplement_euros", "timestamp_collecte", "vb_is_favori",
])

# Features with near-perfect correlation (>0.9) -- keep first, flag second
REDUNDANT_PAIRS = [
    # (keep, remove, correlation)
    ("nb_courses_carriere", "profil_nb_courses_carriere", 1.0),
    ("distance", "rap_distance", 1.0),
    ("numero_course", "rap_numero_course", 1.0),
    ("numero_course", "rap_num_course", 1.0),
    ("age", "profil_age", 1.0),
    ("numero_reunion", "rap_numero_reunion", 1.0),
]
REDUNDANT_TO_REMOVE = frozenset(r[1] for r in REDUNDANT_PAIRS)

# All features to strip from output
ALL_REMOVE = ZERO_FILL_FEATURES | CONSTANT_FEATURES | REDUNDANT_TO_REMOVE

# ===========================================================================
# SYNERGY FEATURES (multiplicative: A * B)
# Discovered via pairwise correlation with is_gagnant target
# ===========================================================================

SYNERGY_FEATURES = [
    # (name, feature_a, feature_b, r_product, synergy_gain)
    (
        "syn_ct_courses_x_breed",
        "aff_ct_nb_courses", "profil_race_breed_encoded",
        0.0623, 0.0581,
    ),
    (
        "syn_eh_place_rate_x_dam_win",
        "aff_eh_taux_place", "ped_dam_win_rate",
        0.2502, 0.0557,
    ),
    (
        "syn_eh_win_rate_x_dam_win",
        "aff_eh_taux_vic", "ped_dam_win_rate",
        0.3004, 0.0487,
    ),
    (
        "syn_eh_win_rate_x_sire_place",
        "aff_eh_taux_vic", "ped_sire_place_rate",
        0.3996, 0.0409,
    ),
    (
        "syn_sire_place_x_dam_win",
        "ped_sire_place_rate", "ped_dam_win_rate",
        0.3899, 0.0313,
    ),
]

# ===========================================================================
# RATIO FEATURES (A / B)
# High synergy with target when expressed as ratio
# ===========================================================================

RATIO_FEATURES = [
    # (name, numerator, denominator, r_ratio, synergy_gain)
    (
        "ratio_wins_per_jt_combo",
        "nb_victoires_carriere", "combo_jt_nb",
        0.3013, 0.2636,
    ),
    (
        "ratio_eh_courses_per_wins",
        "aff_eh_nb_courses", "aff_eh_victoires",
        -0.1377, 0.0920,
    ),
    (
        "ratio_cd_courses_per_jt",
        "aff_cd_nb_courses", "combo_jt_nb",
        -0.1160, 0.0854,
    ),
    (
        "ratio_eh_places_per_wins",
        "aff_eh_places", "aff_eh_victoires",
        -0.1228, 0.0744,
    ),
    (
        "ratio_career_courses_per_jt",
        "nb_courses_carriere", "combo_jt_nb",
        0.1906, 0.1125,
    ),
]

# ===========================================================================
# STREAMING READER
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
                    logger.warning("Malformed JSON line ignored (error %d)", errors)
    logger.info("Read complete: %d records, %d JSON errors", count, errors)


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (ValueError, TypeError):
        return None


def compute_improvements(rec: dict) -> dict:
    """Compute improved features for a single record.

    Returns a dict with:
      - cle_partant (join key)
      - 5 synergy features (multiplicative)
      - 5 ratio features
      - _removed_features_count (how many 0%/constant/redundant fields exist)
    """
    result = {}

    # Join key
    result["cle_partant"] = rec.get("cle_partant", "")

    # Synergy features: A * B
    for feat_name, feat_a, feat_b, _r, _syn in SYNERGY_FEATURES:
        va = _safe_float(rec.get(feat_a))
        vb = _safe_float(rec.get(feat_b))
        if va is not None and vb is not None:
            result[feat_name] = round(va * vb, 6)
        else:
            result[feat_name] = None

    # Ratio features: A / B (with zero-division guard)
    for feat_name, num_key, den_key, _r, _syn in RATIO_FEATURES:
        vn = _safe_float(rec.get(num_key))
        vd = _safe_float(rec.get(den_key))
        if vn is not None and vd is not None and abs(vd) > 1e-10:
            result[feat_name] = round(vn / vd, 6)
        else:
            result[feat_name] = None

    # Count removable fields present in this record
    removable_present = sum(1 for k in rec if k in ALL_REMOVE)
    result["_removed_features_count"] = removable_present
    result["_flagged_zero_fill"] = removable_present > 0

    return result


def strip_useless_features(rec: dict) -> dict:
    """Return a copy of rec with all 0%-fill, constant, and redundant
    features removed. Use this when producing the cleaned matrix."""
    return {k: v for k, v in rec.items() if k not in ALL_REMOVE}


# ===========================================================================
# MAIN
# ===========================================================================


def main():
    logger = setup_logging("feature_improvements_builder")

    parser = argparse.ArgumentParser(
        description="Feature improvements: synergy, ratio, and cleanup"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Path to features_matrix.jsonl (auto-detected if omitted)"
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Also produce a cleaned matrix with useless features stripped"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Process only first N records (0 = all)"
    )
    args = parser.parse_args()

    # Find input
    input_path = None
    if args.input:
        input_path = Path(args.input)
    else:
        for cand in INPUT_CANDIDATES:
            if cand.exists():
                input_path = cand
                break
    if input_path is None or not input_path.exists():
        logger.error("No input file found. Tried: %s", INPUT_CANDIDATES)
        sys.exit(1)

    logger.info("Input: %s", input_path)
    logger.info("Output: %s", OUTPUT_DIR)
    logger.info("Synergy features: %d", len(SYNERGY_FEATURES))
    logger.info("Ratio features: %d", len(RATIO_FEATURES))
    logger.info("Features flagged for removal: %d (0%%: %d, constant: %d, redundant: %d)",
                len(ALL_REMOVE), len(ZERO_FILL_FEATURES),
                len(CONSTANT_FEATURES), len(REDUNDANT_TO_REMOVE))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process in streaming mode with batched writes
    t0 = time.time()
    batch = []
    clean_batch = []
    total = 0
    improvements_file = OUTPUT_DIR / "feature_improvements.jsonl"
    clean_file = OUTPUT_DIR / "features_matrix_improved.jsonl"

    # Clear output files
    improvements_file.write_text("")
    if args.clean:
        clean_file.write_text("")

    for rec in _iter_jsonl(input_path, logger):
        if args.limit and total >= args.limit:
            break

        improved = compute_improvements(rec)
        batch.append(improved)

        if args.clean:
            cleaned = strip_useless_features(rec)
            # Add synergy/ratio features to cleaned record
            for k, v in improved.items():
                if k.startswith(("syn_", "ratio_")):
                    cleaned[k] = v
            clean_batch.append(cleaned)

        total += 1

        if len(batch) >= _BATCH_SIZE:
            _flush_batch(batch, improvements_file)
            batch = []
            if args.clean:
                _flush_batch(clean_batch, clean_file)
                clean_batch = []

        if total % _LOG_EVERY == 0:
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0
            logger.info("Processed %d records (%.0f rec/s)", total, rate)

    # Final flush
    if batch:
        _flush_batch(batch, improvements_file)
    if args.clean and clean_batch:
        _flush_batch(clean_batch, clean_file)

    elapsed = time.time() - t0
    logger.info("Done: %d records in %.1fs (%.0f rec/s)",
                total, elapsed, total / elapsed if elapsed > 0 else 0)
    logger.info("Output: %s", improvements_file)
    if args.clean:
        logger.info("Cleaned matrix: %s", clean_file)

    # Summary report
    report = {
        "total_records": total,
        "synergy_features_added": len(SYNERGY_FEATURES),
        "ratio_features_added": len(RATIO_FEATURES),
        "features_flagged_for_removal": len(ALL_REMOVE),
        "zero_fill_count": len(ZERO_FILL_FEATURES),
        "constant_count": len(CONSTANT_FEATURES),
        "redundant_count": len(REDUNDANT_TO_REMOVE),
        "elapsed_seconds": round(elapsed, 1),
    }
    report_path = OUTPUT_DIR / "improvement_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Report: %s", report_path)


def _flush_batch(batch: list[dict], filepath: Path):
    """Append a batch of records to a JSONL file."""
    with open(filepath, "a", encoding="utf-8") as f:
        for rec in batch:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
