"""
feature_builders.master_feature_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Orchestrates all feature builders and produces the final feature matrix.

CLI usage:
    python3 -m feature_builders.master_feature_builder
    python3 -m feature_builders.master_feature_builder --partants path --courses path --output dir
"""

from __future__ import annotations

import argparse
import json
import csv
import logging
import os
import sys
import time
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from utils.logging_setup import setup_logging

from .cheval_features import build_cheval_features
from .jockey_features import build_jockey_features, build_entraineur_features
from .course_features import build_course_features
from .marche_features import build_marche_features
from .pedigree_features import build_pedigree_features
from .meteo_features import build_meteo_features
from .equipement_features import build_equipement_features
from .poids_features import build_poids_features
from .musique_features import build_musique_features
from .temps_features import build_temps_features
from .profil_cheval_features import build_profil_cheval_features
from .field_strength_builder import build_field_strength_features
from .pace_profile_builder import build_pace_profiles
from .track_bias_detector import build_track_bias_features
from .precomputed_partant_joiner import build_precomputed_partant_features
from .precomputed_entity_joiner import build_precomputed_entity_features
from .combo_features import build_combo_features
from .class_change_features import build_class_change_features
from .interaction_features import build_interaction_features


def _merge_features(base: list[dict], *feature_lists: list[dict]) -> list[dict]:
    """Merge feature dicts onto base partant list by partant_uid (left join).

    Parameters
    ----------
    base : list[dict]
        Base partant records (defines the row set and order).
    *feature_lists : list[dict]
        Each is a list of dicts with 'partant_uid' + feature columns.

    Returns
    -------
    list[dict]
        Merged records.
    """
    # Build lookup per feature list
    lookups = []
    for fl in feature_lists:
        lookup: dict[str, dict] = {}
        for row in fl:
            uid = row.get("partant_uid")
            if uid:
                lookup[uid] = row
        lookups.append(lookup)

    merged = []
    for p in base:
        uid = p.get("partant_uid")
        row = {"partant_uid": uid}
        # Add base identifiers useful in the output
        for key in ("course_uid", "date_reunion_iso", "nom_cheval", "jockey_driver",
                     "entraineur", "hippodrome_normalise", "distance", "discipline",
                     "position_arrivee", "is_gagnant", "is_place", "cote_finale"):
            if key in p:
                row[key] = p[key]
        # Merge features from each builder
        for lookup in lookups:
            feat = lookup.get(uid, {})
            for k, v in feat.items():
                if k != "partant_uid":
                    row[k] = v
        merged.append(row)

    return merged


def _fill_rates(records: list[dict], exclude_keys: set[str] | None = None) -> dict[str, float]:
    """Compute fill rate (non-None) for each key."""
    if not records:
        return {}
    exclude = exclude_keys or set()
    rates = {}
    n = len(records)
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())
    for k in sorted(all_keys):
        if k in exclude:
            continue
        filled = sum(1 for r in records if r.get(k) is not None)
        rates[k] = filled / n
    return rates


def _save_json(records: list[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    print(f"  -> JSON saved: {path} ({os.path.getsize(path) / 1_048_576:.1f} MB)")


def _save_csv(records: list[dict], path: str) -> None:
    if not records:
        return
    fieldnames = list(records[0].keys())
    # Collect any extra keys from other records
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())
    for k in sorted(all_keys):
        if k not in fieldnames:
            fieldnames.append(k)

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    print(f"  -> CSV saved: {path} ({os.path.getsize(path) / 1_048_576:.1f} MB)")


def _save_parquet(records: list[dict], path: str) -> bool:
    """Attempt to save as parquet. Returns True on success."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("  -> Parquet: pyarrow not installed, skipping .parquet output.")
        return False

    if not records:
        return False

    # Collect all keys
    all_keys = []
    seen = set()
    for r in records:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    # Build columnar data
    columns = {}
    for k in all_keys:
        columns[k] = [r.get(k) for r in records]

    table = pa.table(columns)
    pq.write_table(table, path)
    print(f"  -> Parquet saved: {path} ({os.path.getsize(path) / 1_048_576:.1f} MB)")
    return True


def build_all_features(
    partants_path: str,
    courses_path: str,
    output_dir: str,
    meteo_path: str | None = None,
) -> list[dict]:
    """Orchestrate all feature builders and produce the final feature matrix.

    Parameters
    ----------
    partants_path : str
        Path to partants_normalises.json.
    courses_path : str
        Path to courses_normalisees.json.
    output_dir : str
        Directory where output files will be saved.
    meteo_path : str, optional
        Path to meteo_historique.json. Auto-detected if None.

    Returns
    -------
    list[dict]
        The merged feature matrix.
    """
    t0 = time.time()
    logger = setup_logging("master_feature_builder")

    print("=" * 70)
    print("MASTER FEATURE BUILDER (enriched - 19 builders + interactions)")
    print("=" * 70)

    total_steps = 20

    # 1. Load data
    print(f"\n[1/{total_steps}] Loading data...")
    with open(partants_path, encoding="utf-8") as f:
        partants = json.load(f)
    with open(courses_path, encoding="utf-8") as f:
        courses = json.load(f)
    print(f"  Partants: {len(partants)}")
    print(f"  Courses:  {len(courses)}")

    # --- Original 6 builders ---

    print(f"\n[2/{total_steps}] Building cheval features...")
    t1 = time.time()
    cheval_feats = build_cheval_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s ({len(cheval_feats)} records)")

    print(f"\n[3/{total_steps}] Building jockey + entraineur features...")
    t1 = time.time()
    jockey_feats = build_jockey_features(partants)
    entraineur_feats = build_entraineur_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[4/{total_steps}] Building course features...")
    t1 = time.time()
    course_feats = build_course_features(partants, courses)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[5/{total_steps}] Building marche features...")
    t1 = time.time()
    marche_feats = build_marche_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[6/{total_steps}] Building pedigree features...")
    t1 = time.time()
    pedigree_feats = build_pedigree_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    # --- Existing builders not yet integrated ---

    print(f"\n[7/{total_steps}] Building field strength features...")
    t1 = time.time()
    field_feats = build_field_strength_features(partants, logger)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[8/{total_steps}] Building pace profile features...")
    t1 = time.time()
    pace_feats = build_pace_profiles(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[9/{total_steps}] Building track bias features...")
    t1 = time.time()
    bias_feats = build_track_bias_features(partants, courses)
    print(f"  Done in {time.time() - t1:.1f}s")

    # --- New builders ---

    print(f"\n[10/{total_steps}] Building meteo features...")
    t1 = time.time()
    meteo_feats = build_meteo_features(partants, meteo_path=meteo_path)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[11/{total_steps}] Building equipement features...")
    t1 = time.time()
    equip_feats = build_equipement_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[12/{total_steps}] Building poids features...")
    t1 = time.time()
    poids_feats = build_poids_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[13/{total_steps}] Building musique features...")
    t1 = time.time()
    musique_feats = build_musique_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[14/{total_steps}] Building temps + profil cheval features...")
    t1 = time.time()
    temps_feats = build_temps_features(partants)
    profil_feats = build_profil_cheval_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    # --- Pre-computed data joiners (scripts 05-11) ---

    print(f"\n[15/{total_steps}] Joining pre-computed partant data (scripts 07/09/10/11)...")
    t1 = time.time()
    pc_partant_feats = build_precomputed_partant_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    print(f"\n[16/{total_steps}] Joining pre-computed entity data (scripts 05/06/08)...")
    t1 = time.time()
    pc_entity_feats = build_precomputed_entity_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s")

    # --- Phase 3 builders ---

    print(f"\n[17/{total_steps}] Building combo features (jockey-trainer-horse)...")
    t1 = time.time()
    combo_feats = build_combo_features(partants)
    print(f"  Done in {time.time() - t1:.1f}s ({len(combo_feats)} records)")

    print(f"\n[18/{total_steps}] Building class change features...")
    t1 = time.time()
    class_change_feats = build_class_change_features(partants, courses)
    print(f"  Done in {time.time() - t1:.1f}s ({len(class_change_feats)} records)")

    # 3. Merge all features (18 feature lists, before interactions)
    print(f"\n[19/{total_steps}] Merging all features (pre-interaction)...")
    merged = _merge_features(
        partants,
        cheval_feats,
        jockey_feats,
        entraineur_feats,
        course_feats,
        marche_feats,
        pedigree_feats,
        field_feats,
        pace_feats,
        bias_feats,
        meteo_feats,
        equip_feats,
        poids_feats,
        musique_feats,
        temps_feats,
        profil_feats,
        pc_partant_feats,
        pc_entity_feats,
        combo_feats,
        class_change_feats,
    )
    print(f"  Pre-interaction merged: {len(merged)} records")

    # Interaction features operate on the already-merged matrix
    print(f"\n[20/{total_steps}] Building interaction features (cross-feature)...")
    t1 = time.time()
    interaction_feats = build_interaction_features(merged)
    print(f"  Done in {time.time() - t1:.1f}s ({len(interaction_feats)} records)")

    # Merge interaction features onto the merged matrix
    merged = _merge_features(merged, interaction_feats)
    print(f"  Final merged: {len(merged)} records")

    # 4. Save outputs
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nSaving to {output_dir}/")
    _save_json(merged, os.path.join(output_dir, "features_matrix.json"))
    _save_csv(merged, os.path.join(output_dir, "features_matrix.csv"))
    _save_parquet(merged, os.path.join(output_dir, "features_matrix.parquet"))

    # 5. Summary
    id_keys = {
        "partant_uid", "course_uid", "date_reunion_iso", "nom_cheval",
        "jockey_driver", "entraineur", "hippodrome_normalise", "distance",
        "discipline", "position_arrivee", "is_gagnant", "is_place", "cote_finale",
    }
    feature_keys = set()
    for r in merged:
        feature_keys.update(k for k in r if k not in id_keys)

    print(f"\n{'=' * 70}")
    print(f"SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Partants:  {len(merged)}")
    print(f"  ID cols:   {len(id_keys)}")
    print(f"  Features:  {len(feature_keys)}")
    print(f"  Total cols: {len(id_keys) + len(feature_keys)}")
    print(f"  Target:    350+ features")
    if len(feature_keys) >= 350:
        print(f"  Status:    TARGET REACHED ({len(feature_keys)} features)")
    else:
        print(f"  Status:    {350 - len(feature_keys)} features short of target")

    rates = _fill_rates(merged, exclude_keys=id_keys)
    print(f"\n  Fill rates (features):")
    for k, rate in sorted(rates.items()):
        bar = "#" * int(rate * 30)
        print(f"    {k:40s} {rate*100:5.1f}% {bar}")

    elapsed = time.time() - t0
    print(f"\n  Total time: {elapsed:.1f}s")
    print("=" * 70)

    return merged


def main():
    parser = argparse.ArgumentParser(
        description="Build feature matrix from normalized partants and courses.",
    )
    default_base = os.path.join(
        os.path.dirname(__file__), "..", "output", "02_liste_courses"
    )
    parser.add_argument(
        "--partants",
        default=os.path.join(default_base, "partants_normalises.json"),
        help="Path to partants_normalises.json",
    )
    parser.add_argument(
        "--courses",
        default=os.path.join(default_base, "courses_normalisees.json"),
        help="Path to courses_normalisees.json",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(
            os.path.dirname(__file__), "..", "output", "features"
        ),
        help="Output directory",
    )
    parser.add_argument(
        "--meteo",
        default=None,
        help="Path to meteo_historique.json (auto-detected if omitted)",
    )
    args = parser.parse_args()
    build_all_features(args.partants, args.courses, args.output, meteo_path=args.meteo)


if __name__ == "__main__":
    main()
