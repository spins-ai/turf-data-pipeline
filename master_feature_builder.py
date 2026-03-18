#!/usr/bin/env python3
"""
master_feature_builder.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Top-level orchestrator that runs ALL feature builders, affinity scripts
and calculation scripts, then outputs a single features_matrix.jsonl
with 400+ features per partant.

Design:
  - JSONL streaming: never loads the full file in memory at once.
  - Batch architecture: loads all partants once (as a list, required by
    every builder), runs each builder sequentially, merges by partant_uid,
    then streams output line by line.
  - Handles missing data gracefully (None for any feature that can't be
    computed).
  - Logs progress every 10K records.
  - Outputs a per-feature null-rate summary at the end.
  - Windows + Mac compatible (os.path.join everywhere).

Usage:
    python master_feature_builder.py
    python master_feature_builder.py --input output/mega_merge/partants_master.jsonl
    python master_feature_builder.py --input ... --output output/features/features_matrix.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Any, Optional

# ============================================================================
# PATHS
# ============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DEFAULT = os.path.join(BASE_DIR, "output", "mega_merge", "partants_master.jsonl")
OUTPUT_DEFAULT = os.path.join(BASE_DIR, "output", "features", "features_matrix.jsonl")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# External data defaults used by builders that need indexes
SMARKETS_PATH = os.path.join(BASE_DIR, "output", "30_smarkets", "smarkets.jsonl")
RP_PATH = os.path.join(BASE_DIR, "output", "37_racing_post", "racing_post.jsonl")
REUNIONS_PATH = os.path.join(BASE_DIR, "output", "39_reunions_enrichies", "reunions.jsonl")
ENRICHED_PATH = os.path.join(BASE_DIR, "output", "40_partants_enrichis", "partants_enrichis.jsonl")
CT_PATH = os.path.join(BASE_DIR, "output", "24_canalturf", "canalturf.jsonl")
TS_PATH = os.path.join(BASE_DIR, "output", "25_turfostats", "turfostats.jsonl")
GENY_PATH = os.path.join(BASE_DIR, "output", "26_geny", "geny.jsonl")
COURSES_PATH = os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_normalisees.jsonl")
PERF_DET_PATH = os.path.join(BASE_DIR, "output", "22_performances_detaillees", "performances_detaillees.jsonl")

# ============================================================================
# LOGGING
# ============================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("master_feature_builder")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    os.makedirs(LOG_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOG_DIR, "master_feature_builder.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ============================================================================
# GENERIC HELPERS
# ============================================================================

def load_jsonl(path: str, logger: logging.Logger) -> list[dict]:
    """Stream-load a JSONL file. Falls back to .json if .jsonl not found."""
    records: list[dict] = []
    if not os.path.exists(path):
        alt = path.replace(".jsonl", ".json") if path.endswith(".jsonl") else path.replace(".json", ".jsonl")
        if os.path.exists(alt):
            path = alt
        else:
            logger.warning("File not found: %s (skipping)", path)
            return []

    logger.info("Loading %s ...", path)
    if path.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning("Bad JSON at line %d in %s", lineno, path)
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            records = data
        else:
            records = [data]

    logger.info("  -> %d records loaded from %s", len(records), os.path.basename(path))
    return records


def safe_call(func, args, step_name: str, logger: logging.Logger) -> list[dict]:
    """Call a builder function, catching any exception so the pipeline continues."""
    t0 = time.time()
    try:
        result = func(*args)
        elapsed = time.time() - t0
        count = len(result) if isinstance(result, list) else 0
        logger.info("  [OK] %s -> %d records (%.1fs)", step_name, count, elapsed)
        return result if isinstance(result, list) else []
    except Exception as exc:
        elapsed = time.time() - t0
        logger.error("  [FAIL] %s -> %s (%.1fs)", step_name, exc, elapsed)
        return []


def merge_by_uid(base: list[dict], *feature_lists: list[dict]) -> list[dict]:
    """Left-join merge feature dicts onto base by partant_uid."""
    lookups: list[dict[str, dict]] = []
    for fl in feature_lists:
        lk: dict[str, dict] = {}
        for row in fl:
            uid = row.get("partant_uid")
            if uid:
                lk[uid] = row
        lookups.append(lk)

    merged = []
    for p in base:
        uid = p.get("partant_uid")
        row = dict(p)  # shallow copy of base record
        for lk in lookups:
            feat = lk.get(uid, {})
            for k, v in feat.items():
                if k != "partant_uid":
                    row[k] = v
        merged.append(row)
    return merged


# ============================================================================
# PHASE 1 : FEATURE BUILDERS  (feature_builders/*.py)
# ============================================================================

def run_feature_builders(partants: list[dict], courses: list[dict], logger: logging.Logger) -> list[list[dict]]:
    """Run all feature_builders/ modules and return a list of feature-lists."""
    results: list[list[dict]] = []
    step = 0

    def _step(name):
        nonlocal step
        step += 1
        logger.info("[Phase 1 - Step %d] %s ...", step, name)

    # --- Simple builders (partants only) ---
    from feature_builders.musique_features import build_musique_features
    _step("musique_features")
    results.append(safe_call(build_musique_features, (partants,), "musique_features", logger))

    from feature_builders.temps_features import build_temps_features
    _step("temps_features")
    results.append(safe_call(build_temps_features, (partants,), "temps_features", logger))

    from feature_builders.profil_cheval_features import build_profil_cheval_features
    _step("profil_cheval_features")
    results.append(safe_call(build_profil_cheval_features, (partants,), "profil_cheval_features", logger))

    from feature_builders.equipement_features import build_equipement_features
    _step("equipement_features")
    results.append(safe_call(build_equipement_features, (partants,), "equipement_features", logger))

    from feature_builders.poids_features import build_poids_features
    _step("poids_features")
    results.append(safe_call(build_poids_features, (partants,), "poids_features", logger))

    from feature_builders.combo_features import build_combo_features
    _step("combo_features")
    results.append(safe_call(build_combo_features, (partants,), "combo_features", logger))

    from feature_builders.precomputed_partant_joiner import build_precomputed_partant_features
    _step("precomputed_partant_joiner")
    results.append(safe_call(build_precomputed_partant_features, (partants,), "precomputed_partant_joiner", logger))

    from feature_builders.precomputed_entity_joiner import build_precomputed_entity_features
    _step("precomputed_entity_joiner")
    results.append(safe_call(build_precomputed_entity_features, (partants,), "precomputed_entity_joiner", logger))

    # --- Builders needing courses ---
    from feature_builders.class_change_features import build_class_change_features
    _step("class_change_features")
    results.append(safe_call(build_class_change_features, (partants, courses), "class_change_features", logger))

    from feature_builders.meteo_features import build_meteo_features
    _step("meteo_features")
    results.append(safe_call(build_meteo_features, (partants,), "meteo_features", logger))

    # --- Builders needing external index + logger ---

    # perf_detaillees_builder
    from feature_builders.perf_detaillees_builder import build_perf_detaillees_features
    _step("perf_detaillees_builder")
    results.append(safe_call(build_perf_detaillees_features, (partants, logger), "perf_detaillees_builder", logger))

    # smarkets_builder
    _step("smarkets_builder")
    try:
        from feature_builders.smarkets_builder import index_smarkets, build_smarkets_features, load_json_or_jsonl
        smarkets_data = load_json_or_jsonl(SMARKETS_PATH, logger) if os.path.exists(SMARKETS_PATH) else []
        smarkets_idx = index_smarkets(smarkets_data, logger) if smarkets_data else {}
        results.append(safe_call(build_smarkets_features, (partants, smarkets_idx, logger), "smarkets_builder", logger))
    except Exception as exc:
        logger.error("  [FAIL] smarkets_builder index -> %s", exc)
        results.append([])

    # racing_post_builder
    _step("racing_post_builder")
    try:
        from feature_builders.racing_post_builder import index_rp_data, build_racing_post_features, load_json_or_jsonl as rp_load
        rp_data = rp_load(RP_PATH, logger) if os.path.exists(RP_PATH) else []
        rp_idx = index_rp_data(rp_data, logger) if rp_data else {}
        results.append(safe_call(build_racing_post_features, (partants, rp_idx, logger), "racing_post_builder", logger))
    except Exception as exc:
        logger.error("  [FAIL] racing_post_builder index -> %s", exc)
        results.append([])

    # reunions_builder
    _step("reunions_builder")
    try:
        from feature_builders.reunions_builder import index_reunions, build_reunions_features, load_json_or_jsonl as reu_load
        reu_data = reu_load(REUNIONS_PATH, logger) if os.path.exists(REUNIONS_PATH) else []
        reu_idx = index_reunions(reu_data, logger) if reu_data else {}
        results.append(safe_call(build_reunions_features, (partants, reu_idx, logger), "reunions_builder", logger))
    except Exception as exc:
        logger.error("  [FAIL] reunions_builder index -> %s", exc)
        results.append([])

    # enrichissement_builder
    _step("enrichissement_builder")
    try:
        from feature_builders.enrichissement_builder import index_enriched, build_enrichissement_features, load_json_or_jsonl as enr_load
        enr_data = enr_load(ENRICHED_PATH, logger) if os.path.exists(ENRICHED_PATH) else []
        enr_idx = index_enriched(enr_data, logger) if enr_data else {}
        results.append(safe_call(build_enrichissement_features, (partants, enr_idx, logger), "enrichissement_builder", logger))
    except Exception as exc:
        logger.error("  [FAIL] enrichissement_builder index -> %s", exc)
        results.append([])

    # pedigree_advanced_builder
    from feature_builders.pedigree_advanced_builder import build_pedigree_advanced_features
    _step("pedigree_advanced_builder")
    results.append(safe_call(build_pedigree_advanced_features, (partants, logger), "pedigree_advanced_builder", logger))

    # canalturf_builder
    _step("canalturf_builder")
    try:
        from feature_builders.canalturf_builder import index_ct_data, build_canalturf_features, load_json_or_jsonl as ct_load
        ct_data = ct_load(CT_PATH, logger) if os.path.exists(CT_PATH) else []
        ct_idx = index_ct_data(ct_data, logger) if ct_data else {}
        results.append(safe_call(build_canalturf_features, (partants, ct_idx, logger), "canalturf_builder", logger))
    except Exception as exc:
        logger.error("  [FAIL] canalturf_builder index -> %s", exc)
        results.append([])

    # turfostats_builder
    _step("turfostats_builder")
    try:
        from feature_builders.turfostats_builder import index_ts_data, build_turfostats_features, load_json_or_jsonl as ts_load
        ts_data = ts_load(TS_PATH, logger) if os.path.exists(TS_PATH) else []
        ts_idx = index_ts_data(ts_data, logger) if ts_data else {}
        results.append(safe_call(build_turfostats_features, (partants, ts_idx, logger), "turfostats_builder", logger))
    except Exception as exc:
        logger.error("  [FAIL] turfostats_builder index -> %s", exc)
        results.append([])

    # geny_builder
    _step("geny_builder")
    try:
        from feature_builders.geny_builder import index_geny_data, build_geny_features, build_race_consensus, load_json_or_jsonl as geny_load
        geny_data = geny_load(GENY_PATH, logger) if os.path.exists(GENY_PATH) else []
        geny_idx = index_geny_data(geny_data, logger) if geny_data else {}
        consensus = build_race_consensus(geny_data, logger) if geny_data else {}
        results.append(safe_call(build_geny_features, (partants, geny_idx, consensus, logger), "geny_builder", logger))
    except Exception as exc:
        logger.error("  [FAIL] geny_builder index -> %s", exc)
        results.append([])

    # --- interaction_features is special: runs AFTER merge (Phase 1b) ---
    # We return it separately below
    return results


# ============================================================================
# PHASE 2 : AFFINITY SCRIPTS  (feat_*.py at root)
# ============================================================================

def run_affinity_scripts(partants: list[dict], logger: logging.Logger) -> list[list[dict]]:
    """Run all root-level feat_*_affinity / feat_* scripts."""
    results: list[list[dict]] = []
    step = 0

    def _step(name):
        nonlocal step
        step += 1
        logger.info("[Phase 2 - Step %d] %s ...", step, name)

    from feat_cheval_jockey_affinity import compute_cheval_jockey_affinity
    _step("cheval_jockey_affinity")
    results.append(safe_call(compute_cheval_jockey_affinity, (partants,), "cheval_jockey_affinity", logger))

    from feat_cheval_hippodrome_affinity import compute_cheval_hippodrome_affinity
    _step("cheval_hippodrome_affinity")
    results.append(safe_call(compute_cheval_hippodrome_affinity, (partants,), "cheval_hippodrome_affinity", logger))

    from feat_cheval_distance_affinity import compute_cheval_distance_affinity
    _step("cheval_distance_affinity")
    results.append(safe_call(compute_cheval_distance_affinity, (partants,), "cheval_distance_affinity", logger))

    from feat_cheval_terrain_affinity import compute_cheval_terrain_affinity
    _step("cheval_terrain_affinity")
    results.append(safe_call(compute_cheval_terrain_affinity, (partants,), "cheval_terrain_affinity", logger))

    from feat_jockey_entraineur_combo import compute_jockey_entraineur_combo
    _step("jockey_entraineur_combo")
    results.append(safe_call(compute_jockey_entraineur_combo, (partants,), "jockey_entraineur_combo", logger))

    from feat_entraineur_hippodrome import compute_entraineur_hippodrome
    _step("entraineur_hippodrome")
    results.append(safe_call(compute_entraineur_hippodrome, (partants,), "entraineur_hippodrome", logger))

    from feat_value_betting import compute_value_betting
    _step("value_betting")
    results.append(safe_call(compute_value_betting, (partants,), "value_betting", logger))

    from feat_meteo_terrain_interaction import compute_meteo_terrain_interaction
    _step("meteo_terrain_interaction")
    results.append(safe_call(compute_meteo_terrain_interaction, (partants,), "meteo_terrain_interaction", logger))

    from feat_pedigree_discipline_match import compute_pedigree_discipline_match
    _step("pedigree_discipline_match")
    results.append(safe_call(compute_pedigree_discipline_match, (partants,), "pedigree_discipline_match", logger))

    from feat_field_strength import compute_field_strength
    _step("field_strength")
    results.append(safe_call(compute_field_strength, (partants,), "field_strength", logger))

    return results


# ============================================================================
# PHASE 3 : CALCULATION SCRIPTS  (41-49)
# ============================================================================

def run_calculation_scripts(partants: list[dict], logger: logging.Logger) -> list[list[dict]]:
    """Run calculation scripts 41-49 that have a compute_* entry point."""
    results: list[list[dict]] = []
    step = 0

    def _step(name):
        nonlocal step
        step += 1
        logger.info("[Phase 3 - Step %d] %s ...", step, name)

    # 41 - sequences performances
    try:
        sys.path.insert(0, BASE_DIR)
        from importlib import import_module

        mod41 = import_module("41_sequences_performances")
        _step("41_sequences_performances")
        results.append(safe_call(mod41.compute_sequences, (partants,), "41_sequences", logger))
    except Exception as exc:
        logger.error("  [FAIL] 41_sequences_performances -> %s", exc)
        results.append([])

    # 42 - croisement Racing Post x PMU
    try:
        mod42 = import_module("42_croisement_racing_post_pmu")
        _step("42_croisement_racing_post_pmu")
        rp_data_42 = []
        for rp_path in [os.path.join(BASE_DIR, "output", "37_racing_post", "racing_post_fr.jsonl"),
                        os.path.join(BASE_DIR, "output", "37_racing_post", "racing_post_fr.json")]:
            if os.path.exists(rp_path):
                rp_data_42 = load_jsonl(rp_path, logger)
                break
        rp_index_42 = mod42.build_rp_index(rp_data_42) if rp_data_42 else {}
        results.append(safe_call(mod42.compute_croisement, (partants, rp_index_42), "42_croisement_rp", logger))
    except Exception as exc:
        logger.error("  [FAIL] 42_croisement_racing_post_pmu -> %s", exc)
        results.append([])

    # 43 - croisement meteo courses
    try:
        mod43 = import_module("43_croisement_meteo_courses")
        _step("43_croisement_meteo_courses")
        meteo_idx_43 = mod43.load_meteo_index() if hasattr(mod43, "load_meteo_index") else {}
        results.append(safe_call(mod43.compute_croisement, (partants, meteo_idx_43), "43_croisement_meteo", logger))
    except Exception as exc:
        logger.error("  [FAIL] 43_croisement_meteo_courses -> %s", exc)
        results.append([])

    # 44 - croisement pedigree partants
    try:
        mod44 = import_module("44_croisement_pedigree_partants")
        _step("44_croisement_pedigree_partants")
        pedigree_idx_44 = mod44.load_pedigree_index() if hasattr(mod44, "load_pedigree_index") else {}
        sire_stats_44 = mod44.build_sire_stats(partants) if hasattr(mod44, "build_sire_stats") else {}
        results.append(safe_call(mod44.compute_croisement, (partants, pedigree_idx_44, sire_stats_44), "44_croisement_pedigree", logger))
    except Exception as exc:
        logger.error("  [FAIL] 44_croisement_pedigree_partants -> %s", exc)
        results.append([])

    # 45 - graphe relations GNN
    try:
        mod45 = import_module("45_graphe_relations_gnn")
        _step("45_graphe_relations_gnn")
        results.append(safe_call(mod45.compute_graph_features, (partants,), "45_graphe_gnn", logger))
    except Exception as exc:
        logger.error("  [FAIL] 45_graphe_relations_gnn -> %s", exc)
        results.append([])

    # 46 - track bias speed class
    try:
        mod46 = import_module("46_track_bias_speed_class")
        _step("46_track_bias_speed_class")
        results.append(safe_call(mod46.compute_features, (partants,), "46_track_bias_speed", logger))
    except Exception as exc:
        logger.error("  [FAIL] 46_track_bias_speed_class -> %s", exc)
        results.append([])

    # 48 - parse conditions texte
    # This one works per-course, not per-partant. We apply it at the partant level.
    try:
        mod48 = import_module("48_parse_conditions_texte")
        _step("48_parse_conditions_texte")
        enriched_48 = []
        for p in partants:
            texte = p.get("conditions_texte") or p.get("libelle") or ""
            feat = {}
            try:
                feat = mod48.parse_conditions(texte) or {}
            except Exception:
                pass
            feat["partant_uid"] = p.get("partant_uid")
            enriched_48.append(feat)
        logger.info("  [OK] 48_parse_conditions -> %d records", len(enriched_48))
        results.append(enriched_48)
    except Exception as exc:
        logger.error("  [FAIL] 48_parse_conditions_texte -> %s", exc)
        results.append([])

    # 49 - ecart cotes internet national
    try:
        mod49 = import_module("49_ecart_cotes_internet_national")
        _step("49_ecart_cotes_internet_national")
        rapports_internet = mod49.load_rapports_internet() if hasattr(mod49, "load_rapports_internet") else {}
        rapports_nationaux = mod49.load_rapports_nationaux() if hasattr(mod49, "load_rapports_nationaux") else {}
        cotes_marche = mod49.load_cotes_marche() if hasattr(mod49, "load_cotes_marche") else {}
        results.append(safe_call(
            mod49.compute_ecart_features,
            (partants, rapports_internet, rapports_nationaux, cotes_marche),
            "49_ecart_cotes", logger
        ))
    except Exception as exc:
        logger.error("  [FAIL] 49_ecart_cotes_internet_national -> %s", exc)
        results.append([])

    return results


# ============================================================================
# STATS & SUMMARY
# ============================================================================

ID_KEYS = frozenset({
    "partant_uid", "course_uid", "date_reunion_iso", "nom_cheval",
    "jockey_driver", "jockey", "nom_jockey", "entraineur",
    "hippodrome_normalise", "distance", "discipline",
    "position_arrivee", "is_gagnant", "is_place", "cote_finale",
    "numero_reunion", "numero_course", "horse_id", "num_pmu",
})


def compute_feature_stats(merged: list[dict]) -> dict[str, dict]:
    """Compute count and null rate for every feature column."""
    n = len(merged)
    if n == 0:
        return {}

    all_keys: set[str] = set()
    for r in merged:
        all_keys.update(r.keys())

    feature_keys = sorted(all_keys - ID_KEYS)
    stats: dict[str, dict] = {}
    for k in feature_keys:
        filled = sum(1 for r in merged if r.get(k) is not None)
        stats[k] = {
            "count": filled,
            "null_count": n - filled,
            "null_rate": round((n - filled) / n, 4) if n else 0,
            "fill_rate": round(filled / n, 4) if n else 0,
        }
    return stats


def print_summary(merged: list[dict], stats: dict, elapsed: float, logger: logging.Logger):
    """Print a human-readable summary."""
    n = len(merged)
    n_features = len(stats)

    logger.info("=" * 72)
    logger.info("MASTER FEATURE BUILDER - SUMMARY")
    logger.info("=" * 72)
    logger.info("  Total partants:  %d", n)
    logger.info("  Total features:  %d", n_features)
    logger.info("  Target:          400+")
    if n_features >= 400:
        logger.info("  Status:          TARGET REACHED (%d features)", n_features)
    else:
        logger.info("  Status:          %d features short of 400 target", 400 - n_features)
    logger.info("")

    # Top 20 best-filled features
    sorted_by_fill = sorted(stats.items(), key=lambda x: -x[1]["fill_rate"])
    logger.info("  Top 20 best-filled features:")
    for k, s in sorted_by_fill[:20]:
        bar = "#" * int(s["fill_rate"] * 30)
        logger.info("    %-45s %5.1f%% %s", k, s["fill_rate"] * 100, bar)

    # Top 20 worst-filled features (most nulls)
    logger.info("")
    logger.info("  Top 20 most-null features:")
    sorted_by_null = sorted(stats.items(), key=lambda x: -x[1]["null_rate"])
    for k, s in sorted_by_null[:20]:
        logger.info("    %-45s %5.1f%% null", k, s["null_rate"] * 100)

    logger.info("")
    logger.info("  Total time: %.1fs", elapsed)
    logger.info("=" * 72)


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_pipeline(input_path: str, output_path: str, logger: logging.Logger):
    """Run the full feature-building pipeline."""
    t_global = time.time()

    logger.info("=" * 72)
    logger.info("MASTER FEATURE BUILDER - FULL PIPELINE")
    logger.info("  Input:  %s", input_path)
    logger.info("  Output: %s", output_path)
    logger.info("=" * 72)

    # ------------------------------------------------------------------
    # Step 0: Load partants (streaming JSONL, but we need the full list
    #         because every builder requires sorted random access)
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("[Step 0] Loading partants from JSONL ...")
    partants: list[dict] = []
    with open(input_path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                partants.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning("Bad JSON at line %d", lineno)
            if lineno % 10_000 == 0:
                logger.info("  ... loaded %d lines", lineno)

    logger.info("  Total partants loaded: %d", len(partants))

    if not partants:
        logger.error("No partants loaded. Aborting.")
        return

    # Sort by date for point-in-time safety (required by most builders)
    partants.sort(key=lambda p: (
        p.get("date_reunion_iso", ""),
        p.get("numero_reunion", 0),
        p.get("numero_course", 0),
    ))
    logger.info("  Partants sorted by date.")

    # Load courses (needed by some feature_builders)
    logger.info("[Step 0b] Loading courses ...")
    courses = load_jsonl(COURSES_PATH, logger)

    # ------------------------------------------------------------------
    # Phase 1: Feature builders (feature_builders/*.py)
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 72)
    logger.info("PHASE 1: FEATURE BUILDERS (feature_builders/)")
    logger.info("=" * 72)

    builder_results = run_feature_builders(partants, courses, logger)

    # Merge Phase 1 results
    logger.info("")
    logger.info("[Phase 1 - Merge] Merging %d builder outputs ...", len(builder_results))
    # Filter out empty lists
    non_empty = [r for r in builder_results if r]
    merged = merge_by_uid(partants, *non_empty)
    logger.info("  Merged: %d records", len(merged))

    # Phase 1b: Interaction features (needs the merged matrix)
    logger.info("[Phase 1b] Building interaction features (cross-feature) ...")
    try:
        from feature_builders.interaction_features import build_interaction_features
        interaction_feats = safe_call(build_interaction_features, (merged,), "interaction_features", logger)
        if interaction_feats:
            merged = merge_by_uid(merged, interaction_feats)
            logger.info("  Post-interaction: %d records", len(merged))
    except Exception as exc:
        logger.error("  [FAIL] interaction_features -> %s", exc)

    # ------------------------------------------------------------------
    # Phase 2: Affinity scripts (feat_*.py)
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 72)
    logger.info("PHASE 2: AFFINITY SCRIPTS (feat_*.py)")
    logger.info("=" * 72)

    affinity_results = run_affinity_scripts(partants, logger)
    non_empty_aff = [r for r in affinity_results if r]
    if non_empty_aff:
        merged = merge_by_uid(merged, *non_empty_aff)
        logger.info("  Post-affinity merge: %d records", len(merged))

    # ------------------------------------------------------------------
    # Phase 3: Calculation scripts (41-49)
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 72)
    logger.info("PHASE 3: CALCULATION SCRIPTS (41-49)")
    logger.info("=" * 72)

    calc_results = run_calculation_scripts(partants, logger)
    non_empty_calc = [r for r in calc_results if r]
    if non_empty_calc:
        merged = merge_by_uid(merged, *non_empty_calc)
        logger.info("  Post-calc merge: %d records", len(merged))

    # ------------------------------------------------------------------
    # Output: Stream to JSONL
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 72)
    logger.info("OUTPUT: Writing features_matrix.jsonl")
    logger.info("=" * 72)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for i, row in enumerate(merged):
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            if (i + 1) % 10_000 == 0:
                logger.info("  ... written %d / %d records", i + 1, len(merged))

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info("  Output: %s (%.1f MB, %d records)", output_path, file_size_mb, len(merged))

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    stats = compute_feature_stats(merged)
    elapsed = time.time() - t_global
    print_summary(merged, stats, elapsed, logger)

    # Save stats to a sidecar JSON
    stats_path = output_path.replace(".jsonl", "_stats.json")
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump({
            "total_partants": len(merged),
            "total_features": len(stats),
            "elapsed_seconds": round(elapsed, 1),
            "features": stats,
        }, f, ensure_ascii=False, indent=2)
    logger.info("  Stats saved: %s", stats_path)


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Master Feature Builder - orchestrates all builders, affinity scripts and calculations."
    )
    parser.add_argument(
        "--input", "-i",
        default=INPUT_DEFAULT,
        help="Path to partants_master.jsonl (mega-merged)",
    )
    parser.add_argument(
        "--output", "-o",
        default=OUTPUT_DEFAULT,
        help="Output path for features_matrix.jsonl",
    )
    args = parser.parse_args()

    logger = setup_logging()

    # Ensure BASE_DIR is on sys.path so feat_*.py and 4x_*.py can be imported
    if BASE_DIR not in sys.path:
        sys.path.insert(0, BASE_DIR)

    run_pipeline(args.input, args.output, logger)


if __name__ == "__main__":
    main()
