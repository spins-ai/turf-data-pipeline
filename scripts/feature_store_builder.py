#!/usr/bin/env python3
"""
scripts/feature_store_builder.py
================================
Builds a consolidated feature store by joining partants_master.jsonl with all
individual feature builder outputs into a single wide JSONL file.

Architecture (memory-efficient):
  1. Discover all builder JSONL files under output/features/ and output/elo_ratings/.
  2. For each builder file, load only {partant_uid -> feature_dict} into memory.
     Only the builder-specific columns are kept (shared raw columns are skipped
     since they already exist in partants_master).
  3. Stream partants_master.jsonl line by line, merge features from each lookup
     dict, and write the wide record to data_master/feature_store.jsonl.

Memory budget: ~8 GB max.  Each builder lookup is typically < 500 MB because
we only store the delta columns per uid.  With ~30 builders the total index
memory stays well under budget.

No API calls -- 100% local processing.

Usage:
    python3 scripts/feature_store_builder.py
    python3 scripts/feature_store_builder.py --output data_master/feature_store.jsonl
    python3 scripts/feature_store_builder.py --builders output/features output/elo_ratings
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.logging_setup import setup_logging

DATA_MASTER = _PROJECT_ROOT / "data_master"
DEFAULT_MASTER = DATA_MASTER / "partants_master.jsonl"
DEFAULT_OUTPUT = DATA_MASTER / "feature_store.jsonl"

# Directories containing builder JSONL outputs.
DEFAULT_BUILDER_DIRS: list[Path] = [
    _PROJECT_ROOT / "output" / "features",
    _PROJECT_ROOT / "output" / "elo_ratings",
]

# Files to skip (already the full merge or non-builder outputs).
SKIP_FILES: set[str] = {
    "features_matrix.jsonl",
    "features_matrix.json",
    "features_matrix.csv",
    "features_matrix_stats.json",
}

# Columns present in partants_master that should NOT be duplicated from
# builder files (identifiers, raw fields, metadata).  Builder files often
# carry these through; we only want the *new* feature columns.
MASTER_KEY = "partant_uid"

# Large set of raw / identifier columns that every builder copies verbatim.
# We strip these to keep only the feature delta.
RAW_COLUMNS: set[str] = {
    "partant_uid",
    "course_uid",
    "reunion_uid",
    "date_reunion_iso",
    "nom_cheval",
    "cle_partant",
    "source",
    "timestamp_collecte",
    "hippodrome_normalise",
    "horse_id",
    "jockey_driver",
    "entraineur",
    "proprietaire",
    "eleveur",
    "mere",
    "pere",
    "pere_mere",
    "race",
    "robe",
    "sexe",
    "age",
    "allure",
    "avis_entraineur",
    "commentaire_apres_course",
    "corde",
    "cote_finale",
    "cote_reference",
    "deferre",
    "discipline",
    "distance",
    "ecart_precedent",
    "engagement",
    "gains_annee_euros",
    "gains_carriere_euros",
    "handicap_distance_m",
    "handicap_valeur",
    "incident",
    "is_disqualifie",
    "is_gagnant",
    "is_inedit",
    "is_place",
    "jockey_driver_change",
    "jument_pleine",
    "musique",
    "nb_courses_carriere",
    "nb_places_2eme",
    "nb_places_3eme",
    "nb_places_carriere",
    "nb_victoires_carriere",
    "nombre_partants",
    "num_pmu",
    "numero_course",
    "numero_reunion",
    "oeilleres",
    "partant_uid",
    "pays_cheval",
    "pays_entrainement",
    "place_corde",
    "poids_base_kg",
    "poids_monte_change",
    "poids_porte_kg",
    "position_arrivee",
    "proba_implicite",
    "race",
    "reduction_km_ms",
    "sexe",
    "statut",
    "supplement_euros",
    "surcharge_decharge_kg",
    "taux_reclamation_euros",
    "temps_ms",
    "type_piste",
}

# Prefixes from merged/report sources that are not features.
NON_FEATURE_PREFIXES: tuple[str, ...] = (
    "mch_",
    "pgr_",
    "rap_",
)


# ===========================================================================
# BUILDER INDEX LOADING
# ===========================================================================


def discover_builder_files(dirs: list[Path], logger: logging.Logger) -> list[Path]:
    """Find all .jsonl builder output files in the given directories."""
    files: list[Path] = []
    for d in dirs:
        if not d.is_dir():
            logger.warning("Repertoire builder introuvable : %s", d)
            continue
        for f in sorted(d.iterdir()):
            if f.suffix == ".jsonl" and f.name not in SKIP_FILES:
                files.append(f)
    logger.info("Decouvert %d fichiers builder JSONL.", len(files))
    for f in files:
        logger.info("  - %s", f.name)
    return files


def _is_feature_column(col: str) -> bool:
    """Return True if *col* is a builder-specific feature (not a raw column)."""
    if col in RAW_COLUMNS:
        return False
    for prefix in NON_FEATURE_PREFIXES:
        if col.startswith(prefix):
            return False
    return True


def load_builder_index(
    path: Path, logger: logging.Logger
) -> tuple[dict[str, dict], list[str]]:
    """Load a builder JSONL into a {partant_uid -> {feat: val}} dict.

    Only feature columns (not raw/identifier columns) are kept.

    Returns:
        (index_dict, list_of_feature_column_names)
    """
    index: dict[str, dict] = {}
    feature_cols: set[str] = set()
    count = 0

    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            uid = rec.get(MASTER_KEY)
            if uid is None:
                continue

            # Extract only new feature columns.
            features = {}
            for k, v in rec.items():
                if _is_feature_column(k):
                    features[k] = v
                    feature_cols.add(k)

            if features:
                index[uid] = features
            count += 1

            if count % 500_000 == 0:
                logger.info("    ... %d lignes chargees", count)

    sorted_cols = sorted(feature_cols)
    logger.info(
        "  Charge %s : %d uids, %d features (%s ...)",
        path.name,
        len(index),
        len(sorted_cols),
        ", ".join(sorted_cols[:5]),
    )
    return index, sorted_cols


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_feature_store(
    master_path: Path,
    builder_dirs: list[Path],
    output_path: Path,
    logger: logging.Logger,
) -> dict:
    """Build the consolidated feature store.

    Returns a stats dict with counts.
    """
    t0 = time.time()

    # -- Phase 1: load all builder indexes --
    logger.info("=== Feature Store Builder ===")
    logger.info("Master : %s", master_path)
    logger.info("Output : %s", output_path)

    builder_files = discover_builder_files(builder_dirs, logger)
    if not builder_files:
        logger.warning("Aucun fichier builder trouve. Rien a faire.")
        return {"status": "no_builders"}

    indexes: list[dict[str, dict]] = []
    all_feature_cols: list[str] = []

    for bf in builder_files:
        logger.info("Chargement builder : %s", bf.name)
        idx, cols = load_builder_index(bf, logger)
        indexes.append(idx)
        all_feature_cols.extend(cols)
        # Free some pressure after each load.
        gc.collect()

    all_feature_cols = sorted(set(all_feature_cols))
    logger.info(
        "Total : %d indexes, %d features uniques.",
        len(indexes),
        len(all_feature_cols),
    )

    # -- Phase 2: stream master, merge, write --
    logger.info("Phase 2 : streaming merge ...")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    merged = 0
    features_added = 0

    with (
        open(master_path, "r", encoding="utf-8") as fin,
        open(output_path, "w", encoding="utf-8") as fout,
    ):
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total += 1
            uid = rec.get(MASTER_KEY)

            # Merge features from each builder index.
            n_added = 0
            if uid is not None:
                for idx in indexes:
                    feats = idx.get(uid)
                    if feats:
                        # Only add keys not already in rec (master wins ties).
                        for k, v in feats.items():
                            if k not in rec:
                                rec[k] = v
                                n_added += 1

            if n_added > 0:
                merged += 1
                features_added += n_added

            fout.write(json.dumps(rec, ensure_ascii=False))
            fout.write("\n")

            if total % 500_000 == 0:
                logger.info("  ... %d lignes ecrites", total)

    elapsed = time.time() - t0

    stats = {
        "total_master_records": total,
        "records_with_builder_features": merged,
        "total_features_added": features_added,
        "builder_files_used": len(builder_files),
        "unique_feature_columns": len(all_feature_cols),
        "feature_columns": all_feature_cols,
        "output_path": str(output_path),
        "elapsed_seconds": round(elapsed, 1),
    }

    logger.info("=== Resultats ===")
    logger.info("  Records ecrits          : %d", total)
    logger.info("  Records avec features   : %d", merged)
    logger.info("  Features ajoutees total : %d", features_added)
    logger.info("  Colonnes features       : %d", len(all_feature_cols))
    logger.info("  Temps ecoule            : %.1fs", elapsed)
    logger.info("  Sortie                  : %s", output_path)

    # Write stats sidecar.
    stats_path = output_path.with_suffix(".stats.json")
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    logger.info("Stats ecrites : %s", stats_path)

    return stats


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build consolidated feature store from partants_master + builder outputs."
    )
    parser.add_argument(
        "--master",
        type=Path,
        default=DEFAULT_MASTER,
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output feature_store.jsonl path",
    )
    parser.add_argument(
        "--builders",
        type=Path,
        nargs="+",
        default=DEFAULT_BUILDER_DIRS,
        help="Directories containing builder JSONL outputs",
    )
    args = parser.parse_args()

    logger = setup_logging("feature_store_builder")
    stats = build_feature_store(args.master, args.builders, args.output, logger)

    if stats.get("status") == "no_builders":
        logger.warning("Aucun builder -- sortie sans ecriture.")
        sys.exit(1)

    logger.info("Feature store construit avec succes.")
    sys.exit(0)


if __name__ == "__main__":
    main()
