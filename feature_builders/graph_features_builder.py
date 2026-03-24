#!/usr/bin/env python3
"""
feature_builders.graph_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Graph-based relationship features for GNN and advanced models.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes entity-relationship graph features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the graph metrics -- no future leakage.

Produces:
  - graph_features.jsonl   in output/graph_features/

Features per partant:
  - graph_jockey_centrality  : nb unique horses this jockey has ridden (PageRank proxy)
  - graph_trainer_centrality : nb unique horses this trainer trains
  - graph_horse_connectivity : nb unique jockeys who have ridden this horse
  - graph_jt_combo_strength  : jockey-trainer pair strength (courses together / total)
  - graph_hippo_diversity    : nb different hippodromes this horse has raced at

Usage:
    python feature_builders/graph_features_builder.py
    python feature_builders/graph_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
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
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "graph_features"

# Progress log every N records
_LOG_EVERY = 500_000

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
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# GRAPH STATE TRACKERS
# ===========================================================================


class _GraphState:
    """Tracks evolving graph relationships with temporal integrity.

    All sets accumulate entities seen *before* the current record's date.
    The snapshot/update pattern ensures no future leakage.
    """

    def __init__(self) -> None:
        # Jockey -> set of unique horses ridden
        self.jockey_horses: dict[str, set[str]] = defaultdict(set)
        # Trainer -> set of unique horses trained
        self.trainer_horses: dict[str, set[str]] = defaultdict(set)
        # Horse -> set of unique jockeys
        self.horse_jockeys: dict[str, set[str]] = defaultdict(set)
        # Horse -> set of unique hippodromes
        self.horse_hippos: dict[str, set[str]] = defaultdict(set)
        # (jockey, trainer) -> nb courses together
        self.jt_combo_count: dict[tuple[str, str], int] = defaultdict(int)
        # jockey -> total nb courses
        self.jockey_total: dict[str, int] = defaultdict(int)
        # trainer -> total nb courses
        self.trainer_total: dict[str, int] = defaultdict(int)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_graph_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build graph relationship features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory.
      2. Sort chronologically.
      3. Process record by record, snapshotting graph state before updating.

    Memory budget:
      - Slim records: ~16M records * ~160 bytes = ~2.6 GB
      - Graph state sets: ~500K unique entities * ~200 bytes = ~100 MB
      - Output accumulator: written at end
    """
    logger.info("=== Graph Features Builder (GNN features) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "jockey": rec.get("jockey_driver"),
            "entraineur": rec.get("entraineur"),
            "hippodrome": rec.get("hippodrome") or rec.get("hippodrome_code", ""),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Group by course, then process course by course --
    t2 = time.time()
    graph = _GraphState()
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants for this course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (i < total
               and slim_records[i]["course"] == course_uid
               and slim_records[i]["date"] == course_date):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # -- Snapshot graph state for all partants in this course --
        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]
            entraineur = rec["entraineur"]

            # Jockey centrality: how many unique horses this jockey has ridden
            jockey_centrality = (
                len(graph.jockey_horses[jockey]) if jockey else None
            )

            # Trainer centrality: how many unique horses this trainer trains
            trainer_centrality = (
                len(graph.trainer_horses[entraineur]) if entraineur else None
            )

            # Horse connectivity: how many unique jockeys have ridden this horse
            horse_connectivity = (
                len(graph.horse_jockeys[cheval]) if cheval else None
            )

            # Jockey-trainer combo strength
            jt_combo_strength = None
            if jockey and entraineur:
                combo_key = (jockey, entraineur)
                combo_n = graph.jt_combo_count[combo_key]
                # Denominator: average of jockey total and trainer total
                j_total = graph.jockey_total[jockey]
                t_total = graph.trainer_total[entraineur]
                denom = (j_total + t_total) / 2.0 if (j_total + t_total) > 0 else 0
                jt_combo_strength = round(combo_n / denom, 6) if denom > 0 else 0.0

            # Hippodrome diversity: how many different hippodromes this horse raced at
            hippo_diversity = (
                len(graph.horse_hippos[cheval]) if cheval else None
            )

            results.append({
                "partant_uid": rec["uid"],
                "graph_jockey_centrality": jockey_centrality,
                "graph_trainer_centrality": trainer_centrality,
                "graph_horse_connectivity": horse_connectivity,
                "graph_jt_combo_strength": jt_combo_strength,
                "graph_hippo_diversity": hippo_diversity,
            })

        # -- Update graph state after all snapshots (no leakage) --
        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]
            entraineur = rec["entraineur"]
            hippodrome = rec["hippodrome"]

            if jockey and cheval:
                graph.jockey_horses[jockey].add(cheval)
                graph.horse_jockeys[cheval].add(jockey)

            if entraineur and cheval:
                graph.trainer_horses[entraineur].add(cheval)

            if cheval and hippodrome:
                graph.horse_hippos[cheval].add(hippodrome)

            if jockey and entraineur:
                graph.jt_combo_count[(jockey, entraineur)] += 1

            if jockey:
                graph.jockey_total[jockey] += 1
            if entraineur:
                graph.trainer_total[entraineur] += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Graph build termine: %d features en %.1fs "
        "(jockeys: %d, entraineurs: %d, chevaux: %d)",
        len(results), elapsed,
        len(graph.jockey_horses),
        len(graph.trainer_horses),
        len(graph.horse_jockeys),
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features graphe (GNN) a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/graph_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("graph_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_graph_features(input_path, logger)

    # Save
    out_path = output_dir / "graph_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
