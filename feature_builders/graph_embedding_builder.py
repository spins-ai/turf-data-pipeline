#!/usr/bin/env python3
"""
feature_builders.graph_embedding_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Graph / network features for GNN models.  Computes network centrality
and co-occurrence features from the horse racing graph.

Two-pass architecture:
  Pass 1  -- Build co-occurrence graphs in memory:
               horse -> race count, opponent count, field sizes, hippodromes
               jockey -> trainers, hippodromes
               trainer -> jockeys, hippodromes
               jockey-trainer pair loyalty counters
               course_uid -> set of horse_ids (for repeat-opponent calc)
  Pass 2  -- Stream partants again, compute 10 graph metrics per partant,
               write directly to JSONL.

Temporal integrity: for each partant at date D, only data from races
with date < D is used -- no future leakage.  Pass 1 collects everything,
but Pass 2 processes records chronologically and snapshots state BEFORE
updating it.

Features per partant (10):
  grp_horse_race_count         total races for this horse (node degree)
  grp_horse_unique_opponents   number of distinct horses faced
  grp_horse_avg_field_size     average field size in horse's races
  grp_jockey_network_size      distinct trainers this jockey has worked with
  grp_trainer_network_size     distinct jockeys this trainer has used
  grp_jockey_hippo_diversity   distinct hippodromes for this jockey
  grp_trainer_hippo_diversity  distinct hippodromes for this trainer
  grp_horse_hippo_concentration  proportion of horse's races at current hippo
  grp_jockey_trainer_loyalty   proportion of jockey's rides for this trainer
  grp_horse_repeat_opponents   horses in current field already faced before

Memory note:
  Opponent sets can be large (~300K horses x avg 500 opponents).
  We store horse_id -> set(opponent_ids) using compact integer IDs via
  an interning dict to keep RAM manageable.  gc.collect() every 500K.

Produces:
  graph_embedding_features.jsonl  in builder_outputs/graph_embedding/

Usage:
    python feature_builders/graph_embedding_builder.py
    python feature_builders/graph_embedding_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/graph_embedding")

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time."""
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


class _HorseGraph:
    """Compact per-horse graph stats.

    For each horse we track:
      - race_count: int
      - opponent_count: int  (len of opponents set, OR direct counter)
      - field_size_sum: int  (to compute avg)
      - hippo_counts: dict[str, int]  (hippo -> race count at that hippo)
      - opponents: set[int]  (interned IDs of opponent horses)
    """

    __slots__ = ("race_count", "field_size_sum", "hippo_counts", "opponents")

    def __init__(self) -> None:
        self.race_count: int = 0
        self.field_size_sum: int = 0
        self.hippo_counts: dict[str, int] = {}
        self.opponents: set[int] = set()

    def avg_field_size(self) -> Optional[float]:
        if self.race_count == 0:
            return None
        return round(self.field_size_sum / self.race_count, 2)

    def hippo_concentration(self, hippo: str) -> Optional[float]:
        if self.race_count == 0 or not hippo:
            return None
        return round(self.hippo_counts.get(hippo, 0) / self.race_count, 4)


class _ActorNetwork:
    """Tracks partner set and hippodrome set for a jockey or trainer."""

    __slots__ = ("partners", "hippos")

    def __init__(self) -> None:
        self.partners: set[str] = set()
        self.hippos: set[str] = set()


class _LoyaltyTracker:
    """Tracks jockey total rides and rides-per-trainer for loyalty calc."""

    __slots__ = ("total_rides", "rides_per_trainer")

    def __init__(self) -> None:
        self.total_rides: int = 0
        self.rides_per_trainer: dict[str, int] = {}


# ===========================================================================
# HORSE ID INTERNING (memory optimisation)
# ===========================================================================


class _HorseIntern:
    """Map horse name/id strings to compact ints for set storage."""

    __slots__ = ("_map", "_next_id")

    def __init__(self) -> None:
        self._map: dict[str, int] = {}
        self._next_id: int = 0

    def get_id(self, horse: str) -> int:
        iid = self._map.get(horse)
        if iid is not None:
            return iid
        iid = self._next_id
        self._map[horse] = iid
        self._next_id += 1
        return iid

    def __len__(self) -> int:
        return self._next_id


# ===========================================================================
# BUILD
# ===========================================================================


def build_graph_features(input_path: Path, output_path: Path, logger) -> int:
    """Build graph embedding features from partants_master.jsonl.

    Two-pass approach:
      Pass 1 -- Build lightweight chronological index + collect course compositions.
      Pass 2 -- Process chronologically: snapshot graph state, emit features, update state.

    Returns total records written.
    """
    logger.info("=== Graph Embedding Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # ---------------------------------------------------------------
    # Pass 1: Build chronological index + course-level horse lists
    # ---------------------------------------------------------------
    logger.info("--- Pass 1: Indexing + collecting course compositions ---")

    # index entries: (date_str, course_uid, num_pmu, byte_offset)
    index: list[tuple[str, str, int, int]] = []
    # course_uid -> list of horse names (for repeat-opponent calc in pass 2)
    course_horses: dict[str, list[str]] = defaultdict(list)

    n_read = 0
    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Pass 1: indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            horse = rec.get("nom_cheval") or rec.get("cheval_nom") or ""
            if horse:
                horse = horse.strip()

            index.append((date_str, course_uid, num_pmu, offset))

            if course_uid and horse:
                course_horses[course_uid].append(horse)

    logger.info(
        "Pass 1 termine: %d records, %d courses en %.1fs",
        len(index), len(course_horses), time.time() - t0,
    )

    # Sort chronologically
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ---------------------------------------------------------------
    # Pass 2: Process chronologically, snapshot-then-update
    # ---------------------------------------------------------------
    logger.info("--- Pass 2: Computing graph features ---")
    t2 = time.time()

    intern = _HorseIntern()
    horse_graph: dict[str, _HorseGraph] = {}
    jockey_net: dict[str, _ActorNetwork] = defaultdict(_ActorNetwork)
    trainer_net: dict[str, _ActorNetwork] = defaultdict(_ActorNetwork)
    jockey_loyalty: dict[str, _LoyaltyTracker] = defaultdict(_LoyaltyTracker)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    n_processed = 0
    total = len(index)

    feature_names = [
        "grp_horse_race_count",
        "grp_horse_unique_opponents",
        "grp_horse_avg_field_size",
        "grp_jockey_network_size",
        "grp_trainer_network_size",
        "grp_jockey_hippo_diversity",
        "grp_trainer_hippo_diversity",
        "grp_horse_hippo_concentration",
        "grp_jockey_trainer_loyalty",
        "grp_horse_repeat_opponents",
    ]
    fill_counts = {f: 0 for f in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read records for this course
            course_recs = []
            for ci in course_indices:
                rec = _read_at(index[ci][3])
                horse = (rec.get("nom_cheval") or rec.get("cheval_nom") or "").strip()
                jockey = (rec.get("nom_jockey") or rec.get("jockey_nom") or "").strip()
                trainer = (rec.get("nom_entraineur") or rec.get("entraineur_nom") or "").strip()
                hippo = (rec.get("hippodrome_normalise") or rec.get("hippodrome") or "").strip()

                course_recs.append({
                    "partant_uid": rec.get("partant_uid"),
                    "course_uid": rec.get("course_uid", ""),
                    "date": rec.get("date_reunion_iso", ""),
                    "horse": horse,
                    "jockey": jockey,
                    "trainer": trainer,
                    "hippo": hippo,
                })

            # Gather horse names in this course for repeat-opponent calc
            horses_in_course = [r["horse"] for r in course_recs if r["horse"]]

            # ----- SNAPSHOT: compute features BEFORE updating state -----
            for rec in course_recs:
                horse = rec["horse"]
                jockey = rec["jockey"]
                trainer = rec["trainer"]
                hippo = rec["hippo"]

                features = {
                    "partant_uid": rec["partant_uid"],
                    "course_uid": rec["course_uid"],
                    "date_reunion_iso": rec["date"],
                }

                # --- Horse graph features ---
                hg = horse_graph.get(horse) if horse else None

                if hg and hg.race_count > 0:
                    features["grp_horse_race_count"] = hg.race_count
                    fill_counts["grp_horse_race_count"] += 1

                    features["grp_horse_unique_opponents"] = len(hg.opponents)
                    fill_counts["grp_horse_unique_opponents"] += 1

                    avg_fs = hg.avg_field_size()
                    features["grp_horse_avg_field_size"] = avg_fs
                    if avg_fs is not None:
                        fill_counts["grp_horse_avg_field_size"] += 1

                    conc = hg.hippo_concentration(hippo)
                    features["grp_horse_hippo_concentration"] = conc
                    if conc is not None:
                        fill_counts["grp_horse_hippo_concentration"] += 1

                    # Repeat opponents: how many horses in current field
                    # has this horse faced before?
                    if horses_in_course:
                        repeat = 0
                        for other in horses_in_course:
                            if other and other != horse:
                                other_id = intern._map.get(other)
                                if other_id is not None and other_id in hg.opponents:
                                    repeat += 1
                        features["grp_horse_repeat_opponents"] = repeat
                        fill_counts["grp_horse_repeat_opponents"] += 1
                    else:
                        features["grp_horse_repeat_opponents"] = None
                else:
                    features["grp_horse_race_count"] = None
                    features["grp_horse_unique_opponents"] = None
                    features["grp_horse_avg_field_size"] = None
                    features["grp_horse_hippo_concentration"] = None
                    features["grp_horse_repeat_opponents"] = None

                # --- Jockey network features ---
                if jockey and jockey in jockey_net:
                    jn = jockey_net[jockey]
                    features["grp_jockey_network_size"] = len(jn.partners)
                    fill_counts["grp_jockey_network_size"] += 1

                    features["grp_jockey_hippo_diversity"] = len(jn.hippos)
                    fill_counts["grp_jockey_hippo_diversity"] += 1
                else:
                    features["grp_jockey_network_size"] = None
                    features["grp_jockey_hippo_diversity"] = None

                # --- Trainer network features ---
                if trainer and trainer in trainer_net:
                    tn = trainer_net[trainer]
                    features["grp_trainer_network_size"] = len(tn.partners)
                    fill_counts["grp_trainer_network_size"] += 1

                    features["grp_trainer_hippo_diversity"] = len(tn.hippos)
                    fill_counts["grp_trainer_hippo_diversity"] += 1
                else:
                    features["grp_trainer_network_size"] = None
                    features["grp_trainer_hippo_diversity"] = None

                # --- Jockey-trainer loyalty ---
                if jockey and trainer and jockey in jockey_loyalty:
                    jl = jockey_loyalty[jockey]
                    if jl.total_rides > 0:
                        rides_for_trainer = jl.rides_per_trainer.get(trainer, 0)
                        features["grp_jockey_trainer_loyalty"] = round(
                            rides_for_trainer / jl.total_rides, 4
                        )
                        fill_counts["grp_jockey_trainer_loyalty"] += 1
                    else:
                        features["grp_jockey_trainer_loyalty"] = None
                else:
                    features["grp_jockey_trainer_loyalty"] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # ----- UPDATE state after snapshotting -----
            field_size = len(horses_in_course)
            horse_ids_in_course = []
            for h in horses_in_course:
                if h:
                    horse_ids_in_course.append(intern.get_id(h))

            for rec in course_recs:
                horse = rec["horse"]
                jockey = rec["jockey"]
                trainer = rec["trainer"]
                hippo = rec["hippo"]

                # Update horse graph
                if horse:
                    if horse not in horse_graph:
                        horse_graph[horse] = _HorseGraph()
                    hg = horse_graph[horse]
                    hg.race_count += 1
                    hg.field_size_sum += field_size

                    if hippo:
                        hg.hippo_counts[hippo] = hg.hippo_counts.get(hippo, 0) + 1

                    # Add opponents (all other horses in this course)
                    my_id = intern.get_id(horse)
                    for oid in horse_ids_in_course:
                        if oid != my_id:
                            hg.opponents.add(oid)

                # Update jockey network
                if jockey:
                    jn = jockey_net[jockey]
                    if trainer:
                        jn.partners.add(trainer)
                    if hippo:
                        jn.hippos.add(hippo)

                # Update trainer network
                if trainer:
                    tn = trainer_net[trainer]
                    if jockey:
                        tn.partners.add(jockey)
                    if hippo:
                        tn.hippos.add(hippo)

                # Update jockey loyalty
                if jockey:
                    jl = jockey_loyalty[jockey]
                    jl.total_rides += 1
                    if trainer:
                        jl.rides_per_trainer[trainer] = (
                            jl.rides_per_trainer.get(trainer, 0) + 1
                        )

            n_processed += len(course_recs)
            if n_processed % _LOG_EVERY < len(course_recs):
                logger.info(
                    "  Traite %d / %d records (%.1f%%)...",
                    n_processed, total, 100 * n_processed / total if total else 0,
                )
                gc.collect()

    # Atomic rename
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Graph embedding build termine: %d features en %.1fs",
        n_written, elapsed,
    )
    logger.info(
        "  Horses: %d, Jockeys: %d, Trainers: %d, Interned IDs: %d",
        len(horse_graph), len(jockey_net), len(trainer_net), len(intern),
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for fname in feature_names:
        cnt = fill_counts[fname]
        pct = 100 * cnt / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", fname, cnt, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features graph/network a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/graph_embedding/)",
    )
    args = parser.parse_args()

    logger = setup_logging("graph_embedding_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "graph_embedding_features.jsonl"
    build_graph_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
