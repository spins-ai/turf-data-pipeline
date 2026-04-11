#!/usr/bin/env python3
"""
feature_builders.network_centrality_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Network/graph centrality features about connections between horses, jockeys,
trainers, and owners. Designed to feed GNN (Graph Neural Network) models.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically course-by-course, and computes per-partant network features
using running counters (NOT actual graph objects) to avoid memory explosion.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the network statistics -- no future leakage.

Produces:
  - network_centrality.jsonl   in builder_outputs/network_centrality/

Features per partant (15):
  - net_horse_jockey_degree       : number of unique jockeys who have ridden this horse
  - net_horse_trainer_degree      : number of unique trainers who have trained this horse
  - net_jockey_network_size       : number of unique horses this jockey has ridden
  - net_trainer_network_size      : number of unique horses this trainer has trained
  - net_owner_network_size        : number of unique horses this owner owns
  - net_jockey_trainer_strength   : number of races this jockey+trainer pair did together
  - net_jockey_loyalty            : fraction of rides for jockey's most frequent trainer
  - net_trainer_diversification   : number of unique jockeys this trainer has used
  - net_jockey_hippo_specialization : fraction of jockey's rides at this hippodrome
  - net_trainer_hippo_specialization: fraction of trainer's races at this hippodrome
  - net_cross_connection          : does this horse's jockey also ride for this trainer frequently?
  - net_owner_trainer_loyalty     : fraction of owner's horses with this trainer
  - net_horse_total_connections   : total unique entities (jockeys+trainers+owners) connected
  - net_jockey_trainer_win_rate   : jockey win rate with this specific trainer (shrunk)
  - net_field_network_overlap     : count of other horses in field sharing same jockey/trainer/owner

Memory strategy:
  - Sets with cap (MAX_SET_SIZE) for unique entity tracking; when exceeded,
    only count is kept (approximate but bounded memory).
  - defaultdict(int) counters for pairwise connection counts.
  - gc.collect() every 500K records.

Usage:
    python feature_builders/network_centrality_builder.py
    python feature_builders/network_centrality_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/network_centrality")

# Progress log every N records
_LOG_EVERY = 500_000

# Max set size before switching to count-only mode to cap memory
MAX_SET_SIZE = 500

# Bayesian shrinkage for jockey-trainer win rate
PRIOR_WEIGHT = 5
GLOBAL_WIN_RATE = 0.08


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
# CAPPED SET - memory-bounded unique tracking
# ===========================================================================


class CappedSet:
    """Tracks unique items up to MAX_SET_SIZE, then switches to count-only.

    Once the set exceeds the cap, we store the frozen count and keep
    incrementing a counter for each new add, but no longer store items.
    This bounds memory to O(MAX_SET_SIZE) per entity.
    """

    __slots__ = ("_items", "_overflow_count", "_capped")

    def __init__(self) -> None:
        self._items: set = set()
        self._overflow_count: int = 0
        self._capped: bool = False

    def add(self, item) -> None:
        if self._capped:
            self._overflow_count += 1
        else:
            self._items.add(item)
            if len(self._items) > MAX_SET_SIZE:
                self._overflow_count = len(self._items)
                self._items = set()  # free memory
                self._capped = True

    def count(self) -> int:
        if self._capped:
            return self._overflow_count
        return len(self._items)

    def contains(self, item) -> bool:
        """Only reliable when not capped."""
        if self._capped:
            return False  # unknown
        return item in self._items


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class NetworkState:
    """All network state trackers, memory-optimised with CappedSets."""

    def __init__(self) -> None:
        # Horse -> unique jockeys/trainers/owners
        self.horse_jockeys: dict[str, CappedSet] = defaultdict(CappedSet)
        self.horse_trainers: dict[str, CappedSet] = defaultdict(CappedSet)
        self.horse_owners: dict[str, CappedSet] = defaultdict(CappedSet)

        # Jockey -> unique horses
        self.jockey_horses: dict[str, CappedSet] = defaultdict(CappedSet)
        # Trainer -> unique horses
        self.trainer_horses: dict[str, CappedSet] = defaultdict(CappedSet)
        # Owner -> unique horses
        self.owner_horses: dict[str, CappedSet] = defaultdict(CappedSet)

        # Trainer -> unique jockeys
        self.trainer_jockeys: dict[str, CappedSet] = defaultdict(CappedSet)

        # (jockey, trainer) -> race count
        self.jockey_trainer_races: dict[tuple[str, str], int] = defaultdict(int)
        # (jockey, trainer) -> win count
        self.jockey_trainer_wins: dict[tuple[str, str], int] = defaultdict(int)

        # Jockey -> total rides
        self.jockey_total_rides: dict[str, int] = defaultdict(int)
        # Jockey -> {trainer: ride_count} (for loyalty)
        self.jockey_trainer_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        # Trainer -> total races
        self.trainer_total_races: dict[str, int] = defaultdict(int)

        # Jockey -> {hippodrome: ride_count}
        self.jockey_hippo_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        # Trainer -> {hippodrome: race_count}
        self.trainer_hippo_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        # Owner -> {trainer: horse_count} for owner-trainer loyalty
        self.owner_trainer_horses: dict[str, dict[str, CappedSet]] = defaultdict(lambda: defaultdict(CappedSet))

        # For field overlap: horse -> set of (jockey, trainer, owner) history
        # We store horse -> set of associated entity keys
        self.horse_jockey_set: dict[str, set] = defaultdict(set)
        self.horse_trainer_set: dict[str, set] = defaultdict(set)
        self.horse_owner_set: dict[str, set] = defaultdict(set)


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================


def _safe_rate(num: int, denom: int, ndigits: int = 4) -> Optional[float]:
    if denom < 1:
        return None
    return round(num / denom, ndigits)


def _bayes_rate(wins: int, total: int) -> Optional[float]:
    """Shrinkage win rate toward global average."""
    if total < 1:
        return None
    return round(
        (GLOBAL_WIN_RATE * PRIOR_WEIGHT + wins) / (PRIOR_WEIGHT + total), 4
    )


def _compute_features(
    rec: dict,
    state: NetworkState,
    field_horses: list[dict],
) -> dict:
    """Compute network features for a single partant (snapshot before update)."""

    horse = rec.get("horse_id") or ""
    jockey = rec.get("jockey_driver") or ""
    trainer = rec.get("entraineur") or ""
    owner = rec.get("proprietaire") or ""
    hippo = rec.get("hippodrome_normalise") or ""

    features: dict[str, Any] = {"partant_uid": rec.get("partant_uid")}

    # --- 1. Horse jockey degree ---
    if horse:
        features["net_horse_jockey_degree"] = state.horse_jockeys[horse].count() if horse in state.horse_jockeys else 0
    else:
        features["net_horse_jockey_degree"] = None

    # --- 2. Horse trainer degree ---
    if horse:
        features["net_horse_trainer_degree"] = state.horse_trainers[horse].count() if horse in state.horse_trainers else 0
    else:
        features["net_horse_trainer_degree"] = None

    # --- 3. Jockey network size ---
    if jockey:
        features["net_jockey_network_size"] = state.jockey_horses[jockey].count() if jockey in state.jockey_horses else 0
    else:
        features["net_jockey_network_size"] = None

    # --- 4. Trainer network size ---
    if trainer:
        features["net_trainer_network_size"] = state.trainer_horses[trainer].count() if trainer in state.trainer_horses else 0
    else:
        features["net_trainer_network_size"] = None

    # --- 5. Owner network size ---
    if owner:
        features["net_owner_network_size"] = state.owner_horses[owner].count() if owner in state.owner_horses else 0
    else:
        features["net_owner_network_size"] = None

    # --- 6. Jockey-trainer connection strength ---
    if jockey and trainer:
        features["net_jockey_trainer_strength"] = state.jockey_trainer_races.get((jockey, trainer), 0)
    else:
        features["net_jockey_trainer_strength"] = None

    # --- 7. Jockey loyalty (fraction of rides for most frequent trainer) ---
    if jockey and jockey in state.jockey_trainer_counts:
        tc = state.jockey_trainer_counts[jockey]
        total_rides = state.jockey_total_rides.get(jockey, 0)
        if total_rides > 0 and tc:
            max_count = max(tc.values())
            features["net_jockey_loyalty"] = round(max_count / total_rides, 4)
        else:
            features["net_jockey_loyalty"] = None
    else:
        features["net_jockey_loyalty"] = None

    # --- 8. Trainer diversification ---
    if trainer:
        features["net_trainer_diversification"] = state.trainer_jockeys[trainer].count() if trainer in state.trainer_jockeys else 0
    else:
        features["net_trainer_diversification"] = None

    # --- 9. Jockey hippodrome specialization ---
    if jockey and hippo:
        total_rides = state.jockey_total_rides.get(jockey, 0)
        hippo_rides = state.jockey_hippo_counts.get(jockey, {}).get(hippo, 0)
        if total_rides > 0:
            features["net_jockey_hippo_specialization"] = round(hippo_rides / total_rides, 4)
        else:
            features["net_jockey_hippo_specialization"] = None
    else:
        features["net_jockey_hippo_specialization"] = None

    # --- 10. Trainer hippodrome specialization ---
    if trainer and hippo:
        total_races = state.trainer_total_races.get(trainer, 0)
        hippo_races = state.trainer_hippo_counts.get(trainer, {}).get(hippo, 0)
        if total_races > 0:
            features["net_trainer_hippo_specialization"] = round(hippo_races / total_races, 4)
        else:
            features["net_trainer_hippo_specialization"] = None
    else:
        features["net_trainer_hippo_specialization"] = None

    # --- 11. Cross-connection: jockey rides for this trainer frequently? ---
    if jockey and trainer:
        jt_count = state.jockey_trainer_races.get((jockey, trainer), 0)
        total_rides = state.jockey_total_rides.get(jockey, 0)
        if total_rides > 0:
            features["net_cross_connection"] = round(jt_count / total_rides, 4)
        else:
            features["net_cross_connection"] = None
    else:
        features["net_cross_connection"] = None

    # --- 12. Owner-trainer loyalty ---
    if owner and trainer and owner in state.owner_trainer_horses:
        ot_map = state.owner_trainer_horses[owner]
        total_owner_horses = state.owner_horses[owner].count() if owner in state.owner_horses else 0
        this_trainer_horses = ot_map[trainer].count() if trainer in ot_map else 0
        if total_owner_horses > 0:
            features["net_owner_trainer_loyalty"] = round(this_trainer_horses / total_owner_horses, 4)
        else:
            features["net_owner_trainer_loyalty"] = None
    else:
        features["net_owner_trainer_loyalty"] = None

    # --- 13. Horse total connections ---
    if horse:
        n_j = state.horse_jockeys[horse].count() if horse in state.horse_jockeys else 0
        n_t = state.horse_trainers[horse].count() if horse in state.horse_trainers else 0
        n_o = state.horse_owners[horse].count() if horse in state.horse_owners else 0
        features["net_horse_total_connections"] = n_j + n_t + n_o
    else:
        features["net_horse_total_connections"] = None

    # --- 14. Jockey win rate with this specific trainer (shrunk) ---
    if jockey and trainer:
        pair = (jockey, trainer)
        jt_wins = state.jockey_trainer_wins.get(pair, 0)
        jt_races = state.jockey_trainer_races.get(pair, 0)
        features["net_jockey_trainer_win_rate"] = _bayes_rate(jt_wins, jt_races)
    else:
        features["net_jockey_trainer_win_rate"] = None

    # --- 15. Field network overlap ---
    # Count how many OTHER horses in this field share same jockey/trainer/owner history
    if horse:
        overlap = 0
        my_jockeys = state.horse_jockey_set.get(horse, set())
        my_trainers = state.horse_trainer_set.get(horse, set())
        my_owners = state.horse_owner_set.get(horse, set())

        for other in field_horses:
            other_horse = other.get("horse_id") or ""
            if not other_horse or other_horse == horse:
                continue
            # Check if other horse shares any jockey, trainer, or owner
            other_jockeys = state.horse_jockey_set.get(other_horse, set())
            other_trainers = state.horse_trainer_set.get(other_horse, set())
            other_owners = state.horse_owner_set.get(other_horse, set())

            if (my_jockeys & other_jockeys) or (my_trainers & other_trainers) or (my_owners & other_owners):
                overlap += 1

        features["net_field_network_overlap"] = overlap
    else:
        features["net_field_network_overlap"] = None

    return features


def _update_state(rec: dict, state: NetworkState) -> None:
    """Update network state AFTER snapshot (post-race)."""
    horse = rec.get("horse_id") or ""
    jockey = rec.get("jockey_driver") or ""
    trainer = rec.get("entraineur") or ""
    owner = rec.get("proprietaire") or ""
    hippo = rec.get("hippodrome_normalise") or ""
    is_winner = bool(rec.get("is_gagnant"))

    if horse and jockey:
        state.horse_jockeys[horse].add(jockey)
        state.jockey_horses[jockey].add(horse)
        # For field overlap (keep these as plain sets, capped at MAX_SET_SIZE)
        s = state.horse_jockey_set[horse]
        if len(s) < MAX_SET_SIZE:
            s.add(jockey)

    if horse and trainer:
        state.horse_trainers[horse].add(trainer)
        state.trainer_horses[trainer].add(horse)
        s = state.horse_trainer_set[horse]
        if len(s) < MAX_SET_SIZE:
            s.add(trainer)

    if horse and owner:
        state.horse_owners[horse].add(owner)
        state.owner_horses[owner].add(horse)
        s = state.horse_owner_set[horse]
        if len(s) < MAX_SET_SIZE:
            s.add(owner)

    if jockey and trainer:
        state.jockey_trainer_races[(jockey, trainer)] += 1
        if is_winner:
            state.jockey_trainer_wins[(jockey, trainer)] += 1
        state.trainer_jockeys[trainer].add(jockey)

    if jockey:
        state.jockey_total_rides[jockey] += 1
        if trainer:
            state.jockey_trainer_counts[jockey][trainer] += 1
        if hippo:
            state.jockey_hippo_counts[jockey][hippo] += 1

    if trainer:
        state.trainer_total_races[trainer] += 1
        if hippo:
            state.trainer_hippo_counts[trainer][hippo] += 1

    if owner and trainer and horse:
        state.owner_trainer_horses[owner][trainer].add(horse)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_network_centrality_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build network centrality features from partants_master.jsonl.

    Phase 1: Build lightweight index for chronological sorting.
    Phase 2: Sort index.
    Phase 3: Process course-by-course with snapshot-before-update.

    Returns the total number of feature records written.
    """
    logger.info("=== Network Centrality Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    index: list[tuple[str, str, int, int]] = []
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
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    state = NetworkState()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "net_horse_jockey_degree",
        "net_horse_trainer_degree",
        "net_jockey_network_size",
        "net_trainer_network_size",
        "net_owner_network_size",
        "net_jockey_trainer_strength",
        "net_jockey_loyalty",
        "net_trainer_diversification",
        "net_jockey_hippo_specialization",
        "net_trainer_hippo_specialization",
        "net_cross_connection",
        "net_owner_trainer_loyalty",
        "net_horse_total_connections",
        "net_jockey_trainer_win_rate",
        "net_field_network_overlap",
    ]
    fill_counts = {name: 0 for name in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
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

            # Read all records for this course
            course_group = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Snapshot: compute features BEFORE updating state --
            for rec in course_group:
                features = _compute_features(rec, state, course_group)

                # Track fill rates
                for fname in feature_names:
                    val = features.get(fname)
                    if val is not None:
                        fill_counts[fname] += 1

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update state AFTER snapshot --
            for rec in course_group:
                _update_state(rec, state)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Network centrality build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features reseau/centralite a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/network_centrality/)",
    )
    args = parser.parse_args()

    logger = setup_logging("network_centrality_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "network_centrality.jsonl"
    build_network_centrality_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
