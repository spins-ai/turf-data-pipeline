#!/usr/bin/env python3
"""
feature_builders.encounter_history_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Encounter history features -- how familiar are the horses in this race
with each other (simplified version).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant encounter history features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the encounter stats -- no future leakage.

Produces:
  - encounter_history.jsonl   in output/encounter_history/

Features per partant (8):
  - enc_horse_total_opponents_faced : total distinct opponents ever raced against
  - enc_horse_races_count           : total races run (career counter)
  - enc_horse_avg_field_size        : average field size across career
  - enc_horse_biggest_field         : largest field size faced
  - enc_horse_smallest_field        : smallest field size faced
  - enc_horse_field_size_variety    : std of field sizes (varied conditions)
  - enc_current_field_vs_avg        : current nombre_partants / horse avg field size
  - enc_is_big_field_experience     : 1 if horse has run in 16+ field >= 3 times

Memory-optimised version:
  - Phase 1 reads only minimal tuples for sorting
  - Phase 2 streams output to disk instead of accumulating in a list
  - State per horse: opponents set, races count, field_sizes deque(50),
    biggest, smallest -- NO per-pair data
  - gc.collect() called every 500K records

Usage:
    python feature_builders/encounter_history_builder.py
    python feature_builders/encounter_history_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/encounter_history")

_LOG_EVERY = 500_000

# Big field threshold
_BIG_FIELD_THRESHOLD = 16
_BIG_FIELD_MIN_TIMES = 3

# Max field sizes to remember per horse (rolling window)
_FIELD_SIZE_WINDOW = 50


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
# STATE TRACKER
# ===========================================================================


class _HorseEncounterState:
    """Lightweight per-horse encounter state.

    Tracks total distinct opponents (set of horse ids), race count,
    and field size stats via a bounded deque.  No per-pair storage.
    """

    __slots__ = ("opponents", "races", "field_sizes", "biggest", "smallest", "big_field_count")

    def __init__(self) -> None:
        self.opponents: set[str] = set()
        self.races: int = 0
        self.field_sizes: deque = deque(maxlen=_FIELD_SIZE_WINDOW)
        self.biggest: int = 0
        self.smallest: int = 999
        self.big_field_count: int = 0

    def snapshot(self, current_field_size: Optional[int]) -> dict[str, Any]:
        """Return features BEFORE updating with current race."""
        if self.races == 0:
            return {
                "enc_horse_total_opponents_faced": None,
                "enc_horse_races_count": 0,
                "enc_horse_avg_field_size": None,
                "enc_horse_biggest_field": None,
                "enc_horse_smallest_field": None,
                "enc_horse_field_size_variety": None,
                "enc_current_field_vs_avg": None,
                "enc_is_big_field_experience": 0,
            }

        total_opp = len(self.opponents)
        avg_fs = sum(self.field_sizes) / len(self.field_sizes) if self.field_sizes else None

        # Field size variety = std dev
        variety = None
        if len(self.field_sizes) >= 2:
            mean_fs = sum(self.field_sizes) / len(self.field_sizes)
            variance = sum((x - mean_fs) ** 2 for x in self.field_sizes) / len(self.field_sizes)
            variety = round(math.sqrt(variance), 4)

        # Current field vs avg
        field_vs_avg = None
        if avg_fs and avg_fs > 0 and current_field_size and current_field_size > 0:
            field_vs_avg = round(current_field_size / avg_fs, 4)

        # Big field experience
        is_big = 1 if self.big_field_count >= _BIG_FIELD_MIN_TIMES else 0

        return {
            "enc_horse_total_opponents_faced": total_opp,
            "enc_horse_races_count": self.races,
            "enc_horse_avg_field_size": round(avg_fs, 4) if avg_fs is not None else None,
            "enc_horse_biggest_field": self.biggest if self.biggest > 0 else None,
            "enc_horse_smallest_field": self.smallest if self.smallest < 999 else None,
            "enc_horse_field_size_variety": variety,
            "enc_current_field_vs_avg": field_vs_avg,
            "enc_is_big_field_experience": is_big,
        }

    def update(self, opponents_in_race: list[str], field_size: int) -> None:
        """Update state AFTER snapshotting."""
        self.races += 1
        for opp in opponents_in_race:
            self.opponents.add(opp)
        if field_size > 0:
            self.field_sizes.append(field_size)
            if field_size > self.biggest:
                self.biggest = field_size
            if field_size < self.smallest:
                self.smallest = field_size
            if field_size >= _BIG_FIELD_THRESHOLD:
                self.big_field_count += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_encounter_history_features(input_path: Path, output_path: Path, logger) -> int:
    """Build encounter history features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Encounter History Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
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

    # -- Phase 2: Sort the lightweight index --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseEncounterState] = defaultdict(_HorseEncounterState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "enc_horse_total_opponents_faced": 0,
        "enc_horse_races_count": 0,
        "enc_horse_avg_field_size": 0,
        "enc_horse_biggest_field": 0,
        "enc_horse_smallest_field": 0,
        "enc_horse_field_size_variety": 0,
        "enc_current_field_vs_avg": 0,
        "enc_is_big_field_experience": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            nb_partants = rec.get("nombre_partants") or 0
            try:
                nb_partants = int(nb_partants)
            except (ValueError, TypeError):
                nb_partants = 0

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "cheval": rec.get("nom_cheval") or rec.get("horse_id"),
                "nb_partants": nb_partants,
            }

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

            # Read only this course's records from disk
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            # Build field horse list
            field_horses: list[str] = [
                rec["cheval"] for rec in course_group if rec["cheval"]
            ]
            field_size = course_group[0]["nb_partants"] if course_group else len(field_horses)
            if field_size <= 0:
                field_size = len(field_horses)

            # -- Snapshot BEFORE update (temporal integrity) --
            for rec in course_group:
                cheval = rec["cheval"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if cheval:
                    state = horse_state[cheval]
                    snap = state.snapshot(field_size)
                    features.update(snap)

                    # Track fill counts
                    for k in fill_counts:
                        if features.get(k) is not None:
                            fill_counts[k] += 1
                else:
                    for k in fill_counts:
                        features[k] = None

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER snapshotting --
            for rec in course_group:
                cheval = rec["cheval"]
                if not cheval:
                    continue
                opponents = [h for h in field_horses if h != cheval]
                horse_state[cheval].update(opponents, field_size)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Encounter history build termine: %d features en %.1fs (chevaux uniques: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written if n_written else 0)

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
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features encounter history a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: encounter_history/)",
    )
    args = parser.parse_args()

    logger = setup_logging("encounter_history_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "encounter_history.jsonl"
    build_encounter_history_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
