#!/usr/bin/env python3
"""
feature_builders.owner_patterns_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner (proprietaire) pattern features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant owner pattern features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - owner_patterns.jsonl  in output/owner_patterns/

Features per partant (8):
  - own_owner_win_rate          : owner's overall win rate
  - own_owner_runners_total     : total runners for this owner so far
  - own_owner_place_rate        : owner's place rate
  - own_owner_discipline_wr     : owner win rate in this discipline
  - own_owner_trainer_combo_wr  : win rate of this owner+trainer combination
  - own_owner_nb_horses         : number of distinct horses this owner has run
  - own_owner_avg_gains         : average gains_carriere of owner's horses
  - own_is_major_owner          : 1 if owner has 50+ runners (professional operation)

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full records)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records

Usage:
    python feature_builders/owner_patterns_builder.py
    python feature_builders/owner_patterns_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/owner_patterns")

_LOG_EVERY = 500_000

# Threshold for "major owner" flag
_MAJOR_OWNER_THRESHOLD = 50


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _OwnerState:
    """Per-owner cumulative statistics tracker.

    Tracks wins, places, total runners, discipline-level stats,
    trainer combo stats, distinct horses, and cumulative gains.
    """

    __slots__ = (
        "wins", "places", "total",
        "discipline_stats",    # {discipline: [wins, total]}
        "trainer_combo_stats", # {trainer: [wins, total]}
        "horses_set",          # set of horse_ids
        "gains_sum",           # cumulative gains_carriere across all horses
    )

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.total: int = 0
        self.discipline_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.trainer_combo_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.horses_set: set[str] = set()
        self.gains_sum: float = 0.0

    def snapshot(
        self,
        discipline: Optional[str],
        trainer: Optional[str],
    ) -> dict[str, Any]:
        """Compute features BEFORE updating state."""
        feats: dict[str, Any] = {
            "own_owner_win_rate": None,
            "own_owner_runners_total": None,
            "own_owner_place_rate": None,
            "own_owner_discipline_wr": None,
            "own_owner_trainer_combo_wr": None,
            "own_owner_nb_horses": None,
            "own_owner_avg_gains": None,
            "own_is_major_owner": None,
        }

        if self.total == 0:
            return feats

        # 1. own_owner_win_rate
        feats["own_owner_win_rate"] = round(self.wins / self.total, 4)

        # 2. own_owner_runners_total
        feats["own_owner_runners_total"] = self.total

        # 3. own_owner_place_rate
        feats["own_owner_place_rate"] = round(self.places / self.total, 4)

        # 4. own_owner_discipline_wr
        if discipline and discipline in self.discipline_stats:
            dw, dt = self.discipline_stats[discipline]
            if dt > 0:
                feats["own_owner_discipline_wr"] = round(dw / dt, 4)

        # 5. own_owner_trainer_combo_wr
        if trainer and trainer in self.trainer_combo_stats:
            tw, tt = self.trainer_combo_stats[trainer]
            if tt > 0:
                feats["own_owner_trainer_combo_wr"] = round(tw / tt, 4)

        # 6. own_owner_nb_horses
        feats["own_owner_nb_horses"] = len(self.horses_set)

        # 7. own_owner_avg_gains
        nb_horses = len(self.horses_set)
        if nb_horses > 0:
            feats["own_owner_avg_gains"] = round(self.gains_sum / nb_horses, 2)

        # 8. own_is_major_owner
        feats["own_is_major_owner"] = int(self.total >= _MAJOR_OWNER_THRESHOLD)

        return feats

    def update(
        self,
        is_winner: bool,
        is_place: bool,
        discipline: Optional[str],
        trainer: Optional[str],
        horse_id: Optional[str],
        gains_carriere: Optional[float],
    ) -> None:
        """Update state AFTER snapshot."""
        self.total += 1
        if is_winner:
            self.wins += 1
        if is_place:
            self.places += 1

        # Discipline stats
        if discipline:
            self.discipline_stats[discipline][1] += 1
            if is_winner:
                self.discipline_stats[discipline][0] += 1

        # Trainer combo stats
        if trainer:
            self.trainer_combo_stats[trainer][1] += 1
            if is_winner:
                self.trainer_combo_stats[trainer][0] += 1

        # Track distinct horses and their gains
        if horse_id:
            self.horses_set.add(horse_id)
        if gains_carriere is not None:
            self.gains_sum += gains_carriere


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v
    except (ValueError, TypeError):
        return None


def _norm_str(val: Any) -> Optional[str]:
    """Normalise a string field (strip + upper) or return None."""
    if not val or not isinstance(val, str):
        return None
    v = val.strip().upper()
    return v if v else None


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek-based processing)
# ===========================================================================


def build_owner_patterns_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build owner pattern features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Owner Patterns Builder (memory-optimised) ===")
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
    owner_state: dict[str, _OwnerState] = defaultdict(_OwnerState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "own_owner_win_rate",
        "own_owner_runners_total",
        "own_owner_place_rate",
        "own_owner_discipline_wr",
        "own_owner_trainer_combo_wr",
        "own_owner_nb_horses",
        "own_owner_avg_gains",
        "own_is_major_owner",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_fields(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            return {
                "uid": rec.get("partant_uid"),
                "owner": _norm_str(rec.get("proprietaire")),
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "trainer": _norm_str(rec.get("entraineur")),
                "discipline": _norm_str(rec.get("discipline")),
                "is_gagnant": bool(rec.get("is_gagnant")),
                "is_place": bool(rec.get("is_place")),
                "gains_carriere": _safe_float(rec.get("gains_carriere_euros")),
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
            course_group = [
                _extract_fields(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race stats for all partants (temporal integrity) --
            post_updates: list[tuple[
                Optional[str],   # owner
                bool,            # is_winner
                bool,            # is_place
                Optional[str],   # discipline
                Optional[str],   # trainer
                Optional[str],   # horse_id
                Optional[float], # gains_carriere
            ]] = []

            for rec in course_group:
                owner = rec["owner"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if owner:
                    feats = owner_state[owner].snapshot(
                        discipline=rec["discipline"],
                        trainer=rec["trainer"],
                    )
                    features.update(feats)
                else:
                    for k in feature_keys:
                        features[k] = None

                # Track fill rates
                for k in feature_keys:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update
                post_updates.append((
                    owner,
                    rec["is_gagnant"],
                    rec["is_place"],
                    rec["discipline"],
                    rec["trainer"],
                    rec["horse_id"],
                    rec["gains_carriere"],
                ))

            # -- Update states after race (no leakage) --
            for owner, is_winner, is_place, discipline, trainer, horse_id, gains in post_updates:
                if owner:
                    owner_state[owner].update(
                        is_winner=is_winner,
                        is_place=is_place,
                        discipline=discipline,
                        trainer=trainer,
                        horse_id=horse_id,
                        gains_carriere=gains,
                    )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Owner patterns build termine: %d features en %.1fs (owners: %d)",
        n_written, elapsed, len(owner_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)", k, v, n_written,
            100 * v / n_written if n_written else 0,
        )

    return n_written


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features owner patterns a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("owner_patterns_builder")

    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Fichier introuvable: {input_path}")
    else:
        input_path = INPUT_PARTANTS
        if not input_path.exists():
            raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "owner_patterns.jsonl"
    build_owner_patterns_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
