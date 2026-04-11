#!/usr/bin/env python3
"""
feature_builders.horse_rival_history_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse rival / head-to-head history features -- how each horse has performed
against specific competitors in the current field.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the h2h statistics -- no future leakage.  Features are
snapshotted BEFORE the current race updates the state.

Produces:
  - horse_rival_history.jsonl   in builder_outputs/horse_rival_history/

Features per partant (8):
  - hrv_avg_h2h_win_pct          : average h2h win rate against all current opponents this horse has faced before
  - hrv_nb_familiar_rivals       : count of horses in current field that this horse has raced against before
  - hrv_familiar_rival_pct       : nb_familiar / (nombre_partants - 1)
  - hrv_best_h2h_record          : best h2h win rate against any rival in field
  - hrv_worst_h2h_record         : worst h2h win rate against any rival in field
  - hrv_dominated_rivals         : count of rivals this horse has beaten >60% of the time (min 3 meetings)
  - hrv_dominator_rivals         : count of rivals that have beaten this horse >60% of the time (min 3 meetings)
  - hrv_experience_advantage     : this horse's total races minus average total races of field horses

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 streams output to disk via seek-based reads
  - gc.collect() called every 500K records
  - .tmp then atomic rename
  - h2h records stored only for pairs that have actually met (lazy)

Usage:
    python feature_builders/horse_rival_history_builder.py
    python feature_builders/horse_rival_history_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/horse_rival_history")

_LOG_EVERY = 500_000

# Minimum encounters to consider a meaningful h2h record
_MIN_MEETINGS = 3
# Threshold for domination / being dominated
_DOMINATION_THRESHOLD = 0.60


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        v = int(val)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _ordered_pair(a: str, b: str) -> tuple[str, str]:
    """Return (a, b) such that a < b to ensure canonical key ordering."""
    return (a, b) if a < b else (b, a)


# ===========================================================================
# H2H STATE  (compact: only pairs that have actually met)
# ===========================================================================


class _H2HState:
    """Tracks head-to-head records and per-horse race counts.

    h2h_records : dict[(horse_a, horse_b)] -> [wins_a, wins_b, total]
        where horse_a < horse_b alphabetically.
        Entries are created lazily on first encounter.

    horse_total_races : dict[horse_id] -> int
        Running count of races with a valid finishing position.
    """

    __slots__ = ("h2h_records", "horse_total_races")

    def __init__(self) -> None:
        self.h2h_records: dict[tuple[str, str], list[int]] = {}
        self.horse_total_races: dict[str, int] = defaultdict(int)

    def get_record(self, horse_a: str, horse_b: str) -> Optional[list[int]]:
        """Return [wins_a, wins_b, total] from a's perspective, or None if never met."""
        key = _ordered_pair(horse_a, horse_b)
        rec = self.h2h_records.get(key)
        if rec is None:
            return None
        if horse_a < horse_b:
            return [rec[0], rec[1], rec[2]]
        else:
            return [rec[1], rec[0], rec[2]]

    def snapshot_features(
        self,
        horse_id: str,
        field_horses: list[str],
        field_size: int,
    ) -> dict[str, Any]:
        """Compute the 8 features for horse_id given the current field.

        Uses ONLY pre-race state (before update).
        """
        feats: dict[str, Any] = {
            "hrv_avg_h2h_win_pct": None,
            "hrv_nb_familiar_rivals": None,
            "hrv_familiar_rival_pct": None,
            "hrv_best_h2h_record": None,
            "hrv_worst_h2h_record": None,
            "hrv_dominated_rivals": None,
            "hrv_dominator_rivals": None,
            "hrv_experience_advantage": None,
        }

        opponents = [h for h in field_horses if h != horse_id]
        if not opponents:
            return feats

        # --- h2h metrics ---
        win_rates: list[float] = []
        dominated = 0
        dominator = 0
        nb_familiar = 0

        for opp in opponents:
            rec = self.get_record(horse_id, opp)
            if rec is None or rec[2] == 0:
                continue  # never met
            nb_familiar += 1
            wins_me = rec[0]
            total = rec[2]
            wr = wins_me / total
            win_rates.append(wr)

            if total >= _MIN_MEETINGS:
                if wr > _DOMINATION_THRESHOLD:
                    dominated += 1
                if wr < (1.0 - _DOMINATION_THRESHOLD):
                    dominator += 1

        feats["hrv_nb_familiar_rivals"] = nb_familiar

        nb_possible_opponents = field_size - 1
        if nb_possible_opponents > 0:
            feats["hrv_familiar_rival_pct"] = round(nb_familiar / nb_possible_opponents, 4)

        if win_rates:
            feats["hrv_avg_h2h_win_pct"] = round(sum(win_rates) / len(win_rates), 4)
            feats["hrv_best_h2h_record"] = round(max(win_rates), 4)
            feats["hrv_worst_h2h_record"] = round(min(win_rates), 4)

        feats["hrv_dominated_rivals"] = dominated
        feats["hrv_dominator_rivals"] = dominator

        # --- experience advantage ---
        my_races = self.horse_total_races.get(horse_id, 0)
        opp_races = [self.horse_total_races.get(o, 0) for o in opponents]
        if opp_races:
            avg_opp = sum(opp_races) / len(opp_races)
            feats["hrv_experience_advantage"] = round(my_races - avg_opp, 2)

        return feats

    def update_race(self, finishers: list[tuple[str, int]]) -> None:
        """Update h2h records and race counts AFTER feature snapshot.

        finishers: list of (horse_id, position) for horses with valid positions.
        For each pair, the horse with lower (better) position gets a win.
        """
        n = len(finishers)
        for idx_a in range(n):
            horse_a, pos_a = finishers[idx_a]
            # Update total race count
            self.horse_total_races[horse_a] += 1

            for idx_b in range(idx_a + 1, n):
                horse_b, pos_b = finishers[idx_b]
                if pos_a == pos_b:
                    continue  # dead heat: skip

                key = _ordered_pair(horse_a, horse_b)
                rec = self.h2h_records.get(key)
                if rec is None:
                    rec = [0, 0, 0]
                    self.h2h_records[key] = rec

                rec[2] += 1  # total meetings

                if horse_a < horse_b:
                    if pos_a < pos_b:
                        rec[0] += 1  # a wins
                    else:
                        rec[1] += 1  # b wins
                else:
                    if pos_a < pos_b:
                        rec[1] += 1  # a wins (but stored as b-perspective index 1)
                    else:
                        rec[0] += 1  # b wins

        # Also count races for horses without valid position (non-finishers
        # won't appear in finishers, but they still participated).
        # -- handled by caller via separate update if needed --


# ===========================================================================
# MAIN BUILD (index+sort+seek, streaming output)
# ===========================================================================


def build_horse_rival_history_features(input_path: Path, output_path: Path, logger) -> int:
    """Build horse rival history features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Horse Rival History Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date, course_uid, num_pmu, offset) --
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
    state = _H2HState()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "hrv_avg_h2h_win_pct",
        "hrv_nb_familiar_rivals",
        "hrv_familiar_rival_pct",
        "hrv_best_h2h_record",
        "hrv_worst_h2h_record",
        "hrv_dominated_rivals",
        "hrv_dominator_rivals",
        "hrv_experience_advantage",
    ]
    fill_counts = {k: 0 for k in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
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

            # Read only this course's records from disk
            course_records: list[dict[str, Any]] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                position = _safe_int(rec.get("position_arrivee"))
                nb_partants = _safe_int(rec.get("nombre_partants"))
                partant_uid = rec.get("partant_uid")

                course_records.append({
                    "uid": partant_uid,
                    "horse_id": horse_id,
                    "position": position,
                    "nb_partants": nb_partants,
                })

            # Build field list (only horses with a valid ID)
            field_horses: list[str] = [
                r["horse_id"] for r in course_records if r["horse_id"]
            ]
            field_size = len(field_horses)

            # -- Snapshot BEFORE update for all partants in this course --
            for rec in course_records:
                horse_id = rec["horse_id"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if horse_id and field_size >= 2:
                    feats = state.snapshot_features(horse_id, field_horses, field_size)
                    features.update(feats)
                else:
                    for fn in feature_names:
                        features[fn] = None

                # Count fill rates
                for fn in feature_names:
                    if features.get(fn) is not None:
                        fill_counts[fn] += 1

                # Stream directly to output file
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update h2h records AFTER snapshotting (post-race, no leakage) --
            finishers: list[tuple[str, int]] = []
            for rec in course_records:
                if rec["horse_id"] and rec["position"] is not None:
                    finishers.append((rec["horse_id"], rec["position"]))

            if len(finishers) >= 2:
                state.update_race(finishers)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Horse rival history build termine: %d features en %.1fs (paires h2h: %d, chevaux: %d)",
        n_written, elapsed, len(state.h2h_records), len(state.horse_total_races),
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
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features horse rival history a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/horse_rival_history/)",
    )
    args = parser.parse_args()

    logger = setup_logging("horse_rival_history_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "horse_rival_history.jsonl"
    build_horse_rival_history_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
