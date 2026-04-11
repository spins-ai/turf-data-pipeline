#!/usr/bin/env python3
"""
feature_builders.pair_interaction_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pairwise interaction features between horse, jockey, trainer, and hippodrome.

Reads partants_master.jsonl in streaming mode, sorts chronologically, and
computes per-partant pairwise interaction features with strict temporal
integrity: for any partant at date D, only races with date < D contribute
to the statistics -- no future leakage.

Produces:
  - pair_interaction_deep.jsonl  in output/pair_interaction_deep/

Features per partant (10):
  - pid_horse_jockey_runs     : number of times this horse-jockey pair has raced together
  - pid_horse_jockey_wr       : win rate of this horse-jockey pair
  - pid_horse_trainer_runs    : number of times horse has been with this trainer
  - pid_horse_trainer_wr      : win rate of horse with this trainer
  - pid_jockey_hippo_runs     : jockey's total runs at this hippodrome
  - pid_jockey_hippo_wr       : jockey's win rate at this hippodrome
  - pid_trainer_hippo_runs    : trainer's total runs at this hippodrome
  - pid_trainer_hippo_wr      : trainer's win rate at this hippodrome
  - pid_triple_combo_runs     : horse-jockey-trainer triple combo runs
  - pid_triple_combo_wr       : horse-jockey-trainer triple combo win rate

Usage:
    python feature_builders/pair_interaction_deep_builder.py
    python feature_builders/pair_interaction_deep_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/pair_interaction_deep_builder.py --output-dir /path/to/output/
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pair_interaction_deep")

_LOG_EVERY = 500_000

# ===========================================================================
# HELPERS
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


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _normalise_str(val) -> Optional[str]:
    """Strip and lower-case a string field; return None if empty."""
    if not val:
        return None
    s = str(val).strip()
    return s if s else None


# ===========================================================================
# ACCUMULATOR
# ===========================================================================


class _PairStats:
    """Lightweight accumulator for a pair/tuple of entities.

    Tracks total runs and wins so we can emit:
      - *_runs : total races together
      - *_wr   : win rate (wins / runs), None when runs == 0
    """

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    # ------------------------------------------------------------------
    def snapshot_runs(self) -> int:
        return self.total

    def snapshot_wr(self) -> Optional[float]:
        if self.total == 0:
            return None
        return round(self.wins / self.total, 4)

    # ------------------------------------------------------------------
    def update(self, won: bool) -> None:
        self.total += 1
        if won:
            self.wins += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_pair_interaction_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build pairwise interaction features from partants_master.jsonl."""
    logger.info("=== Pair Interaction Deep Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------ #
    # Phase 1: stream and keep only the fields we need                    #
    # ------------------------------------------------------------------ #
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # --- Identifiers ---
        partant_uid = rec.get("partant_uid")
        course_uid = rec.get("course_uid", "")
        date_str = rec.get("date_reunion_iso", "")
        num_pmu = _safe_int(rec.get("num_pmu")) or 0

        # --- Entities ---
        horse = _normalise_str(rec.get("horse_id") or rec.get("nom_cheval"))
        jockey = _normalise_str(rec.get("jockey") or rec.get("nom_jockey"))
        trainer = _normalise_str(rec.get("entraineur") or rec.get("nom_entraineur"))
        hippo = _normalise_str(rec.get("hippodrome"))

        # --- Result ---
        position = _safe_int(rec.get("position_arrivee"))
        # is_gagnant is the canonical flag; fall back to position == 1
        raw_gagnant = rec.get("is_gagnant")
        if raw_gagnant is not None:
            won = bool(raw_gagnant)
        else:
            won = position == 1 if position is not None else False

        slim_records.append({
            "uid": partant_uid,
            "date": date_str,
            "course": course_uid,
            "num": num_pmu,
            "horse": horse,
            "jockey": jockey,
            "trainer": trainer,
            "hippo": hippo,
            "won": won,
        })

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # ------------------------------------------------------------------ #
    # Phase 2: sort chronologically                                       #
    # ------------------------------------------------------------------ #
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------ #
    # Phase 3: process course-by-course, snapshot BEFORE update          #
    # ------------------------------------------------------------------ #
    t2 = time.time()

    # State dicts — keys are tuples, values are _PairStats
    horse_jockey:  dict[tuple, _PairStats] = defaultdict(_PairStats)
    horse_trainer: dict[tuple, _PairStats] = defaultdict(_PairStats)
    jockey_hippo:  dict[tuple, _PairStats] = defaultdict(_PairStats)
    trainer_hippo: dict[tuple, _PairStats] = defaultdict(_PairStats)
    triple_combo:  dict[tuple, _PairStats] = defaultdict(_PairStats)

    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    i = 0
    while i < total:
        # Gather all partants in the same course
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        # -- Snapshot pre-race features (BEFORE update) --
        for rec in course_group:
            horse   = rec["horse"]
            jockey  = rec["jockey"]
            trainer = rec["trainer"]
            hippo   = rec["hippo"]

            feat: dict[str, Any] = {"partant_uid": rec["uid"]}

            # horse-jockey
            if horse and jockey:
                st = horse_jockey.get((horse, jockey))
                feat["pid_horse_jockey_runs"] = st.snapshot_runs() if st else 0
                feat["pid_horse_jockey_wr"]   = st.snapshot_wr()   if st else None
            else:
                feat["pid_horse_jockey_runs"] = None
                feat["pid_horse_jockey_wr"]   = None

            # horse-trainer
            if horse and trainer:
                st = horse_trainer.get((horse, trainer))
                feat["pid_horse_trainer_runs"] = st.snapshot_runs() if st else 0
                feat["pid_horse_trainer_wr"]   = st.snapshot_wr()   if st else None
            else:
                feat["pid_horse_trainer_runs"] = None
                feat["pid_horse_trainer_wr"]   = None

            # jockey-hippo
            if jockey and hippo:
                st = jockey_hippo.get((jockey, hippo))
                feat["pid_jockey_hippo_runs"] = st.snapshot_runs() if st else 0
                feat["pid_jockey_hippo_wr"]   = st.snapshot_wr()   if st else None
            else:
                feat["pid_jockey_hippo_runs"] = None
                feat["pid_jockey_hippo_wr"]   = None

            # trainer-hippo
            if trainer and hippo:
                st = trainer_hippo.get((trainer, hippo))
                feat["pid_trainer_hippo_runs"] = st.snapshot_runs() if st else 0
                feat["pid_trainer_hippo_wr"]   = st.snapshot_wr()   if st else None
            else:
                feat["pid_trainer_hippo_runs"] = None
                feat["pid_trainer_hippo_wr"]   = None

            # triple combo (horse, jockey, trainer)
            if horse and jockey and trainer:
                st = triple_combo.get((horse, jockey, trainer))
                feat["pid_triple_combo_runs"] = st.snapshot_runs() if st else 0
                feat["pid_triple_combo_wr"]   = st.snapshot_wr()   if st else None
            else:
                feat["pid_triple_combo_runs"] = None
                feat["pid_triple_combo_wr"]   = None

            results.append(feat)

        # -- Update states AFTER snapshotting (post-race) --
        for rec in course_group:
            horse   = rec["horse"]
            jockey  = rec["jockey"]
            trainer = rec["trainer"]
            hippo   = rec["hippo"]
            won     = rec["won"]

            if horse and jockey:
                horse_jockey[(horse, jockey)].update(won)

            if horse and trainer:
                horse_trainer[(horse, trainer)].update(won)

            if jockey and hippo:
                jockey_hippo[(jockey, hippo)].update(won)

            if trainer and hippo:
                trainer_hippo[(trainer, hippo)].update(won)

            if horse and jockey and trainer:
                triple_combo[(horse, jockey, trainer)].update(won)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Pair Interaction build termine: %d features en %.1fs "
        "(horse-jockey pairs: %d, horse-trainer: %d, "
        "jockey-hippo: %d, trainer-hippo: %d, triples: %d)",
        len(results), elapsed,
        len(horse_jockey), len(horse_trainer),
        len(jockey_hippo), len(trainer_hippo),
        len(triple_combo),
    )

    # Free large intermediate structures before returning
    del slim_records, horse_jockey, horse_trainer, jockey_hippo, trainer_hippo, triple_combo
    gc.collect()

    return results


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path: CLI arg > well-known candidates."""
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features pairwise interaction (horse/jockey/trainer/hippo)"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pair_interaction_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("pair_interaction_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_pair_interaction_features(input_path, logger)

    # Save
    out_path = output_dir / "pair_interaction_deep.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
