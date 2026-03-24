#!/usr/bin/env python3
"""
feature_builders.hippodrome_expertise_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Measures how well a horse and jockey perform at a specific hippodrome (track).

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant hippodrome-expertise features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the hippodrome statistics -- no future leakage.

Produces:
  - hippodrome_expertise.jsonl   in output/hippodrome_expertise/

Features per partant:
  - horse_hippo_win_rate    : horse's win rate at this hippodrome
  - horse_hippo_nb_runs     : times horse has raced here
  - jockey_hippo_win_rate   : jockey's win rate at this hippodrome
  - jockey_hippo_nb_runs    : times jockey has ridden here
  - hippo_specialist_score  : (horse_hippo_win_rate + jockey_hippo_win_rate) / 2
  - hippo_first_time        : 1 if horse has never raced at this hippodrome

Usage:
    python feature_builders/hippodrome_expertise_builder.py
    python feature_builders/hippodrome_expertise_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "hippodrome_expertise"

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


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# HIPPODROME EXPERTISE TRACKER
# ===========================================================================


class _HippoRecord:
    """A single past race result at a hippodrome."""

    __slots__ = ("date", "won")

    def __init__(self, date: datetime, won: bool) -> None:
        self.date = date
        self.won = won


class _EntityHippoState:
    """Per-(entity, hippodrome) accumulated state.

    Used for both horse-at-hippodrome and jockey-at-hippodrome tracking.
    """

    __slots__ = ("history",)

    def __init__(self) -> None:
        # history is kept sorted chronologically (appended in order)
        self.history: list[_HippoRecord] = []

    def snapshot(self, race_date: datetime) -> tuple[Optional[float], int]:
        """Compute win_rate and nb_runs using only races strictly before race_date.

        Returns:
            (win_rate, nb_runs) -- win_rate is None if nb_runs == 0.
        """
        wins = 0
        total = 0

        for rec in self.history:
            if rec.date >= race_date:
                break
            total += 1
            if rec.won:
                wins += 1

        win_rate = round(wins / total, 4) if total > 0 else None
        return win_rate, total

    def update(self, race_date: datetime, won: bool) -> None:
        """Add a race result (post-race)."""
        self.history.append(_HippoRecord(race_date, won))


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_hippodrome_expertise_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build hippodrome expertise features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory for sorting.
      2. Sort chronologically.
      3. Process record-by-record, snapshotting before update.
    """
    logger.info("=== Hippodrome Expertise Builder ===")
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
            "jockey": rec.get("nom_jockey"),
            "hippo": rec.get("hippodrome_normalise", ""),
            "gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process record by record --
    t2 = time.time()
    # Keys: (entity_name, hippodrome_normalise)
    horse_hippo_states: dict[tuple[str, str], _EntityHippoState] = defaultdict(_EntityHippoState)
    jockey_hippo_states: dict[tuple[str, str], _EntityHippoState] = defaultdict(_EntityHippoState)

    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by (date, course) to handle all partants in a course together
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        race_date = _parse_date(course_date_str)

        # -- Snapshot pre-race features for all partants in this course --
        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]
            hippo = rec["hippo"]

            horse_wr: Optional[float] = None
            horse_runs: int = 0
            jockey_wr: Optional[float] = None
            jockey_runs: int = 0

            if cheval and hippo and race_date:
                horse_wr, horse_runs = horse_hippo_states[(cheval, hippo)].snapshot(race_date)

            if jockey and hippo and race_date:
                jockey_wr, jockey_runs = jockey_hippo_states[(jockey, hippo)].snapshot(race_date)

            # Combined specialist score
            if horse_wr is not None and jockey_wr is not None:
                specialist = round((horse_wr + jockey_wr) / 2.0, 4)
            elif horse_wr is not None:
                specialist = round(horse_wr / 2.0, 4)
            elif jockey_wr is not None:
                specialist = round(jockey_wr / 2.0, 4)
            else:
                specialist = None

            # First time at this hippodrome
            first_time = 1 if horse_runs == 0 else 0

            features = {
                "partant_uid": rec["uid"],
                "horse_hippo_win_rate": horse_wr,
                "horse_hippo_nb_runs": horse_runs,
                "jockey_hippo_win_rate": jockey_wr,
                "jockey_hippo_nb_runs": jockey_runs,
                "hippo_specialist_score": specialist,
                "hippo_first_time": first_time,
            }
            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            cheval = rec["cheval"]
            jockey = rec["jockey"]
            hippo = rec["hippo"]

            if cheval and hippo and race_date:
                horse_hippo_states[(cheval, hippo)].update(race_date, rec["gagnant"])

            if jockey and hippo and race_date:
                jockey_hippo_states[(jockey, hippo)].update(race_date, rec["gagnant"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Hippodrome expertise build termine: %d features en %.1fs "
        "(horse-hippo pairs: %d, jockey-hippo pairs: %d)",
        len(results), elapsed,
        len(horse_hippo_states), len(jockey_hippo_states),
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
        description="Construction des features d'expertise hippodrome a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/hippodrome_expertise/)",
    )
    args = parser.parse_args()

    logger = setup_logging("hippodrome_expertise_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_hippodrome_expertise_features(input_path, logger)

    # Save
    out_path = output_dir / "hippodrome_expertise.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
