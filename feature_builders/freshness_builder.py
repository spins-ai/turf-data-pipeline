#!/usr/bin/env python3
"""
feature_builders.freshness_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Data freshness and completeness features measuring how recent and
complete our data is for each partant record.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant freshness features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - freshness.jsonl   in output/freshness/

Features per partant (5):
  - data_freshness_score  : days since last record for this horse (before current race)
  - form_sample_size      : nb races in last 90 days (confidence measure)
  - odds_available        : 1 if cote_finale is present, 0 if missing
  - pedigree_available    : 1 if pere+mere known, 0 if missing
  - data_completeness     : fraction of key fields filled for this record (0.0-1.0)

Usage:
    python feature_builders/freshness_builder.py
    python feature_builders/freshness_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "freshness"

_LOG_EVERY = 500_000

# Key fields for data_completeness calculation
_KEY_FIELDS = [
    "nom_cheval",
    "age",
    "sexe",
    "distance",
    "discipline",
    "hippodrome",
    "jockey",
    "entraineur",
    "cote_finale",
    "poids_jockey",
    "position_arrivee",
    "pere",
    "mere",
    "nb_partants",
    "terrain",
]


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


def _parse_date(date_str: Optional[str]) -> Optional[int]:
    """Parse YYYY-MM-DD date string to ordinal days for arithmetic."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        parts = date_str[:10].split("-")
        if len(parts) != 3:
            return None
        y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
        # Simplified ordinal: good enough for day differences
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


def _data_completeness(rec: dict) -> float:
    """Compute fraction of key fields that are non-null and non-empty."""
    filled = 0
    for field in _KEY_FIELDS:
        val = rec.get(field)
        if val is not None and val != "" and val != 0:
            filled += 1
    return round(filled / len(_KEY_FIELDS), 4)


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseFreshnessState:
    """Per-horse accumulated state for freshness features."""

    __slots__ = ("race_dates",)

    def __init__(self) -> None:
        self.race_dates: list[int] = []  # ordinal dates of past races

    def snapshot(self, current_date_ord: Optional[int]) -> dict[str, Any]:
        """Compute features using only past races (strict temporal)."""
        if not self.race_dates or current_date_ord is None:
            return {
                "data_freshness_score": None,
                "form_sample_size": 0,
            }

        last_date = self.race_dates[-1]
        days_since = current_date_ord - last_date
        if days_since < 0:
            days_since = 0

        # Count races in last 90 days
        cutoff = current_date_ord - 90
        recent_count = sum(1 for d in self.race_dates if d >= cutoff)

        return {
            "data_freshness_score": days_since,
            "form_sample_size": recent_count,
        }

    def update(self, date_ord: Optional[int]) -> None:
        """Update state with a new race date (post-race)."""
        if date_ord is not None:
            self.race_dates.append(date_ord)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_freshness_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build freshness features from partants_master.jsonl."""
    logger.info("=== Freshness Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        date_str = rec.get("date_reunion_iso", "")
        date_ord = _parse_date(date_str)

        # Compute record-level features directly (no temporal dependency)
        cote = rec.get("cote_finale")
        odds_avail = 1 if (cote is not None and cote != "" and cote != 0) else 0

        pere = rec.get("pere")
        mere = rec.get("mere")
        pedigree_avail = 1 if (pere and pere != "" and mere and mere != "") else 0

        completeness = _data_completeness(rec)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": date_str,
            "date_ord": date_ord,
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "odds_available": odds_avail,
            "pedigree_available": pedigree_avail,
            "data_completeness": completeness,
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
    horse_states: dict[str, _HorseFreshnessState] = defaultdict(_HorseFreshnessState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    # Group by (date, course) for temporal integrity
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

        # -- Snapshot pre-race features --
        for rec in course_group:
            cheval = rec["cheval"]
            date_ord = rec["date_ord"]

            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "data_freshness_score": None,
                "form_sample_size": 0,
                "odds_available": rec["odds_available"],
                "pedigree_available": rec["pedigree_available"],
                "data_completeness": rec["data_completeness"],
            }

            if cheval:
                state = horse_states[cheval]
                snap = state.snapshot(date_ord)
                features["data_freshness_score"] = snap["data_freshness_score"]
                features["form_sample_size"] = snap["form_sample_size"]

            results.append(features)

        # -- Update states after snapshotting (post-race) --
        for rec in course_group:
            cheval = rec["cheval"]
            if cheval:
                horse_states[cheval].update(rec["date_ord"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Freshness build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results), elapsed, len(horse_states),
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
        description="Construction des features freshness a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/freshness/)",
    )
    args = parser.parse_args()

    logger = setup_logging("freshness_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_freshness_features(input_path, logger)

    # Save
    out_path = output_dir / "freshness.jsonl"
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
