#!/usr/bin/env python3
"""
feature_builders.head_to_head_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Head-to-head confrontation features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant head-to-head features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - head_to_head.jsonl   in output/head_to_head/

Features per partant:
  - h2h_win_rate_vs_field     : % de chevaux du peloton deja battus par ce cheval
  - h2h_nb_common_races       : nb de courses en commun avec au moins 1 adversaire
  - h2h_nemesis_in_field      : 1 si un cheval qui le bat souvent est present
  - h2h_dominated_count       : nb d'adversaires qu'il a battu >60% des confrontations

Usage:
    python feature_builders/head_to_head_builder.py
    python feature_builders/head_to_head_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "head_to_head"

_LOG_EVERY = 500_000

# Minimum confrontations to consider a meaningful H2H record
_MIN_H2H_RACES = 2
# Threshold for nemesis (opponent beats this horse >= 60% of encounters)
_NEMESIS_THRESHOLD = 0.60
# Threshold for domination (this horse beats opponent >= 60%)
_DOMINATION_THRESHOLD = 0.60


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


# ===========================================================================
# H2H STATE
# ===========================================================================


class _H2HTracker:
    """Tracks head-to-head records between all horse pairs.

    Uses a compact dict: h2h[(A, B)] = [wins_A, wins_B]
    where A < B alphabetically to avoid duplicates.
    """

    def __init__(self) -> None:
        # h2h_records[(horse_a, horse_b)] = [wins_a, wins_b]  where a < b
        self.h2h_records: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        # Track which horses each horse has raced against
        self.races_against: dict[str, set[str]] = defaultdict(set)

    def get_record(self, horse_a: str, horse_b: str) -> tuple[int, int]:
        """Get (wins_a, wins_b) for the pair."""
        if horse_a < horse_b:
            rec = self.h2h_records.get((horse_a, horse_b))
            if rec is None:
                return (0, 0)
            return (rec[0], rec[1])
        else:
            rec = self.h2h_records.get((horse_b, horse_a))
            if rec is None:
                return (0, 0)
            return (rec[1], rec[0])

    def get_total_encounters(self, horse_a: str, horse_b: str) -> int:
        w_a, w_b = self.get_record(horse_a, horse_b)
        return w_a + w_b

    def update_race(self, finishers: list[tuple[str, int]]) -> None:
        """Update H2H records for a completed race.

        finishers: list of (horse_name, position) for horses with valid positions.
        For each pair, the horse with the better (lower) position gets a win.
        """
        n = len(finishers)
        for idx_a in range(n):
            horse_a, pos_a = finishers[idx_a]
            for idx_b in range(idx_a + 1, n):
                horse_b, pos_b = finishers[idx_b]
                if pos_a == pos_b:
                    continue  # dead heat, skip

                # Record the encounter
                self.races_against[horse_a].add(horse_b)
                self.races_against[horse_b].add(horse_a)

                if horse_a < horse_b:
                    key = (horse_a, horse_b)
                    if pos_a < pos_b:
                        self.h2h_records[key][0] += 1
                    else:
                        self.h2h_records[key][1] += 1
                else:
                    key = (horse_b, horse_a)
                    if pos_a < pos_b:
                        self.h2h_records[key][1] += 1
                    else:
                        self.h2h_records[key][0] += 1

    def compute_features(self, horse: str, field: list[str]) -> dict[str, Any]:
        """Compute H2H features for a horse against its current field."""
        opponents = [h for h in field if h != horse]
        if not opponents:
            return {
                "h2h_win_rate_vs_field": None,
                "h2h_nb_common_races": None,
                "h2h_nemesis_in_field": None,
                "h2h_dominated_count": None,
            }

        beaten_count = 0
        opponents_with_history = 0
        total_common_races = 0
        has_nemesis = 0
        dominated_count = 0

        for opp in opponents:
            total_enc = self.get_total_encounters(horse, opp)
            if total_enc < _MIN_H2H_RACES:
                continue

            opponents_with_history += 1
            wins_me, wins_opp = self.get_record(horse, opp)
            total_common_races += total_enc

            if wins_me > wins_opp:
                beaten_count += 1

            # Nemesis check: opponent beats me >= 60%
            if total_enc >= _MIN_H2H_RACES and wins_opp / total_enc >= _NEMESIS_THRESHOLD:
                has_nemesis = 1

            # Domination check: I beat opponent >= 60%
            if total_enc >= _MIN_H2H_RACES and wins_me / total_enc >= _DOMINATION_THRESHOLD:
                dominated_count += 1

        if opponents_with_history == 0:
            return {
                "h2h_win_rate_vs_field": None,
                "h2h_nb_common_races": total_common_races if total_common_races > 0 else None,
                "h2h_nemesis_in_field": None,
                "h2h_dominated_count": None,
            }

        return {
            "h2h_win_rate_vs_field": round(beaten_count / opponents_with_history, 4),
            "h2h_nb_common_races": total_common_races,
            "h2h_nemesis_in_field": has_nemesis,
            "h2h_dominated_count": dominated_count,
        }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_head_to_head_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build head-to-head features from partants_master.jsonl."""
    logger.info("=== Head-to-Head Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
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
            "cheval": rec.get("nom_cheval") or rec.get("horse_id"),
            "position": _safe_int(rec.get("position_arrivee")),
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
    tracker = _H2HTracker()
    results: list[dict[str, Any]] = []
    n_processed = 0

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

        # Build field list
        field_horses: list[str] = [
            rec["cheval"] for rec in course_group if rec["cheval"]
        ]

        # -- Snapshot pre-race features --
        for rec in course_group:
            cheval = rec["cheval"]
            features: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "h2h_win_rate_vs_field": None,
                "h2h_nb_common_races": None,
                "h2h_nemesis_in_field": None,
                "h2h_dominated_count": None,
            }

            if cheval and len(field_horses) >= 2:
                h2h_feats = tracker.compute_features(cheval, field_horses)
                features.update(h2h_feats)

            results.append(features)

        # -- Update H2H records after snapshotting (post-race) --
        finishers: list[tuple[str, int]] = []
        for rec in course_group:
            if rec["cheval"] and rec["position"] is not None and rec["position"] > 0:
                finishers.append((rec["cheval"], rec["position"]))

        if len(finishers) >= 2:
            tracker.update_race(finishers)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Head-to-head build termine: %d features en %.1fs (paires H2H: %d)",
        len(results), elapsed, len(tracker.h2h_records),
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
        description="Construction des features head-to-head a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/head_to_head/)",
    )
    args = parser.parse_args()

    logger = setup_logging("head_to_head_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_head_to_head_features(input_path, logger)

    # Save
    out_path = output_dir / "head_to_head.jsonl"
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
