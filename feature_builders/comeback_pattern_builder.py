#!/usr/bin/env python3
"""
feature_builders.comeback_pattern_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Comeback / layoff pattern features -- analysing performance after breaks.

Reads partants_master.jsonl in memory-optimised mode (index + sort + seek),
processes all records chronologically, and computes per-partant comeback
pattern features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.  State is snapshotted
BEFORE the current race updates it.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - comeback_pattern.jsonl  in builder_outputs/comeback_pattern/

Features per partant (8):
  - cbp_layoff_days            : days since last race (from ecart_precedent
                                 or computed from dates)
  - cbp_layoff_category        : 0=quick (<14d), 1=normal (14-35),
                                 2=freshened (35-90), 3=layoff (90-180),
                                 4=long_layoff (>180)
  - cbp_horse_after_layoff_wr  : horse's win rate after similar layoff
                                 category historically
  - cbp_horse_first_up_wr      : horse's win rate on first run back
                                 after 35+ day break
  - cbp_horse_second_up_wr     : horse's win rate on second run after
                                 a 35+ day break
  - cbp_is_first_up            : 1 if this is first race back after 35+ days
  - cbp_is_second_up           : 1 if this is second race after a 35+ day break
  - cbp_layoff_x_age           : layoff_category * age (interaction term)

Usage:
    python feature_builders/comeback_pattern_builder.py
    python feature_builders/comeback_pattern_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/comeback_pattern")

_LOG_EVERY = 500_000

# Layoff thresholds (days)
_QUICK_MAX = 14
_NORMAL_MAX = 35
_FRESHENED_MAX = 90
_LAYOFF_MAX = 180
# 35+ days counts as a "long break" for first-up / second-up tracking
_LONG_BREAK_THRESHOLD = 35


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime.  Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _layoff_category(days: int) -> int:
    """Map layoff days to category code.

    0 = quick (<14d), 1 = normal (14-35), 2 = freshened (35-90),
    3 = layoff (90-180), 4 = long_layoff (>180).
    """
    if days < _QUICK_MAX:
        return 0
    if days < _NORMAL_MAX:
        return 1
    if days < _FRESHENED_MAX:
        return 2
    if days < _LAYOFF_MAX:
        return 3
    return 4


def _compute_age(birth_year: Any, race_date: Optional[datetime]) -> Optional[int]:
    """Compute horse age at race time from birth year."""
    if not birth_year or race_date is None:
        return None
    try:
        by = int(birth_year)
    except (ValueError, TypeError):
        return None
    age = race_date.year - by
    if age < 1 or age > 30:
        return None
    return age


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseComebackState:
    """Track comeback / layoff state for one horse."""

    __slots__ = (
        "last_date",
        "last_layoff_was_long",
        "is_coming_back",
        "layoff_cat_stats",
        "first_up_stats",
        "second_up_stats",
    )

    def __init__(self) -> None:
        self.last_date: Optional[str] = None          # ISO date of last race
        self.last_layoff_was_long: bool = False        # previous layoff >= 35d
        self.is_coming_back: bool = False              # just returned from long break
        # layoff_cat -> [wins, total]
        self.layoff_cat_stats: dict[int, list[int]] = {}
        # first-up after 35+ day break: [wins, total]
        self.first_up_stats: list[int] = [0, 0]
        # second-up after 35+ day break: [wins, total]
        self.second_up_stats: list[int] = [0, 0]

    def snapshot(
        self,
        race_date_str: str,
        ecart: Any,
        race_date: Optional[datetime],
        age: Optional[int],
    ) -> dict[str, Any]:
        """Return feature dict BEFORE updating state."""
        feats: dict[str, Any] = {
            "cbp_layoff_days": None,
            "cbp_layoff_category": None,
            "cbp_horse_after_layoff_wr": None,
            "cbp_horse_first_up_wr": None,
            "cbp_horse_second_up_wr": None,
            "cbp_is_first_up": None,
            "cbp_is_second_up": None,
            "cbp_layoff_x_age": None,
        }

        # ---- compute layoff days ----
        layoff_days: Optional[int] = None

        # Try ecart_precedent first
        if ecart is not None:
            try:
                layoff_days = int(ecart)
                if layoff_days < 0:
                    layoff_days = None
            except (ValueError, TypeError):
                layoff_days = None

        # Fallback: compute from dates
        if layoff_days is None and self.last_date and race_date_str:
            prev = _parse_date(self.last_date)
            curr = _parse_date(race_date_str)
            if prev and curr and curr > prev:
                layoff_days = (curr - prev).days

        if layoff_days is None:
            # No history yet (first race) or missing data
            return feats

        feats["cbp_layoff_days"] = layoff_days

        cat = _layoff_category(layoff_days)
        feats["cbp_layoff_category"] = cat

        # Historical win rate after same layoff category
        cat_entry = self.layoff_cat_stats.get(cat)
        if cat_entry and cat_entry[1] > 0:
            feats["cbp_horse_after_layoff_wr"] = round(cat_entry[0] / cat_entry[1], 4)

        # First-up stats
        if self.first_up_stats[1] > 0:
            feats["cbp_horse_first_up_wr"] = round(
                self.first_up_stats[0] / self.first_up_stats[1], 4
            )

        # Second-up stats
        if self.second_up_stats[1] > 0:
            feats["cbp_horse_second_up_wr"] = round(
                self.second_up_stats[0] / self.second_up_stats[1], 4
            )

        # Is first-up? (this race is first back after 35+ day break)
        is_first = int(layoff_days >= _LONG_BREAK_THRESHOLD)
        feats["cbp_is_first_up"] = is_first

        # Is second-up? (previous run was first-up after a long break)
        is_second = int(
            not is_first
            and self.is_coming_back
        )
        feats["cbp_is_second_up"] = is_second

        # Interaction: layoff category * age
        if age is not None:
            feats["cbp_layoff_x_age"] = cat * age

        return feats

    def update(
        self,
        race_date_str: str,
        layoff_days: Optional[int],
        is_winner: bool,
    ) -> None:
        """Update state AFTER snapshot."""
        if layoff_days is not None and layoff_days >= 0:
            cat = _layoff_category(layoff_days)

            # Update layoff category stats
            if cat not in self.layoff_cat_stats:
                self.layoff_cat_stats[cat] = [0, 0]
            self.layoff_cat_stats[cat][1] += 1
            if is_winner:
                self.layoff_cat_stats[cat][0] += 1

            is_first_up = layoff_days >= _LONG_BREAK_THRESHOLD

            if is_first_up:
                # This race is a first-up run
                self.first_up_stats[1] += 1
                if is_winner:
                    self.first_up_stats[0] += 1
                self.is_coming_back = True
            elif self.is_coming_back:
                # This race is a second-up run
                self.second_up_stats[1] += 1
                if is_winner:
                    self.second_up_stats[0] += 1
                self.is_coming_back = False
            else:
                self.is_coming_back = False

        if race_date_str:
            self.last_date = race_date_str


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek + streaming output)
# ===========================================================================


def build_comeback_pattern_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build comeback pattern features from partants_master.jsonl.

    Returns the total number of feature records written.
    """
    logger.info("=== Comeback Pattern Builder (memory-optimised) ===")
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
    horse_states: dict[str, _HorseComebackState] = defaultdict(_HorseComebackState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "cbp_layoff_days": 0,
        "cbp_layoff_category": 0,
        "cbp_horse_after_layoff_wr": 0,
        "cbp_horse_first_up_wr": 0,
        "cbp_horse_second_up_wr": 0,
        "cbp_is_first_up": 0,
        "cbp_is_second_up": 0,
        "cbp_layoff_x_age": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", "") or "",
                "course": rec.get("course_uid", "") or "",
                "num": rec.get("num_pmu", 0) or 0,
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "is_gagnant": bool(rec.get("is_gagnant")),
                "ecart": rec.get("ecart_precedent"),
                "annee_naissance": rec.get("annee_naissance"),
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
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            race_date = _parse_date(course_date_str)

            # -- Snapshot pre-race stats for all partants --
            updates: list[tuple[str, str, Optional[int], bool]] = []

            for rec in course_group:
                hid = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if hid:
                    age = _compute_age(rec["annee_naissance"], race_date)
                    state = horse_states[hid]
                    snap = state.snapshot(
                        rec["date"], rec["ecart"], race_date, age,
                    )
                    features.update(snap)

                    # Compute layoff_days for update (same logic as snapshot)
                    layoff_days: Optional[int] = None
                    ecart = rec["ecart"]
                    if ecart is not None:
                        try:
                            layoff_days = int(ecart)
                            if layoff_days < 0:
                                layoff_days = None
                        except (ValueError, TypeError):
                            layoff_days = None
                    if layoff_days is None and state.last_date and rec["date"]:
                        prev = _parse_date(state.last_date)
                        curr = _parse_date(rec["date"])
                        if prev and curr and curr > prev:
                            layoff_days = (curr - prev).days

                    updates.append((hid, rec["date"], layoff_days, rec["is_gagnant"]))
                else:
                    features.update({
                        "cbp_layoff_days": None,
                        "cbp_layoff_category": None,
                        "cbp_horse_after_layoff_wr": None,
                        "cbp_horse_first_up_wr": None,
                        "cbp_horse_second_up_wr": None,
                        "cbp_is_first_up": None,
                        "cbp_is_second_up": None,
                        "cbp_layoff_x_age": None,
                    })
                    updates.append(("", "", None, False))

                # Track fill rates
                for k in fill_counts:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                # Stream directly to output file
                fout.write(
                    json.dumps(features, ensure_ascii=False, default=str) + "\n"
                )
                n_written += 1

            # -- Update states after race --
            for hid, date_str, layoff_days, is_win in updates:
                if hid:
                    horse_states[hid].update(date_str, layoff_days, is_win)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Comeback pattern build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


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
        description="Construction des features comeback/layoff a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/comeback_pattern/)",
    )
    args = parser.parse_args()

    logger = setup_logging("comeback_pattern_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "comeback_pattern.jsonl"
    build_comeback_pattern_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
