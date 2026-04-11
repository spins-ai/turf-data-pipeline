#!/usr/bin/env python3
"""
feature_builders.annual_race_pattern_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Detects annual recurring race patterns (same race repeated each year) and
computes horse performance features within those recurring races.

A "recurring race pattern" is identified by the key:
    (hippodrome, distance_bucket, month)
where distance_bucket = round(distance / 100) * 100.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant features.

Temporal integrity: features are computed BEFORE state update, so for any
partant at date D only races with date < D contribute -- no future leakage.

Produces:
  annual_race_pattern_features.jsonl  in builder_outputs/annual_race_pattern/

Features per partant (8):
  - arp_is_repeat_race        : 1 if this (hippodrome, distance_bucket, month)
                                combo has occurred in a previous year
  - arp_horse_prev_entries    : nb times this horse entered this pattern before
  - arp_horse_prev_wins       : nb wins for horse in previous editions
  - arp_horse_prev_best_pos   : best finishing position in previous editions
  - arp_avg_field_size        : avg field size across all previous editions
  - arp_horse_improving_trend : 1 if horse's position improved vs previous entry
  - arp_years_experience      : distinct years horse has entered this pattern
  - arp_race_editions_count   : how many editions of this pattern have occurred

Usage:
    python feature_builders/annual_race_pattern_builder.py
    python feature_builders/annual_race_pattern_builder.py --input /path/to/partants_master.jsonl
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

_DEFAULT_INPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
_DEFAULT_OUTPUT = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/"
    "annual_race_pattern/annual_race_pattern_features.jsonl"
)

_LOG_EVERY = 500_000

_FEATURE_NAMES = [
    "arp_is_repeat_race",
    "arp_horse_prev_entries",
    "arp_horse_prev_wins",
    "arp_horse_prev_best_pos",
    "arp_avg_field_size",
    "arp_horse_improving_trend",
    "arp_years_experience",
    "arp_race_editions_count",
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date_parts(date_str: str) -> Optional[tuple[int, int, int]]:
    """Return (year, month, day) from an ISO date string, or None on failure."""
    if not date_str:
        return None
    try:
        parts = date_str[:10].split("-")
        if len(parts) != 3:
            return None
        return int(parts[0]), int(parts[1]), int(parts[2])
    except (ValueError, TypeError, AttributeError):
        return None


def _distance_bucket(distance) -> Optional[int]:
    """Round distance to nearest 100m."""
    if distance is None:
        return None
    try:
        d = int(float(distance))
        return (round(d / 100)) * 100
    except (ValueError, TypeError):
        return None


def _safe_int(val, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_position(pos) -> Optional[int]:
    """Parse finishing position; returns None if non-numeric (e.g. 'D', 'NP')."""
    if pos is None:
        return None
    try:
        p = int(float(str(pos)))
        return p if p > 0 else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE
# ===========================================================================


class _PatternState:
    """Tracks overall stats for a recurring race pattern key.

    pattern_key: (hippodrome_str, distance_bucket_int, month_int)
    """

    __slots__ = ("editions", "field_sizes")

    def __init__(self) -> None:
        # Set of years in which this pattern has been seen
        self.editions: set[int] = set()
        # List of field sizes (int) for each edition recorded
        self.field_sizes: list[int] = []

    @property
    def is_repeat(self) -> int:
        """1 if at least one previous edition has been recorded."""
        return 1 if self.editions else 0

    @property
    def editions_count(self) -> int:
        return len(self.editions)

    @property
    def avg_field_size(self) -> Optional[float]:
        if not self.field_sizes:
            return None
        return round(sum(self.field_sizes) / len(self.field_sizes), 2)


class _HorsePatternState:
    """Tracks a specific horse's history within a recurring race pattern.

    horse_id: str (nom_cheval or partant_uid base)
    pattern_key: same key as _PatternState
    """

    __slots__ = ("entries", "wins")

    def __init__(self) -> None:
        # List of (year: int, position: Optional[int]) in chronological order
        self.entries: list[tuple[int, Optional[int]]] = []
        self.wins: int = 0

    @property
    def prev_entries(self) -> int:
        return len(self.entries)

    @property
    def prev_wins(self) -> int:
        return self.wins

    @property
    def prev_best_pos(self) -> Optional[int]:
        """Lowest (best) finishing position across all previous entries."""
        valid = [pos for (_, pos) in self.entries if pos is not None]
        return min(valid) if valid else None

    @property
    def years_experience(self) -> int:
        return len({year for (year, _) in self.entries})

    def improving_trend(self) -> Optional[int]:
        """1 if position improved in last entry vs the one before it, else 0.

        Returns None if fewer than 2 entries with valid positions.
        """
        valid = [(yr, pos) for (yr, pos) in self.entries if pos is not None]
        if len(valid) < 2:
            return None
        last_pos = valid[-1][1]
        prev_pos = valid[-2][1]
        # Lower position number = better result
        return 1 if last_pos < prev_pos else 0


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_annual_race_pattern_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build annual race pattern features using index+sort+seek strategy.

    Phase 1: Read minimal sort keys + byte offsets (lightweight index).
    Phase 2: Sort index chronologically.
    Phase 3: Seek-and-read records in order, emit features BEFORE state update.

    Returns total number of feature records written.
    """
    logger.info("=== Annual Race Pattern Builder ===")
    logger.info("Input : %s", input_path)
    logger.info("Output: %s", output_path)
    t0 = time.time()

    # -------------------------------------------------------------------------
    # Phase 1: Build lightweight index
    # -------------------------------------------------------------------------
    # index entry: (date_str, course_uid, num_pmu, byte_offset)
    index: list[tuple[str, str, int, int]] = []
    n_read = 0
    n_errors = 0

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
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Phase 1: indexe %d records...", n_read)
                gc.collect()

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = _safe_int(rec.get("num_pmu", 0))

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes, %d erreurs JSON en %.1fs",
        len(index), n_errors, time.time() - t0,
    )

    # -------------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # -------------------------------------------------------------------------
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Phase 2: tri chronologique en %.1fs", time.time() - t1)

    # -------------------------------------------------------------------------
    # Phase 3: Process course by course, emit features before state update
    # -------------------------------------------------------------------------
    t2 = time.time()

    # pattern_key -> _PatternState
    pattern_state: dict[tuple, _PatternState] = defaultdict(_PatternState)
    # (horse_id, pattern_key) -> _HorsePatternState
    horse_pattern_state: dict[tuple, _HorsePatternState] = defaultdict(
        _HorsePatternState
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    # Fill-rate counters
    fill_counts: dict[str, int] = {name: 0 for name in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_fields(rec: dict) -> dict:
            """Extract only the fields we need for this builder."""
            hippodrome = (
                rec.get("hippodrome_normalise")
                or rec.get("hippodrome")
                or rec.get("libelle_hippodrome")
                or ""
            )
            hippodrome = str(hippodrome).strip().upper()

            distance = rec.get("distance") or rec.get("distance_course")
            dist_bucket = _distance_bucket(distance)

            date_str = rec.get("date_reunion_iso", "") or ""
            date_parts = _parse_date_parts(date_str)

            horse_id = (
                rec.get("horse_id")
                or rec.get("cheval_id")
                or rec.get("nom_cheval")
                or ""
            )
            horse_id = str(horse_id).strip().upper()

            pos_raw = rec.get("position_arrivee") or rec.get("position")
            position = _safe_position(pos_raw)

            nb_partants = _safe_int(rec.get("nombre_partants", 0))

            # is_gagnant as fallback for position=1
            is_winner = bool(rec.get("is_gagnant")) or (position == 1)

            return {
                "partant_uid": rec.get("partant_uid"),
                "course_uid": rec.get("course_uid", ""),
                "hippodrome": hippodrome,
                "dist_bucket": dist_bucket,
                "date_parts": date_parts,   # (year, month, day) or None
                "horse_id": horse_id,
                "position": position,
                "nb_partants": nb_partants,
                "is_winner": is_winner,
            }

        i = 0
        while i < total:
            # Collect all index entries for this course (same date + course_uid)
            course_uid_cur = index[i][1]
            course_date_cur = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid_cur
                and index[i][0] == course_date_cur
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read minimal fields for all partants in this course
            course_group: list[dict] = [
                _extract_fields(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # Determine the race pattern key from the first record with enough info
            date_parts: Optional[tuple[int, int, int]] = None
            hippodrome_key: str = ""
            dist_bucket_key: Optional[int] = None

            for rec in course_group:
                if rec["date_parts"] is not None:
                    date_parts = rec["date_parts"]
                if rec["hippodrome"]:
                    hippodrome_key = rec["hippodrome"]
                if rec["dist_bucket"] is not None:
                    dist_bucket_key = rec["dist_bucket"]

            # Build pattern key; None means we cannot identify the pattern
            if date_parts is not None and hippodrome_key and dist_bucket_key is not None:
                year, month, _ = date_parts
                pattern_key: Optional[tuple] = (hippodrome_key, dist_bucket_key, month)
            else:
                year = None
                month = None
                pattern_key = None

            # ------------------------------------------------------------------
            # Snapshot features BEFORE state update (temporal integrity)
            # ------------------------------------------------------------------
            for rec in course_group:
                horse_id = rec["horse_id"]
                uid = rec["partant_uid"]

                features: dict[str, Any] = {"partant_uid": uid}

                if pattern_key is not None:
                    ps = pattern_state[pattern_key]
                    hps_key = (horse_id, pattern_key)
                    hps = horse_pattern_state[hps_key]

                    # arp_is_repeat_race
                    is_repeat = ps.is_repeat
                    features["arp_is_repeat_race"] = is_repeat
                    if is_repeat:
                        fill_counts["arp_is_repeat_race"] += 1

                    # arp_race_editions_count
                    editions_count = ps.editions_count
                    features["arp_race_editions_count"] = editions_count
                    if editions_count > 0:
                        fill_counts["arp_race_editions_count"] += 1

                    # arp_avg_field_size
                    avg_fs = ps.avg_field_size
                    features["arp_avg_field_size"] = avg_fs
                    if avg_fs is not None:
                        fill_counts["arp_avg_field_size"] += 1

                    if horse_id:
                        # arp_horse_prev_entries
                        prev_entries = hps.prev_entries
                        features["arp_horse_prev_entries"] = prev_entries
                        if prev_entries > 0:
                            fill_counts["arp_horse_prev_entries"] += 1

                        # arp_horse_prev_wins
                        prev_wins = hps.prev_wins
                        features["arp_horse_prev_wins"] = prev_wins
                        if prev_wins > 0:
                            fill_counts["arp_horse_prev_wins"] += 1

                        # arp_horse_prev_best_pos
                        prev_best = hps.prev_best_pos
                        features["arp_horse_prev_best_pos"] = prev_best
                        if prev_best is not None:
                            fill_counts["arp_horse_prev_best_pos"] += 1

                        # arp_horse_improving_trend
                        trend = hps.improving_trend()
                        features["arp_horse_improving_trend"] = trend
                        if trend is not None:
                            fill_counts["arp_horse_improving_trend"] += 1

                        # arp_years_experience
                        yrs_exp = hps.years_experience
                        features["arp_years_experience"] = yrs_exp
                        if yrs_exp > 0:
                            fill_counts["arp_years_experience"] += 1
                    else:
                        features["arp_horse_prev_entries"] = None
                        features["arp_horse_prev_wins"] = None
                        features["arp_horse_prev_best_pos"] = None
                        features["arp_horse_improving_trend"] = None
                        features["arp_years_experience"] = None
                else:
                    # Cannot determine pattern key -- all features null
                    for name in _FEATURE_NAMES:
                        features[name] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # ------------------------------------------------------------------
            # Update state AFTER writing features (temporal integrity)
            # ------------------------------------------------------------------
            if pattern_key is not None and year is not None:
                ps = pattern_state[pattern_key]

                # Record this edition (year) if not already seen
                edition_already_seen = year in ps.editions
                ps.editions.add(year)

                # Record field size for this edition (once per course)
                field_sizes_recorded = False
                for rec in course_group:
                    nb = rec["nb_partants"]
                    if nb > 0 and not field_sizes_recorded:
                        ps.field_sizes.append(nb)
                        field_sizes_recorded = True
                        break

                # Update horse-level state
                for rec in course_group:
                    horse_id = rec["horse_id"]
                    if not horse_id:
                        continue
                    hps_key = (horse_id, pattern_key)
                    hps = horse_pattern_state[hps_key]
                    hps.entries.append((year, rec["position"]))
                    if rec["is_winner"]:
                        hps.wins += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info(
                    "  Phase 3: traite %d / %d records... (patterns: %d, "
                    "horse-patterns: %d)",
                    n_processed, total,
                    len(pattern_state), len(horse_pattern_state),
                )
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features ecrits en %.1fs "
        "(patterns: %d, horse-patterns: %d)",
        n_written, elapsed,
        len(pattern_state), len(horse_pattern_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for name in _FEATURE_NAMES:
        count = fill_counts[name]
        pct = 100.0 * count / n_written if n_written else 0.0
        logger.info("  %-35s: %d/%d (%.1f%%)", name, count, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construit les features de patterns de courses annuelles recurrentes "
            "a partir de partants_master.jsonl"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: D:/turf-data-pipeline/...)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Chemin vers le fichier de sortie .jsonl",
    )
    args = parser.parse_args()

    logger = setup_logging("annual_race_pattern_builder")

    # Resolve input
    input_path = Path(args.input) if args.input else _DEFAULT_INPUT
    if not input_path.exists():
        logger.error("Fichier d'entree introuvable: %s", input_path)
        sys.exit(1)

    # Resolve output
    output_path = Path(args.output) if args.output else _DEFAULT_OUTPUT

    build_annual_race_pattern_features(input_path, output_path, logger)


if __name__ == "__main__":
    main()
