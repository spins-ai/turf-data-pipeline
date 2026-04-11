#!/usr/bin/env python3
"""
feature_builders.win_conditions_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Analyzes under which conditions each horse tends to win.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant win-condition features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the win-condition stats -- no future leakage.

Produces:
  - win_conditions.jsonl   in builder_outputs/win_conditions/

Features per partant:
  - wc_wins_small_field      : horse's win rate in small fields (<10 runners)
  - wc_wins_large_field      : horse's win rate in large fields (>=10 runners)
  - wc_wins_as_favorite      : horse's win rate when it was the favorite (lowest odds)
  - wc_wins_as_outsider      : horse's win rate when odds > 10
  - wc_wins_first_half_year  : win rate in months 1-6
  - wc_wins_second_half_year : win rate in months 7-12
  - wc_clutch_factor         : win rate in high-allocation races / win rate in low-allocation races
  - wc_field_size_preference : 0 if prefers large fields, 1 if prefers small fields

Usage:
    python feature_builders/win_conditions_builder.py
    python feature_builders/win_conditions_builder.py --input /path/to/partants_master.jsonl
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/win_conditions")

_LOG_EVERY = 500_000

# Threshold separating small vs large fields
_SMALL_FIELD_THRESHOLD = 10

# Threshold for outsider odds
_OUTSIDER_ODDS_THRESHOLD = 10.0


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _WinConditionsState:
    """Per-horse win-conditions accumulator.

    All counters are updated AFTER emitting features for the current race
    to ensure strict temporal integrity (no leakage).
    """

    __slots__ = (
        "small_wins", "small_total",
        "large_wins", "large_total",
        "fav_wins", "fav_total",
        "outsider_wins", "outsider_total",
        "h1_wins", "h1_total",
        "h2_wins", "h2_total",
        "high_class_wins", "high_class_total",
        "low_class_wins", "low_class_total",
    )

    def __init__(self) -> None:
        self.small_wins: int = 0
        self.small_total: int = 0
        self.large_wins: int = 0
        self.large_total: int = 0
        self.fav_wins: int = 0
        self.fav_total: int = 0
        self.outsider_wins: int = 0
        self.outsider_total: int = 0
        self.h1_wins: int = 0
        self.h1_total: int = 0
        self.h2_wins: int = 0
        self.h2_total: int = 0
        self.high_class_wins: int = 0
        self.high_class_total: int = 0
        self.low_class_wins: int = 0
        self.low_class_total: int = 0


def _safe_rate(wins: int, total: int) -> Optional[float]:
    """Return wins/total rounded to 6 decimal places, or None if total==0."""
    if total == 0:
        return None
    return round(wins / total, 6)


# ===========================================================================
# STREAMING READER & HELPERS
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


def _to_float(v) -> Optional[float]:
    """Safely cast value to float, return None on failure."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _to_int(v) -> Optional[int]:
    """Safely cast value to int, return None on failure."""
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_win_conditions_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build win-conditions features from partants_master.jsonl.

    Two-phase approach:
      1. Build a lightweight byte-offset index (date, course_uid, num_pmu, offset)
         and sort it chronologically.
      2. Process records course by course, seeking directly to each record on disk.
         For each course:
           a. Read and emit features using the PRE-race state (strict temporal integrity).
           b. Then update per-horse state using this race's outcomes.

    Memory budget:
      - Index: ~16M * ~80 bytes = ~1.3 GB
      - State dict: ~390K horses * ~9 int fields * ~28 bytes = ~100 MB
    """
    logger.info("=== Win Conditions Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Build lightweight byte-offset index ──
    index: list[tuple[str, str, int, int]] = []  # (date, course_uid, num_pmu, offset)
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
            num_pmu = _to_int(rec.get("num_pmu")) or 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process course by course, streaming output ──
    t2 = time.time()
    horse_state: dict[str, _WinConditionsState] = defaultdict(_WinConditionsState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    fill_counts = {k: 0 for k in (
        "wc_wins_small_field",
        "wc_wins_large_field",
        "wc_wins_as_favorite",
        "wc_wins_as_outsider",
        "wc_wins_first_half_year",
        "wc_wins_second_half_year",
        "wc_clutch_factor",
        "wc_field_size_preference",
    )}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offs: int) -> dict:
            fin.seek(offs)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract only the fields needed for computation."""
            cheval = rec.get("nom_cheval") or rec.get("horse_id")
            partant_uid = rec.get("partant_uid")

            nb_partants = _to_int(rec.get("nombre_partants")) or 0
            cote = _to_float(rec.get("cote_finale"))
            allocation = _to_float(rec.get("allocation"))
            date_str = rec.get("date_reunion_iso", "") or ""
            pos = _to_int(rec.get("position_arrivee"))
            is_gagnant = bool(rec.get("is_gagnant")) or (pos == 1)

            return {
                "uid": partant_uid,
                "cheval": cheval,
                "nb_partants": nb_partants,
                "cote": cote,
                "allocation": allocation,
                "date_str": date_str,
                "is_gagnant": is_gagnant,
            }

        i = 0
        while i < total:
            # ── Collect all index entries for this (date, course) group ──
            current_date = index[i][0]
            current_course = index[i][1]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][0] == current_date
                and index[i][1] == current_course
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # ── Read slim records for this course from disk ──
            course_group: list[dict] = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # Determine race-level attributes:
            # - field size (use nb_partants from first valid record, or count group)
            nb_partants_race = 0
            for r in course_group:
                if r["nb_partants"] > 0:
                    nb_partants_race = r["nb_partants"]
                    break
            if nb_partants_race == 0:
                nb_partants_race = len(course_group)

            is_small_field = nb_partants_race < _SMALL_FIELD_THRESHOLD

            # Determine allocation median to split high/low class races
            allocations = [r["allocation"] for r in course_group if r["allocation"] is not None]
            if allocations:
                alloc_race = allocations[0]  # all partants share the same race allocation
            else:
                alloc_race = None

            # Determine favorite: the partant with the lowest cote_finale in this race
            # We compute the minimum valid cote across the course group
            min_cote = None
            for r in course_group:
                c = r["cote"]
                if c is not None and c > 0:
                    if min_cote is None or c < min_cote:
                        min_cote = c

            # Parse race date once
            race_date = _parse_date(current_date)
            if race_date is not None:
                month = race_date.month
                is_h1 = month <= 6
            else:
                month = None
                is_h1 = None

            # ── Emit features (PRE-update snapshot) ──
            for rec in course_group:
                cheval = rec["cheval"]
                uid = rec["uid"]

                if not cheval:
                    # Cannot associate with horse state; emit nulls
                    feat: dict[str, Any] = {"partant_uid": uid}
                    for k in fill_counts:
                        feat[k] = None
                    fout.write(json.dumps(feat, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                st = horse_state[cheval]
                feat = {"partant_uid": uid}

                # wc_wins_small_field
                feat["wc_wins_small_field"] = _safe_rate(st.small_wins, st.small_total)
                if feat["wc_wins_small_field"] is not None:
                    fill_counts["wc_wins_small_field"] += 1

                # wc_wins_large_field
                feat["wc_wins_large_field"] = _safe_rate(st.large_wins, st.large_total)
                if feat["wc_wins_large_field"] is not None:
                    fill_counts["wc_wins_large_field"] += 1

                # wc_wins_as_favorite
                feat["wc_wins_as_favorite"] = _safe_rate(st.fav_wins, st.fav_total)
                if feat["wc_wins_as_favorite"] is not None:
                    fill_counts["wc_wins_as_favorite"] += 1

                # wc_wins_as_outsider
                feat["wc_wins_as_outsider"] = _safe_rate(st.outsider_wins, st.outsider_total)
                if feat["wc_wins_as_outsider"] is not None:
                    fill_counts["wc_wins_as_outsider"] += 1

                # wc_wins_first_half_year
                feat["wc_wins_first_half_year"] = _safe_rate(st.h1_wins, st.h1_total)
                if feat["wc_wins_first_half_year"] is not None:
                    fill_counts["wc_wins_first_half_year"] += 1

                # wc_wins_second_half_year
                feat["wc_wins_second_half_year"] = _safe_rate(st.h2_wins, st.h2_total)
                if feat["wc_wins_second_half_year"] is not None:
                    fill_counts["wc_wins_second_half_year"] += 1

                # wc_clutch_factor: high-class win rate / low-class win rate
                high_wr = _safe_rate(st.high_class_wins, st.high_class_total)
                low_wr = _safe_rate(st.low_class_wins, st.low_class_total)
                if high_wr is not None and low_wr is not None and low_wr > 0:
                    feat["wc_clutch_factor"] = round(high_wr / low_wr, 6)
                    fill_counts["wc_clutch_factor"] += 1
                elif high_wr is not None and low_wr == 0.0 and st.low_class_total > 0:
                    # Has low-class history, never won there -- clutch = high_wr / epsilon
                    # Express as high_wr directly (unbounded ratio undesirable); keep None
                    feat["wc_clutch_factor"] = None
                else:
                    feat["wc_clutch_factor"] = None

                # wc_field_size_preference: 1=small, 0=large
                small_wr = _safe_rate(st.small_wins, st.small_total)
                large_wr = _safe_rate(st.large_wins, st.large_total)
                if small_wr is not None and large_wr is not None:
                    feat["wc_field_size_preference"] = 1 if small_wr > large_wr else 0
                    fill_counts["wc_field_size_preference"] += 1
                elif small_wr is not None:
                    # Only small-field history available
                    feat["wc_field_size_preference"] = 1
                    fill_counts["wc_field_size_preference"] += 1
                elif large_wr is not None:
                    # Only large-field history available
                    feat["wc_field_size_preference"] = 0
                    fill_counts["wc_field_size_preference"] += 1
                else:
                    feat["wc_field_size_preference"] = None

                fout.write(json.dumps(feat, ensure_ascii=False) + "\n")
                n_written += 1

            # ── Update states AFTER emitting features (temporal integrity) ──
            for rec in course_group:
                cheval = rec["cheval"]
                if not cheval:
                    continue

                st = horse_state[cheval]
                won = rec["is_gagnant"]
                cote = rec["cote"]

                # Field size counters
                if is_small_field:
                    st.small_total += 1
                    if won:
                        st.small_wins += 1
                else:
                    st.large_total += 1
                    if won:
                        st.large_wins += 1

                # Favorite / outsider
                if cote is not None and cote > 0:
                    # Is this horse the favorite for this race?
                    if min_cote is not None and abs(cote - min_cote) < 1e-9:
                        st.fav_total += 1
                        if won:
                            st.fav_wins += 1

                    # Is this horse an outsider?
                    if cote > _OUTSIDER_ODDS_THRESHOLD:
                        st.outsider_total += 1
                        if won:
                            st.outsider_wins += 1

                # Half-year counters
                if is_h1 is True:
                    st.h1_total += 1
                    if won:
                        st.h1_wins += 1
                elif is_h1 is False:
                    st.h2_total += 1
                    if won:
                        st.h2_wins += 1

                # High/low class (allocation-based)
                # We compute the running median lazily: store total allocation sum
                # and use a simple global threshold derived over first 10K races.
                # Simpler approach: flag race as high-class if allocation > 5000
                # (typical PMU purse for a premium race).
                if alloc_race is not None:
                    if alloc_race >= 5000.0:
                        st.high_class_total += 1
                        if won:
                            st.high_class_wins += 1
                    else:
                        st.low_class_total += 1
                        if won:
                            st.low_class_wins += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Win conditions build termine: %d features en %.1fs (chevaux uniques: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0.0
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
    # Fallback: project-relative path for dev environments
    project_root = Path(__file__).resolve().parent.parent
    for candidate in (
        project_root / "data_master" / "partants_master.jsonl",
        project_root / "data_master" / "partants_master_enrichi.jsonl",
    ):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve. Essayez --input /chemin/vers/partants_master.jsonl"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features win_conditions a partir de partants_master"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: builder_outputs/win_conditions/)",
    )
    args = parser.parse_args()

    logger = setup_logging("win_conditions_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "win_conditions.jsonl"
    build_win_conditions_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
