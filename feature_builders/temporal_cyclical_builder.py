#!/usr/bin/env python3
"""
feature_builders.temporal_cyclical_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cyclical and temporal encoding features for ML models.

Single-pass builder: reads partants_master.jsonl in streaming mode and
computes per-record temporal features from the date, race number, and
hippodrome fields.  No course grouping required (except racing_density
which accumulates a histogram state).

Produces:
  - temporal_cyclical.jsonl   in output/temporal_cyclical/

Features per partant (15 total):
  - day_of_week_sin, day_of_week_cos     : cyclical day of week (Mon=0..Sun=6)
  - month_sin, month_cos                 : cyclical month (1-12)
  - day_of_month_sin, day_of_month_cos   : cyclical day of month (1-31)
  - week_of_year_sin, week_of_year_cos   : cyclical ISO week (1-53)
  - quarter                              : quarter of year (1-4)
  - is_weekend                           : 1 if Saturday or Sunday, else 0
  - is_holiday_period                    : 1 if Christmas / Easter / summer
  - season                               : ordinal (1=spring..4=winter)
  - days_since_season_start              : days since start of flat/trot season
  - race_number_norm                     : race number normalised to [0, 1]
  - month_x_discipline                   : month * discipline hash interaction
  - year                                 : raw year for trend modelling
  - days_since_epoch                     : continuous days since 2000-01-01
  - is_evening_meeting                   : 1 if race number > 6
  - racing_density                       : races at this hippodrome in same month (historical)

Usage:
    python feature_builders/temporal_cyclical_builder.py
    python feature_builders/temporal_cyclical_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

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

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/temporal_cyclical")

# Progress / GC every N records
_LOG_EVERY = 500_000

# Epoch reference for continuous days feature
_EPOCH = date(2000, 1, 1)

# Two-pi constant
_TWO_PI = 2.0 * math.pi

# Season definitions: month -> season code (1=spring, 2=summer, 3=autumn, 4=winter)
_MONTH_TO_SEASON = {
    1: 4, 2: 4, 3: 1, 4: 1, 5: 1,
    6: 2, 7: 2, 8: 2,
    9: 3, 10: 3, 11: 3,
    12: 4,
}

# Approximate flat season start: April 1st; Trot season: September 1st
_FLAT_SEASON_START_MONTH = 4
_FLAT_SEASON_START_DAY = 1
_TROT_SEASON_START_MONTH = 9
_TROT_SEASON_START_DAY = 1

# Holiday periods (month, day) ranges
# Christmas: Dec 20 - Jan 5 ; Easter: ~April 5-21 (approximate) ; Summer: July 1 - Aug 31
_HOLIDAY_RANGES = [
    # (start_month, start_day, end_month, end_day)
    (12, 20, 12, 31),   # Christmas part 1
    (1, 1, 1, 5),       # Christmas part 2
    (4, 5, 4, 21),      # Easter (approximate)
    (7, 1, 8, 31),      # Summer
]

# Max race number for normalisation (typical French racing cards)
_MAX_RACE_NUMBER = 9


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date(date_str: str) -> Optional[date]:
    """Parse ISO date string to date object. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _sin_cos(value: float, period: float) -> tuple[float, float]:
    """Return (sin, cos) encoding of value with given period."""
    angle = _TWO_PI * value / period
    return round(math.sin(angle), 6), round(math.cos(angle), 6)


def _is_holiday(d: date) -> bool:
    """Check whether a date falls in a holiday period."""
    m, day = d.month, d.day
    for sm, sd, em, ed in _HOLIDAY_RANGES:
        if sm == em:
            # Same month range
            if m == sm and sd <= day <= ed:
                return True
        elif sm < em:
            # Range within year
            if (m == sm and day >= sd) or (m == em and day <= ed) or (sm < m < em):
                return True
        else:
            # Wrapping range (Dec -> Jan): handled by splitting into two entries above
            pass
    return False


def _days_since_season_start(d: date, discipline: str) -> Optional[int]:
    """Days since the start of the relevant season (flat or trot).

    For flat (Plat, Obstacle): season starts April 1.
    For trot (Attelé, Monté): season starts September 1.
    Returns None if discipline unknown.
    """
    disc_upper = (discipline or "").strip().upper()

    if disc_upper in ("PLAT", "OBSTACLE", "STEEPLE-CHASE", "HAIES", "CROSS-COUNTRY"):
        start_month, start_day = _FLAT_SEASON_START_MONTH, _FLAT_SEASON_START_DAY
    elif disc_upper in ("ATTELE", "ATTELÉ", "MONTE", "MONTÉ", "TROT"):
        start_month, start_day = _TROT_SEASON_START_MONTH, _TROT_SEASON_START_DAY
    else:
        return None

    # Season start in the current or previous year
    season_start = date(d.year, start_month, start_day)
    if d < season_start:
        season_start = date(d.year - 1, start_month, start_day)

    delta = (d - season_start).days
    return delta


def _discipline_hash(discipline: str) -> int:
    """Simple stable hash of discipline string to a small integer (1-10)."""
    if not discipline:
        return 0
    h = 0
    for ch in discipline.upper():
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return (h % 10) + 1


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
# MAIN BUILD
# ===========================================================================


def build_temporal_cyclical_features(input_path: Path, output_path: Path, logger) -> int:
    """Build temporal/cyclical features from partants_master.jsonl.

    Single-pass streaming: reads each record, computes features from the
    date/metadata fields, and writes immediately to output.

    Racing density requires a state dict (hippodrome, year, month) -> count
    that is updated as records are processed. Since records are not guaranteed
    to be sorted, the density reflects *all records seen so far* for that
    hippodrome/month (not strictly causal, but acceptable for a calendar
    feature -- it measures historical track activity, not future).

    Returns the total number of feature records written.
    """
    logger.info("=== Temporal Cyclical Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    # State for racing density: (hippodrome, year, month) -> race count
    # We do two passes: first pass counts, second pass writes features.
    # But to keep it single-pass, we write density from accumulated state
    # (it reflects past + current month, which is fine for a calendar feature).
    hippo_month_count: dict[tuple[str, int, int], int] = defaultdict(int)

    n_written = 0

    # Feature fill counters
    feature_names = [
        "day_of_week_sin", "day_of_week_cos",
        "month_sin", "month_cos",
        "day_of_month_sin", "day_of_month_cos",
        "week_of_year_sin", "week_of_year_cos",
        "quarter", "is_weekend", "is_holiday_period",
        "season", "days_since_season_start",
        "race_number_norm", "month_x_discipline",
        "year", "days_since_epoch",
        "is_evening_meeting", "racing_density",
    ]
    fill_counts: dict[str, int] = {k: 0 for k in feature_names}

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            partant_uid = rec.get("partant_uid")
            date_str = rec.get("date_reunion_iso", "") or ""
            numero_course = rec.get("numero_course")
            hippodrome = rec.get("hippodrome_normalise", "") or ""
            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip()

            d = _parse_date(date_str)

            features: dict = {"partant_uid": partant_uid}

            if d is not None:
                # --- Cyclical encodings ---
                dow = d.weekday()  # 0=Monday, 6=Sunday
                s, c = _sin_cos(dow, 7.0)
                features["day_of_week_sin"] = s
                features["day_of_week_cos"] = c
                fill_counts["day_of_week_sin"] += 1
                fill_counts["day_of_week_cos"] += 1

                s, c = _sin_cos(d.month, 12.0)
                features["month_sin"] = s
                features["month_cos"] = c
                fill_counts["month_sin"] += 1
                fill_counts["month_cos"] += 1

                s, c = _sin_cos(d.day, 31.0)
                features["day_of_month_sin"] = s
                features["day_of_month_cos"] = c
                fill_counts["day_of_month_sin"] += 1
                fill_counts["day_of_month_cos"] += 1

                iso_week = d.isocalendar()[1]
                s, c = _sin_cos(iso_week, 53.0)
                features["week_of_year_sin"] = s
                features["week_of_year_cos"] = c
                fill_counts["week_of_year_sin"] += 1
                fill_counts["week_of_year_cos"] += 1

                # Quarter
                quarter = (d.month - 1) // 3 + 1
                features["quarter"] = quarter
                fill_counts["quarter"] += 1

                # Weekend
                features["is_weekend"] = 1 if dow >= 5 else 0
                fill_counts["is_weekend"] += 1

                # Holiday period
                features["is_holiday_period"] = 1 if _is_holiday(d) else 0
                fill_counts["is_holiday_period"] += 1

                # Season (ordinal)
                season = _MONTH_TO_SEASON[d.month]
                features["season"] = season
                fill_counts["season"] += 1

                # Days since season start
                dsss = _days_since_season_start(d, discipline)
                features["days_since_season_start"] = dsss
                if dsss is not None:
                    fill_counts["days_since_season_start"] += 1

                # Year (raw)
                features["year"] = d.year
                fill_counts["year"] += 1

                # Days since epoch (continuous)
                features["days_since_epoch"] = (d - _EPOCH).days
                fill_counts["days_since_epoch"] += 1

                # Month x discipline interaction
                disc_h = _discipline_hash(discipline)
                if disc_h > 0:
                    features["month_x_discipline"] = d.month * disc_h
                    fill_counts["month_x_discipline"] += 1
                else:
                    features["month_x_discipline"] = None

                # Racing density: update counter and use current value
                if hippodrome:
                    key = (hippodrome, d.year, d.month)
                    hippo_month_count[key] += 1
                    features["racing_density"] = hippo_month_count[key]
                    fill_counts["racing_density"] += 1
                else:
                    features["racing_density"] = None

            else:
                # No valid date -- all temporal features null
                for fname in feature_names:
                    features[fname] = None

            # --- Race number features (independent of date) ---
            race_num = None
            if numero_course is not None:
                try:
                    race_num = int(numero_course)
                except (ValueError, TypeError):
                    race_num = None

            if race_num is not None and race_num > 0:
                features["race_number_norm"] = round(
                    min(race_num, _MAX_RACE_NUMBER) / _MAX_RACE_NUMBER, 4
                )
                fill_counts["race_number_norm"] += 1

                features["is_evening_meeting"] = 1 if race_num > 6 else 0
                fill_counts["is_evening_meeting"] += 1
            else:
                features["race_number_norm"] = None
                features["is_evening_meeting"] = None

            # Write record
            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

            if n_written % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_written)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Temporal cyclical build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k in feature_names:
        v = fill_counts[k]
        pct = 100.0 * v / n_written if n_written else 0
        logger.info("  %-30s: %d/%d (%.1f%%)", k, v, n_written, pct)

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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features temporelles/cycliques a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/temporal_cyclical/)",
    )
    args = parser.parse_args()

    logger = setup_logging("temporal_cyclical_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "temporal_cyclical.jsonl"
    build_temporal_cyclical_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
