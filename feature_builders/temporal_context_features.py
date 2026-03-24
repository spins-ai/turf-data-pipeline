#!/usr/bin/env python3
"""
feature_builders.temporal_context_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
7 temporal context features derived from date_reunion_iso and heure_depart.

Features:
    1. temp_jour_semaine      — day of week (0=lundi, 6=dimanche)
    2. temp_mois              — month (1-12)
    3. temp_saison            — season string (printemps/ete/automne/hiver)
    4. temp_is_weekend        — 1 if samedi/dimanche, else 0
    5. temp_is_jour_ferie     — 1 if French public holiday, else 0
    6. temp_heure_course      — decimal hour (e.g. 14.5 for 14h30), None if unavailable
    7. temp_nb_jours_depuis_debut_saison — days since start of current season

Streams partants_master.jsonl line by line to limit memory usage.

Usage:
    python feature_builders/temporal_context_features.py
    python feature_builders/temporal_context_features.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("data_master", "partants_master.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "temporal_context_features")

# ---------------------------------------------------------------------------
# French public holidays (fixed dates). Easter-based movable holidays are
# computed per-year in _build_jours_feries().
# ---------------------------------------------------------------------------

_FIXED_HOLIDAYS_MD = [
    (1, 1),    # Jour de l'An
    (5, 1),    # Fete du Travail
    (5, 8),    # Victoire 1945
    (7, 14),   # Fete nationale
    (8, 15),   # Assomption
    (11, 1),   # Toussaint
    (11, 11),  # Armistice
    (12, 25),  # Noel
]

# Season boundaries (month, day) — meteorological seasons
_SEASON_BOUNDS = [
    ((3, 1), "printemps"),
    ((6, 1), "ete"),
    ((9, 1), "automne"),
    ((12, 1), "hiver"),
]

_SEASON_START_DATES = {
    "printemps": (3, 1),
    "ete":       (6, 1),
    "automne":   (9, 1),
    "hiver":     (12, 1),
}

# ===========================================================================
# HELPERS
# ===========================================================================


def _easter(year: int) -> date:
    """Compute Easter Sunday for a given year (Anonymous Gregorian algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7  # noqa: E741
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _build_jours_feries(year: int) -> set[date]:
    """Return the set of French public holidays for *year*."""
    holidays: set[date] = set()

    # Fixed holidays
    for m, d in _FIXED_HOLIDAYS_MD:
        holidays.add(date(year, m, d))

    # Easter-based movable holidays
    easter_d = _easter(year)
    from datetime import timedelta
    holidays.add(easter_d + timedelta(days=1))   # Lundi de Paques
    holidays.add(easter_d + timedelta(days=39))  # Ascension
    holidays.add(easter_d + timedelta(days=50))  # Lundi de Pentecote

    return holidays


# Cache per year to avoid recomputation
_feries_cache: dict[int, set[date]] = {}


def _is_jour_ferie(d: date) -> bool:
    yr = d.year
    if yr not in _feries_cache:
        _feries_cache[yr] = _build_jours_feries(yr)
    return d in _feries_cache[yr]


def _get_saison(month: int) -> str:
    if month in (3, 4, 5):
        return "printemps"
    if month in (6, 7, 8):
        return "ete"
    if month in (9, 10, 11):
        return "automne"
    return "hiver"


def _jours_depuis_debut_saison(d: date) -> int:
    """Days since the start of the current meteorological season."""
    saison = _get_saison(d.month)
    sm, sd = _SEASON_START_DATES[saison]
    if saison == "hiver" and d.month < 3:
        # Jan/Feb belong to winter that started Dec 1 of previous year
        start = date(d.year - 1, sm, sd)
    else:
        start = date(d.year, sm, sd)
    return (d - start).days


def _parse_heure(raw) -> float | None:
    """Parse heure_depart to decimal hours. Handles 'HH:MM', 'HHhMM', floats."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if not s:
        return None
    for sep in (":", "h", "H"):
        if sep in s:
            parts = s.split(sep, 1)
            try:
                h = int(parts[0])
                m = int(parts[1]) if parts[1] else 0
                return round(h + m / 60, 4)
            except (ValueError, IndexError):
                return None
    try:
        return float(s)
    except ValueError:
        return None


# ===========================================================================
# BUILDER  (streaming)
# ===========================================================================

def build_temporal_features_record(p: dict) -> dict:
    """Compute temporal features for a single partant record.

    Returns a dict with the 7 feature columns.
    """
    feat: dict = {}

    date_raw = p.get("date_reunion_iso")
    d: date | None = None
    if date_raw:
        try:
            d = datetime.strptime(str(date_raw)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            d = None

    if d is not None:
        # 1. jour_semaine  (Monday=0 … Sunday=6)
        feat["temp_jour_semaine"] = d.weekday()
        # 2. mois
        feat["temp_mois"] = d.month
        # 3. saison
        feat["temp_saison"] = _get_saison(d.month)
        # 4. is_weekend  (samedi=5, dimanche=6)
        feat["temp_is_weekend"] = 1 if d.weekday() >= 5 else 0
        # 5. is_jour_ferie
        feat["temp_is_jour_ferie"] = 1 if _is_jour_ferie(d) else 0
        # 7. nb_jours_depuis_debut_saison
        feat["temp_nb_jours_depuis_debut_saison"] = _jours_depuis_debut_saison(d)
    else:
        feat["temp_jour_semaine"] = None
        feat["temp_mois"] = None
        feat["temp_saison"] = None
        feat["temp_is_weekend"] = None
        feat["temp_is_jour_ferie"] = None
        feat["temp_nb_jours_depuis_debut_saison"] = None

    # 6. heure_course (from heure_depart if present)
    feat["temp_heure_course"] = _parse_heure(p.get("heure_depart"))

    return feat


def build_temporal_features(partants: list, logger: logging.Logger = None) -> list:
    """Build temporal features for an in-memory list of partants."""
    if logger is None:
        logger = logging.getLogger(__name__)

    results = []
    computed = 0
    for idx, p in enumerate(partants):
        feat = build_temporal_features_record(p)
        if feat.get("temp_jour_semaine") is not None:
            computed += 1
        p.update(feat)
        results.append(p)

        if (idx + 1) % 200_000 == 0:
            logger.info("  %d/%d traites, %d avec date", idx + 1, len(partants), computed)

    logger.info("Features temporelles: %d/%d avec date (%.1f%%)",
                computed, len(results), 100 * computed / max(len(results), 1))
    return results


# ===========================================================================
# STREAMING MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="7 temporal context features (streaming)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT,
                        help="Partants JSONL (streamed line by line)")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT,
                        help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("temporal_context_features")
    logger.info("=" * 70)
    logger.info("temporal_context_features.py")
    logger.info("=" * 70)

    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, "temporal_context_features.jsonl")

    total = 0
    computed = 0
    with open(args.input, "r", encoding="utf-8") as fin, \
         open(out_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
            except json.JSONDecodeError:
                continue

            feat = build_temporal_features_record(p)
            if feat.get("temp_jour_semaine") is not None:
                computed += 1
            p.update(feat)
            fout.write(json.dumps(p, ensure_ascii=False) + "\n")
            total += 1

            if total % 200_000 == 0:
                logger.info("  %d traites, %d avec date", total, computed)

    logger.info("Features temporelles: %d/%d avec date (%.1f%%)",
                computed, total, 100 * computed / max(total, 1))
    logger.info("Output: %s (%d lignes)", out_path, total)
    logger.info("Termine.")


if __name__ == "__main__":
    main()
