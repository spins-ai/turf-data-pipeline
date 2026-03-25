#!/usr/bin/env python3
"""
feature_builders.survival_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Career survival and attrition features for each horse.

Reads partants_master.jsonl in streaming mode (16 GB), processes all records
chronologically, and computes per-partant survival/longevity features.

These features capture a horse's career trajectory and reliability:
  - hazard_rate            : historical probability of DNF (did not finish)
  - top3_survival_rate     : cumulative % of races finishing in top 3
  - career_longevity_days  : days between first and last observed race
  - races_per_year         : average races per year over career
  - career_trend           : career-wide improving/declining trend (OLS slope
                             of normalised positions over all career races)

Temporal integrity: for any partant at date D, only races with date < D
contribute to survival statistics -- no future leakage.

Produces:
  - survival_features.jsonl   in output/survival_features/

Usage:
    python feature_builders/survival_features_builder.py
    python feature_builders/survival_features_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "survival_features"

# Progress log every N records
_LOG_EVERY = 500_000

# DNF indicators: position_arrivee values that mean "did not finish"
# In PMU data, non-finishers often have position 0, None, or very high values
# Also check statut fields for explicit indicators
_DNF_STATUTS = frozenset({
    "tombe", "arrete", "distancie", "disqualifie",
    "non_partant", "reste_au_poteau", "np",
})


# ===========================================================================
# CAREER STATE TRACKER
# ===========================================================================


class _SurvivalState:
    """Track career survival statistics for one horse."""

    __slots__ = (
        "nb_races", "nb_dnf", "nb_top3",
        "first_date", "last_date",
        "positions",  # list of (race_index, normalised_position) for trend
    )

    def __init__(self) -> None:
        self.nb_races: int = 0
        self.nb_dnf: int = 0
        self.nb_top3: int = 0
        self.first_date: str = ""
        self.last_date: str = ""
        self.positions: list[tuple[int, float]] = []


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_date_to_days(date_str: str) -> Optional[int]:
    """Convert ISO date string (YYYY-MM-DD) to an integer day count.

    Uses a simple calculation relative to 2000-01-01 for speed (no datetime).
    """
    if not date_str or len(date_str) < 10:
        return None
    try:
        y = int(date_str[:4])
        m = int(date_str[5:7])
        d = int(date_str[8:10])
        # Approximate days since epoch (good enough for deltas)
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


def _days_between(date_a: str, date_b: str) -> Optional[int]:
    """Approximate days between two ISO date strings."""
    da = _parse_date_to_days(date_a)
    db = _parse_date_to_days(date_b)
    if da is None or db is None:
        return None
    return abs(db - da)


def _is_dnf(rec: dict) -> bool:
    """Determine if a record represents a DNF (did not finish)."""
    # Check explicit statut fields
    statut = str(rec.get("statut", "") or "").lower().strip()
    if statut in _DNF_STATUTS:
        return True
    statut2 = str(rec.get("statut_partant", "") or "").lower().strip()
    if statut2 in _DNF_STATUTS:
        return True

    # Check if position is missing (no finish recorded) while the horse did start
    pos = rec.get("position")
    if pos is None and rec.get("non_partant") not in (True, 1, "1"):
        # No position and not a non-partant -- likely DNF
        # But be conservative: only flag if nb_partants exists (horse was in race)
        if rec.get("nb_partants") is not None or rec.get("course") is not None:
            return True
    return False


def _ols_slope(points: list[tuple[int, float]]) -> Optional[float]:
    """Simple OLS slope for (x, y) pairs. Returns None if < 3 points."""
    n = len(points)
    if n < 3:
        return None
    sum_x = sum_y = sum_xy = sum_x2 = 0.0
    for x, y in points:
        sum_x += x
        sum_y += y
        sum_xy += x * y
        sum_x2 += x * x
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return None
    slope = (n * sum_xy - sum_x * sum_y) / denom
    return slope


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


def _sort_key(rec: dict) -> tuple:
    """Sort key: date, course_uid, num_pmu for determinism."""
    return (rec["date"], rec["course"], rec["num"])


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_survival_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build survival/longevity features from partants_master.jsonl.

    Two-phase approach:
      1. Read minimal fields into memory, sort chronologically.
      2. Process date-by-date; emit pre-race snapshot, then update state.

    Memory budget (~3 GB ceiling):
      - Slim records: ~16M * ~180 bytes = ~2.9 GB
      - State dicts: ~390K horses * ~120 bytes = ~47 MB
    """
    logger.info("=== Survival Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Parse position safely
        pos_raw = rec.get("position_arrivee")
        pos: Optional[int] = None
        if pos_raw is not None:
            try:
                pos = int(pos_raw)
            except (ValueError, TypeError):
                pos = None

        # Parse nombre_partants for normalised position
        nb_raw = rec.get("nombre_partants") or rec.get("nb_partants")
        nb_partants: Optional[int] = None
        if nb_raw is not None:
            try:
                nb_partants = int(nb_raw)
            except (ValueError, TypeError):
                nb_partants = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "position": pos,
            "nb_partants": nb_partants,
            "statut": rec.get("statut_partant") or rec.get("statut"),
            "non_partant": rec.get("non_partant"),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=_sort_key)
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process date-by-date ──
    t2 = time.time()
    horse_state: dict[str, _SurvivalState] = defaultdict(_SurvivalState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        current_date = slim_records[i]["date"]
        date_group: list[dict] = []

        while i < total and slim_records[i]["date"] == current_date:
            date_group.append(slim_records[i])
            i += 1

        # ── Emit features (pre-update snapshot) ──
        for rec in date_group:
            cheval = rec["cheval"]

            if not cheval:
                results.append({
                    "partant_uid": rec["uid"],
                    "hazard_rate": None,
                    "top3_survival_rate": None,
                    "career_longevity_days": None,
                    "races_per_year": None,
                    "career_trend": None,
                })
                continue

            st = horse_state[cheval]

            if st.nb_races == 0:
                # First race ever -- no history
                results.append({
                    "partant_uid": rec["uid"],
                    "hazard_rate": None,
                    "top3_survival_rate": None,
                    "career_longevity_days": None,
                    "races_per_year": None,
                    "career_trend": None,
                })
            else:
                # hazard_rate: DNF / total races
                hazard = round(st.nb_dnf / st.nb_races, 6) if st.nb_races > 0 else None

                # top3_survival_rate: top3 / total
                top3_rate = round(st.nb_top3 / st.nb_races, 6) if st.nb_races > 0 else None

                # career_longevity_days
                longevity = _days_between(st.first_date, st.last_date)

                # races_per_year
                rpy = None
                if longevity is not None and longevity > 30:
                    years = longevity / 365.25
                    if years > 0:
                        rpy = round(st.nb_races / years, 4)
                elif st.nb_races > 0:
                    # Very short career (< 1 month), just report count
                    rpy = round(float(st.nb_races), 4)

                # career_trend: OLS slope of normalised positions
                trend = _ols_slope(st.positions)
                if trend is not None:
                    # Negative slope = improving (lower position = better)
                    # We negate so positive = improving
                    trend = round(-trend, 6)

                results.append({
                    "partant_uid": rec["uid"],
                    "hazard_rate": hazard,
                    "top3_survival_rate": top3_rate,
                    "career_longevity_days": longevity,
                    "races_per_year": rpy,
                    "career_trend": trend,
                })

        # ── Update state after emitting ──
        for rec in date_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            st = horse_state[cheval]

            # Skip non-partants entirely
            if rec.get("non_partant") in (True, 1, "1"):
                continue

            st.nb_races += 1

            # Track dates
            date_str = rec["date"]
            if not st.first_date or date_str < st.first_date:
                st.first_date = date_str
            if not st.last_date or date_str > st.last_date:
                st.last_date = date_str

            # DNF check
            if _is_dnf(rec):
                st.nb_dnf += 1

            # Top 3 check
            pos = rec["position"]
            if pos is not None and 1 <= pos <= 3:
                st.nb_top3 += 1

            # Track normalised position for trend
            if pos is not None and pos >= 1:
                nb = rec["nb_partants"]
                if nb is not None and nb >= 2:
                    norm_pos = pos / nb  # 0..1+ range; lower = better
                    st.positions.append((st.nb_races, norm_pos))

        n_processed += len(date_group)
        if n_processed % _LOG_EVERY < len(date_group):
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Survival build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_state),
    )

    return results


# ===========================================================================
# SAVE & CLI
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
        description="Construction des features de survie a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/survival_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("survival_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_survival_features(input_path, logger)

    # Save
    out_path = output_dir / "survival_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
