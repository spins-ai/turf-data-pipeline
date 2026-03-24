#!/usr/bin/env python3
"""
feature_builders.consistency_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Consistency and form-trend features for horse racing prediction.

Measures how predictable a horse's performances are and whether they are
improving or declining.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant consistency features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - consistency.jsonl   in output/consistency/

Features per partant (5):
  - position_std_5      : standard deviation of last 5 finishing positions
                          (low = consistent)
  - position_cv         : coefficient of variation of all career finishing
                          positions (std / mean)
  - best_worst_gap      : best position - worst position over last 10 races
  - dnf_rate            : fraction of career races that resulted in DNF
                          (non-finisher / disqualified / tombe)
  - improvement_trend   : OLS slope of last 10 finishing positions over time
                          (negative = improving; positions are 1-best)

Usage:
    python feature_builders/consistency_builder.py
    python feature_builders/consistency_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "consistency"

# Window sizes
WINDOW_STD = 5
WINDOW_TREND = 10

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


# ===========================================================================
# STATISTICS HELPERS
# ===========================================================================


def _std(values: list[float]) -> Optional[float]:
    """Population standard deviation."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(variance)


def _cv(values: list[float]) -> Optional[float]:
    """Coefficient of variation (std / mean). Returns None if mean ~ 0."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    if abs(mean) < 1e-9:
        return None
    sd = _std(values)
    if sd is None:
        return None
    return sd / mean


def _ols_slope(values: list[float]) -> Optional[float]:
    """OLS slope of values against their index (0, 1, 2, ...).

    Negative slope = improving (positions getting smaller = better).
    """
    n = len(values)
    if n < 3:
        return None
    # x = 0, 1, ..., n-1
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if abs(den) < 1e-12:
        return None
    return num / den


# ===========================================================================
# HORSE HISTORY TRACKER
# ===========================================================================


class _HorseHistory:
    """Track a horse's career finishing positions and DNF count."""

    __slots__ = ("positions", "nb_races", "nb_dnf")

    def __init__(self) -> None:
        self.positions: list[int] = []  # only valid finishing positions
        self.nb_races: int = 0
        self.nb_dnf: int = 0


def _is_dnf(rec: dict) -> bool:
    """Detect whether a record represents a non-finisher.

    Checks multiple fields: explicit statut, position_arrivee == 0 or None,
    'tombe', 'arrete', 'disqualifie', etc.
    """
    statut = str(rec.get("statut", "") or "").lower()
    if statut in ("tombe", "arrete", "disqualifie", "non_partant", "non-partant"):
        return True

    pos = rec.get("position")
    if pos is not None:
        try:
            if int(pos) == 0:
                return True
        except (ValueError, TypeError):
            pass

    # If position_arrivee is missing entirely and race happened, treat as DNF
    # (but only if the horse participated)
    if rec.get("position") is None and rec.get("is_np") is not True:
        return True

    return False


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_consistency_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build consistency features from partants_master.jsonl."""
    logger.info("=== Consistency Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        pos_raw = rec.get("position_arrivee")
        pos = None
        if pos_raw is not None:
            try:
                pos = int(pos_raw)
                if pos <= 0:
                    pos = None
            except (ValueError, TypeError):
                pos = None

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "position": pos,
            "statut": rec.get("statut"),
            "is_np": rec.get("is_non_partant", False),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process course by course ──
    t2 = time.time()
    horse_hist: dict[str, _HorseHistory] = defaultdict(_HorseHistory)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
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

        if not course_group:
            continue

        # ── Snapshot pre-race features for all partants ──
        for rec in course_group:
            h = rec["cheval"]
            hist = horse_hist[h] if h else _HorseHistory()

            positions = hist.positions
            nb_races = hist.nb_races

            # position_std_5: std of last 5 positions
            pos_std_5 = None
            if len(positions) >= 2:
                last5 = positions[-WINDOW_STD:]
                pos_std_5 = _std([float(p) for p in last5])

            # position_cv: CV over full career
            pos_cv = None
            if len(positions) >= 2:
                pos_cv = _cv([float(p) for p in positions])

            # best_worst_gap: over last 10 races
            bw_gap = None
            if positions:
                last10 = positions[-WINDOW_TREND:]
                bw_gap = min(last10) - max(last10)
                # Convention: gap = best - worst (negative or zero since best<=worst)
                # Actually best is smallest position number
                bw_gap = max(last10) - min(last10)

            # dnf_rate
            dnf_rate = None
            if nb_races > 0:
                dnf_rate = hist.nb_dnf / nb_races

            # improvement_trend: OLS slope of last 10 positions
            trend = None
            if len(positions) >= 3:
                last10 = positions[-WINDOW_TREND:]
                trend = _ols_slope([float(p) for p in last10])

            results.append({
                "partant_uid": rec["uid"],
                "position_std_5": round(pos_std_5, 4) if pos_std_5 is not None else None,
                "position_cv": round(pos_cv, 4) if pos_cv is not None else None,
                "best_worst_gap": bw_gap,
                "dnf_rate": round(dnf_rate, 4) if dnf_rate is not None else None,
                "improvement_trend": round(trend, 4) if trend is not None else None,
            })

        # ── Update history after race ──
        for rec in course_group:
            h = rec["cheval"]
            if not h:
                continue

            hist = horse_hist[h]
            hist.nb_races += 1

            if _is_dnf(rec):
                hist.nb_dnf += 1
            elif rec["position"] is not None:
                hist.positions.append(rec["position"])

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Consistency build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_hist),
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
        description="Construction des features de consistance a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/consistency/)",
    )
    args = parser.parse_args()

    logger = setup_logging("consistency_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_consistency_features(input_path, logger)

    # Save
    out_path = output_dir / "consistency.jsonl"
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
