#!/usr/bin/env python3
"""
feature_builders.quantile_regression_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Quantile-based features useful for bet sizing and uncertainty estimation.

Reads partants_master.jsonl in two phases:
  - Phase 1: index + sort chronologically
  - Phase 2: seek-based with state tracking, process course-by-course

Temporal integrity: for any partant at date D, only races with date < D
contribute to the quantile features -- no future leakage.

Produces:
  - quantile_regression_features.jsonl  in builder_outputs/quantile_regression/

Features per partant (10):
  - qr_odds_percentile         : where cote_finale falls in global odds distribution (0-1)
  - qr_horse_best_position     : best (min) position_arrivee achieved so far
  - qr_horse_worst_position    : worst (max) position among finishers
  - qr_horse_median_position   : median position from history
  - qr_horse_position_iqr      : IQR (Q3-Q1) of positions - consistency measure
  - qr_horse_gains_percentile  : percentile of gains_carriere among all horses seen
  - qr_horse_upside_potential   : ratio best_position / median_position (lower = more upside)
  - qr_odds_vs_horse_median    : cote_finale / median historical odds for this horse
  - qr_field_avg_experience    : avg nb_courses_carriere of all horses in this race
  - qr_horse_consistency_score : 1 - (std / mean) of positions (inverted CV)

Usage:
    python feature_builders/quantile_regression_builder.py
    python feature_builders/quantile_regression_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import bisect
import gc
import json
import math
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/quantile_regression")
OUTPUT_FILENAME = "quantile_regression_features.jsonl"

_LOG_EVERY = 500_000
_HORSE_HISTORY_MAXLEN = 20


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _median(sorted_vals: list[float]) -> float:
    """Median of an already-sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 1:
        return sorted_vals[mid]
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def _iqr(sorted_vals: list[float]) -> float:
    """IQR of an already-sorted list."""
    n = len(sorted_vals)
    if n < 4:
        return 0.0
    q1_idx = n // 4
    q3_idx = (3 * n) // 4
    return sorted_vals[q3_idx] - sorted_vals[q1_idx]


def _std_mean(vals: list[float]) -> tuple[float, float]:
    """Return (std, mean) of a list of floats."""
    n = len(vals)
    if n == 0:
        return (0.0, 0.0)
    m = sum(vals) / n
    if n < 2:
        return (0.0, m)
    variance = sum((x - m) ** 2 for x in vals) / (n - 1)
    return (math.sqrt(variance), m)


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
# STATE TRACKERS
# ===========================================================================


class _HorseState:
    """Per-horse rolling state: last N positions and last N odds."""

    __slots__ = ("positions", "odds")

    def __init__(self) -> None:
        self.positions: deque[float] = deque(maxlen=_HORSE_HISTORY_MAXLEN)
        self.odds: deque[float] = deque(maxlen=_HORSE_HISTORY_MAXLEN)

    def sorted_positions(self) -> list[float]:
        return sorted(self.positions)

    def sorted_odds(self) -> list[float]:
        return sorted(self.odds)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_quantile_regression_features(input_path: Path, output_path: Path, logger) -> int:
    """Two-phase build of quantile regression features.

    Returns total number of feature records written.
    """
    logger.info("=" * 70)
    logger.info("quantile_regression_builder.py -- Quantile-based features")
    logger.info("=" * 70)
    t0 = time.time()

    # ── Phase 1: index + sort chronologically ──────────────────────────
    logger.info("Phase 1: Chargement et tri chronologique...")

    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Phase 1: %d records charges...", n_read)
            gc.collect()

        slim_records.append({
            "uid": rec.get("partant_uid", ""),
            "date": str(rec.get("date_reunion_iso", "") or "")[:10],
            "course": str(rec.get("course_uid", "") or ""),
            "num": rec.get("num_pmu", 0) or 0,
            "horse": str(rec.get("horse_id", "") or ""),
            "cote": _safe_float(rec.get("cote_finale")),
            "pos": _safe_int(rec.get("position_arrivee")),
            "gagnant": bool(rec.get("is_gagnant")),
            "place": bool(rec.get("is_place")),
            "gains": _safe_float(rec.get("gains_carriere_euros")),
            "nb_vic": _safe_int(rec.get("nb_victoires_carriere")),
            "nb_courses": _safe_int(rec.get("nb_courses_carriere")),
            "distance": _safe_int(rec.get("distance")),
            "discipline": str(rec.get("discipline", "") or ""),
            "nb_partants": _safe_int(rec.get("nombre_partants")),
        })

    logger.info(
        "Phase 1: %d records charges en %.1fs", len(slim_records), time.time() - t0
    )

    # Sort chronologically, then by course, then by num_pmu
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("  Tri chronologique termine.")

    # ── Phase 2: seek-based with state tracking ────────────────────────
    logger.info("Phase 2: Calcul des features course par course...")
    t1 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    # Global state
    horse_state: dict[str, _HorseState] = {}
    global_odds: list[float] = []  # sorted list for percentile via bisect
    all_gains_seen: list[float] = []  # sorted list for percentile via bisect

    n_written = 0

    fill_counts = {
        "qr_odds_percentile": 0,
        "qr_horse_best_position": 0,
        "qr_horse_worst_position": 0,
        "qr_horse_median_position": 0,
        "qr_horse_position_iqr": 0,
        "qr_horse_gains_percentile": 0,
        "qr_horse_upside_potential": 0,
        "qr_odds_vs_horse_median": 0,
        "qr_field_avg_experience": 0,
        "qr_horse_consistency_score": 0,
    }

    total = len(slim_records)
    i = 0

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        while i < total:
            # Group records by course_uid (consecutive thanks to sort)
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

            # --- Compute qr_field_avg_experience for this race ---
            exp_values = []
            for rec in course_group:
                nb_c = rec["nb_courses"]
                if nb_c is not None:
                    exp_values.append(nb_c)
            field_avg_exp = round(sum(exp_values) / len(exp_values), 2) if exp_values else None

            # --- Snapshot features BEFORE updating state (temporal integrity) ---
            post_updates: list[tuple[str, Optional[float], Optional[int]]] = []

            for rec in course_group:
                uid = rec["uid"]
                horse = rec["horse"]
                cote = rec["cote"]
                pos = rec["pos"]
                gains = rec["gains"]

                features: dict[str, Any] = {"partant_uid": uid}

                # Get horse state (read BEFORE update)
                hs = horse_state.get(horse)

                # 1. qr_odds_percentile: where cote_finale falls in global distribution
                if cote is not None and cote > 0 and len(global_odds) > 0:
                    rank = bisect.bisect_left(global_odds, cote)
                    features["qr_odds_percentile"] = round(rank / len(global_odds), 4)
                    fill_counts["qr_odds_percentile"] += 1
                else:
                    features["qr_odds_percentile"] = None

                # Features 2-5, 7, 8, 10: require horse history
                if hs and len(hs.positions) > 0:
                    sp = hs.sorted_positions()

                    # 2. qr_horse_best_position
                    features["qr_horse_best_position"] = int(sp[0])
                    fill_counts["qr_horse_best_position"] += 1

                    # 3. qr_horse_worst_position
                    features["qr_horse_worst_position"] = int(sp[-1])
                    fill_counts["qr_horse_worst_position"] += 1

                    # 4. qr_horse_median_position
                    med_pos = _median(sp)
                    features["qr_horse_median_position"] = round(med_pos, 2)
                    fill_counts["qr_horse_median_position"] += 1

                    # 5. qr_horse_position_iqr
                    features["qr_horse_position_iqr"] = round(_iqr(sp), 2)
                    fill_counts["qr_horse_position_iqr"] += 1

                    # 7. qr_horse_upside_potential: best / median (lower = more upside)
                    if med_pos > 0:
                        features["qr_horse_upside_potential"] = round(sp[0] / med_pos, 4)
                        fill_counts["qr_horse_upside_potential"] += 1
                    else:
                        features["qr_horse_upside_potential"] = None

                    # 10. qr_horse_consistency_score: 1 - CV
                    pos_list = list(hs.positions)
                    std_val, mean_val = _std_mean(pos_list)
                    if mean_val > 0 and len(pos_list) >= 2:
                        cv = std_val / mean_val
                        features["qr_horse_consistency_score"] = round(max(0.0, 1.0 - cv), 4)
                        fill_counts["qr_horse_consistency_score"] += 1
                    else:
                        features["qr_horse_consistency_score"] = None
                else:
                    features["qr_horse_best_position"] = None
                    features["qr_horse_worst_position"] = None
                    features["qr_horse_median_position"] = None
                    features["qr_horse_position_iqr"] = None
                    features["qr_horse_upside_potential"] = None
                    features["qr_horse_consistency_score"] = None

                # 6. qr_horse_gains_percentile
                if gains is not None and len(all_gains_seen) > 0:
                    rank_g = bisect.bisect_left(all_gains_seen, gains)
                    features["qr_horse_gains_percentile"] = round(rank_g / len(all_gains_seen), 4)
                    fill_counts["qr_horse_gains_percentile"] += 1
                else:
                    features["qr_horse_gains_percentile"] = None

                # 8. qr_odds_vs_horse_median: cote_finale / median historical odds
                if cote is not None and cote > 0 and hs and len(hs.odds) > 0:
                    so = hs.sorted_odds()
                    med_odds = _median(so)
                    if med_odds > 0:
                        features["qr_odds_vs_horse_median"] = round(cote / med_odds, 4)
                        fill_counts["qr_odds_vs_horse_median"] += 1
                    else:
                        features["qr_odds_vs_horse_median"] = None
                else:
                    features["qr_odds_vs_horse_median"] = None

                # 9. qr_field_avg_experience
                if field_avg_exp is not None:
                    features["qr_field_avg_experience"] = field_avg_exp
                    fill_counts["qr_field_avg_experience"] += 1
                else:
                    features["qr_field_avg_experience"] = None

                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                # Prepare deferred update
                post_updates.append((horse, cote, pos))

            # --- Update state AFTER all features in this course are emitted ---
            for horse, cote, pos in post_updates:
                if not horse:
                    continue

                if horse not in horse_state:
                    horse_state[horse] = _HorseState()

                hs = horse_state[horse]

                # Update position history (only valid finishers)
                if pos is not None and pos > 0:
                    hs.positions.append(float(pos))

                # Update odds history
                if cote is not None and cote > 0:
                    hs.odds.append(cote)
                    bisect.insort(global_odds, cote)

            # Update gains seen (use current gains_carriere for percentile ranking)
            for rec in course_group:
                g = rec["gains"]
                if g is not None:
                    bisect.insort(all_gains_seen, g)

            if n_written % _LOG_EVERY == 0 and n_written > 0:
                logger.info("  Phase 2: %d / %d records traites...", n_written, total)
                gc.collect()

    # Atomic replace
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features ecrites en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features quantile regression a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("quantile_regression_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_quantile_regression_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
