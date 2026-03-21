#!/usr/bin/env python3
"""
feature_builders.perf_detaillees_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
40-60 features from performances_detaillees (output/22).

Rolling means of last 5/10 performances, volatility of positions/times/
reduction km, best/worst performance, consistency score, pattern detection
(improving/declining form).

Temporal integrity: for each partant at date D, only races with date < D
are used (no future leakage).

Usage:
    python feature_builders/perf_detaillees_builder.py
    python feature_builders/perf_detaillees_builder.py --input output/22_performances_detaillees/performances_detaillees.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.loaders import load_json_or_jsonl
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = os.path.join("output", "22_performances_detaillees", "performances_detaillees.jsonl")
PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "perf_detaillees_features")

# ===========================================================================
# HELPERS
# ===========================================================================

def _safe_mean(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 4)


def _safe_stdev(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    return round(statistics.stdev(clean), 4)


def _safe_min(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    return min(clean) if clean else None


def _safe_max(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    return max(clean) if clean else None


def _trend_slope(values: list) -> Optional[float]:
    """Simple linear regression slope over positions (1..n)."""
    clean = [v for v in values if v is not None]
    n = len(clean)
    if n < 3:
        return None
    x_mean = (n + 1) / 2
    y_mean = sum(clean) / n
    num = sum((i + 1 - x_mean) * (clean[i] - y_mean) for i in range(n))
    den = sum((i + 1 - x_mean) ** 2 for i in range(n))
    if den == 0:
        return None
    return round(num / den, 4)


def _consistency_score(positions: list) -> Optional[float]:
    """Lower is more consistent. Coefficient of variation of positions."""
    clean = [p for p in positions if p is not None and p > 0]
    if len(clean) < 3:
        return None
    m = sum(clean) / len(clean)
    if m == 0:
        return None
    sd = statistics.stdev(clean)
    return round(sd / m, 4)

# ===========================================================================
# LOAD
# ===========================================================================

# ===========================================================================
# BUILDER
# ===========================================================================

def build_perf_detaillees_features(partants: list, logger: logging.Logger) -> list:
    """Build 40-60 features from detailed performance history."""

    # Sort chronologically
    sorted_p = sorted(
        partants,
        key=lambda p: (
            str(p.get("date_reunion_iso", "") or ""),
            str(p.get("course_uid", "") or ""),
            p.get("num_pmu", 0) or 0,
        ),
    )

    horse_history: dict[str, list[dict]] = defaultdict(list)
    enriched = 0
    results = []

    for idx, p in enumerate(sorted_p):
        cheval = (p.get("nom_cheval") or "").upper().strip()
        if not cheval:
            results.append(p)
            continue

        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso]

        feat = {}

        if past:
            enriched += 1

            # === Rolling windows: 5, 10, 20 ===
            for wname, w in [("5", 5), ("10", 10), ("20", 20)]:
                window = past[-w:]

                positions = [r["position"] for r in window if r.get("position")]
                temps = [r["temps"] for r in window if r.get("temps")]
                reductions = [r["reduction_km"] for r in window if r.get("reduction_km")]
                gains = [r["gains"] for r in window if r.get("gains") is not None]

                # Position stats
                feat[f"perf_pos_moy_{wname}"] = _safe_mean(positions)
                feat[f"perf_pos_std_{wname}"] = _safe_stdev(positions)
                feat[f"perf_pos_min_{wname}"] = _safe_min(positions)
                feat[f"perf_pos_max_{wname}"] = _safe_max(positions)
                feat[f"perf_pos_median_{wname}"] = round(statistics.median(positions), 2) if positions else None

                # Time stats
                feat[f"perf_temps_moy_{wname}"] = _safe_mean(temps)
                feat[f"perf_temps_std_{wname}"] = _safe_stdev(temps)
                feat[f"perf_temps_best_{wname}"] = _safe_min(temps)

                # Reduction km stats
                feat[f"perf_reduc_moy_{wname}"] = _safe_mean(reductions)
                feat[f"perf_reduc_std_{wname}"] = _safe_stdev(reductions)
                feat[f"perf_reduc_best_{wname}"] = _safe_min(reductions)

                # Gains
                feat[f"perf_gains_moy_{wname}"] = _safe_mean(gains)
                feat[f"perf_gains_total_{wname}"] = round(sum(gains), 2) if gains else None

                # Win / place counts in window
                n_win = sum(1 for r in window if r.get("position") == 1)
                n_place = sum(1 for r in window if r.get("position") and r["position"] <= 3)
                n_valid = len(window)
                feat[f"perf_win_count_{wname}"] = n_win
                feat[f"perf_place_count_{wname}"] = n_place
                feat[f"perf_win_rate_{wname}"] = round(n_win / n_valid, 4) if n_valid else None
                feat[f"perf_place_rate_{wname}"] = round(n_place / n_valid, 4) if n_valid else None

            # === Consistency score (last 10) ===
            pos_10 = [r["position"] for r in past[-10:] if r.get("position")]
            feat["perf_consistency_10"] = _consistency_score(pos_10)

            # === Trend / pattern detection ===
            pos_all = [r["position"] for r in past[-10:] if r.get("position")]
            feat["perf_trend_slope"] = _trend_slope(pos_all)
            slope = feat["perf_trend_slope"]
            if slope is not None:
                if slope < -0.3:
                    feat["perf_form_pattern"] = "improving"
                elif slope > 0.3:
                    feat["perf_form_pattern"] = "declining"
                else:
                    feat["perf_form_pattern"] = "stable"
            else:
                feat["perf_form_pattern"] = None

            # === Best / worst ever (capped at last 20) ===
            pos_20 = [r["position"] for r in past[-20:] if r.get("position")]
            feat["perf_best_pos_20"] = _safe_min(pos_20)
            feat["perf_worst_pos_20"] = _safe_max(pos_20)

            # === DNF rate ===
            dnf_10 = sum(1 for r in past[-10:] if r.get("dnf"))
            feat["perf_dnf_rate_10"] = round(dnf_10 / min(len(past), 10), 4)

            # === Days since last performance ===
            if past:
                feat["perf_nb_past"] = len(past)

        # Merge features into partant
        p.update(feat)
        results.append(p)

        # Update history
        position = None
        for key in ("classement", "arrivee", "place", "position_arrivee"):
            v = p.get(key)
            if v is not None:
                try:
                    position = int(v)
                    break
                except (ValueError, TypeError):
                    pass

        gains_val = 0
        try:
            gains_val = float(p.get("gains_course") or p.get("gains") or 0)
        except (ValueError, TypeError):
            pass

        temps_val = None
        try:
            temps_val = float(p.get("temps_obtenu") or p.get("temps_course") or 0) or None
        except (ValueError, TypeError):
            pass

        reduc_val = None
        try:
            reduc_val = float(p.get("reduction_km_ms") or p.get("reduction_km") or 0) or None
        except (ValueError, TypeError):
            pass

        is_dnf = p.get("est_non_partant") or p.get("statut") in ("non_partant", "tombe", "arrete")

        horse_history[cheval].append({
            "date": date_iso,
            "position": position,
            "gains": gains_val,
            "temps": temps_val,
            "reduction_km": reduc_val,
            "dnf": bool(is_dnf),
        })

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(sorted_p), enriched)

    logger.info("Features perf_detaillees: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Features from performances detaillees (output/22)")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("perf_detaillees_builder")
    logger.info("=" * 70)
    logger.info("perf_detaillees_builder.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_perf_detaillees_features(partants, logger)

    out_path = os.path.join(args.output_dir, "perf_detaillees_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
