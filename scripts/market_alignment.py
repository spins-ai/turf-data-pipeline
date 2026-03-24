#!/usr/bin/env python3
"""
scripts/market_alignment.py — Pilier 20 : Alignement Turf/Marche
=================================================================
Compare our predicted probabilities with market odds.

Steps:
  1. Stream partants_master, compute implied probability from cote_finale
  2. Compare with our Elo-based probability estimate
  3. Find systematic biases (disciplines/hippodromes where market is wrong)
  4. Compute calibration curve data (predicted vs actual win rates in deciles)

Outputs:
  - quality/market_alignment_report.md

RAM budget: < 1 GB (streams JSONL line-by-line, aggregates in memory).

Usage:
    python scripts/market_alignment.py
"""

from __future__ import annotations

import json
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    DATA_MASTER_DIR,
    OUTPUT_ELO,
    PARTANTS_MASTER,
    QUALITY_DIR,
)
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"market_alignment_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_PATH = QUALITY_DIR / "market_alignment_report.md"
ELO_RATINGS_FILE = OUTPUT_ELO / "elo_ratings.jsonl"
N_DECILES = 10
MIN_BUCKET_SIZE = 30  # minimum races per bucket for statistical significance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _implied_probability(cote: float) -> float | None:
    """Convert decimal odds to implied probability.

    cote_finale is typically in decimal European format (e.g. 5.0 means 4/1).
    Implied probability = 1 / cote.
    Returns None if cote is invalid (<= 0).
    """
    if cote is None or cote <= 0:
        return None
    return 1.0 / cote


def _elo_to_probability(elo_self: float, elo_field_avg: float) -> float:
    """Convert Elo rating difference to win probability.

    Uses the standard Elo formula: P = 1 / (1 + 10^((avg - self) / 400))
    """
    diff = elo_field_avg - elo_self
    return 1.0 / (1.0 + math.pow(10, diff / 400.0))


def _stream_partants() -> Any:
    """Stream partants_master.jsonl line by line."""
    if not PARTANTS_MASTER.exists():
        logger.warning("partants_master.jsonl not found at %s", PARTANTS_MASTER)
        return

    with open(PARTANTS_MASTER, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                if line_num <= 5:
                    logger.warning("Skipping malformed line %d", line_num)
                continue


def _load_elo_ratings() -> dict[str, float]:
    """Load Elo ratings index (cheval_id -> elo_rating).

    Returns empty dict if file doesn't exist.
    """
    elo_map: dict[str, float] = {}
    if not ELO_RATINGS_FILE.exists():
        logger.info("No Elo ratings file found at %s", ELO_RATINGS_FILE)
        return elo_map

    with open(ELO_RATINGS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                cid = rec.get("cheval_id") or rec.get("id")
                elo = rec.get("elo") or rec.get("elo_rating")
                if cid and elo is not None:
                    elo_map[str(cid)] = float(elo)
            except (json.JSONDecodeError, ValueError):
                continue
    logger.info("Loaded %d Elo ratings", len(elo_map))
    return elo_map


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
def analyze_market_alignment() -> dict[str, Any]:
    """Main analysis: compare market odds with Elo-based probability."""
    elo_map = _load_elo_ratings()
    default_elo = 1500.0

    # Accumulators
    total_records = 0
    records_with_cote = 0
    records_with_elo = 0
    records_with_result = 0

    # Calibration: bucket by predicted probability decile
    # Each bucket: (sum_predicted, sum_actual_wins, count)
    market_calibration: list[list[float]] = [[0.0, 0.0, 0] for _ in range(N_DECILES)]
    elo_calibration: list[list[float]] = [[0.0, 0.0, 0] for _ in range(N_DECILES)]

    # Bias by discipline
    discipline_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"market_wins": 0, "elo_wins": 0, "total": 0, "surprises": 0}
    )

    # Bias by hippodrome
    hippo_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"market_sum_prob": 0.0, "actual_wins": 0, "total": 0}
    )

    for rec in _stream_partants():
        total_records += 1

        cote = rec.get("cote_finale") or rec.get("coteFinaleDirect")
        if cote is None:
            continue

        try:
            cote = float(cote)
        except (ValueError, TypeError):
            continue

        impl_prob = _implied_probability(cote)
        if impl_prob is None:
            continue
        records_with_cote += 1

        # Determine if this horse won
        place = rec.get("place") or rec.get("ordreArrivee") or rec.get("rang")
        is_winner = False
        if place is not None:
            try:
                is_winner = int(place) == 1
                records_with_result += 1
            except (ValueError, TypeError):
                pass

        # Elo probability
        cheval_id = str(rec.get("cheval_id") or rec.get("chevalId") or "")
        elo_self = elo_map.get(cheval_id, default_elo)
        has_elo = cheval_id in elo_map
        if has_elo:
            records_with_elo += 1
        # Simple estimate: use default_elo as field average
        elo_prob = _elo_to_probability(elo_self, default_elo)

        # Market calibration: assign to decile
        decile_idx = min(int(impl_prob * N_DECILES), N_DECILES - 1)
        market_calibration[decile_idx][0] += impl_prob
        market_calibration[decile_idx][1] += 1.0 if is_winner else 0.0
        market_calibration[decile_idx][2] += 1

        # Elo calibration
        elo_decile_idx = min(int(elo_prob * N_DECILES), N_DECILES - 1)
        elo_calibration[elo_decile_idx][0] += elo_prob
        elo_calibration[elo_decile_idx][1] += 1.0 if is_winner else 0.0
        elo_calibration[elo_decile_idx][2] += 1

        # Discipline bias
        discipline = rec.get("discipline") or rec.get("typeCourse") or "unknown"
        d_stats = discipline_stats[discipline]
        d_stats["total"] += 1
        if is_winner and impl_prob > 0.2:
            d_stats["market_wins"] += 1
        if is_winner and cote > 10.0:
            d_stats["surprises"] += 1

        # Hippodrome bias
        hippo = rec.get("hippodrome") or rec.get("hippodromeCode") or "unknown"
        h_stats = hippo_stats[hippo]
        h_stats["market_sum_prob"] += impl_prob
        h_stats["actual_wins"] += 1 if is_winner else 0
        h_stats["total"] += 1

    # Compute calibration curves
    market_curve = []
    for i, (sum_pred, sum_wins, count) in enumerate(market_calibration):
        if count >= MIN_BUCKET_SIZE:
            avg_pred = sum_pred / count
            actual_rate = sum_wins / count
            market_curve.append({
                "decile": i,
                "range": f"{i * 10}%-{(i + 1) * 10}%",
                "avg_predicted": round(avg_pred, 4),
                "actual_win_rate": round(actual_rate, 4),
                "count": count,
                "bias": round(actual_rate - avg_pred, 4),
            })

    elo_curve = []
    for i, (sum_pred, sum_wins, count) in enumerate(elo_calibration):
        if count >= MIN_BUCKET_SIZE:
            avg_pred = sum_pred / count
            actual_rate = sum_wins / count
            elo_curve.append({
                "decile": i,
                "range": f"{i * 10}%-{(i + 1) * 10}%",
                "avg_predicted": round(avg_pred, 4),
                "actual_win_rate": round(actual_rate, 4),
                "count": count,
                "bias": round(actual_rate - avg_pred, 4),
            })

    # Find hippodromes with strongest biases
    hippo_biases = []
    for hippo, stats in hippo_stats.items():
        if stats["total"] >= MIN_BUCKET_SIZE:
            expected = stats["market_sum_prob"]
            actual = stats["actual_wins"]
            if expected > 0:
                bias_ratio = actual / expected
                hippo_biases.append({
                    "hippodrome": hippo,
                    "expected_wins": round(expected, 1),
                    "actual_wins": actual,
                    "bias_ratio": round(bias_ratio, 3),
                    "total_records": stats["total"],
                })
    hippo_biases.sort(key=lambda x: abs(x["bias_ratio"] - 1.0), reverse=True)

    return {
        "total_records": total_records,
        "records_with_cote": records_with_cote,
        "records_with_elo": records_with_elo,
        "records_with_result": records_with_result,
        "market_calibration": market_curve,
        "elo_calibration": elo_curve,
        "discipline_stats": dict(discipline_stats),
        "hippodrome_biases": hippo_biases[:20],  # top 20
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def _generate_report(results: dict[str, Any], elapsed: float) -> None:
    """Write the market alignment report as Markdown."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Pilier 20 — Alignement Turf/Marche",
        "",
        f"**Date**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Duree**: {elapsed:.1f}s",
        "",
        "## Donnees analysees",
        "",
        f"- Records scannes: {results['total_records']:,}",
        f"- Records avec cote: {results['records_with_cote']:,}",
        f"- Records avec Elo: {results['records_with_elo']:,}",
        f"- Records avec resultat: {results['records_with_result']:,}",
        "",
    ]

    # Market calibration
    lines.append("## Calibration marche (cote_finale)")
    lines.append("")
    mc = results.get("market_calibration", [])
    if mc:
        lines.append("| Decile | Plage | Prob. predite | Taux victoire reel | Biais | N |")
        lines.append("|--------|-------|---------------|-------------------|-------|---|")
        for row in mc:
            lines.append(
                f"| {row['decile']} | {row['range']} | {row['avg_predicted']:.4f} "
                f"| {row['actual_win_rate']:.4f} | {row['bias']:+.4f} | {row['count']:,} |"
            )
    else:
        lines.append("*Pas assez de donnees pour la calibration marche.*")
    lines.append("")

    # Elo calibration
    lines.append("## Calibration Elo")
    lines.append("")
    ec = results.get("elo_calibration", [])
    if ec:
        lines.append("| Decile | Plage | Prob. predite | Taux victoire reel | Biais | N |")
        lines.append("|--------|-------|---------------|-------------------|-------|---|")
        for row in ec:
            lines.append(
                f"| {row['decile']} | {row['range']} | {row['avg_predicted']:.4f} "
                f"| {row['actual_win_rate']:.4f} | {row['bias']:+.4f} | {row['count']:,} |"
            )
    else:
        lines.append("*Pas assez de donnees pour la calibration Elo.*")
    lines.append("")

    # Hippodrome biases
    lines.append("## Biais par hippodrome (top 20)")
    lines.append("")
    hb = results.get("hippodrome_biases", [])
    if hb:
        lines.append("| Hippodrome | Victoires attendues | Victoires reelles | Ratio biais | N |")
        lines.append("|------------|--------------------|--------------------|-------------|---|")
        for row in hb:
            lines.append(
                f"| {row['hippodrome']} | {row['expected_wins']} "
                f"| {row['actual_wins']} | {row['bias_ratio']:.3f} | {row['total_records']:,} |"
            )
    else:
        lines.append("*Pas assez de donnees pour les biais par hippodrome.*")
    lines.append("")

    # Discipline stats
    lines.append("## Statistiques par discipline")
    lines.append("")
    ds = results.get("discipline_stats", {})
    if ds:
        lines.append("| Discipline | Total | Favoris gagnants | Surprises (cote>10) |")
        lines.append("|------------|-------|-------------------|---------------------|")
        for disc, stats in sorted(ds.items(), key=lambda x: x[1]["total"], reverse=True):
            lines.append(
                f"| {disc} | {stats['total']:,} | {stats['market_wins']:,} | {stats['surprises']:,} |"
            )
    lines.append("")

    lines.append("---")
    lines.append("*Genere par scripts/market_alignment.py (Pilier 20)*")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report written to %s", REPORT_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """Run market alignment analysis."""
    t0 = time.time()
    logger.info("=== Pilier 20 : Alignement Turf/Marche ===")

    results = analyze_market_alignment()

    elapsed = time.time() - t0
    _generate_report(results, elapsed)
    logger.info(
        "Analysis complete: %d records, %d with cote, %d with results (%.1fs)",
        results["total_records"],
        results["records_with_cote"],
        results["records_with_result"],
        elapsed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
