#!/usr/bin/env python3
"""
scripts/roi_analyzer.py — Pilier 16 : Rentabilite Turf
========================================================
Analyze historical betting outcomes from pipeline data to compute
Return On Investment (ROI) across multiple dimensions.

Streams training_labels.jsonl or partants_master.jsonl and computes:
  - Overall ROI if betting on every favorite (lowest cote_finale)
  - ROI by discipline (plat / trot attele / trot monte / obstacle)
  - ROI by hippodrome (top 20)
  - ROI by price range (<2, 2-5, 5-10, 10-20, 20+)
  - ROI by year
  - Best value profiles (where actual win rate > implied probability)

Outputs:
  - quality/roi_analysis_report.md

RAM budget: < 2 GB (streaming, per-group accumulators only).

Usage:
    python scripts/roi_analyzer.py
    python scripts/roi_analyzer.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import QUALITY_DIR, TRAINING_LABELS, PARTANTS_MASTER

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_PATH = QUALITY_DIR / "roi_analysis_report.md"
_TODAY = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Price range buckets
PRICE_RANGES = [
    ("< 2.0", 0.0, 2.0),
    ("2.0 - 5.0", 2.0, 5.0),
    ("5.0 - 10.0", 5.0, 10.0),
    ("10.0 - 20.0", 10.0, 20.0),
    ("20.0+", 20.0, float("inf")),
]


# ---------------------------------------------------------------------------
# Accumulators (lightweight, no record storage)
# ---------------------------------------------------------------------------
class ROIAccumulator:
    """Accumulate bets / wins / returns for ROI calculation."""

    __slots__ = ("bets", "wins", "total_return")

    def __init__(self) -> None:
        self.bets: int = 0
        self.wins: int = 0
        self.total_return: float = 0.0

    def add(self, is_win: bool, cote: float) -> None:
        """Record a 1-unit flat bet. Return = cote if win, else 0."""
        self.bets += 1
        if is_win:
            self.wins += 1
            self.total_return += cote  # net return includes stake at PMU

    @property
    def roi_pct(self) -> float:
        """ROI as percentage: (total_return - bets) / bets * 100."""
        if self.bets == 0:
            return 0.0
        return ((self.total_return - self.bets) / self.bets) * 100.0

    @property
    def win_rate(self) -> float:
        """Win rate as fraction."""
        if self.bets == 0:
            return 0.0
        return self.wins / self.bets

    def summary(self) -> dict:
        return {
            "bets": self.bets,
            "wins": self.wins,
            "win_rate": round(self.win_rate * 100, 2),
            "total_return": round(self.total_return, 2),
            "roi_pct": round(self.roi_pct, 2),
        }


# ---------------------------------------------------------------------------
# Streaming reader
# ---------------------------------------------------------------------------
def _iter_records(path: Path) -> Any:
    """Stream JSONL records one at a time."""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue


def _get_price_range(cote: float) -> str:
    """Return the price range label for a given cote."""
    for label, lo, hi in PRICE_RANGES:
        if lo <= cote < hi:
            return label
    return "20.0+"


def _extract_year(rec: dict) -> str | None:
    """Extract year from date_reunion_iso or date field."""
    date_str = rec.get("date_reunion_iso") or rec.get("date") or ""
    if len(date_str) >= 4:
        return date_str[:4]
    return None


# ---------------------------------------------------------------------------
# Phase 1: find favorites per course
# ---------------------------------------------------------------------------
def _find_favorites_and_analyze(path: Path) -> dict:
    """Single-pass streaming analysis.

    We need to find the favorite (lowest cote_finale) per course to compute
    favorite ROI. We also accumulate per-dimension stats for every runner.

    Strategy: buffer records per course_uid, then process each course group.
    To keep RAM low, we flush each course group as soon as we detect a new one
    (requires data sorted by course_uid, which partants_master typically is).
    If not sorted, we use a bounded buffer.
    """

    # Accumulators
    overall = ROIAccumulator()
    favorite_acc = ROIAccumulator()
    by_discipline: dict[str, ROIAccumulator] = defaultdict(ROIAccumulator)
    by_hippodrome: dict[str, ROIAccumulator] = defaultdict(ROIAccumulator)
    by_price_range: dict[str, ROIAccumulator] = defaultdict(ROIAccumulator)
    by_year: dict[str, ROIAccumulator] = defaultdict(ROIAccumulator)
    # For value analysis: by price range, track implied vs actual
    value_tracker: dict[str, dict] = defaultdict(lambda: {
        "implied_sum": 0.0, "actual_wins": 0, "count": 0
    })

    total_records = 0
    skipped_no_cote = 0

    # Buffer for current course group
    current_course_uid: str | None = None
    course_buffer: list[dict] = []
    # Max buffer size to handle unsorted data gracefully
    MAX_BUFFER = 50_000
    course_groups_processed = 0

    def _flush_course(buffer: list[dict]) -> None:
        """Process a course group: find favorite, update accumulators."""
        nonlocal course_groups_processed
        if not buffer:
            return

        # Find favorite (lowest non-zero cote_finale)
        valid = [r for r in buffer if r.get("_cote") and r["_cote"] > 0]
        if valid:
            favorite = min(valid, key=lambda r: r["_cote"])
            favorite_acc.add(
                favorite.get("_is_win", False),
                favorite["_cote"],
            )

        course_groups_processed += 1

    def _process_record(rec: dict) -> dict | None:
        """Extract relevant fields, update per-record accumulators."""
        nonlocal total_records, skipped_no_cote

        total_records += 1

        cote = rec.get("cote_finale")
        if cote is None:
            skipped_no_cote += 1
            return None

        try:
            cote = float(cote)
        except (ValueError, TypeError):
            skipped_no_cote += 1
            return None

        if cote <= 0:
            skipped_no_cote += 1
            return None

        # Determine win
        is_win = False
        pos = rec.get("position_arrivee")
        if pos is not None:
            try:
                is_win = int(pos) == 1
            except (ValueError, TypeError):
                pass
        if not is_win:
            is_win = bool(rec.get("is_gagnant"))

        # Overall
        overall.add(is_win, cote)

        # By discipline
        discipline = rec.get("discipline") or "inconnu"
        by_discipline[discipline].add(is_win, cote)

        # By hippodrome
        hippo = rec.get("hippodrome_normalise") or rec.get("hippodrome") or "inconnu"
        by_hippodrome[hippo].add(is_win, cote)

        # By price range
        pr = _get_price_range(cote)
        by_price_range[pr].add(is_win, cote)

        # By year
        year = _extract_year(rec)
        if year:
            by_year[year].add(is_win, cote)

        # Value tracker: implied probability = 1/cote
        implied_prob = 1.0 / cote if cote > 0 else 0
        vt = value_tracker[pr]
        vt["implied_sum"] += implied_prob
        vt["actual_wins"] += 1 if is_win else 0
        vt["count"] += 1

        return {
            "_cote": cote,
            "_is_win": is_win,
            "_course_uid": rec.get("course_uid", ""),
        }

    # Stream records
    for rec in _iter_records(path):
        slim = _process_record(rec)
        if slim is None:
            continue

        course_uid = slim["_course_uid"]

        if course_uid != current_course_uid:
            _flush_course(course_buffer)
            course_buffer = [slim]
            current_course_uid = course_uid
        else:
            course_buffer.append(slim)
            if len(course_buffer) > MAX_BUFFER:
                _flush_course(course_buffer)
                course_buffer = []
                current_course_uid = None

        if total_records % 500_000 == 0:
            print(f"  ... {total_records:,} records processed")

    # Flush last group
    _flush_course(course_buffer)

    return {
        "total_records": total_records,
        "skipped_no_cote": skipped_no_cote,
        "course_groups": course_groups_processed,
        "overall": overall.summary(),
        "favorite": favorite_acc.summary(),
        "by_discipline": {k: v.summary() for k, v in sorted(by_discipline.items())},
        "by_hippodrome": {k: v.summary() for k, v in sorted(
            by_hippodrome.items(), key=lambda x: x[1].bets, reverse=True
        )},
        "by_price_range": {k: v.summary() for k, v in sorted(
            by_price_range.items(),
            key=lambda x: PRICE_RANGES.index(
                next((pr for pr in PRICE_RANGES if pr[0] == x[0]), PRICE_RANGES[-1])
            ),
        )},
        "by_year": {k: v.summary() for k, v in sorted(by_year.items())},
        "value_profiles": {
            pr: {
                "count": vt["count"],
                "implied_win_rate_pct": round(
                    (vt["implied_sum"] / vt["count"]) * 100, 2
                ) if vt["count"] > 0 else 0,
                "actual_win_rate_pct": round(
                    (vt["actual_wins"] / vt["count"]) * 100, 2
                ) if vt["count"] > 0 else 0,
                "edge_pct": round(
                    ((vt["actual_wins"] / vt["count"]) - (vt["implied_sum"] / vt["count"])) * 100, 2
                ) if vt["count"] > 0 else 0,
            }
            for pr, vt in value_tracker.items()
        },
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def _fmt_roi_table(data: dict[str, dict], label_col: str = "Category", top_n: int = 0) -> list[str]:
    """Format an ROI table as Markdown rows."""
    lines: list[str] = []
    lines.append(f"| {label_col} | Bets | Wins | Win% | ROI% |")
    lines.append("|" + "---|" * 5)

    items = list(data.items())
    if top_n > 0:
        items = items[:top_n]

    for label, stats in items:
        lines.append(
            f"| {label} | {stats['bets']:,} | {stats['wins']:,} "
            f"| {stats['win_rate']:.1f}% | {stats['roi_pct']:+.1f}% |"
        )
    return lines


def generate_report(results: dict, input_path: Path) -> str:
    """Generate the full Markdown report."""
    lines: list[str] = []
    lines.append("# ROI Analysis Report (Pilier 16 - Rentabilite Turf)")
    lines.append(f"\nGenerated: {_TODAY}")
    lines.append(f"Source: `{input_path.name}`")
    lines.append(f"Total records: {results['total_records']:,}")
    lines.append(f"Skipped (no cote): {results['skipped_no_cote']:,}")
    lines.append(f"Course groups: {results['course_groups']:,}\n")

    # Overall
    lines.append("## Overall ROI (flat 1-unit bet on every runner)\n")
    ov = results["overall"]
    lines.append(f"- **Bets:** {ov['bets']:,}")
    lines.append(f"- **Wins:** {ov['wins']:,}")
    lines.append(f"- **Win rate:** {ov['win_rate']:.1f}%")
    lines.append(f"- **ROI:** {ov['roi_pct']:+.1f}%\n")

    # Favorite
    lines.append("## Favorite ROI (bet on lowest cote_finale per course)\n")
    fav = results["favorite"]
    lines.append(f"- **Bets:** {fav['bets']:,}")
    lines.append(f"- **Wins:** {fav['wins']:,}")
    lines.append(f"- **Win rate:** {fav['win_rate']:.1f}%")
    lines.append(f"- **ROI:** {fav['roi_pct']:+.1f}%\n")

    # By discipline
    lines.append("## ROI by Discipline\n")
    lines.extend(_fmt_roi_table(results["by_discipline"], "Discipline"))
    lines.append("")

    # By hippodrome (top 20)
    lines.append("## ROI by Hippodrome (top 20 by volume)\n")
    lines.extend(_fmt_roi_table(results["by_hippodrome"], "Hippodrome", top_n=20))
    lines.append("")

    # By price range
    lines.append("## ROI by Price Range\n")
    lines.extend(_fmt_roi_table(results["by_price_range"], "Price Range"))
    lines.append("")

    # By year
    lines.append("## ROI by Year\n")
    lines.extend(_fmt_roi_table(results["by_year"], "Year"))
    lines.append("")

    # Value profiles
    lines.append("## Value Profiles (actual win rate vs implied probability)\n")
    lines.append("| Price Range | Count | Implied Win% | Actual Win% | Edge% |")
    lines.append("|" + "---|" * 5)
    for pr, vp in results["value_profiles"].items():
        edge_marker = " **" if vp["edge_pct"] > 0 else ""
        lines.append(
            f"| {pr} | {vp['count']:,} | {vp['implied_win_rate_pct']:.1f}% "
            f"| {vp['actual_win_rate_pct']:.1f}% | {vp['edge_pct']:+.1f}%{edge_marker} |"
        )
    lines.append("")

    # Best value insight
    positive_edges = [
        (pr, vp) for pr, vp in results["value_profiles"].items()
        if vp["edge_pct"] > 0 and vp["count"] >= 100
    ]
    if positive_edges:
        lines.append("### Best Value Segments\n")
        lines.append("Price ranges where actual win rate exceeds implied probability (min 100 bets):\n")
        for pr, vp in sorted(positive_edges, key=lambda x: x[1]["edge_pct"], reverse=True):
            lines.append(
                f"- **{pr}**: +{vp['edge_pct']:.1f}% edge "
                f"(actual {vp['actual_win_rate_pct']:.1f}% vs implied {vp['implied_win_rate_pct']:.1f}%)"
            )
    else:
        lines.append("### Best Value Segments\n")
        lines.append("No price range shows a positive edge with sufficient sample size (>= 100 bets).")

    lines.append("\n---")
    lines.append("RAM budget: < 2 GB (streaming accumulators, no record storage)")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="ROI analysis for turf betting data")
    parser.add_argument(
        "--input", type=Path, default=None,
        help="Input JSONL file (default: training_labels.jsonl or partants_master.jsonl)",
    )
    args = parser.parse_args()

    # Determine input file
    input_path: Path | None = args.input
    if input_path is None:
        if TRAINING_LABELS.exists():
            input_path = TRAINING_LABELS
        elif PARTANTS_MASTER.exists():
            input_path = PARTANTS_MASTER
        else:
            print("[ERROR] No input file found. Provide --input or ensure "
                  "training_labels.jsonl / partants_master.jsonl exist.")
            return 1

    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path}")
        return 1

    print("\n" + "=" * 60)
    print("  ROI ANALYZER — Pilier 16")
    print(f"  {_TODAY}")
    print(f"  Input: {input_path}")
    print("=" * 60)

    t0 = time.monotonic()
    results = _find_favorites_and_analyze(input_path)
    elapsed = time.monotonic() - t0

    # Generate report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report(results, input_path)
    REPORT_PATH.write_text(report, encoding="utf-8")

    # Console summary
    ov = results["overall"]
    fav = results["favorite"]
    print(f"\n  Records analyzed: {results['total_records']:,}")
    print(f"  Overall ROI:     {ov['roi_pct']:+.1f}% ({ov['bets']:,} bets)")
    print(f"  Favorite ROI:    {fav['roi_pct']:+.1f}% ({fav['bets']:,} bets)")
    print(f"  Disciplines:     {len(results['by_discipline'])}")
    print(f"  Hippodromes:     {len(results['by_hippodrome'])}")
    print(f"  Duration:        {elapsed:.1f}s")
    print(f"\n  Report: {REPORT_PATH}")
    print("=" * 60 + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
