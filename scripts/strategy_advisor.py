#!/usr/bin/env python3
"""
strategy_advisor.py — Pilier 6 : Strategie

Analyze partants_master data to suggest data-driven betting strategies:
  - Which hippodromes have the highest favorite win rate
  - Which disciplines are most predictable
  - Optimal bet types based on field size

This is pure analysis (no ML), streaming line-by-line to stay under 2 GB RAM.

Outputs quality/strategy_insights_report.md

Usage:
    python scripts/strategy_advisor.py
"""

from __future__ import annotations

import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PARTANTS_MASTER, QUALITY_DIR  # noqa: E402

OUTPUT_REPORT = QUALITY_DIR / "strategy_insights_report.md"


# ---------------------------------------------------------------------------
# Streaming accumulators — fixed-size counters, no unbounded dicts
# ---------------------------------------------------------------------------

class StrategyAccumulator:
    """Collects aggregate statistics in a single pass over partants_master."""

    def __init__(self) -> None:
        self.total = 0

        # Hippodrome favorite win rate
        # favorite = lowest cote_finale among partants in a course
        # We track per-course in a bounded buffer then flush per-hippodrome.
        self.hippo_fav_wins: Counter = Counter()      # hippo -> fav won count
        self.hippo_fav_total: Counter = Counter()      # hippo -> courses with fav
        self.hippo_total_courses: Counter = Counter()  # hippo -> total courses

        # Discipline predictability (top-1 win rate by position_arrivee == 1)
        self.disc_winners: Counter = Counter()
        self.disc_total: Counter = Counter()
        self.disc_fav_wins: Counter = Counter()
        self.disc_fav_total: Counter = Counter()

        # Field size analysis
        self.field_bins: dict[str, Counter] = {
            "winners": Counter(),   # field_bin -> count of winners
            "total": Counter(),     # field_bin -> total partants
        }

        # Per-course buffer (we process course-by-course)
        self._current_course_uid: str | None = None
        self._course_buffer: list[dict] = []

        # Bet type analysis by field size
        self.field_size_place_rate: dict[str, Counter] = {
            "place_wins": Counter(),  # bin -> placed count
            "place_total": Counter(),
        }

        # Year trends
        self.year_winners: Counter = Counter()
        self.year_total: Counter = Counter()
        self.year_fav_wins: Counter = Counter()
        self.year_fav_total: Counter = Counter()

    def _field_bin(self, n: int) -> str:
        if n <= 6:
            return "<=6"
        elif n <= 10:
            return "7-10"
        elif n <= 14:
            return "11-14"
        elif n <= 18:
            return "15-18"
        else:
            return "19+"

    def _flush_course(self) -> None:
        """Process accumulated records for one course."""
        if not self._course_buffer:
            return

        buf = self._course_buffer
        first = buf[0]
        hippo = first.get("hippodrome_normalise", "UNKNOWN")
        disc = first.get("discipline", "UNKNOWN")
        date_str = first.get("date_reunion_iso", "")
        year = date_str[:4] if date_str else "?"
        nb_partants = first.get("nombre_partants") or len(buf)

        field_bin = self._field_bin(nb_partants if isinstance(nb_partants, int) else len(buf))

        self.hippo_total_courses[hippo] += 1

        # Find favorite (lowest cote_finale > 0)
        favorite = None
        for r in buf:
            cote = r.get("cote_finale")
            if cote is not None and isinstance(cote, (int, float)) and cote > 0:
                if favorite is None or cote < favorite.get("cote_finale", 999):
                    favorite = r

        if favorite is not None:
            self.hippo_fav_total[hippo] += 1
            self.disc_fav_total[disc] += 1
            self.year_fav_total[year] += 1
            fav_won = _is_winner(favorite)
            if fav_won:
                self.hippo_fav_wins[hippo] += 1
                self.disc_fav_wins[disc] += 1
                self.year_fav_wins[year] += 1

        # Per-record stats
        for r in buf:
            self.total += 1
            self.disc_total[disc] += 1
            self.field_bins["total"][field_bin] += 1
            self.field_size_place_rate["place_total"][field_bin] += 1
            self.year_total[year] += 1

            if _is_winner(r):
                self.disc_winners[disc] += 1
                self.field_bins["winners"][field_bin] += 1
                self.year_winners[year] += 1

            if _is_placed(r):
                self.field_size_place_rate["place_wins"][field_bin] += 1

        self._course_buffer = []

    def feed(self, rec: dict) -> None:
        """Feed one record. Records must arrive sorted by course_uid (they are)."""
        course_uid = rec.get("course_uid", "")
        if course_uid != self._current_course_uid:
            self._flush_course()
            self._current_course_uid = course_uid
        self._course_buffer.append(rec)

    def finalize(self) -> None:
        self._flush_course()


def _is_winner(rec: dict) -> bool:
    w = rec.get("is_gagnant")
    if w is True or w == 1 or w == "1":
        return True
    pos = rec.get("position_arrivee")
    return pos == 1 or pos == "1"


def _is_placed(rec: dict) -> bool:
    p = rec.get("is_place")
    if p is True or p == 1 or p == "1":
        return True
    pos = rec.get("position_arrivee")
    try:
        return int(pos) <= 3
    except (TypeError, ValueError):
        return False


def stream_and_accumulate(filepath: Path) -> StrategyAccumulator:
    """Single-pass streaming over partants_master.jsonl."""
    acc = StrategyAccumulator()
    with open(filepath, "r", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            acc.feed(rec)
            if (i + 1) % 500_000 == 0:
                print(f"  ... {i + 1:,} lines processed")
    acc.finalize()
    return acc


def write_report(acc: StrategyAccumulator, elapsed: float) -> None:
    """Write the strategy insights report."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: list[str] = []
    lines.append("# Strategy Insights Report (Pilier 6 — Strategie)")
    lines.append("")
    lines.append(f"Generated: {now}")
    lines.append(f"Total partants analyzed: {acc.total:,}")
    lines.append(f"Elapsed: {elapsed:.1f}s")
    lines.append("")

    # -----------------------------------------------------------------------
    # 1. Hippodrome favorite win rate (top 30 by volume)
    # -----------------------------------------------------------------------
    lines.append("## 1. Hippodrome Favorite Win Rate")
    lines.append("")
    lines.append("Hippodromes with the highest favorite win rate (min 100 courses).")
    lines.append("")
    lines.append("| Hippodrome | Courses | Fav Win Rate | Fav Wins |")
    lines.append("|------------|---------|-------------|----------|")

    hippo_rates: list[tuple[str, float, int, int]] = []
    for hippo in acc.hippo_fav_total:
        total = acc.hippo_fav_total[hippo]
        if total < 100:
            continue
        wins = acc.hippo_fav_wins.get(hippo, 0)
        rate = wins / total * 100
        hippo_rates.append((hippo, rate, wins, total))

    hippo_rates.sort(key=lambda x: -x[1])
    for hippo, rate, wins, total in hippo_rates[:30]:
        lines.append(f"| {hippo} | {total:,} | {rate:.1f}% | {wins:,} |")
    lines.append("")

    # Bottom 10
    lines.append("### Least predictable hippodromes (lowest fav win rate)")
    lines.append("")
    lines.append("| Hippodrome | Courses | Fav Win Rate |")
    lines.append("|------------|---------|-------------|")
    for hippo, rate, wins, total in hippo_rates[-10:]:
        lines.append(f"| {hippo} | {total:,} | {rate:.1f}% |")
    lines.append("")

    # -----------------------------------------------------------------------
    # 2. Discipline predictability
    # -----------------------------------------------------------------------
    lines.append("## 2. Discipline Predictability")
    lines.append("")
    lines.append("| Discipline | Partants | Winners | Win Rate | Fav Win Rate |")
    lines.append("|------------|----------|---------|----------|-------------|")

    for disc in sorted(acc.disc_total, key=lambda d: -acc.disc_total[d]):
        tot = acc.disc_total[disc]
        wins = acc.disc_winners.get(disc, 0)
        fav_t = acc.disc_fav_total.get(disc, 0)
        fav_w = acc.disc_fav_wins.get(disc, 0)
        wr = wins / tot * 100 if tot else 0
        fwr = fav_w / fav_t * 100 if fav_t else 0
        lines.append(f"| {disc} | {tot:,} | {wins:,} | {wr:.1f}% | {fwr:.1f}% |")
    lines.append("")

    # -----------------------------------------------------------------------
    # 3. Optimal bet types by field size
    # -----------------------------------------------------------------------
    lines.append("## 3. Field Size Analysis — Optimal Bet Types")
    lines.append("")
    lines.append("| Field Size | Partants | Win Rate | Place Rate (top 3) | Suggested Bet |")
    lines.append("|------------|----------|----------|--------------------|---------------|")

    for fbin in ["<=6", "7-10", "11-14", "15-18", "19+"]:
        tot = acc.field_bins["total"].get(fbin, 0)
        wins = acc.field_bins["winners"].get(fbin, 0)
        place_w = acc.field_size_place_rate["place_wins"].get(fbin, 0)
        place_t = acc.field_size_place_rate["place_total"].get(fbin, 0)
        wr = wins / tot * 100 if tot else 0
        pr = place_w / place_t * 100 if place_t else 0

        if tot == 0:
            suggestion = "N/A"
        elif fbin == "<=6":
            suggestion = "Simple Gagnant (small field, favorites reliable)"
        elif fbin in ("7-10",):
            suggestion = "Simple Place / Couple (balanced field)"
        elif fbin in ("11-14",):
            suggestion = "Couple / Tierce (larger field, place bets safer)"
        else:
            suggestion = "Multi / 2sur4 (big field, value in exotic bets)"

        lines.append(f"| {fbin} | {tot:,} | {wr:.1f}% | {pr:.1f}% | {suggestion} |")
    lines.append("")

    # -----------------------------------------------------------------------
    # 4. Year-over-year trends
    # -----------------------------------------------------------------------
    lines.append("## 4. Year-over-Year Favorite Win Rate")
    lines.append("")
    lines.append("| Year | Partants | Fav Win Rate |")
    lines.append("|------|----------|-------------|")
    for year in sorted(acc.year_fav_total.keys()):
        if year == "?":
            continue
        fav_t = acc.year_fav_total[year]
        fav_w = acc.year_fav_wins.get(year, 0)
        fwr = fav_w / fav_t * 100 if fav_t else 0
        tot = acc.year_total.get(year, 0)
        lines.append(f"| {year} | {tot:,} | {fwr:.1f}% |")
    lines.append("")

    # -----------------------------------------------------------------------
    # 5. Key takeaways
    # -----------------------------------------------------------------------
    lines.append("## 5. Key Takeaways")
    lines.append("")

    if hippo_rates:
        best_hippo = hippo_rates[0]
        lines.append(f"- **Most predictable hippodrome**: {best_hippo[0]} "
                      f"(fav wins {best_hippo[1]:.1f}% of the time, {best_hippo[3]:,} courses)")

    # Best discipline by fav win rate
    best_disc = None
    best_disc_rate = 0
    for disc in acc.disc_fav_total:
        fav_t = acc.disc_fav_total[disc]
        if fav_t < 100:
            continue
        fav_w = acc.disc_fav_wins.get(disc, 0)
        rate = fav_w / fav_t * 100
        if rate > best_disc_rate:
            best_disc_rate = rate
            best_disc = disc
    if best_disc:
        lines.append(f"- **Most predictable discipline**: {best_disc} "
                      f"({best_disc_rate:.1f}% favorite win rate)")

    lines.append("- **Small fields (<=6)**: favor Simple Gagnant on the favorite")
    lines.append("- **Large fields (15+)**: exotic bets (Multi, 2sur4) offer better value")
    lines.append("")

    OUTPUT_REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written to {OUTPUT_REPORT}")


def main() -> None:
    print("=" * 60)
    print("Pilier 6 — Strategy Advisor")
    print("=" * 60)
    t0 = time.time()

    if not PARTANTS_MASTER.exists():
        print(f"ERROR: {PARTANTS_MASTER} not found.")
        sys.exit(1)

    print(f"\nStreaming {PARTANTS_MASTER} ...")
    acc = stream_and_accumulate(PARTANTS_MASTER)

    elapsed = time.time() - t0
    write_report(acc, elapsed)
    print(f"\nDone in {elapsed:.1f}s — {acc.total:,} records analyzed.")


if __name__ == "__main__":
    main()
