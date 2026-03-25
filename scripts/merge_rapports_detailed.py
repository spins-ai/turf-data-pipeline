#!/usr/bin/env python3
"""
scripts/merge_rapports_detailed.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge course-level rapport statistics from the cache goldmine audit
(rapports_detail_complet.jsonl, 3.6M rows) into partants_master.jsonl.

Source: output/21_rapports_definitifs/rapports_detail_complet.jsonl
  Keys: date, num_reunion, num_course, type_pari, famille_pari,
        dividende_pour_un_euro, nombre_gagnants

Strategy:
  Phase 1 - Stream rapports, aggregate per course:
    - nb_gagnants_simple: nombre_gagnants for SIMPLE_GAGNANT
    - dividend_moyen: mean dividende_pour_un_euro across all Simple bets
    - market_concentration: Herfindahl-like index from dividende distribution
      (higher = more concentrated / fewer competitive horses)

  Phase 2 - Stream partants_master, enrich matching rows by
    (date_reunion_iso, numero_reunion, numero_course), write in-place.

Fields added per partant:
  - rap_nb_gagnants_simple  : number of bettors who won on Simple Gagnant
  - rap_dividend_moyen      : average payout per euro across Simple bets
  - rap_market_concentration: Herfindahl index from payout distribution
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAPPORTS_PATH = os.path.join(BASE, "output", "21_rapports_definitifs", "rapports_detail_complet.jsonl")
MASTER_IN = os.path.join(BASE, "data_master", "partants_master.jsonl")
MASTER_OUT = os.path.join(BASE, "data_master", "partants_master.jsonl.tmp")

PROGRESS_EVERY = 1_000_000


def make_course_key(date: str, num_r: int, num_c: int) -> str:
    return f"{date}|{num_r}|{num_c}"


def herfindahl(values: list[float]) -> float | None:
    """Herfindahl index from dividend values (proxy for market concentration).

    Normalise each dividend as a share of total, then sum of squares.
    Range: 1/N (perfectly even) to 1.0 (one payout dominates).
    """
    if not values:
        return None
    total = sum(values)
    if total <= 0:
        return None
    shares = [v / total for v in values]
    return round(sum(s * s for s in shares), 4)


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---------------------------------------------------
    for label, path in [("Rapports", RAPPORTS_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: aggregate rapports per course ----------------------------
    print(f"[1/2] Streaming rapports from {os.path.basename(RAPPORTS_PATH)} ...")

    # course_key -> {nb_gagnants_simple, dividends_simple: list, all_dividends: list}
    course_stats: dict[str, dict] = {}

    total_rap = 0
    skipped = 0

    with open(RAPPORTS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            total_rap += 1

            date = rec.get("date", "")
            num_r = rec.get("num_reunion")
            num_c = rec.get("num_course")

            if not date or num_r is None or num_c is None:
                skipped += 1
                continue

            ck = make_course_key(date, int(num_r), int(num_c))

            if ck not in course_stats:
                course_stats[ck] = {
                    "nb_gagnants_simple": None,
                    "dividends_simple": [],
                    "all_dividends": [],
                }

            cs = course_stats[ck]
            type_pari = rec.get("type_pari", "")
            famille = rec.get("famille_pari", "")
            div_euro = rec.get("dividende_pour_un_euro")
            nb_gagnants = rec.get("nombre_gagnants")

            # Track SIMPLE_GAGNANT specifically
            if type_pari == "SIMPLE_GAGNANT" and nb_gagnants is not None:
                cs["nb_gagnants_simple"] = nb_gagnants

            # Collect Simple family dividends for average
            if famille == "Simple" and div_euro is not None:
                try:
                    cs["dividends_simple"].append(float(div_euro))
                except (ValueError, TypeError):
                    pass

            # Collect all dividends for Herfindahl
            if div_euro is not None:
                try:
                    cs["all_dividends"].append(float(div_euro))
                except (ValueError, TypeError):
                    pass

            if total_rap % PROGRESS_EVERY == 0:
                print(f"       ... {total_rap / 1e6:.1f}M rapport lines read")

    print(f"       {total_rap:,} rapports read, {skipped:,} skipped")
    print(f"       {len(course_stats):,} courses with rapport data")

    # Pre-compute final aggregates to free memory
    course_agg: dict[str, dict] = {}
    for ck, cs in course_stats.items():
        divs = cs["dividends_simple"]
        avg_div = round(sum(divs) / len(divs), 2) if divs else None

        herf = herfindahl(cs["all_dividends"])

        course_agg[ck] = {
            "rap_nb_gagnants_simple": cs["nb_gagnants_simple"],
            "rap_dividend_moyen": avg_div,
            "rap_market_concentration": herf,
        }

    del course_stats  # free ~1GB

    # --- Phase 2: stream master, enrich, write tmp -------------------------
    print(f"[2/2] Streaming master -> enriched output ...")

    total = 0
    matched = 0

    with open(MASTER_IN, "r", encoding="utf-8") as fin, \
         open(MASTER_OUT, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                fout.write(line + "\n")
                total += 1
                continue

            total += 1

            date = rec.get("date_reunion_iso", "")
            num_r = rec.get("numero_reunion")
            num_c = rec.get("numero_course")

            if date and num_r is not None and num_c is not None:
                ck = make_course_key(date, int(num_r), int(num_c))
                hit = course_agg.get(ck)
                if hit:
                    # Only add non-None values
                    for k, v in hit.items():
                        if v is not None:
                            rec[k] = v
                    matched += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

            if total % 500_000 == 0:
                print(f"       ... {total / 1e6:.1f}M master lines processed")

    # --- Atomic replace ----------------------------------------------------
    os.replace(MASTER_OUT, MASTER_IN)

    elapsed = time.time() - t0
    pct = (matched / total * 100) if total else 0
    print(f"\nDone in {elapsed:.1f}s. {total:,} partants processed.")
    print(f"  Courses matched: {matched:,} ({pct:.1f}%)")
    print(f"Output: {MASTER_IN} (in-place)")


if __name__ == "__main__":
    main()
