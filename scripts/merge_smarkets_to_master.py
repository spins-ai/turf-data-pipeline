#!/usr/bin/env python3
"""
scripts/merge_smarkets_to_master.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merge Smarkets exchange odds from smarkets_exchange.jsonl into the
partants master (partants_normalises.jsonl).

Smarkets records carry per-runner exchange market data:
  - event_date, track, runner_name, runner_slug
  - best_back_bp, best_lay_bp (basis points: divide by 100 for decimal odds)
  - best_back_odds, best_lay_odds (decimal odds)
  - best_back_qty, best_lay_qty, market_volume
  - last_executed_bp, last_executed_odds

Merge strategy:
  - Build a lookup keyed by (date, nom_cheval_normalise) from smarkets.
  - The track field (e.g. "auteuil-fra") is used for disambiguation when
    multiple runners share the same name on the same day.
  - For each master partant, match on (date_reunion_iso, nom_cheval).

Fields added to each matching partant:
  - smarkets_back_odds    : best back (buy) decimal odds
  - smarkets_lay_odds     : best lay (sell) decimal odds
  - smarkets_back_qty     : quantity available at best back
  - smarkets_lay_qty      : quantity available at best lay
  - smarkets_last_odds    : last executed decimal odds
  - smarkets_volume       : market volume
  - smarkets_track        : Smarkets track identifier
"""

from __future__ import annotations

import json
import os
import re
import sys
import time

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SMARKETS_PATH = os.path.join(BASE, "output", "30_smarkets_exchange", "smarkets_exchange.jsonl")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_smarkets.jsonl")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_name(name: str) -> str:
    """Normalise horse name: uppercase, strip non-alpha, collapse spaces."""
    if not name:
        return ""
    cleaned = name.strip().upper()
    cleaned = re.sub(r"[^A-Z ]", "", cleaned).strip()
    return re.sub(r"\s+", " ", cleaned)


def _normalise_slug(slug: str) -> str:
    """Convert runner_slug like 'in-between-days' to normalised name."""
    if not slug:
        return ""
    return _normalise_name(slug.replace("-", " "))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---
    for label, path in [("Smarkets", SMARKETS_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: build lookup from Smarkets --------------------------------
    print(f"[1/2] Streaming Smarkets data from {SMARKETS_PATH} ...")

    # Key: "date|horse_name_norm" -> smarkets info
    runner_lookup: dict[str, dict] = {}
    total_sm = 0
    indexed = 0

    with open(SMARKETS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_sm += 1

            # Extract date from event_date (ISO datetime)
            event_date = str(rec.get("event_date", ""))[:10]
            runner_name = rec.get("runner_name", "")
            runner_slug = rec.get("runner_slug", "")

            # Try name first, fallback to slug
            horse_norm = _normalise_name(runner_name)
            if not horse_norm:
                horse_norm = _normalise_slug(runner_slug)

            if not event_date or not horse_norm:
                continue

            info = {
                "smarkets_back_odds": rec.get("best_back_odds"),
                "smarkets_lay_odds": rec.get("best_lay_odds"),
                "smarkets_back_qty": rec.get("best_back_qty"),
                "smarkets_lay_qty": rec.get("best_lay_qty"),
                "smarkets_last_odds": rec.get("last_executed_odds"),
                "smarkets_volume": rec.get("market_volume"),
                "smarkets_track": rec.get("track", ""),
            }

            key = f"{event_date}|{horse_norm}"
            if key not in runner_lookup:
                runner_lookup[key] = info
                indexed += 1

    print(f"       {total_sm:,} smarkets records, {indexed:,} indexed by date+name")

    # --- Phase 2: stream master, enrich, write out -------------------------
    print(f"[2/2] Streaming master -> enriched output ...")

    total = 0
    matched = 0

    os.makedirs(os.path.dirname(MASTER_OUT), exist_ok=True)

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

            date_iso = str(rec.get("date_reunion_iso", ""))[:10]
            nom_cheval = _normalise_name(rec.get("nom_cheval", ""))

            if date_iso and nom_cheval:
                key = f"{date_iso}|{nom_cheval}"
                hit = runner_lookup.get(key)
                if hit:
                    for fld, val in hit.items():
                        if val is not None and val != "":
                            rec[fld] = val
                    matched += 1

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0
    pct = (matched / total * 100) if total else 0
    print(f"Done in {elapsed:.1f}s. {total:,} partants, {matched:,} enriched ({pct:.1f}%).")
    print(f"Output: {MASTER_OUT}")


if __name__ == "__main__":
    main()
