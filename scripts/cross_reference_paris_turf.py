#!/usr/bin/env python3
"""Cross-reference Paris-Turf runners into partants_master.

Matches by (date, nom_cheval) and reports new fields that Paris-Turf provides
but are missing/empty in the master.
"""

import json
import sys
import time
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent

PT_RUNNERS = ROOT / "output" / "53_paris_turf" / "paris_turf_runners.jsonl"
MASTER = ROOT / "data_master" / "partants_master.jsonl"
REPORT_OUT = ROOT / "output" / "cross_reference" / "paris_turf_crossref_report.json"

# Fields unique to Paris-Turf that could enrich the master
PT_ENRICHMENT_FIELDS = [
    "formFigs",
    "handicapRatingKg",
    "records",
    "shoeing",
    "shoeingFront",
    "shoeingBack",
    "blinkersFirstTime",
    "noShoesFirstTime",
    "protectionFirstTime",
    "hood",
    "tongueTie",
    "margin",
    "daysSincePreviousRace",
    "previousRaceDate",
    "previousRaceId",
    "emoji",
    "raceAutostart",
    "raceDirection",
    "raceSurface",
]


def normalize_name(name: str) -> str:
    """Normalize horse name for matching."""
    if not name:
        return ""
    return name.strip().upper().replace("-", " ").replace("'", "")


def load_paris_turf():
    """Load Paris-Turf runners indexed by (date, normalized_name)."""
    pt_index = {}
    with open(PT_RUNNERS, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            date = rec.get("date", rec.get("raceDate", ""))
            name = normalize_name(rec.get("horseName", ""))
            if date and name:
                pt_index[(date, name)] = rec
    return pt_index


def scan_master_for_matches(pt_index: dict):
    """Scan master JSONL, find matches with PT data, report enrichments."""
    matches = 0
    total_master = 0
    enrichments = defaultdict(int)
    sample_enrichments = {}  # field -> sample value

    print(f"Scanning master for matches with {len(pt_index)} PT runners...")
    start = time.time()

    with open(MASTER, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            if line_no % 500_000 == 0:
                elapsed = time.time() - start
                print(f"  Scanned {line_no:,} master rows ({elapsed:.0f}s, {matches} matches)")

            line = line.strip()
            if not line:
                continue
            total_master += 1

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            date = rec.get("date_reunion_iso", "")
            name = normalize_name(rec.get("nom_cheval", ""))
            if not date or not name:
                continue

            pt_rec = pt_index.get((date, name))
            if pt_rec is None:
                continue

            matches += 1

            # Check which PT fields would add new info
            for field in PT_ENRICHMENT_FIELDS:
                pt_val = pt_rec.get(field)
                if pt_val is not None and pt_val != "" and pt_val != []:
                    # Check if master already has this info
                    master_val = rec.get(field)
                    if master_val is None or master_val == "" or master_val == []:
                        enrichments[field] += 1
                        if field not in sample_enrichments:
                            sample_enrichments[field] = {
                                "horse": rec.get("nom_cheval", ""),
                                "date": date,
                                "pt_value": str(pt_val)[:200],
                            }

    elapsed = time.time() - start
    return {
        "total_master_rows": total_master,
        "total_pt_runners": len(pt_index),
        "matches_found": matches,
        "match_rate": f"{matches / len(pt_index) * 100:.1f}%" if pt_index else "0%",
        "scan_time_seconds": round(elapsed, 1),
        "enrichment_fields": dict(enrichments),
        "sample_enrichments": sample_enrichments,
    }


def main():
    if not PT_RUNNERS.exists():
        sys.exit(f"Paris-Turf runners not found: {PT_RUNNERS}")
    if not MASTER.exists():
        sys.exit(f"Master not found: {MASTER}")

    print("=" * 60)
    print("Paris-Turf Cross-Reference Report")
    print("=" * 60)

    # Load PT data (small: 3775 records)
    print(f"\nLoading Paris-Turf runners from {PT_RUNNERS.name}...")
    pt_index = load_paris_turf()
    print(f"  Loaded {len(pt_index)} runners with unique (date, name) keys")

    # Scan master (large: ~26GB)
    print()
    report = scan_master_for_matches(pt_index)

    # Save report
    REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_OUT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Results:")
    print(f"  Master rows scanned:  {report['total_master_rows']:,}")
    print(f"  PT runners:           {report['total_pt_runners']:,}")
    print(f"  Matches found:        {report['matches_found']:,}")
    print(f"  Match rate:           {report['match_rate']}")
    print(f"  Scan time:            {report['scan_time_seconds']}s")
    print(f"\nEnrichment fields (new data from PT):")
    for field, count in sorted(report["enrichment_fields"].items(), key=lambda x: -x[1]):
        print(f"  {field}: {count} records could be enriched")
    print(f"\nReport saved to: {REPORT_OUT}")


if __name__ == "__main__":
    main()
