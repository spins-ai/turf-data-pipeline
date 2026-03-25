#!/usr/bin/env python3
"""
cross_reference_enriched.py
============================
Cross-reference PMU enriched data and Le Trot data with partants_master
to fill missing fields:

    deferre, tempsObtenu/temps_ms, reductionKilometrique/reduction_km_ms,
    record_personnel, crack_series, handicapValeur, handicapPoids,
    poidsConditionMonte, avisEntraineur, commentaireApresCourse, oeilleres

Strategy:
    1. Build lookup indexes from PMU enriched + Le Trot enriched data
    2. Stream partants_master.jsonl
    3. For each record with missing fields, look up enriched values
    4. Write updated records to partants_master_crossref.jsonl
    5. Report fill rate gains

Usage:
    python scripts/cross_reference_enriched.py
    python scripts/cross_reference_enriched.py --dry-run         # report only, no write
    python scripts/cross_reference_enriched.py --output out.jsonl # custom output path
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

try:
    from utils.logging_setup import setup_logging
    log = setup_logging("cross_reference_enriched")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("cross_reference_enriched")

# Input files
PMU_ENRICHED = REPO_ROOT / "output" / "101_pmu_api" / "pmu_participants_enriched.jsonl"
LETROT_DATA  = REPO_ROOT / "output" / "02b_scraper_letrot" / "letrot_data.jsonl"
PARTANTS_MASTER = REPO_ROOT / "data_master" / "partants_master.jsonl"

# Default output
DEFAULT_OUTPUT = REPO_ROOT / "data_master" / "partants_master_crossref.jsonl"

# ---------------------------------------------------------------------------
# Field mapping: PMU enriched field name -> partants_master field name
# ---------------------------------------------------------------------------
PMU_FIELD_MAP = {
    "deferre":                 "deferre",
    "tempsObtenu":             "temps_ms",        # PMU returns ms
    "reductionKilometrique":   "reduction_km_ms", # PMU returns ms
    "handicapValeur":          "handicap_valeur",
    "handicapPoids":           "poids_base_kg",
    "poidsConditionMonte":     "poids_porte_kg",
    "avisEntraineur":          "avis_entraineur",
    "commentaireApresCourse":  "commentaire_apres_course",
    "oeilleres":               "oeilleres",
}

# Le Trot fields that can fill partants_master directly (same field names)
LETROT_FILL_FIELDS = [
    "deferre",
    "temps_ms",
    "reduction_km_ms",
    "handicap_valeur",
    "poids_base_kg",
    "poids_porte_kg",
    "avis_entraineur",
    "commentaire_apres_course",
    "oeilleres",
    "ecart_precedent",
    "surcharge_decharge_kg",
]


def is_empty(value) -> bool:
    """Check if a value is considered empty/missing."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (int, float)) and value == 0:
        # 0 can be valid for some fields but we treat it carefully
        return False
    return False


# ===================================================================
# Phase 1: Build lookup indexes from enriched sources
# ===================================================================

def build_pmu_enriched_index(path: Path) -> dict:
    """
    Build index from pmu_participants_enriched.jsonl.
    Key: (date, numReunion, numCourse, numPmu) -> dict of enriched fields
    """
    index: dict[tuple, dict] = {}
    if not path.exists():
        log.warning("PMU enriched file not found: %s", path)
        return index

    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            key = (
                rec.get("date"),
                rec.get("numReunion"),
                rec.get("numCourse"),
                rec.get("numPmu"),
            )
            if None in key:
                continue

            # Extract non-empty enriched fields
            enriched = {}
            for pmu_field, master_field in PMU_FIELD_MAP.items():
                val = rec.get(pmu_field)
                if not is_empty(val):
                    enriched[master_field] = val

            if enriched:
                index[key] = enriched
                count += 1

    log.info("PMU enriched index: %d records with enriched fields", count)
    return index


def build_letrot_index(path: Path) -> dict:
    """
    Build index from letrot_data.jsonl (partant records only).
    Key: partant_uid -> dict of fillable fields
    Also builds secondary key: (date, numero_reunion, numero_course, num_pmu)
    """
    index_uid: dict[str, dict] = {}
    index_key: dict[tuple, dict] = {}
    if not path.exists():
        log.warning("Le Trot data file not found: %s", path)
        return index_uid, index_key

    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Only partant records
            if rec.get("_type") != "partant":
                continue

            fillable = {}
            for field in LETROT_FILL_FIELDS:
                val = rec.get(field)
                if not is_empty(val):
                    fillable[field] = val

            if not fillable:
                continue

            # Primary key: partant_uid
            uid = rec.get("partant_uid")
            if uid:
                index_uid[uid] = fillable

            # Secondary key: (date, reunion, course, numPmu)
            sec_key = (
                rec.get("date_reunion_iso"),
                rec.get("numero_reunion"),
                rec.get("numero_course"),
                rec.get("num_pmu"),
            )
            if None not in sec_key:
                index_key[sec_key] = fillable

            count += 1

    log.info("Le Trot index: %d partant records (%d by uid, %d by key)",
             count, len(index_uid), len(index_key))
    return index_uid, index_key


# ===================================================================
# Phase 2: Stream partants_master and fill missing fields
# ===================================================================

def cross_reference(
    pmu_index: dict,
    letrot_uid_index: dict,
    letrot_key_index: dict,
    master_path: Path,
    output_path: Path | None,
    dry_run: bool = False,
) -> dict:
    """
    Stream partants_master, fill missing fields from enriched sources.
    Returns fill rate statistics.
    """
    # Track fill rates: field -> {before: count_filled, after: count_filled, total: count}
    stats: dict[str, dict] = defaultdict(lambda: {"before": 0, "after": 0, "total": 0, "filled_pmu": 0, "filled_letrot": 0})

    ALL_FIELDS = list(set(PMU_FIELD_MAP.values()) | set(LETROT_FILL_FIELDS))

    total_records = 0
    records_updated = 0
    outf = None

    if not dry_run and output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        outf = open(output_path, "w", encoding="utf-8")

    t0 = time.time()

    with open(master_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                if outf:
                    outf.write(line + "\n")
                continue

            total_records += 1
            updated = False

            # Count 'before' fill rates
            for field in ALL_FIELDS:
                stats[field]["total"] += 1
                if not is_empty(rec.get(field)):
                    stats[field]["before"] += 1

            # --- Try PMU enriched lookup ---
            pmu_key = (
                rec.get("date_reunion_iso"),
                rec.get("numero_reunion"),
                rec.get("numero_course"),
                rec.get("num_pmu"),
            )
            pmu_enriched = pmu_index.get(pmu_key, {})

            for field, val in pmu_enriched.items():
                if is_empty(rec.get(field)):
                    rec[field] = val
                    stats[field]["filled_pmu"] += 1
                    updated = True

            # --- Try Le Trot lookup (by uid first, then by key) ---
            letrot_enriched = {}
            uid = rec.get("partant_uid")
            if uid and uid in letrot_uid_index:
                letrot_enriched = letrot_uid_index[uid]
            else:
                lt_key = (
                    rec.get("date_reunion_iso"),
                    rec.get("numero_reunion"),
                    rec.get("numero_course"),
                    rec.get("num_pmu"),
                )
                letrot_enriched = letrot_key_index.get(lt_key, {})

            for field, val in letrot_enriched.items():
                if is_empty(rec.get(field)):
                    rec[field] = val
                    stats[field]["filled_letrot"] += 1
                    updated = True

            # Count 'after' fill rates
            for field in ALL_FIELDS:
                if not is_empty(rec.get(field)):
                    stats[field]["after"] += 1

            if updated:
                records_updated += 1

            if outf:
                outf.write(json.dumps(rec, ensure_ascii=False) + "\n")

            if total_records % 500_000 == 0:
                elapsed = time.time() - t0
                log.info("  ... %d records processed (%.0fs), %d updated so far",
                         total_records, elapsed, records_updated)

    if outf:
        outf.close()

    elapsed = time.time() - t0
    log.info("Cross-reference complete: %d records, %d updated (%.1fs)",
             total_records, records_updated, elapsed)

    return {
        "total_records": total_records,
        "records_updated": records_updated,
        "elapsed_seconds": round(elapsed, 1),
        "field_stats": {
            field: {
                "total": s["total"],
                "before": s["before"],
                "after": s["after"],
                "fill_rate_before_pct": round(100.0 * s["before"] / max(s["total"], 1), 2),
                "fill_rate_after_pct": round(100.0 * s["after"] / max(s["total"], 1), 2),
                "gain_pct": round(100.0 * (s["after"] - s["before"]) / max(s["total"], 1), 2),
                "filled_from_pmu": s["filled_pmu"],
                "filled_from_letrot": s["filled_letrot"],
            }
            for field, s in sorted(stats.items())
        },
    }


def print_report(result: dict):
    """Print a human-readable fill rate report."""
    print("\n" + "=" * 80)
    print("CROSS-REFERENCE FILL RATE REPORT")
    print("=" * 80)
    print(f"Total records:   {result['total_records']:,}")
    print(f"Records updated: {result['records_updated']:,}")
    print(f"Elapsed:         {result['elapsed_seconds']}s")
    print()
    print(f"{'Field':<30} {'Before':>8} {'After':>8} {'Gain':>8} {'PMU':>8} {'LeTrot':>8}")
    print("-" * 80)

    for field, fs in result["field_stats"].items():
        print(
            f"{field:<30} "
            f"{fs['fill_rate_before_pct']:>7.1f}% "
            f"{fs['fill_rate_after_pct']:>7.1f}% "
            f"{'+' if fs['gain_pct'] >= 0 else ''}{fs['gain_pct']:>6.1f}% "
            f"{fs['filled_from_pmu']:>8,} "
            f"{fs['filled_from_letrot']:>8,}"
        )

    print("=" * 80)


# ===================================================================
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(description="Cross-reference enriched data with partants_master")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report fill rate gains without writing output")
    parser.add_argument("--output", type=str, default=None,
                        help="Output file path (default: data_master/partants_master_crossref.jsonl)")
    parser.add_argument("--pmu-enriched", type=str, default=None,
                        help="Path to PMU enriched JSONL")
    parser.add_argument("--letrot-data", type=str, default=None,
                        help="Path to Le Trot data JSONL")
    parser.add_argument("--partants-master", type=str, default=None,
                        help="Path to partants_master JSONL")
    parser.add_argument("--report-json", type=str, default=None,
                        help="Save JSON report to this path")
    args = parser.parse_args()

    pmu_path = Path(args.pmu_enriched) if args.pmu_enriched else PMU_ENRICHED
    letrot_path = Path(args.letrot_data) if args.letrot_data else LETROT_DATA
    master_path = Path(args.partants_master) if args.partants_master else PARTANTS_MASTER
    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT

    if not master_path.exists():
        log.error("partants_master not found: %s", master_path)
        sys.exit(1)

    # Phase 1: Build indexes
    log.info("Phase 1: Building enriched data indexes...")
    pmu_index = build_pmu_enriched_index(pmu_path)
    letrot_uid_index, letrot_key_index = build_letrot_index(letrot_path)

    total_enriched = len(pmu_index) + len(letrot_uid_index)
    if total_enriched == 0:
        log.warning("No enriched data found. Nothing to cross-reference.")
        log.warning("  PMU enriched: %s (exists=%s)", pmu_path, pmu_path.exists())
        log.warning("  Le Trot data: %s (exists=%s)", letrot_path, letrot_path.exists())
        # Still run to report current fill rates
    else:
        log.info("Total enriched records available: %d (PMU: %d, LeTrot uid: %d, LeTrot key: %d)",
                 total_enriched, len(pmu_index), len(letrot_uid_index), len(letrot_key_index))

    # Phase 2: Stream and fill
    log.info("Phase 2: Streaming partants_master and filling gaps...")
    result = cross_reference(
        pmu_index=pmu_index,
        letrot_uid_index=letrot_uid_index,
        letrot_key_index=letrot_key_index,
        master_path=master_path,
        output_path=output_path if not args.dry_run else None,
        dry_run=args.dry_run,
    )

    # Phase 3: Report
    print_report(result)

    if not args.dry_run and output_path:
        log.info("Output written to: %s", output_path)

    # Save JSON report
    report_path = Path(args.report_json) if args.report_json else (REPO_ROOT / "output" / "quality" / "crossref_fill_rate_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    log.info("JSON report saved to: %s", report_path)


if __name__ == "__main__":
    main()
