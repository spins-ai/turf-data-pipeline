#!/usr/bin/env python3
"""
scripts/remove_high_null_features.py
=====================================
Identifies and removes features with >90% None from features_matrix.jsonl.

Writes a cleaned version to output/features/features_matrix_clean.jsonl and
a report to output/quality/high_null_features_report.json.

No external APIs needed -- pure local data processing.

Usage:
    python scripts/remove_high_null_features.py                # dry-run (report only)
    python scripts/remove_high_null_features.py --execute      # remove and rewrite
    python scripts/remove_high_null_features.py --threshold 95 # custom threshold
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

FEATURES_MATRIX = ROOT / "output" / "features" / "features_matrix.jsonl"
FEATURES_CLEAN = ROOT / "output" / "features" / "features_matrix_clean.jsonl"
REPORT_PATH = ROOT / "output" / "quality" / "high_null_features_report.json"


def analyze_null_rates(path: Path, sample_size: int = 50000) -> dict:
    """Sample the JSONL and compute null rates per column."""
    null_counts: dict[str, int] = {}
    total_counts: dict[str, int] = {}
    n = 0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if n >= sample_size:
                break
            try:
                rec = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            n += 1
            for k, v in rec.items():
                total_counts[k] = total_counts.get(k, 0) + 1
                if v is None:
                    null_counts[k] = null_counts.get(k, 0) + 1

    rates = {}
    for k in sorted(total_counts.keys()):
        rate = null_counts.get(k, 0) / total_counts[k] * 100
        rates[k] = round(rate, 2)

    return {"sample_size": n, "total_columns": len(total_counts), "null_rates": rates}


def main():
    parser = argparse.ArgumentParser(description="Remove high-null features from features_matrix")
    parser.add_argument("--execute", action="store_true", help="Actually rewrite the file (default: dry-run)")
    parser.add_argument("--threshold", type=float, default=90.0, help="Null percentage threshold (default: 90)")
    parser.add_argument("--sample", type=int, default=50000, help="Sample size for analysis (default: 50000)")
    args = parser.parse_args()

    if not FEATURES_MATRIX.exists():
        print(f"ERROR: {FEATURES_MATRIX} not found")
        sys.exit(1)

    print(f"Analyzing null rates (sampling {args.sample} records)...")
    analysis = analyze_null_rates(FEATURES_MATRIX, args.sample)

    high_null = {
        k: v for k, v in analysis["null_rates"].items() if v > args.threshold
    }
    low_null = {
        k: v for k, v in analysis["null_rates"].items() if v <= args.threshold
    }

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "source_file": str(FEATURES_MATRIX),
        "sample_size": analysis["sample_size"],
        "total_columns": analysis["total_columns"],
        "threshold_pct": args.threshold,
        "columns_above_threshold": len(high_null),
        "columns_below_threshold": len(low_null),
        "removed_features": sorted(high_null.keys()),
        "removed_details": {k: f"{v}% null" for k, v in sorted(high_null.items(), key=lambda x: -x[1])},
        "retained_columns": len(low_null),
    }

    # Ensure output directory exists
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"Report written to {REPORT_PATH}")

    print(f"\nResults:")
    print(f"  Total columns: {analysis['total_columns']}")
    print(f"  Columns > {args.threshold}% null: {len(high_null)}")
    print(f"  Columns retained: {len(low_null)}")
    print(f"\nFeatures to remove ({len(high_null)}):")
    for k, v in sorted(high_null.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}% null")

    if not args.execute:
        print(f"\nDry-run mode. Use --execute to rewrite features_matrix.")
        return

    # Rewrite the file without the high-null columns
    remove_set = set(high_null.keys())
    print(f"\nRewriting features_matrix without {len(remove_set)} high-null columns...")

    tmp_path = FEATURES_CLEAN.with_suffix(".tmp")
    n_written = 0
    with open(FEATURES_MATRIX, "r", encoding="utf-8") as fin, \
         open(tmp_path, "w", encoding="utf-8") as fout:
        for line in fin:
            try:
                rec = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            cleaned = {k: v for k, v in rec.items() if k not in remove_set}
            fout.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
            n_written += 1
            if n_written % 100000 == 0:
                print(f"  ...{n_written} records written")

    # Atomic rename
    if FEATURES_CLEAN.exists():
        FEATURES_CLEAN.unlink()
    tmp_path.rename(FEATURES_CLEAN)

    print(f"\nDone: {n_written} records written to {FEATURES_CLEAN}")
    print(f"Removed {len(remove_set)} features: {sorted(remove_set)}")
    print(f"\nOriginal file preserved at: {FEATURES_MATRIX}")
    print(f"Cleaned file at: {FEATURES_CLEAN}")


if __name__ == "__main__":
    main()
