#!/usr/bin/env python3
"""Audit outliers across builder outputs.

For each numeric feature, computes mean/std from a sample and identifies:
1. Features with values > 5 sigma from mean
2. Features with extreme range (max/min ratio > 1000)
3. Recommended capping thresholds (mean +/- 5*std)

Output: CSV with capping thresholds for consolidation.
"""
import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

BASE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
OUTPUT_CSV = Path("D:/turf-data-pipeline/04_FEATURES/outlier_capping_thresholds.csv")
SAMPLE_SIZE = 1000
SIGMA_THRESHOLD = 5


def main():
    builders = sorted(d for d in BASE.iterdir() if d.is_dir())
    total = len(builders)
    results = []

    print(f"Scanning {total} builders for outliers...", file=sys.stderr)

    for i, bdir in enumerate(builders):
        jsonls = [f for f in bdir.iterdir() if f.suffix == ".jsonl" and ".tmp" not in f.name]
        if not jsonls:
            continue
        fpath = jsonls[0]
        size = fpath.stat().st_size
        if size < 1000:
            continue

        # Sample from tail
        feature_vals = defaultdict(list)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                f.seek(max(0, int(size * 0.8)))
                f.readline()
                count = 0
                for line in f:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    for k, v in rec.items():
                        if k == "partant_uid":
                            continue
                        if v is not None:
                            try:
                                fv = float(v)
                                if math.isfinite(fv):
                                    feature_vals[k].append(fv)
                            except (ValueError, TypeError):
                                pass
                    count += 1
                    if count >= SAMPLE_SIZE:
                        break
        except Exception:
            continue

        builder_name = bdir.name

        for feat, vals in feature_vals.items():
            n = len(vals)
            if n < 20:
                continue

            mean = sum(vals) / n
            var = sum((v - mean) ** 2 for v in vals) / n
            std = math.sqrt(var) if var > 0 else 0
            vmin = min(vals)
            vmax = max(vals)

            # Count outliers
            if std > 1e-10:
                outliers_high = sum(1 for v in vals if v > mean + SIGMA_THRESHOLD * std)
                outliers_low = sum(1 for v in vals if v < mean - SIGMA_THRESHOLD * std)
                cap_high = round(mean + SIGMA_THRESHOLD * std, 6)
                cap_low = round(mean - SIGMA_THRESHOLD * std, 6)
            else:
                outliers_high = 0
                outliers_low = 0
                cap_high = vmax
                cap_low = vmin

            total_outliers = outliers_high + outliers_low
            outlier_pct = round(total_outliers / n * 100, 2)

            # Flag extreme ranges
            range_ratio = (vmax - vmin) / std if std > 1e-10 else 0

            if total_outliers > 0 or range_ratio > 20:
                results.append({
                    "builder": builder_name,
                    "feature": feat,
                    "n_sampled": n,
                    "mean": round(mean, 6),
                    "std": round(std, 6),
                    "min": round(vmin, 6),
                    "max": round(vmax, 6),
                    "range_sigma": round(range_ratio, 2),
                    "outliers_high": outliers_high,
                    "outliers_low": outliers_low,
                    "outlier_pct": outlier_pct,
                    "cap_low": cap_low,
                    "cap_high": cap_high,
                })

        if (i + 1) % 50 == 0:
            print(f"  Scanned {i+1}/{total}...", file=sys.stderr)

    results.sort(key=lambda x: -x["outlier_pct"])

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "builder", "feature", "n_sampled", "mean", "std", "min", "max",
            "range_sigma", "outliers_high", "outliers_low", "outlier_pct",
            "cap_low", "cap_high"
        ])
        writer.writeheader()
        writer.writerows(results)

    # Summary
    features_with_outliers = [r for r in results if r["outlier_pct"] > 0]
    severe = [r for r in results if r["outlier_pct"] > 1]

    print(f"\n{'='*60}")
    print(f"OUTLIER AUDIT (>{SIGMA_THRESHOLD} sigma)")
    print(f"{'='*60}")
    print(f"Features flagged: {len(results)}")
    print(f"Features with outliers: {len(features_with_outliers)}")
    print(f"Features with >1% outliers: {len(severe)}")
    print(f"\nOutput: {OUTPUT_CSV}")

    if severe:
        print(f"\n--- Features with >1% outliers (top 20) ---")
        for r in severe[:20]:
            print(f"  {r['builder']}/{r['feature']}: {r['outlier_pct']}% outliers "
                  f"(range: [{r['min']}, {r['max']}], {r['range_sigma']} sigma)")


if __name__ == "__main__":
    main()
