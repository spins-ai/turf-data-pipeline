#!/usr/bin/env python3
"""Detect duplicate/near-duplicate features across builder outputs.

Strategy:
1. Sample 500 records from each builder (from tail/warm zone)
2. For each feature, compute a fingerprint (sorted values hash)
3. Compare fingerprints to find exact duplicates
4. For numeric features, compute pairwise Pearson correlation on shared samples
5. Flag pairs with correlation > 0.99 as near-duplicates

Output: CSV with duplicate pairs and their correlation.
"""
import csv
import hashlib
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

BASE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
OUTPUT_CSV = Path("D:/turf-data-pipeline/04_FEATURES/dedup_audit.csv")
SAMPLE_SIZE = 500


def _sample_builder(dirpath: Path) -> dict[str, dict[str, float | None]]:
    """Sample records from a builder's JSONL, return {partant_uid: {feat: val}}."""
    jsonls = [f for f in dirpath.iterdir() if f.suffix == ".jsonl" and ".tmp" not in f.name]
    if not jsonls:
        return {}
    fpath = jsonls[0]
    size = fpath.stat().st_size
    if size < 1000:
        return {}

    records = {}
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            f.seek(max(0, int(size * 0.8)))
            f.readline()  # skip partial
            count = 0
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                uid = rec.get("partant_uid", "")
                if uid:
                    records[uid] = {k: v for k, v in rec.items() if k != "partant_uid"}
                count += 1
                if count >= SAMPLE_SIZE:
                    break
    except Exception as e:
        print(f"  ERROR {dirpath.name}: {e}", file=sys.stderr)
    return records


def _pearson(xs, ys):
    """Compute Pearson correlation between two lists."""
    n = len(xs)
    if n < 10:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sx = math.sqrt(max(0, sum((x - mx) ** 2 for x in xs) / n))
    sy = math.sqrt(max(0, sum((y - my) ** 2 for y in ys) / n))
    if sx < 1e-12 or sy < 1e-12:
        return None
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
    return cov / (sx * sy)


def main():
    # Step 1: Collect all features with their values keyed by partant_uid
    # Structure: {(builder, feature): {uid: value}}
    all_features: dict[tuple[str, str], dict[str, float]] = {}

    builders = sorted(d for d in BASE.iterdir() if d.is_dir())
    total = len(builders)
    print(f"Sampling {total} builder directories...", file=sys.stderr)

    for i, bdir in enumerate(builders):
        records = _sample_builder(bdir)
        if not records:
            continue
        # Collect each feature
        feat_names = set()
        for uid, feats in records.items():
            feat_names.update(feats.keys())

        for fname in feat_names:
            vals = {}
            for uid, feats in records.items():
                v = feats.get(fname)
                if v is not None:
                    try:
                        vals[uid] = float(v)
                    except (ValueError, TypeError):
                        # Non-numeric, use hash-based comparison only
                        vals[uid] = str(v)
            if len(vals) >= 10:
                all_features[(bdir.name, fname)] = vals

        if (i + 1) % 50 == 0:
            print(f"  Sampled {i+1}/{total}...", file=sys.stderr)

    print(f"\nTotal features collected: {len(all_features)}", file=sys.stderr)

    # Step 2: Build fingerprints for exact duplicate detection
    # Fingerprint = hash of sorted (uid, value) pairs
    fingerprints: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for (builder, feat), vals in all_features.items():
        # Only use UIDs that have values
        sorted_pairs = sorted(vals.items())
        # Hash the values
        h = hashlib.md5(str(sorted_pairs).encode()).hexdigest()
        fingerprints[h].append((builder, feat))

    # Step 3: Find exact duplicates
    exact_dupes = []
    for h, features in fingerprints.items():
        if len(features) > 1:
            for i in range(len(features)):
                for j in range(i + 1, len(features)):
                    exact_dupes.append({
                        "builder_a": features[i][0],
                        "feature_a": features[i][1],
                        "builder_b": features[j][0],
                        "feature_b": features[j][1],
                        "correlation": 1.0,
                        "type": "exact_duplicate",
                        "shared_samples": len(all_features[features[i]]),
                    })

    print(f"Exact duplicates found: {len(exact_dupes)}", file=sys.stderr)

    # Step 4: For numeric features, find near-duplicates (correlation > 0.99)
    # Group features by shared UIDs to make comparison feasible
    # Only compare features from DIFFERENT builders
    numeric_features = {}
    for key, vals in all_features.items():
        if vals and all(isinstance(v, (int, float)) for v in vals.values()):
            numeric_features[key] = vals

    print(f"Numeric features for correlation: {len(numeric_features)}", file=sys.stderr)

    # Build index of features per builder
    builder_features = defaultdict(list)
    for (builder, feat) in numeric_features:
        builder_features[builder].append(feat)

    near_dupes = []
    builder_names = sorted(builder_features.keys())

    # Compare features across different builders
    checked = 0
    for bi in range(len(builder_names)):
        for bj in range(bi + 1, len(builder_names)):
            b1, b2 = builder_names[bi], builder_names[bj]
            for f1 in builder_features[b1]:
                vals1 = numeric_features[(b1, f1)]
                for f2 in builder_features[b2]:
                    vals2 = numeric_features[(b2, f2)]
                    # Find shared UIDs
                    shared = set(vals1.keys()) & set(vals2.keys())
                    if len(shared) < 20:
                        continue
                    xs = [vals1[uid] for uid in shared]
                    ys = [vals2[uid] for uid in shared]
                    r = _pearson(xs, ys)
                    if r is not None and abs(r) > 0.99:
                        near_dupes.append({
                            "builder_a": b1,
                            "feature_a": f1,
                            "builder_b": b2,
                            "feature_b": f2,
                            "correlation": round(r, 6),
                            "type": "near_duplicate",
                            "shared_samples": len(shared),
                        })
                    checked += 1
                    if checked % 1_000_000 == 0:
                        print(f"  Checked {checked:,} pairs...", file=sys.stderr)

    print(f"Near duplicates found: {len(near_dupes)}", file=sys.stderr)

    # Write results
    results = exact_dupes + near_dupes
    results.sort(key=lambda x: (-abs(x["correlation"]), x["builder_a"], x["feature_a"]))

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "type", "builder_a", "feature_a", "builder_b", "feature_b",
            "correlation", "shared_samples"
        ])
        writer.writeheader()
        writer.writerows(results)

    # Summary
    print(f"\n{'='*60}")
    print(f"DEDUP AUDIT RESULTS")
    print(f"{'='*60}")
    print(f"Total features analyzed: {len(all_features)}")
    print(f"Exact duplicates: {len(exact_dupes)}")
    print(f"Near duplicates (|r| > 0.99): {len(near_dupes)}")
    print(f"Total pairs checked: {checked:,}")
    print(f"\nOutput: {OUTPUT_CSV}")

    if exact_dupes:
        print(f"\n--- EXACT DUPLICATES ---")
        for d in exact_dupes[:30]:
            print(f"  {d['builder_a']}/{d['feature_a']} == {d['builder_b']}/{d['feature_b']}")

    if near_dupes:
        print(f"\n--- NEAR DUPLICATES (top 30) ---")
        for d in near_dupes[:30]:
            print(f"  {d['builder_a']}/{d['feature_a']} ~= {d['builder_b']}/{d['feature_b']} (r={d['correlation']})")


if __name__ == "__main__":
    main()
