#!/usr/bin/env python3
"""Find feature pairs with correlation > 0.95 across builder outputs.

Extends the dedup audit to a lower threshold. Produces:
1. CSV of all pairs with |r| > 0.95
2. A "drop list" — features to remove (keeping the one with highest fill rate per group)

Uses the fill_rate_audit.csv to decide which feature to keep in each correlated group.
"""
import csv
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

BASE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
FILL_CSV = Path("D:/turf-data-pipeline/04_FEATURES/fill_rate_audit.csv")
OUTPUT_PAIRS = Path("D:/turf-data-pipeline/04_FEATURES/high_correlation_pairs.csv")
OUTPUT_DROP = Path("D:/turf-data-pipeline/04_FEATURES/features_to_drop.csv")
THRESHOLD = 0.95
SAMPLE_SIZE = 500


def _sample_builder(dirpath: Path) -> dict[str, dict[str, float]]:
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
            f.readline()
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
    except Exception:
        pass
    return records


def _pearson(xs, ys):
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
    # Load fill rates
    fill_rates = {}
    if FILL_CSV.exists():
        with open(FILL_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = f"{row['builder']}/{row['feature']}"
                fill_rates[key] = float(row['fill_pct'])

    # Collect numeric features
    all_features: dict[tuple[str, str], dict[str, float]] = {}
    builders = sorted(d for d in BASE.iterdir() if d.is_dir())
    total = len(builders)
    print(f"Sampling {total} builders...", file=sys.stderr)

    for i, bdir in enumerate(builders):
        records = _sample_builder(bdir)
        if not records:
            continue
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
                        pass
            if len(vals) >= 20:
                all_features[(bdir.name, fname)] = vals

        if (i + 1) % 50 == 0:
            print(f"  Sampled {i+1}/{total}...", file=sys.stderr)

    print(f"Total numeric features: {len(all_features)}", file=sys.stderr)

    # Group by builder for cross-builder comparison
    builder_features = defaultdict(list)
    for (builder, feat) in all_features:
        builder_features[builder].append(feat)

    builder_names = sorted(builder_features.keys())
    pairs = []
    checked = 0

    # Cross-builder correlations
    for bi in range(len(builder_names)):
        for bj in range(bi + 1, len(builder_names)):
            b1, b2 = builder_names[bi], builder_names[bj]
            for f1 in builder_features[b1]:
                vals1 = all_features[(b1, f1)]
                for f2 in builder_features[b2]:
                    vals2 = all_features[(b2, f2)]
                    shared = set(vals1.keys()) & set(vals2.keys())
                    if len(shared) < 20:
                        continue
                    xs = [vals1[uid] for uid in shared]
                    ys = [vals2[uid] for uid in shared]
                    r = _pearson(xs, ys)
                    if r is not None and abs(r) > THRESHOLD:
                        pairs.append({
                            "builder_a": b1, "feature_a": f1,
                            "builder_b": b2, "feature_b": f2,
                            "correlation": round(r, 6),
                            "shared_samples": len(shared),
                        })
                    checked += 1
                    if checked % 500_000 == 0:
                        print(f"  Checked {checked:,} pairs, found {len(pairs)} so far...", file=sys.stderr)

    # Also check intra-builder correlations
    for b in builder_names:
        feats = builder_features[b]
        for fi in range(len(feats)):
            for fj in range(fi + 1, len(feats)):
                f1, f2 = feats[fi], feats[fj]
                vals1 = all_features[(b, f1)]
                vals2 = all_features[(b, f2)]
                shared = set(vals1.keys()) & set(vals2.keys())
                if len(shared) < 20:
                    continue
                xs = [vals1[uid] for uid in shared]
                ys = [vals2[uid] for uid in shared]
                r = _pearson(xs, ys)
                if r is not None and abs(r) > THRESHOLD:
                    pairs.append({
                        "builder_a": b, "feature_a": f1,
                        "builder_b": b, "feature_b": f2,
                        "correlation": round(r, 6),
                        "shared_samples": len(shared),
                    })

    pairs.sort(key=lambda x: (-abs(x["correlation"]), x["builder_a"]))

    # Write pairs CSV
    OUTPUT_PAIRS.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PAIRS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "builder_a", "feature_a", "builder_b", "feature_b", "correlation", "shared_samples"
        ])
        writer.writeheader()
        writer.writerows(pairs)

    # Build drop list using Union-Find to group correlated features
    # In each group, keep the feature with the highest fill rate
    parent = {}

    def find(x):
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for p in pairs:
        fa = f"{p['builder_a']}/{p['feature_a']}"
        fb = f"{p['builder_b']}/{p['feature_b']}"
        union(fa, fb)

    groups = defaultdict(list)
    all_feats = set()
    for p in pairs:
        all_feats.add(f"{p['builder_a']}/{p['feature_a']}")
        all_feats.add(f"{p['builder_b']}/{p['feature_b']}")

    for feat in all_feats:
        root = find(feat)
        groups[root].append(feat)

    to_drop = []
    for root, members in groups.items():
        # Keep the member with highest fill rate
        members_sorted = sorted(members, key=lambda f: fill_rates.get(f, 0), reverse=True)
        keep = members_sorted[0]
        for feat in members_sorted[1:]:
            to_drop.append({
                "feature_to_drop": feat,
                "kept_feature": keep,
                "fill_rate_drop": fill_rates.get(feat, 0),
                "fill_rate_keep": fill_rates.get(keep, 0),
                "reason": f"correlated with {keep} (r > {THRESHOLD})",
            })

    to_drop.sort(key=lambda x: x["feature_to_drop"])
    with open(OUTPUT_DROP, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "feature_to_drop", "kept_feature", "fill_rate_drop", "fill_rate_keep", "reason"
        ])
        writer.writeheader()
        writer.writerows(to_drop)

    # Summary
    print(f"\n{'='*60}")
    print(f"HIGH CORRELATION AUDIT (threshold={THRESHOLD})")
    print(f"{'='*60}")
    print(f"Features analyzed: {len(all_features)}")
    print(f"Pairs checked: {checked:,}")
    print(f"Correlated pairs (|r| > {THRESHOLD}): {len(pairs)}")
    print(f"Correlation groups: {len(groups)}")
    print(f"Features to drop: {len(to_drop)}")
    print(f"\nPairs CSV: {OUTPUT_PAIRS}")
    print(f"Drop list CSV: {OUTPUT_DROP}")

    if to_drop:
        print(f"\n--- FEATURES TO DROP (first 30) ---")
        for d in to_drop[:30]:
            print(f"  DROP {d['feature_to_drop']} (fill={d['fill_rate_drop']}%) → KEEP {d['kept_feature']} (fill={d['fill_rate_keep']}%)")


if __name__ == "__main__":
    main()
