#!/usr/bin/env python3
"""Audit fill rates across ALL builder outputs.
Samples 1000 records from the tail of each file (where accumulators are warm).
Outputs a CSV with builder, feature, fill_rate."""
import csv
import json
import os
import sys

BASE = "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs"
OUTPUT_CSV = "D:/turf-data-pipeline/04_FEATURES/fill_rate_audit.csv"
SAMPLE_SIZE = 1000


def main():
    results = []
    builders = sorted(os.listdir(BASE))
    total = len(builders)

    for i, name in enumerate(builders):
        d = os.path.join(BASE, name)
        if not os.path.isdir(d):
            continue
        jsonls = [f for f in os.listdir(d) if f.endswith('.jsonl') and not f.endswith('.tmp')]
        if not jsonls:
            continue

        fpath = os.path.join(d, jsonls[0])
        size = os.path.getsize(fpath)
        if size < 1000:
            continue

        # Sample from tail (80% offset)
        fills = {}
        total_sampled = 0
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                f.seek(max(0, int(size * 0.8)))
                f.readline()  # skip partial
                for line in f:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    total_sampled += 1
                    for k, v in rec.items():
                        if k == 'partant_uid':
                            continue
                        if v is not None and str(v).strip() != '':
                            fills[k] = fills.get(k, 0) + 1
                    if total_sampled >= SAMPLE_SIZE:
                        break
        except Exception as e:
            print(f"  ERROR {name}: {e}", file=sys.stderr)
            continue

        if total_sampled == 0:
            continue

        for feat, count in sorted(fills.items()):
            rate = count / total_sampled
            results.append({
                'builder': name,
                'feature': feat,
                'fill_rate': round(rate, 4),
                'fill_pct': round(rate * 100, 1),
                'sampled': total_sampled
            })

        if (i + 1) % 50 == 0:
            print(f"  Processed {i+1}/{total} builders...", file=sys.stderr)

    # Write CSV
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['builder', 'feature', 'fill_rate', 'fill_pct', 'sampled'])
        writer.writeheader()
        writer.writerows(results)

    # Summary
    total_features = len(results)
    low_fill = [r for r in results if r['fill_pct'] < 10]
    zero_fill = [r for r in results if r['fill_pct'] == 0]

    print(f"\n=== FILL RATE AUDIT ===")
    print(f"Total features scanned: {total_features}")
    print(f"Features with fill <10%: {len(low_fill)}")
    print(f"Features with fill 0%: {len(zero_fill)}")
    print(f"\nOutput: {OUTPUT_CSV}")

    if low_fill:
        print(f"\n--- Features <10% fill rate ---")
        for r in sorted(low_fill, key=lambda x: x['fill_pct']):
            print(f"  {r['builder']}/{r['feature']}: {r['fill_pct']}%")


if __name__ == "__main__":
    main()
