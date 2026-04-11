#!/usr/bin/env python3
"""Auto-generate FEATURE_CATALOG.md from fill_rate_audit.csv and feature_catalog.json."""
import csv
import json
from collections import defaultdict
from pathlib import Path

FILL_CSV = Path("D:/turf-data-pipeline/04_FEATURES/fill_rate_audit.csv")
OUTPUT = Path("D:/turf-data-pipeline/docs/FEATURE_CATALOG.md")


def main():
    with open(FILL_CSV, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Group by builder
    by_builder = defaultdict(list)
    for r in rows:
        by_builder[r["builder"]].append(r)

    lines = ["# Feature Catalog", ""]
    lines.append(f"Auto-generated. {len(rows)} features across {len(by_builder)} builders.")
    lines.append("")
    lines.append("| Builder | Features | Avg Fill % | Min Fill % |")
    lines.append("|---------|----------|------------|------------|")

    for builder in sorted(by_builder.keys()):
        feats = by_builder[builder]
        fills = [float(f["fill_pct"]) for f in feats]
        avg_fill = sum(fills) / len(fills) if fills else 0
        min_fill = min(fills) if fills else 0
        lines.append(f"| {builder} | {len(feats)} | {avg_fill:.1f}% | {min_fill:.1f}% |")

    lines.append("")
    lines.append("## Feature Details")
    lines.append("")

    for builder in sorted(by_builder.keys()):
        feats = by_builder[builder]
        lines.append(f"### {builder}")
        lines.append("")
        lines.append("| Feature | Fill % |")
        lines.append("|---------|--------|")
        for f in sorted(feats, key=lambda x: x["feature"]):
            lines.append(f"| {f['feature']} | {f['fill_pct']}% |")
        lines.append("")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Generated {OUTPUT} ({len(rows)} features, {len(by_builder)} builders)")


if __name__ == "__main__":
    main()
