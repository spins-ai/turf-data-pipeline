#!/usr/bin/env python3
"""
fix_14_consolidate.py
=====================
Reads ALL JSON files from output/14_pedigree/cache/,
filters only those with "found": true,
and exports to output/14_pedigree/pedigrees_pq.json.
"""

import json
from pathlib import Path

CACHE_DIR = Path(__file__).resolve().parent / "output" / "14_pedigree" / "cache"
OUTPUT_PATH = Path(__file__).resolve().parent / "output" / "14_pedigree" / "pedigrees_pq.json"


def main():
    cache_files = sorted(CACHE_DIR.glob("*.json"))
    print(f"Cache files found: {len(cache_files)}")

    records = []
    errors = 0

    for f in cache_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if data.get("found") is True:
                records.append(data)
        except (json.JSONDecodeError, OSError) as e:
            errors += 1

    print(f"Records with found=true: {len(records)}")
    if errors:
        print(f"Files with errors (skipped): {errors}")

    # Deduplicate by horse_id
    seen = {}
    for r in records:
        hid = r.get("horse_id", "")
        if hid not in seen:
            seen[hid] = r
    deduped = list(seen.values())
    print(f"After dedup by horse_id: {len(deduped)}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as out:
        json.dump(deduped, out, ensure_ascii=False, indent=2, default=str)

    print(f"Exported to {OUTPUT_PATH} ({len(deduped)} records)")


if __name__ == "__main__":
    main()
