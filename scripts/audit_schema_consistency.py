#!/usr/bin/env python3
"""Audit schema consistency across builder outputs.

Checks:
1. Do all records in a builder have the same set of keys?
2. Are there mixed types (e.g., some float, some string) for the same key?
3. Are there keys with empty strings instead of null?

Outputs a CSV with inconsistencies.
"""
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

BASE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
OUTPUT_CSV = Path("D:/turf-data-pipeline/04_FEATURES/schema_consistency_audit.csv")
SAMPLE_SIZE = 500


def _type_name(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        if v.strip() == "":
            return "empty_string"
        try:
            float(v)
            return "numeric_string"
        except ValueError:
            return "string"
    return type(v).__name__


def audit_builder(dirpath: Path) -> list[dict]:
    issues = []
    jsonls = [f for f in dirpath.iterdir() if f.suffix == ".jsonl" and ".tmp" not in f.name]
    if not jsonls:
        return issues
    fpath = jsonls[0]
    size = fpath.stat().st_size
    if size < 500:
        return issues

    all_keys = Counter()  # key -> count of records having it
    key_types = defaultdict(Counter)  # key -> {type: count}
    empty_strings = Counter()  # key -> count of empty string values
    total = 0

    try:
        with open(fpath, "r", encoding="utf-8") as f:
            # Sample from head (first 250) + tail (last 250)
            head_records = []
            for i, line in enumerate(f):
                if i >= 250:
                    break
                try:
                    head_records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

            # Tail
            f.seek(max(0, int(size * 0.8)))
            f.readline()
            tail_records = []
            for line in f:
                try:
                    tail_records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
                if len(tail_records) >= 250:
                    break

        records = head_records + tail_records
        total = len(records)
        if total == 0:
            return issues

        for rec in records:
            for k in rec:
                all_keys[k] += 1
                t = _type_name(rec[k])
                key_types[k][t] += 1
                if t == "empty_string":
                    empty_strings[k] += 1

    except Exception as e:
        return [{"builder": dirpath.name, "issue_type": "read_error", "key": "", "detail": str(e)}]

    builder_name = dirpath.name

    # Check 1: Keys not present in all records (schema inconsistency)
    for key, count in all_keys.items():
        if key == "partant_uid":
            continue
        if count < total:
            missing_pct = round((1 - count / total) * 100, 1)
            if missing_pct > 5:  # Only flag if >5% missing
                issues.append({
                    "builder": builder_name,
                    "issue_type": "missing_key",
                    "key": key,
                    "detail": f"Key missing in {missing_pct}% of sampled records ({total - count}/{total})"
                })

    # Check 2: Mixed types
    for key, types in key_types.items():
        if key == "partant_uid":
            continue
        non_null_types = {t: c for t, c in types.items() if t not in ("null",)}
        if len(non_null_types) > 1:
            # int + float is OK (normal JSON behavior)
            type_set = set(non_null_types.keys())
            if type_set == {"int", "float"}:
                continue
            if type_set == {"int", "float", "numeric_string"}:
                continue
            issues.append({
                "builder": builder_name,
                "issue_type": "mixed_types",
                "key": key,
                "detail": f"Mixed types: {dict(non_null_types)}"
            })

    # Check 3: Empty strings (should be null)
    for key, count in empty_strings.items():
        if count > 0:
            issues.append({
                "builder": builder_name,
                "issue_type": "empty_string",
                "key": key,
                "detail": f"{count}/{total} records have empty string instead of null"
            })

    return issues


def main():
    builders = sorted(d for d in BASE.iterdir() if d.is_dir())
    total = len(builders)
    all_issues = []

    print(f"Auditing {total} builders for schema consistency...", file=sys.stderr)

    for i, bdir in enumerate(builders):
        issues = audit_builder(bdir)
        all_issues.extend(issues)
        if (i + 1) % 50 == 0:
            print(f"  Audited {i+1}/{total}...", file=sys.stderr)

    # Write CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["builder", "issue_type", "key", "detail"])
        writer.writeheader()
        writer.writerows(all_issues)

    # Summary
    by_type = Counter(i["issue_type"] for i in all_issues)

    print(f"\n{'='*60}")
    print(f"SCHEMA CONSISTENCY AUDIT")
    print(f"{'='*60}")
    print(f"Builders audited: {total}")
    print(f"Total issues: {len(all_issues)}")
    for t, c in by_type.most_common():
        print(f"  {t}: {c}")
    print(f"\nOutput: {OUTPUT_CSV}")

    # Show worst offenders
    builder_counts = Counter(i["builder"] for i in all_issues)
    print(f"\n--- Builders with most issues (top 15) ---")
    for b, c in builder_counts.most_common(15):
        print(f"  {b}: {c} issues")


if __name__ == "__main__":
    main()
