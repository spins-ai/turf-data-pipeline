#!/usr/bin/env python3
"""
Test value ranges in data files.
Checks that cotes > 0, distances > 0, no negative values where not expected, etc.
"""
import argparse
import json
import os
import sys
from collections import defaultdict


# Fields that must be strictly positive (> 0)
POSITIVE_FIELDS = {
    "cote", "cote_probable", "cote_depart", "cote_actuelle",
    "rapport_simple", "rapport_couple",
    "distance", "distance_course", "dist",
    "numero", "numPmu",
    "allocation", "prix", "dotation",
    "age",
    "poids", "poids_jockey", "handicap_poids",
}

# Fields that must be non-negative (>= 0)
NON_NEGATIVE_FIELDS = {
    "place", "rang", "classement",
    "gain", "gains", "gains_carriere", "gain_annee",
    "nb_courses", "nb_victoires", "nb_places",
    "temps", "reduction_km", "temps_km",
    "ecart",
}

# Fields where negative values are acceptable
ALLOW_NEGATIVE = {
    "variation_cote", "delta", "diff", "ecart_cote",
    "temperature", "temp",
}


def is_numeric(value):
    """Check if value is numeric and return float, or None."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    return None


def check_field_value(field_name, value):
    """Check if a field's value is in expected range. Returns issue string or None."""
    field_lower = field_name.lower()
    num = is_numeric(value)
    if num is None:
        return None  # Not numeric, skip

    # Check allow-negative first
    for pattern in ALLOW_NEGATIVE:
        if pattern in field_lower:
            return None

    # Check strictly positive fields
    for pattern in POSITIVE_FIELDS:
        if pattern in field_lower:
            if num <= 0:
                return f"expected > 0, got {value}"
            return None

    # Check non-negative fields
    for pattern in NON_NEGATIVE_FIELDS:
        if pattern in field_lower:
            if num < 0:
                return f"expected >= 0, got {value}"
            return None

    return None


def analyze_file(filepath):
    """Analyze value ranges in a single file."""
    issues_by_field = defaultdict(lambda: {"count": 0, "examples": []})
    record_count = 0

    def check_record(record):
        nonlocal record_count
        if not isinstance(record, dict):
            return
        record_count += 1
        for key, value in record.items():
            if value is None:
                continue
            issue = check_field_value(key, value)
            if issue:
                issues_by_field[key]["count"] += 1
                if len(issues_by_field[key]["examples"]) < 3:
                    issues_by_field[key]["examples"].append(issue)

    try:
        if filepath.endswith(".jsonl"):
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        check_record(record)
                    except json.JSONDecodeError:
                        continue
        else:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for record in data:
                    check_record(record)
            elif isinstance(data, dict):
                for key in ("data", "records", "results", "items", "courses", "partants"):
                    if key in data and isinstance(data[key], list):
                        for record in data[key]:
                            check_record(record)
                        break
                else:
                    check_record(data)
    except Exception as e:
        return None, 0, str(e)

    return dict(issues_by_field), record_count, None


def main():
    parser = argparse.ArgumentParser(description="Test value ranges in data files")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Directory to scan (default: ../output or backup_20260314)",
    )
    args = parser.parse_args()

    # Resolve output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        candidate_output = os.path.join(parent_dir, "output")
        candidate_backup = os.path.join(parent_dir, "backup_20260314")
        if os.path.isdir(candidate_output):
            output_dir = candidate_output
        elif os.path.isdir(candidate_backup):
            output_dir = candidate_backup
        else:
            print(f"FAIL: No output directory found (tried {candidate_output}, {candidate_backup})")
            sys.exit(1)

    print(f"=== Value Range Test ===")
    print(f"Scanning: {output_dir}\n")

    files = []
    for root, _dirs, filenames in os.walk(output_dir):
        for fname in sorted(filenames):
            if fname.endswith(".json") or fname.endswith(".jsonl"):
                files.append(os.path.join(root, fname))

    if not files:
        print(f"WARN: No .json/.jsonl files found in {output_dir}")
        sys.exit(0)

    total_issues = 0
    files_with_issues = 0

    for filepath in files:
        relpath = os.path.relpath(filepath, output_dir)
        issues_by_field, record_count, error = analyze_file(filepath)

        if error:
            print(f"  FAIL  {relpath}: {error}")
            files_with_issues += 1
            continue

        if record_count == 0:
            continue

        if issues_by_field:
            files_with_issues += 1
            file_total = sum(v["count"] for v in issues_by_field.values())
            total_issues += file_total
            print(f"  FAIL  {relpath} ({record_count} records, {file_total} violations)")
            for field, info in sorted(issues_by_field.items()):
                examples = "; ".join(info["examples"][:3])
                print(f"        {field}: {info['count']}x  ({examples})")
        else:
            print(f"  PASS  {relpath} ({record_count} records)")

    print(f"\n--- Summary ---")
    print(f"Files with range issues: {files_with_issues}")
    print(f"Total range violations: {total_issues}")

    overall = "PASS" if total_issues == 0 else "FAIL"
    print(f"\nOverall: {overall}")
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
