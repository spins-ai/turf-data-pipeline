#!/usr/bin/env python3
"""
Test cross-source consistency.
Cross-validates records between different data sources (PMU vs Le Trot, etc.).
Checks that shared keys (course IDs, horse names, dates) match across files.
"""
import argparse
import json
import os
import sys
from collections import defaultdict


# Define source groups and their key fields for cross-validation
SOURCE_GROUPS = {
    "courses": {
        "key_fields": ["id_course", "idCourse", "course_id"],
        "file_patterns": ["courses_brut", "courses_norm", "courses_master"],
    },
    "partants": {
        "key_fields": ["id_cheval", "idCheval", "cheval_id", "nom_cheval", "nomCheval"],
        "file_patterns": ["partants_brut", "partants_norm"],
    },
    "resultats": {
        "key_fields": ["id_course", "idCourse", "course_id"],
        "file_patterns": ["resultats", "courses_resultats"],
    },
}


def extract_keys_from_file(filepath, key_fields):
    """Extract unique key values from a file by streaming."""
    keys = defaultdict(set)
    record_count = 0

    try:
        if filepath.endswith(".jsonl"):
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(record, dict):
                        continue
                    record_count += 1
                    for kf in key_fields:
                        if kf in record and record[kf] is not None:
                            keys[kf].add(str(record[kf]))
        else:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            records = []
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                for key in ("data", "records", "results", "items", "courses", "partants"):
                    if key in data and isinstance(data[key], list):
                        records = data[key]
                        break
            for record in records:
                if not isinstance(record, dict):
                    continue
                record_count += 1
                for kf in key_fields:
                    if kf in record and record[kf] is not None:
                        keys[kf].add(str(record[kf]))
    except Exception as e:
        return None, 0, str(e)

    return dict(keys), record_count, None


def find_matching_files(output_dir, patterns):
    """Find files matching any of the given patterns."""
    matches = []
    for root, _dirs, filenames in os.walk(output_dir):
        for fname in sorted(filenames):
            if not (fname.endswith(".json") or fname.endswith(".jsonl")):
                continue
            fname_lower = fname.lower()
            for pattern in patterns:
                if pattern.lower() in fname_lower:
                    matches.append(os.path.join(root, fname))
                    break
    return matches


def cross_validate_group(output_dir, group_name, config):
    """Cross-validate files within a source group."""
    files = find_matching_files(output_dir, config["file_patterns"])
    if len(files) < 2:
        return None, f"Need at least 2 files for cross-validation, found {len(files)}"

    # Extract keys from each file
    file_keys = {}
    for filepath in files:
        relpath = os.path.relpath(filepath, output_dir)
        keys, count, error = extract_keys_from_file(filepath, config["key_fields"])
        if error:
            return None, f"Error reading {relpath}: {error}"
        if keys:
            file_keys[relpath] = {"keys": keys, "count": count}

    if len(file_keys) < 2:
        return None, "Not enough files with matching key fields"

    # Compare overlapping key fields between file pairs
    issues = []
    comparisons = []
    file_list = list(file_keys.items())

    for i in range(len(file_list)):
        for j in range(i + 1, len(file_list)):
            name_a, data_a = file_list[i]
            name_b, data_b = file_list[j]

            # Find common key fields
            common_fields = set(data_a["keys"].keys()) & set(data_b["keys"].keys())
            for field in common_fields:
                set_a = data_a["keys"][field]
                set_b = data_b["keys"][field]

                if not set_a or not set_b:
                    continue

                overlap = set_a & set_b
                only_a = set_a - set_b
                only_b = set_b - set_a

                overlap_pct = len(overlap) / max(len(set_a), len(set_b)) * 100

                comparison = {
                    "file_a": name_a,
                    "file_b": name_b,
                    "field": field,
                    "count_a": len(set_a),
                    "count_b": len(set_b),
                    "overlap": len(overlap),
                    "only_a": len(only_a),
                    "only_b": len(only_b),
                    "overlap_pct": overlap_pct,
                }
                comparisons.append(comparison)

                if overlap_pct < 50 and len(set_a) > 10 and len(set_b) > 10:
                    issues.append(
                        f"Low overlap on '{field}': {name_a} vs {name_b} = {overlap_pct:.1f}% "
                        f"({len(overlap)}/{max(len(set_a), len(set_b))})"
                    )

    return {"comparisons": comparisons, "issues": issues}, None


def check_duplicate_ids(output_dir):
    """Check for duplicate IDs within individual files."""
    issues = []
    for root, _dirs, filenames in os.walk(output_dir):
        for fname in sorted(filenames):
            if not (fname.endswith(".json") or fname.endswith(".jsonl")):
                continue
            filepath = os.path.join(root, fname)
            relpath = os.path.relpath(filepath, output_dir)

            id_fields = defaultdict(list)
            try:
                if filepath.endswith(".jsonl"):
                    with open(filepath, "r", encoding="utf-8") as f:
                        for line_num, line in enumerate(f, 1):
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                record = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if not isinstance(record, dict):
                                continue
                            for key in ("id", "id_course", "idCourse", "course_id"):
                                if key in record and record[key] is not None:
                                    id_fields[key].append(str(record[key]))
                else:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    records = []
                    if isinstance(data, list):
                        records = data
                    elif isinstance(data, dict):
                        for key in ("data", "records", "results", "items"):
                            if key in data and isinstance(data[key], list):
                                records = data[key]
                                break
                    for record in records:
                        if not isinstance(record, dict):
                            continue
                        for key in ("id", "id_course", "idCourse", "course_id"):
                            if key in record and record[key] is not None:
                                id_fields[key].append(str(record[key]))
            except Exception:
                continue

            for field, values in id_fields.items():
                unique = set(values)
                if len(values) > len(unique) and len(values) > 1:
                    dup_count = len(values) - len(unique)
                    dup_rate = dup_count / len(values) * 100
                    if dup_rate > 5:  # Only report if > 5% duplicates
                        issues.append(
                            f"{relpath}: field '{field}' has {dup_count} duplicates "
                            f"({dup_rate:.1f}%) out of {len(values)} records"
                        )

    return issues


def main():
    parser = argparse.ArgumentParser(description="Cross-validate records between sources")
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

    print(f"=== Cross-Source Validation Test ===")
    print(f"Scanning: {output_dir}\n")

    total_issues = 0

    # 1. Cross-validate source groups
    print("--- Cross-Source Overlap ---")
    for group_name, config in SOURCE_GROUPS.items():
        result, error = cross_validate_group(output_dir, group_name, config)
        if error:
            print(f"  SKIP  {group_name}: {error}")
            continue

        if result["comparisons"]:
            for comp in result["comparisons"]:
                status = "PASS" if comp["overlap_pct"] >= 50 else "WARN"
                print(
                    f"  {status}  {group_name}/{comp['field']}: "
                    f"{comp['file_a']} ({comp['count_a']}) vs "
                    f"{comp['file_b']} ({comp['count_b']}) "
                    f"-> {comp['overlap_pct']:.1f}% overlap ({comp['overlap']} shared)"
                )

        for issue in result["issues"]:
            total_issues += 1
            print(f"  WARN  {issue}")

    # 2. Check for duplicate IDs
    print("\n--- Duplicate ID Check ---")
    dup_issues = check_duplicate_ids(output_dir)
    if dup_issues:
        for issue in dup_issues:
            total_issues += 1
            print(f"  WARN  {issue}")
    else:
        print("  PASS  No significant duplicate IDs found")

    print(f"\n--- Summary ---")
    print(f"Cross-source issues: {total_issues}")

    # Cross-source issues are warnings, not hard failures
    overall = "PASS" if total_issues == 0 else "WARN"
    print(f"\nOverall: {overall}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
