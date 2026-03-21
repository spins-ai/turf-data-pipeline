#!/usr/bin/env python3
"""
Test feature quality in numeric data.
Checks for NaN, Inf, and excessive null rates in numeric features.
"""
import argparse
import json
import math
import os
import sys
from collections import defaultdict


def is_nan_or_inf(value):
    """Check if a value is NaN or Inf (works for floats and string representations)."""
    if isinstance(value, float):
        return math.isnan(value) or math.isinf(value)
    if isinstance(value, str):
        lower = value.strip().lower()
        return lower in ("nan", "inf", "-inf", "infinity", "-infinity")
    return False


def is_numeric_value(value):
    """Check if a value looks numeric."""
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    return False


def analyze_jsonl_features(filepath, max_lines=100000):
    """Stream a JSONL file and analyze feature quality."""
    stats = defaultdict(lambda: {"total": 0, "null": 0, "nan_inf": 0, "numeric": 0})
    record_count = 0

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                if line_num > max_lines:
                    break
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
                for key, value in record.items():
                    stats[key]["total"] += 1
                    if value is None:
                        stats[key]["null"] += 1
                    elif is_nan_or_inf(value):
                        stats[key]["nan_inf"] += 1
                    elif is_numeric_value(value):
                        stats[key]["numeric"] += 1
    except Exception as e:
        return None, record_count, str(e)

    return dict(stats), record_count, None


def analyze_json_features(filepath):
    """Analyze a JSON file for feature quality."""
    stats = defaultdict(lambda: {"total": 0, "null": 0, "nan_inf": 0, "numeric": 0})
    record_count = 0

    try:
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
            for key, value in record.items():
                stats[key]["total"] += 1
                if value is None:
                    stats[key]["null"] += 1
                elif is_nan_or_inf(value):
                    stats[key]["nan_inf"] += 1
                elif is_numeric_value(value):
                    stats[key]["numeric"] += 1
    except Exception as e:
        return None, record_count, str(e)

    return dict(stats), record_count, None


def main():
    parser = argparse.ArgumentParser(description="Test feature quality (NaN/Inf/null rates)")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Directory to scan (default: ../output or backup_20260314)",
    )
    parser.add_argument(
        "--null-threshold",
        type=float,
        default=0.5,
        help="Max null rate before warning (default: 0.5 = 50%%)",
    )
    parser.add_argument(
        "--max-nan-inf",
        type=int,
        default=0,
        help="Max allowed NaN/Inf values per feature (default: 0)",
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

    print(f"=== Feature Quality Test ===")
    print(f"Scanning: {output_dir}")
    print(f"Null threshold: {args.null_threshold:.0%}")
    print(f"Max NaN/Inf: {args.max_nan_inf}\n")

    skip_dirs = {"cache", "cache_corrupted", "html_cache", "html"}
    files = []
    for root, dirs, filenames in os.walk(output_dir):
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        for fname in sorted(filenames):
            if fname.endswith(".json") or fname.endswith(".jsonl"):
                files.append(os.path.join(root, fname))

    if not files:
        print(f"WARN: No .json/.jsonl files found in {output_dir}")
        sys.exit(0)

    total_files = len(files)
    files_with_issues = 0
    total_nan_inf = 0
    total_high_null = 0

    for filepath in files:
        relpath = os.path.relpath(filepath, output_dir)

        if filepath.endswith(".jsonl"):
            stats, record_count, error = analyze_jsonl_features(filepath)
        else:
            stats, record_count, error = analyze_json_features(filepath)

        if error:
            print(f"  FAIL  {relpath}: {error}")
            files_with_issues += 1
            continue

        if stats is None or record_count == 0:
            print(f"  SKIP  {relpath}: no records")
            continue

        # Check each feature
        file_issues = []
        for feature_name, feature_stats in sorted(stats.items()):
            total = feature_stats["total"]
            if total == 0:
                continue

            nan_inf_count = feature_stats["nan_inf"]
            null_count = feature_stats["null"]
            null_rate = null_count / total

            if nan_inf_count > args.max_nan_inf:
                file_issues.append(
                    f"  NaN/Inf: {feature_name} has {nan_inf_count} NaN/Inf values"
                )
                total_nan_inf += nan_inf_count

            if null_rate > args.null_threshold:
                file_issues.append(
                    f"  High null: {feature_name} = {null_rate:.1%} null ({null_count}/{total})"
                )
                total_high_null += 1

        if file_issues:
            files_with_issues += 1
            print(f"  WARN  {relpath} ({record_count} records)")
            for issue in file_issues[:20]:  # Limit output
                print(f"        {issue}")
            if len(file_issues) > 20:
                print(f"        ... and {len(file_issues) - 20} more issues")
        else:
            print(f"  PASS  {relpath} ({record_count} records, {len(stats)} features)")

    print(f"\n--- Summary ---")
    print(f"Total files: {total_files}")
    print(f"Files with issues: {files_with_issues}")
    print(f"Total NaN/Inf values found: {total_nan_inf}")
    print(f"Features with high null rate: {total_high_null}")

    # NaN/Inf is a hard failure, high nulls are warnings
    overall = "PASS" if total_nan_inf == 0 else "FAIL"
    print(f"\nOverall: {overall}")
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
