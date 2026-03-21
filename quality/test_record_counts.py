#!/usr/bin/env python3
"""
Test record counts per file.
Counts records in each JSON/JSONL file and compares with expected minimums.
"""
import argparse
import json
import os
import sys


# Expected minimum record counts per known file pattern.
# Keys are substrings matched against relative file paths.
EXPECTED_MINIMUMS = {
    "reunions": 100,
    "courses_brut": 500,
    "courses_norm": 500,
    "partants_brut": 1000,
    "partants_norm": 1000,
    "resultats": 1000,
    "historique_chevaux": 500,
    "historique_jockeys": 200,
    "cotes": 500,
    "pedigree": 200,
    "equipements": 100,
    "poids": 100,
    "sectionals": 50,
    "meteo": 50,
    "features": 500,
    "courses_master": 500,
}


def count_json_records(filepath):
    """Count records in a .json file (expects array or single object)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return len(data), None
        elif isinstance(data, dict):
            # Check common wrapper patterns
            for key in ("data", "records", "results", "items", "courses", "partants"):
                if key in data and isinstance(data[key], list):
                    return len(data[key]), None
            return 1, None
        return 0, None
    except Exception as e:
        return -1, str(e)


def count_jsonl_records(filepath):
    """Count records in a .jsonl file by streaming line by line."""
    count = 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    count += 1
        return count, None
    except Exception as e:
        return -1, str(e)


def get_expected_minimum(relpath):
    """Return expected minimum record count for a file based on its path."""
    relpath_lower = relpath.lower()
    for pattern, minimum in EXPECTED_MINIMUMS.items():
        if pattern in relpath_lower:
            return minimum, pattern
    return None, None


def main():
    parser = argparse.ArgumentParser(description="Test record counts per file")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Directory to scan (default: ../output or backup_20260314)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if any file is below expected minimum",
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

    print(f"=== Record Count Test ===")
    print(f"Scanning: {output_dir}\n")

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

    total = len(files)
    passed = 0
    failed = 0
    warnings = 0
    total_records = 0

    print(f"  {'Status':<6}  {'Records':>10}  {'Expected':>10}  File")
    print(f"  {'------':<6}  {'-------':>10}  {'--------':>10}  ----")

    for filepath in files:
        relpath = os.path.relpath(filepath, output_dir)

        if filepath.endswith(".jsonl"):
            count, error = count_jsonl_records(filepath)
        else:
            count, error = count_json_records(filepath)

        if error:
            failed += 1
            print(f"  FAIL  {'ERROR':>10}  {'':>10}  {relpath}  ({error})")
            continue

        total_records += max(count, 0)
        expected_min, pattern = get_expected_minimum(relpath)

        if expected_min is not None:
            if count < expected_min:
                if args.strict:
                    failed += 1
                    status = "FAIL"
                else:
                    warnings += 1
                    status = "WARN"
            else:
                passed += 1
                status = "PASS"
            print(f"  {status:<6}  {count:>10}  {expected_min:>10}  {relpath}")
        else:
            passed += 1
            print(f"  {'PASS':<6}  {count:>10}  {'N/A':>10}  {relpath}")

    print(f"\n--- Summary ---")
    print(f"Total files: {total}")
    print(f"Total records: {total_records:,}")
    print(f"Passed: {passed}")
    print(f"Warnings: {warnings}")
    print(f"Failed: {failed}")

    overall = "PASS" if failed == 0 and (not args.strict or warnings == 0) else "FAIL"
    print(f"\nOverall: {overall}")
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
