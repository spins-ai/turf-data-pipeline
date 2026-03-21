#!/usr/bin/env python3
"""
Test JSON/JSONL file integrity.
Checks that all JSON/JSONL files are valid (not truncated, valid encoding, parseable).
"""
import argparse
import json
import os
import sys


SKIP_DIRS = {"cache", "cache_corrupted", "html_cache", "html"}


def find_json_files(output_dir):
    """Find all .json and .jsonl files recursively."""
    files = []
    for root, dirs, filenames in os.walk(output_dir):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fname in sorted(filenames):
            if fname.endswith(".json") or fname.endswith(".jsonl"):
                files.append(os.path.join(root, fname))
    return files


def test_json_file(filepath):
    """Test a single .json file for validity."""
    errors = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        if not content.strip():
            errors.append("File is empty")
            return errors
        json.loads(content)
    except UnicodeDecodeError as e:
        errors.append(f"Encoding error: {e}")
    except json.JSONDecodeError as e:
        errors.append(f"Invalid JSON: {e}")
    except Exception as e:
        errors.append(f"Read error: {e}")
    return errors


def test_jsonl_file(filepath):
    """Test a single .jsonl file line by line (streaming)."""
    errors = []
    line_count = 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                line_count += 1
                try:
                    json.loads(line)
                except json.JSONDecodeError as e:
                    errors.append(f"Line {line_num}: {e}")
                    if len(errors) >= 10:
                        errors.append(f"... stopping after 10 errors (checked {line_num} lines)")
                        break
    except UnicodeDecodeError as e:
        errors.append(f"Encoding error: {e}")
    except Exception as e:
        errors.append(f"Read error: {e}")
    if line_count == 0 and not errors:
        errors.append("File has no valid JSONL records")
    return errors


def main():
    parser = argparse.ArgumentParser(description="Test JSON/JSONL file integrity")
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

    print(f"=== JSON/JSONL Integrity Test ===")
    print(f"Scanning: {output_dir}\n")

    files = find_json_files(output_dir)
    if not files:
        print(f"WARN: No .json/.jsonl files found in {output_dir}")
        sys.exit(0)

    total = len(files)
    passed = 0
    failed = 0
    results = []

    for filepath in files:
        relpath = os.path.relpath(filepath, output_dir)
        if filepath.endswith(".jsonl"):
            errors = test_jsonl_file(filepath)
        else:
            errors = test_json_file(filepath)

        if errors:
            failed += 1
            status = "FAIL"
            results.append({"file": relpath, "status": "FAIL", "errors": errors})
            print(f"  FAIL  {relpath}")
            for err in errors:
                print(f"        -> {err}")
        else:
            passed += 1
            status = "PASS"
            results.append({"file": relpath, "status": "PASS", "errors": []})
            print(f"  PASS  {relpath}")

    print(f"\n--- Summary ---")
    print(f"Total files: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    overall = "PASS" if failed == 0 else "FAIL"
    print(f"\nOverall: {overall}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
