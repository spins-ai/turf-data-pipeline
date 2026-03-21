#!/usr/bin/env python3
"""
Test date validity in data files.
Validates that all date fields are valid ISO format and within range 2004-2026.
"""
import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime


log = logging.getLogger(__name__)

# Date field names to look for (case-insensitive substrings)
DATE_FIELD_PATTERNS = [
    "date", "jour", "annee", "mois", "created", "updated",
    "naissance", "birth", "debut", "fin", "start", "end",
]

# ISO date patterns
DATE_REGEX_FULL = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$")
DATE_REGEX_SHORT = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_REGEX_SLASH = re.compile(r"^\d{2}/\d{2}/\d{4}$")
DATE_REGEX_YEAR = re.compile(r"^\d{4}$")

MIN_YEAR = 2004
MAX_YEAR = 2026


def is_date_field(field_name):
    """Check if a field name looks like a date field."""
    lower = field_name.lower()
    return any(pattern in lower for pattern in DATE_FIELD_PATTERNS)


def parse_date(value):
    """Try to parse a date string, return (datetime, format_used) or (None, error)."""
    if not isinstance(value, str):
        return None, "not a string"

    value = value.strip()
    if not value:
        return None, "empty"

    # Try ISO format: 2024-01-15
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M",
                "%d/%m/%Y", "%Y"):
        try:
            dt = datetime.strptime(value[:len(value)], fmt)
            return dt, fmt
        except ValueError:
            continue

    # Try partial ISO with timezone
    try:
        # Handle 2024-01-15T10:30:00+01:00
        clean = value
        if "+" in value[10:] or value.endswith("Z"):
            clean = re.sub(r"[+-]\d{2}:\d{2}$", "", value)
            clean = clean.rstrip("Z")
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(clean, fmt)
                return dt, fmt + "+tz"
            except ValueError:
                continue
    except Exception as e:
        log.debug("Error parsing date value '%s': %s", value[:50], e)

    return None, f"unparseable: {value[:50]}"


def validate_date_range(dt):
    """Check if date is within expected range."""
    if dt.year < MIN_YEAR:
        return False, f"year {dt.year} < {MIN_YEAR}"
    if dt.year > MAX_YEAR:
        return False, f"year {dt.year} > {MAX_YEAR}"
    return True, None


def analyze_file(filepath, output_dir):
    """Analyze date fields in a single file."""
    issues = []
    date_fields_found = {}
    record_count = 0

    def check_record(record, line_info=""):
        nonlocal record_count
        if not isinstance(record, dict):
            return
        record_count += 1
        for key, value in record.items():
            if not is_date_field(key):
                continue
            if value is None:
                date_fields_found.setdefault(key, {"valid": 0, "invalid": 0, "null": 0, "out_of_range": 0})
                date_fields_found[key]["null"] += 1
                continue

            date_fields_found.setdefault(key, {"valid": 0, "invalid": 0, "null": 0, "out_of_range": 0})
            dt, fmt_or_error = parse_date(str(value))
            if dt is None:
                date_fields_found[key]["invalid"] += 1
                if date_fields_found[key]["invalid"] <= 3:
                    issues.append(f"{line_info}field '{key}': {fmt_or_error}")
            else:
                in_range, range_error = validate_date_range(dt)
                if not in_range:
                    date_fields_found[key]["out_of_range"] += 1
                    if date_fields_found[key]["out_of_range"] <= 3:
                        issues.append(f"{line_info}field '{key}': {range_error} (value={value})")
                else:
                    date_fields_found[key]["valid"] += 1

    try:
        if filepath.endswith(".jsonl"):
            with open(filepath, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        check_record(record, f"line {line_num}: ")
                    except json.JSONDecodeError:
                        continue
        else:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for i, record in enumerate(data):
                    check_record(record, f"record {i}: ")
            elif isinstance(data, dict):
                for key in ("data", "records", "results", "items", "courses", "partants"):
                    if key in data and isinstance(data[key], list):
                        for i, record in enumerate(data[key]):
                            check_record(record, f"record {i}: ")
                        break
                else:
                    check_record(data)
    except Exception as e:
        issues.append(f"Read error: {e}")

    return date_fields_found, issues, record_count


def main():
    parser = argparse.ArgumentParser(description="Validate date fields in data files")
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

    print(f"=== Date Validity Test ===")
    print(f"Scanning: {output_dir}")
    print(f"Expected range: {MIN_YEAR}-{MAX_YEAR}\n")

    files = []
    for root, _dirs, filenames in os.walk(output_dir):
        for fname in sorted(filenames):
            if fname.endswith(".json") or fname.endswith(".jsonl"):
                files.append(os.path.join(root, fname))

    if not files:
        print(f"WARN: No .json/.jsonl files found in {output_dir}")
        sys.exit(0)

    total_invalid = 0
    total_out_of_range = 0
    files_with_issues = 0

    for filepath in files:
        relpath = os.path.relpath(filepath, output_dir)
        date_fields, issues, record_count = analyze_file(filepath, output_dir)

        if not date_fields:
            # No date fields found, skip silently
            continue

        file_invalid = sum(f["invalid"] for f in date_fields.values())
        file_oor = sum(f["out_of_range"] for f in date_fields.values())
        file_valid = sum(f["valid"] for f in date_fields.values())

        total_invalid += file_invalid
        total_out_of_range += file_oor

        has_issues = file_invalid > 0 or file_oor > 0

        if has_issues:
            files_with_issues += 1
            print(f"  FAIL  {relpath}")
            for field_name, counts in sorted(date_fields.items()):
                total = counts["valid"] + counts["invalid"] + counts["null"] + counts["out_of_range"]
                parts = []
                if counts["valid"]:
                    parts.append(f"{counts['valid']} valid")
                if counts["invalid"]:
                    parts.append(f"{counts['invalid']} invalid")
                if counts["out_of_range"]:
                    parts.append(f"{counts['out_of_range']} out-of-range")
                if counts["null"]:
                    parts.append(f"{counts['null']} null")
                print(f"        {field_name}: {', '.join(parts)}")
            for issue in issues[:5]:
                print(f"        -> {issue}")
        else:
            field_summary = ", ".join(
                f"{k}({v['valid']})" for k, v in sorted(date_fields.items())
            )
            print(f"  PASS  {relpath}  [{field_summary}]")

    print(f"\n--- Summary ---")
    print(f"Total invalid dates: {total_invalid}")
    print(f"Total out-of-range: {total_out_of_range}")
    print(f"Files with issues: {files_with_issues}")

    overall = "PASS" if total_invalid == 0 and total_out_of_range == 0 else "FAIL"
    print(f"\nOverall: {overall}")
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
