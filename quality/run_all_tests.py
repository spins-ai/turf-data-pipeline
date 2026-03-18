#!/usr/bin/env python3
"""
Run all quality tests and generate a report in quality/report.md.
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime


TESTS = [
    {
        "name": "JSON/JSONL Integrity",
        "script": "test_json_integrity.py",
        "description": "Validates all JSON/JSONL files are parseable and not truncated",
    },
    {
        "name": "Zero-Byte Files",
        "script": "test_zero_bytes.py",
        "description": "Finds all 0-byte files that indicate failed writes",
    },
    {
        "name": "Record Counts",
        "script": "test_record_counts.py",
        "description": "Counts records per file and compares with expected minimums",
    },
    {
        "name": "Feature Quality",
        "script": "test_features_quality.py",
        "description": "Checks for NaN/Inf in numeric features and high null rates",
    },
    {
        "name": "Date Validity",
        "script": "test_dates_valid.py",
        "description": "Validates date fields are ISO format and in range 2004-2026",
    },
    {
        "name": "Value Ranges",
        "script": "test_values_range.py",
        "description": "Checks cotes > 0, distances > 0, no invalid negative values",
    },
    {
        "name": "Cross-Source Consistency",
        "script": "test_cross_source.py",
        "description": "Cross-validates records between PMU, Le Trot, and other sources",
    },
]


def run_test(script_path, output_dir):
    """Run a single test script and capture output."""
    cmd = [sys.executable, script_path]
    if output_dir:
        cmd.append(output_dir)

    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min timeout per test
        )
        elapsed = time.time() - start
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "elapsed": elapsed,
        }
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": "TIMEOUT after 300s",
            "elapsed": elapsed,
        }
    except Exception as e:
        elapsed = time.time() - start
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
            "elapsed": elapsed,
        }


def extract_overall(stdout):
    """Extract the Overall: PASS/FAIL/WARN line from test output."""
    for line in stdout.strip().splitlines()[::-1]:
        line = line.strip()
        if line.startswith("Overall:"):
            return line.split(":", 1)[1].strip()
    return "UNKNOWN"


def generate_report(results, output_dir, report_path):
    """Generate a Markdown report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_pass = sum(1 for r in results if r["overall"] == "PASS")
    total_fail = sum(1 for r in results if r["overall"] == "FAIL")
    total_warn = sum(1 for r in results if r["overall"] == "WARN")
    total_other = len(results) - total_pass - total_fail - total_warn

    lines = []
    lines.append(f"# Data Quality Report")
    lines.append(f"")
    lines.append(f"Generated: {now}")
    lines.append(f"Data directory: `{output_dir}`")
    lines.append(f"")
    lines.append(f"## Summary")
    lines.append(f"")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total tests | {len(results)} |")
    lines.append(f"| Passed | {total_pass} |")
    lines.append(f"| Failed | {total_fail} |")
    lines.append(f"| Warnings | {total_warn} |")
    if total_other:
        lines.append(f"| Other | {total_other} |")
    total_time = sum(r["elapsed"] for r in results)
    lines.append(f"| Total time | {total_time:.1f}s |")
    lines.append(f"")

    # Overall status
    if total_fail > 0:
        overall = "FAIL"
    elif total_warn > 0:
        overall = "WARN"
    else:
        overall = "PASS"
    lines.append(f"**Overall: {overall}**")
    lines.append(f"")

    # Quick status table
    lines.append(f"## Test Results")
    lines.append(f"")
    lines.append(f"| Test | Status | Time |")
    lines.append(f"|------|--------|------|")
    for r in results:
        status_icon = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN"}.get(r["overall"], "???")
        lines.append(f"| {r['name']} | {status_icon} | {r['elapsed']:.1f}s |")
    lines.append(f"")

    # Detailed output per test
    lines.append(f"## Details")
    lines.append(f"")
    for r in results:
        lines.append(f"### {r['name']}")
        lines.append(f"")
        lines.append(f"**Status: {r['overall']}** | Time: {r['elapsed']:.1f}s")
        lines.append(f"")
        lines.append(f"{r['description']}")
        lines.append(f"")
        if r["stdout"].strip():
            lines.append(f"```")
            # Limit output to avoid huge reports
            output_lines = r["stdout"].strip().splitlines()
            if len(output_lines) > 80:
                lines.extend(output_lines[:60])
                lines.append(f"... ({len(output_lines) - 60} more lines)")
                lines.extend(output_lines[-10:])
            else:
                lines.extend(output_lines)
            lines.append(f"```")
        if r["stderr"].strip():
            lines.append(f"")
            lines.append(f"**Errors:**")
            lines.append(f"```")
            lines.append(r["stderr"].strip()[:2000])
            lines.append(f"```")
        lines.append(f"")

    report_content = "\n".join(lines)

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    return report_content


def main():
    parser = argparse.ArgumentParser(description="Run all quality tests and generate report")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Directory to scan (default: ../output or backup_20260314)",
    )
    parser.add_argument(
        "--report",
        default=None,
        help="Report output path (default: quality/report.md)",
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Resolve output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
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

    report_path = args.report or os.path.join(script_dir, "report.md")

    print(f"=== Running All Quality Tests ===")
    print(f"Data directory: {output_dir}")
    print(f"Report: {report_path}")
    print(f"")

    results = []
    all_start = time.time()

    for test_info in TESTS:
        script_path = os.path.join(script_dir, test_info["script"])
        if not os.path.isfile(script_path):
            print(f"  SKIP  {test_info['name']}: script not found ({script_path})")
            continue

        print(f"  RUN   {test_info['name']} ...", end="", flush=True)
        result = run_test(script_path, output_dir)
        overall = extract_overall(result["stdout"])

        results.append({
            "name": test_info["name"],
            "script": test_info["script"],
            "description": test_info["description"],
            "overall": overall,
            "returncode": result["returncode"],
            "stdout": result["stdout"],
            "stderr": result["stderr"],
            "elapsed": result["elapsed"],
        })

        print(f" {overall} ({result['elapsed']:.1f}s)")

    total_time = time.time() - all_start

    # Generate report
    generate_report(results, output_dir, report_path)
    print(f"\n--- All Tests Complete ({total_time:.1f}s) ---")
    print(f"Report saved to: {report_path}")

    # Print summary
    total_pass = sum(1 for r in results if r["overall"] == "PASS")
    total_fail = sum(1 for r in results if r["overall"] == "FAIL")
    total_warn = sum(1 for r in results if r["overall"] == "WARN")
    print(f"\nPassed: {total_pass} | Failed: {total_fail} | Warnings: {total_warn}")

    if total_fail > 0:
        print(f"\nOverall: FAIL")
        return 1
    elif total_warn > 0:
        print(f"\nOverall: WARN")
        return 0
    else:
        print(f"\nOverall: PASS")
        return 0


if __name__ == "__main__":
    sys.exit(main())
