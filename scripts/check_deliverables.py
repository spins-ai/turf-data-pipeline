#!/usr/bin/env python3
"""
Deliverables Checker — Verify all key project deliverables exist.
Checks:
  - data_master/ files (parquet master tables)
  - output/features/ (feature files)
  - output/labels/ (label files)
  - docs/ (documentation)
  - config/ (pipeline configuration)
  - schemas/ (JSON schemas)
  - scripts/ (utility scripts)
  - tests/ (test files)
Reports missing items and overall completeness.
"""

import os
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Expected deliverables
# ---------------------------------------------------------------------------

EXPECTED = {
    "data_master": {
        "path": os.path.join(BASE_DIR, "data_master"),
        "required_files": [
            "partants_master.parquet",
            "courses_master.parquet",
            "pedigree_master.parquet",
            "meteo_master.parquet",
            "rapports_master.parquet",
            "equipements_master.parquet",
            "marche_master.parquet",
            "horse_career_stats.parquet",
            "jockey_stats.parquet",
            "trainer_stats.parquet",
            "course_profiles.parquet",
        ],
        "min_files": 8,
    },
    "output/features": {
        "path": os.path.join(BASE_DIR, "output", "features"),
        "required_files": [
            "features_matrix.parquet",
        ],
        "required_extensions": [".parquet", ".jsonl", ".csv"],
        "min_files": 3,
    },
    "output/labels": {
        "path": os.path.join(BASE_DIR, "output", "labels"),
        "required_files": [
            "labels.parquet",
        ],
        "required_extensions": [".parquet", ".csv"],
        "min_files": 2,
    },
    "docs": {
        "path": os.path.join(BASE_DIR, "docs"),
        "required_extensions": [".json", ".md"],
        "min_files": 5,
    },
    "config": {
        "path": os.path.join(BASE_DIR, "config"),
        "required_files": [
            "pipeline_config.yaml",
        ],
        "min_files": 1,
    },
    "schemas": {
        "path": os.path.join(BASE_DIR, "schemas"),
        "required_files": [
            "partant_schema.json",
            "course_schema.json",
            "label_schema.json",
        ],
        "min_files": 3,
    },
    "scripts": {
        "path": os.path.join(BASE_DIR, "scripts"),
        "required_extensions": [".py"],
        "min_files": 10,
    },
    "tests": {
        "path": os.path.join(BASE_DIR, "tests"),
        "required_extensions": [".py"],
        "min_files": 1,
    },
}


def check_deliverable(name, spec):
    """Check a single deliverable category. Returns dict with results."""
    path = spec["path"]
    result = {
        "name": name,
        "path": path,
        "exists": os.path.isdir(path),
        "required_files_found": [],
        "required_files_missing": [],
        "total_files": 0,
        "min_files_expected": spec.get("min_files", 0),
        "status": "FAIL",
    }

    if not result["exists"]:
        result["status"] = "FAIL"
        result["reason"] = "directory does not exist"
        return result

    # List files
    try:
        all_files = os.listdir(path)
        # Filter to actual files (not dirs)
        files = [f for f in all_files if os.path.isfile(os.path.join(path, f))]
        result["total_files"] = len(files)
    except OSError as e:
        result["status"] = "FAIL"
        result["reason"] = f"cannot list directory: {e}"
        return result

    # Check required files
    required = spec.get("required_files", [])
    for rf in required:
        if rf in files:
            result["required_files_found"].append(rf)
        else:
            result["required_files_missing"].append(rf)

    # Check required extensions
    required_ext = spec.get("required_extensions", [])
    if required_ext:
        ext_found = set()
        for f in files:
            _, ext = os.path.splitext(f)
            if ext in required_ext:
                ext_found.add(ext)
        result["extensions_found"] = sorted(ext_found)
        result["extensions_missing"] = sorted(set(required_ext) - ext_found)

    # Determine status
    min_ok = result["total_files"] >= result["min_files_expected"]
    required_ok = len(result["required_files_missing"]) == 0
    ext_ok = len(result.get("extensions_missing", [])) == 0

    if min_ok and required_ok and ext_ok:
        result["status"] = "PASS"
    elif min_ok and (required_ok or ext_ok):
        result["status"] = "WARN"
    else:
        result["status"] = "FAIL"

    # File sizes for required files
    result["file_sizes"] = {}
    for rf in result["required_files_found"]:
        fp = os.path.join(path, rf)
        try:
            size = os.path.getsize(fp)
            result["file_sizes"][rf] = size
        except OSError:
            result["file_sizes"][rf] = -1

    return result


def main():
    import json

    print("=" * 60)
    print("DELIVERABLES CHECKER")
    print(f"  Base dir: {BASE_DIR}")
    print(f"  Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    results = {
        "timestamp": datetime.now().isoformat(),
        "base_dir": BASE_DIR,
        "checks": [],
        "summary": {"pass": 0, "warn": 0, "fail": 0},
    }

    for name, spec in EXPECTED.items():
        print(f"\n[CHECK] {name}/")
        r = check_deliverable(name, spec)
        results["checks"].append(r)

        status_icon = {"PASS": "OK", "WARN": "!!", "FAIL": "XX"}[r["status"]]
        print(f"  [{status_icon}] {r['status']} — "
              f"{r['total_files']} files (min {r['min_files_expected']})")

        if r.get("required_files_found"):
            print(f"      Required found: {', '.join(r['required_files_found'])}")
        if r.get("required_files_missing"):
            print(f"      Required MISSING: {', '.join(r['required_files_missing'])}")
        if r.get("extensions_missing"):
            print(f"      Extensions MISSING: {', '.join(r['extensions_missing'])}")
        if not r["exists"]:
            print(f"      Directory does not exist: {r['path']}")

        results["summary"][r["status"].lower()] += 1

    # Overall summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total = len(results["checks"])
    s = results["summary"]
    print(f"  Total checks: {total}")
    print(f"  PASS: {s['pass']}")
    print(f"  WARN: {s['warn']}")
    print(f"  FAIL: {s['fail']}")

    overall = "PASS" if s["fail"] == 0 and s["warn"] == 0 else (
        "WARN" if s["fail"] == 0 else "FAIL"
    )
    results["overall"] = overall
    print(f"\n  Overall: {overall}")

    # Save report
    report_dir = os.path.join(BASE_DIR, "output", "quality")
    os.makedirs(report_dir, exist_ok=True)
    report_file = os.path.join(report_dir, "deliverables_check.json")
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nReport saved: {report_file}")

    # Exit code
    if overall == "FAIL":
        sys.exit(1)
    elif overall == "WARN":
        sys.exit(0)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
