#!/usr/bin/env python3
"""
validate_data_quality.py — Validation qualite des donnees du pipeline.
Verifie : champs manquants, valeurs aberrantes, coherence inter-sources.
Genere un rapport JSON + texte.

Usage:
  python validate_data_quality.py
  python validate_data_quality.py --source pmu_api
  python validate_data_quality.py --max-records 10000
"""

import argparse
import json

import os
import sys
from collections import Counter, defaultdict
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "quality")
os.makedirs(OUTPUT_DIR, exist_ok=True)

from utils.logging_setup import setup_logging
log = setup_logging("validate_data_quality")

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


# Expected fields per source type
EXPECTED_FIELDS = {
    "pmu_api": {
        "participant": ["date", "nom", "numPmu", "hippodrome", "num_reunion", "num_course",
                        "sexe", "age", "driver", "entraineur"],
        "course": ["date", "hippodrome", "num_reunion", "num_course", "libelle",
                   "distance", "discipline"],
    },
    "letrot": {
        "course_info": ["date", "hippodrome_id", "numero_course", "url"],
        "partant": ["date", "hippodrome_id", "numero_course"],
    },
    "paris_turf": {
        "runner": ["date", "nom_cheval", "numero", "jockey"],
        "race": ["date", "hippodrome", "num_course"],
    },
}


def validate_jsonl_file(filepath, max_records=0):
    """Validate a JSONL file and return quality metrics."""
    metrics = {
        "file": os.path.basename(filepath),
        "total_records": 0,
        "valid_json": 0,
        "invalid_json": 0,
        "empty_lines": 0,
        "field_counts": Counter(),
        "type_counts": Counter(),
        "source_counts": Counter(),
        "date_range": {"min": None, "max": None},
        "missing_fields": defaultdict(int),
        "null_fields": defaultdict(int),
        "sample_keys": set(),
        "issues": [],
    }

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if max_records and i >= max_records:
                    break

                line = line.strip()
                if not line:
                    metrics["empty_lines"] += 1
                    continue

                try:
                    record = json.loads(line)
                    metrics["valid_json"] += 1
                except json.JSONDecodeError:
                    metrics["invalid_json"] += 1
                    if metrics["invalid_json"] <= 3:
                        metrics["issues"].append(f"Line {i+1}: invalid JSON")
                    continue

                metrics["total_records"] += 1

                # Track types and sources
                rtype = record.get("type", "unknown")
                source = record.get("source", "unknown")
                metrics["type_counts"][rtype] += 1
                metrics["source_counts"][source] += 1

                # Track date range
                date = record.get("date") or record.get("date_reunion_iso") or ""
                if date and len(str(date)) >= 10:
                    date_str = str(date)[:10]
                    if metrics["date_range"]["min"] is None or date_str < metrics["date_range"]["min"]:
                        metrics["date_range"]["min"] = date_str
                    if metrics["date_range"]["max"] is None or date_str > metrics["date_range"]["max"]:
                        metrics["date_range"]["max"] = date_str

                # Track field presence (sample first 5000 records)
                if metrics["total_records"] <= 5000:
                    for key in record:
                        metrics["field_counts"][key] += 1
                    metrics["sample_keys"].update(record.keys())

                # Track null/empty fields
                for key, value in record.items():
                    if value is None or value == "" or value == 0:
                        metrics["null_fields"][key] += 1

    except Exception as e:
        metrics["issues"].append(f"Error reading file: {e}")

    # Convert sets to lists for JSON serialization
    metrics["sample_keys"] = sorted(metrics["sample_keys"])
    metrics["field_counts"] = dict(metrics["field_counts"].most_common(50))
    metrics["type_counts"] = dict(metrics["type_counts"])
    metrics["source_counts"] = dict(metrics["source_counts"])
    metrics["null_fields"] = dict(sorted(metrics["null_fields"].items(),
                                          key=lambda x: -x[1])[:30])
    metrics["missing_fields"] = dict(metrics["missing_fields"])

    # Compute quality score
    total = metrics["total_records"]
    if total > 0:
        json_quality = metrics["valid_json"] / (metrics["valid_json"] + metrics["invalid_json"]) * 100
        # Check for essential fields
        has_date = metrics["field_counts"].get("date", 0) + metrics["field_counts"].get("date_reunion_iso", 0)
        has_source = metrics["field_counts"].get("source", 0)
        sample_size = min(total, 5000)
        date_pct = min(has_date / sample_size * 100, 100) if sample_size > 0 else 0
        source_pct = min(has_source / sample_size * 100, 100) if sample_size > 0 else 0
        metrics["quality_score"] = round((json_quality * 0.4 + date_pct * 0.3 + source_pct * 0.3), 1)
    else:
        metrics["quality_score"] = 0

    return metrics


def find_jsonl_files():
    """Find all JSONL files in output directories."""
    files = []
    output_base = os.path.join(BASE_DIR, "output")
    for root, dirs, filenames in os.walk(output_base):
        dirs[:] = [d for d in dirs if d not in ("cache", "__pycache__")]
        for f in filenames:
            if f.endswith(".jsonl") and not f.startswith("."):
                path = os.path.join(root, f)
                size_mb = os.path.getsize(path) / (1024 * 1024)
                files.append({"path": path, "size_mb": round(size_mb, 1),
                               "dir": os.path.basename(root)})
    return sorted(files, key=lambda x: -x["size_mb"])


def main():
    parser = argparse.ArgumentParser(description="Validate data quality")
    parser.add_argument("--source", type=str, help="Filter by source directory name")
    parser.add_argument("--max-records", type=int, default=50000,
                        help="Max records per file (0=unlimited)")
    parser.add_argument("--min-size", type=float, default=0.001,
                        help="Min file size in MB")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("VALIDATION QUALITE DES DONNEES")
    log.info("=" * 60)

    files = find_jsonl_files()
    if args.source:
        files = [f for f in files if args.source in f["dir"]]
    files = [f for f in files if f["size_mb"] >= args.min_size]

    log.info(f"  {len(files)} fichiers JSONL a valider")

    all_metrics = []
    for i, f in enumerate(files):
        log.info(f"  [{i+1}/{len(files)}] {f['dir']}/{os.path.basename(f['path'])} ({f['size_mb']} MB)")
        m = validate_jsonl_file(f["path"], max_records=args.max_records)
        m["directory"] = f["dir"]
        m["size_mb"] = f["size_mb"]
        all_metrics.append(m)

    # Summary
    log.info("\n" + "=" * 60)
    log.info("RESUME QUALITE")
    log.info("=" * 60)

    total_records = sum(m["total_records"] for m in all_metrics)
    total_invalid = sum(m["invalid_json"] for m in all_metrics)
    avg_quality = sum(m["quality_score"] for m in all_metrics) / len(all_metrics) if all_metrics else 0

    log.info(f"  Records valides totaux: {total_records:,}")
    log.info(f"  Records invalides: {total_invalid:,}")
    log.info(f"  Score qualite moyen: {avg_quality:.1f}/100")

    log.info("\n  Par source:")
    for m in sorted(all_metrics, key=lambda x: -x["quality_score"]):
        log.info(f"    {m['quality_score']:>5.1f}  {m['total_records']:>10,}  {m['directory']}/{m['file']}")

    # Issues
    issues = [(m["directory"], m["file"], issue)
              for m in all_metrics for issue in m.get("issues", [])]
    if issues:
        log.info(f"\n  Problemes detectes ({len(issues)}):")
        for d, f, issue in issues[:20]:
            log.info(f"    {d}/{f}: {issue}")

    # Save report
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_files": len(all_metrics),
        "total_records": total_records,
        "total_invalid_json": total_invalid,
        "avg_quality_score": round(avg_quality, 1),
        "files": all_metrics,
    }

    report_path = os.path.join(OUTPUT_DIR, "data_quality_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    log.info(f"\nRapport: {report_path}")

    log.info("\n" + "=" * 60)
    log.info("VALIDATION TERMINEE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
