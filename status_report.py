#!/usr/bin/env python3
"""
status_report.py — Genere un rapport de statut complet du pipeline.
Verifie : scrapers, output, qualite, pipeline, git.
Sortie : rapport texte + JSON.
"""

import glob
import json
import os
import subprocess
import sys
from datetime import datetime

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
REPORT_FILE = os.path.join(OUTPUT_DIR, "status_report.json")


def count_jsonl_lines(filepath, max_lines=None):
    """Count lines in a JSONL file efficiently."""
    count = 0
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for _ in f:
                count += 1
                if max_lines and count >= max_lines:
                    return count
    except Exception:
        pass
    return count


def get_file_size_mb(filepath):
    try:
        return os.path.getsize(filepath) / (1024 * 1024)
    except Exception:
        return 0


def scan_output_dirs():
    """Scan all output directories and count records."""
    results = []
    if not os.path.isdir(OUTPUT_DIR):
        return results

    for d in sorted(os.listdir(OUTPUT_DIR)):
        full = os.path.join(OUTPUT_DIR, d)
        if not os.path.isdir(full):
            continue

        jsonl_files = glob.glob(os.path.join(full, "*.jsonl"))
        total_lines = 0
        total_size_mb = 0
        file_details = []

        for f in jsonl_files:
            lines = count_jsonl_lines(f)
            size_mb = get_file_size_mb(f)
            total_lines += lines
            total_size_mb += size_mb
            file_details.append({
                "file": os.path.basename(f),
                "lines": lines,
                "size_mb": round(size_mb, 2),
            })

        # Check checkpoint
        checkpoint = None
        cp_file = os.path.join(full, ".checkpoint.json")
        if os.path.exists(cp_file):
            try:
                with open(cp_file, "r", encoding="utf-8") as f:
                    checkpoint = json.load(f)
            except Exception:
                pass

        # Check cache
        cache_dir = os.path.join(full, "cache")
        cache_count = len(glob.glob(os.path.join(cache_dir, "*"))) if os.path.isdir(cache_dir) else 0

        results.append({
            "directory": d,
            "total_records": total_lines,
            "total_size_mb": round(total_size_mb, 2),
            "jsonl_files": len(jsonl_files),
            "file_details": file_details,
            "has_checkpoint": checkpoint is not None,
            "checkpoint": checkpoint,
            "cache_files": cache_count,
        })

    return results


def scan_scrapers():
    """Find all scraper Python files."""
    base = os.path.dirname(os.path.abspath(__file__))
    scrapers = []
    for f in sorted(glob.glob(os.path.join(base, "*_scraper.py"))):
        name = os.path.basename(f)
        scrapers.append(name)
    return scrapers


def main():
    print("=" * 60)
    print("RAPPORT DE STATUT — TURF DATA PIPELINE")
    print(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Scan outputs
    print("\n--- DONNEES PAR SOURCE ---")
    dirs = scan_output_dirs()

    with_data = [d for d in dirs if d["total_records"] > 0]
    empty = [d for d in dirs if d["total_records"] == 0]

    grand_total = sum(d["total_records"] for d in dirs)
    grand_size = sum(d["total_size_mb"] for d in dirs)

    print(f"\nTotal : {grand_total:,} records, {grand_size:,.1f} MB")
    print(f"Dossiers avec donnees : {len(with_data)}")
    print(f"Dossiers vides : {len(empty)}")

    print("\n  Top sources :")
    for d in sorted(with_data, key=lambda x: -x["total_records"])[:20]:
        status = "checkpoint" if d["has_checkpoint"] else ""
        print(f"    {d['total_records']:>12,}  {d['total_size_mb']:>8.1f} MB  {d['directory']}  {status}")

    if empty:
        print(f"\n  Dossiers vides ({len(empty)}) :")
        for d in empty[:20]:
            print(f"    {d['directory']}")

    # Scrapers
    print("\n--- SCRAPERS ---")
    scrapers = scan_scrapers()
    print(f"  {len(scrapers)} fichiers *_scraper.py")

    # Save JSON report
    report = {
        "generated_at": datetime.now().isoformat(),
        "grand_total_records": grand_total,
        "grand_total_size_mb": round(grand_size, 2),
        "dirs_with_data": len(with_data),
        "dirs_empty": len(empty),
        "scrapers_count": len(scrapers),
        "directories": dirs,
        "scrapers": scrapers,
    }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nRapport JSON : {REPORT_FILE}")

    print("\n" + "=" * 60)
    print("FIN DU RAPPORT")
    print("=" * 60)


if __name__ == "__main__":
    main()
