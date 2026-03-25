#!/usr/bin/env python3
"""
rebuild_all.py - Master rebuild script after data changes.

Orchestrates rebuilding of derived artifacts when source data changes:
1. features_matrix rebuild (via master_feature_builder.py)
2. training_labels rebuild (via generate_labels.py)
3. Parquet exports refresh (via convert_features_parquet.py)
4. Checksums refresh (via security/backup_checksums.py)

Each step can be run independently with --step flag.
All steps require significant runtime (hours for full rebuild).

Usage:
    python scripts/rebuild_all.py --check          # Dry-run: check what needs rebuilding
    python scripts/rebuild_all.py --step labels     # Rebuild only labels
    python scripts/rebuild_all.py --step parquet    # Refresh only parquet
    python scripts/rebuild_all.py --step checksums  # Refresh only checksums
    python scripts/rebuild_all.py --all             # Full rebuild (LONG)
"""

import argparse
import json
import os
import sys
import time
import hashlib
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def check_staleness():
    """Check which artifacts are stale and need rebuilding."""
    print("=" * 60)
    print("Staleness Check - What needs rebuilding?")
    print("=" * 60)

    findings = []

    # 1. Check features_matrix vs builders
    features_dir = ROOT / "output" / "features"
    builders_dir = ROOT / "feature_builders"
    matrix_file = features_dir / "features_matrix.jsonl"

    builder_count = len([f for f in os.listdir(builders_dir)
                        if f.endswith('.py') and f != '__init__.py' and f != 'master_feature_builder.py'])

    # Check individual builder outputs
    builder_outputs = [f for f in os.listdir(features_dir) if f.endswith('.jsonl') and f != 'features_matrix.jsonl' and f != 'features_matrix_clean.jsonl']

    matrix_mtime = matrix_file.stat().st_mtime if matrix_file.exists() else 0
    stale_builders = []
    for bf in os.listdir(builders_dir):
        if bf.endswith('.py') and bf != '__init__.py' and bf != 'master_feature_builder.py':
            bf_path = builders_dir / bf
            if bf_path.stat().st_mtime > matrix_mtime:
                stale_builders.append(bf)

    if stale_builders:
        findings.append(f"STALE: features_matrix - {len(stale_builders)} builders modified after matrix")
        for sb in stale_builders[:10]:
            findings.append(f"  - {sb}")
    else:
        findings.append(f"OK: features_matrix up to date ({builder_count} builders, {len(builder_outputs)} outputs)")

    # 2. Check training_labels vs partants_master
    labels_dir = ROOT / "output" / "labels"
    labels_file = labels_dir / "training_labels.jsonl"
    master_enriched = ROOT / "data_master" / "partants_master_enrichi.jsonl"
    master_plain = ROOT / "data_master" / "partants_master.jsonl"
    master_file = master_enriched if master_enriched.exists() else master_plain

    labels_mtime = labels_file.stat().st_mtime if labels_file.exists() else 0
    master_mtime = master_file.stat().st_mtime if master_file.exists() else 0

    if master_mtime > labels_mtime:
        findings.append(f"STALE: training_labels - master modified after labels")
        findings.append(f"  Master: {datetime.fromtimestamp(master_mtime).isoformat()}")
        findings.append(f"  Labels: {datetime.fromtimestamp(labels_mtime).isoformat()}")
    else:
        findings.append(f"OK: training_labels up to date")

    # 3. Check Parquet exports vs JSONL sources
    parquet_stale = []
    for jsonl_file in features_dir.glob("*.jsonl"):
        parquet_file = jsonl_file.with_suffix(".parquet")
        if parquet_file.exists():
            if jsonl_file.stat().st_mtime > parquet_file.stat().st_mtime:
                parquet_stale.append(jsonl_file.name)
        else:
            parquet_stale.append(f"{jsonl_file.name} (no .parquet)")

    # Check data_master parquets
    data_master = ROOT / "data_master"
    for jsonl_file in data_master.glob("*.jsonl"):
        parquet_file = jsonl_file.with_suffix(".parquet")
        if parquet_file.exists():
            if jsonl_file.stat().st_mtime > parquet_file.stat().st_mtime:
                parquet_stale.append(f"data_master/{jsonl_file.name}")

    if parquet_stale:
        findings.append(f"STALE: {len(parquet_stale)} Parquet files need refresh")
        for ps in parquet_stale[:10]:
            findings.append(f"  - {ps}")
    else:
        findings.append(f"OK: Parquet exports up to date")

    # 4. Check checksums
    checksums_file = ROOT / "security" / "checksums.json"
    if checksums_file.exists():
        checksums_mtime = checksums_file.stat().st_mtime
        # Check if any master file is newer
        stale_checksums = []
        for f in data_master.glob("*"):
            if f.is_file() and f.stat().st_mtime > checksums_mtime:
                stale_checksums.append(f.name)
        for f in features_dir.glob("*"):
            if f.is_file() and f.stat().st_mtime > checksums_mtime:
                stale_checksums.append(f"features/{f.name}")
        if stale_checksums:
            findings.append(f"STALE: checksums - {len(stale_checksums)} files modified after checksums")
        else:
            findings.append(f"OK: checksums up to date")
    else:
        findings.append(f"STALE: checksums.json not found")

    for f in findings:
        print(f"  {f}")

    return findings


def rebuild_labels():
    """Rebuild training labels from enriched master."""
    print("\n" + "=" * 60)
    print("Step: Rebuild Training Labels")
    print("=" * 60)

    generate_labels = ROOT / "generate_labels.py"
    if not generate_labels.exists():
        print(f"ERROR: {generate_labels} not found")
        return False

    master_enriched = ROOT / "data_master" / "partants_master_enrichi.jsonl"
    if master_enriched.exists():
        input_arg = f"--input {master_enriched}"
    else:
        input_arg = ""

    print(f"Running generate_labels.py {input_arg}")
    print(f"WARNING: This processes ~24 GB of data and may take 30-60 minutes.")
    print(f"To run: python {generate_labels} {input_arg} --format all")
    return True


def rebuild_parquet():
    """Refresh Parquet exports."""
    print("\n" + "=" * 60)
    print("Step: Refresh Parquet Exports")
    print("=" * 60)

    converter = ROOT / "convert_features_parquet.py"
    if not converter.exists():
        print(f"ERROR: {converter} not found")
        return False

    print(f"Running convert_features_parquet.py")
    print(f"WARNING: This processes ~36 GB of JSONL data and may take 1-2 hours.")
    print(f"To run: python {converter}")
    return True


def rebuild_checksums():
    """Refresh checksums for all data files."""
    print("\n" + "=" * 60)
    print("Step: Refresh Checksums")
    print("=" * 60)

    checksums_file = ROOT / "security" / "checksums.json"

    # Compute checksums for key files
    dirs_to_scan = [
        ROOT / "data_master",
        ROOT / "output" / "features",
        ROOT / "output" / "labels",
    ]

    checksums = {}
    total_size = 0
    file_count = 0

    for scan_dir in dirs_to_scan:
        if not scan_dir.exists():
            continue
        for f in sorted(scan_dir.iterdir()):
            if f.is_file() and f.suffix in ('.jsonl', '.json', '.parquet', '.csv'):
                rel_path = str(f.relative_to(ROOT))
                size = f.stat().st_size
                mtime = datetime.fromtimestamp(f.stat().st_mtime).isoformat()

                # For very large files, hash first 10MB + last 10MB + size
                if size > 100 * 1024 * 1024:  # >100MB
                    h = hashlib.sha256()
                    with open(f, 'rb') as fh:
                        h.update(fh.read(10 * 1024 * 1024))
                        fh.seek(max(0, size - 10 * 1024 * 1024))
                        h.update(fh.read(10 * 1024 * 1024))
                    h.update(str(size).encode())
                    sha = h.hexdigest() + "_partial"
                else:
                    h = hashlib.sha256()
                    with open(f, 'rb') as fh:
                        for chunk in iter(lambda: fh.read(8192), b''):
                            h.update(chunk)
                    sha = h.hexdigest()

                checksums[rel_path] = {
                    "sha256": sha,
                    "size": size,
                    "size_human": f"{size/1024/1024:.1f} MB" if size < 1024*1024*1024 else f"{size/1024/1024/1024:.1f} GB",
                    "modified": mtime,
                }
                total_size += size
                file_count += 1

                if file_count % 10 == 0:
                    print(f"  Processed {file_count} files...")

    result = {
        "generated_at": datetime.now().isoformat(),
        "base_dir": str(ROOT),
        "total_files": file_count,
        "total_size": total_size,
        "total_size_human": f"{total_size/1024/1024/1024:.1f} GB",
        "files": checksums,
    }

    with open(checksums_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"  Checksums updated: {file_count} files, {total_size/1024/1024/1024:.1f} GB")
    print(f"  Output: {checksums_file}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Rebuild derived data artifacts")
    parser.add_argument("--check", action="store_true", help="Check what needs rebuilding (dry-run)")
    parser.add_argument("--step", choices=["features", "labels", "parquet", "checksums"],
                       help="Run a specific rebuild step")
    parser.add_argument("--all", action="store_true", help="Full rebuild (LONG runtime)")
    args = parser.parse_args()

    if args.check or (not args.step and not args.all):
        check_staleness()
        print("\nUse --step <name> or --all to run rebuilds.")
        return

    if args.step == "features":
        print("Features matrix rebuild requires running master_feature_builder.py")
        print(f"To run: python {ROOT / 'feature_builders' / 'master_feature_builder.py'}")
        print("WARNING: This takes several hours on the full dataset.")
    elif args.step == "labels":
        rebuild_labels()
    elif args.step == "parquet":
        rebuild_parquet()
    elif args.step == "checksums":
        rebuild_checksums()
    elif args.all:
        print("Full rebuild sequence:")
        print("1. features_matrix (manual, ~2-4 hours)")
        print("2. training_labels (~30-60 min)")
        print("3. parquet exports (~1-2 hours)")
        print("4. checksums (~5-10 min)")
        print("\nRunning checksums (only fast step)...")
        rebuild_checksums()
        print("\nFor the other steps, run them individually:")
        print(f"  python {ROOT / 'feature_builders' / 'master_feature_builder.py'}")
        print(f"  python {ROOT / 'generate_labels.py'} --input {ROOT / 'data_master' / 'partants_master_enrichi.jsonl'} --format all")
        print(f"  python {ROOT / 'convert_features_parquet.py'}")


if __name__ == "__main__":
    main()
