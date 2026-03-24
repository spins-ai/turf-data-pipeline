#!/usr/bin/env python3
"""
Cleanup and verification tasks:
1. List corrupted cache files (size-based check + sampled JSON check)
2. Delete temp files in output/
3. Verify categorical field values
4. Sample 100 records for manual check
"""

import os
import sys
import json
import random
import subprocess
from collections import defaultdict
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE / "output"
DATA_MASTER = BASE / "data_master" / "partants_master.jsonl"
QUALITY_DIR = OUTPUT_DIR / "quality"


def task1():
    """Scan cache dirs for corrupted files using size checks + sampled content check."""
    print("=" * 70)
    print("TASK 1: Corrupted cache files scan")
    print("=" * 70)
    sys.stdout.flush()

    cache_dirs = sorted(OUTPUT_DIR.glob("*/cache"))
    total_corrupted = 0
    dir_results = []

    for cache_dir in cache_dirs:
        scraper_name = cache_dir.parent.name
        zero_bytes = 0
        invalid_json = 0
        suspiciously_small = 0
        total_files = 0
        invalid_examples = []
        small_examples = []

        # Phase 1: Fast size scan using os.scandir (no file open)
        entries = []
        try:
            for entry in os.scandir(str(cache_dir)):
                if entry.is_file(follow_symlinks=False):
                    total_files += 1
                    try:
                        size = entry.stat().st_size
                    except OSError:
                        continue

                    if size == 0:
                        zero_bytes += 1
                    elif size < 10:
                        suspiciously_small += 1
                        if len(small_examples) < 3:
                            small_examples.append((entry.name, size))
                    else:
                        entries.append(entry)
        except OSError:
            continue

        # Phase 2: Sample up to 200 files for JSON validity check
        sample_entries = entries if len(entries) <= 200 else random.sample(entries, 200)
        invalid_in_sample = 0
        for entry in sample_entries:
            try:
                with open(entry.path, "rb") as fh:
                    b = fh.read(1)
                    if b and b not in (b"{", b"["):
                        invalid_in_sample += 1
                        if len(invalid_examples) < 3:
                            invalid_examples.append((entry.name, repr(b)))
            except Exception:
                invalid_in_sample += 1

        # Extrapolate invalid JSON count
        if len(sample_entries) > 0:
            ratio = invalid_in_sample / len(sample_entries)
            invalid_json = round(ratio * len(entries))
        else:
            invalid_json = 0

        count = zero_bytes + invalid_json + suspiciously_small
        total_corrupted += count
        dir_results.append({
            "name": scraper_name,
            "total_files": total_files,
            "zero_bytes": zero_bytes,
            "invalid_json": invalid_json,
            "invalid_in_sample": invalid_in_sample,
            "sample_size": len(sample_entries),
            "suspiciously_small": suspiciously_small,
            "total_issues": count,
            "invalid_examples": invalid_examples,
            "small_examples": small_examples,
        })
        print(f"  {scraper_name}: {total_files} files, {count} issues"
              + (f" (sampled {len(sample_entries)}/{len(entries)} for JSON check)" if len(entries) > 200 else ""))
        sys.stdout.flush()

    print(f"\nScanned {len(cache_dirs)} cache directories, ~{sum(d['total_files'] for d in dir_results):,} total files")
    print(f"Total corrupted/suspicious files: {total_corrupted}\n")

    issues = [d for d in dir_results if d["total_issues"] > 0]
    if issues:
        print(f"{'Scraper Directory':<40} {'Files':>8} {'0-byte':>8} {'Bad JSON':>10} {'<10b':>6} {'Total':>7}")
        print("-" * 83)
        for d in sorted(issues, key=lambda x: -x["total_issues"]):
            note = " (est.)" if d["sample_size"] < d["total_files"] and d["invalid_json"] > 0 else ""
            print(f"{d['name']:<40} {d['total_files']:>8} {d['zero_bytes']:>8} {d['invalid_json']:>10}{note:>5} {d['suspiciously_small']:>6} {d['total_issues']:>7}")

        has_invalid = [d for d in issues if d["invalid_examples"]]
        if has_invalid:
            print("\nExamples of invalid JSON (first byte not { or [):")
            for d in has_invalid:
                for fname, fb in d["invalid_examples"]:
                    print(f"  {d['name']}/cache/{fname} -> {fb}")

        has_small = [d for d in issues if d["small_examples"]]
        if has_small:
            print("\nExamples of small files (<10 bytes):")
            for d in has_small:
                for fname, sz in d["small_examples"]:
                    print(f"  {d['name']}/cache/{fname} -> {sz} bytes")
    else:
        print("No corrupted cache files found!")

    return total_corrupted


def task2():
    """Delete .tmp, .bak, .partial files in output/."""
    print("\n" + "=" * 70)
    print("TASK 2: Delete temp files in output/")
    print("=" * 70)
    sys.stdout.flush()

    extensions = {".tmp", ".bak", ".partial"}
    deleted_files = []

    for root, dirs, files in os.walk(str(OUTPUT_DIR)):
        for fname in files:
            _, ext = os.path.splitext(fname)
            if ext in extensions:
                fpath = os.path.join(root, fname)
                try:
                    size = os.path.getsize(fpath)
                    rel = os.path.relpath(fpath, str(OUTPUT_DIR))
                    os.unlink(fpath)
                    deleted_files.append((rel, size, ext))
                except OSError as e:
                    print(f"  Error deleting {fpath}: {e}")

    if deleted_files:
        print(f"\nDeleted {len(deleted_files)} temp files:")
        for rel, size, ext in sorted(deleted_files):
            print(f"  [{ext}] {rel} ({size:,} bytes)")
        total_size = sum(s for _, s, _ in deleted_files)
        print(f"\nTotal freed: {total_size:,} bytes")
    else:
        print("\nNo .tmp, .bak, or .partial files found in output/")

    return deleted_files


def task3():
    """Verify categorical field values from 5000-record sample."""
    print("\n" + "=" * 70)
    print("TASK 3: Categorical field values (5000 record sample)")
    print("=" * 70)
    sys.stdout.flush()

    all_fields = ["discipline", "sexe", "race", "robe", "type_piste",
                  "pgr_sexe", "pgr_race", "pgr_robe"]

    total_lines = 2930290
    sample_size = 5000
    random.seed(42)
    sample_indices = set(random.sample(range(total_lines), sample_size))

    field_values = defaultdict(lambda: defaultdict(int))

    print(f"Sampling {sample_size} records from {total_lines:,} total...")
    sys.stdout.flush()

    sampled = 0
    with open(DATA_MASTER, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i in sample_indices:
                try:
                    record = json.loads(line)
                    for field in all_fields:
                        val = record.get(field)
                        if val is not None:
                            field_values[field][str(val)] += 1
                        else:
                            field_values[field]["<NULL>"] += 1
                except json.JSONDecodeError:
                    pass
                sampled += 1
                if sampled >= sample_size:
                    break
            if i % 1000000 == 0 and i > 0:
                print(f"  ... line {i:,}, sampled {sampled}/{sample_size}")
                sys.stdout.flush()

    print(f"Sampled {sampled} records.\n")

    for field in all_fields:
        values = field_values[field]
        if not values:
            print(f"--- {field} --- (no values found)")
            continue

        print(f"--- {field} ---  ({len(values)} unique values)")
        for val, count in sorted(values.items(), key=lambda x: -x[1]):
            pct = count / sampled * 100
            flag = ""
            if val != "<NULL>":
                if field == "discipline" and val.lower() not in (
                    "plat", "trot_attele", "trot_monte", "obstacle",
                    "haies", "steeplechase", "cross", "trot", "galop",
                    "steeple-chase", "course_de_haies",
                ):
                    flag = " ** UNEXPECTED"
                elif field in ("sexe", "pgr_sexe") and val.upper() not in (
                    "M", "F", "H", "MALE", "FEMELLE", "HONGRE",
                ):
                    flag = " ** UNEXPECTED"
            print(f"    {val!r:40s} : {count:>5} ({pct:5.1f}%){flag}")
        print()


def task4():
    """Sample 100 records and save for manual review."""
    print("=" * 70)
    print("TASK 4: Sample 100 records for manual check")
    print("=" * 70)
    sys.stdout.flush()

    total_lines = 2930290
    random.seed(123)
    sample_100_indices = set(random.sample(range(total_lines), 100))
    sample_records = []

    with open(DATA_MASTER, "r", encoding="utf-8") as f:
        collected = 0
        for i, line in enumerate(f):
            if i in sample_100_indices:
                try:
                    sample_records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
                collected += 1
                if collected >= 100:
                    break

    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    output_file = QUALITY_DIR / "sample_100_records.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(sample_records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(sample_records)} records to {output_file}")
    print(f"File size: {output_file.stat().st_size:,} bytes")

    years = defaultdict(int)
    disciplines = defaultdict(int)
    hippos = defaultdict(int)
    for r in sample_records:
        date = r.get("date_reunion_iso", "")
        if date:
            years[date[:4]] += 1
        disciplines[r.get("discipline", "<NULL>")] += 1
        hippos[r.get("hippodrome_normalise", "<NULL>")] += 1

    print(f"\nSample distribution:")
    print(f"  Years: {dict(sorted(years.items()))}")
    print(f"  Disciplines: {dict(sorted(disciplines.items(), key=lambda x: -x[1]))}")
    print(f"  Unique hippodromes: {len(hippos)}")


if __name__ == "__main__":
    random.seed(42)
    task1()
    task2()
    task3()
    task4()
    print("\n" + "=" * 70)
    print("ALL TASKS COMPLETE")
    print("=" * 70)
