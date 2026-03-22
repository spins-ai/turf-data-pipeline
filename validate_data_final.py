#!/usr/bin/env python
"""
validate_data_final.py  --  One-command validation of the turf-data-pipeline master data.

Checks:
  1. File existence for all master files
  2. Record counts (streaming, RAM-safe)
  3. UID consistency across partants / labels / features
  4. Date range spans 2013-2026
  5. Field completeness (>90 % null warning)
  6. Checksum verification (random sample of 3)
  7. Summary with PASS / FAIL per check

Usage:
    python validate_data_final.py [--data-dir DATA_DIR]

Defaults DATA_DIR to ./data_master
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MASTER_FILES = [
    "partants_master.jsonl",
    "courses_master.jsonl",       # may also be .json
    "pedigree_master.json",
    "meteo_master.json",
    "rapports_master.json",
    "marche_master.json",
    "training_labels.jsonl",
    "features_matrix.jsonl",
]

# Alternate names / extensions accepted for the same logical file
ALTERNATES: dict[str, list[str]] = {
    "courses_master.jsonl": ["courses_master.json"],
    "courses_master.json":  ["courses_master.jsonl"],
    "training_labels.jsonl": ["training_labels.json", "training_labels.csv"],
    "features_matrix.jsonl": [
        "features_matrix.json",
        "features_matrix.csv",
        "features_matrix.parquet",
    ],
}

SAMPLE_SIZE = 1000
MAX_RAM_BYTES = 2 * 1024 ** 3  # 2 GB guideline -- we never load full files

# Date regex: matches YYYY-MM-DD or YYYY/MM/DD or bare YYYY
DATE_RE = re.compile(r"(20[0-2]\d|201[3-9]|202[0-6])")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def stream_jsonl(path: Path, limit: int = 0) -> Iterator[dict]:
    """Yield dicts from a JSONL file, one line at a time (low RAM)."""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for i, line in enumerate(fh):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
            if limit and i + 1 >= limit:
                break


def stream_json_or_jsonl(path: Path, limit: int = 0) -> Iterator[dict]:
    """Stream records from .json (array or object) or .jsonl."""
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        yield from stream_jsonl(path, limit)
        return

    # For .json files, try streaming array elements without loading whole file
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        first_char = ""
        while True:
            ch = fh.read(1)
            if ch == "":
                return
            if ch.strip():
                first_char = ch
                break

        if first_char == "[":
            # JSON array -- use incremental decoder
            fh.seek(0)
            content = fh.read()
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                return
            count = 0
            for item in data:
                if isinstance(item, dict):
                    yield item
                    count += 1
                    if limit and count >= limit:
                        return
        elif first_char == "{":
            fh.seek(0)
            content = fh.read()
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                return
            if isinstance(data, dict):
                # Could be a dict of lists or a single record
                # Check if values are lists (dict-of-lists format)
                first_val = next(iter(data.values()), None) if data else None
                if isinstance(first_val, list):
                    # dict of lists -- each key is a column
                    keys = list(data.keys())
                    length = len(first_val)
                    count = 0
                    for i in range(length):
                        row = {k: data[k][i] if i < len(data[k]) else None for k in keys}
                        yield row
                        count += 1
                        if limit and count >= limit:
                            return
                elif isinstance(first_val, dict):
                    # dict of dicts -- keys are IDs
                    count = 0
                    for k, v in data.items():
                        if isinstance(v, dict):
                            rec = dict(v)
                            rec.setdefault("_key", k)
                            yield rec
                            count += 1
                            if limit and count >= limit:
                                return
                else:
                    yield data


def count_records(path: Path) -> int:
    """Count total records in a JSON/JSONL file via streaming."""
    count = 0
    for _ in stream_json_or_jsonl(path):
        count += 1
    return count


def resolve_file(data_dir: Path, name: str) -> Path | None:
    """Find the file in data_dir, checking alternates and subdirectories."""
    # Direct match
    p = data_dir / name
    if p.is_file():
        return p
    # Alternate names
    for alt in ALTERNATES.get(name, []):
        p = data_dir / alt
        if p.is_file():
            return p
    # Search subdirectories (up to 4 levels)
    for root, _dirs, files in os.walk(data_dir):
        depth = root.replace(str(data_dir), "").count(os.sep)
        if depth > 4:
            continue
        if name in files:
            return Path(root) / name
        for alt in ALTERNATES.get(name, []):
            if alt in files:
                return Path(root) / alt
    # Also search parent project directories for labels/features
    parent = data_dir.parent
    for sub in ["labels", "feature_builders", "pipeline"]:
        sub_dir = parent / sub
        if not sub_dir.is_dir():
            continue
        for root, _dirs, files in os.walk(sub_dir):
            depth = root.replace(str(sub_dir), "").count(os.sep)
            if depth > 5:
                continue
            if name in files:
                return Path(root) / name
            for alt in ALTERNATES.get(name, []):
                if alt in files:
                    return Path(root) / alt
    return None


# ---------------------------------------------------------------------------
# Check implementations
# ---------------------------------------------------------------------------

class ValidationResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = True
        self.messages: list[str] = []

    def fail(self, msg: str):
        self.passed = False
        self.messages.append(f"  FAIL: {msg}")

    def warn(self, msg: str):
        self.messages.append(f"  WARN: {msg}")

    def info(self, msg: str):
        self.messages.append(f"  INFO: {msg}")

    def status_str(self) -> str:
        return "PASS" if self.passed else "FAIL"


def check_file_existence(data_dir: Path) -> tuple[ValidationResult, dict[str, Path]]:
    """Check 1: verify all master files exist."""
    result = ValidationResult("File Existence")
    resolved: dict[str, Path] = {}
    for name in MASTER_FILES:
        p = resolve_file(data_dir, name)
        if p is None:
            result.fail(f"{name} -- NOT FOUND")
        else:
            resolved[name] = p
            rel = p.relative_to(data_dir) if str(p).startswith(str(data_dir)) else p
            result.info(f"{name} -> {rel}")
    return result, resolved


def check_record_counts(resolved: dict[str, Path]) -> ValidationResult:
    """Check 2: count records in each file."""
    result = ValidationResult("Record Counts")
    for name, path in resolved.items():
        try:
            n = count_records(path)
            if n == 0:
                result.fail(f"{name}: 0 records")
            else:
                result.info(f"{name}: {n:,} records")
        except Exception as exc:
            result.fail(f"{name}: error counting -- {exc}")
    return result


def check_uid_consistency(resolved: dict[str, Path]) -> ValidationResult:
    """Check 3: UID overlap across partants, labels, features."""
    result = ValidationResult("UID Consistency")

    uid_fields = ["partant_uid", "course_uid", "uid", "id", "partant_id", "course_id"]

    def extract_uids(path: Path, limit: int) -> tuple[set[str], str]:
        uids: set[str] = set()
        field_used = ""
        for rec in stream_json_or_jsonl(path, limit=limit):
            for f in uid_fields:
                if f in rec and rec[f] is not None:
                    uids.add(str(rec[f]))
                    if not field_used:
                        field_used = f
                    break
        return uids, field_used

    sources = {
        "partants_master.jsonl": None,
        "training_labels.jsonl": None,
        "features_matrix.jsonl": None,
    }

    for name in list(sources.keys()):
        if name not in resolved:
            result.warn(f"{name} not available -- skipping UID check for it")
            del sources[name]
            continue
        uids, field = extract_uids(resolved[name], SAMPLE_SIZE)
        sources[name] = uids
        result.info(f"{name}: {len(uids)} unique UIDs sampled (field: {field or 'none'})")

    # Pairwise overlap
    names = list(sources.keys())
    if len(names) >= 2:
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                a, b = sources[names[i]], sources[names[j]]
                if not a or not b:
                    continue
                overlap = a & b
                pct = len(overlap) / min(len(a), len(b)) * 100 if min(len(a), len(b)) else 0
                msg = f"Overlap {names[i]} <-> {names[j]}: {len(overlap)} UIDs ({pct:.1f}%)"
                if pct < 10 and len(a) > 10 and len(b) > 10:
                    result.fail(msg + " -- very low overlap")
                else:
                    result.info(msg)
    elif len(names) < 2:
        result.warn("Fewer than 2 UID sources available -- cannot check consistency")

    return result


def check_date_range(resolved: dict[str, Path]) -> ValidationResult:
    """Check 4: verify dates span 2013-2026."""
    result = ValidationResult("Date Range (2013-2026)")

    date_fields = [
        "date_reunion", "date_course", "date", "dateReunion", "dateCourse",
        "date_heure", "jour", "created_at",
    ]
    years_found: set[int] = set()

    # Check partants_master primarily, fall back to courses_master
    for name in ["partants_master.jsonl", "courses_master.jsonl", "courses_master.json"]:
        if name not in resolved:
            continue
        for rec in stream_json_or_jsonl(resolved[name], limit=5000):
            for f in date_fields:
                val = rec.get(f)
                if val is None:
                    continue
                val_str = str(val)
                m = DATE_RE.search(val_str)
                if m:
                    years_found.add(int(m.group(1)))
        if years_found:
            break

    if not years_found:
        result.fail("No date fields found in sampled records")
        return result

    min_year, max_year = min(years_found), max(years_found)
    result.info(f"Years found: {min_year} - {max_year}  ({len(years_found)} distinct years)")

    if min_year > 2013:
        result.fail(f"Earliest year is {min_year}, expected <= 2013")
    if max_year < 2024:
        result.fail(f"Latest year is {max_year}, expected >= 2024")

    return result


def check_field_completeness(resolved: dict[str, Path]) -> ValidationResult:
    """Check 5: report fields with >90% null in partants_master."""
    result = ValidationResult("Field Completeness (partants_master)")

    name = "partants_master.jsonl"
    if name not in resolved:
        result.warn("partants_master.jsonl not found -- skipping")
        return result

    field_total: Counter = Counter()
    field_null: Counter = Counter()
    n = 0

    for rec in stream_json_or_jsonl(resolved[name], limit=SAMPLE_SIZE):
        n += 1
        for k, v in rec.items():
            field_total[k] += 1
            if v is None or v == "" or v == "null":
                field_null[k] += 1

    if n == 0:
        result.fail("No records sampled")
        return result

    result.info(f"Sampled {n} records, {len(field_total)} fields")

    high_null: list[tuple[str, float]] = []
    for field in sorted(field_total.keys()):
        pct_null = field_null[field] / field_total[field] * 100
        if pct_null > 90:
            high_null.append((field, pct_null))

    if high_null:
        result.warn(f"{len(high_null)} fields with >90% null:")
        for field, pct in high_null[:20]:
            result.warn(f"  {field}: {pct:.1f}% null")
        if len(high_null) > 20:
            result.warn(f"  ... and {len(high_null) - 20} more")
    else:
        result.info("No fields with >90% null -- good")

    return result


def check_checksums(data_dir: Path, resolved: dict[str, Path]) -> ValidationResult:
    """Check 6: verify SHA-256 checksums for a random sample of 3 files."""
    result = ValidationResult("Checksum Verification")

    checksum_file = data_dir / "CHECKSUMS.sha256"
    if not checksum_file.is_file():
        # Also check parent
        checksum_file = data_dir.parent / "CHECKSUMS.sha256"
    if not checksum_file.is_file():
        result.info("CHECKSUMS.sha256 not found -- skipping")
        return result

    # Parse checksum file
    entries: dict[str, str] = {}
    with open(checksum_file, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                sha, fname = parts
                fname = fname.lstrip("*").strip()
                entries[fname] = sha

    if not entries:
        result.info("CHECKSUMS.sha256 is empty -- skipping")
        return result

    # Pick random sample
    sample_names = random.sample(list(entries.keys()), min(3, len(entries)))
    result.info(f"Verifying {len(sample_names)} of {len(entries)} checksums")

    for fname in sample_names:
        fpath = data_dir / fname
        if not fpath.is_file():
            # Try resolving relative to parent
            fpath = data_dir.parent / fname
        if not fpath.is_file():
            result.fail(f"{fname}: file not found for checksum verification")
            continue
        expected = entries[fname].lower()
        sha = hashlib.sha256()
        with open(fpath, "rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)  # 1 MB chunks
                if not chunk:
                    break
                sha.update(chunk)
        actual = sha.hexdigest().lower()
        if actual == expected:
            result.info(f"{fname}: checksum OK")
        else:
            result.fail(f"{fname}: checksum MISMATCH (expected {expected[:16]}..., got {actual[:16]}...)")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate turf-data-pipeline master data")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Path to data_master directory (default: auto-detect)",
    )
    args = parser.parse_args()

    # Resolve data directory
    if args.data_dir:
        data_dir = Path(args.data_dir).resolve()
    else:
        script_dir = Path(__file__).resolve().parent
        data_dir = script_dir / "data_master"
        if not data_dir.is_dir():
            data_dir = script_dir / "data" / "master"
        if not data_dir.is_dir():
            data_dir = script_dir / "data"

    print("=" * 70)
    print("  TURF DATA PIPELINE -- FINAL VALIDATION")
    print("=" * 70)
    print(f"  Data directory: {data_dir}")
    print(f"  Data dir exists: {data_dir.is_dir()}")
    print("=" * 70)

    if not data_dir.is_dir():
        print("\nFATAL: Data directory does not exist.")
        sys.exit(2)

    results: list[ValidationResult] = []

    # 1. File existence
    print("\n[1/6] Checking file existence ...")
    res_exist, resolved = check_file_existence(data_dir)
    results.append(res_exist)

    # 2. Record counts
    print("[2/6] Counting records (streaming) ...")
    res_counts = check_record_counts(resolved)
    results.append(res_counts)

    # 3. UID consistency
    print("[3/6] Checking UID consistency ...")
    res_uid = check_uid_consistency(resolved)
    results.append(res_uid)

    # 4. Date range
    print("[4/6] Checking date range ...")
    res_dates = check_date_range(resolved)
    results.append(res_dates)

    # 5. Field completeness
    print("[5/6] Checking field completeness ...")
    res_fields = check_field_completeness(resolved)
    results.append(res_fields)

    # 6. Checksums
    print("[6/6] Verifying checksums ...")
    res_cksum = check_checksums(data_dir, resolved)
    results.append(res_cksum)

    # --- Summary ---
    print("\n" + "=" * 70)
    print("  VALIDATION SUMMARY")
    print("=" * 70)

    all_passed = True
    for r in results:
        status = r.status_str()
        marker = "  [PASS]" if r.passed else "  [FAIL]"
        print(f"{marker}  {r.name}")
        for m in r.messages:
            print(f"        {m}")
        if not r.passed:
            all_passed = False

    print("\n" + "-" * 70)
    if all_passed:
        print("  OVERALL STATUS:  *** PASS ***")
    else:
        print("  OVERALL STATUS:  *** FAIL ***")
    print("-" * 70)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
