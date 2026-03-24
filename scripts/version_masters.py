#!/usr/bin/env python3
"""
version_masters.py
==================
Creates a versioned registry of all master data files.
Computes SHA256 checksums, file sizes, record counts, and timestamps.

Output: data_master/versions_registry.json

Usage:
    python scripts/version_masters.py
"""

import hashlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_MASTER = BASE_DIR / "data_master"
OUTPUT = DATA_MASTER / "versions_registry.json"

# Files to skip (not real masters)
SKIP = {"CHECKSUMS.sha256", "MANIFEST.json", "versions_registry.json",
        "data_catalog.json", "sources_status.json", "mega_merge_rapport.json"}

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB for hashing


def sha256_file(path: Path) -> str:
    """Compute SHA256 of a file in streaming mode."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def count_records(path: Path) -> int | None:
    """Count records in a JSONL or JSON file. Returns None for non-countable."""
    suffix = path.suffix.lower()
    name = path.name

    if suffix == ".jsonl":
        count = 0
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line:
                    count += 1
        return count

    if suffix == ".json":
        # Try to detect if it's a JSON array
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                first_char = ""
                for ch in f.read(100):
                    if ch.strip():
                        first_char = ch
                        break
                if first_char == "[":
                    # Count top-level array items by streaming
                    f.seek(0)
                    data = json.load(f)
                    if isinstance(data, list):
                        return len(data)
                    elif isinstance(data, dict):
                        return 1
        except (json.JSONDecodeError, MemoryError):
            return None

    if suffix == ".csv":
        count = -1  # subtract header
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for _ in f:
                count += 1
        return max(count, 0)

    return None


def format_size(size_bytes: int) -> str:
    """Human-readable file size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def main():
    print("=" * 60)
    print("VERSION MASTERS REGISTRY")
    print("=" * 60)

    if not DATA_MASTER.exists():
        print(f"ERROR: {DATA_MASTER} not found")
        sys.exit(1)

    # Gather all files
    all_files = sorted(DATA_MASTER.iterdir())
    data_files = [f for f in all_files if f.is_file() and f.name not in SKIP]

    # Group by base name (stem without extension)
    groups = {}
    for f in data_files:
        stem = f.stem
        # Handle double extensions like .jsonl.tmp
        if stem.endswith(".jsonl"):
            continue
        if f.name.startswith("."):
            continue
        groups.setdefault(stem, []).append(f)

    registry = {
        "version": "1.0.0",
        "generated_at": datetime.now().isoformat(),
        "generator": "scripts/version_masters.py",
        "total_files": len(data_files),
        "total_size_bytes": 0,
        "masters": {}
    }

    total_size = 0

    for f in sorted(data_files, key=lambda x: x.name):
        stat = f.stat()
        size = stat.st_size
        total_size += size
        mtime = datetime.fromtimestamp(stat.st_mtime).isoformat()

        print(f"\n  Processing {f.name} ({format_size(size)})...")

        # Skip hash for very large files (>5 GB) to save time
        if size > 5 * 1024**3:
            sha = "SKIPPED_TOO_LARGE"
            print(f"    SHA256: skipped (file > 5 GB)")
        else:
            sha = sha256_file(f)
            print(f"    SHA256: {sha[:16]}...")

        # Count records for small-to-medium files only
        records = None
        if size < 2 * 1024**3 and f.suffix in (".jsonl", ".json", ".csv"):
            print(f"    Counting records...")
            records = count_records(f)
            if records is not None:
                print(f"    Records: {records:,}")

        entry = {
            "file": f.name,
            "size_bytes": size,
            "size_human": format_size(size),
            "modified_at": mtime,
            "sha256": sha,
        }
        if records is not None:
            entry["records"] = records

        # Detect format
        entry["format"] = f.suffix.lstrip(".")

        registry["masters"][f.name] = entry

    registry["total_size_bytes"] = total_size
    registry["total_size_human"] = format_size(total_size)

    # Write registry
    with open(OUTPUT, "w", encoding="utf-8") as fout:
        json.dump(registry, fout, indent=2, ensure_ascii=False)

    print(f"\n{'=' * 60}")
    print(f"Registry written to {OUTPUT}")
    print(f"Total files: {len(data_files)}")
    print(f"Total size: {format_size(total_size)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
