#!/usr/bin/env python3
"""
Generate a comprehensive inventory report of all cache directories.
Identifies data goldmines: caches with rich data not fully exported to JSONL.
"""
import json
import os
import sys

BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")


def count_jsonl_lines(filepath):
    """Count lines in a JSONL file."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            return sum(1 for _ in f)
    except IOError:
        return 0


def get_jsonl_keys(filepath):
    """Get keys from first line of JSONL."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            line = f.readline().strip()
            if line:
                data = json.loads(line)
                if isinstance(data, dict):
                    return set(data.keys())
    except (IOError, json.JSONDecodeError):
        pass
    return set()


def sample_cache_keys(cache_dir, max_files=3):
    """Sample cache files and return all keys found."""
    all_keys = set()
    files = [f for f in os.listdir(cache_dir) if f.endswith(".json")][:max_files]
    for fn in files:
        try:
            with open(os.path.join(cache_dir, fn), "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, dict):
                all_keys.update(data.keys())
            elif isinstance(data, list) and data and isinstance(data[0], dict):
                all_keys.update(data[0].keys())
        except (IOError, json.JSONDecodeError):
            pass
    return all_keys


def main():
    dirs = sorted(os.listdir(BASE))

    print("=" * 90)
    print("CACHE INVENTORY REPORT")
    print("=" * 90)

    goldmines = []

    for d in dirs:
        cache_dir = os.path.join(BASE, d, "cache")
        if not os.path.isdir(cache_dir):
            continue

        cache_files = [f for f in os.listdir(cache_dir) if f.endswith(".json")]
        n_cache = len(cache_files)
        if n_cache == 0:
            continue

        parent = os.path.join(BASE, d)
        jsonl_files = [f for f in os.listdir(parent) if f.endswith(".jsonl")]

        # Sample cache keys
        cache_keys = sample_cache_keys(cache_dir)

        # Get JSONL keys
        jsonl_key_sets = {}
        for jf in jsonl_files:
            jpath = os.path.join(parent, jf)
            jsonl_key_sets[jf] = get_jsonl_keys(jpath)

        all_jsonl_keys = set()
        for ks in jsonl_key_sets.values():
            all_jsonl_keys.update(ks)

        # Find missing keys (rough - just top-level comparison)
        missing = cache_keys - all_jsonl_keys
        # Filter out metadata/internal keys
        ignore = {"cached", "timezoneOffset", "spritesCasaques", "spriteCasaques"}
        missing -= ignore

        status = "OK" if not missing or not jsonl_files else "MISSING FIELDS"
        if not jsonl_files:
            status = "NO JSONL"

        print(f"\n{'-' * 90}")
        print(f"  {d}")
        print(f"    Cache files: {n_cache:,}")
        print(f"    JSONL files: {', '.join(jsonl_files) if jsonl_files else 'NONE'}")
        print(f"    Cache keys:  {len(cache_keys)} top-level")
        print(f"    Status:      {status}")

        if missing and len(missing) > 0:
            print(f"    Missing:     {sorted(missing)[:15]}")
            if len(missing) > 15:
                print(f"                 ...and {len(missing)-15} more")

        if status in ("NO JSONL", "MISSING FIELDS") and n_cache >= 100:
            goldmines.append((d, n_cache, len(missing), status))

    print(f"\n\n{'=' * 90}")
    print("DATA GOLDMINES (rich cache, poor/no JSONL, 100+ files)")
    print("=" * 90)
    goldmines.sort(key=lambda x: x[1], reverse=True)
    for d, n, nm, st in goldmines:
        print(f"  {d:40s} {n:>8,} files  {nm:>3} missing fields  [{st}]")


if __name__ == "__main__":
    main()
