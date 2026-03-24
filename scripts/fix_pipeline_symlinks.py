#!/usr/bin/env python3
"""
fix_pipeline_symlinks.py
========================
Fixes pipeline data reference files that point to old Mac paths.
Updates them to use relative paths from the project root.

Old: /Users/quentinherve/models hybride/output/07_cotes_marche
New: output/07_cotes_marche

Also validates that all referenced directories exist.

Usage:
    python scripts/fix_pipeline_symlinks.py          # dry-run
    python scripts/fix_pipeline_symlinks.py --execute # apply fixes
"""

import argparse
import os
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
PIPELINE_DIR = BASE_DIR / "pipeline"

# Old path prefixes to replace
OLD_PREFIXES = [
    "/Users/quentinherve/models hybride/",
    "/Users/quentinherve/models_hybride/",
]


def find_data_refs():
    """Find all data reference files in pipeline/."""
    refs = []
    for root, dirs, files in os.walk(PIPELINE_DIR):
        # Skip __pycache__
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for fname in files:
            if fname.startswith("__") or fname.endswith(".py") or fname.endswith(".pyc"):
                continue
            fpath = Path(root) / fname
            if fpath.stat().st_size < 200:  # These files should be small
                refs.append(fpath)
    return refs


def fix_path(old_content: str) -> str | None:
    """Convert old Mac path to relative path. Returns None if no fix needed."""
    content = old_content.strip()
    for prefix in OLD_PREFIXES:
        if content.startswith(prefix):
            relative = content[len(prefix):]
            return relative
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Apply fixes")
    args = parser.parse_args()

    print("=" * 60)
    print("FIX PIPELINE DATA REFERENCES")
    print("=" * 60)

    refs = find_data_refs()
    print(f"\nFound {len(refs)} data reference files in pipeline/")

    fixes_needed = []
    already_ok = []
    broken_targets = []

    for ref_path in sorted(refs):
        with open(ref_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read().strip()

        new_path = fix_path(content)
        if new_path is None:
            # Check if it's already a relative path
            if content.startswith("output/") or content.startswith("data_master/"):
                target = BASE_DIR / content
                if target.exists():
                    already_ok.append((ref_path, content))
                else:
                    broken_targets.append((ref_path, content, "target not found"))
            else:
                # Unknown format
                broken_targets.append((ref_path, content, "unknown path format"))
            continue

        # Check if target exists
        target = BASE_DIR / new_path
        exists = target.exists()

        rel = ref_path.relative_to(BASE_DIR)
        fixes_needed.append((ref_path, content, new_path, exists))

        status = "OK" if exists else "MISSING"
        print(f"  [{status}] {rel}")
        print(f"         old: {content}")
        print(f"         new: {new_path}")

    print(f"\nSummary:")
    print(f"  Already OK:    {len(already_ok)}")
    print(f"  Fixes needed:  {len(fixes_needed)}")
    print(f"  Targets OK:    {sum(1 for _, _, _, e in fixes_needed if e)}")
    print(f"  Targets MISS:  {sum(1 for _, _, _, e in fixes_needed if not e)}")
    print(f"  Broken refs:   {len(broken_targets)}")

    if broken_targets:
        print(f"\n  Broken references:")
        for ref_path, content, reason in broken_targets:
            rel = ref_path.relative_to(BASE_DIR)
            print(f"    {rel}: {reason} ({content})")

    if not args.execute:
        print(f"\n  DRY-RUN: no changes made. Use --execute to apply.")
        return

    # Apply fixes
    fixed = 0
    for ref_path, old_content, new_path, exists in fixes_needed:
        with open(ref_path, "w", encoding="utf-8") as f:
            f.write(new_path + "\n")
        fixed += 1

    print(f"\n  EXECUTED: {fixed} files updated.")


if __name__ == "__main__":
    main()
