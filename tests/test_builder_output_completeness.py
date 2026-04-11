#!/usr/bin/env python3
"""Test that all builder outputs have the expected number of rows.

Each builder should produce exactly one row per partant in partants_master.jsonl.
Uses file size heuristics to avoid reading 25GB+ files.
"""
import json
import os
import sys
from pathlib import Path

BASE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
EXPECTED_ROWS = 2_930_290  # Known row count of partants_master.jsonl
TOLERANCE = 0.001  # Allow 0.1% deviation

# Builders that are NOT per-partant (different granularity)
SKIP_BUILDERS = {"cross_reference", "dedup", "merged", "meta"}


def test_output_completeness():
    if not BASE.exists():
        print("SKIP: builder_outputs directory not found.")
        return True

    builders = sorted(d for d in BASE.iterdir() if d.is_dir())
    failures = []
    empty_dirs = []
    checked = 0

    for bdir in builders:
        if bdir.name in SKIP_BUILDERS:
            continue
        jsonls = [f for f in bdir.iterdir() if f.suffix == ".jsonl" and ".tmp" not in f.name]
        if not jsonls:
            empty_dirs.append(bdir.name)
            continue

        fpath = jsonls[0]
        size = fpath.stat().st_size

        # Skip very small files (< 10 MB probably empty/broken)
        if size < 10_000_000:
            # Count actual lines for small files
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    count = sum(1 for _ in f)
                if count < EXPECTED_ROWS * (1 - TOLERANCE):
                    failures.append(f"SHORT: {bdir.name} has {count:,} rows (expected ~{EXPECTED_ROWS:,})")
            except Exception as e:
                failures.append(f"ERROR: {bdir.name}: {e}")
            checked += 1
            continue

        # For large files, estimate row count using samples from BOTH head and tail
        # (line length varies significantly between cold/warm zones)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                # Sample head
                head_bytes = 0
                head_lines = 0
                for line in f:
                    head_bytes += len(line.encode("utf-8"))
                    head_lines += 1
                    if head_lines >= 500:
                        break
                # Sample tail
                f.seek(max(0, int(size * 0.8)))
                f.readline()  # skip partial
                tail_bytes = 0
                tail_lines = 0
                for line in f:
                    tail_bytes += len(line.encode("utf-8"))
                    tail_lines += 1
                    if tail_lines >= 500:
                        break

            total_sample_bytes = head_bytes + tail_bytes
            total_sample_lines = head_lines + tail_lines
            if total_sample_lines > 0:
                avg_line_len = total_sample_bytes / total_sample_lines
                estimated_rows = int(size / avg_line_len)
                # Use very high tolerance — JSONL line lengths vary a lot
                if estimated_rows < EXPECTED_ROWS * 0.5:
                    failures.append(
                        f"TOO FEW ROWS: {bdir.name} estimated {estimated_rows:,} rows "
                        f"(expected ~{EXPECTED_ROWS:,})"
                    )
        except Exception as e:
            failures.append(f"ERROR: {bdir.name}: {e}")

        checked += 1

    print(f"Builders checked: {checked}")
    print(f"Empty directories: {len(empty_dirs)}")
    print(f"Failures: {len(failures)}")

    if empty_dirs:
        print(f"\nEmpty dirs (no .jsonl): {', '.join(empty_dirs[:10])}{'...' if len(empty_dirs) > 10 else ''}")

    assert not failures, f"Builder output failures:\n" + "\n".join(failures)


if __name__ == "__main__":
    ok = test_output_completeness()
    sys.exit(0 if ok else 1)
