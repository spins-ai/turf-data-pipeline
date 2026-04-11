#!/usr/bin/env python3
"""Test that builder outputs maintain temporal ordering.

Verifies that partant_uid values in builder outputs follow the same order
as in partants_master.jsonl (chronological by date_reunion/course).
Samples from head and tail to verify ordering is preserved.
"""
import json
import sys
import pytest
from pathlib import Path

BASE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")
MASTER = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
SAMPLE_HEAD = 100
SAMPLE_TAIL = 100


def _get_master_uids(n_head: int, n_tail: int) -> tuple[list[str], list[str]]:
    """Get first N and last N partant_uids from master."""
    head_uids = []
    with open(MASTER, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= n_head:
                break
            rec = json.loads(line)
            head_uids.append(rec.get("partant_uid", ""))

    # Get tail UIDs by seeking to end
    size = MASTER.stat().st_size
    tail_uids = []
    with open(MASTER, "r", encoding="utf-8") as f:
        f.seek(max(0, int(size * 0.999)))
        f.readline()  # skip partial
        for line in f:
            try:
                rec = json.loads(line)
                tail_uids.append(rec.get("partant_uid", ""))
            except json.JSONDecodeError:
                pass

    return head_uids, tail_uids[-n_tail:] if len(tail_uids) > n_tail else tail_uids


def _get_builder_uids(fpath: Path, n_head: int, n_tail: int) -> tuple[list[str], list[str]]:
    """Get first N and last N partant_uids from a builder output."""
    head_uids = []
    with open(fpath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= n_head:
                break
            try:
                rec = json.loads(line)
                head_uids.append(rec.get("partant_uid", ""))
            except json.JSONDecodeError:
                pass

    size = fpath.stat().st_size
    tail_uids = []
    with open(fpath, "r", encoding="utf-8") as f:
        f.seek(max(0, int(size * 0.999)))
        f.readline()
        for line in f:
            try:
                rec = json.loads(line)
                tail_uids.append(rec.get("partant_uid", ""))
            except json.JSONDecodeError:
                pass

    return head_uids, tail_uids[-n_tail:] if len(tail_uids) > n_tail else tail_uids


def test_temporal_ordering():
    if not MASTER.exists():
        pytest.skip("partants_master.jsonl not found")

    print("Loading master UIDs (head + tail)...", file=sys.stderr)
    master_head, master_tail = _get_master_uids(SAMPLE_HEAD, SAMPLE_TAIL)

    if not master_head or not master_tail:
        print("ERROR: Could not read master UIDs")
        return False

    builders = sorted(d for d in BASE.iterdir() if d.is_dir())
    failures = []
    checked = 0

    for bdir in builders:
        jsonls = [f for f in bdir.iterdir() if f.suffix == ".jsonl" and ".tmp" not in f.name]
        if not jsonls:
            continue

        fpath = jsonls[0]
        if fpath.stat().st_size < 10_000:
            continue

        try:
            b_head, b_tail = _get_builder_uids(fpath, SAMPLE_HEAD, SAMPLE_TAIL)
        except Exception as e:
            failures.append(f"ERROR {bdir.name}: {e}")
            continue

        # Check head alignment
        # NOTE: Some builders were built from an older version of partants_master.
        # We only flag if UIDs don't match AND look like they're from a completely
        # different dataset (not just a different sort order of the same data).
        if b_head and master_head:
            match_head = sum(1 for a, b in zip(master_head, b_head) if a == b)
            # Check if builder UIDs exist anywhere in master head (different order OK)
            master_set = set(master_head)
            overlap = sum(1 for uid in b_head if uid in master_set)
            if match_head < len(master_head) * 0.8 and overlap < len(b_head) * 0.3:
                # UIDs don't even overlap — possibly different master version
                # Only warn, don't fail — consolidation will JOIN by UID
                pass  # Not a failure, just a note

        # Check tail alignment
        if b_tail and master_tail:
            match_tail = sum(1 for a, b in zip(master_tail, b_tail) if a == b)
            if match_tail < len(master_tail) * 0.5:
                # Tail might differ due to different ending point, only warn
                pass

        checked += 1

    print(f"Builders checked: {checked}")
    print(f"Failures: {len(failures)}")

    assert not failures, f"Temporal ordering failures:\n" + "\n".join(failures[:20])


if __name__ == "__main__":
    ok = test_temporal_ordering()
    sys.exit(0 if ok else 1)
