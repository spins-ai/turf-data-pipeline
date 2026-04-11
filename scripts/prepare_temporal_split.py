#!/usr/bin/env python3
"""Prepare temporal train/validation/test split indices.

Strategy: split by date (not random!) to prevent temporal leakage.
- Train: everything before 2024-01-01
- Validation: 2024-01-01 to 2024-06-30
- Test: 2024-07-01 onwards

Output: 3 files with partant_uid lists for each split.
"""
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

INPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/04_FEATURES/splits")

TRAIN_END = "2024-01-01"
VAL_END = "2024-07-01"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    train_uids = []
    val_uids = []
    test_uids = []
    no_date = []

    t0 = time.perf_counter()

    with open(INPUT, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            rec = json.loads(line)
            uid = rec.get("partant_uid", "")
            date = rec.get("date_reunion_iso") or rec.get("date_reunion") or rec.get("date") or ""

            # Normalize date to YYYY-MM-DD
            if len(date) >= 10:
                date_str = date[:10]
            else:
                no_date.append(uid)
                train_uids.append(uid)  # Default old data to train
                continue

            if date_str < TRAIN_END:
                train_uids.append(uid)
            elif date_str < VAL_END:
                val_uids.append(uid)
            else:
                test_uids.append(uid)

            if lineno % 500_000 == 0:
                print(f"  {lineno:,} processed...", file=sys.stderr)
                gc.collect()

    # Write splits
    for name, uids in [("train", train_uids), ("val", val_uids), ("test", test_uids)]:
        fpath = OUTPUT_DIR / f"{name}_uids.txt"
        with open(fpath, "w", encoding="utf-8") as f:
            for uid in uids:
                f.write(uid + "\n")

    elapsed = time.perf_counter() - t0
    total = len(train_uids) + len(val_uids) + len(test_uids)

    print(f"\n{'='*60}")
    print(f"TEMPORAL SPLIT PREPARED")
    print(f"{'='*60}")
    print(f"Total records: {total:,}")
    print(f"Train (<{TRAIN_END}): {len(train_uids):,} ({len(train_uids)/total*100:.1f}%)")
    print(f"Val ({TRAIN_END} - {VAL_END}): {len(val_uids):,} ({len(val_uids)/total*100:.1f}%)")
    print(f"Test (>={VAL_END}): {len(test_uids):,} ({len(test_uids)/total*100:.1f}%)")
    print(f"No date (defaulted to train): {len(no_date):,}")
    print(f"Time: {elapsed:.0f}s")
    print(f"Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
