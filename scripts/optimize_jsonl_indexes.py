#!/usr/bin/env python3
"""
scripts/optimize_jsonl_indexes.py
=================================
Creates byte-offset index files for partants_master.jsonl, enabling O(1)
random-access lookups without loading the full file into memory.

Indexes produced (under data_master/indexes/):
  - partant_uid_index.json : partant_uid -> byte_offset
  - course_uid_index.json  : course_uid  -> byte_offset  (first occurrence)
  - date_index.json        : date_reunion_iso -> [byte_offset, ...]

Architecture:
  Single streaming pass over partants_master.jsonl.  We record the byte
  offset of each line *before* reading it (via f.tell()), then extract the
  three key fields from the parsed JSON.  Indexes are written as JSON dicts
  at the end.

Memory budget: < 4 GB for ~3M records.
  - partant_uid index:  ~3M entries * ~40 bytes key + 8 bytes value  -> ~150 MB
  - course_uid index:   ~200K entries                                 -> ~10 MB
  - date index:         ~5K dates * list of offsets                   -> ~50 MB

No external dependencies -- stdlib only.
No API calls -- 100% local processing.

Usage:
    python scripts/optimize_jsonl_indexes.py
    python scripts/optimize_jsonl_indexes.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_MASTER = _PROJECT_ROOT / "data_master"
INDEXES_DIR = DATA_MASTER / "indexes"
DEFAULT_INPUT = DATA_MASTER / "partants_master.jsonl"

# Progress report interval (seconds)
_REPORT_INTERVAL = 30


def human_size(nbytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def build_indexes(input_path: Path, output_dir: Path) -> dict:
    """Stream *input_path* once and build three byte-offset indexes.

    Returns a summary dict with counts.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- accumulators (dict values are byte offsets) ---
    partant_idx: dict[str, int] = {}
    course_idx: dict[str, int] = {}          # first occurrence only
    date_idx: dict[str, list[int]] = {}      # date -> [offsets...]

    total_lines = 0
    skipped = 0
    t0 = time.time()
    last_report = t0

    file_size = input_path.stat().st_size

    # IMPORTANT: open in binary mode so that f.tell() returns true byte
    # offsets (text mode on Windows would give unreliable values due to
    # newline translation).
    with open(input_path, "rb") as f:
        while True:
            offset = f.tell()
            raw = f.readline()
            if not raw:
                break

            line = raw.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            total_lines += 1

            # --- partant_uid ---
            puid = rec.get("partant_uid")
            if puid:
                partant_idx[puid] = offset

            # --- course_uid (first occurrence per course) ---
            cuid = rec.get("course_uid")
            if cuid and cuid not in course_idx:
                course_idx[cuid] = offset

            # --- date_reunion_iso ---
            date_str = rec.get("date_reunion_iso")
            if date_str:
                if date_str not in date_idx:
                    date_idx[date_str] = []
                date_idx[date_str].append(offset)

            # Progress
            now = time.time()
            if now - last_report > _REPORT_INTERVAL:
                elapsed = now - t0
                pct = (offset / file_size * 100) if file_size else 0
                rate = total_lines / elapsed if elapsed else 0
                print(
                    f"  [{int(elapsed)}s] {total_lines:,} lines "
                    f"({pct:.1f}%), {rate:,.0f} lines/s"
                )
                last_report = now

    elapsed = time.time() - t0
    rate = total_lines / elapsed if elapsed else 0
    print(
        f"Streaming done: {total_lines:,} lines in {elapsed:.1f}s "
        f"({rate:,.0f} lines/s, {skipped} skipped)"
    )

    # --- Write indexes ---
    def _write_index(name: str, data: dict) -> Path:
        out = output_dir / name
        with open(out, "w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False, separators=(",", ":"))
        size = out.stat().st_size
        print(f"  {name}: {len(data):,} entries, {human_size(size)}")
        return out

    print("\nWriting indexes ...")
    _write_index("partant_uid_index.json", partant_idx)
    _write_index("course_uid_index.json", course_idx)
    _write_index("date_index.json", date_idx)

    return {
        "total_lines": total_lines,
        "skipped": skipped,
        "nb_partant_uids": len(partant_idx),
        "nb_course_uids": len(course_idx),
        "nb_dates": len(date_idx),
        "elapsed_s": round(elapsed, 1),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build byte-offset indexes for partants_master.jsonl"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to partants_master.jsonl (default: data_master/partants_master.jsonl)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=INDEXES_DIR,
        help="Directory for index files (default: data_master/indexes/)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"ERROR: input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    print("=" * 60)
    print("JSONL Byte-Offset Index Builder")
    print("=" * 60)
    print(f"Input : {args.input} ({human_size(args.input.stat().st_size)})")
    print(f"Output: {args.output_dir}/")
    print()

    summary = build_indexes(args.input, args.output_dir)

    print("\n--- Summary ---")
    for k, v in summary.items():
        print(f"  {k}: {v:,}" if isinstance(v, int) else f"  {k}: {v}")
    print("Done.")


if __name__ == "__main__":
    main()
