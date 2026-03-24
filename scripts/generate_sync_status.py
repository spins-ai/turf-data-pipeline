#!/usr/bin/env python3
"""
scripts/generate_sync_status.py
================================
Generates sync_status.json: synchronization state of each data source.

For each output directory, reports:
- last_modified: most recent file modification timestamp
- file_count: number of data files (json/jsonl/csv/parquet)
- total_size_bytes: total size of data files
- has_master: whether a corresponding data_master/ file exists
- status: "synced" | "stale" | "empty"

No external APIs needed -- pure filesystem inspection.

Usage:
    python scripts/generate_sync_status.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"
DATA_MASTER_DIR = ROOT / "data_master"
SYNC_STATUS_PATH = ROOT / "sync_status.json"

DATA_EXTENSIONS = {".json", ".jsonl", ".csv", ".parquet"}


def inspect_directory(dirpath: Path) -> dict:
    """Inspect a single output directory for sync status."""
    data_files = []
    latest_mtime = 0.0

    if not dirpath.exists() or not dirpath.is_dir():
        return {"status": "missing", "file_count": 0, "total_size_bytes": 0}

    for f in dirpath.rglob("*"):
        if f.is_file() and f.suffix in DATA_EXTENSIONS:
            sz = f.stat().st_size
            mt = f.stat().st_mtime
            if sz > 0:
                data_files.append({"name": f.name, "size": sz, "mtime": mt})
                if mt > latest_mtime:
                    latest_mtime = mt

    total_size = sum(f["size"] for f in data_files)

    if not data_files:
        return {"status": "empty", "file_count": 0, "total_size_bytes": 0}

    last_mod = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(latest_mtime))

    # Determine staleness: if last modified > 7 days ago
    age_days = (time.time() - latest_mtime) / 86400
    status = "synced" if age_days < 7 else "stale"

    return {
        "status": status,
        "file_count": len(data_files),
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 1),
        "last_modified": last_mod,
        "age_days": round(age_days, 1),
    }


def main():
    sync_status = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "output_dir": str(OUTPUT_DIR),
        "data_master_dir": str(DATA_MASTER_DIR),
        "sources": {},
    }

    # Inspect output/ subdirectories
    if OUTPUT_DIR.exists():
        for entry in sorted(OUTPUT_DIR.iterdir()):
            if entry.is_dir() and not entry.name.startswith("."):
                info = inspect_directory(entry)
                # Check if there's a corresponding master file
                base_name = entry.name
                has_master = any(
                    (DATA_MASTER_DIR / f).exists() and (DATA_MASTER_DIR / f).stat().st_size > 0
                    for f in [
                        f"{base_name}.jsonl",
                        f"{base_name}.json",
                        f"{base_name}.parquet",
                    ]
                )
                info["has_master"] = has_master
                sync_status["sources"][entry.name] = info

    # Inspect data_master/ files
    master_files = {}
    if DATA_MASTER_DIR.exists():
        for f in sorted(DATA_MASTER_DIR.iterdir()):
            if f.is_file() and f.suffix in DATA_EXTENSIONS and f.stat().st_size > 0:
                master_files[f.name] = {
                    "size_bytes": f.stat().st_size,
                    "size_mb": round(f.stat().st_size / (1024 * 1024), 1),
                    "last_modified": time.strftime(
                        "%Y-%m-%dT%H:%M:%S", time.localtime(f.stat().st_mtime)
                    ),
                }

    sync_status["data_master_files"] = master_files

    # Summary
    sources = sync_status["sources"]
    sync_status["summary"] = {
        "total_sources": len(sources),
        "synced": sum(1 for s in sources.values() if s["status"] == "synced"),
        "stale": sum(1 for s in sources.values() if s["status"] == "stale"),
        "empty": sum(1 for s in sources.values() if s["status"] == "empty"),
        "missing": sum(1 for s in sources.values() if s["status"] == "missing"),
        "total_master_files": len(master_files),
    }

    with open(SYNC_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(sync_status, f, indent=2, ensure_ascii=False)

    print(f"sync_status.json written to {SYNC_STATUS_PATH}")
    print(f"Summary:")
    for k, v in sync_status["summary"].items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
