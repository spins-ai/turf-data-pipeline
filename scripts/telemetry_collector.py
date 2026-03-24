#!/usr/bin/env python3
"""
scripts/telemetry_collector.py — Pilier 13 Telemetrie
=====================================================
Pipeline telemetry collector for the turf-data pipeline.

Collects:
  - Disk usage per output directory
  - Record counts per JSONL file
  - Last modified dates

Computes:
  - Daily data growth rate
  - Scraper success rates (based on non-empty output dirs)
  - Pipeline throughput (records per output dir)

Outputs:
  - JSON to logs/telemetry_YYYYMMDD.json
  - Human-readable summary to console and log

Usage:
    python scripts/telemetry_collector.py

RAM budget: < 1 GB (streams files, no bulk loading).
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    DATA_MASTER_DIR,
    FEATURES_DIR,
    LABELS_DIR,
    LOGS_DIR,
    OUTPUT_DIR,
)
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
_LOG_NAME = f"telemetry_{_TODAY}"

logger = setup_logging(_LOG_NAME)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _dir_size_bytes(d: Path) -> int:
    """Total size of all files in a directory (non-recursive for speed)."""
    total = 0
    if not d.is_dir():
        return 0
    try:
        for f in d.iterdir():
            if f.is_file():
                try:
                    total += f.stat().st_size
                except OSError:
                    pass
    except OSError:
        pass
    return total


def _count_jsonl_records(path: Path) -> int:
    """Count non-empty lines in a JSONL file (streaming, low RAM)."""
    count = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.strip():
                    count += 1
    except OSError:
        return -1
    return count


def _human_size(nbytes: int | float) -> str:
    """Convert bytes to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def _last_modified_iso(path: Path) -> str | None:
    """Return ISO timestamp of last modification, or None."""
    try:
        mtime = path.stat().st_mtime
        return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
    except OSError:
        return None


# ===================================================================
# Collect: disk usage per output dir
# ===================================================================
def collect_disk_usage() -> list[dict]:
    """Collect disk usage for each subdirectory under output/."""
    logger.info("=== Collecting disk usage per output directory ===")
    results = []

    if not OUTPUT_DIR.is_dir():
        logger.warning("output/ directory not found")
        return results

    for d in sorted(OUTPUT_DIR.iterdir()):
        if not d.is_dir():
            continue
        size = _dir_size_bytes(d)
        file_count = sum(1 for f in d.iterdir() if f.is_file())
        results.append({
            "directory": d.name,
            "size_bytes": size,
            "size_human": _human_size(size),
            "file_count": file_count,
            "last_modified": _last_modified_iso(d),
        })

    logger.info(f"  Scanned {len(results)} output directories")
    return results


# ===================================================================
# Collect: record counts per JSONL
# ===================================================================
def collect_record_counts() -> list[dict]:
    """Count records in key JSONL files across data_master and output."""
    logger.info("=== Collecting record counts per JSONL ===")
    results = []

    # data_master JSONLs
    if DATA_MASTER_DIR.is_dir():
        for f in sorted(DATA_MASTER_DIR.glob("*.jsonl")):
            count = _count_jsonl_records(f)
            results.append({
                "file": f"data_master/{f.name}",
                "records": count,
                "size_bytes": f.stat().st_size if f.exists() else 0,
                "size_human": _human_size(f.stat().st_size) if f.exists() else "0 B",
                "last_modified": _last_modified_iso(f),
            })

    # features matrix
    if FEATURES_DIR.is_dir():
        for f in sorted(FEATURES_DIR.glob("*.jsonl")):
            count = _count_jsonl_records(f)
            results.append({
                "file": f"output/features/{f.name}",
                "records": count,
                "size_bytes": f.stat().st_size if f.exists() else 0,
                "size_human": _human_size(f.stat().st_size) if f.exists() else "0 B",
                "last_modified": _last_modified_iso(f),
            })

    # labels
    if LABELS_DIR.is_dir():
        for f in sorted(LABELS_DIR.glob("*.jsonl")):
            count = _count_jsonl_records(f)
            results.append({
                "file": f"output/labels/{f.name}",
                "records": count,
                "size_bytes": f.stat().st_size if f.exists() else 0,
                "size_human": _human_size(f.stat().st_size) if f.exists() else "0 B",
                "last_modified": _last_modified_iso(f),
            })

    logger.info(f"  Counted records in {len(results)} JSONL files")
    return results


# ===================================================================
# Compute: daily data growth rate
# ===================================================================
def compute_growth_rate() -> dict:
    """Estimate daily data growth by comparing today's telemetry with yesterday's."""
    logger.info("=== Computing daily data growth rate ===")

    # Try loading yesterday's telemetry
    yesterday_files = sorted(LOGS_DIR.glob("telemetry_*.json"))
    # Filter out today's file and find the most recent previous one
    previous = [
        f for f in yesterday_files
        if f.stem != f"telemetry_{_TODAY}"
    ]

    if not previous:
        logger.info("  No previous telemetry found, cannot compute growth rate")
        return {
            "growth_bytes_per_day": None,
            "growth_human_per_day": "N/A (first run)",
            "previous_telemetry": None,
        }

    latest_prev = previous[-1]
    try:
        with open(latest_prev, "r", encoding="utf-8") as fh:
            prev_data = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(f"  Could not load previous telemetry {latest_prev.name}: {exc}")
        return {
            "growth_bytes_per_day": None,
            "growth_human_per_day": "N/A (parse error)",
            "previous_telemetry": latest_prev.name,
        }

    prev_total = prev_data.get("summary", {}).get("total_output_size_bytes", 0)
    # Compute current total
    current_total = 0
    if OUTPUT_DIR.is_dir():
        for d in OUTPUT_DIR.iterdir():
            if d.is_dir():
                current_total += _dir_size_bytes(d)

    growth = current_total - prev_total
    # Calculate days between
    prev_date_str = prev_data.get("timestamp", "")[:10]
    try:
        prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d")
        today_date = datetime.strptime(_TODAY[:4] + "-" + _TODAY[4:6] + "-" + _TODAY[6:8], "%Y-%m-%d")
        days = max((today_date - prev_date).days, 1)
    except (ValueError, IndexError):
        days = 1

    daily_growth = growth / days

    logger.info(f"  Growth since {latest_prev.name}: {_human_size(growth)} over {days} day(s)")
    logger.info(f"  Daily growth rate: {_human_size(daily_growth)}/day")

    return {
        "growth_bytes_per_day": int(daily_growth),
        "growth_human_per_day": f"{_human_size(daily_growth)}/day",
        "previous_telemetry": latest_prev.name,
        "days_since_previous": days,
    }


# ===================================================================
# Compute: scraper success rates
# ===================================================================
def compute_scraper_success_rates(disk_usage: list[dict]) -> dict:
    """Compute success rates: dirs with files vs total scraper dirs."""
    logger.info("=== Computing scraper success rates ===")

    # Scraper output dirs are numbered (e.g., 00_, 01_, ..., 99_, 100_, ...)
    scraper_dirs = [
        d for d in disk_usage
        if d["directory"][:2].isdigit() or d["directory"][:3].isdigit()
    ]
    total = len(scraper_dirs)
    non_empty = sum(1 for d in scraper_dirs if d["file_count"] > 0)
    empty = total - non_empty

    rate = (non_empty / total * 100) if total > 0 else 0.0

    logger.info(f"  Scraper directories: {total} total, {non_empty} with data, {empty} empty")
    logger.info(f"  Success rate: {rate:.1f}%")

    return {
        "total_scraper_dirs": total,
        "dirs_with_data": non_empty,
        "dirs_empty": empty,
        "success_rate_pct": round(rate, 1),
    }


# ===================================================================
# Compute: pipeline throughput
# ===================================================================
def compute_pipeline_throughput(record_counts: list[dict]) -> dict:
    """Compute throughput metrics from record counts."""
    logger.info("=== Computing pipeline throughput ===")

    total_records = sum(
        r["records"] for r in record_counts if r["records"] > 0
    )
    total_size = sum(
        r["size_bytes"] for r in record_counts if r["records"] > 0
    )
    file_count = sum(1 for r in record_counts if r["records"] > 0)

    logger.info(f"  Total JSONL records: {total_records:,}")
    logger.info(f"  Total JSONL size: {_human_size(total_size)}")
    logger.info(f"  Active JSONL files: {file_count}")

    return {
        "total_records": total_records,
        "total_size_bytes": total_size,
        "total_size_human": _human_size(total_size),
        "active_jsonl_files": file_count,
        "avg_records_per_file": round(total_records / file_count, 1) if file_count > 0 else 0,
    }


# ===================================================================
# Human-readable summary
# ===================================================================
def print_summary(telemetry: dict) -> None:
    """Output a human-readable summary."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("  TELEMETRY SUMMARY")
    logger.info("=" * 60)

    summary = telemetry.get("summary", {})
    logger.info(f"  Timestamp:         {telemetry.get('timestamp', 'N/A')}")
    logger.info(f"  Output dirs:       {summary.get('total_output_dirs', 'N/A')}")
    logger.info(f"  Total output size: {summary.get('total_output_size_human', 'N/A')}")

    throughput = telemetry.get("pipeline_throughput", {})
    logger.info(f"  JSONL records:     {throughput.get('total_records', 0):,}")
    logger.info(f"  Active JSONL:      {throughput.get('active_jsonl_files', 0)}")

    scraper = telemetry.get("scraper_success_rates", {})
    logger.info(f"  Scraper success:   {scraper.get('success_rate_pct', 0)}%"
                f" ({scraper.get('dirs_with_data', 0)}/{scraper.get('total_scraper_dirs', 0)})")

    growth = telemetry.get("growth_rate", {})
    logger.info(f"  Daily growth:      {growth.get('growth_human_per_day', 'N/A')}")

    # Top 5 largest output dirs
    disk_usage = telemetry.get("disk_usage", [])
    if disk_usage:
        top5 = sorted(disk_usage, key=lambda x: x["size_bytes"], reverse=True)[:5]
        logger.info("")
        logger.info("  Top 5 largest output directories:")
        for d in top5:
            logger.info(f"    {d['size_human']:>10s}  {d['directory']}  ({d['file_count']} files)")

    # Top 5 largest JSONL by record count
    records = telemetry.get("record_counts", [])
    if records:
        top5r = sorted(records, key=lambda x: x["records"], reverse=True)[:5]
        logger.info("")
        logger.info("  Top 5 JSONL files by record count:")
        for r in top5r:
            logger.info(f"    {r['records']:>10,}  {r['file']}")

    logger.info("=" * 60)


# ===================================================================
# Main
# ===================================================================
def main() -> int:
    start = time.monotonic()

    logger.info("=" * 60)
    logger.info("  TELEMETRY COLLECTOR — turf-data-pipeline")
    logger.info(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Project root: {PROJECT_ROOT}")
    logger.info("=" * 60)

    # Collect
    disk_usage = collect_disk_usage()
    record_counts = collect_record_counts()

    # Compute
    growth_rate = compute_growth_rate()
    scraper_rates = compute_scraper_success_rates(disk_usage)
    throughput = compute_pipeline_throughput(record_counts)

    # Aggregate summary
    total_output_bytes = sum(d["size_bytes"] for d in disk_usage)

    elapsed = time.monotonic() - start

    telemetry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "project_root": str(PROJECT_ROOT),
        "collection_duration_s": round(elapsed, 2),
        "summary": {
            "total_output_dirs": len(disk_usage),
            "total_output_size_bytes": total_output_bytes,
            "total_output_size_human": _human_size(total_output_bytes),
        },
        "disk_usage": disk_usage,
        "record_counts": record_counts,
        "growth_rate": growth_rate,
        "scraper_success_rates": scraper_rates,
        "pipeline_throughput": throughput,
    }

    # Write JSON output
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = LOGS_DIR / f"telemetry_{_TODAY}.json"
    try:
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(telemetry, fh, indent=2, ensure_ascii=False)
        logger.info(f"Telemetry written to {output_path}")
    except OSError as exc:
        logger.error(f"Could not write telemetry JSON: {exc}")
        return 1

    # Human-readable summary
    print_summary(telemetry)

    logger.info(f"  Duration: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
