#!/usr/bin/env python3
"""
scripts/verify_cache_coherence.py — Cache vs Consolidated File Coherence
=========================================================================
Verifies that cache files are consistent with consolidated master files.

For each source with cache/ directory:
  1. Count records in cache
  2. Count records in consolidated output file
  3. Compare and report discrepancies

Usage:
    python verify_cache_coherence.py
    python verify_cache_coherence.py --source 02_courses
    python verify_cache_coherence.py --fix  # report only, suggest fixes

Output:
    output/quality/cache_coherence_report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

OUTPUT_DIR = ROOT / "output"
REPORT_PATH = OUTPUT_DIR / "quality" / "cache_coherence_report.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def count_cache_files(cache_dir: Path) -> int:
    """Count JSON/JSONL files in cache directory."""
    if not cache_dir.exists():
        return 0
    count = 0
    for ext in ["*.json", "*.jsonl"]:
        count += sum(1 for _ in cache_dir.glob(ext))
    return count


def count_consolidated_records(output_dir: Path) -> tuple[int, str]:
    """Count records in the main consolidated file."""
    # Look for main output file
    for pattern in ["*.jsonl", "*.json"]:
        files = sorted(output_dir.glob(pattern))
        # Skip cache directory files
        files = [f for f in files if "cache" not in str(f) and f.is_file()]
        if files:
            # Use the largest file as the consolidated output
            main_file = max(files, key=lambda f: f.stat().st_size)
            if main_file.suffix == ".jsonl":
                count = 0
                with open(main_file, "r", encoding="utf-8", errors="replace") as f:
                    for _ in f:
                        count += 1
                return count, main_file.name
            else:
                try:
                    data = json.loads(main_file.read_text(encoding="utf-8", errors="replace"))
                    if isinstance(data, list):
                        return len(data), main_file.name
                    elif isinstance(data, dict):
                        return 1, main_file.name
                except (json.JSONDecodeError, MemoryError):
                    return -1, main_file.name
    return 0, "N/A"


def verify_coherence(source_filter: str | None = None) -> dict:
    """Check cache vs consolidated coherence for all sources."""
    results = []
    sources_dir = OUTPUT_DIR

    if not sources_dir.exists():
        logger.warning(f"Output directory not found: {sources_dir}")
        return {"status": "no_output_dir", "results": []}

    for source_dir in sorted(sources_dir.iterdir()):
        if not source_dir.is_dir():
            continue
        if source_dir.name in ("quality", "audit", "features", "labels"):
            continue
        if source_filter and source_filter not in source_dir.name:
            continue

        cache_dir = source_dir / "cache"
        cache_count = count_cache_files(cache_dir)

        if cache_count == 0:
            continue  # No cache, skip

        consolidated_count, consolidated_file = count_consolidated_records(source_dir)

        status = "ok"
        if consolidated_count == 0:
            status = "no_consolidated"
        elif consolidated_count < 0:
            status = "parse_error"
        elif cache_count > 0 and consolidated_count > 0:
            ratio = consolidated_count / cache_count if cache_count > 0 else 0
            if ratio < 0.5:
                status = "consolidated_too_small"
            elif ratio > 2.0:
                status = "consolidated_too_large"

        results.append({
            "source": source_dir.name,
            "cache_files": cache_count,
            "consolidated_records": consolidated_count,
            "consolidated_file": consolidated_file,
            "status": status,
        })

    # Summary
    ok_count = sum(1 for r in results if r["status"] == "ok")
    issue_count = sum(1 for r in results if r["status"] != "ok")

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_sources": len(results),
        "ok": ok_count,
        "issues": issue_count,
        "results": results,
    }

    # Save report
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Report saved: {REPORT_PATH}")
    logger.info(f"Sources checked: {len(results)}, OK: {ok_count}, Issues: {issue_count}")

    # Print issues
    for r in results:
        if r["status"] != "ok":
            logger.warning(f"  {r['source']}: {r['status']} (cache={r['cache_files']}, consolidated={r['consolidated_records']})")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify cache vs consolidated file coherence")
    parser.add_argument("--source", help="Filter by source name")
    args = parser.parse_args()

    verify_coherence(source_filter=args.source)


if __name__ == "__main__":
    main()
