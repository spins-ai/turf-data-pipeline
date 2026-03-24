#!/usr/bin/env python3
"""
scripts/stability_monitor.py — Pilier 3 : Stabilite
=====================================================
Compare current data statistics with previous runs to detect regressions.

Alerts if:
  - Record count changes > 10%
  - New null fields appear
  - Field types change
  - Date range shrinks

Reads previous stats from telemetry logs (logs/telemetry_*.json) and computes
current stats by streaming JSONL files.

Outputs:
  - quality/stability_report.md

RAM budget: < 2 GB (streams files, no bulk loading).

Usage:
    python scripts/stability_monitor.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

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
    QUALITY_DIR,
)
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"stability_monitor_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_PATH = QUALITY_DIR / "stability_report.md"
RECORD_COUNT_THRESHOLD = 0.10  # 10% change triggers alert
SAMPLE_LINES_FOR_SCHEMA = 500  # lines sampled to detect schema


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
class Alert:
    """A single stability alert."""

    __slots__ = ("level", "category", "file", "message")

    def __init__(self, level: str, category: str, file: str, message: str):
        self.level = level  # "error", "warning", "info"
        self.category = category
        self.file = file
        self.message = message


class FileStats:
    """Statistics for a single JSONL file."""

    def __init__(self, name: str):
        self.name = name
        self.record_count: int = 0
        self.size_bytes: int = 0
        self.fields: dict[str, str] = {}  # field_name -> dominant type
        self.null_fields: set[str] = set()  # fields that are always null
        self.non_null_fields: set[str] = set()
        self.min_date: Optional[str] = None
        self.max_date: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "record_count": self.record_count,
            "size_bytes": self.size_bytes,
            "fields": self.fields,
            "null_fields": sorted(self.null_fields),
            "min_date": self.min_date,
            "max_date": self.max_date,
        }


# ---------------------------------------------------------------------------
# Stats collection (streaming)
# ---------------------------------------------------------------------------
DATE_FIELD_CANDIDATES = {
    "date_reunion", "date_reunion_iso", "date_course", "date",
    "date_programme", "timestamp_collecte",
}


def _infer_type(value: Any) -> str:
    """Return a simple type label for a value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "dict"
    return type(value).__name__


def collect_file_stats(path: Path) -> FileStats:
    """Stream a JSONL file and collect schema/count/date stats."""
    stats = FileStats(path.name)

    if not path.exists():
        return stats

    stats.size_bytes = path.stat().st_size
    field_types: dict[str, dict[str, int]] = {}  # field -> {type -> count}
    all_fields: set[str] = set()
    null_only_fields: set[str] = set()
    seen_non_null: set[str] = set()

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            stats.record_count += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not isinstance(rec, dict):
                continue

            # Schema sampling: only check first N records for field types
            if stats.record_count <= SAMPLE_LINES_FOR_SCHEMA:
                for key, value in rec.items():
                    all_fields.add(key)
                    t = _infer_type(value)
                    if key not in field_types:
                        field_types[key] = {}
                    field_types[key][t] = field_types[key].get(t, 0) + 1

                    if value is None:
                        null_only_fields.add(key)
                    else:
                        seen_non_null.add(key)

            # Date tracking (all records)
            for date_field in DATE_FIELD_CANDIDATES:
                dval = rec.get(date_field)
                if isinstance(dval, str) and len(dval) >= 10:
                    date_str = dval[:10]  # YYYY-MM-DD
                    if stats.min_date is None or date_str < stats.min_date:
                        stats.min_date = date_str
                    if stats.max_date is None or date_str > stats.max_date:
                        stats.max_date = date_str

    # Determine dominant types
    for key, type_counts in field_types.items():
        # Pick the most common non-null type
        non_null = {t: c for t, c in type_counts.items() if t != "null"}
        if non_null:
            stats.fields[key] = max(non_null, key=non_null.get)
        else:
            stats.fields[key] = "null"

    # Null fields = fields that appeared but were never non-null
    stats.null_fields = null_only_fields - seen_non_null
    stats.non_null_fields = seen_non_null

    return stats


# ---------------------------------------------------------------------------
# Previous run lookup (telemetry logs)
# ---------------------------------------------------------------------------
def load_previous_stats() -> Optional[dict]:
    """Load the most recent telemetry or stability snapshot for comparison.

    Looks in logs/ for telemetry_*.json or stability_snapshot_*.json.
    Returns the parsed dict or None.
    """
    # First try stability snapshots
    snapshot_dir = QUALITY_DIR
    snapshots = sorted(snapshot_dir.glob("stability_snapshot_*.json"), reverse=True)
    if snapshots:
        try:
            with open(snapshots[0], "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info("Previous snapshot: %s", snapshots[0].name)
            return data
        except (json.JSONDecodeError, OSError):
            pass

    # Fall back to telemetry logs
    if LOGS_DIR.is_dir():
        telemetry_files = sorted(LOGS_DIR.glob("telemetry_*.json"), reverse=True)
        for tf in telemetry_files[:5]:
            try:
                with open(tf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info("Previous telemetry: %s", tf.name)
                return data
            except (json.JSONDecodeError, OSError):
                continue

    return None


def _extract_prev_file_stats(prev_data: dict) -> dict[str, dict]:
    """Extract per-file stats from previous run data.

    Returns {filename: {record_count, fields, null_fields, min_date, max_date}}.
    """
    result: dict[str, dict] = {}

    # Stability snapshot format
    if "file_stats" in prev_data:
        for fs in prev_data["file_stats"]:
            name = fs.get("name", "")
            if name:
                result[name] = fs
        return result

    # Telemetry format: look for record_counts
    if "record_counts" in prev_data:
        for fname, count in prev_data["record_counts"].items():
            result[fname] = {"record_count": count}
    elif "files" in prev_data:
        for entry in prev_data["files"]:
            name = entry.get("file", entry.get("name", ""))
            if name:
                result[name] = entry

    return result


# ---------------------------------------------------------------------------
# Compare current vs previous
# ---------------------------------------------------------------------------
def compare_stats(
    current: list[FileStats], prev_file_stats: dict[str, dict]
) -> list[Alert]:
    """Compare current file stats with previous run, return alerts."""
    alerts: list[Alert] = []

    for fs in current:
        prev = prev_file_stats.get(fs.name)
        if prev is None:
            alerts.append(
                Alert("info", "new_file", fs.name, "New file (no previous data)")
            )
            continue

        # Record count change
        prev_count = prev.get("record_count", 0)
        if prev_count > 0 and fs.record_count > 0:
            pct_change = abs(fs.record_count - prev_count) / prev_count
            if pct_change > RECORD_COUNT_THRESHOLD:
                direction = "increased" if fs.record_count > prev_count else "decreased"
                alerts.append(
                    Alert(
                        "warning",
                        "record_count_change",
                        fs.name,
                        f"Record count {direction} by {pct_change:.1%}: "
                        f"{prev_count} -> {fs.record_count}",
                    )
                )
        elif prev_count > 0 and fs.record_count == 0:
            alerts.append(
                Alert(
                    "error",
                    "data_loss",
                    fs.name,
                    f"File now empty (was {prev_count} records)",
                )
            )

        # New null fields
        prev_null = set(prev.get("null_fields", []))
        new_nulls = fs.null_fields - prev_null
        if new_nulls:
            alerts.append(
                Alert(
                    "warning",
                    "new_null_fields",
                    fs.name,
                    f"New null-only fields: {', '.join(sorted(new_nulls))}",
                )
            )

        # Field type changes
        prev_fields = prev.get("fields", {})
        for field_name, curr_type in fs.fields.items():
            prev_type = prev_fields.get(field_name)
            if prev_type and prev_type != curr_type and curr_type != "null":
                alerts.append(
                    Alert(
                        "warning",
                        "type_change",
                        fs.name,
                        f"Field '{field_name}' type changed: "
                        f"{prev_type} -> {curr_type}",
                    )
                )

        # Date range shrinkage
        prev_min = prev.get("min_date")
        prev_max = prev.get("max_date")
        if prev_min and fs.min_date and fs.min_date > prev_min:
            alerts.append(
                Alert(
                    "warning",
                    "date_range_shrink",
                    fs.name,
                    f"Min date increased: {prev_min} -> {fs.min_date} "
                    f"(lost earlier data)",
                )
            )
        if prev_max and fs.max_date and fs.max_date < prev_max:
            alerts.append(
                Alert(
                    "warning",
                    "date_range_shrink",
                    fs.name,
                    f"Max date decreased: {prev_max} -> {fs.max_date} "
                    f"(lost recent data)",
                )
            )

    # Check for files that disappeared
    current_names = {fs.name for fs in current}
    for prev_name in prev_file_stats:
        if prev_name not in current_names:
            alerts.append(
                Alert(
                    "error",
                    "file_missing",
                    prev_name,
                    "File present in previous run but now missing",
                )
            )

    return alerts


# ---------------------------------------------------------------------------
# Discover files to monitor
# ---------------------------------------------------------------------------
def discover_jsonl_files() -> list[Path]:
    """Return JSONL files to monitor (data_master + features + labels)."""
    files: list[Path] = []
    for d in [DATA_MASTER_DIR, FEATURES_DIR, LABELS_DIR]:
        if d.is_dir():
            files.extend(sorted(d.glob("*.jsonl")))
    return files


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def generate_report(
    current_stats: list[FileStats],
    alerts: list[Alert],
    had_previous: bool,
) -> str:
    """Generate the stability report in Markdown."""
    errors = [a for a in alerts if a.level == "error"]
    warnings = [a for a in alerts if a.level == "warning"]
    infos = [a for a in alerts if a.level == "info"]

    lines: list[str] = []
    lines.append("# Stability Monitor Report (Pilier 3)")
    lines.append(f"\nGenerated: {datetime.now().isoformat()}")

    status = "PASS"
    if errors:
        status = "FAIL"
    elif warnings:
        status = "WARN"

    lines.append(f"\n## Summary\n")
    lines.append(f"- Files monitored: {len(current_stats)}")
    lines.append(f"- Previous run data: {'available' if had_previous else 'none (first run)'}")
    lines.append(f"- Errors: {len(errors)}")
    lines.append(f"- Warnings: {len(warnings)}")
    lines.append(f"- Info: {len(infos)}")
    lines.append(f"- **Status: {status}**")

    # Current stats table
    lines.append("\n## Current File Statistics\n")
    lines.append("| File | Records | Size (MB) | Fields | Date Range |")
    lines.append("|------|---------|-----------|--------|------------|")
    for fs in current_stats:
        size_mb = round(fs.size_bytes / (1024 * 1024), 1)
        date_range = ""
        if fs.min_date and fs.max_date:
            date_range = f"{fs.min_date} .. {fs.max_date}"
        lines.append(
            f"| {fs.name} | {fs.record_count} | {size_mb} "
            f"| {len(fs.fields)} | {date_range} |"
        )

    # Alerts
    if errors:
        lines.append(f"\n## Errors ({len(errors)})\n")
        lines.append("| Category | File | Message |")
        lines.append("|----------|------|---------|")
        for a in errors:
            lines.append(f"| {a.category} | {a.file} | {a.message} |")

    if warnings:
        lines.append(f"\n## Warnings ({len(warnings)})\n")
        lines.append("| Category | File | Message |")
        lines.append("|----------|------|---------|")
        for a in warnings:
            lines.append(f"| {a.category} | {a.file} | {a.message} |")

    if infos:
        lines.append(f"\n## Info ({len(infos)})\n")
        for a in infos:
            lines.append(f"- **{a.file}**: {a.message}")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    logger.info("=== Stability Monitor (Pilier 3) ===")

    # 1. Collect current stats
    jsonl_files = discover_jsonl_files()
    logger.info("Monitoring %d JSONL files", len(jsonl_files))

    current_stats: list[FileStats] = []
    for p in jsonl_files:
        logger.info("  Collecting stats: %s", p.name)
        current_stats.append(collect_file_stats(p))

    # 2. Load previous run data
    prev_data = load_previous_stats()
    had_previous = prev_data is not None
    prev_file_stats = _extract_prev_file_stats(prev_data) if prev_data else {}

    # 3. Compare
    alerts = compare_stats(current_stats, prev_file_stats)
    logger.info(
        "Comparison: %d errors, %d warnings",
        sum(1 for a in alerts if a.level == "error"),
        sum(1 for a in alerts if a.level == "warning"),
    )

    # 4. Generate report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report(current_stats, alerts, had_previous)
    REPORT_PATH.write_text(report, encoding="utf-8")
    logger.info("Report: %s", REPORT_PATH)

    # 5. Save snapshot for next run
    snapshot_path = QUALITY_DIR / f"stability_snapshot_{_TODAY}.json"
    snapshot = {
        "timestamp": datetime.now().isoformat(),
        "file_stats": [fs.to_dict() for fs in current_stats],
    }
    snapshot_path.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info("Snapshot saved: %s", snapshot_path)

    errors = [a for a in alerts if a.level == "error"]
    if errors:
        print(f"\n[FAIL] {len(errors)} stability errors. See {REPORT_PATH}")
        return 1
    else:
        print(f"\n[OK] Stability check passed. Report: {REPORT_PATH}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
