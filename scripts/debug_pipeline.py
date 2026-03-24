#!/usr/bin/env python3
"""
scripts/debug_pipeline.py — Pilier 14 : Debugging approfondi
==============================================================
Complements diagnostic.py with deeper debugging checks:

  1. Orphaned files — files in output/ not referenced by any scraper
  2. Stale checkpoints — checkpoint files older than 30 days
  3. Log file errors — scan logs/ for ERROR/CRITICAL patterns
  4. Zombie .tmp files — temporary files left behind by crashed scrapers
  5. Disk usage per directory — detailed breakdown

Outputs:
  - quality/debug_report.md

RAM budget: < 2 GB (file scanning, no bulk loading).

Usage:
    python scripts/debug_pipeline.py
"""

from __future__ import annotations

import json
import os
import re
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
    BASE_DIR,
    CACHE_DIR,
    LOGS_DIR,
    OUTPUT_DIR,
    QUALITY_DIR,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_PATH = QUALITY_DIR / "debug_report.md"
_TODAY = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
_NOW_UTC = datetime.now(timezone.utc)
STALE_DAYS = 30

# Patterns for error detection in logs
ERROR_PATTERNS = re.compile(
    r"\b(ERROR|CRITICAL|FATAL|Traceback|Exception|FAILED)\b",
    re.IGNORECASE,
)

# Known scraper output directory prefixes (numbered scripts produce these)
_SCRAPER_DIR_PATTERN = re.compile(r"^\d{2,3}_")

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------
_findings: list[dict] = []


def _add_finding(
    category: str, severity: str, message: str, detail: str = ""
) -> None:
    """Record a debugging finding."""
    _findings.append({
        "category": category,
        "severity": severity,
        "message": message,
        "detail": detail,
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _human_size(nbytes: int | float) -> str:
    """Convert bytes to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def _dir_size(d: Path) -> int:
    """Total size of all files in a directory (recursive)."""
    total = 0
    try:
        for f in d.rglob("*"):
            if f.is_file():
                try:
                    total += f.stat().st_size
                except OSError:
                    pass
    except OSError:
        pass
    return total


def _file_age_days(path: Path) -> float:
    """Return file age in days."""
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return (_NOW_UTC - mtime).total_seconds() / 86400.0
    except OSError:
        return -1.0


# ===================================================================
# CHECK 1: Orphaned files
# ===================================================================
def check_orphaned_files() -> dict:
    """Find files in output/ not in a recognized scraper directory."""
    print("  [1/5] Checking for orphaned files ...")

    if not OUTPUT_DIR.is_dir():
        _add_finding("orphaned", "WARN", "output/ directory does not exist")
        return {"orphaned_files": 0}

    # Collect all known output subdirectory names from config
    # (any directory matching numbered pattern is "known")
    known_dirs: set[str] = set()
    orphaned_files: list[str] = []

    for item in OUTPUT_DIR.iterdir():
        if item.is_dir():
            if _SCRAPER_DIR_PATTERN.match(item.name):
                known_dirs.add(item.name)
            elif item.name in (
                "features", "labels", "exports", "quality", "elo_ratings",
                "comblage", "dedup", "nettoyage", "audit", "rapports_merged",
                "meteo_complete", "pedigree_complete", "field_strength",
                "career_stats", "checkpoints",
            ):
                known_dirs.add(item.name)
            else:
                # Unknown directory — check if it has files
                file_count = sum(1 for f in item.rglob("*") if f.is_file())
                if file_count > 0:
                    orphaned_files.append(f"{item.name}/ ({file_count} files)")
                    _add_finding(
                        "orphaned", "WARN",
                        f"Unrecognized output dir: {item.name}/",
                        f"{file_count} files, {_human_size(_dir_size(item))}",
                    )
        elif item.is_file():
            # Files directly in output/ (not in a subdir)
            orphaned_files.append(item.name)
            _add_finding(
                "orphaned", "WARN",
                f"File directly in output/: {item.name}",
                _human_size(item.stat().st_size),
            )

    if not orphaned_files:
        print("    No orphaned files found")

    return {
        "orphaned_files": len(orphaned_files),
        "known_dirs": len(known_dirs),
        "details": orphaned_files[:50],
    }


# ===================================================================
# CHECK 2: Stale checkpoints
# ===================================================================
def check_stale_checkpoints() -> dict:
    """Find checkpoint files older than STALE_DAYS."""
    print("  [2/5] Checking for stale checkpoints ...")

    stale: list[dict] = []

    # Check common checkpoint locations
    checkpoint_dirs = [
        CACHE_DIR,
        OUTPUT_DIR / "checkpoints",
        PROJECT_ROOT / "checkpoints",
    ]

    checkpoint_patterns = ["*checkpoint*", "*ckpt*", "*.checkpoint.json"]

    for d in checkpoint_dirs:
        if not d.is_dir():
            continue
        for pattern in checkpoint_patterns:
            for f in d.rglob(pattern):
                if not f.is_file():
                    continue
                age = _file_age_days(f)
                if age > STALE_DAYS:
                    stale.append({
                        "path": str(f.relative_to(PROJECT_ROOT)),
                        "age_days": round(age, 1),
                        "size": _human_size(f.stat().st_size),
                    })
                    _add_finding(
                        "stale_checkpoint", "WARN",
                        f"Stale checkpoint: {f.relative_to(PROJECT_ROOT)}",
                        f"{round(age)} days old, {_human_size(f.stat().st_size)}",
                    )

    # Also look for .json files in cache that look like checkpoints
    if CACHE_DIR.is_dir():
        for f in CACHE_DIR.rglob("*.json"):
            if not f.is_file():
                continue
            age = _file_age_days(f)
            if age > STALE_DAYS:
                # Quick check if it looks like a checkpoint
                try:
                    with open(f, "r", encoding="utf-8") as fh:
                        content = fh.read(512)
                    if any(kw in content.lower() for kw in ("checkpoint", "last_line", "resume", "offset")):
                        stale.append({
                            "path": str(f.relative_to(PROJECT_ROOT)),
                            "age_days": round(age, 1),
                            "size": _human_size(f.stat().st_size),
                        })
                        _add_finding(
                            "stale_checkpoint", "WARN",
                            f"Likely stale checkpoint: {f.relative_to(PROJECT_ROOT)}",
                            f"{round(age)} days old",
                        )
                except OSError:
                    pass

    if not stale:
        print("    No stale checkpoints found")

    return {
        "stale_checkpoints": len(stale),
        "threshold_days": STALE_DAYS,
        "details": stale[:30],
    }


# ===================================================================
# CHECK 3: Log file error patterns
# ===================================================================
def check_log_errors() -> dict:
    """Scan log files for ERROR/CRITICAL patterns."""
    print("  [3/5] Scanning log files for errors ...")

    error_summary: dict[str, list[str]] = {}
    total_errors = 0

    if not LOGS_DIR.is_dir():
        _add_finding("log_errors", "WARN", "logs/ directory does not exist")
        return {"log_files_scanned": 0, "total_errors": 0}

    log_files = list(LOGS_DIR.glob("*.log"))
    log_files.extend(LOGS_DIR.glob("*.txt"))

    for log_file in sorted(log_files):
        if not log_file.is_file():
            continue
        errors_in_file: list[str] = []
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as fh:
                for line_num, line in enumerate(fh, 1):
                    if ERROR_PATTERNS.search(line):
                        errors_in_file.append(
                            f"L{line_num}: {line.strip()[:200]}"
                        )
                        total_errors += 1
                        # Cap per file to avoid huge reports
                        if len(errors_in_file) >= 20:
                            errors_in_file.append("... (truncated)")
                            break
        except OSError:
            continue

        if errors_in_file:
            rel_path = str(log_file.relative_to(PROJECT_ROOT))
            error_summary[rel_path] = errors_in_file
            _add_finding(
                "log_errors",
                "ERROR" if len(errors_in_file) > 5 else "WARN",
                f"{len(errors_in_file)} error(s) in {log_file.name}",
                errors_in_file[0] if errors_in_file else "",
            )

    if total_errors == 0:
        print("    No errors found in logs")

    return {
        "log_files_scanned": len(log_files),
        "total_errors": total_errors,
        "files_with_errors": len(error_summary),
        "details": error_summary,
    }


# ===================================================================
# CHECK 4: Zombie .tmp files
# ===================================================================
def check_zombie_tmp_files() -> dict:
    """Find .tmp files left behind by crashed processes."""
    print("  [4/5] Checking for zombie .tmp files ...")

    zombies: list[dict] = []

    # Scan output/ and data_master/
    scan_dirs = [OUTPUT_DIR, PROJECT_ROOT / "data_master", CACHE_DIR, LOGS_DIR]

    for d in scan_dirs:
        if not d.is_dir():
            continue
        for f in d.rglob("*.tmp"):
            if not f.is_file():
                continue
            age = _file_age_days(f)
            zombies.append({
                "path": str(f.relative_to(PROJECT_ROOT)),
                "age_days": round(age, 1),
                "size": _human_size(f.stat().st_size),
            })
            _add_finding(
                "zombie_tmp", "WARN",
                f"Zombie .tmp file: {f.relative_to(PROJECT_ROOT)}",
                f"{round(age, 1)} days old, {_human_size(f.stat().st_size)}",
            )

        # Also check for common temp patterns
        for pattern in ["*.partial", "*.temp", "*.bak", "*~"]:
            for f in d.rglob(pattern):
                if not f.is_file():
                    continue
                age = _file_age_days(f)
                if age > 1:  # Only flag if older than 1 day
                    zombies.append({
                        "path": str(f.relative_to(PROJECT_ROOT)),
                        "age_days": round(age, 1),
                        "size": _human_size(f.stat().st_size),
                    })
                    _add_finding(
                        "zombie_tmp", "WARN",
                        f"Temp file: {f.relative_to(PROJECT_ROOT)}",
                        f"{round(age, 1)} days old",
                    )

    if not zombies:
        print("    No zombie temp files found")

    return {
        "zombie_files": len(zombies),
        "details": zombies[:50],
    }


# ===================================================================
# CHECK 5: Disk usage per directory
# ===================================================================
def check_disk_usage() -> dict:
    """Report disk usage per top-level project directory."""
    print("  [5/5] Computing disk usage per directory ...")

    usage: list[dict] = []

    # Top-level directories
    for item in sorted(PROJECT_ROOT.iterdir()):
        if not item.is_dir():
            continue
        if item.name.startswith("."):
            continue
        if item.name in ("__pycache__", "node_modules", ".git"):
            continue

        size = _dir_size(item)
        file_count = sum(1 for f in item.rglob("*") if f.is_file())
        usage.append({
            "directory": item.name,
            "size_bytes": size,
            "size_human": _human_size(size),
            "file_count": file_count,
        })

    # Sort by size descending
    usage.sort(key=lambda x: x["size_bytes"], reverse=True)

    total_size = sum(d["size_bytes"] for d in usage)

    # Flag large directories
    for d in usage:
        if d["size_bytes"] > 5 * 1024 * 1024 * 1024:  # > 5 GB
            _add_finding(
                "disk_usage", "WARN",
                f"Large directory: {d['directory']}/",
                f"{d['size_human']}, {d['file_count']} files",
            )

    return {
        "total_size": _human_size(total_size),
        "total_size_bytes": total_size,
        "directories": usage,
    }


# ===================================================================
# Report generation
# ===================================================================
def generate_report(
    orphaned: dict,
    stale: dict,
    log_errors: dict,
    zombies: dict,
    disk: dict,
) -> str:
    """Generate the Markdown debug report."""
    lines: list[str] = []
    lines.append("# Debug Pipeline Report (Pilier 14)")
    lines.append(f"\nGenerated: {_TODAY}\n")

    # Summary
    errors = sum(1 for f in _findings if f["severity"] == "ERROR")
    warnings = sum(1 for f in _findings if f["severity"] == "WARN")
    lines.append(f"**Findings: {errors} errors, {warnings} warnings, "
                 f"{len(_findings)} total**\n")

    # 1. Orphaned files
    lines.append("## 1. Orphaned Files\n")
    lines.append(f"Known output directories: {orphaned['known_dirs']}")
    lines.append(f"Orphaned items: {orphaned['orphaned_files']}\n")
    if orphaned["details"]:
        for item in orphaned["details"]:
            lines.append(f"- {item}")
        lines.append("")

    # 2. Stale checkpoints
    lines.append("## 2. Stale Checkpoints (> {0} days)\n".format(stale["threshold_days"]))
    lines.append(f"Stale checkpoints found: {stale['stale_checkpoints']}\n")
    if stale["details"]:
        lines.append("| Path | Age (days) | Size |")
        lines.append("|------|------------|------|")
        for ck in stale["details"]:
            lines.append(f"| {ck['path']} | {ck['age_days']} | {ck['size']} |")
        lines.append("")

    # 3. Log errors
    lines.append("## 3. Log File Errors\n")
    lines.append(f"Log files scanned: {log_errors['log_files_scanned']}")
    lines.append(f"Files with errors: {log_errors['files_with_errors']}")
    lines.append(f"Total error lines: {log_errors['total_errors']}\n")
    if log_errors["details"]:
        for log_path, errors_list in log_errors["details"].items():
            lines.append(f"### {log_path}\n")
            lines.append("```")
            for err_line in errors_list[:10]:
                lines.append(err_line)
            lines.append("```\n")

    # 4. Zombie tmp files
    lines.append("## 4. Zombie Temporary Files\n")
    lines.append(f"Zombie files found: {zombies['zombie_files']}\n")
    if zombies["details"]:
        lines.append("| Path | Age (days) | Size |")
        lines.append("|------|------------|------|")
        for z in zombies["details"][:30]:
            lines.append(f"| {z['path']} | {z['age_days']} | {z['size']} |")
        lines.append("")

    # 5. Disk usage
    lines.append("## 5. Disk Usage per Directory\n")
    lines.append(f"**Total project size: {disk['total_size']}**\n")
    lines.append("| Directory | Size | Files |")
    lines.append("|-----------|------|-------|")
    for d in disk["directories"]:
        lines.append(f"| {d['directory']}/ | {d['size_human']} | {d['file_count']:,} |")
    lines.append("")

    # All findings summary
    if _findings:
        lines.append("## All Findings\n")
        lines.append("| # | Category | Severity | Message |")
        lines.append("|---|----------|----------|---------|")
        for i, f in enumerate(_findings, 1):
            lines.append(
                f"| {i} | {f['category']} | {f['severity']} | {f['message']} |"
            )
        lines.append("")

    lines.append("---")
    lines.append("RAM budget: < 2 GB (file scanning only, no data loading)")
    lines.append("")
    return "\n".join(lines)


# ===================================================================
# Main
# ===================================================================
def main() -> int:
    print("\n" + "=" * 60)
    print("  DEBUG PIPELINE — Pilier 14")
    print(f"  {_TODAY}")
    print(f"  Project root: {PROJECT_ROOT}")
    print("=" * 60 + "\n")

    t0 = time.monotonic()

    orphaned = check_orphaned_files()
    stale = check_stale_checkpoints()
    log_errors = check_log_errors()
    zombies = check_zombie_tmp_files()
    disk = check_disk_usage()

    elapsed = time.monotonic() - t0

    # Generate report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report(orphaned, stale, log_errors, zombies, disk)
    REPORT_PATH.write_text(report, encoding="utf-8")

    # Console summary
    errors = sum(1 for f in _findings if f["severity"] == "ERROR")
    warnings = sum(1 for f in _findings if f["severity"] == "WARN")

    print(f"\n{'=' * 60}")
    print(f"  Findings: {errors} errors, {warnings} warnings")
    print(f"  Project size: {disk['total_size']}")
    print(f"  Duration: {elapsed:.1f}s")
    print(f"  Report: {REPORT_PATH}")
    print("=" * 60 + "\n")

    return 1 if errors > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
