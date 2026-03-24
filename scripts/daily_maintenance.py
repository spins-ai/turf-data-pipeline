#!/usr/bin/env python3
"""
scripts/daily_maintenance.py — Maintenance quotidienne
======================================================
Daily maintenance tasks for the turf-data pipeline.

Tasks performed:
  1. Clean up __pycache__ directories
  2. Clean up .tmp files older than 24h
  3. Check disk space (warn if < 50GB free)
  4. Check data freshness (warn if partants_master older than 7 days)
  5. Verify CHECKSUMS.sha256 for 3 random files
  6. Run diagnostic.py health check
  7. Output summary to logs/daily_maintenance_YYYYMMDD.log

Usage:
    python scripts/daily_maintenance.py
"""

from __future__ import annotations

import hashlib
import os
import random
import shutil
import subprocess
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
    LOGS_DIR,
    PARTANTS_MASTER,
)
from utils.logging_setup import setup_logging  # noqa: E402

# Create a dated log name
_TODAY = datetime.now().strftime("%Y%m%d")
_LOG_NAME = f"daily_maintenance_{_TODAY}"

logger = setup_logging(_LOG_NAME)

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
_cleaned = 0
_warnings = 0
_errors = 0


def _warn(msg: str) -> None:
    global _warnings
    _warnings += 1
    logger.warning(msg)


def _error(msg: str) -> None:
    global _errors
    _errors += 1
    logger.error(msg)


# ===================================================================
# 1. Clean up __pycache__ directories
# ===================================================================
def clean_pycache() -> None:
    global _cleaned
    logger.info("=== Task 1: Clean __pycache__ directories ===")
    count = 0
    total_bytes = 0
    for d in PROJECT_ROOT.rglob("__pycache__"):
        if not d.is_dir():
            continue
        if ".git" in d.parts or ".claude" in d.parts:
            continue
        try:
            dir_size = sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
            shutil.rmtree(str(d))
            count += 1
            total_bytes += dir_size
        except OSError as exc:
            _warn(f"Could not remove {d}: {exc}")

    _cleaned += count
    mb = total_bytes / (1024 * 1024)
    logger.info(f"  Removed {count} __pycache__ directories ({mb:.1f} MB freed)")


# ===================================================================
# 2. Clean up .tmp files older than 24h
# ===================================================================
def clean_tmp_files() -> None:
    global _cleaned
    logger.info("=== Task 2: Clean .tmp files older than 24h ===")
    now = time.time()
    cutoff = now - 86400  # 24 hours in seconds
    count = 0
    total_bytes = 0

    for f in PROJECT_ROOT.rglob("*.tmp"):
        if ".git" in f.parts or ".claude" in f.parts:
            continue
        if not f.is_file():
            continue
        try:
            if f.stat().st_mtime < cutoff:
                size = f.stat().st_size
                f.unlink()
                count += 1
                total_bytes += size
        except OSError as exc:
            _warn(f"Could not remove {f}: {exc}")

    _cleaned += count
    mb = total_bytes / (1024 * 1024)
    logger.info(f"  Removed {count} old .tmp files ({mb:.2f} MB freed)")


# ===================================================================
# 3. Check disk space (warn if < 50GB free)
# ===================================================================
def check_disk_space() -> None:
    logger.info("=== Task 3: Check disk space ===")
    try:
        usage = shutil.disk_usage(str(PROJECT_ROOT))
        free_gb = usage.free / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        used_pct = (usage.used / usage.total) * 100

        if free_gb >= 50:
            logger.info(
                f"  OK: {free_gb:.1f} GB free / {total_gb:.1f} GB total "
                f"({used_pct:.0f}% used)"
            )
        else:
            _warn(
                f"Low disk space: {free_gb:.1f} GB free / {total_gb:.1f} GB total "
                f"({used_pct:.0f}% used) — threshold is 50 GB"
            )
    except Exception as exc:
        _error(f"Could not check disk space: {exc}")


# ===================================================================
# 4. Check data freshness (warn if partants_master older than 7 days)
# ===================================================================
def check_data_freshness() -> None:
    logger.info("=== Task 4: Check data freshness ===")
    files_to_check = [
        ("partants_master.jsonl", PARTANTS_MASTER),
    ]
    now = datetime.now(timezone.utc)

    for name, path in files_to_check:
        if not path.exists():
            _warn(f"{name} does not exist at {path}")
            continue
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        age_days = (now - mtime).days
        if age_days <= 7:
            logger.info(f"  OK: {name} modified {age_days} day(s) ago")
        else:
            _warn(f"{name} is {age_days} days old (> 7 days) — data may be stale")


# ===================================================================
# 5. Verify CHECKSUMS.sha256 for 3 random files
# ===================================================================
def verify_checksums() -> None:
    logger.info("=== Task 5: Verify CHECKSUMS.sha256 (3 random files) ===")
    checksums_file = DATA_MASTER_DIR / "CHECKSUMS.sha256"

    if not checksums_file.exists():
        _warn(f"CHECKSUMS.sha256 not found at {checksums_file}")
        return

    # Parse checksum file
    entries: list[tuple[str, str]] = []
    try:
        for line in checksums_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                expected_hash, rel_path = parts
                # Path in checksums is relative to project root
                entries.append((expected_hash, rel_path))
    except OSError as exc:
        _error(f"Could not read CHECKSUMS.sha256: {exc}")
        return

    if not entries:
        _warn("CHECKSUMS.sha256 is empty")
        return

    # Filter to files that actually exist
    existing = [
        (h, p) for h, p in entries
        if (PROJECT_ROOT / p).is_file()
    ]

    if not existing:
        _warn("No files from CHECKSUMS.sha256 found on disk")
        return

    # Pick up to 3 random files
    sample_size = min(3, len(existing))
    sample = random.sample(existing, sample_size)

    verified = 0
    for expected_hash, rel_path in sample:
        full_path = PROJECT_ROOT / rel_path
        try:
            sha256 = hashlib.sha256()
            with open(full_path, "rb") as fh:
                while True:
                    chunk = fh.read(65536)
                    if not chunk:
                        break
                    sha256.update(chunk)
            actual_hash = sha256.hexdigest()

            if actual_hash == expected_hash:
                logger.info(f"  OK: {rel_path} checksum matches")
                verified += 1
            else:
                _error(
                    f"Checksum MISMATCH for {rel_path}: "
                    f"expected {expected_hash[:16]}..., got {actual_hash[:16]}..."
                )
        except OSError as exc:
            _error(f"Could not read {rel_path} for checksum verification: {exc}")

    logger.info(f"  Verified {verified}/{sample_size} files")


# ===================================================================
# 6. Run diagnostic.py health check
# ===================================================================
def run_diagnostic() -> None:
    logger.info("=== Task 6: Run diagnostic.py health check ===")
    diagnostic_script = SCRIPT_DIR / "diagnostic.py"

    if not diagnostic_script.exists():
        _warn(f"diagnostic.py not found at {diagnostic_script}")
        return

    try:
        # Determine python executable
        python_exe = sys.executable or "python"
        result = subprocess.run(
            [python_exe, str(diagnostic_script)],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "NO_COLOR": "1"},
        )

        if result.returncode == 0:
            logger.info("  OK: diagnostic.py passed (exit code 0)")
        else:
            _warn(f"diagnostic.py returned exit code {result.returncode}")

        # Log last few lines of diagnostic output
        output_lines = result.stdout.strip().splitlines()
        if output_lines:
            for line in output_lines[-5:]:
                logger.info(f"  [diag] {line}")
        if result.stderr.strip():
            for line in result.stderr.strip().splitlines()[-3:]:
                logger.warning(f"  [diag-err] {line}")

    except subprocess.TimeoutExpired:
        _error("diagnostic.py timed out after 120 seconds")
    except FileNotFoundError:
        _error(f"Python executable not found: {sys.executable}")
    except Exception as exc:
        _error(f"Could not run diagnostic.py: {exc}")


# ===================================================================
# Main
# ===================================================================
def main() -> int:
    start = time.monotonic()

    logger.info("=" * 60)
    logger.info("  DAILY MAINTENANCE — turf-data-pipeline")
    logger.info(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Project root: {PROJECT_ROOT}")
    logger.info("=" * 60)

    clean_pycache()
    clean_tmp_files()
    check_disk_space()
    check_data_freshness()
    verify_checksums()
    run_diagnostic()

    elapsed = time.monotonic() - start

    # Summary
    logger.info("=" * 60)
    logger.info(f"  SUMMARY: {_cleaned} items cleaned, {_warnings} warnings, {_errors} errors")
    logger.info(f"  Duration: {elapsed:.1f}s")
    logger.info(f"  Log file: {LOGS_DIR / (_LOG_NAME + '.log')}")

    if _errors > 0:
        logger.error("  STATUS: ISSUES DETECTED — review errors above")
        logger.info("=" * 60)
        return 1
    elif _warnings > 0:
        logger.warning("  STATUS: OK with warnings")
        logger.info("=" * 60)
        return 0
    else:
        logger.info("  STATUS: ALL OK")
        logger.info("=" * 60)
        return 0


if __name__ == "__main__":
    sys.exit(main())
