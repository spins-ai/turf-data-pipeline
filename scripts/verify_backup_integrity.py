#!/usr/bin/env python3
"""
scripts/verify_backup_integrity.py — Verify backup integrity
==============================================================
Compares backup files against current master files:
  - File existence check
  - Size comparison
  - SHA256 checksum verification (if checksums.json exists)

Usage:
    python verify_backup_integrity.py
    python verify_backup_integrity.py --backup-dir backups/backup_20260315
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

BACKUPS_DIR = ROOT / "backups"
DATA_MASTER = ROOT / "data_master"
CHECKSUMS_FILE = ROOT / "security" / "checksums.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def sha256_file(path: Path) -> str:
    """Compute SHA256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def find_latest_backup() -> Path | None:
    """Find the most recent backup directory."""
    if not BACKUPS_DIR.exists():
        return None
    backups = sorted(
        [d for d in BACKUPS_DIR.iterdir() if d.is_dir() and d.name.startswith("backup")],
        key=lambda d: d.name,
        reverse=True,
    )
    return backups[0] if backups else None


def verify_backup(backup_dir: Path | None = None) -> dict:
    """Verify backup integrity."""
    if backup_dir is None:
        backup_dir = find_latest_backup()

    if backup_dir is None or not backup_dir.exists():
        logger.warning("No backup directory found.")
        return {"status": "no_backup", "details": []}

    logger.info(f"Verifying backup: {backup_dir}")

    # Load checksums if available
    checksums = {}
    if CHECKSUMS_FILE.exists():
        checksums = json.loads(CHECKSUMS_FILE.read_text(encoding="utf-8"))

    results = []
    masters = list(DATA_MASTER.glob("*")) if DATA_MASTER.exists() else []

    for master_file in masters:
        if not master_file.is_file():
            continue

        backup_file = backup_dir / master_file.name
        result = {
            "file": master_file.name,
            "master_size": master_file.stat().st_size,
            "backup_exists": backup_file.exists(),
        }

        if backup_file.exists():
            result["backup_size"] = backup_file.stat().st_size
            result["size_match"] = result["master_size"] == result["backup_size"]

            # Check checksum
            if master_file.name in checksums:
                expected = checksums[master_file.name]
                actual = sha256_file(backup_file)
                result["checksum_match"] = expected == actual
            else:
                result["checksum_match"] = "no_reference"

            result["status"] = "ok" if result["size_match"] else "size_mismatch"
        else:
            result["status"] = "missing"

        results.append(result)

    ok = sum(1 for r in results if r["status"] == "ok")
    missing = sum(1 for r in results if r["status"] == "missing")
    mismatch = sum(1 for r in results if r["status"] == "size_mismatch")

    report = {
        "backup_dir": str(backup_dir),
        "total_files": len(results),
        "ok": ok,
        "missing": missing,
        "size_mismatch": mismatch,
        "results": results,
    }

    logger.info(f"Results: {ok} OK, {missing} missing, {mismatch} size mismatch")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify backup integrity")
    parser.add_argument("--backup-dir", type=Path, help="Backup directory to verify")
    args = parser.parse_args()

    result = verify_backup(args.backup_dir)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
