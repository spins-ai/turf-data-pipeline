#!/usr/bin/env python3
"""
sync_checker.py -- Pilier 11 : Synchronisation Inter-Blocs.

Verifie la coherence entre les differentes sorties du pipeline :
  1. partants_master record count vs labels count
  2. features_matrix record count vs partants_master
  3. Tous les course_uid dans features existent dans courses_master
  4. Toutes les dates dans labels existent dans partants_master
  5. Cross-checks entre fichiers master

Utilise le streaming JSONL (ijson/ligne par ligne) pour rester sous 2 GB RAM.

Genere un rapport dans quality/sync_report.md.

Usage :
    python scripts/sync_checker.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    COURSES_MASTER,
    DATA_MASTER_DIR,
    FEATURES_DIR,
    FEATURES_MATRIX,
    LABELS_DIR,
    PARTANTS_MASTER,
    PARTANTS_MASTER_ENRICHI,
    QUALITY_DIR,
    TRAINING_LABELS,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_SAMPLE_MISMATCHES = 20  # Max examples to show per check


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class SyncCheck:
    """Stores a synchronisation check result."""

    __slots__ = ("name", "passed", "detail", "mismatches")

    def __init__(
        self,
        name: str,
        passed: bool,
        detail: str = "",
        mismatches: list[str] | None = None,
    ) -> None:
        self.name = name
        self.passed = passed
        self.detail = detail
        self.mismatches = mismatches or []


def count_jsonl_lines(path: Path) -> int:
    """Count non-empty lines in a JSONL file (streaming, low RAM)."""
    if not path.exists():
        return -1
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def extract_field_set(path: Path, field: str, limit: int = 0) -> set[str]:
    """Extract a set of unique values for a given field from JSONL (streaming).

    If limit > 0, stop after collecting that many unique values.
    """
    values: set[str] = set()
    if not path.exists():
        return values
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                val = obj.get(field)
                if val is not None:
                    values.add(str(val))
                if limit and len(values) >= limit:
                    break
            except (json.JSONDecodeError, KeyError):
                continue
    return values


def extract_field_set_parquet(path: Path, field: str) -> set[str]:
    """Extract unique values from a parquet column (if pyarrow available)."""
    values: set[str] = set()
    if not path.exists():
        return values
    try:
        import pyarrow.parquet as pq

        table = pq.read_table(path, columns=[field])
        col = table.column(field)
        for chunk in col.chunks:
            for val in chunk.to_pylist():
                if val is not None:
                    values.add(str(val))
    except Exception:
        pass
    return values


# ---------------------------------------------------------------------------
# Individual sync checks
# ---------------------------------------------------------------------------

def check_partants_vs_labels() -> SyncCheck:
    """Check partants_master record count vs training labels."""
    p_count = count_jsonl_lines(PARTANTS_MASTER)
    l_count = count_jsonl_lines(TRAINING_LABELS)

    if p_count < 0:
        return SyncCheck(
            "Partants vs Labels (count)",
            False,
            f"partants_master not found at {PARTANTS_MASTER}",
        )
    if l_count < 0:
        return SyncCheck(
            "Partants vs Labels (count)",
            False,
            f"training_labels not found at {TRAINING_LABELS}",
        )

    # Labels should be <= partants (not all partants have labels)
    ok = l_count <= p_count
    detail = (
        f"partants_master: {p_count:,} records, "
        f"training_labels: {l_count:,} records"
    )
    if not ok:
        detail += " (ANOMALY: more labels than partants)"
    return SyncCheck("Partants vs Labels (count)", ok, detail)


def check_features_vs_partants() -> SyncCheck:
    """Check features_matrix record count vs partants_master."""
    p_count = count_jsonl_lines(PARTANTS_MASTER)
    f_count = count_jsonl_lines(FEATURES_MATRIX)

    if p_count < 0:
        return SyncCheck(
            "Features vs Partants (count)",
            False,
            f"partants_master not found",
        )
    if f_count < 0:
        return SyncCheck(
            "Features vs Partants (count)",
            False,
            f"features_matrix not found at {FEATURES_MATRIX}",
        )

    # Features should be close to partants count
    ratio = f_count / p_count if p_count > 0 else 0
    ok = 0.5 <= ratio <= 1.1  # allow some tolerance
    detail = (
        f"partants_master: {p_count:,}, "
        f"features_matrix: {f_count:,} "
        f"(ratio: {ratio:.2f})"
    )
    if not ok:
        detail += " (ANOMALY: significant mismatch)"
    return SyncCheck("Features vs Partants (count)", ok, detail)


def check_course_uids_in_features() -> SyncCheck:
    """Check all course_uid in features exist in courses_master."""
    # Build set of valid course_uids from courses_master
    courses_uids: set[str] = set()
    courses_path = COURSES_MASTER

    if not courses_path.exists():
        # Try parquet fallback
        parquet_path = DATA_MASTER_DIR / "courses_master.parquet"
        if parquet_path.exists():
            courses_uids = extract_field_set_parquet(parquet_path, "course_uid")
        else:
            return SyncCheck(
                "Course UIDs (features -> courses)",
                False,
                "courses_master not found (neither JSONL nor parquet)",
            )
    else:
        courses_uids = extract_field_set(courses_path, "course_uid")

    if not courses_uids:
        return SyncCheck(
            "Course UIDs (features -> courses)",
            False,
            "No course_uid values found in courses_master",
        )

    # Stream features and check course_uids
    if not FEATURES_MATRIX.exists():
        return SyncCheck(
            "Course UIDs (features -> courses)",
            False,
            f"features_matrix not found at {FEATURES_MATRIX}",
        )

    missing: list[str] = []
    total_checked = 0
    with open(FEATURES_MATRIX, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                uid = obj.get("course_uid")
                if uid is not None and str(uid) not in courses_uids:
                    if len(missing) < MAX_SAMPLE_MISMATCHES:
                        missing.append(str(uid))
                total_checked += 1
            except (json.JSONDecodeError, KeyError):
                continue

    ok = len(missing) == 0
    detail = f"Checked {total_checked:,} feature records against {len(courses_uids):,} course UIDs"
    if missing:
        detail += f"; {len(missing)} orphan course_uid(s) found"
    return SyncCheck(
        "Course UIDs (features -> courses)",
        ok,
        detail,
        missing[:MAX_SAMPLE_MISMATCHES],
    )


def check_dates_in_labels() -> SyncCheck:
    """Check all dates in labels exist in partants_master."""
    # Collect dates from partants
    partants_dates: set[str] = set()
    if not PARTANTS_MASTER.exists():
        return SyncCheck(
            "Dates (labels -> partants)",
            False,
            "partants_master not found",
        )

    # Stream partants for dates (field: dateReunion or date)
    with open(PARTANTS_MASTER, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                for key in ("dateReunion", "date", "date_reunion"):
                    val = obj.get(key)
                    if val:
                        # Normalise to YYYY-MM-DD
                        date_str = str(val)[:10]
                        partants_dates.add(date_str)
                        break
            except (json.JSONDecodeError, KeyError):
                continue

    if not partants_dates:
        return SyncCheck(
            "Dates (labels -> partants)",
            False,
            "No dates found in partants_master",
        )

    # Stream labels for dates
    if not TRAINING_LABELS.exists():
        return SyncCheck(
            "Dates (labels -> partants)",
            False,
            "training_labels not found",
        )

    missing_dates: list[str] = []
    labels_dates: set[str] = set()
    with open(TRAINING_LABELS, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                for key in ("dateReunion", "date", "date_reunion"):
                    val = obj.get(key)
                    if val:
                        date_str = str(val)[:10]
                        labels_dates.add(date_str)
                        if date_str not in partants_dates:
                            if len(missing_dates) < MAX_SAMPLE_MISMATCHES:
                                missing_dates.append(date_str)
                        break
            except (json.JSONDecodeError, KeyError):
                continue

    ok = len(missing_dates) == 0
    detail = (
        f"{len(labels_dates)} unique dates in labels, "
        f"{len(partants_dates)} unique dates in partants"
    )
    if missing_dates:
        detail += f"; {len(missing_dates)} label date(s) not in partants"
    return SyncCheck(
        "Dates (labels -> partants)",
        ok,
        detail,
        missing_dates[:MAX_SAMPLE_MISMATCHES],
    )


def check_master_files_exist() -> SyncCheck:
    """Check that all expected master files exist and are non-empty."""
    expected = [
        PARTANTS_MASTER,
        COURSES_MASTER,
        FEATURES_MATRIX,
        TRAINING_LABELS,
    ]
    missing: list[str] = []
    empty: list[str] = []

    for p in expected:
        if not p.exists():
            missing.append(str(p.name))
        elif p.stat().st_size == 0:
            empty.append(str(p.name))

    ok = len(missing) == 0 and len(empty) == 0
    problems: list[str] = []
    if missing:
        problems.append(f"missing: {', '.join(missing)}")
    if empty:
        problems.append(f"empty: {', '.join(empty)}")

    detail = "All master files present and non-empty" if ok else "; ".join(problems)
    return SyncCheck("Master files exist", ok, detail, missing + empty)


def check_enrichi_vs_base() -> SyncCheck:
    """Check enriched partants count >= base partants count."""
    if not PARTANTS_MASTER_ENRICHI.exists():
        return SyncCheck(
            "Enrichi vs Base partants",
            True,  # Not a failure if enrichi doesn't exist yet
            "partants_master_enrichi not found (optional)",
        )

    base_count = count_jsonl_lines(PARTANTS_MASTER)
    enrichi_count = count_jsonl_lines(PARTANTS_MASTER_ENRICHI)

    if base_count < 0:
        return SyncCheck(
            "Enrichi vs Base partants",
            False,
            "partants_master not found",
        )

    # Enrichi should have same or fewer records (some may be filtered)
    ratio = enrichi_count / base_count if base_count > 0 else 0
    ok = 0.5 <= ratio <= 1.05
    detail = (
        f"base: {base_count:,}, enrichi: {enrichi_count:,} "
        f"(ratio: {ratio:.2f})"
    )
    if not ok:
        detail += " (ANOMALY)"
    return SyncCheck("Enrichi vs Base partants", ok, detail)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def generate_report(checks: list[SyncCheck]) -> str:
    """Generate the sync check markdown report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    total = len(checks)
    passed = sum(1 for c in checks if c.passed)
    failed = total - passed

    lines: list[str] = [
        "# Synchronisation Report (Pilier 11)",
        "",
        f"Generated: {now}",
        "",
        f"**Results: {passed}/{total} passed, {failed} failed**",
        "",
        "## Check Results",
        "",
        "| Status | Check | Detail |",
        "|--------|-------|--------|",
    ]

    for c in checks:
        icon = "PASS" if c.passed else "FAIL"
        lines.append(f"| {icon} | {c.name} | {c.detail} |")

    lines.append("")

    # Detail mismatches
    has_mismatches = any(c.mismatches for c in checks)
    if has_mismatches:
        lines.append("## Mismatch Details")
        lines.append("")
        for c in checks:
            if c.mismatches:
                lines.append(f"### {c.name}")
                lines.append("")
                lines.append(
                    f"Sample mismatches (up to {MAX_SAMPLE_MISMATCHES}):"
                )
                lines.append("")
                for m in c.mismatches:
                    lines.append(f"- `{m}`")
                lines.append("")

    # Failures summary
    if failed > 0:
        lines.append("## Action Items")
        lines.append("")
        for c in checks:
            if not c.passed:
                lines.append(f"- **{c.name}**: {c.detail}")
        lines.append("")

    lines.append("---")
    lines.append("*Report generated by sync_checker.py (Pilier 11)*")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """Run all synchronisation checks."""
    print("=== Pilier 11 : Sync Checker ===\n")

    checks: list[SyncCheck] = []

    # 1. Master files exist
    print("Checking master files exist...")
    checks.append(check_master_files_exist())

    # 2. Partants vs Labels
    print("Checking partants vs labels count...")
    checks.append(check_partants_vs_labels())

    # 3. Features vs Partants
    print("Checking features vs partants count...")
    checks.append(check_features_vs_partants())

    # 4. Course UIDs
    print("Checking course_uid consistency (features -> courses)...")
    checks.append(check_course_uids_in_features())

    # 5. Dates in labels
    print("Checking dates consistency (labels -> partants)...")
    checks.append(check_dates_in_labels())

    # 6. Enrichi vs base
    print("Checking enrichi vs base partants...")
    checks.append(check_enrichi_vs_base())

    # Print summary
    passed = sum(1 for c in checks if c.passed)
    failed = sum(1 for c in checks if not c.passed)
    print(f"\nResults: {passed} passed, {failed} failed\n")

    for c in checks:
        tag = "  OK " if c.passed else "FAIL "
        print(f"  [{tag}] {c.name}: {c.detail}")

    # Write report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report_path = QUALITY_DIR / "sync_report.md"
    report_content = generate_report(checks)
    report_path.write_text(report_content, encoding="utf-8")
    print(f"\nReport written to {report_path}")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
