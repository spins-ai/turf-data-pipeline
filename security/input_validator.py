#!/usr/bin/env python3
"""
security/input_validator.py — Pilier 2 : Securite - Validation des entrees
===========================================================================
Validate all input data before processing to detect malicious or corrupt data.

Checks:
  - No SQL injection patterns in string fields
  - No path traversal in filenames (../, ~/, etc.)
  - No excessively long strings (> 10 KB)
  - No binary data in text fields
  - Checkpoint files: valid JSON, reasonable date values, no negative counts
  - Sample 1000 records from partants_master, report suspicious values

RAM budget: < 2 GB (streams files, reservoir sampling).

Usage:
    python security/input_validator.py
"""

from __future__ import annotations

import json
import random
import re
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATA_MASTER_DIR, OUTPUT_DIR, PARTANTS_MASTER, QUALITY_DIR
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"input_validator_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SAMPLE_SIZE = 1_000
RESERVOIR_SEED = 42
MAX_STRING_LEN = 10_240  # 10 KB
REPORT_PATH = QUALITY_DIR / "input_validation_report.md"

# Date range considered "reasonable" for racing data
MIN_REASONABLE_DATE = "1900-01-01"
MAX_REASONABLE_DATE = "2030-12-31"

# ---------------------------------------------------------------------------
# SQL injection patterns (common payloads)
# ---------------------------------------------------------------------------
SQL_INJECTION_PATTERNS = [
    re.compile(r"('|\")\s*(OR|AND)\s+('|\")?\s*\d+\s*=\s*\d+", re.IGNORECASE),
    re.compile(r";\s*(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE)\s+", re.IGNORECASE),
    re.compile(r"UNION\s+(ALL\s+)?SELECT\s+", re.IGNORECASE),
    re.compile(r"--\s*$", re.MULTILINE),
    re.compile(r"/\*.*?\*/", re.DOTALL),
    re.compile(r"xp_cmdshell", re.IGNORECASE),
    re.compile(r"EXEC(\s+|\()sp_", re.IGNORECASE),
    re.compile(r"WAITFOR\s+DELAY", re.IGNORECASE),
    re.compile(r"BENCHMARK\s*\(", re.IGNORECASE),
    re.compile(r"LOAD_FILE\s*\(", re.IGNORECASE),
]

# ---------------------------------------------------------------------------
# Path traversal patterns
# ---------------------------------------------------------------------------
PATH_TRAVERSAL_PATTERNS = [
    re.compile(r"\.\./"),
    re.compile(r"\.\.\\"),
    re.compile(r"~/"),
    re.compile(r"%2e%2e", re.IGNORECASE),
    re.compile(r"%252e%252e", re.IGNORECASE),
    re.compile(r"/etc/passwd", re.IGNORECASE),
    re.compile(r"\\windows\\", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# Validation functions
# ---------------------------------------------------------------------------
class ValidationIssue:
    """Represents a single validation issue found."""

    __slots__ = ("level", "category", "field", "message", "value_preview")

    def __init__(
        self,
        level: str,
        category: str,
        field: str,
        message: str,
        value_preview: str = "",
    ):
        self.level = level  # "error", "warning"
        self.category = category
        self.field = field
        self.message = message
        self.value_preview = value_preview[:200] if value_preview else ""


def _has_binary_data(s: str) -> bool:
    """Check if a string contains non-printable binary characters."""
    if not s:
        return False
    # Allow common whitespace and printable ASCII + extended unicode
    for ch in s[:2000]:  # only check first 2000 chars for performance
        cp = ord(ch)
        if cp < 32 and cp not in (9, 10, 13):  # tab, newline, CR
            return True
    return False


def validate_string_field(
    field_name: str, value: str, record_idx: int
) -> list[ValidationIssue]:
    """Validate a single string field for security issues."""
    issues: list[ValidationIssue] = []

    # Too long
    if len(value) > MAX_STRING_LEN:
        issues.append(
            ValidationIssue(
                "warning",
                "long_string",
                field_name,
                f"String length {len(value)} exceeds {MAX_STRING_LEN} at record {record_idx}",
                value[:100],
            )
        )

    # SQL injection
    for pat in SQL_INJECTION_PATTERNS:
        if pat.search(value):
            issues.append(
                ValidationIssue(
                    "error",
                    "sql_injection",
                    field_name,
                    f"SQL injection pattern at record {record_idx}: {pat.pattern}",
                    value[:100],
                )
            )
            break  # one match is enough

    # Path traversal
    for pat in PATH_TRAVERSAL_PATTERNS:
        if pat.search(value):
            issues.append(
                ValidationIssue(
                    "error",
                    "path_traversal",
                    field_name,
                    f"Path traversal pattern at record {record_idx}: {pat.pattern}",
                    value[:100],
                )
            )
            break

    # Binary data
    if _has_binary_data(value):
        issues.append(
            ValidationIssue(
                "warning",
                "binary_data",
                field_name,
                f"Binary data in text field at record {record_idx}",
                repr(value[:80]),
            )
        )

    return issues


def validate_record(record: dict, idx: int) -> list[ValidationIssue]:
    """Validate all fields in a single record."""
    issues: list[ValidationIssue] = []
    for key, value in record.items():
        if isinstance(value, str):
            issues.extend(validate_string_field(key, value, idx))
        elif isinstance(value, dict):
            # Check nested dicts (one level deep only to limit RAM)
            for sub_key, sub_val in value.items():
                if isinstance(sub_val, str):
                    issues.extend(
                        validate_string_field(f"{key}.{sub_key}", sub_val, idx)
                    )
    return issues


# ---------------------------------------------------------------------------
# Checkpoint validation
# ---------------------------------------------------------------------------
def validate_checkpoint_file(path: Path) -> list[ValidationIssue]:
    """Validate a JSON checkpoint file for integrity."""
    issues: list[ValidationIssue] = []
    fname = path.name

    # Must be valid JSON
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        issues.append(
            ValidationIssue(
                "error",
                "checkpoint_json",
                fname,
                f"Invalid JSON: {exc}",
            )
        )
        return issues
    except OSError as exc:
        issues.append(
            ValidationIssue(
                "error",
                "checkpoint_io",
                fname,
                f"Cannot read file: {exc}",
            )
        )
        return issues

    # Check for reasonable values in checkpoint data
    if isinstance(data, dict):
        _validate_checkpoint_dict(data, fname, issues)
    elif isinstance(data, list):
        for i, item in enumerate(data[:100]):  # limit to first 100 items
            if isinstance(item, dict):
                _validate_checkpoint_dict(item, f"{fname}[{i}]", issues)

    return issues


def _validate_checkpoint_dict(
    data: dict, context: str, issues: list[ValidationIssue]
) -> None:
    """Validate dict values in a checkpoint."""
    for key, value in data.items():
        # Negative counts
        if "count" in key.lower() or "total" in key.lower():
            if isinstance(value, (int, float)) and value < 0:
                issues.append(
                    ValidationIssue(
                        "error",
                        "checkpoint_negative",
                        f"{context}.{key}",
                        f"Negative count value: {value}",
                    )
                )

        # Date fields
        if "date" in key.lower() or "timestamp" in key.lower():
            if isinstance(value, str) and value:
                try:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    if (
                        dt.year < 1900
                        or dt.year > 2030
                    ):
                        issues.append(
                            ValidationIssue(
                                "warning",
                                "checkpoint_date",
                                f"{context}.{key}",
                                f"Date out of range: {value}",
                            )
                        )
                except (ValueError, TypeError):
                    # Not all date-like fields are ISO dates, skip silently
                    pass


# ---------------------------------------------------------------------------
# Discover checkpoint files
# ---------------------------------------------------------------------------
def discover_checkpoints() -> list[Path]:
    """Find JSON checkpoint files in the project."""
    checkpoints: list[Path] = []
    search_dirs = [OUTPUT_DIR, DATA_MASTER_DIR, PROJECT_ROOT / "cache"]
    patterns = ["*checkpoint*.json", "*checkpoint*.jsonl", "*state*.json"]

    for d in search_dirs:
        if not d.is_dir():
            continue
        for pat in patterns:
            checkpoints.extend(d.glob(f"**/{pat}"))

    # Also check any JSON files directly in output sub-dirs that look like state
    if OUTPUT_DIR.is_dir():
        for p in OUTPUT_DIR.glob("*/*.json"):
            if any(
                kw in p.stem.lower()
                for kw in ("checkpoint", "state", "progress", "resume")
            ):
                if p not in checkpoints:
                    checkpoints.append(p)

    return sorted(set(checkpoints))


# ---------------------------------------------------------------------------
# Sample and validate partants_master
# ---------------------------------------------------------------------------
def validate_partants_sample() -> tuple[list[ValidationIssue], int]:
    """Reservoir-sample SAMPLE_SIZE records and validate them.

    Returns (issues, total_records_seen).
    """
    issues: list[ValidationIssue] = []

    if not PARTANTS_MASTER.exists():
        issues.append(
            ValidationIssue(
                "error",
                "file_missing",
                "partants_master.jsonl",
                "File not found",
            )
        )
        return issues, 0

    reservoir: list[tuple[int, dict]] = []
    random.seed(RESERVOIR_SEED)
    total = 0

    with open(PARTANTS_MASTER, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                issues.append(
                    ValidationIssue(
                        "warning",
                        "parse_error",
                        "partants_master.jsonl",
                        f"Invalid JSON at line {line_no + 1}",
                    )
                )
                continue

            # Reservoir sampling
            if len(reservoir) < SAMPLE_SIZE:
                reservoir.append((line_no, rec))
            else:
                j = random.randint(0, total - 1)
                if j < SAMPLE_SIZE:
                    reservoir[j] = (line_no, rec)

    logger.info(
        "Scanned %d records from partants_master, sampled %d",
        total,
        len(reservoir),
    )

    # Validate sampled records
    for idx, (line_no, rec) in enumerate(reservoir):
        rec_issues = validate_record(rec, line_no)
        issues.extend(rec_issues)

    return issues, total


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def generate_report(
    partant_issues: list[ValidationIssue],
    checkpoint_issues: list[ValidationIssue],
    total_records: int,
    num_checkpoints: int,
) -> str:
    """Generate a Markdown validation report."""
    all_issues = partant_issues + checkpoint_issues
    errors = [i for i in all_issues if i.level == "error"]
    warnings = [i for i in all_issues if i.level == "warning"]

    lines: list[str] = []
    lines.append("# Input Validation Report (Pilier 2 - Securite)")
    lines.append(f"\nGenerated: {datetime.now().isoformat()}")
    lines.append(f"\n## Summary\n")
    lines.append(f"- partants_master records scanned: {total_records}")
    lines.append(f"- Sample size: {SAMPLE_SIZE}")
    lines.append(f"- Checkpoint files checked: {num_checkpoints}")
    lines.append(f"- **Errors: {len(errors)}**")
    lines.append(f"- **Warnings: {len(warnings)}**")

    status = "PASS" if len(errors) == 0 else "FAIL"
    lines.append(f"- **Status: {status}**")

    # Breakdown by category
    categories: dict[str, int] = {}
    for issue in all_issues:
        categories[issue.category] = categories.get(issue.category, 0) + 1

    if categories:
        lines.append("\n## Issues by Category\n")
        lines.append("| Category | Count |")
        lines.append("|----------|-------|")
        for cat, cnt in sorted(categories.items(), key=lambda x: -x[1]):
            lines.append(f"| {cat} | {cnt} |")

    # Detail: errors first, then warnings (cap at 50 each)
    if errors:
        lines.append(f"\n## Errors ({len(errors)})\n")
        lines.append("| Category | Field | Message |")
        lines.append("|----------|-------|---------|")
        for issue in errors[:50]:
            msg = issue.message.replace("|", "\\|")
            lines.append(f"| {issue.category} | {issue.field} | {msg} |")
        if len(errors) > 50:
            lines.append(f"\n... and {len(errors) - 50} more errors.")

    if warnings:
        lines.append(f"\n## Warnings ({len(warnings)})\n")
        lines.append("| Category | Field | Message |")
        lines.append("|----------|-------|---------|")
        for issue in warnings[:50]:
            msg = issue.message.replace("|", "\\|")
            lines.append(f"| {issue.category} | {issue.field} | {msg} |")
        if len(warnings) > 50:
            lines.append(f"\n... and {len(warnings) - 50} more warnings.")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    logger.info("=== Input Validator (Pilier 2 - Securite) ===")

    # 1. Validate partants_master sample
    logger.info("Validating partants_master sample (%d records) ...", SAMPLE_SIZE)
    partant_issues, total_records = validate_partants_sample()
    logger.info("  Found %d issues in partants data", len(partant_issues))

    # 2. Validate checkpoint files
    checkpoints = discover_checkpoints()
    logger.info("Found %d checkpoint files to validate", len(checkpoints))
    checkpoint_issues: list[ValidationIssue] = []
    for cp in checkpoints:
        checkpoint_issues.extend(validate_checkpoint_file(cp))
    logger.info("  Found %d issues in checkpoints", len(checkpoint_issues))

    # 3. Generate report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report(
        partant_issues, checkpoint_issues, total_records, len(checkpoints)
    )
    REPORT_PATH.write_text(report, encoding="utf-8")
    logger.info("Report written to %s", REPORT_PATH)

    all_errors = [
        i
        for i in partant_issues + checkpoint_issues
        if i.level == "error"
    ]
    if all_errors:
        print(f"\n[WARN] {len(all_errors)} security issues found. See {REPORT_PATH}")
        return 1
    else:
        print(f"\n[OK] No security issues. Report: {REPORT_PATH}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
