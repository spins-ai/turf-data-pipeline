#!/usr/bin/env python3
"""
scripts/redundancy_checker.py — Pilier 4 : Redondance
=======================================================
Check that critical data files have backups or can be regenerated.

Verifies:
  - Checkpoint files exist for all scrapers
  - All master files have source files (output dirs that feed them)
  - Features can be recomputed from source data
  - Report single-point-of-failure data (files with no backup/regeneration path)

RAM budget: < 2 GB (file-system checks only, no data loading).

Usage:
    python scripts/redundancy_checker.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import COURSES_MASTER, DATA_MASTER_DIR, OUTPUT_DIR, PARTANTS_MASTER, PARTANTS_MASTER_ENRICHI, QUALITY_DIR
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"redundancy_checker_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_PATH = QUALITY_DIR / "redundancy_report.md"
FEATURE_BUILDERS_DIR = PROJECT_ROOT / "feature_builders"
CACHE_DIR = PROJECT_ROOT / "cache"
SECURITY_DIR = PROJECT_ROOT / "security"


# ---------------------------------------------------------------------------
# Alert
# ---------------------------------------------------------------------------
class Finding:
    """A single redundancy finding."""

    __slots__ = ("level", "category", "target", "message")

    def __init__(self, level: str, category: str, target: str, message: str):
        self.level = level  # "error", "warning", "ok"
        self.category = category
        self.target = target
        self.message = message


# ---------------------------------------------------------------------------
# 1. Checkpoint files for scrapers
# ---------------------------------------------------------------------------
def _discover_scrapers() -> list[tuple[str, Path]]:
    """Discover scraper scripts and their expected output dirs."""
    scrapers: list[tuple[str, Path]] = []

    # Root-level scraper scripts (XX_*.py)
    for py_file in sorted(PROJECT_ROOT.glob("[0-9][0-9]_*.py")):
        scrapers.append((py_file.stem, py_file))
    for py_file in sorted(PROJECT_ROOT.glob("[0-9][0-9][0-9]_*.py")):
        scrapers.append((py_file.stem, py_file))

    # Playwright scrapers
    pw_dir = PROJECT_ROOT / "scrapers_playwright"
    if pw_dir.is_dir():
        for py_file in sorted(pw_dir.glob("[0-9]*.py")):
            scrapers.append((py_file.stem, py_file))

    return scrapers


def check_scraper_checkpoints() -> list[Finding]:
    """Check that scrapers have checkpoint/state files."""
    findings: list[Finding] = []
    scrapers = _discover_scrapers()
    logger.info("Found %d scraper scripts", len(scrapers))

    checkpoint_patterns = [
        "checkpoint", "state", "progress", "resume", "last_run",
    ]

    for name, script_path in scrapers:
        # Extract the number prefix for output dir matching
        prefix = ""
        for ch in name:
            if ch.isdigit() or ch == "_":
                prefix += ch
            else:
                break
        prefix = prefix.rstrip("_")

        # Look for checkpoint files in output dir, cache, or alongside script
        has_checkpoint = False
        searched: list[str] = []

        # Check output dir
        if prefix:
            output_candidates = list(OUTPUT_DIR.glob(f"{prefix}_*"))
            for od in output_candidates:
                if od.is_dir():
                    for pat in checkpoint_patterns:
                        matches = list(od.glob(f"*{pat}*"))
                        if matches:
                            has_checkpoint = True
                            break
                if has_checkpoint:
                    break
            searched.append(f"output/{prefix}_*")

        # Check cache dir
        if not has_checkpoint and CACHE_DIR.is_dir():
            for pat in checkpoint_patterns:
                matches = list(CACHE_DIR.glob(f"*{name}*{pat}*"))
                if not matches:
                    matches = list(CACHE_DIR.glob(f"*{prefix}*{pat}*"))
                if matches:
                    has_checkpoint = True
                    break
            searched.append("cache/")

        if has_checkpoint:
            findings.append(
                Finding("ok", "checkpoint", name, "Checkpoint file found")
            )
        else:
            findings.append(
                Finding(
                    "warning",
                    "checkpoint",
                    name,
                    f"No checkpoint file found (searched: {', '.join(searched)})",
                )
            )

    return findings


# ---------------------------------------------------------------------------
# 2. Master files have source files
# ---------------------------------------------------------------------------

# Map of master files to their known source directories/files
MASTER_SOURCES: dict[str, list[str]] = {
    "partants_master.jsonl": [
        "output/02_liste_courses",
        "output/04_resultats",
        "output/40_enrichissement_partants",
    ],
    "courses_master.jsonl": [
        "output/02_liste_courses",
        "output/01_calendrier_reunions",
    ],
    "partants_master_enrichi.jsonl": [
        "data_master/partants_master.jsonl",
    ],
    "horse_career_stats.jsonl": [
        "output/05_historique_chevaux",
    ],
    "jockey_stats.jsonl": [
        "output/06_historique_jockeys",
    ],
    "trainer_stats.jsonl": [
        "output/06_historique_jockeys",
    ],
    "course_profiles.jsonl": [
        "data_master/courses_master.jsonl",
    ],
}


def check_master_sources() -> list[Finding]:
    """Check that master files have their source data available."""
    findings: list[Finding] = []

    for master_name, sources in MASTER_SOURCES.items():
        master_path = DATA_MASTER_DIR / master_name
        if not master_path.exists():
            findings.append(
                Finding(
                    "warning",
                    "master_missing",
                    master_name,
                    "Master file does not exist",
                )
            )
            continue

        missing_sources: list[str] = []
        present_sources: list[str] = []

        for source in sources:
            source_path = PROJECT_ROOT / source
            if source_path.exists():
                present_sources.append(source)
            else:
                missing_sources.append(source)

        if missing_sources:
            findings.append(
                Finding(
                    "warning",
                    "source_missing",
                    master_name,
                    f"Missing sources: {', '.join(missing_sources)}",
                )
            )
        else:
            findings.append(
                Finding(
                    "ok",
                    "master_sources",
                    master_name,
                    f"All {len(present_sources)} sources present",
                )
            )

    return findings


# ---------------------------------------------------------------------------
# 3. Features can be recomputed
# ---------------------------------------------------------------------------
def check_feature_recomputation() -> list[Finding]:
    """Check that feature data can be regenerated from builders + source data."""
    findings: list[Finding] = []

    # Check feature builders exist
    if not FEATURE_BUILDERS_DIR.is_dir():
        findings.append(
            Finding(
                "error",
                "feature_builders",
                "feature_builders/",
                "Feature builders directory missing",
            )
        )
        return findings

    builders = list(FEATURE_BUILDERS_DIR.glob("*.py"))
    builder_names = [b.stem for b in builders if not b.stem.startswith("_")]

    if not builder_names:
        findings.append(
            Finding(
                "error",
                "feature_builders",
                "feature_builders/",
                "No feature builder modules found",
            )
        )
        return findings

    findings.append(
        Finding(
            "ok",
            "feature_builders",
            "feature_builders/",
            f"{len(builder_names)} builder modules available for recomputation",
        )
    )

    # Check that source data for features exists
    if PARTANTS_MASTER.exists():
        findings.append(
            Finding(
                "ok",
                "feature_source",
                "partants_master.jsonl",
                "Source data for feature building exists",
            )
        )
    else:
        findings.append(
            Finding(
                "error",
                "feature_source",
                "partants_master.jsonl",
                "Primary source data for features is missing",
            )
        )

    # Check master_feature_builder script
    mfb = FEATURE_BUILDERS_DIR / "master_feature_builder.py"
    if mfb.exists():
        findings.append(
            Finding(
                "ok",
                "feature_orchestrator",
                "master_feature_builder.py",
                "Feature orchestrator script exists",
            )
        )
    else:
        findings.append(
            Finding(
                "warning",
                "feature_orchestrator",
                "master_feature_builder.py",
                "Feature orchestrator not found (features may not be auto-rebuildable)",
            )
        )

    return findings


# ---------------------------------------------------------------------------
# 4. Single points of failure
# ---------------------------------------------------------------------------
def check_single_points_of_failure() -> list[Finding]:
    """Identify files that cannot be regenerated if lost."""
    findings: list[Finding] = []

    # Critical files and whether they have a regeneration path
    critical_files: list[tuple[str, Path, bool, str]] = [
        (
            "partants_master.jsonl",
            PARTANTS_MASTER,
            True,
            "Can be rebuilt from output/ scrapers + mega_merge",
        ),
        (
            "courses_master.jsonl",
            COURSES_MASTER,
            True,
            "Can be rebuilt from output/02_liste_courses",
        ),
        (
            "partants_master_enrichi.jsonl",
            PARTANTS_MASTER_ENRICHI,
            True,
            "Can be rebuilt from partants_master + enrichment scripts",
        ),
    ]

    # Check for parquet duplicates (backup format)
    for jsonl_file in sorted(DATA_MASTER_DIR.glob("*.jsonl")):
        parquet_twin = jsonl_file.with_suffix(".parquet")
        has_parquet = parquet_twin.exists()
        name = jsonl_file.name

        # Is it in our known-regenerable list?
        known = any(n == name for n, _, _, _ in critical_files)

        if not has_parquet and not known:
            findings.append(
                Finding(
                    "warning",
                    "spof",
                    name,
                    "No parquet backup and no known regeneration path "
                    "(potential single point of failure)",
                )
            )
        elif has_parquet:
            findings.append(
                Finding(
                    "ok",
                    "backup_format",
                    name,
                    f"Parquet backup exists: {parquet_twin.name}",
                )
            )

    # Check checksums file
    checksums_path = SECURITY_DIR / "checksums.json"
    if checksums_path.exists():
        findings.append(
            Finding(
                "ok",
                "integrity",
                "checksums.json",
                "Integrity checksums file exists",
            )
        )
    else:
        findings.append(
            Finding(
                "warning",
                "integrity",
                "checksums.json",
                "No checksums file for integrity verification",
            )
        )

    # Check data_master SHA256
    sha_path = DATA_MASTER_DIR / "CHECKSUMS.sha256"
    if sha_path.exists():
        findings.append(
            Finding("ok", "integrity", "CHECKSUMS.sha256", "SHA256 checksums present")
        )

    return findings


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def generate_report(all_findings: list[Finding]) -> str:
    """Generate a Markdown redundancy report."""
    errors = [f for f in all_findings if f.level == "error"]
    warnings = [f for f in all_findings if f.level == "warning"]
    oks = [f for f in all_findings if f.level == "ok"]

    status = "PASS"
    if errors:
        status = "FAIL"
    elif warnings:
        status = "WARN"

    lines: list[str] = []
    lines.append("# Redundancy Check Report (Pilier 4)")
    lines.append(f"\nGenerated: {datetime.now().isoformat()}")

    lines.append(f"\n## Summary\n")
    lines.append(f"- Checks passed: {len(oks)}")
    lines.append(f"- Warnings: {len(warnings)}")
    lines.append(f"- Errors: {len(errors)}")
    lines.append(f"- **Status: {status}**")

    # Group by category
    categories: dict[str, list[Finding]] = {}
    for f in all_findings:
        categories.setdefault(f.category, []).append(f)

    for cat, items in sorted(categories.items()):
        lines.append(f"\n## {cat.replace('_', ' ').title()}\n")
        lines.append("| Status | Target | Details |")
        lines.append("|--------|--------|---------|")
        for item in items:
            icon = {"ok": "OK", "warning": "WARN", "error": "FAIL"}.get(
                item.level, "?"
            )
            msg = item.message.replace("|", "\\|")
            lines.append(f"| {icon} | {item.target} | {msg} |")

    # Single points of failure summary
    spofs = [f for f in all_findings if f.category == "spof"]
    if spofs:
        lines.append("\n## Single Points of Failure\n")
        lines.append(
            "The following files have no known backup or regeneration path:\n"
        )
        for f in spofs:
            lines.append(f"- **{f.target}**: {f.message}")
    else:
        lines.append("\n## Single Points of Failure\n")
        lines.append("No single points of failure detected.")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    logger.info("=== Redundancy Checker (Pilier 4) ===")

    all_findings: list[Finding] = []

    # 1. Scraper checkpoints
    logger.info("Checking scraper checkpoints ...")
    all_findings.extend(check_scraper_checkpoints())

    # 2. Master sources
    logger.info("Checking master file sources ...")
    all_findings.extend(check_master_sources())

    # 3. Feature recomputation
    logger.info("Checking feature recomputation path ...")
    all_findings.extend(check_feature_recomputation())

    # 4. Single points of failure
    logger.info("Checking for single points of failure ...")
    all_findings.extend(check_single_points_of_failure())

    # Summary
    errors = [f for f in all_findings if f.level == "error"]
    warnings = [f for f in all_findings if f.level == "warning"]
    oks = [f for f in all_findings if f.level == "ok"]
    logger.info(
        "Results: %d ok, %d warnings, %d errors", len(oks), len(warnings), len(errors)
    )

    # Generate report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report(all_findings)
    REPORT_PATH.write_text(report, encoding="utf-8")
    logger.info("Report: %s", REPORT_PATH)

    if errors:
        print(f"\n[FAIL] {len(errors)} redundancy errors. See {REPORT_PATH}")
        return 1
    elif warnings:
        print(f"\n[WARN] {len(warnings)} warnings. See {REPORT_PATH}")
        return 0
    else:
        print(f"\n[OK] All redundancy checks passed. Report: {REPORT_PATH}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
