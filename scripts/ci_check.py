#!/usr/bin/env python3
"""
scripts/ci_check.py — Pilier Reproductibilite & CI/CD
======================================================
Pre-commit / CI validation script for the turf-data pipeline.

Checks performed:
  1. py_compile all .py files
  2. Import check: try importing the utils package
  3. No hardcoded paths (C:\\Users, /home/, /Users/)
  4. No print() in production code (allowed in scripts/ and quality/)
  5. All feature builders have __main__ blocks
  6. Exit 0 if all pass, 1 if any fail

Usage:
    python scripts/ci_check.py

Can also be used as a pre-commit hook or CI step.
"""

from __future__ import annotations

import importlib
import py_compile
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.logging_setup import setup_logging  # noqa: E402

logger = setup_logging("ci_check")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
_errors: list[str] = []
_warnings: list[str] = []

# Directories where print() is allowed
_PRINT_ALLOWED_DIRS = {"scripts", "quality"}

# Patterns for hardcoded paths
_HARDCODED_PATH_RE = re.compile(
    r"""(?:C:\\Users|/home/|/Users/)""",
    re.IGNORECASE,
)

# Pattern for print() calls (not inside comments or strings — simplified heuristic)
_PRINT_CALL_RE = re.compile(r"^\s*print\s*\(")

# Pattern for __main__ guard
_MAIN_BLOCK_RE = re.compile(r"""if\s+__name__\s*==\s*['"]__main__['"]""")


def _collect_py_files() -> list[Path]:
    """Collect all .py files in the project, excluding __pycache__ and .git."""
    return sorted(
        f
        for f in PROJECT_ROOT.rglob("*.py")
        if "__pycache__" not in f.parts
        and ".git" not in f.parts
        and "node_modules" not in f.parts
        and ".claude" not in f.parts
    )


# ===================================================================
# 1. py_compile all .py files
# ===================================================================
def check_compile(py_files: list[Path]) -> int:
    """Compile-check every Python file. Returns number of failures."""
    logger.info("=== Check 1: py_compile all .py files ===")
    failures = 0
    for f in py_files:
        try:
            py_compile.compile(str(f), doraise=True)
        except py_compile.PyCompileError as exc:
            rel = f.relative_to(PROJECT_ROOT)
            msg = f"Compile error in {rel}: {exc}"
            logger.error(msg)
            _errors.append(msg)
            failures += 1
    if failures == 0:
        logger.info(f"  PASS: {len(py_files)} files compiled OK")
    else:
        logger.error(f"  FAIL: {failures}/{len(py_files)} files failed to compile")
    return failures


# ===================================================================
# 2. Import check: try importing utils package
# ===================================================================
def check_imports() -> int:
    """Try importing the utils package and its submodules. Returns failure count."""
    logger.info("=== Check 2: Import utils package ===")
    failures = 0

    # Try importing the package itself
    try:
        importlib.import_module("utils")
        logger.info("  PASS: import utils")
    except Exception as exc:
        msg = f"Cannot import utils: {type(exc).__name__}: {exc}"
        logger.error(f"  FAIL: {msg}")
        _errors.append(msg)
        failures += 1

    # Try importing key submodules
    utils_dir = PROJECT_ROOT / "utils"
    if utils_dir.is_dir():
        modules = sorted(
            f.stem
            for f in utils_dir.glob("*.py")
            if f.stem != "__init__" and not f.stem.startswith("_")
        )
        for mod_name in modules:
            full_name = f"utils.{mod_name}"
            try:
                importlib.import_module(full_name)
                logger.info(f"  PASS: import {full_name}")
            except Exception as exc:
                msg = f"Cannot import {full_name}: {type(exc).__name__}: {exc}"
                logger.error(f"  FAIL: {msg}")
                _errors.append(msg)
                failures += 1

    return failures


# ===================================================================
# 3. No hardcoded paths
# ===================================================================
def check_no_hardcoded_paths(py_files: list[Path]) -> int:
    """Check that no .py file contains hardcoded user paths. Returns failure count."""
    logger.info("=== Check 3: No hardcoded paths ===")
    failures = 0

    # config.py is allowed to have PYTHON_EXE with a hardcoded fallback
    skip_files = {"config.py", "ci_check.py"}

    for f in py_files:
        if f.name in skip_files:
            continue
        rel = f.relative_to(PROJECT_ROOT)
        try:
            content = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line_no, line in enumerate(content.splitlines(), 1):
            # Skip comments
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if _HARDCODED_PATH_RE.search(line):
                msg = f"Hardcoded path in {rel}:{line_no}: {line.strip()[:120]}"
                logger.error(f"  FAIL: {msg}")
                _errors.append(msg)
                failures += 1
                break  # One error per file is enough

    if failures == 0:
        logger.info(f"  PASS: No hardcoded paths found in {len(py_files)} files")
    else:
        logger.error(f"  FAIL: {failures} file(s) contain hardcoded paths")
    return failures


# ===================================================================
# 4. No print() in production code (allow in scripts/ and quality/)
# ===================================================================
def check_no_print_in_production(py_files: list[Path]) -> int:
    """Check that production code uses logging, not print(). Returns failure count."""
    logger.info("=== Check 4: No print() in production code ===")
    failures = 0

    for f in py_files:
        rel = f.relative_to(PROJECT_ROOT)
        parts = rel.parts

        # Allow print() in scripts/ and quality/ directories
        if any(p in _PRINT_ALLOWED_DIRS for p in parts):
            continue

        # Also allow in __init__.py and setup.py
        if f.name in ("__init__.py", "setup.py"):
            continue

        try:
            content = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        for line_no, line in enumerate(content.splitlines(), 1):
            stripped = line.lstrip()
            # Skip comments and strings
            if stripped.startswith("#"):
                continue
            if _PRINT_CALL_RE.match(stripped):
                msg = f"print() in production code {rel}:{line_no}"
                logger.error(f"  FAIL: {msg}")
                _errors.append(msg)
                failures += 1
                break  # One error per file is enough

    if failures == 0:
        logger.info("  PASS: No print() in production code")
    else:
        logger.error(f"  FAIL: {failures} file(s) use print() outside scripts/quality/")
    return failures


# ===================================================================
# 5. All feature builders have __main__ blocks
# ===================================================================
def check_feature_builders_main() -> int:
    """Check that all feature builders have if __name__ == '__main__' blocks."""
    logger.info("=== Check 5: Feature builders have __main__ blocks ===")
    fb_dir = PROJECT_ROOT / "feature_builders"
    if not fb_dir.is_dir():
        logger.warning("  WARN: feature_builders/ directory not found, skipping")
        return 0

    failures = 0
    builders = sorted(
        f
        for f in fb_dir.glob("*.py")
        if f.stem != "__init__" and not f.stem.startswith("_")
    )

    for f in builders:
        try:
            content = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if not _MAIN_BLOCK_RE.search(content):
            msg = f"Feature builder {f.name} missing __main__ block"
            logger.error(f"  FAIL: {msg}")
            _errors.append(msg)
            failures += 1

    checked = len(builders)
    if failures == 0:
        logger.info(f"  PASS: All {checked} feature builders have __main__ blocks")
    else:
        logger.error(f"  FAIL: {failures}/{checked} feature builders missing __main__")
    return failures


# ===================================================================
# Main
# ===================================================================
def main() -> int:
    logger.info("=" * 60)
    logger.info("  CI CHECK — turf-data-pipeline")
    logger.info("=" * 60)

    py_files = _collect_py_files()
    logger.info(f"Found {len(py_files)} Python files to check")

    total_failures = 0
    total_failures += check_compile(py_files)
    total_failures += check_imports()
    total_failures += check_no_hardcoded_paths(py_files)
    total_failures += check_no_print_in_production(py_files)
    total_failures += check_feature_builders_main()

    # Summary
    logger.info("=" * 60)
    if total_failures == 0:
        logger.info(f"  ALL CHECKS PASSED ({len(_errors)} errors, {len(_warnings)} warnings)")
        logger.info("=" * 60)
        return 0
    else:
        logger.error(f"  {total_failures} CHECK(S) FAILED")
        for err in _errors:
            logger.error(f"    - {err}")
        logger.info("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
