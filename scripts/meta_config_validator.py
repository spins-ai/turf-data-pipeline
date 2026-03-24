#!/usr/bin/env python3
"""
scripts/meta_config_validator.py — Pilier 22 : Meta-Configuration
==================================================================
Validate config.py and pipeline_config.yaml.

Checks:
  1. All paths in config.py exist or are creatable
  2. All scripts referenced in pipeline_config.yaml exist on disk
  3. RAM budgets don't exceed system RAM
  4. No circular dependencies in the pipeline DAG
  5. All timeouts are reasonable (> 60s, < 24h)

Output:
  - quality/meta_config_report.md

RAM budget: < 1 GB (reads config files, no data loading).

Usage:
    python scripts/meta_config_validator.py
"""

from __future__ import annotations

import importlib
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    BASE_DIR,
    QUALITY_DIR,
)
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"meta_config_validator_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_PATH = QUALITY_DIR / "meta_config_report.md"
CONFIG_YAML_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"
MIN_TIMEOUT_S = 60
MAX_TIMEOUT_S = 86400  # 24 hours


# ---------------------------------------------------------------------------
# Validation checks
# ---------------------------------------------------------------------------
def _get_system_ram_mb() -> int | None:
    """Get total system RAM in MB."""
    try:
        import psutil
        return int(psutil.virtual_memory().total / (1024 * 1024))
    except ImportError:
        pass
    # Fallback: read from OS
    try:
        if sys.platform == "win32":
            import ctypes
            kernel32 = ctypes.windll.kernel32
            c_ulonglong = ctypes.c_ulonglong
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", c_ulonglong),
                    ("ullAvailPhys", c_ulonglong),
                    ("ullTotalPageFile", c_ulonglong),
                    ("ullAvailPageFile", c_ulonglong),
                    ("ullTotalVirtual", c_ulonglong),
                    ("ullAvailVirtual", c_ulonglong),
                    ("ullAvailExtendedVirtual", c_ulonglong),
                ]
            mem = MEMORYSTATUSEX()
            mem.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            kernel32.GlobalMemoryStatusEx(ctypes.byref(mem))
            return int(mem.ullTotalPhys / (1024 * 1024))
        else:
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        return kb // 1024
    except Exception:
        pass
    return None


def check_config_paths() -> list[dict[str, Any]]:
    """Check that all Path-type attributes in config.py exist or are creatable."""
    issues: list[dict[str, Any]] = []

    try:
        import config as cfg
    except ImportError as e:
        issues.append({"check": "config_import", "status": "FAIL", "detail": str(e)})
        return issues

    for attr_name in dir(cfg):
        if attr_name.startswith("_"):
            continue
        val = getattr(cfg, attr_name)
        if not isinstance(val, Path):
            continue

        # Skip if it's a file path (check parent dir exists)
        if "." in val.name:  # likely a file
            parent = val.parent
            if parent.exists():
                issues.append({
                    "check": f"path:{attr_name}",
                    "status": "OK",
                    "detail": f"Parent dir exists: {parent}",
                })
            else:
                # Check if it's creatable
                try:
                    parent.mkdir(parents=True, exist_ok=True)
                    issues.append({
                        "check": f"path:{attr_name}",
                        "status": "WARN",
                        "detail": f"Parent dir created: {parent}",
                    })
                except OSError as e:
                    issues.append({
                        "check": f"path:{attr_name}",
                        "status": "FAIL",
                        "detail": f"Cannot create parent dir: {e}",
                    })
        else:  # likely a directory
            if val.exists():
                issues.append({
                    "check": f"path:{attr_name}",
                    "status": "OK",
                    "detail": f"Dir exists: {val}",
                })
            else:
                try:
                    val.mkdir(parents=True, exist_ok=True)
                    issues.append({
                        "check": f"path:{attr_name}",
                        "status": "WARN",
                        "detail": f"Dir created: {val}",
                    })
                except OSError as e:
                    issues.append({
                        "check": f"path:{attr_name}",
                        "status": "FAIL",
                        "detail": f"Cannot create dir: {e}",
                    })

    return issues


def check_yaml_scripts() -> list[dict[str, Any]]:
    """Check all scripts referenced in pipeline_config.yaml exist on disk."""
    issues: list[dict[str, Any]] = []

    if not CONFIG_YAML_PATH.exists():
        issues.append({
            "check": "yaml_exists",
            "status": "WARN",
            "detail": f"pipeline_config.yaml not found at {CONFIG_YAML_PATH}",
        })
        return issues

    try:
        import yaml
    except ImportError:
        # Fallback: parse YAML manually for script references
        logger.info("PyYAML not installed, using regex fallback")
        return _check_yaml_scripts_regex()

    try:
        with open(CONFIG_YAML_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        issues.append({"check": "yaml_parse", "status": "FAIL", "detail": str(e)})
        return issues

    phases = config.get("phases", [])
    for phase in phases:
        scripts = phase.get("scripts", [])
        for script_info in scripts:
            script_name = script_info.get("script", "")
            if not script_name:
                continue

            # Search in project root and common dirs
            found = False
            for search_dir in [PROJECT_ROOT, PROJECT_ROOT / "pipeline", PROJECT_ROOT / "scripts"]:
                candidate = search_dir / script_name
                if candidate.exists():
                    found = True
                    break
                # Also check subdirectories
                for sub in search_dir.glob(f"**/{script_name}"):
                    found = True
                    break
                if found:
                    break

            status = "OK" if found else "FAIL"
            issues.append({
                "check": f"script:{script_name}",
                "status": status,
                "detail": f"Phase {phase.get('phase', '?')}: {script_name} {'found' if found else 'NOT FOUND'}",
            })

    return issues


def _check_yaml_scripts_regex() -> list[dict[str, Any]]:
    """Fallback YAML script checker using regex (no PyYAML)."""
    import re

    issues: list[dict[str, Any]] = []
    try:
        content = CONFIG_YAML_PATH.read_text(encoding="utf-8")
    except OSError:
        return issues

    # Match lines like: script: something.py
    pattern = re.compile(r"^\s*script:\s*(.+\.py)\s*$", re.MULTILINE)
    for match in pattern.finditer(content):
        script_name = match.group(1).strip().strip('"').strip("'")
        found = False
        for search_dir in [PROJECT_ROOT, PROJECT_ROOT / "pipeline", PROJECT_ROOT / "scripts"]:
            if (search_dir / script_name).exists():
                found = True
                break
            for _ in search_dir.glob(f"**/{script_name}"):
                found = True
                break
            if found:
                break

        issues.append({
            "check": f"script:{script_name}",
            "status": "OK" if found else "FAIL",
            "detail": f"{script_name} {'found' if found else 'NOT FOUND'}",
        })

    return issues


def check_ram_budgets() -> list[dict[str, Any]]:
    """Check RAM budgets in config don't exceed system RAM."""
    issues: list[dict[str, Any]] = []

    system_ram = _get_system_ram_mb()
    if system_ram is None:
        issues.append({
            "check": "system_ram",
            "status": "WARN",
            "detail": "Could not determine system RAM",
        })
        return issues

    issues.append({
        "check": "system_ram",
        "status": "OK",
        "detail": f"System RAM: {system_ram:,} MB",
    })

    try:
        from config import RAM_LIMITS, MAX_CONCURRENT_HEAVY
    except ImportError:
        issues.append({"check": "ram_import", "status": "FAIL", "detail": "Cannot import RAM_LIMITS"})
        return issues

    # Check individual limits
    for task_type, limit_mb in RAM_LIMITS.items():
        if limit_mb > system_ram:
            issues.append({
                "check": f"ram:{task_type}",
                "status": "FAIL",
                "detail": f"{task_type}={limit_mb} MB exceeds system RAM ({system_ram:,} MB)",
            })
        elif limit_mb > system_ram * 0.8:
            issues.append({
                "check": f"ram:{task_type}",
                "status": "WARN",
                "detail": f"{task_type}={limit_mb} MB is >80% of system RAM ({system_ram:,} MB)",
            })
        else:
            issues.append({
                "check": f"ram:{task_type}",
                "status": "OK",
                "detail": f"{task_type}={limit_mb} MB (system: {system_ram:,} MB)",
            })

    # Check worst-case concurrent usage
    sorted_limits = sorted(RAM_LIMITS.values(), reverse=True)
    worst_case = sum(sorted_limits[:MAX_CONCURRENT_HEAVY])
    if worst_case > system_ram:
        issues.append({
            "check": "ram_concurrent",
            "status": "FAIL",
            "detail": f"Worst-case concurrent: {worst_case:,} MB > system RAM ({system_ram:,} MB)",
        })
    else:
        issues.append({
            "check": "ram_concurrent",
            "status": "OK",
            "detail": f"Worst-case concurrent: {worst_case:,} MB (system: {system_ram:,} MB)",
        })

    return issues


def check_dag_cycles() -> list[dict[str, Any]]:
    """Check for circular dependencies in the pipeline DAG."""
    issues: list[dict[str, Any]] = []

    if not CONFIG_YAML_PATH.exists():
        issues.append({"check": "dag_cycles", "status": "SKIP", "detail": "No pipeline_config.yaml"})
        return issues

    import re

    content = CONFIG_YAML_PATH.read_text(encoding="utf-8")

    # Build adjacency list from depends_on declarations
    # Pattern: name: X ... depends_on: [A, B]
    graph: dict[str, list[str]] = defaultdict(list)
    all_nodes: set[str] = set()

    current_name = None
    for line in content.split("\n"):
        name_match = re.match(r"\s*-?\s*name:\s*(.+)", line)
        if name_match:
            current_name = name_match.group(1).strip().strip('"').strip("'")
            all_nodes.add(current_name)

        dep_match = re.match(r"\s*depends_on:\s*\[([^\]]*)\]", line)
        if dep_match and current_name:
            deps_str = dep_match.group(1).strip()
            if deps_str:
                deps = [d.strip().strip('"').strip("'") for d in deps_str.split(",")]
                graph[current_name] = [d for d in deps if d]
                for d in deps:
                    if d:
                        all_nodes.add(d)

    # Topological sort to detect cycles (Kahn's algorithm)
    in_degree: dict[str, int] = defaultdict(int)
    for node in all_nodes:
        if node not in in_degree:
            in_degree[node] = 0
    for node, deps in graph.items():
        for dep in deps:
            in_degree[node] += 1  # node depends on dep

    queue = deque([n for n in all_nodes if in_degree[n] == 0])
    visited = 0

    while queue:
        node = queue.popleft()
        visited += 1
        # Find all nodes that depend on this node
        for other, deps in graph.items():
            if node in deps:
                in_degree[other] -= 1
                if in_degree[other] == 0:
                    queue.append(other)

    if visited < len(all_nodes):
        cycle_nodes = [n for n in all_nodes if in_degree[n] > 0]
        issues.append({
            "check": "dag_cycles",
            "status": "FAIL",
            "detail": f"Circular dependency detected among: {', '.join(cycle_nodes)}",
        })
    else:
        issues.append({
            "check": "dag_cycles",
            "status": "OK",
            "detail": f"No cycles in DAG ({len(all_nodes)} nodes)",
        })

    return issues


def check_timeouts() -> list[dict[str, Any]]:
    """Check that all timeouts are reasonable (> 60s, < 24h)."""
    issues: list[dict[str, Any]] = []

    if not CONFIG_YAML_PATH.exists():
        issues.append({"check": "timeouts", "status": "SKIP", "detail": "No pipeline_config.yaml"})
        return issues

    import re

    content = CONFIG_YAML_PATH.read_text(encoding="utf-8")

    current_name = "defaults"
    for line in content.split("\n"):
        name_match = re.match(r"\s*-?\s*name:\s*(.+)", line)
        if name_match:
            current_name = name_match.group(1).strip().strip('"').strip("'")

        timeout_match = re.match(r"\s*timeout_seconds:\s*(\d+)", line)
        if timeout_match:
            timeout = int(timeout_match.group(1))
            if timeout < MIN_TIMEOUT_S:
                issues.append({
                    "check": f"timeout:{current_name}",
                    "status": "WARN",
                    "detail": f"{current_name}: {timeout}s < minimum {MIN_TIMEOUT_S}s",
                })
            elif timeout > MAX_TIMEOUT_S:
                issues.append({
                    "check": f"timeout:{current_name}",
                    "status": "WARN",
                    "detail": f"{current_name}: {timeout}s > maximum {MAX_TIMEOUT_S}s (24h)",
                })
            else:
                issues.append({
                    "check": f"timeout:{current_name}",
                    "status": "OK",
                    "detail": f"{current_name}: {timeout}s",
                })

    return issues


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def _generate_report(all_issues: dict[str, list[dict[str, Any]]], elapsed: float) -> None:
    """Write validation report as Markdown."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    total_ok = sum(1 for issues in all_issues.values() for i in issues if i["status"] == "OK")
    total_warn = sum(1 for issues in all_issues.values() for i in issues if i["status"] == "WARN")
    total_fail = sum(1 for issues in all_issues.values() for i in issues if i["status"] == "FAIL")
    total_skip = sum(1 for issues in all_issues.values() for i in issues if i["status"] == "SKIP")

    lines = [
        "# Pilier 22 — Meta-Configuration Validation",
        "",
        f"**Date**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Duree**: {elapsed:.1f}s",
        "",
        "## Sommaire",
        "",
        f"- OK: {total_ok}",
        f"- WARN: {total_warn}",
        f"- FAIL: {total_fail}",
        f"- SKIP: {total_skip}",
        "",
    ]

    for section, issues in all_issues.items():
        lines.append(f"## {section}")
        lines.append("")
        if not issues:
            lines.append("*Aucun probleme detecte.*")
        else:
            lines.append("| Statut | Check | Detail |")
            lines.append("|--------|-------|--------|")
            for issue in issues:
                status_icon = {"OK": "OK", "WARN": "WARN", "FAIL": "FAIL", "SKIP": "SKIP"}.get(
                    issue["status"], "?"
                )
                lines.append(f"| {status_icon} | {issue['check']} | {issue['detail']} |")
        lines.append("")

    lines.append("---")
    lines.append("*Genere par scripts/meta_config_validator.py (Pilier 22)*")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report written to %s", REPORT_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """Run all meta-configuration validation checks."""
    t0 = time.time()
    logger.info("=== Pilier 22 : Meta-Configuration Validation ===")

    all_issues: dict[str, list[dict[str, Any]]] = {}

    logger.info("Checking config.py paths...")
    all_issues["Config Paths"] = check_config_paths()

    logger.info("Checking pipeline_config.yaml scripts...")
    all_issues["YAML Scripts"] = check_yaml_scripts()

    logger.info("Checking RAM budgets...")
    all_issues["RAM Budgets"] = check_ram_budgets()

    logger.info("Checking DAG cycles...")
    all_issues["DAG Cycles"] = check_dag_cycles()

    logger.info("Checking timeouts...")
    all_issues["Timeouts"] = check_timeouts()

    elapsed = time.time() - t0
    _generate_report(all_issues, elapsed)

    # Summary
    total_fail = sum(1 for issues in all_issues.values() for i in issues if i["status"] == "FAIL")
    if total_fail > 0:
        logger.warning("Validation found %d FAIL(s)", total_fail)
    else:
        logger.info("All checks passed (%.1fs)", elapsed)

    return 1 if total_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
