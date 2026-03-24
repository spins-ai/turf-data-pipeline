#!/usr/bin/env python3
"""
orchestration_monitor.py — Pilier 8 : Orchestration

Monitor pipeline execution:
  - Check which scripts ran recently (from logs/)
  - Check DAG consistency: verify all dependencies in pipeline_config.yaml
    are satisfiable (no missing scripts, no circular deps)
  - Report broken dependencies or missing inputs

Outputs quality/orchestration_report.md

Usage:
    python scripts/orchestration_monitor.py
"""

from __future__ import annotations

import re
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import CONFIG_DIR, DATA_MASTER_DIR, LOGS_DIR, QUALITY_DIR

OUTPUT_REPORT = QUALITY_DIR / "orchestration_report.md"
PIPELINE_CONFIG = CONFIG_DIR / "pipeline_config.yaml"


# ---------------------------------------------------------------------------
# Minimal YAML parser (avoid external dependency)
# ---------------------------------------------------------------------------

def _load_yaml_minimal(path: Path) -> dict:
    """
    Load pipeline_config.yaml using a minimal approach.
    Try PyYAML first, fall back to a regex-based parser for the DAG.
    """
    try:
        import yaml  # type: ignore
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except ImportError:
        pass

    # Fallback: regex-based extraction of script names and depends_on
    return _parse_pipeline_config_fallback(path)


def _parse_pipeline_config_fallback(path: Path) -> dict:
    """
    Extract phase/script/depends_on info from pipeline_config.yaml without PyYAML.
    Returns a structure similar enough for our DAG checks.
    """
    text = path.read_text(encoding="utf-8")
    phases: list[dict] = []
    current_phase: dict | None = None
    current_script: dict | None = None

    for line in text.splitlines():
        stripped = line.strip()

        # Phase start
        m = re.match(r"-\s*phase:\s*(\d+)", stripped)
        if m:
            if current_phase is not None:
                if current_script is not None:
                    current_phase.setdefault("scripts", []).append(current_script)
                    current_script = None
                phases.append(current_phase)
            current_phase = {"phase": int(m.group(1)), "scripts": []}
            continue

        if current_phase is None:
            continue

        # Script name
        m = re.match(r"-?\s*name:\s*[\"']?(\S+?)[\"']?\s*$", stripped)
        if m and "phase" not in stripped:
            if current_script is not None:
                current_phase.setdefault("scripts", []).append(current_script)
            current_script = {"name": m.group(1)}
            continue

        # Script file
        m = re.match(r"script:\s*[\"']?(.+?)[\"']?\s*$", stripped)
        if m and current_script is not None:
            current_script["script"] = m.group(1)
            continue

        # depends_on (inline list)
        m = re.match(r"depends_on:\s*\[([^\]]*)\]", stripped)
        if m and current_script is not None:
            deps_str = m.group(1).strip()
            if deps_str:
                deps = [d.strip().strip("\"'") for d in deps_str.split(",")]
            else:
                deps = []
            current_script["depends_on"] = deps
            continue

        # depends_on (multi-line list item)
        m = re.match(r"-\s+(\w[\w_]*)\s*$", stripped)
        if m and current_script is not None and "depends_on" in current_script:
            current_script["depends_on"].append(m.group(1))
            continue

        # Start of multi-line depends_on
        if stripped == "depends_on:" and current_script is not None:
            current_script["depends_on"] = []
            continue

    # Flush last
    if current_script is not None and current_phase is not None:
        current_phase.setdefault("scripts", []).append(current_script)
    if current_phase is not None:
        phases.append(current_phase)

    return {"phases": phases}


# ---------------------------------------------------------------------------
# DAG validation
# ---------------------------------------------------------------------------

def extract_dag(config: dict) -> tuple[dict[str, list[str]], dict[str, str]]:
    """
    Extract the dependency graph and script-file mapping from config.
    Returns (graph: name -> [deps], scripts: name -> script_file).
    """
    graph: dict[str, list[str]] = {}
    scripts: dict[str, str] = {}
    phases = config.get("phases", [])

    for phase in phases:
        for s in phase.get("scripts", []):
            name = s.get("name", "")
            if not name:
                continue
            deps = s.get("depends_on", [])
            graph[name] = deps if deps else []
            scripts[name] = s.get("script", "")

    return graph, scripts


def check_missing_deps(graph: dict[str, list[str]]) -> list[str]:
    """Find dependencies that reference scripts not defined in the DAG."""
    all_names = set(graph.keys())
    issues = []
    for name, deps in graph.items():
        for dep in deps:
            if dep not in all_names:
                issues.append(f"`{name}` depends on `{dep}` which is not defined in the DAG")
    return issues


def check_circular_deps(graph: dict[str, list[str]]) -> list[str]:
    """Detect cycles using Kahn's algorithm (topological sort)."""
    in_degree: dict[str, int] = defaultdict(int)
    for name in graph:
        in_degree.setdefault(name, 0)
        for dep in graph[name]:
            if dep in graph:
                in_degree[name] += 1  # wrong direction; fix below

    # Rebuild in_degree properly: in_degree[X] = number of deps X has
    in_degree = {name: 0 for name in graph}
    adj: dict[str, list[str]] = defaultdict(list)  # dep -> [dependents]
    for name, deps in graph.items():
        for dep in deps:
            if dep in graph:
                adj[dep].append(name)
                in_degree[name] += 1

    queue = deque([n for n, d in in_degree.items() if d == 0])
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for dependent in adj[node]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if visited < len(graph):
        # Find nodes in cycle
        in_cycle = [n for n, d in in_degree.items() if d > 0]
        return [f"Circular dependency detected involving: {', '.join(sorted(in_cycle))}"]
    return []


def check_script_files_exist(
    scripts: dict[str, str],
    project_root: Path,
) -> list[str]:
    """Check if the script files referenced in the config exist on disk."""
    issues = []
    for name, script_path in scripts.items():
        if not script_path:
            issues.append(f"`{name}`: no script file specified")
            continue
        # Scripts can be in project root or pipeline/ or feature_builders/
        candidates = [
            project_root / script_path,
            project_root / "pipeline" / script_path,
        ]
        found = any(c.exists() for c in candidates)
        if not found:
            issues.append(f"`{name}`: script `{script_path}` not found on disk")
    return issues


# ---------------------------------------------------------------------------
# Log analysis
# ---------------------------------------------------------------------------

def analyze_logs(logs_dir: Path) -> list[dict]:
    """Analyze log files for recent execution info."""
    if not logs_dir.exists():
        return []

    log_info = []
    for f in sorted(logs_dir.iterdir()):
        if not f.is_file():
            continue
        stat = f.stat()
        size = stat.st_size
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)

        # Peek at last few lines for status info
        last_lines = ""
        has_error = False
        try:
            with open(f, "r", encoding="utf-8", errors="replace") as fh:
                # Read last 2KB
                fh.seek(max(0, size - 2048))
                tail = fh.read()
                last_lines = tail[-500:]
                has_error = any(
                    kw in tail.lower()
                    for kw in ["error", "traceback", "exception", "failed"]
                )
        except Exception:
            pass

        log_info.append({
            "name": f.name,
            "size_kb": round(size / 1024, 1),
            "last_modified": mtime.strftime("%Y-%m-%d %H:%M"),
            "has_error": has_error,
        })

    return log_info


# ---------------------------------------------------------------------------
# Master file health check
# ---------------------------------------------------------------------------

def check_master_files(data_master_dir: Path) -> list[dict]:
    """Check health of master files."""
    if not data_master_dir.exists():
        return []

    files_info = []
    for f in sorted(data_master_dir.iterdir()):
        if not f.is_file():
            continue
        stat = f.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        files_info.append({
            "name": f.name,
            "size_mb": round(stat.st_size / 1_048_576, 1),
            "last_modified": mtime.strftime("%Y-%m-%d %H:%M"),
            "is_empty": stat.st_size == 0,
        })
    return files_info


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def write_report(
    dag_issues: list[str],
    circular_issues: list[str],
    file_issues: list[str],
    graph: dict[str, list[str]],
    scripts: dict[str, str],
    log_info: list[dict],
    master_info: list[dict],
    elapsed: float,
) -> None:
    """Write orchestration report."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    all_issues = dag_issues + circular_issues + file_issues
    status = "PASS" if not all_issues else "FAIL"

    lines: list[str] = []
    lines.append("# Orchestration Report (Pilier 8 — Orchestration)")
    lines.append("")
    lines.append(f"Generated: {now}")
    lines.append(f"Elapsed: {elapsed:.1f}s")
    lines.append("")

    # --- Summary ---
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Status | **{status}** |")
    lines.append(f"| DAG nodes (scripts) | {len(graph)} |")
    lines.append(f"| DAG edges (dependencies) | {sum(len(d) for d in graph.values())} |")
    lines.append(f"| Missing dependency issues | {len(dag_issues)} |")
    lines.append(f"| Circular dependency issues | {len(circular_issues)} |")
    lines.append(f"| Missing script file issues | {len(file_issues)} |")
    lines.append(f"| Log files analyzed | {len(log_info)} |")
    lines.append(f"| Master files checked | {len(master_info)} |")
    lines.append("")

    # --- Issues ---
    if all_issues:
        lines.append("## Issues Found")
        lines.append("")
        for issue in all_issues:
            lines.append(f"- {issue}")
        lines.append("")
    else:
        lines.append("## No Issues Found")
        lines.append("")
        lines.append("All DAG dependencies are satisfiable, no circular deps, "
                      "no missing script files.")
        lines.append("")

    # --- DAG overview ---
    lines.append("## DAG Overview")
    lines.append("")
    lines.append("| Script | Depends On | Script File |")
    lines.append("|--------|-----------|-------------|")
    for name in sorted(graph.keys()):
        deps = ", ".join(graph[name]) if graph[name] else "(none)"
        sf = scripts.get(name, "?")
        lines.append(f"| `{name}` | {deps} | `{sf}` |")
    lines.append("")

    # --- Execution order (topological sort) ---
    lines.append("## Topological Execution Order")
    lines.append("")
    if not circular_issues:
        order = _topological_sort(graph)
        for i, name in enumerate(order, 1):
            lines.append(f"{i}. `{name}`")
    else:
        lines.append("Cannot compute execution order due to circular dependencies.")
    lines.append("")

    # --- Recent logs ---
    lines.append("## Recent Log Files")
    lines.append("")
    if log_info:
        lines.append("| Log File | Size (KB) | Last Modified | Errors? |")
        lines.append("|----------|-----------|--------------|---------|")
        for info in sorted(log_info, key=lambda x: x["last_modified"], reverse=True):
            err = "YES" if info["has_error"] else "no"
            lines.append(
                f"| `{info['name']}` | {info['size_kb']} "
                f"| {info['last_modified']} | {err} |"
            )
    else:
        lines.append("No log files found.")
    lines.append("")

    # Logs with errors
    error_logs = [l for l in log_info if l["has_error"]]
    if error_logs:
        lines.append("### Logs with Errors")
        lines.append("")
        for info in error_logs:
            lines.append(f"- `{info['name']}` (last modified: {info['last_modified']})")
        lines.append("")

    # --- Master file health ---
    lines.append("## Master File Health")
    lines.append("")
    if master_info:
        lines.append("| File | Size (MB) | Last Modified | Status |")
        lines.append("|------|-----------|--------------|--------|")
        for info in master_info:
            status_str = "EMPTY" if info["is_empty"] else "OK"
            lines.append(
                f"| `{info['name']}` | {info['size_mb']} "
                f"| {info['last_modified']} | {status_str} |"
            )
        empty_count = sum(1 for i in master_info if i["is_empty"])
        if empty_count:
            lines.append("")
            lines.append(f"**Warning**: {empty_count} empty master file(s) detected.")
    else:
        lines.append("No master files found.")
    lines.append("")

    OUTPUT_REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written to {OUTPUT_REPORT}")


def _topological_sort(graph: dict[str, list[str]]) -> list[str]:
    """Kahn's algorithm for topological sort."""
    in_degree: dict[str, int] = {n: 0 for n in graph}
    adj: dict[str, list[str]] = defaultdict(list)
    for name, deps in graph.items():
        for dep in deps:
            if dep in graph:
                adj[dep].append(name)
                in_degree[name] += 1

    queue = deque(sorted(n for n, d in in_degree.items() if d == 0))
    result: list[str] = []
    while queue:
        node = queue.popleft()
        result.append(node)
        for dependent in sorted(adj[node]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    return result


def main() -> None:
    print("=" * 60)
    print("Pilier 8 — Orchestration Monitor")
    print("=" * 60)
    t0 = time.time()

    # --- Load pipeline config ---
    print(f"\n[1/4] Loading pipeline config from {PIPELINE_CONFIG} ...")
    if not PIPELINE_CONFIG.exists():
        print(f"WARNING: {PIPELINE_CONFIG} not found — skipping DAG checks.")
        config: dict = {"phases": []}
    else:
        config = _load_yaml_minimal(PIPELINE_CONFIG)

    graph, scripts = extract_dag(config)
    print(f"  Found {len(graph)} scripts in DAG.")

    # --- DAG validation ---
    print("\n[2/4] Validating DAG ...")
    dag_issues = check_missing_deps(graph)
    circular_issues = check_circular_deps(graph)
    file_issues = check_script_files_exist(scripts, PROJECT_ROOT)
    total_issues = len(dag_issues) + len(circular_issues) + len(file_issues)
    print(f"  {total_issues} issue(s) found.")

    # --- Log analysis ---
    print(f"\n[3/4] Analyzing logs in {LOGS_DIR} ...")
    log_info = analyze_logs(LOGS_DIR)
    print(f"  {len(log_info)} log files analyzed.")

    # --- Master file check ---
    print(f"\n[4/4] Checking master files in {DATA_MASTER_DIR} ...")
    master_info = check_master_files(DATA_MASTER_DIR)
    print(f"  {len(master_info)} master files checked.")

    elapsed = time.time() - t0
    write_report(
        dag_issues, circular_issues, file_issues,
        graph, scripts, log_info, master_info, elapsed,
    )
    print(f"\nDone in {elapsed:.1f}s.")


if __name__ == "__main__":
    main()
