#!/usr/bin/env python3
"""
modularity_analyzer.py -- Pilier 12 : Modularite.

Analyse la modularite du codebase :
  1. Graphe d'imports entre modules du projet
  2. Comptage de fonctions par fichier, lignes par fonction
  3. Detection de modules fortement couples (>= 5 imports internes)
  4. Detection de code mort (fonctions jamais importees/appelees ailleurs)

Utilise uniquement ast (stdlib) pour le parsing, pas de dependance externe.
Reste sous 2 GB de RAM en traitant les fichiers un par un.

Genere un rapport dans quality/modularity_report.md.

Usage :
    python scripts/modularity_analyzer.py
"""

from __future__ import annotations

import ast
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import QUALITY_DIR  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Directories to skip when scanning
SKIP_DIRS = {
    "__pycache__",
    ".git",
    ".claude",
    "node_modules",
    ".venv",
    "venv",
    "env",
    ".mypy_cache",
    ".pytest_cache",
    ".tox",
}

# Threshold for "tightly coupled" modules
TIGHT_COUPLING_THRESHOLD = 5

# Max function line count considered "too long"
LONG_FUNCTION_LINES = 100


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def find_python_files(root: Path) -> list[Path]:
    """Find all .py files under root, skipping excluded directories."""
    py_files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Prune skipped directories
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if fname.endswith(".py"):
                py_files.append(Path(dirpath) / fname)
    return sorted(py_files)


def relative_module_path(filepath: Path, root: Path) -> str:
    """Convert a file path to a dotted module name relative to root."""
    try:
        rel = filepath.relative_to(root)
    except ValueError:
        return filepath.stem
    parts = list(rel.with_suffix("").parts)
    return ".".join(parts)


# ---------------------------------------------------------------------------
# AST analysis
# ---------------------------------------------------------------------------

class FileAnalysis:
    """Analysis results for a single Python file."""

    __slots__ = (
        "path",
        "module_name",
        "functions",
        "classes",
        "imports_from_project",
        "defined_names",
        "line_count",
        "parse_error",
    )

    def __init__(self, path: Path, module_name: str) -> None:
        self.path = path
        self.module_name = module_name
        self.functions: list[dict] = []  # [{name, start, end, lines}]
        self.classes: list[str] = []
        self.imports_from_project: set[str] = set()  # module names imported
        self.defined_names: set[str] = set()  # top-level function/class names
        self.line_count: int = 0
        self.parse_error: str | None = None


def _is_project_import(module_name: str | None, project_modules: set[str]) -> bool:
    """Check if an import refers to a project module."""
    if module_name is None:
        return False
    # Direct match
    if module_name in project_modules:
        return True
    # Parent package match (e.g., "config" matches "config.py")
    parts = module_name.split(".")
    for i in range(len(parts)):
        prefix = ".".join(parts[: i + 1])
        if prefix in project_modules:
            return True
    return False


def analyze_file(
    filepath: Path,
    root: Path,
    project_module_names: set[str],
) -> FileAnalysis:
    """Parse a single Python file and extract analysis data."""
    module_name = relative_module_path(filepath, root)
    analysis = FileAnalysis(filepath, module_name)

    try:
        source = filepath.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        analysis.parse_error = f"read error: {exc}"
        return analysis

    analysis.line_count = source.count("\n") + (1 if source and not source.endswith("\n") else 0)

    try:
        tree = ast.parse(source, filename=str(filepath))
    except SyntaxError as exc:
        analysis.parse_error = f"syntax error line {exc.lineno}: {exc.msg}"
        return analysis

    for node in ast.walk(tree):
        # Top-level function definitions
        if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
            start = node.lineno
            end = node.end_lineno or start
            func_info = {
                "name": node.name,
                "start": start,
                "end": end,
                "lines": end - start + 1,
            }
            analysis.functions.append(func_info)
            # Only track top-level definitions for dead code detection
            if hasattr(node, "col_offset") and node.col_offset == 0:
                analysis.defined_names.add(node.name)

        elif isinstance(node, ast.ClassDef):
            analysis.classes.append(node.name)
            if hasattr(node, "col_offset") and node.col_offset == 0:
                analysis.defined_names.add(node.name)

        # Imports
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _is_project_import(alias.name, project_module_names):
                    analysis.imports_from_project.add(alias.name.split(".")[0])

        elif isinstance(node, ast.ImportFrom):
            if node.module and _is_project_import(
                node.module, project_module_names
            ):
                analysis.imports_from_project.add(node.module.split(".")[0])

    return analysis


# ---------------------------------------------------------------------------
# Cross-file analysis
# ---------------------------------------------------------------------------

def find_dead_code(analyses: list[FileAnalysis]) -> list[tuple[str, str]]:
    """Find functions/classes defined at top-level but never referenced elsewhere.

    Returns list of (module_name, function_name).
    """
    # Collect all names referenced in imports or source of OTHER files
    externally_referenced: set[str] = set()

    # Collect all "from X import Y" style references
    for analysis in analyses:
        try:
            source = analysis.path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        tree = None
        try:
            tree = ast.parse(source, filename=str(analysis.path))
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.names:
                for alias in node.names:
                    externally_referenced.add(alias.name)
            # Also check for function calls using attribute access
            elif isinstance(node, ast.Attribute):
                externally_referenced.add(node.attr)

    # Now find defined names that are never referenced externally
    dead: list[tuple[str, str]] = []
    for analysis in analyses:
        for name in analysis.defined_names:
            # Skip dunder methods and common entry points
            if name.startswith("_"):
                continue
            if name in ("main", "setup", "run", "cli"):
                continue
            if name not in externally_referenced:
                dead.append((analysis.module_name, name))

    return sorted(dead)


def find_tightly_coupled(analyses: list[FileAnalysis]) -> list[tuple[str, int, list[str]]]:
    """Find modules that import from >= TIGHT_COUPLING_THRESHOLD project files.

    Returns list of (module_name, import_count, imported_modules).
    """
    coupled: list[tuple[str, int, list[str]]] = []
    for analysis in analyses:
        count = len(analysis.imports_from_project)
        if count >= TIGHT_COUPLING_THRESHOLD:
            coupled.append(
                (analysis.module_name, count, sorted(analysis.imports_from_project))
            )
    return sorted(coupled, key=lambda x: -x[1])


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def generate_report(
    analyses: list[FileAnalysis],
    tightly_coupled: list[tuple[str, int, list[str]]],
    dead_code: list[tuple[str, str]],
) -> str:
    """Generate the modularity markdown report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    total_files = len(analyses)
    total_functions = sum(len(a.functions) for a in analyses)
    total_lines = sum(a.line_count for a in analyses)
    parse_errors = [a for a in analyses if a.parse_error]

    lines: list[str] = [
        "# Modularity Analysis Report (Pilier 12)",
        "",
        f"Generated: {now}",
        "",
        "## Overview",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Python files | {total_files} |",
        f"| Total lines of code | {total_lines:,} |",
        f"| Total functions/methods | {total_functions} |",
        f"| Files with parse errors | {len(parse_errors)} |",
        f"| Tightly coupled modules (>= {TIGHT_COUPLING_THRESHOLD} internal imports) | {len(tightly_coupled)} |",
        f"| Potentially dead functions/classes | {len(dead_code)} |",
        "",
    ]

    # --- Functions per file (top 20 by count) ---
    lines.append("## Functions per File (top 20)")
    lines.append("")
    lines.append("| File | Functions | Lines |")
    lines.append("|------|-----------|-------|")

    by_func_count = sorted(analyses, key=lambda a: -len(a.functions))
    for a in by_func_count[:20]:
        lines.append(f"| {a.module_name} | {len(a.functions)} | {a.line_count} |")
    lines.append("")

    # --- Long functions (> LONG_FUNCTION_LINES lines) ---
    long_funcs: list[tuple[str, str, int]] = []
    for a in analyses:
        for f in a.functions:
            if f["lines"] > LONG_FUNCTION_LINES:
                long_funcs.append((a.module_name, f["name"], f["lines"]))
    long_funcs.sort(key=lambda x: -x[2])

    lines.append(f"## Long Functions (> {LONG_FUNCTION_LINES} lines)")
    lines.append("")
    if long_funcs:
        lines.append("| Module | Function | Lines |")
        lines.append("|--------|----------|-------|")
        for mod, fname, flines in long_funcs[:30]:
            lines.append(f"| {mod} | {fname} | {flines} |")
    else:
        lines.append("None found.")
    lines.append("")

    # --- Import graph summary ---
    lines.append("## Import Graph (internal imports)")
    lines.append("")
    lines.append("| Module | Internal Imports |")
    lines.append("|--------|-----------------|")

    by_imports = sorted(
        [a for a in analyses if a.imports_from_project],
        key=lambda a: -len(a.imports_from_project),
    )
    for a in by_imports[:30]:
        imports_str = ", ".join(sorted(a.imports_from_project))
        lines.append(f"| {a.module_name} | {imports_str} |")
    lines.append("")

    # --- Tightly coupled ---
    lines.append(f"## Tightly Coupled Modules (>= {TIGHT_COUPLING_THRESHOLD} internal imports)")
    lines.append("")
    if tightly_coupled:
        lines.append("| Module | Import Count | Imports From |")
        lines.append("|--------|-------------|--------------|")
        for mod, count, imports in tightly_coupled:
            lines.append(f"| {mod} | {count} | {', '.join(imports)} |")
    else:
        lines.append("None found. Good modularity!")
    lines.append("")

    # --- Dead code ---
    lines.append("## Potentially Dead Code")
    lines.append("")
    lines.append(
        "Functions/classes defined at top-level but never imported or referenced "
        "in other project files. Review before removing (may be used via CLI or "
        "dynamic imports)."
    )
    lines.append("")
    if dead_code:
        lines.append("| Module | Name |")
        lines.append("|--------|------|")
        for mod, name in dead_code[:50]:
            lines.append(f"| {mod} | {name} |")
        if len(dead_code) > 50:
            lines.append(f"| ... | ({len(dead_code) - 50} more) |")
    else:
        lines.append("None detected.")
    lines.append("")

    # --- Parse errors ---
    if parse_errors:
        lines.append("## Parse Errors")
        lines.append("")
        for a in parse_errors:
            lines.append(f"- **{a.module_name}**: {a.parse_error}")
        lines.append("")

    lines.append("---")
    lines.append("*Report generated by modularity_analyzer.py (Pilier 12)*")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """Analyze codebase modularity and generate report."""
    print("=== Pilier 12 : Modularity Analyzer ===\n")

    # Discover Python files
    print("Scanning for Python files...")
    py_files = find_python_files(PROJECT_ROOT)
    print(f"  Found {len(py_files)} Python files")

    # Build set of known project module names
    project_modules: set[str] = set()
    for pf in py_files:
        mod = relative_module_path(pf, PROJECT_ROOT)
        project_modules.add(mod)
        # Also add the first component (package/file name)
        project_modules.add(mod.split(".")[0])

    # Analyze each file
    print("Analyzing files...")
    analyses: list[FileAnalysis] = []
    for i, pf in enumerate(py_files):
        if (i + 1) % 50 == 0:
            print(f"  ... {i + 1}/{len(py_files)}")
        analysis = analyze_file(pf, PROJECT_ROOT, project_modules)
        analyses.append(analysis)

    # Cross-file analysis
    print("Detecting tightly coupled modules...")
    tightly_coupled = find_tightly_coupled(analyses)

    print("Detecting potentially dead code...")
    dead_code = find_dead_code(analyses)

    # Summary
    total_functions = sum(len(a.functions) for a in analyses)
    print(f"\n  Total functions: {total_functions}")
    print(f"  Tightly coupled modules: {len(tightly_coupled)}")
    print(f"  Potentially dead code items: {len(dead_code)}")

    # Write report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report_path = QUALITY_DIR / "modularity_report.md"
    report_content = generate_report(analyses, tightly_coupled, dead_code)
    report_path.write_text(report_content, encoding="utf-8")
    print(f"\nReport written to {report_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
