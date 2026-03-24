#!/usr/bin/env python3
"""
scripts/traceability_builder.py — Pilier 21 : Tracabilite
==========================================================
Build a complete data lineage graph.

Maps:
  - Each output file to its input files
  - Each feature to its builder script
  - Each master file to its merge script

Output:
  - data_master/lineage_graph.json
    Format: {file: {inputs: [...], script: ..., last_run: ...}}

RAM budget: < 1 GB (scans files and builds a JSON graph in memory).

Usage:
    python scripts/traceability_builder.py
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATA_MASTER_DIR, FEATURES_DIR, OUTPUT_DIR
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"traceability_builder_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LINEAGE_OUTPUT = DATA_MASTER_DIR / "lineage_graph.json"

# Patterns for detecting file references in Python scripts
# Matches common patterns: Path("..."), open("..."), read_json("..."), etc.
FILE_REF_PATTERNS = [
    re.compile(r"""(?:open|read_json|read_csv|read_parquet|Path)\s*\(\s*["']([^"']+)["']"""),
    re.compile(r"""(?:OUTPUT_DIR|DATA_MASTER_DIR|FEATURES_DIR|LABELS_DIR)\s*/\s*["']([^"']+)["']"""),
    re.compile(r"""output_path\s*\(\s*\d+\s*,\s*["']([^"']+)["']\s*\)"""),
]

# Known mappings: merge scripts -> master files
KNOWN_MERGES: dict[str, list[str]] = {
    "mega_merge_partants_master.py": [
        "data_master/partants_master.jsonl",
        "data_master/partants_master_enrichi.jsonl",
    ],
    "merge_courses.py": [
        "data_master/courses_master.jsonl",
    ],
    "merge_equipements.py": [
        "data_master/equipements_master.json",
    ],
}

# Known feature builders -> feature files
KNOWN_FEATURES: dict[str, list[str]] = {
    "master_feature_builder.py": [
        "output/features/features_matrix.jsonl",
        "output/features/features_matrix.parquet",
    ],
}


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------
def _get_mtime_iso(filepath: Path) -> str | None:
    """Return file modification time as ISO string, or None if not found."""
    if not filepath.exists():
        return None
    mtime = os.path.getmtime(filepath)
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def _extract_file_references(script_path: Path) -> list[str]:
    """Extract file path references from a Python script (regex-based)."""
    refs: list[str] = []
    try:
        content = script_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return refs

    for pattern in FILE_REF_PATTERNS:
        matches = pattern.findall(content)
        refs.extend(matches)

    return refs


def _extract_config_imports(script_path: Path) -> list[str]:
    """Extract config.py imports to determine which data files a script uses."""
    imports: list[str] = []
    try:
        content = script_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return imports

    # Match: from config import (PARTANTS_MASTER, COURSES_MASTER, ...)
    import_pattern = re.compile(
        r"from\s+config\s+import\s+\(([^)]+)\)", re.DOTALL
    )
    for match in import_pattern.finditer(content):
        block = match.group(1)
        names = [n.strip().rstrip(",") for n in block.split("\n")]
        imports.extend(n for n in names if n and not n.startswith("#"))

    # Single-line imports
    single_pattern = re.compile(r"from\s+config\s+import\s+(.+)")
    for match in single_pattern.finditer(content):
        line = match.group(1)
        if "(" not in line:
            names = [n.strip() for n in line.split(",")]
            imports.extend(n for n in names if n)

    return imports


def _map_config_name_to_file(name: str) -> str | None:
    """Map a config.py constant name to a relative file path."""
    # Known mappings for master files
    mapping = {
        "PARTANTS_MASTER": "data_master/partants_master.jsonl",
        "PARTANTS_MASTER_ENRICHI": "data_master/partants_master_enrichi.jsonl",
        "COURSES_MASTER": "data_master/courses_master.jsonl",
        "EQUIPEMENTS_MASTER": "data_master/equipements_master.json",
        "COURSE_PROFILES": "data_master/course_profiles.jsonl",
        "FEATURES_MATRIX": "output/features/features_matrix.jsonl",
        "FEATURES_MATRIX_PARQUET": "output/features/features_matrix.parquet",
        "TRAINING_LABELS": "output/labels/training_labels.jsonl",
    }
    return mapping.get(name)


def _scan_scripts() -> dict[str, dict[str, Any]]:
    """Scan all Python scripts and build a lineage map."""
    lineage: dict[str, dict[str, Any]] = {}

    # Scan numbered scripts (XX_*.py) at project root
    root_scripts = sorted(PROJECT_ROOT.glob("[0-9]*.py"))

    # Scan scripts/ directory
    scripts_dir_files = sorted((PROJECT_ROOT / "scripts").glob("*.py"))

    # Scan pipeline/ directory
    pipeline_files = sorted((PROJECT_ROOT / "pipeline").glob("**/*.py"))

    all_scripts = root_scripts + scripts_dir_files + pipeline_files

    for script_path in all_scripts:
        if script_path.name == "__init__.py":
            continue

        rel_script = script_path.relative_to(PROJECT_ROOT).as_posix()
        file_refs = _extract_file_references(script_path)
        config_imports = _extract_config_imports(script_path)

        # Determine output files (from known mappings)
        outputs: list[str] = []
        basename = script_path.name

        if basename in KNOWN_MERGES:
            outputs.extend(KNOWN_MERGES[basename])
        if basename in KNOWN_FEATURES:
            outputs.extend(KNOWN_FEATURES[basename])

        # Determine input files (from config imports)
        inputs: list[str] = []
        for imp_name in config_imports:
            mapped = _map_config_name_to_file(imp_name)
            if mapped:
                inputs.append(mapped)

        # Add regex-detected file refs as inputs
        for ref in file_refs:
            if ref not in inputs and not ref.startswith("http"):
                inputs.append(ref)

        # Detect output directory from script number
        match = re.match(r"^(\d+)", basename)
        if match:
            num = int(match.group(1))
            out_dir = f"output/{num:02d}_"
            # Find matching output directory
            for d in OUTPUT_DIR.iterdir() if OUTPUT_DIR.exists() else []:
                if d.is_dir() and d.name.startswith(f"{num:02d}_"):
                    out_rel = d.relative_to(PROJECT_ROOT).as_posix()
                    if out_rel not in outputs:
                        outputs.append(out_rel)
                    break

        mtime = _get_mtime_iso(script_path)

        # Store lineage for each output
        for output_file in outputs:
            lineage[output_file] = {
                "inputs": sorted(set(inputs)),
                "script": rel_script,
                "last_run": mtime,
            }

        # Also store the script itself
        if rel_script not in lineage:
            lineage[rel_script] = {
                "inputs": sorted(set(inputs)),
                "script": rel_script,
                "last_run": mtime,
                "outputs": sorted(set(outputs)),
            }

    return lineage


def _scan_master_files(lineage: dict[str, dict[str, Any]]) -> None:
    """Add master files to lineage if not already mapped."""
    if not DATA_MASTER_DIR.exists():
        return

    for f in DATA_MASTER_DIR.iterdir():
        if f.name.startswith("__") or f.name.startswith("."):
            continue
        rel = f.relative_to(PROJECT_ROOT).as_posix()
        if rel not in lineage:
            lineage[rel] = {
                "inputs": [],
                "script": "unknown",
                "last_run": _get_mtime_iso(f),
            }


def _scan_feature_files(lineage: dict[str, dict[str, Any]]) -> None:
    """Add feature files to lineage."""
    if not FEATURES_DIR.exists():
        return

    for f in FEATURES_DIR.iterdir():
        if f.name.startswith("__") or f.name.startswith("."):
            continue
        rel = f.relative_to(PROJECT_ROOT).as_posix()
        if rel not in lineage:
            lineage[rel] = {
                "inputs": ["data_master/partants_master.jsonl"],
                "script": "master_feature_builder.py",
                "last_run": _get_mtime_iso(f),
            }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """Build the data lineage graph."""
    t0 = time.time()
    logger.info("=== Pilier 21 : Tracabilite — Lineage Graph ===")

    # Build lineage
    lineage = _scan_scripts()
    _scan_master_files(lineage)
    _scan_feature_files(lineage)

    # Sort by key for readability
    sorted_lineage = dict(sorted(lineage.items()))

    # Write output
    DATA_MASTER_DIR.mkdir(parents=True, exist_ok=True)
    with open(LINEAGE_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(sorted_lineage, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - t0
    logger.info(
        "Lineage graph built: %d entries in %.1fs -> %s",
        len(sorted_lineage),
        elapsed,
        LINEAGE_OUTPUT,
    )

    # Summary stats
    scripts_mapped = len({v["script"] for v in sorted_lineage.values() if v.get("script") != "unknown"})
    unknown_sources = sum(1 for v in sorted_lineage.values() if v.get("script") == "unknown")
    logger.info("Scripts mapped: %d, Unknown sources: %d", scripts_mapped, unknown_sources)

    return 0


if __name__ == "__main__":
    sys.exit(main())
