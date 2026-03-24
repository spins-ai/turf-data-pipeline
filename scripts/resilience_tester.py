#!/usr/bin/env python3
"""
scripts/resilience_tester.py — Pilier 17 : Resilience Algorithmique
=====================================================================
Test that feature builders handle edge cases gracefully:

  - Missing fields
  - Division by zero in rate calculations
  - Empty course groups (zero partants)
  - Single-horse races
  - Zero cote_finale

Strategy:
  1. Load a sample of 100 records from partants_master.jsonl
  2. For each feature builder that accepts a list of records:
     a. Run the builder on clean sample (baseline)
     b. For each field in the sample, deliberately corrupt it:
        - Set to None
        - Set to 0 (for numerics)
        - Set to empty string (for strings)
        - Remove the field entirely
     c. Verify no crash (exception) for each corruption
  3. Also test special edge cases:
     - Empty list of records
     - Single-record list
     - All records with zero cote_finale
     - All records with same course_uid (single race)

Reports pass/fail per builder per corruption type.

RAM budget: < 2 GB.

Usage:
    python scripts/resilience_tester.py
"""

from __future__ import annotations

import copy
import importlib
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    PARTANTS_MASTER,
    QUALITY_DIR,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_PATH = QUALITY_DIR / "resilience_test_report.md"
FEATURE_BUILDERS_DIR = PROJECT_ROOT / "feature_builders"
_TODAY = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
SAMPLE_SIZE = 100

# Fields to corrupt (common fields across partant records)
FIELDS_TO_CORRUPT = [
    "partant_uid",
    "course_uid",
    "date_reunion_iso",
    "nom_cheval",
    "discipline",
    "hippodrome_normalise",
    "cote_finale",
    "position_arrivee",
    "is_gagnant",
    "is_place",
    "nombre_partants",
    "num_pmu",
    "poids_porte_kg",
    "gains_carriere_euros",
    "nb_courses_carriere",
    "nb_victoires_carriere",
    "nb_places_carriere",
    "distance",
    "jockey_driver",
    "entraineur",
    "musique",
    "reduction_km_ms",
    "temps_ms",
]

# Corruption strategies
CORRUPTION_TYPES = [
    ("null", lambda _v: None),
    ("zero", lambda _v: 0),
    ("empty_str", lambda _v: ""),
    ("missing", "REMOVE"),  # sentinel: remove the field entirely
]


# ---------------------------------------------------------------------------
# Sample loading
# ---------------------------------------------------------------------------
def load_sample(path: Path, n: int = SAMPLE_SIZE) -> list[dict]:
    """Load up to n records from a JSONL file."""
    records: list[dict] = []
    if not path.exists():
        return records
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except (json.JSONDecodeError, ValueError):
                continue
            if len(records) >= n:
                break
    return records


def _make_synthetic_records(n: int = SAMPLE_SIZE) -> list[dict]:
    """Generate synthetic records when no real data is available."""
    records = []
    for i in range(n):
        records.append({
            "partant_uid": f"2024-01-{15 + i % 28:02d}_VIN_C{1 + i % 8}_P{i:04d}",
            "course_uid": f"2024-01-{15 + i % 28:02d}_VIN_C{1 + i % 8}",
            "date_reunion_iso": f"2024-01-{15 + i % 28:02d}",
            "nom_cheval": f"Cheval_{i % 50}",
            "discipline": ["plat", "trot attele", "trot monte", "obstacle"][i % 4],
            "hippodrome_normalise": ["VINCENNES", "LONGCHAMP", "AUTEUIL", "DEAUVILLE"][i % 4],
            "cote_finale": 2.0 + (i % 30) * 0.5,
            "position_arrivee": (i % 16) + 1,
            "is_gagnant": i % 16 == 0,
            "is_place": i % 16 < 3,
            "nombre_partants": 16,
            "num_pmu": (i % 16) + 1,
            "poids_porte_kg": 55.0 + (i % 10),
            "gains_carriere_euros": 30000 + i * 500,
            "nb_courses_carriere": 10 + i,
            "nb_victoires_carriere": i % 5,
            "nb_places_carriere": i % 8,
            "distance": [1600, 2000, 2400, 2850][i % 4],
            "jockey_driver": f"Jockey_{i % 20}",
            "entraineur": f"Entraineur_{i % 15}",
            "musique": "1p2p3p0p" if i % 3 == 0 else "5s4s3s",
            "reduction_km_ms": 68000 + i * 100,
            "temps_ms": 120000 + i * 500,
        })
    return records


# ---------------------------------------------------------------------------
# Builder discovery
# ---------------------------------------------------------------------------
def discover_list_builders() -> list[tuple[str, str, Callable]]:
    """Find feature builders that accept a list of dicts.

    Returns list of (module_name, function_name, callable).
    Only includes builders whose main function takes (partants: list[dict])
    as the primary argument (no file paths required).
    """
    builders: list[tuple[str, str, Callable]] = []

    if not FEATURE_BUILDERS_DIR.is_dir():
        return builders

    for p in sorted(FEATURE_BUILDERS_DIR.glob("*.py")):
        name = p.stem
        if name.startswith("_") or name == "master_feature_builder":
            continue

        try:
            mod = importlib.import_module(f"feature_builders.{name}")
        except Exception:
            continue

        # Look for build_* functions that accept a list
        for attr_name in dir(mod):
            if not attr_name.startswith("build_"):
                continue
            func = getattr(mod, attr_name, None)
            if not callable(func):
                continue

            # Check signature: we want functions that take partants as first arg
            import inspect
            try:
                sig = inspect.signature(func)
                params = list(sig.parameters.values())
                if not params:
                    continue
                first_param = params[0]
                # Skip builders that require Path as first arg
                ann = first_param.annotation
                if ann is not inspect.Parameter.empty:
                    ann_str = str(ann)
                    if "Path" in ann_str:
                        continue
                # Skip if too many required params (e.g., need external data)
                required = [
                    p for p in params
                    if p.default is inspect.Parameter.empty
                    and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
                ]
                if len(required) > 2:
                    continue
                builders.append((name, attr_name, func))
            except (ValueError, TypeError):
                continue

    return builders


# ---------------------------------------------------------------------------
# Test runners
# ---------------------------------------------------------------------------
class TestResult:
    """Track results for a single builder."""

    def __init__(self, builder_name: str) -> None:
        self.builder_name = builder_name
        self.baseline_ok: bool = False
        self.baseline_error: str = ""
        self.corruption_results: list[dict] = []
        self.edge_case_results: list[dict] = []

    @property
    def total_tests(self) -> int:
        return 1 + len(self.corruption_results) + len(self.edge_case_results)

    @property
    def total_passed(self) -> int:
        count = 1 if self.baseline_ok else 0
        count += sum(1 for r in self.corruption_results if r["passed"])
        count += sum(1 for r in self.edge_case_results if r["passed"])
        return count

    @property
    def all_passed(self) -> bool:
        return self.total_passed == self.total_tests


def _run_builder_safe(func: Callable, records: list[dict]) -> tuple[bool, str]:
    """Run a builder function and catch any exception.

    Returns (success: bool, error_message: str).
    """
    try:
        result = func(records)
        # Basic validation: should return a list
        if not isinstance(result, list):
            return False, f"Returned {type(result).__name__} instead of list"
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def test_builder(
    builder_name: str,
    func_name: str,
    func: Callable,
    sample: list[dict],
) -> TestResult:
    """Run all resilience tests on a single builder."""
    result = TestResult(builder_name)

    # Baseline test with clean data
    ok, err = _run_builder_safe(func, copy.deepcopy(sample))
    result.baseline_ok = ok
    result.baseline_error = err

    if not ok:
        # If baseline fails, skip corruption tests
        return result

    # Corruption tests: for each field, corrupt all records
    for field in FIELDS_TO_CORRUPT:
        for corr_name, corr_func in CORRUPTION_TYPES:
            corrupted = copy.deepcopy(sample)
            for rec in corrupted:
                if field not in rec and corr_func != "REMOVE":
                    continue  # field not present, skip
                if corr_func == "REMOVE":
                    rec.pop(field, None)
                else:
                    rec[field] = corr_func(rec.get(field))

            ok, err = _run_builder_safe(func, corrupted)
            result.corruption_results.append({
                "field": field,
                "corruption": corr_name,
                "passed": ok,
                "error": err,
            })

    # Edge case tests
    edge_cases = [
        ("empty_list", []),
        ("single_record", copy.deepcopy(sample[:1])),
        ("all_zero_cote", _make_zero_cote(sample)),
        ("single_course", _make_single_course(sample)),
        ("single_horse_race", _make_single_horse_race(sample)),
    ]

    for case_name, case_data in edge_cases:
        ok, err = _run_builder_safe(func, case_data)
        result.edge_case_results.append({
            "case": case_name,
            "passed": ok,
            "error": err,
        })

    return result


def _make_zero_cote(sample: list[dict]) -> list[dict]:
    """Create a copy where all cote_finale = 0."""
    records = copy.deepcopy(sample)
    for rec in records:
        rec["cote_finale"] = 0
    return records


def _make_single_course(sample: list[dict]) -> list[dict]:
    """Create a copy where all records share one course_uid."""
    records = copy.deepcopy(sample)
    for rec in records:
        rec["course_uid"] = "2024-01-15_VIN_C1"
        rec["date_reunion_iso"] = "2024-01-15"
    return records


def _make_single_horse_race(sample: list[dict]) -> list[dict]:
    """Create a list with just 1 horse in 1 course."""
    rec = copy.deepcopy(sample[0]) if sample else _make_synthetic_records(1)[0]
    rec["nombre_partants"] = 1
    rec["num_pmu"] = 1
    rec["position_arrivee"] = 1
    rec["is_gagnant"] = True
    return [rec]


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def generate_report(results: list[TestResult], elapsed: float) -> str:
    """Generate the Markdown resilience report."""
    lines: list[str] = []
    lines.append("# Resilience Test Report (Pilier 17)")
    lines.append(f"\nGenerated: {_TODAY}")
    lines.append(f"Duration: {elapsed:.1f}s")
    lines.append(f"Sample size: {SAMPLE_SIZE} records\n")

    # Summary
    total_builders = len(results)
    all_pass = sum(1 for r in results if r.all_passed)
    total_tests = sum(r.total_tests for r in results)
    total_passed = sum(r.total_passed for r in results)

    lines.append(f"**Builders tested: {total_builders}**")
    lines.append(f"**All passing: {all_pass}/{total_builders}**")
    lines.append(f"**Total tests: {total_passed}/{total_tests} passed**\n")

    # Per-builder summary table
    lines.append("## Builder Summary\n")
    lines.append("| Builder | Baseline | Corruptions | Edge Cases | Status |")
    lines.append("|---------|----------|-------------|------------|--------|")

    for r in results:
        baseline = "OK" if r.baseline_ok else "FAIL"
        corr_pass = sum(1 for c in r.corruption_results if c["passed"])
        corr_total = len(r.corruption_results)
        edge_pass = sum(1 for e in r.edge_case_results if e["passed"])
        edge_total = len(r.edge_case_results)
        status = "PASS" if r.all_passed else "FAIL"

        lines.append(
            f"| {r.builder_name} | {baseline} "
            f"| {corr_pass}/{corr_total} "
            f"| {edge_pass}/{edge_total} "
            f"| {status} |"
        )
    lines.append("")

    # Failures detail
    failures = []
    for r in results:
        if not r.baseline_ok:
            failures.append({
                "builder": r.builder_name,
                "type": "baseline",
                "detail": r.baseline_error,
            })
        for c in r.corruption_results:
            if not c["passed"]:
                failures.append({
                    "builder": r.builder_name,
                    "type": f"corrupt_{c['field']}_{c['corruption']}",
                    "detail": c["error"],
                })
        for e in r.edge_case_results:
            if not e["passed"]:
                failures.append({
                    "builder": r.builder_name,
                    "type": f"edge_{e['case']}",
                    "detail": e["error"],
                })

    if failures:
        lines.append("## Failures Detail\n")
        lines.append("| Builder | Test Type | Error |")
        lines.append("|---------|-----------|-------|")
        for f in failures[:100]:  # Cap at 100 to keep report readable
            error_short = f["detail"][:120].replace("|", "/")
            lines.append(f"| {f['builder']} | {f['type']} | {error_short} |")
        if len(failures) > 100:
            lines.append(f"\n... and {len(failures) - 100} more failures")
        lines.append("")

    # Edge case summary
    lines.append("## Edge Case Results\n")
    lines.append("| Builder | Empty List | Single Record | Zero Cote | Single Course | 1-Horse Race |")
    lines.append("|---------|-----------|---------------|-----------|---------------|--------------|")
    for r in results:
        edge_map = {e["case"]: e["passed"] for e in r.edge_case_results}
        def _icon(case: str) -> str:
            if case not in edge_map:
                return "N/A"
            return "OK" if edge_map[case] else "FAIL"
        lines.append(
            f"| {r.builder_name} "
            f"| {_icon('empty_list')} "
            f"| {_icon('single_record')} "
            f"| {_icon('all_zero_cote')} "
            f"| {_icon('single_course')} "
            f"| {_icon('single_horse_race')} |"
        )
    lines.append("")

    lines.append("---")
    lines.append("RAM budget: < 2 GB (sample of 100 records, deep-copied per test)")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    print("\n" + "=" * 60)
    print("  RESILIENCE TESTER — Pilier 17")
    print(f"  {_TODAY}")
    print("=" * 60)

    t0 = time.monotonic()

    # Load sample
    if PARTANTS_MASTER.exists():
        print(f"\n  Loading {SAMPLE_SIZE} records from {PARTANTS_MASTER.name} ...")
        sample = load_sample(PARTANTS_MASTER, SAMPLE_SIZE)
    else:
        print(f"\n  partants_master.jsonl not found, using synthetic data ...")
        sample = _make_synthetic_records(SAMPLE_SIZE)

    print(f"  Loaded {len(sample)} records")

    # Discover builders
    builders = discover_list_builders()
    print(f"  Found {len(builders)} testable feature builders\n")

    if not builders:
        print("  [WARN] No feature builders found that accept list[dict].")
        print("  Generating report with zero results.\n")

    # Run tests
    all_results: list[TestResult] = []
    for mod_name, func_name, func in builders:
        print(f"  Testing {mod_name}.{func_name} ...", end=" ", flush=True)
        result = test_builder(mod_name, func_name, func, sample)
        all_results.append(result)
        status = "OK" if result.all_passed else f"FAIL ({result.total_passed}/{result.total_tests})"
        print(status)

    elapsed = time.monotonic() - t0

    # Generate report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report(all_results, elapsed)
    REPORT_PATH.write_text(report, encoding="utf-8")

    # Console summary
    total_builders = len(all_results)
    all_pass = sum(1 for r in all_results if r.all_passed)
    total_tests = sum(r.total_tests for r in all_results)
    total_passed = sum(r.total_passed for r in all_results)

    print(f"\n{'=' * 60}")
    print(f"  Builders: {all_pass}/{total_builders} fully passing")
    print(f"  Tests: {total_passed}/{total_tests} passed")
    print(f"  Duration: {elapsed:.1f}s")
    print(f"  Report: {REPORT_PATH}")
    print("=" * 60 + "\n")

    # Return 0 even if some builders fail (this is a diagnostic, not a gate)
    return 0


if __name__ == "__main__":
    sys.exit(main())
