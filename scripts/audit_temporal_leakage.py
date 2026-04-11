#!/usr/bin/env python3
"""Audit temporal leakage across all feature builders.

Checks for:
1. Direct use of result columns (position_arrivee, is_gagnant, rapport_*) as features
   for the SAME race (leakage if used without snapshot-before-update)
2. Accumulator update BEFORE feature write (update-before-snapshot pattern)
3. Use of future-looking columns without proper temporal ordering

Outputs a report to stdout and a CSV file.
"""
import ast
import csv
import os
import re
import sys
from pathlib import Path

BUILDERS_DIR = Path(__file__).resolve().parent.parent / "feature_builders"
OUTPUT_CSV = Path("D:/turf-data-pipeline/04_FEATURES/temporal_leakage_audit.csv")

# Columns that contain RESULT data — should never be used as raw input for same-race features
RESULT_COLUMNS = {
    "position_arrivee", "place", "is_gagnant", "is_place",
    "rapport_simple_gagnant", "rapport_simple_place",
    "rapport_couple_gagnant", "rapport_couple_place",
    "rapport_trio", "rapport_quarte", "rapport_quinte",
    "gains_course", "gains_carriere_avant_course",
    "temps_obtenu", "ecart_premier",
    # These are OK if used from PAST races via accumulators, but risky if same-race
}

# Columns that are result-adjacent (OK in accumulators, dangerous if same-race)
RESULT_ADJACENT = {
    "reduction_km_ms", "temps_ms", "allure",
    "ecart_precedent", "musique",
}

# Patterns indicating update-before-snapshot (dangerous)
# Good pattern: compute features → write → update accumulators
# Bad pattern: update accumulators → compute features → write


def scan_builder(filepath: Path) -> list[dict]:
    """Scan a single builder file for temporal leakage risks."""
    issues = []
    try:
        source = filepath.read_text(encoding="utf-8")
    except Exception:
        return issues

    lines = source.split("\n")
    filename = filepath.name

    # --- Check 1: Direct result column access in feature computation ---
    # Look for rec.get("result_column") or rec["result_column"] patterns
    for col in RESULT_COLUMNS:
        pattern = rf'rec\.get\(\s*["\']({col})["\']|rec\[\s*["\']({col})["\']'
        for i, line in enumerate(lines):
            if re.search(pattern, line):
                # Check if it's in an accumulator update section (after write) — that's OK
                # or in a feature computation section (before write) — that's a risk
                context = _get_context(lines, i)
                if context == "feature_computation":
                    issues.append({
                        "file": filename,
                        "line": i + 1,
                        "severity": "HIGH",
                        "type": "result_column_as_feature",
                        "detail": f"Uses result column '{col}' — potential same-race leakage",
                        "code": lines[i].strip()[:120],
                    })
                elif context == "accumulator_update":
                    # Using result in accumulator update is EXPECTED (for training labels)
                    pass
                else:
                    issues.append({
                        "file": filename,
                        "line": i + 1,
                        "severity": "MEDIUM",
                        "type": "result_column_unclear_context",
                        "detail": f"Uses result column '{col}' — context unclear",
                        "code": lines[i].strip()[:120],
                    })

    # --- Check 2: Update-before-snapshot pattern ---
    # Look for functions that update accumulators before writing features
    # Typical safe pattern:
    #   for rec in records:
    #       feat = compute(accumulators)  # snapshot
    #       fout.write(feat)
    #   for rec in records:  # second pass
    #       update(accumulators)
    #
    # Dangerous pattern:
    #   for rec in records:
    #       update(accumulators)  # update first!
    #       feat = compute(accumulators)
    #       fout.write(feat)

    # Heuristic: find the main processing function and check ordering
    write_positions = []
    update_patterns = []

    for i, line in enumerate(lines):
        stripped = line.strip()
        # Detect write operations
        if "fout.write" in stripped or "json.dumps(feat" in stripped:
            write_positions.append(i)
        # Detect accumulator updates (append, +=, [key] =)
        if any(p in stripped for p in [".append(", "[horse]", "[jockey]", "[trainer]",
                                        "[cheval]", "[hippo]", "+= 1", "+= red"]):
            # Check if this is updating a dict/deque that's used for features
            if any(acc in stripped for acc in ["_speeds", "_stats", "_wins", "_runs",
                                                "_history", "_counts", "_perf",
                                                "_records", "_streaks", "_elo"]):
                update_patterns.append(i)

    # Check if any update happens in the same loop as a write, BEFORE the write
    if write_positions and update_patterns:
        # Find functions containing writes
        for wp in write_positions:
            # Look for updates in the same function, between function start and write
            func_start = _find_function_start(lines, wp)
            updates_before_write = [u for u in update_patterns if func_start <= u < wp]

            # Now check: are these in the same loop?
            # If there's a "for rec in records" containing both update and write, that's risky
            for_loop = _find_enclosing_for(lines, wp)
            if for_loop is not None:
                updates_in_same_loop = [u for u in updates_before_write if u > for_loop]
                if updates_in_same_loop:
                    issues.append({
                        "file": filename,
                        "line": updates_in_same_loop[0] + 1,
                        "severity": "HIGH",
                        "type": "update_before_snapshot",
                        "detail": "Accumulator updated BEFORE feature write in same loop — temporal leakage",
                        "code": lines[updates_in_same_loop[0]].strip()[:120],
                    })

    # --- Check 3: Two-pass pattern verification ---
    # Good builders have 2 passes: first compute+write, then update
    # Check if there are 2 separate "for rec in" loops in _process_course
    process_funcs = _find_functions(source, "_process")
    for fname, fbody, fstart in process_funcs:
        for_loops = [i for i, l in enumerate(fbody.split("\n"))
                     if re.match(r'\s+for\s+rec\s+in\s+records', l)]
        if len(for_loops) >= 2:
            # Good: likely snapshot-before-update with 2 passes
            pass
        elif len(for_loops) == 1:
            # Only 1 loop — need to verify ordering within it
            # This was already checked above, but flag as info
            if any(u["file"] == filename and u["type"] == "update_before_snapshot" for u in issues):
                pass  # Already flagged
            else:
                # Single loop might be OK if it only reads from accumulators, never updates
                loop_body = fbody[fbody.find("for rec in"):]
                has_update = any(p in loop_body for p in [".append(", "+= 1", "]+= ", "[horse] =",
                                                           "[cheval] =", "[jockey] ="])
                has_write = "fout.write" in loop_body or "json.dumps" in loop_body
                if has_update and has_write:
                    issues.append({
                        "file": filename,
                        "line": fstart + for_loops[0] + 1,
                        "severity": "MEDIUM",
                        "type": "single_loop_update_write",
                        "detail": "Single loop contains both accumulator update and feature write — verify ordering",
                        "code": "(single loop pattern)",
                    })

    # --- Check 4: Same-race cross-runner leakage ---
    # If a builder accesses other runners' data from the SAME race
    for i, line in enumerate(lines):
        stripped = line.strip()
        if "for other" in stripped or "for runner" in stripped or "for r in records" in stripped:
            # Check if it reads result columns from other runners
            block = "\n".join(lines[i:min(i+20, len(lines))])
            for col in RESULT_COLUMNS:
                if col in block and "get" in block:
                    issues.append({
                        "file": filename,
                        "line": i + 1,
                        "severity": "HIGH",
                        "type": "same_race_cross_runner",
                        "detail": f"Accesses '{col}' from other runners in same race",
                        "code": stripped[:120],
                    })
                    break

    return issues


def _get_context(lines: list[str], line_idx: int) -> str:
    """Determine if a line is in feature computation or accumulator update context."""
    # Look backwards for function name or comment hints
    for i in range(line_idx, max(0, line_idx - 30), -1):
        l = lines[i].strip().lower()
        if "# update" in l or "# mise a jour" in l or "# maj" in l:
            return "accumulator_update"
        if "feat[" in l or "feat =" in l or "features" in l:
            return "feature_computation"
        if "def _update" in l or "def update" in l:
            return "accumulator_update"
        if "def _process" in l or "def _compute" in l or "def compute" in l:
            return "feature_computation"

    # Look at surrounding code
    nearby = "\n".join(lines[max(0, line_idx - 5):line_idx + 5])
    if "fout.write" in nearby or "json.dumps(feat" in nearby:
        return "feature_computation"

    return "unknown"


def _find_function_start(lines: list[str], line_idx: int) -> int:
    """Find the start of the function containing line_idx."""
    for i in range(line_idx, -1, -1):
        if lines[i].strip().startswith("def "):
            return i
    return 0


def _find_enclosing_for(lines: list[str], line_idx: int) -> int | None:
    """Find the for loop enclosing line_idx."""
    indent = len(lines[line_idx]) - len(lines[line_idx].lstrip())
    for i in range(line_idx, -1, -1):
        l = lines[i]
        l_indent = len(l) - len(l.lstrip())
        if l_indent < indent and l.strip().startswith("for "):
            return i
    return None


def _find_functions(source: str, prefix: str) -> list[tuple[str, str, int]]:
    """Find functions starting with prefix, return (name, body, start_line)."""
    results = []
    lines = source.split("\n")
    for i, line in enumerate(lines):
        m = re.match(rf'\s*def\s+({prefix}\w*)\s*\(', line)
        if m:
            # Find function body (until next def at same or lower indent)
            indent = len(line) - len(line.lstrip())
            end = len(lines)
            for j in range(i + 1, len(lines)):
                if lines[j].strip() and not lines[j].strip().startswith("#"):
                    j_indent = len(lines[j]) - len(lines[j].lstrip())
                    if j_indent <= indent and lines[j].strip().startswith("def "):
                        end = j
                        break
            body = "\n".join(lines[i:end])
            results.append((m.group(1), body, i))
    return results


def main():
    all_issues = []
    builders = sorted(BUILDERS_DIR.glob("*.py"))
    total = len(builders)

    print(f"Scanning {total} builder files for temporal leakage...\n")

    for i, bp in enumerate(builders):
        if bp.name == "__init__.py":
            continue
        issues = scan_builder(bp)
        all_issues.extend(issues)
        if (i + 1) % 100 == 0:
            print(f"  Scanned {i+1}/{total}...")

    # Write CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["file", "line", "severity", "type", "detail", "code"])
        writer.writeheader()
        writer.writerows(all_issues)

    # Summary
    high = [i for i in all_issues if i["severity"] == "HIGH"]
    medium = [i for i in all_issues if i["severity"] == "MEDIUM"]

    print(f"\n{'='*60}")
    print(f"TEMPORAL LEAKAGE AUDIT RESULTS")
    print(f"{'='*60}")
    print(f"Builders scanned: {total}")
    print(f"Total issues found: {len(all_issues)}")
    print(f"  HIGH severity: {len(high)}")
    print(f"  MEDIUM severity: {len(medium)}")
    print(f"\nOutput: {OUTPUT_CSV}")

    if high:
        print(f"\n{'='*60}")
        print("HIGH SEVERITY ISSUES:")
        print(f"{'='*60}")
        for issue in sorted(high, key=lambda x: (x["file"], x["line"])):
            print(f"  [{issue['type']}] {issue['file']}:{issue['line']}")
            print(f"    {issue['detail']}")
            print(f"    Code: {issue['code']}")
            print()

    if medium:
        print(f"\n{'='*60}")
        print("MEDIUM SEVERITY ISSUES:")
        print(f"{'='*60}")
        for issue in sorted(medium, key=lambda x: (x["file"], x["line"])):
            print(f"  [{issue['type']}] {issue['file']}:{issue['line']}")
            print(f"    {issue['detail']}")
            print()


if __name__ == "__main__":
    main()
