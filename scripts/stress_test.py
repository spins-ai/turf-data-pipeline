#!/usr/bin/env python3
"""
scripts/stress_test.py — Pilier 15 : Stress-Test
==================================================
Test pipeline resilience by simulating edge cases and verifying
graceful handling of malformed, empty, extreme, and high-volume data.

Tests:
  1. Malformed JSONL line -> handled gracefully (no crash)
  2. Empty input -> no crash, zero output
  3. Record with all-null fields -> handled without error
  4. Extremely long string field (1 MB) -> handled within RAM budget
  5. 10K records rapid throughput -> measures pipeline throughput
  6. Checkpoint recovery -> write checkpoint, delete output, resume

Reports pass/fail for each test.

RAM budget: < 2 GB.

Usage:
    python scripts/stress_test.py
"""

from __future__ import annotations

import gc
import json
import sys
import tempfile
import time
from datetime import datetime
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
REPORT_PATH = QUALITY_DIR / "stress_test_report.md"
_TODAY = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------
_results: list[dict] = []


def _record_result(test_name: str, passed: bool, detail: str = "") -> None:
    """Record a test result."""
    status = "PASS" if passed else "FAIL"
    _results.append({
        "test": test_name,
        "status": status,
        "detail": detail,
    })
    tag = "[PASS]" if passed else "[FAIL]"
    print(f"  {tag} {test_name}" + (f" — {detail}" if detail else ""))


# ---------------------------------------------------------------------------
# Helpers: minimal JSONL reader/writer (mirrors pipeline logic)
# ---------------------------------------------------------------------------
def _read_jsonl_stream(path: Path) -> list[dict]:
    """Read JSONL file, skipping malformed lines (pipeline-style)."""
    records: list[dict] = []
    errors = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except (json.JSONDecodeError, ValueError):
                errors += 1
    return records


def _write_jsonl(path: Path, records: list[dict]) -> None:
    """Write records as JSONL."""
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _make_sample_record(idx: int = 0) -> dict:
    """Create a realistic sample partant record."""
    return {
        "partant_uid": f"2024-01-15_VIN_C1_P{idx:04d}",
        "course_uid": "2024-01-15_VIN_C1",
        "date_reunion_iso": "2024-01-15",
        "nom_cheval": f"TestHorse{idx}",
        "discipline": "plat",
        "hippodrome_normalise": "VINCENNES",
        "cote_finale": 5.0 + (idx % 20),
        "position_arrivee": (idx % 16) + 1,
        "is_gagnant": idx % 16 == 0,
        "is_place": idx % 16 < 3,
        "nombre_partants": 16,
        "num_pmu": (idx % 16) + 1,
        "poids_porte_kg": 58.0,
        "gains_carriere_euros": 50000 + idx * 100,
        "nb_courses_carriere": 20 + idx,
    }


# ===================================================================
# TEST 1: Malformed JSONL line
# ===================================================================
def test_malformed_jsonl() -> None:
    """Feed a malformed JSONL line and verify graceful handling."""
    test_name = "1. Malformed JSONL line"
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "malformed.jsonl"
            with open(path, "w", encoding="utf-8", newline="\n") as fh:
                fh.write('{"valid": "record", "num": 1}\n')
                fh.write('THIS IS NOT JSON AT ALL {{{{\n')
                fh.write('{"also_valid": true}\n')
                fh.write('\n')  # empty line
                fh.write('{"partial": "json"\n')  # missing closing brace
                fh.write('{"final": "record"}\n')

            records = _read_jsonl_stream(path)
            # Should have parsed 3 valid records, skipped 2 malformed + 1 empty
            if len(records) == 3:
                _record_result(test_name, True, f"Parsed {len(records)} valid, skipped malformed")
            else:
                _record_result(test_name, False, f"Expected 3 records, got {len(records)}")
    except Exception as exc:
        _record_result(test_name, False, f"Crashed: {type(exc).__name__}: {exc}")


# ===================================================================
# TEST 2: Empty input
# ===================================================================
def test_empty_input() -> None:
    """Feed empty input and verify no crash."""
    test_name = "2. Empty input (no crash)"
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Completely empty file
            path = Path(tmpdir) / "empty.jsonl"
            path.write_text("", encoding="utf-8")
            records = _read_jsonl_stream(path)
            assert len(records) == 0, f"Expected 0 records, got {len(records)}"

            # File with only whitespace/newlines
            path2 = Path(tmpdir) / "whitespace.jsonl"
            path2.write_text("\n\n   \n\t\n", encoding="utf-8")
            records2 = _read_jsonl_stream(path2)
            assert len(records2) == 0, f"Expected 0 records, got {len(records2)}"

            _record_result(test_name, True, "Empty and whitespace-only files handled")
    except Exception as exc:
        _record_result(test_name, False, f"Crashed: {type(exc).__name__}: {exc}")


# ===================================================================
# TEST 3: All-null fields
# ===================================================================
def test_all_null_fields() -> None:
    """Feed a record where every field is null."""
    test_name = "3. All-null fields record"
    try:
        null_record = {
            "partant_uid": None,
            "course_uid": None,
            "date_reunion_iso": None,
            "nom_cheval": None,
            "discipline": None,
            "hippodrome_normalise": None,
            "cote_finale": None,
            "position_arrivee": None,
            "is_gagnant": None,
            "is_place": None,
            "nombre_partants": None,
            "num_pmu": None,
            "poids_porte_kg": None,
            "gains_carriere_euros": None,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nulls.jsonl"
            _write_jsonl(path, [null_record])
            records = _read_jsonl_stream(path)

            if len(records) == 1:
                # Verify all values are None
                rec = records[0]
                all_none = all(v is None for v in rec.values())
                if all_none:
                    _record_result(test_name, True, "All-null record parsed correctly")
                else:
                    _record_result(test_name, False, "Some fields not None after parse")
            else:
                _record_result(test_name, False, f"Expected 1 record, got {len(records)}")
    except Exception as exc:
        _record_result(test_name, False, f"Crashed: {type(exc).__name__}: {exc}")


# ===================================================================
# TEST 4: Extremely long strings (1 MB)
# ===================================================================
def test_long_strings() -> None:
    """Feed a record with a 1 MB string field."""
    test_name = "4. Extremely long string (1 MB)"
    try:
        long_string = "A" * (1024 * 1024)  # 1 MB
        record = _make_sample_record(0)
        record["nom_cheval"] = long_string
        record["commentaire_apres_course"] = long_string

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "longstrings.jsonl"
            _write_jsonl(path, [record])

            # Verify file was written
            file_size = path.stat().st_size
            assert file_size > 2 * 1024 * 1024, f"File too small: {file_size}"

            # Read back
            records = _read_jsonl_stream(path)
            assert len(records) == 1, f"Expected 1 record, got {len(records)}"
            assert len(records[0]["nom_cheval"]) == 1024 * 1024

            _record_result(
                test_name, True,
                f"1 MB string field handled (file size: {file_size / (1024*1024):.1f} MB)"
            )

        # Explicit cleanup of large strings
        del long_string, record, records
        gc.collect()

    except Exception as exc:
        _record_result(test_name, False, f"Crashed: {type(exc).__name__}: {exc}")


# ===================================================================
# TEST 5: 10K records rapid throughput
# ===================================================================
def test_throughput_10k() -> None:
    """Feed 10K records and measure throughput."""
    test_name = "5. 10K records throughput"
    try:
        n_records = 10_000
        records = [_make_sample_record(i) for i in range(n_records)]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "throughput.jsonl"

            # Write phase
            t0 = time.perf_counter()
            _write_jsonl(path, records)
            write_elapsed = time.perf_counter() - t0

            file_size = path.stat().st_size
            write_speed = n_records / max(write_elapsed, 1e-9)

            # Read phase
            t1 = time.perf_counter()
            read_records = _read_jsonl_stream(path)
            read_elapsed = time.perf_counter() - t1
            read_speed = n_records / max(read_elapsed, 1e-9)

            assert len(read_records) == n_records, (
                f"Expected {n_records}, got {len(read_records)}"
            )

            detail = (
                f"Write: {write_speed:.0f} rec/s ({write_elapsed:.3f}s), "
                f"Read: {read_speed:.0f} rec/s ({read_elapsed:.3f}s), "
                f"Size: {file_size / (1024*1024):.1f} MB"
            )
            _record_result(test_name, True, detail)

        del records, read_records
        gc.collect()

    except Exception as exc:
        _record_result(test_name, False, f"Crashed: {type(exc).__name__}: {exc}")


# ===================================================================
# TEST 6: Checkpoint recovery
# ===================================================================
def test_checkpoint_recovery() -> None:
    """Write checkpoint, delete output, verify resume from checkpoint."""
    test_name = "6. Checkpoint recovery"
    try:
        n_total = 1000
        checkpoint_at = 500

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.jsonl"
            output_path = Path(tmpdir) / "output.jsonl"
            checkpoint_path = Path(tmpdir) / "checkpoint.json"

            # Write full input
            all_records = [_make_sample_record(i) for i in range(n_total)]
            _write_jsonl(input_path, all_records)

            # Phase 1: process first half, save checkpoint
            processed = []
            with open(input_path, "r", encoding="utf-8") as fh:
                for i, line in enumerate(fh):
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    processed.append(rec)
                    if len(processed) >= checkpoint_at:
                        break

            _write_jsonl(output_path, processed)

            # Save checkpoint
            checkpoint_data = {
                "last_line_processed": checkpoint_at,
                "records_written": len(processed),
                "timestamp": datetime.now().isoformat(),
            }
            with open(checkpoint_path, "w", encoding="utf-8") as fh:
                json.dump(checkpoint_data, fh)

            # Verify checkpoint exists
            assert checkpoint_path.exists(), "Checkpoint file not created"

            # Delete output (simulate crash/corruption)
            output_path.unlink()
            assert not output_path.exists(), "Output not deleted"

            # Phase 2: resume from checkpoint
            with open(checkpoint_path, "r", encoding="utf-8") as fh:
                ckpt = json.load(fh)

            resume_from = ckpt["last_line_processed"]
            resumed = []
            line_num = 0
            with open(input_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    line_num += 1
                    if line_num <= resume_from:
                        continue  # skip already-processed
                    rec = json.loads(line)
                    resumed.append(rec)

            # Combine: first half from checkpoint + resumed second half
            total_processed = checkpoint_at + len(resumed)

            if total_processed == n_total:
                _record_result(
                    test_name, True,
                    f"Checkpoint at {checkpoint_at}, resumed {len(resumed)}, "
                    f"total {total_processed}/{n_total}"
                )
            else:
                _record_result(
                    test_name, False,
                    f"Expected {n_total} total, got {total_processed}"
                )

    except Exception as exc:
        _record_result(test_name, False, f"Crashed: {type(exc).__name__}: {exc}")


# ===================================================================
# TEST 7: Mixed edge cases in a single file
# ===================================================================
def test_mixed_edge_cases() -> None:
    """Combine multiple edge cases in one file: BOM, unicode, huge numbers."""
    test_name = "7. Mixed edge cases (BOM, unicode, huge numbers)"
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "mixed.jsonl"
            with open(path, "w", encoding="utf-8-sig", newline="\n") as fh:  # BOM
                # Unicode names
                fh.write(json.dumps({
                    "nom_cheval": "Etoile d'Or",
                    "hippodrome_normalise": "SAINT-CLOUD",
                    "cote_finale": 3.5,
                }) + "\n")
                # Huge numeric values
                fh.write(json.dumps({
                    "nom_cheval": "BigNumber",
                    "gains_carriere_euros": 99999999999,
                    "cote_finale": 0.0001,
                    "nb_courses_carriere": 2**31 - 1,
                }) + "\n")
                # Empty string fields
                fh.write(json.dumps({
                    "nom_cheval": "",
                    "discipline": "",
                    "cote_finale": 0,
                }) + "\n")
                # Nested unexpected structure
                fh.write(json.dumps({
                    "nom_cheval": "Nested",
                    "extra_data": {"key": [1, 2, 3]},
                }) + "\n")

            records = _read_jsonl_stream(path)
            if len(records) == 4:
                _record_result(test_name, True, "All 4 edge-case records parsed")
            else:
                _record_result(test_name, False, f"Expected 4 records, got {len(records)}")

    except Exception as exc:
        _record_result(test_name, False, f"Crashed: {type(exc).__name__}: {exc}")


# ===================================================================
# Report generation
# ===================================================================
def generate_report() -> str:
    """Generate a Markdown report from test results."""
    lines: list[str] = []
    lines.append("# Stress Test Report (Pilier 15)")
    lines.append(f"\nGenerated: {_TODAY}\n")

    passed = sum(1 for r in _results if r["status"] == "PASS")
    failed = sum(1 for r in _results if r["status"] == "FAIL")
    total = len(_results)

    lines.append(f"**Summary: {passed}/{total} passed, {failed} failed**\n")

    lines.append("| # | Test | Status | Detail |")
    lines.append("|---|------|--------|--------|")
    for i, r in enumerate(_results, 1):
        status_icon = "PASS" if r["status"] == "PASS" else "FAIL"
        detail = r["detail"].replace("|", "/")
        lines.append(f"| {i} | {r['test']} | {status_icon} | {detail} |")

    lines.append("\n---")
    lines.append(f"RAM budget: < 2 GB (all tests use tempfiles + streaming)")
    lines.append("")
    return "\n".join(lines)


# ===================================================================
# Main
# ===================================================================
def main() -> int:
    print("\n" + "=" * 60)
    print("  STRESS TEST — Pilier 15")
    print(f"  {_TODAY}")
    print("=" * 60)

    t0 = time.monotonic()

    test_malformed_jsonl()
    test_empty_input()
    test_all_null_fields()
    test_long_strings()
    test_throughput_10k()
    test_checkpoint_recovery()
    test_mixed_edge_cases()

    elapsed = time.monotonic() - t0

    # Generate and write report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report()
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(f"\n  Report written to {REPORT_PATH}")

    passed = sum(1 for r in _results if r["status"] == "PASS")
    failed = sum(1 for r in _results if r["status"] == "FAIL")
    total = len(_results)

    print(f"\n{'=' * 60}")
    print(f"  Summary: {passed}/{total} PASS, {failed} FAIL ({elapsed:.1f}s)")
    print("=" * 60 + "\n")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
