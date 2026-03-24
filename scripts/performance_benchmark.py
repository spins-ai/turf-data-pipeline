#!/usr/bin/env python3
"""
scripts/performance_benchmark.py — Pilier 1 : Performance
==========================================================
Benchmark each pipeline phase to measure throughput and memory usage.

Measures:
  - Read speed (MB/s) and parse speed (records/s) for each JSONL file
  - Memory usage per file (via tracemalloc)
  - Feature builder throughput (records/1000) for each builder module

Outputs:
  - quality/performance_report.md

RAM budget: < 2 GB (streams files, no bulk loading).

Usage:
    python scripts/performance_benchmark.py
"""

from __future__ import annotations

import gc
import importlib
import json
import sys
import time
import tracemalloc
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATA_MASTER_DIR, FEATURES_DIR, LABELS_DIR, OUTPUT_DIR, QUALITY_DIR
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"performance_benchmark_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_RECORDS = 10_000
FEATURE_BENCH_RECORDS = 1_000
REPORT_PATH = QUALITY_DIR / "performance_report.md"
FEATURE_BUILDERS_DIR = PROJECT_ROOT / "feature_builders"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _file_size_mb(p: Path) -> float:
    """Return file size in MB, or 0 if missing."""
    try:
        return p.stat().st_size / (1024 * 1024)
    except OSError:
        return 0.0


def _count_lines_fast(p: Path, max_lines: int = 0) -> int:
    """Count non-empty lines in a file, optionally stopping at max_lines."""
    count = 0
    try:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    count += 1
                    if max_lines and count >= max_lines:
                        break
    except OSError:
        pass
    return count


# ---------------------------------------------------------------------------
# Benchmark: JSONL read + parse
# ---------------------------------------------------------------------------
def benchmark_jsonl(path: Path, max_records: int = MAX_RECORDS) -> dict:
    """Benchmark reading and parsing a JSONL file.

    Returns dict with keys: file, size_mb, records_read, read_time_s,
    read_speed_mbs, parse_speed_recs, peak_memory_mb.
    """
    result = {
        "file": path.name,
        "size_mb": round(_file_size_mb(path), 2),
        "records_read": 0,
        "read_time_s": 0.0,
        "read_speed_mbs": 0.0,
        "parse_speed_recs": 0.0,
        "peak_memory_mb": 0.0,
    }

    if not path.exists():
        logger.warning("File not found: %s", path)
        return result

    gc.collect()
    tracemalloc.start()

    t0 = time.perf_counter()
    count = 0
    bytes_read = 0

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                bytes_read += len(line.encode("utf-8"))
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    pass
                count += 1
                if count >= max_records:
                    break
    except OSError as exc:
        logger.error("Error reading %s: %s", path, exc)

    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    result["records_read"] = count
    result["read_time_s"] = round(elapsed, 4)
    result["read_speed_mbs"] = round(
        (bytes_read / (1024 * 1024)) / max(elapsed, 1e-9), 2
    )
    result["parse_speed_recs"] = round(count / max(elapsed, 1e-9), 1)
    result["peak_memory_mb"] = round(peak / (1024 * 1024), 2)

    logger.info(
        "%s: %d records in %.2fs (%.1f rec/s, %.1f MB/s, peak %.1f MB)",
        path.name,
        count,
        elapsed,
        result["parse_speed_recs"],
        result["read_speed_mbs"],
        result["peak_memory_mb"],
    )
    return result


# ---------------------------------------------------------------------------
# Benchmark: feature builders
# ---------------------------------------------------------------------------
def _load_sample_records(path: Path, n: int = FEATURE_BENCH_RECORDS) -> list[dict]:
    """Load up to n records from a JSONL file for feature benchmarks."""
    records: list[dict] = []
    if not path.exists():
        return records
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
            if len(records) >= n:
                break
    return records


def benchmark_feature_builder(module_name: str, records: list[dict]) -> dict:
    """Benchmark a single feature builder module.

    Expects the module to expose a ``build_features(records)`` or
    ``compute(records)`` function. Returns timing info.
    """
    result = {
        "builder": module_name,
        "records": len(records),
        "time_s": 0.0,
        "recs_per_s": 0.0,
        "peak_memory_mb": 0.0,
        "status": "skipped",
    }

    try:
        mod = importlib.import_module(f"feature_builders.{module_name}")
    except Exception as exc:
        result["status"] = f"import_error: {exc}"
        return result

    # Look for a callable entry point
    func = getattr(mod, "build_features", None) or getattr(mod, "compute", None)
    if func is None:
        result["status"] = "no_entry_point"
        return result

    gc.collect()
    tracemalloc.start()
    t0 = time.perf_counter()

    try:
        func(records)
        result["status"] = "ok"
    except Exception as exc:
        result["status"] = f"error: {type(exc).__name__}: {exc}"

    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    result["time_s"] = round(elapsed, 4)
    result["recs_per_s"] = round(len(records) / max(elapsed, 1e-9), 1)
    result["peak_memory_mb"] = round(peak / (1024 * 1024), 2)

    logger.info(
        "  builder %s: %.3fs (%s)",
        module_name,
        elapsed,
        result["status"],
    )
    return result


# ---------------------------------------------------------------------------
# Discover JSONL files to benchmark
# ---------------------------------------------------------------------------
def discover_jsonl_files() -> list[Path]:
    """Return a list of JSONL files to benchmark across the project."""
    files: list[Path] = []

    # data_master
    if DATA_MASTER_DIR.is_dir():
        files.extend(sorted(DATA_MASTER_DIR.glob("*.jsonl")))

    # features
    if FEATURES_DIR.is_dir():
        files.extend(sorted(FEATURES_DIR.glob("*.jsonl")))

    # labels
    if LABELS_DIR.is_dir():
        files.extend(sorted(LABELS_DIR.glob("*.jsonl")))

    # output sub-directories (sample up to 20 for speed)
    if OUTPUT_DIR.is_dir():
        output_jsonl = sorted(OUTPUT_DIR.glob("*/*.jsonl"))
        files.extend(output_jsonl[:20])

    return files


def discover_feature_builders() -> list[str]:
    """Return names of feature builder modules (without .py extension)."""
    if not FEATURE_BUILDERS_DIR.is_dir():
        return []
    builders = []
    for p in sorted(FEATURE_BUILDERS_DIR.glob("*.py")):
        name = p.stem
        if name.startswith("_"):
            continue
        builders.append(name)
    return builders


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def generate_report(
    jsonl_results: list[dict],
    builder_results: list[dict],
) -> str:
    """Generate a Markdown report string."""
    lines: list[str] = []
    lines.append("# Performance Benchmark Report")
    lines.append(f"\nGenerated: {datetime.now().isoformat()}")
    lines.append(f"\nMax records per JSONL: {MAX_RECORDS}")
    lines.append(f"Feature benchmark records: {FEATURE_BENCH_RECORDS}\n")

    # --- JSONL table ---
    lines.append("## JSONL Read/Parse Performance\n")
    lines.append(
        "| File | Size (MB) | Records | Time (s) | Read (MB/s) "
        "| Parse (rec/s) | Peak RAM (MB) |"
    )
    lines.append("|------|-----------|---------|----------|-------------|"
                 "---------------|---------------|")
    for r in jsonl_results:
        lines.append(
            f"| {r['file']} | {r['size_mb']} | {r['records_read']} "
            f"| {r['read_time_s']} | {r['read_speed_mbs']} "
            f"| {r['parse_speed_recs']} | {r['peak_memory_mb']} |"
        )

    # --- Feature builders table ---
    lines.append("\n## Feature Builder Performance\n")
    lines.append(
        "| Builder | Records | Time (s) | Rec/s | Peak RAM (MB) | Status |"
    )
    lines.append(
        "|---------|---------|----------|-------|---------------|--------|"
    )
    for r in builder_results:
        lines.append(
            f"| {r['builder']} | {r['records']} | {r['time_s']} "
            f"| {r['recs_per_s']} | {r['peak_memory_mb']} | {r['status']} |"
        )

    # --- Summary ---
    ok_jsonl = [r for r in jsonl_results if r["records_read"] > 0]
    ok_builders = [r for r in builder_results if r["status"] == "ok"]
    lines.append("\n## Summary\n")
    lines.append(f"- JSONL files benchmarked: {len(ok_jsonl)} / {len(jsonl_results)}")
    if ok_jsonl:
        avg_speed = sum(r["read_speed_mbs"] for r in ok_jsonl) / len(ok_jsonl)
        avg_parse = sum(r["parse_speed_recs"] for r in ok_jsonl) / len(ok_jsonl)
        lines.append(f"- Average read speed: {avg_speed:.1f} MB/s")
        lines.append(f"- Average parse speed: {avg_parse:.0f} rec/s")

    lines.append(
        f"- Feature builders tested: {len(ok_builders)} / {len(builder_results)}"
    )
    if ok_builders:
        avg_fb = sum(r["recs_per_s"] for r in ok_builders) / len(ok_builders)
        lines.append(f"- Average builder throughput: {avg_fb:.0f} rec/s")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    logger.info("=== Performance Benchmark (Pilier 1) ===")

    # 1. Benchmark JSONL files
    jsonl_files = discover_jsonl_files()
    logger.info("Found %d JSONL files to benchmark", len(jsonl_files))
    jsonl_results = []
    for p in jsonl_files:
        jsonl_results.append(benchmark_jsonl(p))
        gc.collect()

    # 2. Benchmark feature builders
    from config import PARTANTS_MASTER  # noqa: E402

    logger.info("Loading sample records for feature benchmarks ...")
    sample_records = _load_sample_records(PARTANTS_MASTER, FEATURE_BENCH_RECORDS)
    logger.info("Loaded %d sample records", len(sample_records))

    builder_names = discover_feature_builders()
    logger.info("Found %d feature builders", len(builder_names))
    builder_results = []
    for name in builder_names:
        builder_results.append(benchmark_feature_builder(name, sample_records))
        gc.collect()

    # 3. Generate report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report(jsonl_results, builder_results)
    REPORT_PATH.write_text(report, encoding="utf-8")
    logger.info("Report written to %s", REPORT_PATH)

    # 4. Also save raw JSON for downstream tools
    raw_path = QUALITY_DIR / "performance_benchmark_raw.json"
    raw_data = {
        "timestamp": datetime.now().isoformat(),
        "jsonl_benchmarks": jsonl_results,
        "feature_builder_benchmarks": builder_results,
    }
    raw_path.write_text(json.dumps(raw_data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Raw JSON saved to %s", raw_path)

    print(f"\n[OK] Performance report: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
