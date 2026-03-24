#!/usr/bin/env python3
"""
adaptive_config.py -- Pilier 10 : Auto-Adaptativite.

Analyse les specs systeme (RAM, CPU, disque) et recommande des parametres
optimaux pour le pipeline : batch sizes, nombre de workers paralleles,
tailles de chunks. Genere un fichier config_tuned.py utilisable directement.

Genere un rapport dans quality/adaptive_config_report.md.

Usage :
    python scripts/adaptive_config.py
"""

from __future__ import annotations

import os
import platform
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import QUALITY_DIR, RAM_LIMITS  # noqa: E402

# ---------------------------------------------------------------------------
# System spec readers
# ---------------------------------------------------------------------------

def get_cpu_count() -> int:
    """Return the number of logical CPU cores."""
    return os.cpu_count() or 2


def get_total_ram_mb() -> int:
    """Return total physical RAM in MB."""
    try:
        if sys.platform == "win32":
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            mem = MEMORYSTATUSEX()
            mem.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem))
            return int(mem.ullTotalPhys / (1024 * 1024))
        else:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        return int(line.split()[1]) // 1024
    except Exception:
        pass
    return 8192  # fallback 8 GB


def get_available_ram_mb() -> int:
    """Return available RAM in MB."""
    try:
        if sys.platform == "win32":
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            mem = MEMORYSTATUSEX()
            mem.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem))
            return int(mem.ullAvailPhys / (1024 * 1024))
        else:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        return int(line.split()[1]) // 1024
    except Exception:
        pass
    return 4096  # fallback


def estimate_disk_speed_mbps(test_dir: Path) -> float:
    """Estimate sequential write speed by writing a small temp file."""
    test_dir.mkdir(parents=True, exist_ok=True)
    test_file = test_dir / "_disk_speed_test.tmp"
    block = b"\x00" * (1024 * 1024)  # 1 MB
    num_blocks = 16  # 16 MB test

    try:
        start = time.perf_counter()
        with open(test_file, "wb") as f:
            for _ in range(num_blocks):
                f.write(block)
            f.flush()
            os.fsync(f.fileno())
        elapsed = time.perf_counter() - start
        speed = num_blocks / elapsed if elapsed > 0 else 100.0
        return speed
    except Exception:
        return 100.0  # conservative fallback
    finally:
        try:
            test_file.unlink(missing_ok=True)
        except Exception:
            pass


def get_disk_free_gb(path: Path) -> float:
    """Return free disk space in GB."""
    try:
        usage = shutil.disk_usage(path)
        return usage.free / (1024**3)
    except Exception:
        return 50.0


# ---------------------------------------------------------------------------
# Recommendation engine
# ---------------------------------------------------------------------------

def compute_recommendations(
    total_ram_mb: int,
    available_ram_mb: int,
    cpu_cores: int,
    disk_speed_mbps: float,
    disk_free_gb: float,
) -> dict:
    """Compute optimal pipeline parameters based on system specs."""
    recs: dict = {}

    # --- Workers ---
    # Reserve 1 core for OS, limit by RAM (each worker needs ~1 GB headroom)
    max_by_cpu = max(1, cpu_cores - 1)
    max_by_ram = max(1, available_ram_mb // 1500)
    recs["max_workers"] = min(max_by_cpu, max_by_ram, 8)

    # --- Batch sizes ---
    # Scraper batch: more RAM = bigger batches, but cap at 2000
    ram_factor = available_ram_mb / 4096  # normalise to 4GB
    recs["scraper_batch_size"] = min(2000, max(100, int(500 * ram_factor)))
    recs["api_batch_size"] = min(1000, max(50, int(250 * ram_factor)))

    # --- Chunk sizes for JSONL processing ---
    # Larger chunks are faster but use more RAM
    recs["jsonl_chunk_size"] = min(50000, max(5000, int(20000 * ram_factor)))

    # --- Feature builder ---
    recs["feature_chunk_rows"] = min(100000, max(10000, int(50000 * ram_factor)))

    # --- Concurrent heavy tasks ---
    heavy_ram_mb = 2048
    recs["max_concurrent_heavy"] = max(1, min(3, available_ram_mb // heavy_ram_mb))

    # --- RAM limits (adjusted) ---
    scale = min(2.0, total_ram_mb / 8192)
    adjusted_limits: dict[str, int] = {}
    for key, base_mb in RAM_LIMITS.items():
        adjusted = int(base_mb * scale)
        # Never exceed 80% of total RAM for a single task
        adjusted = min(adjusted, int(total_ram_mb * 0.8))
        adjusted_limits[key] = adjusted
    recs["ram_limits"] = adjusted_limits

    # --- Disk-dependent ---
    if disk_speed_mbps < 50:
        # Slow disk: smaller batches to avoid I/O bottleneck
        recs["scraper_batch_size"] = min(recs["scraper_batch_size"], 300)
        recs["jsonl_chunk_size"] = min(recs["jsonl_chunk_size"], 10000)
        recs["disk_note"] = "Slow disk detected; reduced batch sizes."
    else:
        recs["disk_note"] = "Disk speed adequate."

    # Low disk space warning
    if disk_free_gb < 20:
        recs["disk_warning"] = (
            f"Only {disk_free_gb:.1f} GB free. Consider freeing space."
        )

    # --- Playwright concurrency ---
    # Each Playwright browser uses ~200-400 MB
    recs["playwright_concurrency"] = max(1, min(3, available_ram_mb // 500))

    return recs


def generate_tuned_config(recs: dict) -> str:
    """Generate a config_tuned.py file content from recommendations."""
    lines: list[str] = [
        '"""',
        "config_tuned.py -- Auto-generated optimal configuration.",
        "",
        "Generated by scripts/adaptive_config.py (Pilier 10).",
        f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "Import these values to override defaults from config.py:",
        "    from config_tuned import TUNED_BATCH_SIZE, TUNED_MAX_WORKERS, ...",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "# --- Workers & Concurrency ---",
        f"TUNED_MAX_WORKERS: int = {recs['max_workers']}",
        f"TUNED_MAX_CONCURRENT_HEAVY: int = {recs['max_concurrent_heavy']}",
        f"TUNED_PLAYWRIGHT_CONCURRENCY: int = {recs['playwright_concurrency']}",
        "",
        "# --- Batch Sizes ---",
        f"TUNED_BATCH_SIZE: int = {recs['scraper_batch_size']}",
        f"TUNED_API_BATCH_SIZE: int = {recs['api_batch_size']}",
        "",
        "# --- Chunk Sizes ---",
        f"TUNED_JSONL_CHUNK_SIZE: int = {recs['jsonl_chunk_size']}",
        f"TUNED_FEATURE_CHUNK_ROWS: int = {recs['feature_chunk_rows']}",
        "",
        "# --- RAM Limits (MB) ---",
        "TUNED_RAM_LIMITS: dict[str, int] = {",
    ]
    for key, val in sorted(recs["ram_limits"].items()):
        lines.append(f'    "{key}": {val},')
    lines.append("}")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(
    total_ram_mb: int,
    available_ram_mb: int,
    cpu_cores: int,
    disk_speed_mbps: float,
    disk_free_gb: float,
    recs: dict,
) -> str:
    """Generate the adaptive config markdown report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: list[str] = [
        "# Adaptive Configuration Report (Pilier 10)",
        "",
        f"Generated: {now}",
        "",
        "## System Specifications",
        "",
        "| Spec | Value |",
        "|------|-------|",
        f"| Platform | {platform.platform()} |",
        f"| CPU Cores (logical) | {cpu_cores} |",
        f"| Total RAM | {total_ram_mb / 1024:.1f} GB ({total_ram_mb} MB) |",
        f"| Available RAM | {available_ram_mb / 1024:.1f} GB ({available_ram_mb} MB) |",
        f"| Disk Speed (est.) | {disk_speed_mbps:.1f} MB/s |",
        f"| Disk Free | {disk_free_gb:.1f} GB |",
        "",
        "## Recommended Configuration",
        "",
        "| Parameter | Recommended Value |",
        "|-----------|-------------------|",
        f"| max_workers | {recs['max_workers']} |",
        f"| max_concurrent_heavy | {recs['max_concurrent_heavy']} |",
        f"| playwright_concurrency | {recs['playwright_concurrency']} |",
        f"| scraper_batch_size | {recs['scraper_batch_size']} |",
        f"| api_batch_size | {recs['api_batch_size']} |",
        f"| jsonl_chunk_size | {recs['jsonl_chunk_size']} |",
        f"| feature_chunk_rows | {recs['feature_chunk_rows']} |",
        "",
        "## RAM Budget per Task Type",
        "",
        "| Task Type | Recommended (MB) | Default (MB) |",
        "|-----------|-------------------|--------------|",
    ]

    for key in sorted(recs["ram_limits"]):
        rec_val = recs["ram_limits"][key]
        default_val = RAM_LIMITS.get(key, "N/A")
        lines.append(f"| {key} | {rec_val} | {default_val} |")

    lines.append("")

    # Notes
    lines.append("## Notes")
    lines.append("")
    lines.append(f"- {recs['disk_note']}")
    if "disk_warning" in recs:
        lines.append(f"- **Warning**: {recs['disk_warning']}")
    lines.append(
        f"- Tuned config written to `config_tuned.py` at project root."
    )
    lines.append("")
    lines.append("---")
    lines.append("*Report generated by adaptive_config.py (Pilier 10)*")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """Analyse system and generate adaptive configuration."""
    print("=== Pilier 10 : Adaptive Config ===\n")

    # Gather specs
    print("Reading system specs...")
    total_ram = get_total_ram_mb()
    avail_ram = get_available_ram_mb()
    cpu_cores = get_cpu_count()
    disk_free = get_disk_free_gb(PROJECT_ROOT)

    print("Estimating disk speed (16 MB write test)...")
    disk_speed = estimate_disk_speed_mbps(PROJECT_ROOT / "cache")

    print(f"  CPU cores:      {cpu_cores}")
    print(f"  Total RAM:      {total_ram / 1024:.1f} GB")
    print(f"  Available RAM:  {avail_ram / 1024:.1f} GB")
    print(f"  Disk speed:     {disk_speed:.1f} MB/s")
    print(f"  Disk free:      {disk_free:.1f} GB")

    # Compute recommendations
    print("\nComputing optimal parameters...")
    recs = compute_recommendations(total_ram, avail_ram, cpu_cores, disk_speed, disk_free)

    print(f"  max_workers:           {recs['max_workers']}")
    print(f"  scraper_batch_size:    {recs['scraper_batch_size']}")
    print(f"  jsonl_chunk_size:      {recs['jsonl_chunk_size']}")
    print(f"  max_concurrent_heavy:  {recs['max_concurrent_heavy']}")

    # Write tuned config
    tuned_path = PROJECT_ROOT / "config_tuned.py"
    tuned_content = generate_tuned_config(recs)
    tuned_path.write_text(tuned_content, encoding="utf-8")
    print(f"\nTuned config written to {tuned_path}")

    # Write report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report_path = QUALITY_DIR / "adaptive_config_report.md"
    report_content = generate_report(
        total_ram, avail_ram, cpu_cores, disk_speed, disk_free, recs
    )
    report_path.write_text(report_content, encoding="utf-8")
    print(f"Report written to {report_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
