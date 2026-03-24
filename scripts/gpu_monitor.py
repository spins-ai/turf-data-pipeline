#!/usr/bin/env python3
"""
scripts/gpu_monitor.py — Pilier 23 : GPU/Monitoring/Haute Dispo
================================================================
Check GPU availability and monitor system health.

Steps:
  1. Check GPU availability (CUDA/ROCm via torch or tensorflow)
  2. Monitor system health: CPU usage, RAM, disk I/O, network
  3. Check if critical processes are running
  4. Output to quality/system_health_report.md

This is preparation for ML model training (XGBoost, LightGBM, neural nets).

RAM budget: < 1 GB (monitoring only, no data loading).

Usage:
    python scripts/gpu_monitor.py
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
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

from config import (  # noqa: E402
    QUALITY_DIR,
)
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"gpu_monitor_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_PATH = QUALITY_DIR / "system_health_report.md"

# Critical processes to check (name -> description)
CRITICAL_PROCESSES: dict[str, str] = {
    "python": "Python interpreter (pipeline scripts)",
}

# Disk usage warning thresholds
DISK_WARN_PERCENT = 85
DISK_CRITICAL_PERCENT = 95

# RAM warning thresholds
RAM_WARN_PERCENT = 85
RAM_CRITICAL_PERCENT = 95


# ---------------------------------------------------------------------------
# GPU checks
# ---------------------------------------------------------------------------
def check_gpu_torch() -> dict[str, Any]:
    """Check GPU availability via PyTorch."""
    result: dict[str, Any] = {"available": False, "backend": "torch"}
    try:
        import torch
        result["torch_version"] = torch.__version__
        result["cuda_available"] = torch.cuda.is_available()
        if torch.cuda.is_available():
            result["available"] = True
            result["cuda_version"] = torch.version.cuda or "unknown"
            result["gpu_count"] = torch.cuda.device_count()
            result["gpus"] = []
            for i in range(torch.cuda.device_count()):
                props = torch.cuda.get_device_properties(i)
                result["gpus"].append({
                    "index": i,
                    "name": props.name,
                    "total_memory_mb": props.total_mem // (1024 * 1024),
                    "compute_capability": f"{props.major}.{props.minor}",
                })
        # Check ROCm
        if hasattr(torch.version, "hip") and torch.version.hip:
            result["rocm_available"] = True
            result["rocm_version"] = torch.version.hip
            result["available"] = True
    except ImportError:
        result["error"] = "torch not installed"
    except Exception as e:
        result["error"] = str(e)
    return result


def check_gpu_tensorflow() -> dict[str, Any]:
    """Check GPU availability via TensorFlow."""
    result: dict[str, Any] = {"available": False, "backend": "tensorflow"}
    try:
        import tensorflow as tf
        result["tf_version"] = tf.__version__
        gpus = tf.config.list_physical_devices("GPU")
        result["gpu_count"] = len(gpus)
        if gpus:
            result["available"] = True
            result["gpus"] = [{"name": g.name, "type": g.device_type} for g in gpus]
    except ImportError:
        result["error"] = "tensorflow not installed"
    except Exception as e:
        result["error"] = str(e)
    return result


def check_nvidia_smi() -> dict[str, Any]:
    """Check GPU via nvidia-smi command."""
    result: dict[str, Any] = {"available": False, "tool": "nvidia-smi"}
    try:
        output = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name,memory.total,memory.used,utilization.gpu",
             "--format=csv,noheader"],
            timeout=10,
            stderr=subprocess.DEVNULL,
        ).decode("utf-8", errors="replace").strip()
        if output:
            result["available"] = True
            result["gpus"] = []
            for line in output.split("\n"):
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 4:
                    result["gpus"].append({
                        "name": parts[0],
                        "memory_total": parts[1],
                        "memory_used": parts[2],
                        "utilization": parts[3],
                    })
    except FileNotFoundError:
        result["error"] = "nvidia-smi not found"
    except subprocess.TimeoutExpired:
        result["error"] = "nvidia-smi timed out"
    except Exception as e:
        result["error"] = str(e)
    return result


# ---------------------------------------------------------------------------
# System health checks
# ---------------------------------------------------------------------------
def check_cpu() -> dict[str, Any]:
    """Check CPU information and usage."""
    result: dict[str, Any] = {
        "processor": platform.processor() or "unknown",
        "architecture": platform.machine(),
        "cpu_count_logical": os.cpu_count() or 0,
    }
    try:
        import psutil
        result["cpu_percent"] = psutil.cpu_percent(interval=1)
        result["cpu_count_physical"] = psutil.cpu_count(logical=False) or 0
        freq = psutil.cpu_freq()
        if freq:
            result["cpu_freq_mhz"] = round(freq.current, 0)
    except ImportError:
        # Fallback: no psutil
        result["note"] = "psutil not installed, limited CPU info"
    return result


def check_ram() -> dict[str, Any]:
    """Check RAM usage."""
    result: dict[str, Any] = {}
    try:
        import psutil
        mem = psutil.virtual_memory()
        result["total_mb"] = mem.total // (1024 * 1024)
        result["available_mb"] = mem.available // (1024 * 1024)
        result["used_mb"] = mem.used // (1024 * 1024)
        result["percent_used"] = mem.percent
        if mem.percent >= RAM_CRITICAL_PERCENT:
            result["status"] = "CRITICAL"
        elif mem.percent >= RAM_WARN_PERCENT:
            result["status"] = "WARN"
        else:
            result["status"] = "OK"
    except ImportError:
        # Fallback for Windows
        try:
            if sys.platform == "win32":
                import ctypes
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
                ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem))
                total_mb = mem.ullTotalPhys // (1024 * 1024)
                avail_mb = mem.ullAvailPhys // (1024 * 1024)
                result["total_mb"] = total_mb
                result["available_mb"] = avail_mb
                result["used_mb"] = total_mb - avail_mb
                result["percent_used"] = round((total_mb - avail_mb) / total_mb * 100, 1) if total_mb else 0
                result["status"] = "OK"
        except Exception:
            result["error"] = "Cannot determine RAM (install psutil)"
    return result


def check_disk() -> dict[str, Any]:
    """Check disk usage for the project directory."""
    result: dict[str, Any] = {}
    try:
        usage = shutil.disk_usage(str(PROJECT_ROOT))
        total_gb = usage.total / (1024 ** 3)
        used_gb = usage.used / (1024 ** 3)
        free_gb = usage.free / (1024 ** 3)
        percent_used = (usage.used / usage.total) * 100

        result["total_gb"] = round(total_gb, 1)
        result["used_gb"] = round(used_gb, 1)
        result["free_gb"] = round(free_gb, 1)
        result["percent_used"] = round(percent_used, 1)

        if percent_used >= DISK_CRITICAL_PERCENT:
            result["status"] = "CRITICAL"
        elif percent_used >= DISK_WARN_PERCENT:
            result["status"] = "WARN"
        else:
            result["status"] = "OK"
    except Exception as e:
        result["error"] = str(e)
    return result


def check_network() -> dict[str, Any]:
    """Check basic network connectivity."""
    result: dict[str, Any] = {}
    try:
        import psutil
        net = psutil.net_io_counters()
        result["bytes_sent"] = net.bytes_sent
        result["bytes_recv"] = net.bytes_recv
        result["packets_sent"] = net.packets_sent
        result["packets_recv"] = net.packets_recv
        result["errors_in"] = net.errin
        result["errors_out"] = net.errout
    except ImportError:
        result["note"] = "psutil not installed, no network stats"

    # Basic connectivity test
    try:
        import socket
        socket.setdefaulttimeout(5)
        socket.create_connection(("8.8.8.8", 53))
        result["internet_connected"] = True
    except (OSError, socket.timeout):
        result["internet_connected"] = False

    return result


def check_critical_processes() -> dict[str, Any]:
    """Check if critical processes are running."""
    result: dict[str, Any] = {"processes": {}}
    try:
        import psutil
        running_names = set()
        for proc in psutil.process_iter(["name"]):
            try:
                name = proc.info["name"]
                if name:
                    running_names.add(name.lower())
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        for proc_name, description in CRITICAL_PROCESSES.items():
            is_running = any(proc_name.lower() in rn for rn in running_names)
            result["processes"][proc_name] = {
                "description": description,
                "running": is_running,
            }
    except ImportError:
        result["note"] = "psutil not installed, cannot check processes"
    return result


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def _generate_report(results: dict[str, Any], elapsed: float) -> None:
    """Write system health report as Markdown."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Pilier 23 — Sante Systeme & GPU Monitoring",
        "",
        f"**Date**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Machine**: {platform.node()} ({platform.system()} {platform.release()})",
        f"**Python**: {platform.python_version()}",
        f"**Duree scan**: {elapsed:.1f}s",
        "",
    ]

    # GPU section
    lines.append("## GPU")
    lines.append("")
    gpu_available = False

    for key in ["gpu_torch", "gpu_tensorflow", "gpu_nvidia_smi"]:
        info = results.get(key, {})
        if info.get("available"):
            gpu_available = True
            backend = info.get("backend") or info.get("tool", key)
            lines.append(f"### {backend}")
            lines.append("")
            for k, v in info.items():
                if k in ("available", "backend", "tool"):
                    continue
                if k == "gpus" and isinstance(v, list):
                    for gpu in v:
                        lines.append(f"- GPU: {gpu}")
                else:
                    lines.append(f"- **{k}**: {v}")
            lines.append("")

    if not gpu_available:
        lines.append("**Aucun GPU detecte.** Les modeles ML utiliseront le CPU.")
        lines.append("")
        for key in ["gpu_torch", "gpu_tensorflow", "gpu_nvidia_smi"]:
            info = results.get(key, {})
            err = info.get("error")
            if err:
                lines.append(f"- {info.get('backend', info.get('tool', key))}: {err}")
        lines.append("")

    # CPU section
    lines.append("## CPU")
    lines.append("")
    cpu = results.get("cpu", {})
    for k, v in cpu.items():
        lines.append(f"- **{k}**: {v}")
    lines.append("")

    # RAM section
    lines.append("## RAM")
    lines.append("")
    ram = results.get("ram", {})
    status = ram.get("status", "?")
    lines.append(f"- **Statut**: {status}")
    for k, v in ram.items():
        if k != "status":
            lines.append(f"- **{k}**: {v}")
    lines.append("")

    # Disk section
    lines.append("## Disque")
    lines.append("")
    disk = results.get("disk", {})
    status = disk.get("status", "?")
    lines.append(f"- **Statut**: {status}")
    for k, v in disk.items():
        if k != "status":
            lines.append(f"- **{k}**: {v}")
    lines.append("")

    # Network section
    lines.append("## Reseau")
    lines.append("")
    net = results.get("network", {})
    for k, v in net.items():
        lines.append(f"- **{k}**: {v}")
    lines.append("")

    # Critical processes
    lines.append("## Processus critiques")
    lines.append("")
    procs = results.get("processes", {}).get("processes", {})
    if procs:
        lines.append("| Processus | Description | En cours |")
        lines.append("|-----------|-------------|----------|")
        for name, info in procs.items():
            running = "Oui" if info.get("running") else "Non"
            lines.append(f"| {name} | {info.get('description', '')} | {running} |")
    else:
        pnote = results.get("processes", {}).get("note", "Aucune info")
        lines.append(f"*{pnote}*")
    lines.append("")

    # ML readiness summary
    lines.append("## Pret pour ML ?")
    lines.append("")
    ml_ready = True
    issues_list: list[str] = []

    if not gpu_available:
        issues_list.append("Pas de GPU (entrainement CPU uniquement, plus lent)")
    ram_total = ram.get("total_mb", 0)
    if ram_total < 8000:
        ml_ready = False
        issues_list.append(f"RAM insuffisante ({ram_total} MB, recommande >= 8 GB)")
    disk_free = disk.get("free_gb", 0)
    if disk_free < 10:
        ml_ready = False
        issues_list.append(f"Espace disque faible ({disk_free} GB libre, recommande >= 10 GB)")

    if ml_ready and not issues_list:
        lines.append("**Systeme pret pour l'entrainement ML.**")
    elif ml_ready:
        lines.append("**Systeme utilisable avec limitations:**")
        for issue in issues_list:
            lines.append(f"- {issue}")
    else:
        lines.append("**Systeme NON pret pour l'entrainement ML:**")
        for issue in issues_list:
            lines.append(f"- {issue}")
    lines.append("")

    lines.append("---")
    lines.append("*Genere par scripts/gpu_monitor.py (Pilier 23)*")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report written to %s", REPORT_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """Run system health monitoring."""
    t0 = time.time()
    logger.info("=== Pilier 23 : GPU/Monitoring/Haute Dispo ===")

    results: dict[str, Any] = {}

    logger.info("Checking GPU (torch)...")
    results["gpu_torch"] = check_gpu_torch()

    logger.info("Checking GPU (tensorflow)...")
    results["gpu_tensorflow"] = check_gpu_tensorflow()

    logger.info("Checking GPU (nvidia-smi)...")
    results["gpu_nvidia_smi"] = check_nvidia_smi()

    logger.info("Checking CPU...")
    results["cpu"] = check_cpu()

    logger.info("Checking RAM...")
    results["ram"] = check_ram()

    logger.info("Checking disk...")
    results["disk"] = check_disk()

    logger.info("Checking network...")
    results["network"] = check_network()

    logger.info("Checking critical processes...")
    results["processes"] = check_critical_processes()

    elapsed = time.time() - t0
    _generate_report(results, elapsed)

    # Summary
    gpu_avail = any(
        results.get(k, {}).get("available", False)
        for k in ["gpu_torch", "gpu_tensorflow", "gpu_nvidia_smi"]
    )
    logger.info(
        "Health check complete: GPU=%s, RAM=%s, Disk=%s (%.1fs)",
        "YES" if gpu_avail else "NO",
        results.get("ram", {}).get("status", "?"),
        results.get("disk", {}).get("status", "?"),
        elapsed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
