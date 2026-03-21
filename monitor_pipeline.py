#!/usr/bin/env python3
"""
monitor_pipeline.py
===================
Moniteur en temps reel du pipeline turf-data.

A executer dans un terminal separe pendant que run_pipeline.py tourne.
Rafraichit toutes les 5 secondes avec :
  - Barre de progression ASCII
  - Etape en cours, ETA, succes/echec
  - Utilisation RAM / CPU
  - Resume des etapes completees / en echec

Usage :
    python monitor_pipeline.py
    python monitor_pipeline.py --interval 3
    python monitor_pipeline.py --once
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
CHECKPOINT_FILE = BASE_DIR / "pipeline_checkpoint.json"
LOG_FILE = BASE_DIR / "pipeline.log"

# Total steps in the pipeline (mirrors run_pipeline.py DAG)
# Updated dynamically from checkpoint data if available
TOTAL_STEPS_FALLBACK = 70

# Phase labels
PHASE_NAMES = {
    1: "Audit",
    2: "Nettoyage",
    3: "Deduplication",
    4: "Comblage",
    5: "Merges",
    6: "Mega merge",
    7: "Features",
    8: "Master features",
    9: "Quality",
}


# ---------------------------------------------------------------------------
# System metrics
# ---------------------------------------------------------------------------

def get_ram_usage() -> Tuple[float, float, float]:
    """Return (used_gb, total_gb, percent) or (-1, -1, -1) if unavailable."""
    try:
        import psutil
        mem = psutil.virtual_memory()
        return (
            mem.used / (1024 ** 3),
            mem.total / (1024 ** 3),
            mem.percent,
        )
    except ImportError:
        pass

    # Fallback: parse /proc/meminfo on Linux
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            info = {}
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    info[parts[0].rstrip(":")] = int(parts[1])
            total = info.get("MemTotal", 0) / (1024 ** 2)
            available = info.get("MemAvailable", info.get("MemFree", 0)) / (1024 ** 2)
            used = total - available
            pct = (used / total * 100) if total > 0 else 0
            return (used, total, pct)
    except (FileNotFoundError, KeyError):
        pass

    return (-1, -1, -1)


def get_cpu_usage() -> float:
    """Return CPU usage percent or -1 if unavailable."""
    try:
        import psutil
        return psutil.cpu_percent(interval=0.5)
    except ImportError:
        pass

    # Fallback: read /proc/stat on Linux (rough estimate)
    try:
        def read_stat():
            with open("/proc/stat", "r", encoding="utf-8") as f:
                line = f.readline()
            vals = list(map(int, line.split()[1:]))
            idle = vals[3]
            total = sum(vals)
            return idle, total

        idle1, total1 = read_stat()
        time.sleep(0.3)
        idle2, total2 = read_stat()
        d_idle = idle2 - idle1
        d_total = total2 - total1
        if d_total > 0:
            return round((1 - d_idle / d_total) * 100, 1)
    except (FileNotFoundError, IndexError):
        pass

    return -1


# ---------------------------------------------------------------------------
# Checkpoint reading
# ---------------------------------------------------------------------------

def load_checkpoint() -> Dict:
    """Load the pipeline checkpoint file."""
    default = {
        "completed": [],
        "failed": {},
        "timings": {},
        "started_at": None,
        "finished_at": None,
    }
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for k, v in default.items():
                data.setdefault(k, v)
            return data
        except (json.JSONDecodeError, IOError):
            pass
    return default


# ---------------------------------------------------------------------------
# Log file parsing
# ---------------------------------------------------------------------------

def get_latest_log_lines(n: int = 20) -> List[str]:
    """Return the last n lines from pipeline.log."""
    if not LOG_FILE.exists():
        return []
    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return [l.rstrip() for l in lines[-n:]]
    except IOError:
        return []


def detect_running_step(log_lines: List[str]) -> Optional[str]:
    """Try to detect which step is currently running from recent log lines."""
    for line in reversed(log_lines):
        if "] Demarrage -> " in line:
            # Extract step name from "[step_name] Demarrage -> script.py"
            try:
                bracket_start = line.index("[") + 1
                bracket_end = line.index("]", bracket_start)
                return line[bracket_start:bracket_end]
            except ValueError:
                pass
        if "Vague " in line and "RUN" in line:
            return line.split("RUN")[-1].strip().rstrip(")")
    return None


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def progress_bar(current: int, total: int, width: int = 40) -> str:
    """Return an ASCII progress bar string."""
    if total <= 0:
        return f"[{'?' * width}] ?/??"
    pct = current / total
    filled = int(width * pct)
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {current}/{total} ({pct * 100:.1f}%)"


def format_duration(seconds: float) -> str:
    """Format seconds into human readable duration."""
    if seconds < 0:
        return "N/A"
    td = timedelta(seconds=int(seconds))
    hours, remainder = divmod(int(td.total_seconds()), 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    elif minutes > 0:
        return f"{minutes}m{secs:02d}s"
    else:
        return f"{secs}s"


def clear_screen():
    """Clear terminal screen."""
    if platform.system() == "Windows":
        os.system("cls")
    else:
        print("\033[2J\033[H", end="")


# ---------------------------------------------------------------------------
# Main display
# ---------------------------------------------------------------------------

def render_dashboard(ckpt: Dict, total_steps: int):
    """Render one frame of the monitoring dashboard."""
    clear_screen()

    completed = ckpt.get("completed", [])
    failed = ckpt.get("failed", {})
    timings = ckpt.get("timings", {})
    started_at = ckpt.get("started_at")
    finished_at = ckpt.get("finished_at")

    n_completed = len(completed)
    n_failed = len(failed)
    n_done = n_completed + n_failed

    now = datetime.now()

    # Header
    print("=" * 72)
    print("   TURF-DATA PIPELINE MONITOR")
    print(f"   {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)

    # Progress bar
    print()
    print(f"  Progression : {progress_bar(n_completed, total_steps, 45)}")
    print()

    # Status
    if finished_at:
        print(f"  Statut      : TERMINE")
    elif started_at and n_done < total_steps:
        print(f"  Statut      : EN COURS")
    elif not started_at:
        print(f"  Statut      : PAS DEMARRE")
    else:
        print(f"  Statut      : EN COURS")

    # Counts
    print(f"  Completees  : {n_completed}")
    print(f"  En echec    : {n_failed}")
    pending = total_steps - n_completed - n_failed
    if pending < 0:
        pending = 0
    print(f"  Restantes   : {pending}")

    # Timings
    if timings:
        total_time = sum(timings.values())
        print(f"  Temps exec  : {format_duration(total_time)}")

        # ETA calculation based on average step time
        if n_completed > 0 and pending > 0:
            avg_time = total_time / n_completed
            eta_seconds = avg_time * pending
            eta_finish = now + timedelta(seconds=eta_seconds)
            print(f"  ETA         : ~{format_duration(eta_seconds)} (vers {eta_finish.strftime('%H:%M:%S')})")
        elif pending == 0:
            print(f"  ETA         : Termine")

    if started_at:
        try:
            start_dt = datetime.fromisoformat(started_at)
            elapsed = (now - start_dt).total_seconds()
            print(f"  Elapsed     : {format_duration(elapsed)}")
        except (ValueError, TypeError):
            pass

    # System resources
    print()
    print("-" * 72)
    print("  RESSOURCES SYSTEME")
    print("-" * 72)

    ram_used, ram_total, ram_pct = get_ram_usage()
    cpu_pct = get_cpu_usage()

    if ram_used >= 0:
        ram_bar_w = 30
        ram_filled = int(ram_bar_w * ram_pct / 100)
        ram_bar = "#" * ram_filled + "-" * (ram_bar_w - ram_filled)
        print(f"  RAM : [{ram_bar}] {ram_used:.1f}/{ram_total:.1f} GB ({ram_pct:.0f}%)")
    else:
        print(f"  RAM : (installez psutil pour le suivi memoire)")

    if cpu_pct >= 0:
        cpu_bar_w = 30
        cpu_filled = int(cpu_bar_w * cpu_pct / 100)
        cpu_bar = "#" * cpu_filled + "-" * (cpu_bar_w - cpu_filled)
        print(f"  CPU : [{cpu_bar}] {cpu_pct:.0f}%")
    else:
        print(f"  CPU : (installez psutil pour le suivi CPU)")

    # Currently running step
    print()
    print("-" * 72)
    print("  ETAPE EN COURS")
    print("-" * 72)

    log_lines = get_latest_log_lines(50)
    running = detect_running_step(log_lines)
    if running:
        print(f"  -> {running}")
    elif finished_at:
        print(f"  (pipeline termine)")
    elif not started_at:
        print(f"  (pipeline non demarre)")
    else:
        print(f"  (detection impossible — verifiez pipeline.log)")

    # Failed steps
    if failed:
        print()
        print("-" * 72)
        print(f"  ETAPES EN ECHEC ({n_failed})")
        print("-" * 72)
        for name, err in sorted(failed.items()):
            short_err = err.splitlines()[-1][:60] if err else "?"
            print(f"  X {name}: {short_err}")

    # Top 5 slowest completed steps
    if timings:
        print()
        print("-" * 72)
        print("  TOP 5 ETAPES LES PLUS LENTES")
        print("-" * 72)
        sorted_t = sorted(timings.items(), key=lambda x: x[1], reverse=True)
        for name, dur in sorted_t[:5]:
            status = "OK" if name in completed else "FAIL"
            print(f"  {status:4s}  {name:40s}  {format_duration(dur)}")

    # Last log lines
    print()
    print("-" * 72)
    print("  DERNIERS LOGS")
    print("-" * 72)
    for line in log_lines[-8:]:
        # Truncate long lines
        if len(line) > 70:
            line = line[:67] + "..."
        print(f"  {line}")

    print()
    print("=" * 72)
    print("  Ctrl+C pour quitter | Rafraichissement automatique")
    print("=" * 72)


# ---------------------------------------------------------------------------
# Estimate total steps from checkpoint data
# ---------------------------------------------------------------------------

def estimate_total_steps(ckpt: Dict) -> int:
    """
    Try to figure out total step count.
    We look at all known step names (completed + failed + timings keys).
    If that gives us a reasonable number, use it. Otherwise use fallback.
    """
    known = set(ckpt.get("completed", []))
    known.update(ckpt.get("failed", {}).keys())
    known.update(ckpt.get("timings", {}).keys())

    if len(known) > 10:
        # Probably not all steps are known yet, but at minimum we know this many
        return max(len(known), TOTAL_STEPS_FALLBACK)

    # Try to import the DAG from run_pipeline to get the exact count
    try:
        sys.path.insert(0, str(BASE_DIR))
        from run_pipeline import build_dag
        dag = build_dag()
        return len(dag)
    except Exception as e:
        log.debug("Could not import build_dag to determine total steps: %s", e)

    return TOTAL_STEPS_FALLBACK


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Moniteur en temps reel du pipeline turf-data"
    )
    parser.add_argument(
        "--interval", type=int, default=5,
        help="Intervalle de rafraichissement en secondes (defaut: 5)"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Afficher une seule fois et quitter"
    )
    args = parser.parse_args()

    print("Demarrage du moniteur pipeline...")
    print(f"Checkpoint : {CHECKPOINT_FILE}")
    print(f"Log        : {LOG_FILE}")
    print()

    ckpt = load_checkpoint()
    total_steps = estimate_total_steps(ckpt)

    if args.once:
        render_dashboard(ckpt, total_steps)
        return

    try:
        while True:
            ckpt = load_checkpoint()
            total_steps = estimate_total_steps(ckpt)
            render_dashboard(ckpt, total_steps)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nMoniteur arrete.")


if __name__ == "__main__":
    main()
