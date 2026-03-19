#!/usr/bin/env python3
"""
pilier_performance_profiler.py -- Pilier Qualite : Profilage de performance
============================================================================

Mesure les performances des scripts du pipeline : temps d'execution,
pic de RAM, I/O disque. Fournit un decorateur @profile pour integration
facile dans les scripts existants.

Fonctionnalites :
  1. Mesure temps d'execution, RAM peak, I/O disque
  2. Stocke les resultats dans logs/performance_profile.json
  3. Decorateur @profile pour instrumenter les scripts
  4. Mode standalone : profiler un script en ligne de commande

Usage:
    # En tant que module (decorateur):
    from pilier_performance_profiler import profile
    @profile
    def mon_traitement():
        ...

    # En ligne de commande:
    python pilier_performance_profiler.py run some_script.py
    python pilier_performance_profiler.py report
    python pilier_performance_profiler.py compare
"""

import argparse
import functools
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "logs"
PROFILE_FILE = LOGS_DIR / "performance_profile.json"


# -----------------------------------------------------------------------
# Mesure memoire cross-platform
# -----------------------------------------------------------------------

def get_memory_mb() -> float:
    """Retourne l'usage memoire courant en MB."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    except ImportError:
        pass

    # Fallback Windows
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            ):
                return counters.WorkingSetSize / (1024 * 1024)
        except Exception:
            pass

    # Fallback Linux/Mac
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        # maxrss en KB sur Linux, en bytes sur Mac
        if sys.platform == "darwin":
            return usage.ru_maxrss / (1024 * 1024)
        else:
            return usage.ru_maxrss / 1024
    except (ImportError, AttributeError):
        pass

    return 0.0


def get_disk_io() -> dict:
    """Retourne les compteurs I/O disque."""
    try:
        import psutil
        counters = psutil.Process(os.getpid()).io_counters()
        return {
            "read_bytes": counters.read_bytes,
            "write_bytes": counters.write_bytes,
            "read_count": counters.read_count,
            "write_count": counters.write_count,
        }
    except (ImportError, AttributeError):
        pass

    # Fallback: pas de I/O disponible
    return {
        "read_bytes": 0,
        "write_bytes": 0,
        "read_count": 0,
        "write_count": 0,
    }


# -----------------------------------------------------------------------
# Profiler
# -----------------------------------------------------------------------

class PerformanceProfiler:
    """Profileur de performance pour les scripts du pipeline."""

    def __init__(self, profile_file: Path = PROFILE_FILE):
        self.profile_file = profile_file
        self.profile_file.parent.mkdir(parents=True, exist_ok=True)

    def load_history(self) -> list[dict]:
        """Charge l'historique de profilage."""
        if not self.profile_file.exists():
            return []
        try:
            with open(self.profile_file, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            return []
        except (json.JSONDecodeError, IOError):
            return []

    def save_result(self, result: dict):
        """Ajoute un resultat de profilage."""
        history = self.load_history()
        history.append(result)

        # Garder les 500 derniers
        if len(history) > 500:
            history = history[-500:]

        with open(self.profile_file, "w", encoding="utf-8", errors="replace") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)

    def profile_function(self, func_name: str, func, *args, **kwargs):
        """Profile l'execution d'une fonction."""
        mem_before = get_memory_mb()
        io_before = get_disk_io()
        t0 = time.time()
        mem_peak = mem_before

        error_msg = ""
        status = "success"
        result_value = None

        try:
            result_value = func(*args, **kwargs)
        except Exception as e:
            status = "error"
            error_msg = str(e)
            traceback.print_exc()
        finally:
            duration = time.time() - t0
            mem_after = get_memory_mb()
            io_after = get_disk_io()

            mem_peak = max(mem_before, mem_after)

            profile_result = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "script": func_name,
                "status": status,
                "error": error_msg,
                "duration_s": round(duration, 3),
                "memory_mb": {
                    "before": round(mem_before, 1),
                    "after": round(mem_after, 1),
                    "peak_estimate": round(mem_peak, 1),
                    "delta": round(mem_after - mem_before, 1),
                },
                "disk_io": {
                    "read_mb": round(
                        (io_after["read_bytes"] - io_before["read_bytes"]) / (1024 * 1024), 2
                    ),
                    "write_mb": round(
                        (io_after["write_bytes"] - io_before["write_bytes"]) / (1024 * 1024), 2
                    ),
                    "read_ops": io_after["read_count"] - io_before["read_count"],
                    "write_ops": io_after["write_count"] - io_before["write_count"],
                },
            }

            self.save_result(profile_result)

            print(f"  [PROFILE] {func_name}")
            print(f"    Duree:    {duration:.2f}s")
            print(f"    RAM:      {mem_after:.1f} MB (delta: {mem_after - mem_before:+.1f} MB)")
            print(f"    I/O read: {profile_result['disk_io']['read_mb']:.1f} MB")
            print(f"    I/O write:{profile_result['disk_io']['write_mb']:.1f} MB")
            print(f"    Status:   {status}")

        return result_value

    def profile_script(self, script_path: str) -> dict:
        """Profile un script Python via subprocess."""
        script = Path(script_path)
        if not script.exists():
            script = BASE_DIR / script_path
        if not script.exists():
            print(f"ERREUR: Script introuvable: {script_path}")
            return {"error": "not_found"}

        t0 = time.time()
        status = "success"
        error_msg = ""

        try:
            proc = subprocess.run(
                [sys.executable, str(script)],
                capture_output=True,
                text=True,
                timeout=3600,
                cwd=str(BASE_DIR),
            )
            if proc.returncode != 0:
                status = "error"
                error_msg = proc.stderr[:500] if proc.stderr else f"exit code {proc.returncode}"
        except subprocess.TimeoutExpired:
            status = "timeout"
            error_msg = "Script timeout (3600s)"
        except Exception as e:
            status = "error"
            error_msg = str(e)

        duration = time.time() - t0

        profile_result = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "script": script.name,
            "mode": "subprocess",
            "status": status,
            "error": error_msg,
            "duration_s": round(duration, 3),
        }

        self.save_result(profile_result)
        return profile_result

    def get_report(self) -> dict:
        """Genere un rapport de performance."""
        history = self.load_history()
        if not history:
            return {"status": "empty", "message": "Aucun profil enregistre"}

        # Grouper par script
        by_script = {}
        for entry in history:
            name = entry.get("script", "unknown")
            if name not in by_script:
                by_script[name] = []
            by_script[name].append(entry)

        report = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_runs": len(history),
            "scripts": {},
        }

        for script_name, runs in sorted(by_script.items()):
            durations = [r["duration_s"] for r in runs if "duration_s" in r]
            successes = sum(1 for r in runs if r.get("status") == "success")
            errors = sum(1 for r in runs if r.get("status") == "error")

            mem_peaks = []
            for r in runs:
                mem = r.get("memory_mb", {})
                if isinstance(mem, dict) and "peak_estimate" in mem:
                    mem_peaks.append(mem["peak_estimate"])

            script_report = {
                "n_runs": len(runs),
                "successes": successes,
                "errors": errors,
                "last_run": runs[-1].get("timestamp", ""),
                "duration": {
                    "avg_s": round(sum(durations) / len(durations), 2) if durations else 0,
                    "min_s": round(min(durations), 2) if durations else 0,
                    "max_s": round(max(durations), 2) if durations else 0,
                },
            }

            if mem_peaks:
                script_report["memory_peak_mb"] = {
                    "avg": round(sum(mem_peaks) / len(mem_peaks), 1),
                    "max": round(max(mem_peaks), 1),
                }

            report["scripts"][script_name] = script_report

        return report


# -----------------------------------------------------------------------
# Decorateur @profile
# -----------------------------------------------------------------------

_global_profiler = None


def _get_profiler() -> PerformanceProfiler:
    global _global_profiler
    if _global_profiler is None:
        _global_profiler = PerformanceProfiler()
    return _global_profiler


def profile(func=None, *, name=None):
    """
    Decorateur pour profiler une fonction.

    Usage:
        @profile
        def ma_fonction():
            ...

        @profile(name="etape_custom")
        def autre_fonction():
            ...
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            func_name = name or f"{fn.__module__}.{fn.__name__}"
            profiler = _get_profiler()
            return profiler.profile_function(func_name, fn, *args, **kwargs)
        return wrapper

    if func is not None:
        # @profile sans parentheses
        return decorator(func)
    # @profile(name="...")
    return decorator


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------

def cmd_run(args):
    """Profiler un script."""
    profiler = PerformanceProfiler()
    print(f"Profilage de: {args.script}")
    print("-" * 60)
    result = profiler.profile_script(args.script)
    print("-" * 60)
    print(f"Duree: {result.get('duration_s', 0):.2f}s")
    print(f"Status: {result.get('status', 'unknown')}")
    if result.get("error"):
        print(f"Erreur: {result['error'][:200]}")


def cmd_report(args):
    """Afficher le rapport de performance."""
    profiler = PerformanceProfiler()
    report = profiler.get_report()

    print("=" * 60)
    print("RAPPORT DE PERFORMANCE")
    print("=" * 60)

    if report.get("status") == "empty":
        print("Aucun profil enregistre.")
        return

    print(f"Total runs: {report['total_runs']}")
    print("-" * 60)

    # Trier par duree moyenne decroissante
    scripts = sorted(
        report["scripts"].items(),
        key=lambda x: x[1]["duration"]["avg_s"],
        reverse=True,
    )

    fmt = "{:<35} {:>8} {:>8} {:>8} {:>6}"
    print(fmt.format("Script", "Avg(s)", "Max(s)", "RAM(MB)", "Runs"))
    print("-" * 70)

    for name, info in scripts:
        mem = info.get("memory_peak_mb", {}).get("max", 0)
        print(fmt.format(
            name[:35],
            f"{info['duration']['avg_s']:.1f}",
            f"{info['duration']['max_s']:.1f}",
            f"{mem:.0f}" if mem else "-",
            str(info["n_runs"]),
        ))

    print("=" * 60)

    # Sauvegarder le rapport
    report_path = LOGS_DIR / "performance_report.json"
    with open(report_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"Rapport sauvegarde: {report_path}")


def cmd_compare(args):
    """Comparer les dernieres performances."""
    profiler = PerformanceProfiler()
    history = profiler.load_history()

    if len(history) < 2:
        print("Pas assez de donnees pour comparer.")
        return

    print("=" * 60)
    print("COMPARAISON DES PERFORMANCES")
    print("=" * 60)

    # Grouper par script, comparer derniere vs avant-derniere
    by_script = {}
    for entry in history:
        name = entry.get("script", "unknown")
        if name not in by_script:
            by_script[name] = []
        by_script[name].append(entry)

    fmt = "{:<35} {:>10} {:>10} {:>10}"
    print(fmt.format("Script", "Avant(s)", "Apres(s)", "Delta"))
    print("-" * 70)

    for name, runs in sorted(by_script.items()):
        if len(runs) < 2:
            continue
        prev = runs[-2].get("duration_s", 0)
        curr = runs[-1].get("duration_s", 0)
        delta = curr - prev
        sign = "+" if delta > 0 else ""
        print(fmt.format(
            name[:35],
            f"{prev:.2f}",
            f"{curr:.2f}",
            f"{sign}{delta:.2f}",
        ))

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Profilage de performance des scripts")
    subparsers = parser.add_subparsers(dest="command")

    # run
    run_parser = subparsers.add_parser("run", help="Profiler un script")
    run_parser.add_argument("script", help="Chemin du script a profiler")

    # report
    subparsers.add_parser("report", help="Afficher le rapport")

    # compare
    subparsers.add_parser("compare", help="Comparer les performances")

    args = parser.parse_args()

    if args.command == "run":
        cmd_run(args)
    elif args.command == "report":
        cmd_report(args)
    elif args.command == "compare":
        cmd_compare(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
