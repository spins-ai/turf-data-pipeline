#!/usr/bin/env python3
"""
diagnostic.py — Pilier 14 : verifie tout le pipeline et liste les problemes.

Lance toutes les verifications de sante du pipeline en une seule commande :
  1. Fichiers master existent ?
  2. Dossiers output existent ?
  3. Fichiers de taille zero ?
  4. Import des modules utils ?
  5. py_compile d'un echantillon de 20 fichiers Python
  6. Fraicheur des fichiers master (< 7 jours)
  7. Nombre d'enregistrements (partants_master, features_matrix, labels)
  8. Espace disque (warn si < 50 Go)
  9. RAM disponible
 10. Statut Git (modifications non committees)

Exit code 0 = tout passe, 1 = au moins un FAIL.

Usage :
    python scripts/diagnostic.py
"""

from __future__ import annotations

import importlib
import os
import platform
import py_compile
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve project root (two levels up from scripts/diagnostic.py)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    BASE_DIR,
    COURSES_MASTER,
    DATA_MASTER_DIR,
    EQUIPEMENTS_MASTER,
    EXPORTS_DIR,
    FEATURES_DIR,
    FEATURES_MATRIX,
    LABELS_DIR,
    LOGS_DIR,
    OUTPUT_DIR,
    PARTANTS_MASTER,
    PARTANTS_MASTER_ENRICHI,
    QUALITY_DIR,
    TRAINING_LABELS,
)

# ---------------------------------------------------------------------------
# ANSI colors (with fallback for terminals that don't support them)
# ---------------------------------------------------------------------------
if os.environ.get("NO_COLOR") or not sys.stdout.isatty():
    GREEN = YELLOW = RED = CYAN = BOLD = RESET = ""
else:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
_pass = 0
_fail = 0
_warn = 0


def _print_result(status: str, message: str) -> None:
    global _pass, _fail, _warn
    if status == "PASS":
        tag = f"{GREEN}[PASS]{RESET}"
        _pass += 1
    elif status == "FAIL":
        tag = f"{RED}[FAIL]{RESET}"
        _fail += 1
    elif status == "WARN":
        tag = f"{YELLOW}[WARN]{RESET}"
        _warn += 1
    else:
        tag = f"[{status}]"
    print(f"  {tag} {message}")


def _section(title: str) -> None:
    print(f"\n{BOLD}{CYAN}=== {title} ==={RESET}")


# ===================================================================
# 1. FILE CHECKS — master files exist?
# ===================================================================
def check_master_files() -> None:
    _section("1. Master files")
    master_files = [
        ("partants_master.jsonl", PARTANTS_MASTER),
        ("partants_master_enrichi.jsonl", PARTANTS_MASTER_ENRICHI),
        ("courses_master.jsonl", COURSES_MASTER),
        ("equipements_master.json", EQUIPEMENTS_MASTER),
        ("features_matrix.jsonl", FEATURES_MATRIX),
        ("training_labels.jsonl", TRAINING_LABELS),
    ]
    for name, path in master_files:
        if path.exists():
            size = path.stat().st_size
            if size == 0:
                _print_result("FAIL", f"{name} exists but is EMPTY (0 bytes)")
            else:
                _print_result("PASS", f"{name} ({_human_size(size)})")
        else:
            _print_result("FAIL", f"{name} NOT FOUND at {path}")


# ===================================================================
# 2. OUTPUT DIRS — do they all exist?
# ===================================================================
def check_output_dirs() -> None:
    _section("2. Output directories")
    key_dirs = [
        ("output/", OUTPUT_DIR),
        ("data_master/", DATA_MASTER_DIR),
        ("output/features/", FEATURES_DIR),
        ("output/labels/", LABELS_DIR),
        ("output/exports/", EXPORTS_DIR),
        ("output/quality/", QUALITY_DIR),
        ("logs/", LOGS_DIR),
    ]
    for name, path in key_dirs:
        if path.is_dir():
            _print_result("PASS", f"{name} exists")
        else:
            _print_result("FAIL", f"{name} MISSING ({path})")


# ===================================================================
# 3. ZERO-BYTE FILES in output/
# ===================================================================
def check_zero_byte_files() -> None:
    _section("3. Zero-byte files in output/")
    if not OUTPUT_DIR.is_dir():
        _print_result("WARN", "output/ directory does not exist, skipping")
        return
    zero_files: list[Path] = []
    for f in OUTPUT_DIR.rglob("*"):
        if f.is_file() and f.stat().st_size == 0:
            zero_files.append(f)
    if zero_files:
        _print_result("WARN", f"{len(zero_files)} zero-byte file(s) found:")
        for zf in zero_files[:10]:
            print(f"         - {zf.relative_to(PROJECT_ROOT)}")
        if len(zero_files) > 10:
            print(f"         ... and {len(zero_files) - 10} more")
    else:
        _print_result("PASS", "No zero-byte files in output/")


# ===================================================================
# 4. IMPORT CHECKS — can we import all utils modules?
# ===================================================================
def check_imports() -> None:
    _section("4. Utils imports")
    utils_dir = PROJECT_ROOT / "utils"
    if not utils_dir.is_dir():
        _print_result("FAIL", "utils/ directory not found")
        return
    modules = sorted(
        f.stem for f in utils_dir.glob("*.py")
        if f.stem != "__init__" and not f.stem.startswith("_")
    )
    for mod_name in modules:
        full_name = f"utils.{mod_name}"
        try:
            importlib.import_module(full_name)
            _print_result("PASS", f"import {full_name}")
        except Exception as exc:
            _print_result("FAIL", f"import {full_name} -> {type(exc).__name__}: {exc}")


# ===================================================================
# 5. COMPILE CHECK — py_compile 20 random Python files
# ===================================================================
def check_compile() -> None:
    _section("5. py_compile (random sample of 20 files)")
    all_py = [
        f for f in PROJECT_ROOT.rglob("*.py")
        if "__pycache__" not in f.parts
        and ".git" not in f.parts
        and "node_modules" not in f.parts
    ]
    if not all_py:
        _print_result("WARN", "No Python files found")
        return
    sample_size = min(20, len(all_py))
    sample = random.sample(all_py, sample_size)
    errors = 0
    for f in sample:
        try:
            py_compile.compile(str(f), doraise=True)
            _print_result("PASS", f"{f.relative_to(PROJECT_ROOT)}")
        except py_compile.PyCompileError as exc:
            errors += 1
            _print_result("FAIL", f"{f.relative_to(PROJECT_ROOT)} -> {exc}")
    if errors == 0:
        print(f"  {GREEN}All {sample_size} files compiled OK{RESET}")


# ===================================================================
# 6. DATA FRESHNESS — master files less than 7 days old?
# ===================================================================
def check_data_freshness() -> None:
    _section("6. Data freshness (master files < 7 days)")
    files_to_check = [
        ("partants_master.jsonl", PARTANTS_MASTER),
        ("courses_master.jsonl", COURSES_MASTER),
    ]
    now = datetime.now(timezone.utc)
    for name, path in files_to_check:
        if not path.exists():
            _print_result("FAIL", f"{name} does not exist")
            continue
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        age_days = (now - mtime).days
        if age_days <= 7:
            _print_result("PASS", f"{name} modified {age_days} day(s) ago")
        else:
            _print_result("WARN", f"{name} is {age_days} days old (> 7 days)")


# ===================================================================
# 7. RECORD COUNTS — partants_master, features_matrix, labels > 0?
# ===================================================================
def check_record_counts() -> None:
    _section("7. Record counts")
    files_to_count = [
        ("partants_master.jsonl", PARTANTS_MASTER),
        ("features_matrix.jsonl", FEATURES_MATRIX),
        ("training_labels.jsonl", TRAINING_LABELS),
    ]
    for name, path in files_to_count:
        if not path.exists():
            _print_result("FAIL", f"{name} not found")
            continue
        try:
            count = _count_lines(path)
            if count > 0:
                _print_result("PASS", f"{name}: {count:,} records")
            else:
                _print_result("FAIL", f"{name}: 0 records")
        except Exception as exc:
            _print_result("FAIL", f"{name}: error counting -> {exc}")


def _count_lines(path: Path) -> int:
    """Count non-empty lines in a file (fast, no JSON parsing)."""
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


# ===================================================================
# 8. DISK SPACE — warn if < 50 GB free
# ===================================================================
def check_disk_space() -> None:
    _section("8. Disk space")
    try:
        usage = shutil.disk_usage(str(PROJECT_ROOT))
        free_gb = usage.free / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        used_pct = (usage.used / usage.total) * 100
        if free_gb >= 50:
            _print_result(
                "PASS",
                f"{free_gb:.1f} GB free / {total_gb:.1f} GB total ({used_pct:.0f}% used)",
            )
        else:
            _print_result(
                "WARN",
                f"Only {free_gb:.1f} GB free / {total_gb:.1f} GB total ({used_pct:.0f}% used)",
            )
    except Exception as exc:
        _print_result("FAIL", f"Could not check disk space: {exc}")


# ===================================================================
# 9. RAM CHECK — how much RAM available?
# ===================================================================
def check_ram() -> None:
    _section("9. RAM")
    try:
        import psutil  # type: ignore[import-untyped]

        mem = psutil.virtual_memory()
        total_gb = mem.total / (1024 ** 3)
        avail_gb = mem.available / (1024 ** 3)
        used_pct = mem.percent
        if avail_gb >= 4:
            _print_result(
                "PASS",
                f"{avail_gb:.1f} GB available / {total_gb:.1f} GB total ({used_pct:.0f}% used)",
            )
        elif avail_gb >= 2:
            _print_result(
                "WARN",
                f"{avail_gb:.1f} GB available / {total_gb:.1f} GB total ({used_pct:.0f}% used)",
            )
        else:
            _print_result(
                "FAIL",
                f"Only {avail_gb:.1f} GB available / {total_gb:.1f} GB total ({used_pct:.0f}% used)",
            )
    except ImportError:
        # Fallback without psutil on Windows
        if platform.system() == "Windows":
            try:
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

                stat = MEMORYSTATUSEX()
                stat.dwLength = ctypes.sizeof(stat)
                ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
                total_gb = stat.ullTotalPhys / (1024 ** 3)
                avail_gb = stat.ullAvailPhys / (1024 ** 3)
                used_pct = stat.dwMemoryLoad
                if avail_gb >= 4:
                    _print_result(
                        "PASS",
                        f"{avail_gb:.1f} GB available / {total_gb:.1f} GB total ({used_pct}% used)",
                    )
                elif avail_gb >= 2:
                    _print_result(
                        "WARN",
                        f"{avail_gb:.1f} GB available / {total_gb:.1f} GB total ({used_pct}% used)",
                    )
                else:
                    _print_result(
                        "FAIL",
                        f"Only {avail_gb:.1f} GB available / {total_gb:.1f} GB total ({used_pct}% used)",
                    )
            except Exception as exc:
                _print_result("WARN", f"Could not read RAM (no psutil, ctypes failed: {exc})")
        else:
            _print_result("WARN", "psutil not installed — cannot check RAM")


# ===================================================================
# 10. GIT STATUS — any uncommitted changes?
# ===================================================================
def check_git_status() -> None:
    _section("10. Git status")
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            _print_result("WARN", f"git status failed: {result.stderr.strip()}")
            return
        lines = [l for l in result.stdout.strip().splitlines() if l.strip()]
        if not lines:
            _print_result("PASS", "Working tree is clean")
        else:
            _print_result("WARN", f"{len(lines)} uncommitted change(s):")
            for line in lines[:10]:
                print(f"         {line}")
            if len(lines) > 10:
                print(f"         ... and {len(lines) - 10} more")

        # Also show current branch
        branch_result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=10,
        )
        if branch_result.returncode == 0:
            branch = branch_result.stdout.strip()
            print(f"  {CYAN}Branch: {branch}{RESET}")
    except FileNotFoundError:
        _print_result("WARN", "git not found in PATH")
    except Exception as exc:
        _print_result("WARN", f"Could not run git: {exc}")


# ===================================================================
# UTILS
# ===================================================================
def _human_size(nbytes: int) -> str:
    """Convert bytes to a human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024  # type: ignore[assignment]
    return f"{nbytes:.1f} PB"


# ===================================================================
# MAIN
# ===================================================================
def main() -> int:
    global _pass, _fail, _warn
    _pass = _fail = _warn = 0

    print(f"\n{BOLD}{CYAN}{'=' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  TURF DATA PIPELINE — Diagnostic Report{RESET}")
    print(f"{BOLD}{CYAN}  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{CYAN}  Project root: {PROJECT_ROOT}{RESET}")
    print(f"{BOLD}{CYAN}{'=' * 60}{RESET}")

    start = time.monotonic()

    check_master_files()
    check_output_dirs()
    check_zero_byte_files()
    check_imports()
    check_compile()
    check_data_freshness()
    check_record_counts()
    check_disk_space()
    check_ram()
    check_git_status()

    elapsed = time.monotonic() - start

    # Summary
    print(f"\n{BOLD}{CYAN}{'=' * 60}{RESET}")
    total = _pass + _fail + _warn
    print(f"  {BOLD}Summary:{RESET}  {GREEN}{_pass} PASS{RESET}  |  "
          f"{RED}{_fail} FAIL{RESET}  |  {YELLOW}{_warn} WARN{RESET}  "
          f"({total} checks, {elapsed:.1f}s)")

    if _fail == 0:
        print(f"  {GREEN}{BOLD}Pipeline health: OK{RESET}")
    else:
        print(f"  {RED}{BOLD}Pipeline health: ISSUES DETECTED{RESET}")
    print(f"{BOLD}{CYAN}{'=' * 60}{RESET}\n")

    return 1 if _fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
