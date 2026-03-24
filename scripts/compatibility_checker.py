#!/usr/bin/env python3
"""
compatibility_checker.py -- Pilier 9 : Compatibilite Systeme.

Verifie que l'environnement d'execution est compatible avec le pipeline :
  1. Version Python >= 3.10
  2. Packages requis installes (requirements.txt)
  3. Compatibilite OS (Windows paths, encoding UTF-8)
  4. Espace disque disponible
  5. RAM disponible

Genere un rapport dans quality/compatibility_report.md.

Usage :
    python scripts/compatibility_checker.py
"""

from __future__ import annotations

import importlib
import locale
import os
import platform
import shutil
import struct
import subprocess
import sys
from datetime import datetime, timezone
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
MIN_PYTHON = (3, 10)
MIN_DISK_GB = 10
MIN_RAM_GB = 4
REQUIREMENTS_FILE = PROJECT_ROOT / "requirements.txt"

# Package name -> importable module name mapping (where they differ)
PACKAGE_IMPORT_MAP: dict[str, str] = {
    "beautifulsoup4": "bs4",
    "pyyaml": "yaml",
    "scikit-learn": "sklearn",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class CheckResult:
    """Stores a single check outcome."""

    __slots__ = ("name", "passed", "detail")

    def __init__(self, name: str, passed: bool, detail: str = "") -> None:
        self.name = name
        self.passed = passed
        self.detail = detail

    @property
    def icon(self) -> str:
        return "PASS" if self.passed else "FAIL"


def _get_ram_mb() -> int | None:
    """Return total physical RAM in MB, or None if unavailable."""
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
    return None


def _get_available_ram_mb() -> int | None:
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
    return None


def _parse_requirements(path: Path) -> list[str]:
    """Parse requirements.txt and return list of package names."""
    packages: list[str] = []
    if not path.exists():
        return packages
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        # Strip version specifiers
        for sep in (">=", "<=", "==", "!=", "~=", ">", "<"):
            if sep in line:
                line = line[: line.index(sep)]
                break
        # Strip extras like package[extra]
        if "[" in line:
            line = line[: line.index("[")]
        pkg = line.strip()
        if pkg:
            packages.append(pkg)
    return packages


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------
def check_python_version() -> CheckResult:
    """Check Python >= 3.10."""
    current = sys.version_info[:2]
    ok = current >= MIN_PYTHON
    detail = (
        f"Python {current[0]}.{current[1]} "
        f"({'OK' if ok else f'need >= {MIN_PYTHON[0]}.{MIN_PYTHON[1]}'})"
    )
    return CheckResult("Python version", ok, detail)


def check_python_bitness() -> CheckResult:
    """Check 64-bit Python (needed for large datasets)."""
    bits = struct.calcsize("P") * 8
    ok = bits == 64
    detail = f"{bits}-bit Python ({'OK' if ok else 'recommend 64-bit'})"
    return CheckResult("Python 64-bit", ok, detail)


def check_packages() -> list[CheckResult]:
    """Check all packages from requirements.txt are importable."""
    results: list[CheckResult] = []
    packages = _parse_requirements(REQUIREMENTS_FILE)
    if not packages:
        results.append(
            CheckResult("requirements.txt", False, "File not found or empty")
        )
        return results

    for pkg in packages:
        module_name = PACKAGE_IMPORT_MAP.get(pkg, pkg.replace("-", "_"))
        try:
            importlib.import_module(module_name)
            results.append(CheckResult(f"Package: {pkg}", True, "installed"))
        except ImportError:
            results.append(CheckResult(f"Package: {pkg}", False, "NOT installed"))
    return results


def check_os_compatibility() -> list[CheckResult]:
    """Check OS-level compatibility."""
    results: list[CheckResult] = []

    # OS detection
    os_name = platform.system()
    results.append(
        CheckResult("OS", True, f"{os_name} {platform.release()}")
    )

    # Windows long path support
    if os_name == "Windows":
        try:
            import winreg

            key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"SYSTEM\CurrentControlSet\Control\FileSystem",
            )
            value, _ = winreg.QueryValueEx(key, "LongPathsEnabled")
            winreg.CloseKey(key)
            ok = value == 1
            results.append(
                CheckResult(
                    "Windows long paths",
                    ok,
                    "enabled" if ok else "disabled (recommend enabling)",
                )
            )
        except Exception:
            results.append(
                CheckResult("Windows long paths", False, "could not read registry")
            )

    # Encoding
    preferred = locale.getpreferredencoding(False)
    stdout_enc = sys.stdout.encoding or "unknown"
    ok = "utf" in preferred.lower() or "utf" in stdout_enc.lower()
    detail = f"preferred={preferred}, stdout={stdout_enc}"
    results.append(CheckResult("UTF-8 encoding", ok, detail))

    # Filesystem encoding
    fs_enc = sys.getfilesystemencoding()
    ok_fs = "utf" in fs_enc.lower()
    results.append(
        CheckResult("Filesystem encoding", ok_fs, f"{fs_enc}")
    )

    return results


def check_disk_space() -> CheckResult:
    """Check available disk space on the project drive."""
    try:
        usage = shutil.disk_usage(PROJECT_ROOT)
        free_gb = usage.free / (1024**3)
        total_gb = usage.total / (1024**3)
        ok = free_gb >= MIN_DISK_GB
        detail = f"{free_gb:.1f} GB free / {total_gb:.1f} GB total"
        if not ok:
            detail += f" (need >= {MIN_DISK_GB} GB)"
        return CheckResult("Disk space", ok, detail)
    except Exception as exc:
        return CheckResult("Disk space", False, f"error: {exc}")


def check_ram() -> CheckResult:
    """Check total and available RAM."""
    total = _get_ram_mb()
    avail = _get_available_ram_mb()
    if total is None:
        return CheckResult("RAM", False, "could not determine RAM")

    total_gb = total / 1024
    ok = total_gb >= MIN_RAM_GB
    detail = f"Total: {total_gb:.1f} GB"
    if avail is not None:
        detail += f", Available: {avail / 1024:.1f} GB"
    if not ok:
        detail += f" (need >= {MIN_RAM_GB} GB)"
    return CheckResult("RAM", ok, detail)


def check_pip_version() -> CheckResult:
    """Check pip is available and reasonably recent."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "--version"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            version_str = result.stdout.strip().split()[1]
            major = int(version_str.split(".")[0])
            ok = major >= 21
            return CheckResult(
                "pip", ok, f"v{version_str} ({'OK' if ok else 'consider upgrading'})"
            )
        return CheckResult("pip", False, "pip returned error")
    except Exception as exc:
        return CheckResult("pip", False, f"error: {exc}")


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def generate_report(results: list[CheckResult]) -> str:
    """Generate markdown report content."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    lines: list[str] = [
        "# Compatibility Report (Pilier 9)",
        "",
        f"Generated: {now}",
        "",
        f"**Results: {passed}/{total} passed, {failed} failed**",
        "",
        "## System Info",
        "",
        f"| Property | Value |",
        f"|----------|-------|",
        f"| Platform | {platform.platform()} |",
        f"| Python | {sys.version} |",
        f"| Architecture | {platform.machine()} |",
        f"| Executable | {sys.executable} |",
        "",
        "## Check Results",
        "",
        "| Status | Check | Detail |",
        "|--------|-------|--------|",
    ]

    for r in results:
        lines.append(f"| {r.icon} | {r.name} | {r.detail} |")

    lines.append("")

    # Summary
    if failed > 0:
        lines.append("## Failures")
        lines.append("")
        for r in results:
            if not r.passed:
                lines.append(f"- **{r.name}**: {r.detail}")
        lines.append("")

    lines.append("---")
    lines.append(f"*Report generated by compatibility_checker.py (Pilier 9)*")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """Run all compatibility checks and output report."""
    print("=== Pilier 9 : Compatibility Checker ===\n")

    results: list[CheckResult] = []

    # Python version and bitness
    results.append(check_python_version())
    results.append(check_python_bitness())

    # pip
    results.append(check_pip_version())

    # Packages
    print("Checking packages...")
    results.extend(check_packages())

    # OS compatibility
    print("Checking OS compatibility...")
    results.extend(check_os_compatibility())

    # Disk space
    results.append(check_disk_space())

    # RAM
    results.append(check_ram())

    # Print summary to console
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    print(f"\nResults: {passed} passed, {failed} failed\n")

    for r in results:
        tag = "  OK " if r.passed else "FAIL "
        print(f"  [{tag}] {r.name}: {r.detail}")

    # Write report
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    report_path = QUALITY_DIR / "compatibility_report.md"
    report_content = generate_report(results)
    report_path.write_text(report_content, encoding="utf-8")
    print(f"\nReport written to {report_path}")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
