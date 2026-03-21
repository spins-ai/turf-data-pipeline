#!/usr/bin/env python3
"""
setup.py
========
Script d'installation et de verification du projet turf-data-pipeline.

Verifie :
  - Version Python >= 3.10
  - Installation des dependances (requirements.txt)
  - Creation de l'arborescence output/ et logs/
  - Existence de tous les scripts du pipeline
  - Informations systeme (RAM, CPU, espace disque)

Usage :
    python setup.py
    python setup.py --install
    python setup.py --check-only
    python setup.py --verbose
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
REQUIREMENTS_FILE = BASE_DIR / "requirements.txt"
MIN_PYTHON = (3, 10)

# Output directory structure to create
OUTPUT_DIRS = [
    "output/00_meteo",
    "output/01_calendrier",
    "output/02_liste_courses",
    "output/04_resultats",
    "output/05_historique_chevaux",
    "output/06_historique_jockeys",
    "output/07_cotes_marche",
    "output/08_pedigree",
    "output/09_equipements",
    "output/10_poids_handicaps",
    "output/11_sectionals",
    "output/12_pedigree",
    "output/13_meteo_historique",
    "output/14_pedigree",
    "output/15_external_datasets",
    "output/16_nanaelie",
    "output/17_sire",
    "output/18_letrot_records",
    "output/19_boturfers",
    "output/20_ifce",
    "output/21_rapports_definitifs",
    "output/22_performances_detaillees",
    "output/23_pronostics_equidia",
    "output/24_canalturf",
    "output/25_turfostats",
    "output/26_geny",
    "output/27_citations_enjeux",
    "output/28_combinaisons_marche",
    "output/29_arqana",
    "output/30_smarkets",
    "output/31_zone_turf",
    "output/32_turfomania",
    "output/33_turf_fr",
    "output/34_unibet",
    "output/35_meteo_france",
    "output/36_pedigree_query",
    "output/37_racing_post",
    "output/38_rapports_internet",
    "output/39_reunions_enrichies",
    "output/40_enrichissement_partants",
    "output/masters",
    "output/features",
    "output/labels",
    "output/quality",
    "output/models",
    "logs",
]

# Key scripts that must exist
KEY_SCRIPTS = [
    "run_pipeline.py",
    "monitor_pipeline.py",
    "generate_labels.py",
    "nettoyage_global.py",
    "deduplication.py",
    "comblage_trous.py",
    "mega_merge_partants_master.py",
    "master_feature_builder.py",
    "audit_data_integrity.py",
    "04_resultats.py",
    "labels/label_builder.py",
]


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

class Colors:
    """ANSI color codes (disabled on Windows without VT support)."""
    ENABLED = (os.name != "nt") or ("WT_SESSION" in os.environ) or os.environ.get("TERM")

    @staticmethod
    def _wrap(code: str, text: str) -> str:
        if Colors.ENABLED:
            return f"\033[{code}m{text}\033[0m"
        return text

    @staticmethod
    def green(text: str) -> str:
        return Colors._wrap("32", text)

    @staticmethod
    def red(text: str) -> str:
        return Colors._wrap("31", text)

    @staticmethod
    def yellow(text: str) -> str:
        return Colors._wrap("33", text)

    @staticmethod
    def bold(text: str) -> str:
        return Colors._wrap("1", text)

    @staticmethod
    def cyan(text: str) -> str:
        return Colors._wrap("36", text)


def ok(msg: str):
    print(f"  {Colors.green('[OK]')}   {msg}")


def fail(msg: str):
    print(f"  {Colors.red('[FAIL]')} {msg}")


def warn(msg: str):
    print(f"  {Colors.yellow('[WARN]')} {msg}")


def info(msg: str):
    print(f"  {Colors.cyan('[INFO]')} {msg}")


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_python_version() -> bool:
    """Verify Python >= 3.10."""
    print()
    print(Colors.bold("1. Version Python"))
    print("-" * 50)

    v = sys.version_info
    version_str = f"{v.major}.{v.minor}.{v.micro}"

    if (v.major, v.minor) >= MIN_PYTHON:
        ok(f"Python {version_str} >= {MIN_PYTHON[0]}.{MIN_PYTHON[1]}")
        return True
    else:
        fail(f"Python {version_str} < {MIN_PYTHON[0]}.{MIN_PYTHON[1]} requis")
        fail(f"Installez Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ depuis https://www.python.org/downloads/")
        return False


def check_and_install_requirements(do_install: bool = False) -> bool:
    """Check requirements.txt dependencies."""
    print()
    print(Colors.bold("2. Dependances (requirements.txt)"))
    print("-" * 50)

    if not REQUIREMENTS_FILE.exists():
        fail(f"Fichier introuvable : {REQUIREMENTS_FILE}")
        return False

    with open(REQUIREMENTS_FILE, "r", encoding="utf-8") as f:
        packages = [
            line.strip().split("==")[0].split(">=")[0].split("<")[0].strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]

    if not packages:
        warn("requirements.txt est vide")
        return True

    info(f"{len(packages)} packages dans requirements.txt")

    # Check which are missing
    missing = []
    for pkg in packages:
        try:
            __import__(pkg.replace("-", "_"))
        except ImportError:
            # Some packages have different import names
            alt_names = {
                "beautifulsoup4": "bs4",
                "scikit-learn": "sklearn",
                "pyyaml": "yaml",
                "pyarrow": "pyarrow",
            }
            alt = alt_names.get(pkg.lower())
            if alt:
                try:
                    __import__(alt)
                    continue
                except ImportError:
                    pass
            missing.append(pkg)

    if not missing:
        ok(f"Tous les {len(packages)} packages sont installes")
        return True

    warn(f"{len(missing)} package(s) manquant(s) : {', '.join(missing)}")

    if do_install:
        info("Installation des dependances...")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", str(REQUIREMENTS_FILE)],
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode == 0:
                ok("Dependances installees avec succes")
                return True
            else:
                fail(f"Echec de pip install : {result.stderr.strip()[-200:]}")
                return False
        except subprocess.TimeoutExpired:
            fail("Timeout lors de l'installation (> 5 min)")
            return False
        except Exception as e:
            fail(f"Erreur : {e}")
            return False
    else:
        info("Lancez 'python setup.py --install' pour installer automatiquement")
        return False


def create_directories() -> bool:
    """Create the output/ and logs/ directory structure."""
    print()
    print(Colors.bold("3. Arborescence des repertoires"))
    print("-" * 50)

    created = 0
    existed = 0
    errors = 0

    for dir_path in OUTPUT_DIRS:
        full_path = BASE_DIR / dir_path
        try:
            if full_path.exists():
                existed += 1
            else:
                full_path.mkdir(parents=True, exist_ok=True)
                created += 1
        except OSError as e:
            fail(f"Impossible de creer {dir_path} : {e}")
            errors += 1

    if created > 0:
        ok(f"{created} repertoire(s) cree(s)")
    if existed > 0:
        info(f"{existed} repertoire(s) existaient deja")
    if errors > 0:
        fail(f"{errors} erreur(s)")
        return False

    ok(f"Arborescence complete ({created + existed} repertoires)")
    return True


def check_scripts() -> bool:
    """Verify all key scripts exist."""
    print()
    print(Colors.bold("4. Verification des scripts"))
    print("-" * 50)

    missing = []
    found = 0

    for script in KEY_SCRIPTS:
        script_path = BASE_DIR / script
        if script_path.exists():
            found += 1
        else:
            missing.append(script)

    if not missing:
        ok(f"Tous les {found} scripts cles sont presents")
        return True

    warn(f"{found}/{len(KEY_SCRIPTS)} scripts trouves")
    for s in missing:
        fail(f"  Manquant : {s}")
    return False


def print_system_info():
    """Print system information: RAM, CPU, disk space."""
    print()
    print(Colors.bold("5. Informations systeme"))
    print("-" * 50)

    # OS
    info(f"OS         : {platform.system()} {platform.release()} ({platform.machine()})")
    info(f"Python     : {sys.version.split()[0]} ({sys.executable})")

    # CPU
    cpu_count = os.cpu_count()
    info(f"CPU cores  : {cpu_count}")

    try:
        if platform.system() == "Windows":
            import winreg
            try:
                key = winreg.OpenKey(
                    winreg.HKEY_LOCAL_MACHINE,
                    r"HARDWARE\DESCRIPTION\System\CentralProcessor\0"
                )
                cpu_name, _ = winreg.QueryValueEx(key, "ProcessorNameString")
                winreg.CloseKey(key)
                info(f"CPU        : {cpu_name.strip()}")
            except (OSError, FileNotFoundError):
                pass
        else:
            try:
                with open("/proc/cpuinfo", "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("model name"):
                            info(f"CPU        : {line.split(':')[1].strip()}")
                            break
            except FileNotFoundError:
                pass
    except Exception:
        pass

    # RAM
    ram_gb = -1
    try:
        import psutil
        mem = psutil.virtual_memory()
        ram_gb = mem.total / (1024 ** 3)
        ram_used = mem.used / (1024 ** 3)
        info(f"RAM        : {ram_used:.1f} / {ram_gb:.1f} GB ({mem.percent}% utilise)")
    except ImportError:
        # Fallback
        try:
            if platform.system() == "Windows":
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
                ram_gb = stat.ullTotalPhys / (1024 ** 3)
                used_gb = (stat.ullTotalPhys - stat.ullAvailPhys) / (1024 ** 3)
                info(f"RAM        : {used_gb:.1f} / {ram_gb:.1f} GB")
            else:
                with open("/proc/meminfo", "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("MemTotal"):
                            kb = int(line.split()[1])
                            ram_gb = kb / (1024 ** 2)
                            info(f"RAM        : {ram_gb:.1f} GB total")
                            break
        except Exception:
            warn("RAM        : impossible a determiner (installez psutil)")

    # Disk space
    try:
        usage = shutil.disk_usage(str(BASE_DIR))
        total_gb = usage.total / (1024 ** 3)
        free_gb = usage.free / (1024 ** 3)
        used_gb = (usage.total - usage.free) / (1024 ** 3)
        pct = (usage.total - usage.free) / usage.total * 100
        info(f"Disque     : {used_gb:.1f} / {total_gb:.1f} GB ({pct:.0f}% utilise, {free_gb:.1f} GB libre)")

        if free_gb < 10:
            warn(f"Espace disque faible ! ({free_gb:.1f} GB libre)")
    except OSError:
        warn("Disque     : impossible a determiner")

    # Project size
    try:
        output_dir = BASE_DIR / "output"
        if output_dir.exists():
            total_size = sum(
                f.stat().st_size
                for f in output_dir.rglob("*")
                if f.is_file()
            )
            info(f"output/    : {total_size / (1024 ** 3):.2f} GB")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Installation et verification du projet turf-data-pipeline"
    )
    parser.add_argument(
        "--install", action="store_true",
        help="Installer automatiquement les dependances manquantes"
    )
    parser.add_argument(
        "--check-only", action="store_true",
        help="Verifier uniquement, sans creer de repertoires"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Afficher plus de details"
    )
    args = parser.parse_args()

    print()
    print("=" * 60)
    print("  TURF-DATA-PIPELINE — Setup & Verification")
    print("=" * 60)
    print(f"  Repertoire : {BASE_DIR}")

    all_ok = True

    # 1. Python version
    if not check_python_version():
        all_ok = False

    # 2. Requirements
    if not check_and_install_requirements(do_install=args.install):
        all_ok = False

    # 3. Directories
    if args.check_only:
        print()
        print(Colors.bold("3. Arborescence des repertoires"))
        print("-" * 50)
        info("(ignore en mode --check-only)")
    else:
        if not create_directories():
            all_ok = False

    # 4. Scripts
    if not check_scripts():
        all_ok = False

    # 5. System info
    print_system_info()

    # Summary
    print()
    print("=" * 60)
    if all_ok:
        print(f"  {Colors.green('TOUT EST OK')} — Le projet est pret.")
    else:
        print(f"  {Colors.yellow('ATTENTION')} — Certaines verifications ont echoue.")
        print(f"  Relancez avec --install pour corriger automatiquement.")
    print("=" * 60)
    print()

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
