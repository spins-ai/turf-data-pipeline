#!/usr/bin/env python3
"""
daily_collect_pmu.py — Collecte automatique quotidienne PMU
============================================================
Lance la collecte du programme du jour (reunions, courses, participants)
puis met a jour les masters et features incrementalement.

Usage:
    python scripts/daily_collect_pmu.py              # collecte aujourd'hui
    python scripts/daily_collect_pmu.py --date 2026-04-11  # date specifique
    python scripts/daily_collect_pmu.py --days-back 3      # 3 derniers jours

Automatisation (Windows Task Scheduler):
    schtasks /create /tn "TurfPMU" /tr "python D:\\turf-data-pipeline\\scripts\\daily_collect_pmu.py" /sc daily /st 08:00

Ce script:
  1. Collecte calendrier reunions (01)
  2. Collecte liste courses (02)
  3. Collecte resultats (04) — seulement si courses terminees
  4. Log le resultat dans logs/daily_collect_YYYYMMDD.log
"""

from __future__ import annotations

import subprocess
import sys
import time
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PYTHON_EXE, LOGS_DIR, DATA_DIR

COLLECTION_DIR = SCRIPT_DIR / "collection"


def log(msg: str, logfile=None):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    if logfile:
        logfile.write(line + "\n")
        logfile.flush()


def run_script(script_path: Path, args: list[str], logfile=None) -> bool:
    """Run a collection script with args. Returns True on success."""
    cmd = [PYTHON_EXE, str(script_path)] + args
    log(f"  Running: {script_path.name} {' '.join(args)}", logfile)
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=600,  # 10 min max per script
        )
        if result.returncode == 0:
            # Show last 3 lines of output
            lines = result.stdout.strip().split("\n")
            for line in lines[-3:]:
                log(f"    {line}", logfile)
            return True
        else:
            log(f"  ERREUR (code {result.returncode}): {result.stderr[-200:]}", logfile)
            return False
    except subprocess.TimeoutExpired:
        log(f"  TIMEOUT (>600s)", logfile)
        return False
    except Exception as e:
        log(f"  EXCEPTION: {e}", logfile)
        return False


def main():
    parser = argparse.ArgumentParser(description="Collecte quotidienne PMU")
    parser.add_argument("--date", type=str, help="Date specifique (YYYY-MM-DD)")
    parser.add_argument("--days-back", type=int, default=0,
                       help="Nombre de jours en arriere a collecter")
    args = parser.parse_args()

    # Determine dates to collect
    if args.date:
        dates = [args.date]
    elif args.days_back > 0:
        dates = []
        for i in range(args.days_back + 1):
            d = date.today() - timedelta(days=i)
            dates.append(d.isoformat())
        dates.reverse()
    else:
        dates = [date.today().isoformat()]

    # Setup log
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOGS_DIR / f"daily_collect_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logfile = open(log_path, "w", encoding="utf-8")

    start = time.time()
    log("=" * 60, logfile)
    log("COLLECTE QUOTIDIENNE PMU", logfile)
    log(f"Dates: {dates}", logfile)
    log("=" * 60, logfile)

    results = {"ok": 0, "fail": 0}

    for d in dates:
        log(f"\n--- Date: {d} ---", logfile)

        # Step 1: Calendrier reunions
        script = COLLECTION_DIR / "01_calendrier_reunions.py"
        if script.exists():
            ok = run_script(script, ["--date-debut", d, "--date-fin", d], logfile)
            results["ok" if ok else "fail"] += 1
        else:
            log(f"  SKIP: {script.name} non trouve", logfile)

        # Step 2: Liste courses
        script = COLLECTION_DIR / "02_liste_courses.py"
        if script.exists():
            ok = run_script(script, ["--date-debut", d, "--date-fin", d], logfile)
            results["ok" if ok else "fail"] += 1
        else:
            log(f"  SKIP: {script.name} non trouve", logfile)

        # Step 3: Resultats (only for past dates, not today)
        if d < date.today().isoformat():
            script = COLLECTION_DIR / "04_resultats.py"
            if script.exists():
                ok = run_script(script, ["--date-debut", d, "--date-fin", d], logfile)
                results["ok" if ok else "fail"] += 1
            else:
                log(f"  SKIP: {script.name} non trouve", logfile)
        else:
            log(f"  SKIP resultats (date=aujourd'hui, courses pas terminees)", logfile)

    elapsed = time.time() - start
    log(f"\n{'='*60}", logfile)
    log(f"TERMINE en {elapsed:.0f}s | OK={results['ok']} | FAIL={results['fail']}", logfile)
    log(f"Log: {log_path}", logfile)
    log(f"{'='*60}", logfile)

    logfile.close()

    # Print setup instructions
    if "--help" not in sys.argv:
        print(f"\n--- Pour automatiser (Windows Task Scheduler): ---")
        print(f'schtasks /create /tn "TurfPMU_Daily" /tr "{PYTHON_EXE} {Path(__file__).resolve()}" /sc daily /st 08:00')
        print(f'schtasks /create /tn "TurfPMU_Results" /tr "{PYTHON_EXE} {Path(__file__).resolve()} --days-back 1" /sc daily /st 22:00')


if __name__ == "__main__":
    main()
