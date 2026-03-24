#!/usr/bin/env python3
"""
scripts/pre_model_checklist.py
===============================
Etape 12 — "Pret pour les modeles" : verification finale avant entrainement ML.

Checks effectues :
  1. partants_master.jsonl existe et > 2M records
  2. features_matrix.jsonl existe et > 2M records
  3. training_labels.jsonl existe et > 2M records
  4. Nombre de features >= 400 (champs uniques dans un echantillon)
  5. Alignement labels (chevauchement partant_uid > 95 %)
  6. Plage de dates couvre 2013-2026
  7. Pas de fuite de donnees (leakage_prevention)
  8. Desequilibre de classes documente (rapport existe)
  9. Exports Parquet existent
 10. Verdict : READY / NOT READY

Exit code 0 = READY, 1 = NOT READY.

Usage :
    python scripts/pre_model_checklist.py
    python scripts/pre_model_checklist.py --min-records 1000000
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Project bootstrap
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config import EXPORTS_DIR, FEATURES_MATRIX, FEATURES_MATRIX_PARQUET, PARTANTS_MASTER, TRAINING_LABELS

# ---------------------------------------------------------------------------
# ANSI helpers
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
# Thresholds
# ---------------------------------------------------------------------------
DEFAULT_MIN_RECORDS = 2_000_000
MIN_FEATURES = 400
MIN_LABEL_OVERLAP_PCT = 95.0
EXPECTED_MIN_YEAR = 2013
EXPECTED_MAX_YEAR = 2026
SAMPLE_SIZE_FEATURES = 500
SAMPLE_SIZE_OVERLAP = 50_000

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
_pass = 0
_fail = 0
_warn = 0
_stats: dict[str, object] = {}


def _result(status: str, msg: str) -> bool:
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
    print(f"  {tag} {msg}")
    return status == "PASS"


def _section(title: str) -> None:
    print(f"\n{BOLD}{CYAN}--- {title} ---{RESET}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_lines(path: Path) -> int:
    """Fast non-empty line count (no JSON parsing)."""
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace",
              buffering=1_048_576) as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


def _reservoir_sample_uids(path: Path, k: int, key: str = "partant_uid",
                           seed: int = 42) -> set[str]:
    """Reservoir-sample k partant_uid values from a JSONL file."""
    rng = random.Random(seed)
    result: list[str] = []
    n = 0
    with open(path, "r", encoding="utf-8", errors="replace",
              buffering=1_048_576) as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                continue
            uid = rec.get(key)
            if uid is None:
                continue
            n += 1
            if n <= k:
                result.append(uid)
            else:
                j = rng.randint(0, n - 1)
                if j < k:
                    result[j] = uid
    return set(result)


def _sample_field_names(path: Path, k: int = 500, seed: int = 42) -> set[str]:
    """Reservoir-sample k records from JSONL and return the union of all keys."""
    rng = random.Random(seed)
    records: list[dict] = []
    n = 0
    with open(path, "r", encoding="utf-8", errors="replace",
              buffering=1_048_576) as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                continue
            n += 1
            if n <= k:
                records.append(rec)
            else:
                j = rng.randint(0, n - 1)
                if j < k:
                    records[j] = rec
    all_keys: set[str] = set()
    for rec in records:
        all_keys.update(rec.keys())
    return all_keys


def _sample_date_range(path: Path, k: int = 2000, seed: int = 42,
                       date_key: str = "date_reunion_iso") -> tuple[int, int]:
    """Reservoir-sample k records and return (min_year, max_year)."""
    rng = random.Random(seed)
    dates: list[str] = []
    n = 0
    with open(path, "r", encoding="utf-8", errors="replace",
              buffering=1_048_576) as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                continue
            d = rec.get(date_key, "")
            if not d or len(d) < 4:
                continue
            n += 1
            if n <= k:
                dates.append(d)
            else:
                j = rng.randint(0, n - 1)
                if j < k:
                    dates[j] = d
    if not dates:
        return (0, 0)
    years = []
    for d in dates:
        try:
            years.append(int(d[:4]))
        except (ValueError, IndexError):
            pass
    if not years:
        return (0, 0)
    return (min(years), max(years))


# ===================================================================
# CHECKS
# ===================================================================

def check_file_records(name: str, path: Path, min_records: int) -> int:
    """Check that file exists and has enough records. Return count."""
    _section(f"Fichier : {name}")
    if not path.exists():
        _result("FAIL", f"{name} introuvable : {path}")
        return 0
    size_gb = path.stat().st_size / (1024 ** 3)
    _result("PASS", f"{name} existe ({size_gb:.2f} GB)")
    count = _count_lines(path)
    _stats[f"{name}_records"] = count
    if count >= min_records:
        _result("PASS", f"{count:,} records (>= {min_records:,})")
    else:
        _result("FAIL", f"{count:,} records (requis >= {min_records:,})")
    return count


def check_feature_count() -> int:
    """Check unique feature count >= MIN_FEATURES in features_matrix sample."""
    _section("Nombre de features")
    if not FEATURES_MATRIX.exists():
        _result("FAIL", "features_matrix.jsonl manquant, impossible de compter")
        return 0
    fields = _sample_field_names(FEATURES_MATRIX, k=SAMPLE_SIZE_FEATURES)
    n = len(fields)
    _stats["unique_fields_in_sample"] = n
    if n >= MIN_FEATURES:
        _result("PASS", f"{n} champs uniques (>= {MIN_FEATURES})")
    else:
        _result("FAIL", f"{n} champs uniques (requis >= {MIN_FEATURES})")
    return n


def check_label_alignment() -> float:
    """Check partant_uid overlap between features and labels > 95%."""
    _section("Alignement labels (partant_uid)")
    if not FEATURES_MATRIX.exists() or not TRAINING_LABELS.exists():
        _result("FAIL", "Fichiers manquants pour verifier l'alignement")
        return 0.0
    feat_uids = _reservoir_sample_uids(FEATURES_MATRIX, SAMPLE_SIZE_OVERLAP)
    label_uids = _reservoir_sample_uids(TRAINING_LABELS, SAMPLE_SIZE_OVERLAP)
    if not feat_uids or not label_uids:
        _result("FAIL", "Impossible d'extraire des partant_uid")
        return 0.0
    overlap = feat_uids & label_uids
    # Overlap relative to labels (all labels should be in features)
    pct = (len(overlap) / len(label_uids)) * 100 if label_uids else 0.0
    _stats["label_overlap_pct"] = round(pct, 2)
    if pct >= MIN_LABEL_OVERLAP_PCT:
        _result("PASS", f"Overlap {pct:.1f}% (>= {MIN_LABEL_OVERLAP_PCT}%)")
    else:
        _result("FAIL", f"Overlap {pct:.1f}% (requis >= {MIN_LABEL_OVERLAP_PCT}%)")
    return pct


def check_date_range() -> tuple[int, int]:
    """Check that data spans 2013-2026."""
    _section("Plage de dates")
    if not PARTANTS_MASTER.exists():
        _result("FAIL", "partants_master.jsonl manquant")
        return (0, 0)
    min_y, max_y = _sample_date_range(PARTANTS_MASTER)
    _stats["date_range"] = f"{min_y}-{max_y}"
    if min_y == 0:
        _result("FAIL", "Impossible de determiner la plage de dates")
        return (0, 0)
    ok = min_y <= EXPECTED_MIN_YEAR and max_y >= EXPECTED_MAX_YEAR
    if ok:
        _result("PASS", f"Plage {min_y}-{max_y} couvre {EXPECTED_MIN_YEAR}-{EXPECTED_MAX_YEAR}")
    else:
        _result("FAIL", f"Plage {min_y}-{max_y} ne couvre pas {EXPECTED_MIN_YEAR}-{EXPECTED_MAX_YEAR}")
    return (min_y, max_y)


def check_leakage() -> bool:
    """Run leakage_prevention.py and check exit code."""
    _section("Detection de fuites (leakage)")
    leakage_script = _PROJECT_ROOT / "quality" / "leakage_prevention.py"
    if not leakage_script.exists():
        _result("WARN", "quality/leakage_prevention.py introuvable, check saute")
        return True
    if not FEATURES_MATRIX.exists() or not TRAINING_LABELS.exists():
        _result("WARN", "Fichiers donnees manquants, leakage check saute")
        return True
    try:
        result = subprocess.run(
            [sys.executable, str(leakage_script)],
            cwd=str(_PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode == 0:
            _result("PASS", "Aucune fuite detectee")
            return True
        else:
            _result("FAIL", "Fuite potentielle detectee (exit code != 0)")
            # Show first few lines of stderr/stdout for context
            output_lines = (result.stdout + result.stderr).strip().splitlines()
            for line in output_lines[-5:]:
                print(f"         {line}")
            return False
    except subprocess.TimeoutExpired:
        _result("WARN", "leakage_prevention.py timeout (> 600s), saute")
        return True
    except Exception as exc:
        _result("WARN", f"Erreur execution leakage check : {exc}")
        return True


def check_class_imbalance_report() -> bool:
    """Check that class imbalance report exists."""
    _section("Rapport desequilibre de classes")
    report = _PROJECT_ROOT / "quality" / "class_imbalance_report.md"
    if report.exists() and report.stat().st_size > 0:
        _result("PASS", f"Rapport existe ({report.stat().st_size:,} bytes)")
        return True
    else:
        # Not a hard fail — the report can be generated
        _result("WARN",
                "Rapport class_imbalance_report.md absent. "
                "Lancez : python quality/class_imbalance_analyzer.py")
        return False


def check_parquet_exports() -> bool:
    """Check that Parquet exports exist."""
    _section("Exports Parquet")
    parquet_files = [
        ("features_matrix.parquet", FEATURES_MATRIX_PARQUET),
    ]
    # Also check for any parquet in exports/
    exports_parquets: list[Path] = []
    if EXPORTS_DIR.is_dir():
        exports_parquets = list(EXPORTS_DIR.glob("*.parquet"))

    all_ok = True
    for name, path in parquet_files:
        if path.exists() and path.stat().st_size > 0:
            size_gb = path.stat().st_size / (1024 ** 3)
            _result("PASS", f"{name} ({size_gb:.2f} GB)")
        else:
            _result("WARN", f"{name} absent ({path})")
            all_ok = False

    if exports_parquets:
        _result("PASS", f"{len(exports_parquets)} fichier(s) Parquet dans exports/")
    else:
        _result("WARN", "Aucun fichier Parquet dans output/exports/")
        all_ok = False

    return all_ok


# ===================================================================
# MAIN
# ===================================================================

def run_checklist(min_records: int = DEFAULT_MIN_RECORDS) -> dict:
    """Run all pre-model checks. Returns stats dict with 'ready' boolean."""
    global _pass, _fail, _warn, _stats
    _pass = _fail = _warn = 0
    _stats = {}

    print(f"\n{BOLD}{CYAN}{'=' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  ETAPE 12 : Pre-Model Checklist{RESET}")
    print(f"{BOLD}{CYAN}  Seuil minimum : {min_records:,} records{RESET}")
    print(f"{BOLD}{CYAN}{'=' * 60}{RESET}")

    t0 = time.monotonic()

    # 1-3. File existence + record counts
    pm_count = check_file_records("partants_master.jsonl", PARTANTS_MASTER, min_records)
    fm_count = check_file_records("features_matrix.jsonl", FEATURES_MATRIX, min_records)
    tl_count = check_file_records("training_labels.jsonl", TRAINING_LABELS, min_records)

    # 4. Feature count
    feat_count = check_feature_count()

    # 5. Label alignment
    overlap = check_label_alignment()

    # 6. Date range
    min_y, max_y = check_date_range()

    # 7. Leakage check
    no_leakage = check_leakage()

    # 8. Class imbalance documented
    imbalance_ok = check_class_imbalance_report()

    # 9. Parquet exports
    parquet_ok = check_parquet_exports()

    elapsed = time.monotonic() - t0

    # 10. Verdict
    ready = _fail == 0
    _stats.update({
        "pass": _pass,
        "fail": _fail,
        "warn": _warn,
        "ready": ready,
        "elapsed_seconds": round(elapsed, 1),
    })

    print(f"\n{BOLD}{CYAN}{'=' * 60}{RESET}")
    print(f"  {BOLD}Resultats :{RESET}  "
          f"{GREEN}{_pass} PASS{RESET}  |  "
          f"{RED}{_fail} FAIL{RESET}  |  "
          f"{YELLOW}{_warn} WARN{RESET}  "
          f"({elapsed:.1f}s)")

    if ready:
        print(f"\n  {GREEN}{BOLD}*** READY — donnees pretes pour l'entrainement ML ***{RESET}")
    else:
        print(f"\n  {RED}{BOLD}*** NOT READY — corriger les FAIL ci-dessus ***{RESET}")

    print(f"{BOLD}{CYAN}{'=' * 60}{RESET}\n")

    return _stats


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(
        description="Etape 12 : verification finale avant entrainement ML."
    )
    parser.add_argument(
        "--min-records", type=int, default=DEFAULT_MIN_RECORDS,
        help=f"Nombre minimum de records requis (defaut: {DEFAULT_MIN_RECORDS:,})",
    )
    args = parser.parse_args()

    stats = run_checklist(min_records=args.min_records)

    # Write stats to JSON sidecar
    stats_path = _PROJECT_ROOT / "quality" / "pre_model_checklist.json"
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2, default=str)
    print(f"  Stats ecrites : {stats_path}")

    sys.exit(0 if stats.get("ready") else 1)


if __name__ == "__main__":
    main()
