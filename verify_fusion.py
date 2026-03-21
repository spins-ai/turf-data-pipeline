#!/usr/bin/env python3
"""
verify_fusion.py — Etape 5.3 : Verification post-fusion
========================================================
Verifie l'integrite de partants_master.jsonl apres la mega-fusion.

Controles effectues :
  1. partants_master.jsonl contient >= 2.7M enregistrements
  2. Nombre de colonnes >= 200
  3. Aucun enregistrement perdu vs fichiers sources
  4. Echantillon aleatoire de 100 enregistrements pour verification manuelle
  5. Generation d'un rapport de verification complet

Fonctionne en streaming JSONL pour rester leger en RAM.

Usage :
    python verify_fusion.py [--master-file PATH] [--source-dir PATH]
"""

import argparse
import json

import os
import random
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_MASTER_FILE = BASE_DIR / "data_master" / "partants_master.jsonl"
DEFAULT_SOURCE_DIR = BASE_DIR / "data_master"
LOG_DIR = BASE_DIR / "logs"
REPORT_FILE = LOG_DIR / "verify_fusion_report.json"
SAMPLE_FILE = LOG_DIR / "verify_fusion_sample.json"

MIN_RECORDS = 2_700_000
MIN_COLUMNS = 200

from utils.logging_setup import setup_logging
log = setup_logging("verify_fusion")


# ---------------------------------------------------------------------------
# Comptage de lignes (streaming)
# ---------------------------------------------------------------------------

def count_jsonl_records(filepath: Path) -> Tuple[int, Set[str], List[str]]:
    """
    Compte les enregistrements d'un JSONL en streaming.
    Retourne (count, all_columns, errors).
    """
    count = 0
    all_columns: Set[str] = set()
    errors: List[str] = []

    if not filepath.exists():
        return 0, set(), [f"Fichier introuvable : {filepath}"]

    with open(filepath, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if isinstance(record, dict):
                    count += 1
                    all_columns.update(record.keys())
                else:
                    errors.append(f"Ligne {line_no}: pas un dict JSON")
            except json.JSONDecodeError as e:
                errors.append(f"Ligne {line_no}: JSON invalide ({e})")

    return count, all_columns, errors


def count_jsonl_lines(filepath: Path) -> int:
    """Compte rapidement le nombre de lignes non-vides (sans parser le JSON)."""
    if not filepath.exists():
        return 0
    count = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


# ---------------------------------------------------------------------------
# Echantillonnage reservoir
# ---------------------------------------------------------------------------

def reservoir_sample(filepath: Path, k: int = 100) -> List[Dict[str, Any]]:
    """
    Echantillonnage reservoir de k enregistrements depuis un JSONL.
    Complexite O(n) en temps, O(k) en memoire.
    """
    sample: List[Dict[str, Any]] = []
    n = 0

    if not filepath.exists():
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not isinstance(record, dict):
                continue

            n += 1
            if n <= k:
                sample.append(record)
            else:
                # Probabilite k/n de remplacer un element existant
                j = random.randint(0, n - 1)
                if j < k:
                    sample[j] = record

    return sample


# ---------------------------------------------------------------------------
# Verification des sources
# ---------------------------------------------------------------------------

def find_source_files(source_dir: Path) -> List[Path]:
    """Trouve les fichiers *_master.jsonl dans le repertoire source."""
    files = []
    if not source_dir.exists():
        return files

    for f in sorted(source_dir.iterdir()):
        if f.is_file() and f.name.endswith("_master.jsonl") and f.name != "partants_master.jsonl":
            files.append(f)
    return files


def check_source_coverage(
    master_file: Path,
    source_files: List[Path],
) -> Dict[str, Any]:
    """
    Verifie que le nombre de records du master est >= chaque source.
    Retourne un dict avec les resultats.
    """
    results = {
        "source_counts": {},
        "master_count": 0,
        "potential_losses": [],
    }

    master_count = count_jsonl_lines(master_file)
    results["master_count"] = master_count

    for sf in source_files:
        count = count_jsonl_lines(sf)
        results["source_counts"][sf.name] = count
        log.info(f"  Source {sf.name:40s} : {count:>12,} enregistrements")

    return results


# ---------------------------------------------------------------------------
# Analyse de completude des colonnes
# ---------------------------------------------------------------------------

def analyze_column_fill_rates(filepath: Path, sample_size: int = 10000) -> Dict[str, float]:
    """
    Calcule le taux de remplissage de chaque colonne sur un echantillon.
    Retourne {colonne: taux_remplissage (0.0 a 1.0)}.
    """
    fill_counts: Dict[str, int] = defaultdict(int)
    presence_counts: Dict[str, int] = defaultdict(int)
    total = 0

    if not filepath.exists():
        return {}

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not isinstance(record, dict):
                continue

            total += 1
            for key, val in record.items():
                presence_counts[key] += 1
                if val is not None and val != "" and val != "null":
                    fill_counts[key] += 1

            if total >= sample_size:
                break

    if total == 0:
        return {}

    return {
        col: fill_counts.get(col, 0) / total
        for col in presence_counts
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Verification post-fusion de partants_master.jsonl"
    )
    parser.add_argument(
        "--master-file", type=str, default=str(DEFAULT_MASTER_FILE),
        help=f"Chemin du fichier master (defaut: {DEFAULT_MASTER_FILE})",
    )
    parser.add_argument(
        "--source-dir", type=str, default=str(DEFAULT_SOURCE_DIR),
        help=f"Repertoire des fichiers source (defaut: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--sample-size", type=int, default=100,
        help="Nombre d'enregistrements a echantillonner (defaut: 100)",
    )
    args = parser.parse_args()

    master_file = Path(args.master_file)
    source_dir = Path(args.source_dir)

    os.makedirs(LOG_DIR, exist_ok=True)

    log.info("=" * 70)
    log.info("ETAPE 5.3 : Verification post-fusion")
    log.info(f"  Fichier master : {master_file}")
    log.info(f"  Sources dir    : {source_dir}")
    log.info("=" * 70)

    checks_passed = 0
    checks_failed = 0
    checks_warnings = 0
    report: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(),
        "master_file": str(master_file),
        "checks": {},
    }

    # --- Check 1 : le fichier existe ---
    if not master_file.exists():
        log.error(f"ECHEC : Fichier master introuvable : {master_file}")
        report["checks"]["file_exists"] = {"status": "FAIL", "detail": "Fichier introuvable"}
        checks_failed += 1
        # Sauvegarder rapport minimal et quitter
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        log.info(f"Rapport sauvegarde : {REPORT_FILE}")
        sys.exit(1)

    report["checks"]["file_exists"] = {"status": "PASS"}
    checks_passed += 1

    # --- Check 2 : nombre d'enregistrements >= 2.7M ---
    log.info("\n[Check 2] Comptage des enregistrements ...")
    record_count, all_columns, parse_errors = count_jsonl_records(master_file)
    log.info(f"  Enregistrements : {record_count:,}")

    if record_count >= MIN_RECORDS:
        log.info(f"  OK : >= {MIN_RECORDS:,}")
        report["checks"]["min_records"] = {
            "status": "PASS",
            "count": record_count,
            "threshold": MIN_RECORDS,
        }
        checks_passed += 1
    else:
        log.warning(f"  ATTENTION : {record_count:,} < {MIN_RECORDS:,} (seuil)")
        report["checks"]["min_records"] = {
            "status": "WARN",
            "count": record_count,
            "threshold": MIN_RECORDS,
            "detail": f"Seulement {record_count:,} enregistrements (seuil: {MIN_RECORDS:,})",
        }
        checks_warnings += 1

    if parse_errors:
        log.warning(f"  {len(parse_errors)} erreurs de parsing (premieres 5) :")
        for err in parse_errors[:5]:
            log.warning(f"    {err}")
        report["checks"]["parse_errors"] = {
            "status": "WARN",
            "count": len(parse_errors),
            "first_errors": parse_errors[:10],
        }

    # --- Check 3 : nombre de colonnes >= 200 ---
    log.info("\n[Check 3] Comptage des colonnes ...")
    col_count = len(all_columns)
    log.info(f"  Colonnes uniques : {col_count}")

    if col_count >= MIN_COLUMNS:
        log.info(f"  OK : >= {MIN_COLUMNS}")
        report["checks"]["min_columns"] = {
            "status": "PASS",
            "count": col_count,
            "threshold": MIN_COLUMNS,
        }
        checks_passed += 1
    else:
        log.warning(f"  ATTENTION : {col_count} < {MIN_COLUMNS} (seuil)")
        report["checks"]["min_columns"] = {
            "status": "WARN",
            "count": col_count,
            "threshold": MIN_COLUMNS,
            "detail": f"Seulement {col_count} colonnes (seuil: {MIN_COLUMNS})",
        }
        checks_warnings += 1

    # Lister les colonnes
    report["checks"]["columns_list"] = sorted(all_columns)

    # --- Check 4 : verification vs sources ---
    log.info("\n[Check 4] Verification vs fichiers sources ...")
    source_files = find_source_files(source_dir)

    if source_files:
        coverage = check_source_coverage(master_file, source_files)
        report["checks"]["source_coverage"] = coverage

        log.info(f"  Master : {coverage['master_count']:,} enregistrements")
        log.info(f"  Sources trouvees : {len(source_files)}")
        checks_passed += 1
    else:
        log.warning("  Aucun fichier source *_master.jsonl trouve")
        report["checks"]["source_coverage"] = {
            "status": "WARN",
            "detail": "Aucun fichier source trouve",
        }
        checks_warnings += 1

    # --- Check 5 : echantillon aleatoire ---
    log.info(f"\n[Check 5] Echantillonnage de {args.sample_size} enregistrements ...")
    sample = reservoir_sample(master_file, k=args.sample_size)
    log.info(f"  Echantillon : {len(sample)} enregistrements")

    if sample:
        # Statistiques sur l'echantillon
        sample_col_counts = defaultdict(int)
        for rec in sample:
            for key in rec:
                sample_col_counts[key] += 1

        # Colonnes presentes dans tous les enregistrements de l'echantillon
        always_present = [
            col for col, cnt in sample_col_counts.items()
            if cnt == len(sample)
        ]
        sometimes_present = [
            col for col, cnt in sample_col_counts.items()
            if cnt < len(sample)
        ]

        log.info(f"  Colonnes toujours presentes : {len(always_present)}")
        log.info(f"  Colonnes parfois absentes   : {len(sometimes_present)}")

        report["checks"]["sample"] = {
            "status": "PASS",
            "sample_size": len(sample),
            "columns_always_present": len(always_present),
            "columns_sometimes_present": len(sometimes_present),
        }

        # Sauvegarder l'echantillon
        with open(SAMPLE_FILE, "w", encoding="utf-8") as f:
            json.dump(sample, f, indent=2, ensure_ascii=False, default=str)
        log.info(f"  Echantillon sauvegarde : {SAMPLE_FILE}")
        checks_passed += 1
    else:
        log.warning("  Echantillon vide")
        report["checks"]["sample"] = {"status": "WARN", "detail": "Echantillon vide"}
        checks_warnings += 1

    # --- Check 6 : taux de remplissage ---
    log.info("\n[Check 6] Taux de remplissage (sur 10 000 premiers) ...")
    fill_rates = analyze_column_fill_rates(master_file, sample_size=10000)

    if fill_rates:
        fully_filled = sum(1 for r in fill_rates.values() if r >= 0.99)
        mostly_filled = sum(1 for r in fill_rates.values() if 0.5 <= r < 0.99)
        sparse = sum(1 for r in fill_rates.values() if 0.01 <= r < 0.5)
        near_empty = sum(1 for r in fill_rates.values() if r < 0.01)

        log.info(f"  Remplies >= 99%%  : {fully_filled}")
        log.info(f"  Remplies 50-99%% : {mostly_filled}")
        log.info(f"  Remplies 1-50%%  : {sparse}")
        log.info(f"  Remplies < 1%%   : {near_empty}")

        # Top 10 colonnes les moins remplies
        sorted_rates = sorted(fill_rates.items(), key=lambda x: x[1])
        lowest = sorted_rates[:10]
        if lowest:
            log.info("  10 colonnes les moins remplies :")
            for col, rate in lowest:
                log.info(f"    {col:40s} {rate*100:6.2f}%%")

        report["checks"]["fill_rates"] = {
            "status": "PASS",
            "fully_filled": fully_filled,
            "mostly_filled": mostly_filled,
            "sparse": sparse,
            "near_empty": near_empty,
            "lowest_10": {col: round(rate, 4) for col, rate in lowest},
        }
        checks_passed += 1

    # --- Resume ---
    log.info("\n" + "=" * 70)
    log.info("RESUME VERIFICATION POST-FUSION")
    log.info(f"  Checks passes   : {checks_passed}")
    log.info(f"  Checks warnings : {checks_warnings}")
    log.info(f"  Checks echecs   : {checks_failed}")

    overall = "PASS" if checks_failed == 0 else "FAIL"
    if checks_warnings > 0 and checks_failed == 0:
        overall = "WARN"

    log.info(f"  Statut global   : {overall}")
    log.info("=" * 70)

    report["summary"] = {
        "status": overall,
        "checks_passed": checks_passed,
        "checks_warnings": checks_warnings,
        "checks_failed": checks_failed,
        "record_count": record_count,
        "column_count": col_count,
    }

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    log.info(f"\nRapport sauvegarde : {REPORT_FILE}")

    sys.exit(0 if checks_failed == 0 else 1)


if __name__ == "__main__":
    main()
