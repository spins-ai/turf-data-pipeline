#!/usr/bin/env python3
"""
remove_empty_fields.py — Etape 3.4 : Suppression des donnees inutiles
======================================================================
Scanne tous les fichiers JSONL dans output/ et identifie :
  1. Les champs 100% null sur l'ensemble des enregistrements
  2. Les champs redondants (meme valeur partout)

Supprime ces champs et sauvegarde les versions nettoyees.
Genere un rapport detaille de ce qui a ete supprime.

Fonctionne en streaming JSONL pour rester leger en RAM.

Usage :
    python remove_empty_fields.py [--output-dir OUTPUT_DIR] [--dry-run]
"""

import argparse
import json

import os
import sys
import tempfile
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SCAN_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
REPORT_FILE = LOG_DIR / "remove_empty_fields_report.json"

from utils.logging_setup import setup_logging
log = setup_logging("remove_empty_fields")


# ---------------------------------------------------------------------------
# Analyse des champs
# ---------------------------------------------------------------------------

def is_null_value(v: Any) -> bool:
    """Verifie si une valeur est consideree comme nulle."""
    if v is None:
        return True
    if isinstance(v, str) and v.strip() in ("", "null", "None", "N/A", "n/a", "NA", "-"):
        return True
    return False


def analyze_file(filepath: Path) -> Tuple[int, Dict[str, int], Dict[str, Dict[Any, int]]]:
    """
    Analyse un fichier JSONL et retourne :
      - total_records : nombre d'enregistrements
      - null_counts   : {field: nb_de_null}
      - value_counts  : {field: {valeur: count}} (uniquement si <= 1 valeur unique)

    Pour les champs redondants, on garde un compteur borne : des qu'on a > 1
    valeur distincte, on arrete de compter pour ce champ (optimisation).
    """
    total = 0
    null_counts: Dict[str, int] = defaultdict(int)
    # Pour detecter les champs a valeur unique, on stocke les valeurs vues
    # mais on arrete quand on a plus d'une valeur distincte
    unique_values: Dict[str, set] = defaultdict(set)
    # Ensemble des champs deja multi-valeurs (on les ignore)
    multi_valued: Set[str] = set()
    # Compteur de presences par champ
    presence_counts: Dict[str, int] = defaultdict(int)

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                log.warning(f"  Ligne {line_no} invalide dans {filepath.name}, ignoree")
                continue

            if not isinstance(record, dict):
                continue

            total += 1

            for key, val in record.items():
                presence_counts[key] += 1

                if is_null_value(val):
                    null_counts[key] += 1
                else:
                    # Tracking valeur unique
                    if key not in multi_valued:
                        # Convertir en hashable pour le set
                        try:
                            hashable = json.dumps(val, sort_keys=True, ensure_ascii=False) \
                                if isinstance(val, (dict, list)) else val
                        except (TypeError, ValueError):
                            hashable = str(val)

                        unique_values[key].add(hashable)
                        if len(unique_values[key]) > 1:
                            multi_valued.add(key)
                            del unique_values[key]  # libere memoire

    # Construire value_counts pour les champs a valeur unique
    value_counts: Dict[str, Dict[Any, int]] = {}
    for key, vals in unique_values.items():
        if len(vals) == 1:
            val = next(iter(vals))
            non_null_count = presence_counts[key] - null_counts.get(key, 0)
            value_counts[key] = {str(val): non_null_count}

    return total, dict(null_counts), value_counts


def identify_removable_fields(
    total: int,
    null_counts: Dict[str, int],
    value_counts: Dict[str, Dict[Any, int]],
    presence_counts: Optional[Dict[str, int]] = None,
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Identifie les champs a supprimer :
      - 100% null : le champ est null dans tous les enregistrements
      - Redondants : une seule valeur non-null dans tous les enregistrements

    Retourne (champs_null, [(champ, valeur_constante), ...])
    """
    if total == 0:
        return [], []

    # Champs 100% null
    all_null_fields = []
    for field, count in null_counts.items():
        if count >= total:
            all_null_fields.append(field)

    # Champs redondants (une seule valeur non-null, et 0 nulls)
    redundant_fields = []
    for field, vals in value_counts.items():
        null_count = null_counts.get(field, 0)
        # Si le champ a UNE seule valeur non-null et aucun null -> redondant
        if len(vals) == 1 and null_count == 0:
            const_val = next(iter(vals.keys()))
            redundant_fields.append((field, const_val))

    return sorted(all_null_fields), sorted(redundant_fields, key=lambda x: x[0])


# ---------------------------------------------------------------------------
# Nettoyage du fichier
# ---------------------------------------------------------------------------

def clean_file(filepath: Path, fields_to_remove: Set[str]) -> int:
    """
    Reecrit le fichier JSONL en supprimant les champs specifies.
    Utilise un fichier temporaire pour eviter la corruption.
    Retourne le nombre de lignes ecrites.
    """
    if not fields_to_remove:
        return 0

    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".jsonl",
        dir=str(filepath.parent),
    )
    os.close(tmp_fd)

    written = 0
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as fin, \
             open(tmp_path, "w", encoding="utf-8", errors="replace") as fout:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if isinstance(record, dict):
                    for field in fields_to_remove:
                        record.pop(field, None)

                fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                written += 1

        # Remplacer l'original
        shutil.move(tmp_path, str(filepath))
    except Exception:
        # Nettoyer en cas d'erreur
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    return written


# ---------------------------------------------------------------------------
# Recherche des fichiers JSONL
# ---------------------------------------------------------------------------

def find_jsonl_files(scan_dir: Path) -> List[Path]:
    """Trouve tous les fichiers .jsonl dans le repertoire (recursif)."""
    files = []
    for root, _dirs, filenames in os.walk(scan_dir):
        for fname in sorted(filenames):
            if fname.endswith(".jsonl"):
                files.append(Path(root) / fname)
    return files


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Supprime les champs 100%% null et redondants des fichiers JSONL"
    )
    parser.add_argument(
        "--scan-dir", type=str, default=str(DEFAULT_SCAN_DIR),
        help=f"Repertoire a scanner (defaut: {DEFAULT_SCAN_DIR})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Afficher ce qui serait supprime sans modifier les fichiers",
    )
    args = parser.parse_args()

    scan_dir = Path(args.scan_dir)
    if not scan_dir.exists():
        log.error(f"Repertoire introuvable : {scan_dir}")
        sys.exit(1)

    os.makedirs(LOG_DIR, exist_ok=True)

    log.info("=" * 70)
    log.info("ETAPE 3.4 : Suppression des champs inutiles")
    log.info(f"  Repertoire scanne : {scan_dir}")
    log.info(f"  Mode dry-run      : {args.dry_run}")
    log.info("=" * 70)

    jsonl_files = find_jsonl_files(scan_dir)
    if not jsonl_files:
        log.warning("Aucun fichier .jsonl trouve")
        sys.exit(0)

    log.info(f"Fichiers JSONL trouves : {len(jsonl_files)}")

    report = {
        "scan_dir": str(scan_dir),
        "dry_run": args.dry_run,
        "files": {},
        "summary": {
            "total_files": len(jsonl_files),
            "files_modified": 0,
            "total_null_fields_removed": 0,
            "total_redundant_fields_removed": 0,
        },
    }

    for filepath in jsonl_files:
        relative = filepath.relative_to(scan_dir)
        log.info(f"\nAnalyse de {relative} ...")

        total, null_counts, value_counts = analyze_file(filepath)
        if total == 0:
            log.info(f"  Fichier vide, ignore")
            continue

        null_fields, redundant_fields = identify_removable_fields(
            total, null_counts, value_counts
        )

        file_report = {
            "total_records": total,
            "total_fields": len(set(list(null_counts.keys()) + list(value_counts.keys()))),
            "null_fields": null_fields,
            "redundant_fields": {f: v for f, v in redundant_fields},
            "fields_removed": len(null_fields) + len(redundant_fields),
        }
        report["files"][str(relative)] = file_report

        if null_fields:
            log.info(f"  Champs 100%% null ({len(null_fields)}) : {', '.join(null_fields[:10])}"
                     + (f" ... (+{len(null_fields)-10})" if len(null_fields) > 10 else ""))

        if redundant_fields:
            log.info(f"  Champs redondants ({len(redundant_fields)}) :")
            for field, val in redundant_fields[:5]:
                val_str = str(val)[:50]
                log.info(f"    {field} = {val_str}")
            if len(redundant_fields) > 5:
                log.info(f"    ... (+{len(redundant_fields)-5} autres)")

        fields_to_remove = set(null_fields) | {f for f, _ in redundant_fields}
        if fields_to_remove:
            if not args.dry_run:
                written = clean_file(filepath, fields_to_remove)
                log.info(f"  -> {len(fields_to_remove)} champs supprimes, {written} lignes reecrites")
            else:
                log.info(f"  -> [DRY-RUN] {len(fields_to_remove)} champs seraient supprimes")

            report["summary"]["files_modified"] += 1
            report["summary"]["total_null_fields_removed"] += len(null_fields)
            report["summary"]["total_redundant_fields_removed"] += len(redundant_fields)
        else:
            log.info(f"  Aucun champ a supprimer")

    # Sauvegarder le rapport
    with open(REPORT_FILE, "w", encoding="utf-8", errors="replace") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    log.info(f"\nRapport sauvegarde : {REPORT_FILE}")

    log.info("\n" + "=" * 70)
    log.info("RESUME")
    log.info(f"  Fichiers analyses      : {report['summary']['total_files']}")
    log.info(f"  Fichiers modifies      : {report['summary']['files_modified']}")
    log.info(f"  Champs null supprimes  : {report['summary']['total_null_fields_removed']}")
    log.info(f"  Champs redondants sup. : {report['summary']['total_redundant_fields_removed']}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
