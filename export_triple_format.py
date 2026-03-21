#!/usr/bin/env python3
"""
export_triple_format.py — Etape 9.4 : Export triple format
==========================================================
Lit chaque fichier *_master.jsonl dans data_master/ et exporte vers :
  - JSON  (.json)   : tableau JSON complet
  - CSV   (.csv)    : fichier CSV avec header
  - Parquet (.parquet) : format columnar compresse

Gere le streaming JSONL pour les gros fichiers (pas de chargement complet
en RAM pour JSON et CSV).

Pour Parquet, utilise pyarrow si disponible, sinon skip avec un warning.

Usage :
    python export_triple_format.py [--source-dir PATH] [--output-dir PATH]
"""

import argparse
import csv
import io
import json

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE_DIR = BASE_DIR / "data_master"
DEFAULT_OUTPUT_DIR = BASE_DIR / "output" / "exports"
LOG_DIR = BASE_DIR / "logs"

from utils.logging_setup import setup_logging
log = setup_logging("export_triple_format")

# Essayer d'importer pyarrow pour Parquet
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


# ---------------------------------------------------------------------------
# Streaming JSONL reader
# ---------------------------------------------------------------------------

def iter_jsonl(filepath: Path):
    """Iterateur streaming sur les enregistrements d'un JSONL."""
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def count_and_collect_columns(filepath: Path) -> Tuple[int, List[str]]:
    """
    Premiere passe : compte les enregistrements et collecte toutes les colonnes.
    Retourne (count, sorted_columns).
    """
    count = 0
    columns: Set[str] = set()

    for record in iter_jsonl(filepath):
        if isinstance(record, dict):
            count += 1
            columns.update(record.keys())

    return count, sorted(columns)


# ---------------------------------------------------------------------------
# Export JSON (streaming)
# ---------------------------------------------------------------------------

def export_json(source: Path, output: Path, total: int) -> int:
    """
    Exporte un JSONL vers un fichier JSON (tableau).
    Utilise le streaming pour eviter de tout charger en RAM.
    Retourne la taille du fichier en octets.
    """
    with open(output, "w", encoding="utf-8") as fout:
        fout.write("[\n")
        first = True
        for record in iter_jsonl(source):
            if not isinstance(record, dict):
                continue
            if not first:
                fout.write(",\n")
            fout.write("  " + json.dumps(record, ensure_ascii=False, default=str))
            first = False
        fout.write("\n]\n")

    return output.stat().st_size


# ---------------------------------------------------------------------------
# Export CSV (streaming)
# ---------------------------------------------------------------------------

def export_csv(source: Path, output: Path, columns: List[str]) -> int:
    """
    Exporte un JSONL vers un fichier CSV.
    Utilise le streaming pour eviter de tout charger en RAM.
    Retourne la taille du fichier en octets.
    """
    with open(output, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(
            fout,
            fieldnames=columns,
            extrasaction="ignore",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()

        for record in iter_jsonl(source):
            if not isinstance(record, dict):
                continue
            # Convertir les valeurs complexes en JSON string
            row = {}
            for col in columns:
                val = record.get(col)
                if isinstance(val, (dict, list)):
                    row[col] = json.dumps(val, ensure_ascii=False, default=str)
                elif val is None:
                    row[col] = ""
                else:
                    row[col] = val
            writer.writerow(row)

    return output.stat().st_size


# ---------------------------------------------------------------------------
# Export Parquet (batch)
# ---------------------------------------------------------------------------

def export_parquet(source: Path, output: Path, columns: List[str]) -> int:
    """
    Exporte un JSONL vers un fichier Parquet.
    Charge par batches pour limiter la RAM.
    Retourne la taille du fichier en octets.
    """
    if not HAS_PYARROW:
        log.warning("  pyarrow non installe, export Parquet ignore")
        return 0

    BATCH_SIZE = 50_000
    batch: List[Dict[str, Any]] = []
    writer = None

    try:
        for record in iter_jsonl(source):
            if not isinstance(record, dict):
                continue

            # Normaliser les valeurs complexes en string pour Parquet
            row = {}
            for col in columns:
                val = record.get(col)
                if isinstance(val, (dict, list)):
                    row[col] = json.dumps(val, ensure_ascii=False, default=str)
                else:
                    row[col] = val
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                table = _batch_to_table(batch, columns)
                if writer is None:
                    writer = pq.ParquetWriter(str(output), table.schema)
                writer.write_table(table)
                batch.clear()

        # Ecrire le dernier batch
        if batch:
            table = _batch_to_table(batch, columns)
            if writer is None:
                writer = pq.ParquetWriter(str(output), table.schema)
            writer.write_table(table)

    finally:
        if writer is not None:
            writer.close()

    if output.exists():
        return output.stat().st_size
    return 0


def _batch_to_table(batch: List[Dict[str, Any]], columns: List[str]) -> "pa.Table":
    """Convertit un batch de dicts en pa.Table."""
    col_data: Dict[str, list] = {col: [] for col in columns}
    for row in batch:
        for col in columns:
            col_data[col].append(row.get(col))

    arrays = {}
    for col in columns:
        values = col_data[col]
        # Tout convertir en string pour eviter les conflits de types
        str_values = [str(v) if v is not None else None for v in values]
        arrays[col] = pa.array(str_values, type=pa.string())

    return pa.table(arrays)


# ---------------------------------------------------------------------------
# Formatage taille
# ---------------------------------------------------------------------------

def format_size(size_bytes: int) -> str:
    """Formate une taille en octets en unite lisible."""
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    size = float(size_bytes)
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    return f"{size:.1f} {units[idx]}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Export des fichiers master en JSON + CSV + Parquet"
    )
    parser.add_argument(
        "--source-dir", type=str, default=str(DEFAULT_SOURCE_DIR),
        help=f"Repertoire source (defaut: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR),
        help=f"Repertoire de sortie (defaut: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_dir)

    if not source_dir.exists():
        log.error(f"Repertoire source introuvable : {source_dir}")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    log.info("=" * 70)
    log.info("ETAPE 9.4 : Export triple format (JSON + CSV + Parquet)")
    log.info(f"  Source  : {source_dir}")
    log.info(f"  Output  : {output_dir}")
    log.info(f"  Parquet : {'disponible (pyarrow)' if HAS_PYARROW else 'INDISPONIBLE (pip install pyarrow)'}")
    log.info("=" * 70)

    # Trouver les fichiers *_master.jsonl
    master_files = sorted([
        f for f in source_dir.iterdir()
        if f.is_file() and f.name.endswith("_master.jsonl")
    ])

    if not master_files:
        log.warning("Aucun fichier *_master.jsonl trouve")
        sys.exit(0)

    log.info(f"Fichiers master trouves : {len(master_files)}")

    report = {
        "timestamp": datetime.now().isoformat(),
        "source_dir": str(source_dir),
        "output_dir": str(output_dir),
        "pyarrow_available": HAS_PYARROW,
        "files": {},
    }

    total_json_size = 0
    total_csv_size = 0
    total_parquet_size = 0

    for master_file in master_files:
        stem = master_file.stem  # e.g. "partants_master"
        log.info(f"\n{'—' * 50}")
        log.info(f"Traitement : {master_file.name}")

        # Premiere passe : compter et collecter les colonnes
        record_count, columns = count_and_collect_columns(master_file)
        log.info(f"  Enregistrements : {record_count:,}")
        log.info(f"  Colonnes        : {len(columns)}")

        if record_count == 0:
            log.warning(f"  Fichier vide, ignore")
            continue

        file_report: Dict[str, Any] = {
            "records": record_count,
            "columns": len(columns),
            "source_size": format_size(master_file.stat().st_size),
            "source_size_bytes": master_file.stat().st_size,
        }

        # Export JSON
        json_out = output_dir / f"{stem}.json"
        log.info(f"  Export JSON -> {json_out.name} ...")
        try:
            json_size = export_json(master_file, json_out, record_count)
            log.info(f"    Taille : {format_size(json_size)}")
            file_report["json_size"] = format_size(json_size)
            file_report["json_size_bytes"] = json_size
            total_json_size += json_size
        except Exception as e:
            log.error(f"    Erreur JSON : {e}")
            file_report["json_error"] = str(e)

        # Export CSV
        csv_out = output_dir / f"{stem}.csv"
        log.info(f"  Export CSV -> {csv_out.name} ...")
        try:
            csv_size = export_csv(master_file, csv_out, columns)
            log.info(f"    Taille : {format_size(csv_size)}")
            file_report["csv_size"] = format_size(csv_size)
            file_report["csv_size_bytes"] = csv_size
            total_csv_size += csv_size
        except Exception as e:
            log.error(f"    Erreur CSV : {e}")
            file_report["csv_error"] = str(e)

        # Export Parquet
        parquet_out = output_dir / f"{stem}.parquet"
        if HAS_PYARROW:
            log.info(f"  Export Parquet -> {parquet_out.name} ...")
            try:
                parquet_size = export_parquet(master_file, parquet_out, columns)
                log.info(f"    Taille : {format_size(parquet_size)}")
                file_report["parquet_size"] = format_size(parquet_size)
                file_report["parquet_size_bytes"] = parquet_size
                total_parquet_size += parquet_size
            except Exception as e:
                log.error(f"    Erreur Parquet : {e}")
                file_report["parquet_error"] = str(e)
        else:
            file_report["parquet_skipped"] = True

        report["files"][master_file.name] = file_report

    # Resume
    log.info("\n" + "=" * 70)
    log.info("RESUME EXPORT TRIPLE FORMAT")
    log.info(f"  Fichiers traites    : {len(report['files'])}")
    log.info(f"  Taille totale JSON  : {format_size(total_json_size)}")
    log.info(f"  Taille totale CSV   : {format_size(total_csv_size)}")
    if HAS_PYARROW:
        log.info(f"  Taille totale Parquet : {format_size(total_parquet_size)}")
    log.info(f"  Repertoire sortie   : {output_dir}")
    log.info("=" * 70)

    report["summary"] = {
        "total_json_size": format_size(total_json_size),
        "total_csv_size": format_size(total_csv_size),
        "total_parquet_size": format_size(total_parquet_size) if HAS_PYARROW else "N/A",
    }

    report_file = LOG_DIR / "export_triple_format_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    log.info(f"Rapport sauvegarde : {report_file}")


if __name__ == "__main__":
    main()
