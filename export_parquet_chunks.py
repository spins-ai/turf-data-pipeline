#!/usr/bin/env python3
"""
export_parquet_chunks.py
========================
Convertit un gros fichier JSONL en Parquet par chunks de 50K lignes.
Evite de charger tout en memoire : lit chunk par chunk, ecrit en append.

Necessite: pyarrow

Usage:
    python3 export_parquet_chunks.py data_master/partants_master.jsonl
    python3 export_parquet_chunks.py data_master/partants_master_v2.jsonl --chunk-size 100000
    python3 export_parquet_chunks.py data_master/courses_master.jsonl -o data_master/courses.parquet
"""

import argparse
import json
import logging
import os
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

CHUNK_SIZE_DEFAULT = 50000


def parse_args():
    parser = argparse.ArgumentParser(description="Convertir JSONL en Parquet par chunks")
    parser.add_argument("input_file", help="Chemin vers le fichier JSONL source")
    parser.add_argument("-o", "--output", default=None,
                        help="Chemin du fichier Parquet en sortie (defaut: meme nom .parquet)")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE_DEFAULT,
                        help="Nombre de lignes par chunk (defaut: %d)" % CHUNK_SIZE_DEFAULT)
    return parser.parse_args()


def read_jsonl_chunk(file_handle, chunk_size):
    """Lit un chunk de lignes JSONL et retourne une liste de dicts."""
    records = []
    for _ in range(chunk_size):
        line = file_handle.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def infer_and_unify_schema(records, known_columns):
    """Ajoute les colonnes manquantes avec None pour uniformiser le schema."""
    for rec in records:
        for col in rec:
            known_columns.add(col)
    # S'assurer que chaque record a toutes les colonnes connues
    for rec in records:
        for col in known_columns:
            if col not in rec:
                rec[col] = None
    return records


def main():
    args = parse_args()
    input_file = args.input_file
    chunk_size = args.chunk_size

    if not os.path.exists(input_file):
        log.error("Fichier introuvable: %s", input_file)
        sys.exit(1)

    if not input_file.endswith(".jsonl"):
        log.error("Le fichier doit etre un .jsonl")
        sys.exit(1)

    # Import pyarrow
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        log.error("pyarrow non installe. Installer avec: pip install pyarrow")
        sys.exit(1)

    output_file = args.output
    if output_file is None:
        output_file = input_file.rsplit(".", 1)[0] + ".parquet"

    file_size = os.path.getsize(input_file)
    log.info("=" * 70)
    log.info("EXPORT PARQUET PAR CHUNKS")
    log.info("  Source: %s (%.2f GB)", input_file, file_size / (1024 ** 3))
    log.info("  Output: %s", output_file)
    log.info("  Chunk size: %d lignes", chunk_size)
    log.info("=" * 70)

    t0 = time.time()
    total_rows = 0
    chunk_num = 0
    known_columns = set()
    writer = None

    try:
        with open(input_file, "r", encoding="utf-8", errors="replace") as fin:
            while True:
                records = read_jsonl_chunk(fin, chunk_size)
                if not records:
                    break

                chunk_num += 1
                total_rows += len(records)

                # Uniformiser le schema
                records = infer_and_unify_schema(records, known_columns)

                # Convertir en table pyarrow
                # On passe par colonnes pour eviter les problemes de types mixtes
                columns = {}
                for col in known_columns:
                    values = [rec.get(col) for rec in records]
                    # Laisser pyarrow inferer le type
                    try:
                        columns[col] = values
                    except Exception:
                        columns[col] = [str(v) if v is not None else None for v in values]

                try:
                    table = pa.table(columns)
                except pa.ArrowInvalid:
                    # Fallback: tout en string
                    str_columns = {}
                    for col, vals in columns.items():
                        str_columns[col] = [
                            str(v) if v is not None else None for v in vals
                        ]
                    table = pa.table(str_columns)

                if writer is None:
                    writer = pq.ParquetWriter(output_file, table.schema,
                                              compression="snappy")
                    writer.write_table(table)
                else:
                    # Schema evolution: ajouter les nouvelles colonnes
                    try:
                        writer.write_table(table)
                    except pa.ArrowInvalid:
                        # Schema a change, on ferme et reouvre avec le nouveau schema
                        log.warning("  Schema change au chunk %d, reouverture du writer", chunk_num)
                        writer.close()
                        # Relire ce qu'on a deja et fusionner
                        existing = pq.read_table(output_file)
                        # Combiner les schemas
                        merged_schema = pa.unify_schemas([existing.schema, table.schema])
                        writer = pq.ParquetWriter(output_file, merged_schema,
                                                  compression="snappy")
                        # Re-ecrire l'existant avec le nouveau schema
                        for col_name in merged_schema.names:
                            if col_name not in existing.column_names:
                                null_arr = pa.nulls(len(existing), type=pa.string())
                                existing = existing.append_column(col_name, null_arr)
                        writer.write_table(existing)
                        del existing
                        # Ecrire le chunk courant
                        for col_name in merged_schema.names:
                            if col_name not in table.column_names:
                                null_arr = pa.nulls(len(table), type=pa.string())
                                table = table.append_column(col_name, null_arr)
                        writer.write_table(table)

                del table, columns, records

                elapsed = time.time() - t0
                rate = total_rows / elapsed if elapsed > 0 else 0
                log.info("  Chunk %d: %d lignes totales | %.0fs | %.0f rec/s",
                         chunk_num, total_rows, elapsed, rate)

    finally:
        if writer is not None:
            writer.close()

    elapsed = time.time() - t0
    out_size = os.path.getsize(output_file) if os.path.exists(output_file) else 0

    log.info("")
    log.info("=" * 70)
    log.info("EXPORT TERMINE")
    log.info("  Total lignes: %d", total_rows)
    log.info("  Chunks ecrits: %d", chunk_num)
    log.info("  Colonnes: %d", len(known_columns))
    log.info("  Taille Parquet: %.2f GB", out_size / (1024 ** 3))
    log.info("  Ratio compression: %.1f%%", 100 * out_size / max(file_size, 1))
    log.info("  Duree: %.1fs", elapsed)
    log.info("  Output: %s", output_file)
    log.info("=" * 70)


if __name__ == "__main__":
    main()
