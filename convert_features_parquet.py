#!/usr/bin/env python3
"""
convert_features_parquet.py
===========================

Convertit les 11 fichiers features JSONL (~253 GB total) en format Parquet
compressé (zstd). Le Parquet est ~5-10x plus petit et beaucoup plus rapide
à lire pour le ML (colonnar, compression par colonne, predicate pushdown).

Fichiers traités :
  output/features/*.jsonl  (11 fichiers, ~18-36 GB chacun)

Sortie :
  output/features/*.parquet  (même nom, extension changée)

Streaming par chunks de 50 000 lignes pour limiter la RAM.

Dépendances :
  pip install pyarrow pandas

Usage:
    python convert_features_parquet.py
    python convert_features_parquet.py --file interaction_features.jsonl  # un seul
    python convert_features_parquet.py --chunk-size 100000  # plus gros chunks
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERREUR: pyarrow non installé. Exécuter: pip install pyarrow")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERREUR: pandas non installé. Exécuter: pip install pandas")
    sys.exit(1)


BASE_DIR = Path(__file__).resolve().parent
FEATURES_DIR = BASE_DIR / "output" / "features"

# Fichiers à convertir (tous les .jsonl dans features/)
DEFAULT_FILES = [
    "class_change_features.jsonl",
    "combo_features.jsonl",
    "equipement_features.jsonl",
    "features_matrix.jsonl",
    "interaction_features.jsonl",
    "meteo_features.jsonl",
    "musique_features.jsonl",
    "poids_features.jsonl",
    "precomputed_entity_features.jsonl",
    "precomputed_partant_features.jsonl",
    "profil_cheval_features.jsonl",
    "temps_features.jsonl",
]


def infer_parquet_schema(jsonl_path: Path, sample_lines: int = 1000) -> list[str]:
    """Lit les premières lignes pour déduire les colonnes."""
    all_keys = set()
    count = 0
    with open(jsonl_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                all_keys.update(rec.keys())
                count += 1
                if count >= sample_lines:
                    break
            except json.JSONDecodeError:
                continue
    return sorted(all_keys)


def align_table_to_schema(table, target_schema):
    """Align a table to match target schema: reorder columns, add missing as null, drop extra."""
    columns = []
    for field in target_schema:
        if field.name in table.schema.names:
            col = table.column(field.name)
            # Cast if types differ
            if col.type != field.type:
                try:
                    col = col.cast(field.type)
                except (pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError):
                    col = pa.nulls(len(table), type=field.type)
            columns.append(col)
        else:
            # Missing column: fill with nulls
            columns.append(pa.nulls(len(table), type=field.type))
    return pa.table(columns, schema=target_schema)


def sanitize_df_for_arrow(df):
    """Convert columns with mixed types (dict/list mixed with scalars) to JSON strings.

    pa.Table.from_pandas() crashes on columns containing both dicts and strings.
    Scans all object columns and converts any dict/list values to JSON strings.
    """
    for col in df.columns:
        if df[col].dtype == object:
            # Check if ANY value in the column is a dict or list
            mask = df[col].apply(lambda x: isinstance(x, (dict, list)))
            if mask.any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
                )
    return df


def convert_one_file(jsonl_path: Path, parquet_path: Path, chunk_size: int = 50_000):
    """Convertit un fichier JSONL en Parquet par chunks."""
    fname = jsonl_path.name
    file_size_gb = jsonl_path.stat().st_size / (1024**3)
    print(f"\n{'='*60}")
    print(f"  {fname}  ({file_size_gb:.1f} GB)")
    print(f"{'='*60}")

    t0 = time.time()

    # Phase 1: déduire le schéma
    print(f"  Phase 1: Inférence schéma (1000 premières lignes) ...")
    columns = infer_parquet_schema(jsonl_path)
    print(f"    -> {len(columns)} colonnes détectées")

    # Phase 2: conversion par chunks
    print(f"  Phase 2: Conversion par chunks de {chunk_size:,} lignes ...")

    parquet_tmp = parquet_path.with_suffix(".parquet.tmp")
    writer = None
    total_rows = 0
    chunk_num = 0
    chunk_buffer = []

    with open(jsonl_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                chunk_buffer.append(rec)
            except json.JSONDecodeError:
                continue

            if len(chunk_buffer) >= chunk_size:
                chunk_num += 1
                df = pd.DataFrame(chunk_buffer)
                sanitize_df_for_arrow(df)
                try:
                    table = pa.Table.from_pandas(df, preserve_index=False)
                except (pa.lib.ArrowTypeError, pa.lib.ArrowInvalid) as e:
                    # Fallback: force all object columns to string
                    for c in df.columns:
                        if df[c].dtype == object:
                            df[c] = df[c].apply(
                                lambda x: json.dumps(x, ensure_ascii=False)
                                if isinstance(x, (dict, list)) else
                                (str(x) if x is not None else None)
                            )
                    table = pa.Table.from_pandas(df, preserve_index=False)

                if writer is None:
                    writer_schema = table.schema
                    writer = pq.ParquetWriter(
                        str(parquet_tmp),
                        writer_schema,
                        compression="zstd",
                        compression_level=3,
                    )

                try:
                    # Align table to writer schema: reorder, add missing, drop extra
                    aligned = align_table_to_schema(table, writer_schema)
                    writer.write_table(aligned)
                except (ValueError, pa.lib.ArrowInvalid) as e:
                    print(f"    [WARN] Schema align failed chunk {chunk_num}: {e}")
                    # Skip this chunk rather than crash
                    pass

                total_rows += len(chunk_buffer)
                chunk_buffer = []

                elapsed = time.time() - t0
                rate = total_rows / elapsed if elapsed > 0 else 0
                print(f"    Chunk {chunk_num}: {total_rows:>10,} lignes  "
                      f"[{rate:.0f} rec/s]", flush=True)

    # Dernier chunk
    if chunk_buffer:
        chunk_num += 1
        df = pd.DataFrame(chunk_buffer)
        sanitize_df_for_arrow(df)
        try:
            table = pa.Table.from_pandas(df, preserve_index=False)
        except (pa.lib.ArrowTypeError, pa.lib.ArrowInvalid) as e:
            for c in df.columns:
                if df[c].dtype == object:
                    df[c] = df[c].apply(
                        lambda x: json.dumps(x, ensure_ascii=False)
                        if isinstance(x, (dict, list)) else
                        (str(x) if x is not None else None)
                    )
            table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:
            writer_schema = table.schema
            writer = pq.ParquetWriter(
                str(parquet_tmp),
                writer_schema,
                compression="zstd",
                compression_level=3,
            )

        try:
            aligned = align_table_to_schema(table, writer_schema)
            writer.write_table(aligned)
        except (ValueError, pa.lib.ArrowInvalid) as e:
            print(f"    [WARN] Schema align failed last chunk: {e}")
            pass

        total_rows += len(chunk_buffer)

    if writer is not None:
        writer.close()

    # Remplacement atomique
    if parquet_tmp.exists():
        os.replace(str(parquet_tmp), str(parquet_path))

    elapsed = time.time() - t0
    parquet_size_gb = parquet_path.stat().st_size / (1024**3) if parquet_path.exists() else 0
    ratio = file_size_gb / parquet_size_gb if parquet_size_gb > 0 else 0

    print(f"\n  Résultat: {fname}")
    print(f"    JSONL:   {file_size_gb:.2f} GB")
    print(f"    Parquet: {parquet_size_gb:.2f} GB")
    print(f"    Ratio:   {ratio:.1f}x plus petit")
    print(f"    Lignes:  {total_rows:,}")
    print(f"    Temps:   {elapsed:.1f}s")

    return {
        "file": fname,
        "rows": total_rows,
        "jsonl_gb": round(file_size_gb, 2),
        "parquet_gb": round(parquet_size_gb, 2),
        "ratio": round(ratio, 1),
        "elapsed_s": round(elapsed, 1),
    }


def main():
    parser = argparse.ArgumentParser(description="Convertir features JSONL -> Parquet")
    parser.add_argument("--file", type=str, default=None,
                        help="Convertir un seul fichier (nom du .jsonl)")
    parser.add_argument("--chunk-size", type=int, default=50_000,
                        help="Nombre de lignes par chunk (défaut: 50000)")
    args = parser.parse_args()

    t0 = time.time()
    print("=" * 60)
    print("CONVERSION FEATURES JSONL -> PARQUET (zstd)")
    print("=" * 60)

    if not FEATURES_DIR.exists():
        print(f"ERREUR: {FEATURES_DIR} introuvable")
        sys.exit(1)

    # Déterminer les fichiers à traiter
    if args.file:
        files = [args.file]
    else:
        files = [f for f in DEFAULT_FILES if (FEATURES_DIR / f).exists()]
        # Aussi les JSONL non listés
        for f in os.listdir(FEATURES_DIR):
            if f.endswith(".jsonl") and f not in files:
                files.append(f)

    print(f"\nFichiers à convertir : {len(files)}")
    for f in files:
        size = (FEATURES_DIR / f).stat().st_size / (1024**3) if (FEATURES_DIR / f).exists() else 0
        print(f"  - {f}  ({size:.1f} GB)")

    # Conversion
    results = []
    for fname in files:
        jsonl_path = FEATURES_DIR / fname
        if not jsonl_path.exists():
            print(f"\n  [SKIP] {fname} — fichier introuvable")
            continue
        parquet_path = FEATURES_DIR / fname.replace(".jsonl", ".parquet")
        result = convert_one_file(jsonl_path, parquet_path, chunk_size=args.chunk_size)
        results.append(result)

    # Rapport final
    elapsed_total = time.time() - t0
    print("\n" + "=" * 60)
    print("RAPPORT FINAL")
    print("=" * 60)
    print(f"\n{'Fichier':<40s} {'Lignes':>12s} {'JSONL':>8s} {'Parquet':>8s} {'Ratio':>6s}")
    print("-" * 80)

    total_jsonl = 0
    total_parquet = 0
    total_rows = 0
    for r in results:
        print(f"  {r['file']:<38s} {r['rows']:>12,} {r['jsonl_gb']:>7.1f}G "
              f"{r['parquet_gb']:>7.1f}G {r['ratio']:>5.1f}x")
        total_jsonl += r["jsonl_gb"]
        total_parquet += r["parquet_gb"]
        total_rows += r["rows"]

    print("-" * 80)
    total_ratio = total_jsonl / total_parquet if total_parquet > 0 else 0
    print(f"  {'TOTAL':<38s} {total_rows:>12,} {total_jsonl:>7.1f}G "
          f"{total_parquet:>7.1f}G {total_ratio:>5.1f}x")
    print(f"\nTemps total : {elapsed_total:.1f}s ({elapsed_total/60:.1f} min)")
    print("Terminé.")


if __name__ == "__main__":
    main()
