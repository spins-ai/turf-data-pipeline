#!/usr/bin/env python3
"""
json_to_jsonl.py — Convertit les gros fichiers JSON (array) en JSONL.
Utilise un parser streaming (ijson) pour ne pas exploser la RAM.
Fallback sur json standard pour les petits fichiers.

Usage:
  python json_to_jsonl.py                  # convertit tous les gros JSON
  python json_to_jsonl.py --file X.json    # convertit un fichier specifique
  python json_to_jsonl.py --min-size 100   # seuil en MB (defaut: 50)
  python json_to_jsonl.py --dry-run        # affiche ce qui serait converti
"""

import argparse
import json
import logging
import os
import sys
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal objects from ijson streaming parser."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_BASE = os.path.join(BASE_DIR, "output")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("json_to_jsonl")

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

try:
    import ijson
    HAS_IJSON = True
except ImportError:
    HAS_IJSON = False
    log.warning("ijson non installe — fallback json standard (attention RAM pour gros fichiers)")


def find_large_json_files(min_size_mb=50):
    """Trouve les fichiers JSON > min_size_mb dans output/."""
    results = []
    for root, dirs, files in os.walk(OUTPUT_BASE):
        # Skip cache dirs
        dirs[:] = [d for d in dirs if d != "cache"]
        for f in files:
            if not f.endswith(".json"):
                continue
            if f.startswith(".checkpoint"):
                continue
            path = os.path.join(root, f)
            size_mb = os.path.getsize(path) / (1024 * 1024)
            if size_mb >= min_size_mb:
                # Check if JSONL equivalent already exists
                jsonl_path = path.replace(".json", ".jsonl")
                results.append({
                    "path": path,
                    "size_mb": round(size_mb, 1),
                    "jsonl_exists": os.path.exists(jsonl_path),
                    "jsonl_path": jsonl_path,
                })
    return sorted(results, key=lambda x: -x["size_mb"])


def convert_streaming(json_path, jsonl_path):
    """Convertit un JSON array en JSONL via streaming (ijson)."""
    count = 0
    with open(json_path, "rb") as fin, \
         open(jsonl_path, "w", encoding="utf-8", newline="\n") as fout:
        # ijson.items parses each item in the root array one by one
        for record in ijson.items(fin, "item"):
            fout.write(json.dumps(record, ensure_ascii=False, cls=DecimalEncoder) + "\n")
            count += 1
            if count % 100000 == 0:
                log.info(f"    {count:,} records...")
    return count


def convert_standard(json_path, jsonl_path):
    """Convertit un JSON en JSONL via json standard (charge tout en RAM)."""
    log.warning(f"  Chargement complet en RAM: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        log.warning(f"  Fichier n'est pas un array JSON, skip: {json_path}")
        return 0

    count = 0
    with open(jsonl_path, "w", encoding="utf-8", newline="\n") as fout:
        for record in data:
            fout.write(json.dumps(record, ensure_ascii=False, cls=DecimalEncoder) + "\n")
            count += 1
    del data
    return count


def convert_file(json_path, jsonl_path, force=False):
    """Convertit un fichier JSON en JSONL."""
    if os.path.exists(jsonl_path) and not force:
        log.info(f"  JSONL existe deja: {jsonl_path}")
        return -1

    size_mb = os.path.getsize(json_path) / (1024 * 1024)
    log.info(f"  Conversion: {os.path.basename(json_path)} ({size_mb:.0f} MB)")

    if HAS_IJSON and size_mb > 500:
        # Streaming pour les gros fichiers
        count = convert_streaming(json_path, jsonl_path)
    else:
        if size_mb > 2000:
            log.error(f"  Fichier trop gros ({size_mb:.0f} MB) sans ijson. Installez: pip install ijson")
            return 0
        count = convert_standard(json_path, jsonl_path)

    log.info(f"  → {count:,} records → {os.path.basename(jsonl_path)}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Convertir JSON arrays en JSONL")
    parser.add_argument("--file", type=str, help="Fichier JSON specifique")
    parser.add_argument("--min-size", type=int, default=50, help="Taille min en MB (defaut: 50)")
    parser.add_argument("--dry-run", action="store_true", help="Affiche sans convertir")
    parser.add_argument("--force", action="store_true", help="Reconvertir meme si JSONL existe")
    parser.add_argument("--max-files", type=int, default=0, help="Nombre max de fichiers")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("JSON → JSONL Converter")
    log.info(f"  ijson: {'OUI' if HAS_IJSON else 'NON (fallback standard)'}")
    log.info("=" * 60)

    if args.file:
        path = args.file if os.path.isabs(args.file) else os.path.join(BASE_DIR, args.file)
        jsonl_path = path.replace(".json", ".jsonl")
        convert_file(path, jsonl_path, force=args.force)
        return

    files = find_large_json_files(args.min_size)
    log.info(f"\n{len(files)} fichiers JSON >= {args.min_size} MB trouvés:")
    for f in files:
        status = "JSONL existe" if f["jsonl_exists"] else "A convertir"
        log.info(f"  {f['size_mb']:>8.0f} MB  {status:15s}  {os.path.relpath(f['path'], BASE_DIR)}")

    if args.dry_run:
        return

    to_convert = [f for f in files if not f["jsonl_exists"] or args.force]
    if not to_convert:
        log.info("\nTous les fichiers ont deja un JSONL. Utilisez --force pour reconvertir.")
        return

    log.info(f"\nConversion de {len(to_convert)} fichiers...")
    converted = 0
    for f in to_convert:
        if args.max_files and converted >= args.max_files:
            break
        count = convert_file(f["path"], f["jsonl_path"], force=args.force)
        if count > 0:
            converted += 1

    log.info(f"\n{converted} fichiers convertis.")


if __name__ == "__main__":
    main()
