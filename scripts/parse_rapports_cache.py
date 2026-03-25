#!/usr/bin/env python3
"""
Parse 21_rapports_definitifs cache (200K files).

Reads JSON files from output/21_rapports_definitifs/cache/
Extracts: nombreGagnants per bet type (market concentration signal).

Outputs to output/21_rapports_definitifs/rapports_enriched.jsonl

Usage:
    python scripts/parse_rapports_cache.py              # full run
    python scripts/parse_rapports_cache.py --sample 100 # sample first 100 files
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "output" / "21_rapports_definitifs" / "cache"
OUTPUT_FILE = ROOT / "output" / "21_rapports_definitifs" / "rapports_enriched.jsonl"


def parse_filename(filename: str):
    """Extract date, reunion, course from filename like 01012014_R1_C1.json."""
    base = filename.replace(".json", "")
    parts = base.split("_")
    if len(parts) != 3:
        return None, None, None
    date_str = parts[0]  # DDMMYYYY
    reunion = parts[1]   # R1
    course = parts[2]    # C1
    if len(date_str) == 8:
        iso_date = f"{date_str[4:8]}-{date_str[2:4]}-{date_str[0:2]}"
    else:
        iso_date = date_str
    return iso_date, reunion, course


def process_file(filepath: Path) -> list:
    """Process a single cache file and return enriched records."""
    records = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return records

    iso_date, reunion, course = parse_filename(filepath.name)
    if iso_date is None:
        return records

    # Data is a list of bet types
    if not isinstance(data, list):
        # Some files may wrap in a dict
        if isinstance(data, dict):
            data = data.get("rapports", data.get("data", []))
        if not isinstance(data, list):
            return records

    for bet in data:
        if not isinstance(bet, dict):
            continue

        type_pari = bet.get("typePari")
        mise_base = bet.get("miseBase")
        rembourse = bet.get("rembourse")
        audience = bet.get("audience")
        famille_pari = bet.get("famillePari")

        rapports = bet.get("rapports", [])
        if not isinstance(rapports, list):
            continue

        for rapport in rapports:
            if not isinstance(rapport, dict):
                continue

            record = {
                "source_file": filepath.name,
                "date_course": iso_date,
                "reunion": reunion,
                "course": course,
                # Bet type info
                "typePari": type_pari,
                "miseBase": mise_base,
                "rembourse": rembourse,
                "audience": audience,
                "famillePari": famille_pari,
                # Rapport details
                "libelle": rapport.get("libelle"),
                "dividende": rapport.get("dividende"),
                "dividendePourUnEuro": rapport.get("dividendePourUnEuro"),
                "combinaison": rapport.get("combinaison"),
                "nombreGagnants": rapport.get("nombreGagnants"),
                "dividendePourUneMiseDeBase": rapport.get("dividendePourUneMiseDeBase"),
                "dividendeUnite": rapport.get("dividendeUnite"),
            }
            records.append(record)

    return records


def main():
    parser = argparse.ArgumentParser(description="Parse 21_rapports_definitifs cache")
    parser.add_argument("--sample", type=int, default=0, help="Process only first N files (0=all)")
    args = parser.parse_args()

    if not CACHE_DIR.exists():
        print(f"ERROR: Cache directory not found: {CACHE_DIR}")
        sys.exit(1)

    files = sorted([f for f in CACHE_DIR.iterdir() if f.suffix == ".json"])
    total_files = len(files)
    print(f"Found {total_files} JSON files in cache")

    if args.sample > 0:
        files = files[: args.sample]
        print(f"Sample mode: processing first {len(files)} files")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    total_records = 0
    errors = 0
    non_null_ng = 0
    bet_types_seen = {}

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        for i, filepath in enumerate(files):
            try:
                records = process_file(filepath)
                for rec in records:
                    out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    total_records += 1
                    if rec.get("nombreGagnants") is not None:
                        non_null_ng += 1
                    tp = rec.get("typePari")
                    if tp:
                        bet_types_seen[tp] = bet_types_seen.get(tp, 0) + 1
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  ERROR on {filepath.name}: {e}")

            if (i + 1) % 10000 == 0 or (i + 1) == len(files):
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                print(
                    f"  [{i+1}/{len(files)}] {total_records:,} records | "
                    f"{rate:.0f} files/s | {elapsed:.1f}s | errors={errors}"
                )

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Files processed: {len(files)}")
    print(f"Total records: {total_records:,}")
    print(f"Errors: {errors}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Output size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"\nnombreGagnants coverage: {non_null_ng:,} / {total_records:,} ({100*non_null_ng/max(total_records,1):.1f}%)")
    print(f"\nBet types distribution:")
    for tp, count in sorted(bet_types_seen.items(), key=lambda x: -x[1]):
        print(f"  {tp}: {count:,}")


if __name__ == "__main__":
    main()
