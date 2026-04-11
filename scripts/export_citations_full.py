"""
Export all cached citations/enjeux JSON files to a single JSONL file.
Covers 2013-2025 (177,700 files).
Streams one file at a time - max RAM usage is O(1 file).
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

CACHE_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/27_citations_enjeux/cache")
OUTPUT_FILE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/27_citations_enjeux/citations_enjeux_full.jsonl")
LOG_FILE = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/27_citations_enjeux/export_citations_full.log")


class Tee:
    """Write to both stdout and log file."""
    def __init__(self, log_path):
        self.log = open(log_path, "w", encoding="utf-8", buffering=1)
        self.stdout = sys.stdout
    def write(self, msg):
        self.stdout.write(msg)
        self.log.write(msg)
    def flush(self):
        self.stdout.flush()
        self.log.flush()

def parse_date_from_filename(filename):
    """Extract date from filename like 01012014_R1_C1.json -> datetime"""
    try:
        date_part = filename.split("_")[0]  # e.g. '01012014'
        return datetime.strptime(date_part, "%d%m%Y")
    except Exception:
        return None

def main():
    sys.stdout = Tee(LOG_FILE)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Export started")
    print(f"Scanning cache directory: {CACHE_DIR}")
    if not CACHE_DIR.exists():
        print(f"ERROR: Cache directory not found: {CACHE_DIR}")
        sys.exit(1)

    # List and sort all JSON files
    all_files = sorted(CACHE_DIR.glob("*.json"))
    total_files = len(all_files)
    print(f"Found {total_files:,} JSON files")

    if total_files == 0:
        print("No files found, exiting.")
        sys.exit(0)

    # Print 2 sample filenames
    print(f"First: {all_files[0].name}")
    print(f"Last:  {all_files[-1].name}")

    # Stats tracking
    valid_records = 0
    skipped_malformed = 0
    skipped_empty = 0
    min_date = None
    max_date = None

    print(f"\nWriting output to: {OUTPUT_FILE}")
    print("Processing...\n")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        for i, json_path in enumerate(all_files, 1):
            # Progress every 10,000 files
            if i % 10000 == 0 or i == 1 or i == total_files:
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] [{i:>7,} / {total_files:,}]  valid={valid_records:,}  skipped_malformed={skipped_malformed}  skipped_empty={skipped_empty}")

            # Parse date from filename
            file_date = parse_date_from_filename(json_path.name)

            # Read and parse JSON
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"  WARNING: Malformed JSON in {json_path.name}: {e}")
                skipped_malformed += 1
                continue
            except Exception as e:
                print(f"  WARNING: Could not read {json_path.name}: {e}")
                skipped_malformed += 1
                continue

            # Skip files with no useful data
            liste = data.get("listeCitations", [])
            if not liste:
                skipped_empty += 1
                continue

            # Check if any citation has actual participant data
            has_data = any(
                "participants" in c and len(c["participants"]) > 0
                for c in liste
            )
            if not has_data:
                skipped_empty += 1
                continue

            # Enrich with metadata from filename
            record = {
                "source_file": json_path.name,
                "date_str": json_path.name.split("_")[0] if "_" in json_path.name else json_path.stem,
                "date_parsed": file_date.strftime("%Y-%m-%d") if file_date else None,
                "data": data
            }

            # Track date range
            if file_date:
                if min_date is None or file_date < min_date:
                    min_date = file_date
                if max_date is None or file_date > max_date:
                    max_date = file_date

            # Write one JSON line
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
            valid_records += 1

    print(f"\n{'='*60}")
    print(f"EXPORT COMPLETE")
    print(f"{'='*60}")
    print(f"Total files processed : {total_files:,}")
    print(f"Valid records written  : {valid_records:,}")
    print(f"Skipped (malformed)    : {skipped_malformed:,}")
    print(f"Skipped (no data)      : {skipped_empty:,}")
    if min_date and max_date:
        print(f"Date range covered     : {min_date.strftime('%Y-%m-%d')} -> {max_date.strftime('%Y-%m-%d')}")
    else:
        print(f"Date range             : (unknown)")

    # Output file size
    out_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print(f"Output file size       : {out_size_mb:.1f} MB")
    print(f"Output file            : {OUTPUT_FILE}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
