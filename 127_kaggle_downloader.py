#!/usr/bin/env python3
"""
Script 127 -- Kaggle Horse Racing Dataset Downloader (CLI tool).
Downloads free horse racing datasets from Kaggle, saves to output/127_kaggle/,
and converts CSV files to JSONL format for pipeline integration.

NOT a web scraper -- uses the Kaggle API (pip install kaggle) or direct HTTP
download as fallback.

Known free datasets:
    gdaley/hk-horse-racing, hwaynhu/australian-horse-racing, etc.

PRE-REQUIS:
    pip install kaggle requests
    Place kaggle.json in ~/.kaggle/ (from kaggle.com -> Account -> API)

Usage:
    python 127_kaggle_downloader.py
    python 127_kaggle_downloader.py --no-download   # list-only mode
    python 127_kaggle_downloader.py --max-datasets 5
    python 127_kaggle_downloader.py --convert-only   # skip download, just CSV->JSONL
"""

import argparse
import csv
import glob
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
import zipfile
from datetime import datetime

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

SCRIPT_NAME = "127_kaggle"
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME
)
DOWNLOAD_DIR = os.path.join(OUTPUT_DIR, "downloads")
JSONL_DIR = os.path.join(OUTPUT_DIR, "jsonl")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(JSONL_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import load_checkpoint, save_checkpoint, append_jsonl

log = setup_logging("127_kaggle_downloader")


# ============================================================
# Known free Kaggle datasets -- horse racing
# ============================================================
KAGGLE_DATASETS = [
    # Hong Kong
    "gdaley/hk-horse-racing",
    "gdaley/hkracing",
    "lantanacamara/hong-kong-horse-racing",
    # Australia
    "hwaynhu/australian-horse-racing",
    "braquets/australian-horse-racing",
    "tokyoracer/horse-racing-results-australia",
    "alanvourch/horse-racing",
    # UK / Ireland
    "lukeclarke123/uk-horse-racing-results",
    "hwaitt/horse-racing",
    "zygmunt/horse-racing-in-the-uk",
    # US
    "ionaskel/kentucky-derby-winners",
    "bogdanbaraban/horse-racing",
    "jcaliz/us-horse-racing",
    "hawerroth/horse-racing-dataset",
    # France
    "faressalah/french-horse-racing",
    "olivierbour/french-horse-racing-pmu",
    # General / Multi-country
    "cprasad/horse-racing-results",
    "adamzakaria/horse-racing-data",
    "mattop/horse-racing-prediction",
    "drhoogstrate/horse-racing",
    "thedevastator/horse-racing-results",
    "sanjeetsinghnaik/horse-racing",
    "lemonkoala/horse-racing-data",
    # Betting / Odds
    "robjan/horse-racing-betting-data",
    "aldozeng/horse-racing-dataset",
    # Pedigree / Breeding
    "andrewgeorge/horse-pedigree",
    # Large compilations
    "paultimothymooney/horse-racing",
    "ailab2023/horse-racing-dataset",
]

# Keywords for discovering additional datasets
SEARCH_KEYWORDS = [
    "horse racing",
    "horse racing results",
    "horse racing betting",
    "horse racing prediction",
    "thoroughbred racing",
    "harness racing",
    "horse pedigree",
    "PMU courses hippiques",
]


# ------------------------------------------------------------------
# Kaggle CLI helpers
# ------------------------------------------------------------------

def check_kaggle_cli():
    """Check if the Kaggle CLI is installed and configured."""
    try:
        result = subprocess.run(
            ["kaggle", "--version"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            log.info("  Kaggle CLI: %s", result.stdout.strip())
            return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    log.warning("  Kaggle CLI not available. Install: pip install kaggle")
    log.warning("  Config: place kaggle.json in ~/.kaggle/")
    return False


def search_kaggle_datasets(keyword, max_results=20):
    """Search for datasets via the Kaggle CLI."""
    try:
        result = subprocess.run(
            [
                "kaggle", "datasets", "list",
                "-s", keyword,
                "--sort-by", "relevance",
                "--max-size", "10737418240",
                "--csv",
            ],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            log.warning("  Search failed: %s", result.stderr.strip()[:200])
            return []

        datasets = []
        lines = result.stdout.strip().split("\n")
        if len(lines) < 2:
            return []

        for line in lines[1:max_results + 1]:
            parts = line.split(",")
            if len(parts) >= 2:
                datasets.append(parts[0])

        return datasets

    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        log.warning("  Search error: %s", exc)
        return []


# ------------------------------------------------------------------
# Download functions
# ------------------------------------------------------------------

def download_via_cli(dataset_ref, target_dir):
    """Download a Kaggle dataset using the CLI."""
    ds_dir = os.path.join(target_dir, dataset_ref.replace("/", "_"))
    marker = os.path.join(ds_dir, ".download_complete")

    if os.path.exists(marker):
        log.info("    Already downloaded: %s", dataset_ref)
        return ds_dir

    os.makedirs(ds_dir, exist_ok=True)

    try:
        # Try with --unzip first
        result = subprocess.run(
            ["kaggle", "datasets", "download", "-d", dataset_ref,
             "-p", ds_dir, "--unzip"],
            capture_output=True, text=True, timeout=600,
        )

        if result.returncode != 0:
            # Fallback without --unzip
            result = subprocess.run(
                ["kaggle", "datasets", "download", "-d", dataset_ref,
                 "-p", ds_dir],
                capture_output=True, text=True, timeout=600,
            )

            if result.returncode != 0:
                log.warning(
                    "    CLI download failed for %s: %s",
                    dataset_ref, result.stderr.strip()[:200],
                )
                return None

            # Manual unzip
            for zf in glob.glob(os.path.join(ds_dir, "*.zip")):
                try:
                    with zipfile.ZipFile(zf, "r") as z:
                        z.extractall(ds_dir)
                    os.remove(zf)
                except zipfile.BadZipFile:
                    log.warning("    Bad ZIP: %s", zf)

        with open(marker, "w", encoding="utf-8") as f:
            f.write(datetime.now().isoformat())

        log.info("    OK (CLI): %s", dataset_ref)
        return ds_dir

    except subprocess.TimeoutExpired:
        log.warning("    Timeout downloading %s", dataset_ref)
        return None
    except Exception as exc:
        log.warning("    CLI download error for %s: %s", dataset_ref, exc)
        return None


def download_via_http(dataset_ref, target_dir):
    """Fallback: download via direct HTTP (requires kaggle.json credentials)."""
    if requests is None:
        log.warning("    requests library not installed, cannot use HTTP fallback")
        return None

    ds_dir = os.path.join(target_dir, dataset_ref.replace("/", "_"))
    marker = os.path.join(ds_dir, ".download_complete")

    if os.path.exists(marker):
        log.info("    Already downloaded: %s", dataset_ref)
        return ds_dir

    os.makedirs(ds_dir, exist_ok=True)

    url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_ref}"
    zip_path = os.path.join(ds_dir, "dataset.zip")

    try:
        # Load credentials
        kaggle_json = os.path.expanduser("~/.kaggle/kaggle.json")
        auth = None
        if os.path.exists(kaggle_json):
            with open(kaggle_json, encoding="utf-8") as f:
                creds = json.load(f)
            from requests.auth import HTTPBasicAuth
            auth = HTTPBasicAuth(creds.get("username", ""), creds.get("key", ""))
        else:
            log.warning("    No Kaggle credentials, trying without auth...")

        resp = requests.get(url, auth=auth, stream=True, timeout=300)
        if resp.status_code != 200:
            log.warning(
                "    HTTP %d for %s", resp.status_code, dataset_ref,
            )
            return None

        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        # Unzip
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(ds_dir)
            os.remove(zip_path)
        except zipfile.BadZipFile:
            log.warning("    Invalid ZIP for %s", dataset_ref)

        with open(marker, "w", encoding="utf-8") as f:
            f.write(datetime.now().isoformat())

        log.info("    OK (HTTP): %s", dataset_ref)
        return ds_dir

    except Exception as exc:
        log.warning("    HTTP download error for %s: %s", dataset_ref, exc)
        return None


# ------------------------------------------------------------------
# Inventory & CSV -> JSONL conversion
# ------------------------------------------------------------------

def inventory_dataset(ds_dir, dataset_ref):
    """Inventory the contents of a downloaded dataset."""
    inventory = {
        "source": "kaggle",
        "dataset_ref": dataset_ref,
        "type": "dataset_inventory",
        "scraped_at": datetime.now().isoformat(),
        "files": [],
        "total_size_bytes": 0,
        "total_rows_estimate": 0,
    }

    if not ds_dir or not os.path.exists(ds_dir):
        return inventory

    for root, dirs, files in os.walk(ds_dir):
        for fname in files:
            if fname.startswith("."):
                continue
            fpath = os.path.join(root, fname)
            fsize = os.path.getsize(fpath)
            rel_path = os.path.relpath(fpath, ds_dir)

            file_info = {
                "path": rel_path,
                "size_bytes": fsize,
                "extension": os.path.splitext(fname)[1].lower(),
            }

            # Count lines for CSV/TSV
            if file_info["extension"] in (".csv", ".tsv", ".txt"):
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        line_count = sum(1 for _ in f)
                    file_info["line_count"] = line_count
                    inventory["total_rows_estimate"] += max(0, line_count - 1)
                except Exception as exc:
                    log.debug("  Line count error %s: %s", fpath, exc)

            # Read CSV headers
            if file_info["extension"] == ".csv":
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        header = f.readline().strip()
                    file_info["columns"] = header.split(",")[:30]
                except Exception as exc:
                    log.debug("  Header read error %s: %s", fpath, exc)

            inventory["files"].append(file_info)
            inventory["total_size_bytes"] += fsize

    return inventory


def convert_csv_to_jsonl(ds_dir, dataset_ref, jsonl_dir):
    """Convert all CSV files in a dataset directory to JSONL format.

    Returns the number of rows converted.
    """
    if not ds_dir or not os.path.exists(ds_dir):
        return 0

    total_rows = 0
    ds_slug = dataset_ref.replace("/", "_")

    for root, dirs, files in os.walk(ds_dir):
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in (".csv", ".tsv"):
                continue

            fpath = os.path.join(root, fname)
            rel_name = os.path.splitext(fname)[0]
            # Sanitize filename
            safe_name = re.sub(r"[^a-zA-Z0-9_\-]", "_", rel_name)
            jsonl_file = os.path.join(jsonl_dir, f"{ds_slug}__{safe_name}.jsonl")

            # Skip if already converted and source unchanged
            if os.path.exists(jsonl_file):
                src_mtime = os.path.getmtime(fpath)
                dst_mtime = os.path.getmtime(jsonl_file)
                if dst_mtime >= src_mtime:
                    log.info("    Skip (up-to-date): %s", fname)
                    continue

            log.info("    Converting: %s -> %s", fname, os.path.basename(jsonl_file))

            delimiter = "\t" if ext == ".tsv" else ","
            row_count = 0

            try:
                with open(fpath, "r", encoding="utf-8", errors="replace") as fin:
                    reader = csv.DictReader(fin, delimiter=delimiter)
                    with open(jsonl_file, "w", encoding="utf-8", newline="\n") as fout:
                        for row in reader:
                            # Add metadata
                            record = {
                                "_source": "kaggle",
                                "_dataset": dataset_ref,
                                "_file": fname,
                            }
                            # Clean keys and values
                            for k, v in row.items():
                                if k is None:
                                    continue
                                clean_key = (
                                    k.strip()
                                    .lower()
                                    .replace(" ", "_")
                                    .replace(".", "_")
                                    .replace("-", "_")
                                )
                                if not clean_key:
                                    continue
                                # Try numeric conversion
                                if v is not None:
                                    v = v.strip()
                                    try:
                                        if "." in v:
                                            record[clean_key] = float(v)
                                        else:
                                            record[clean_key] = int(v)
                                    except (ValueError, TypeError):
                                        record[clean_key] = v
                                else:
                                    record[clean_key] = None

                            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                            row_count += 1

                total_rows += row_count
                log.info(
                    "    Converted %d rows: %s", row_count, os.path.basename(jsonl_file),
                )

            except Exception as exc:
                log.warning("    Conversion error %s: %s", fname, str(exc)[:200])

    return total_rows


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 127 -- Kaggle Horse Racing Dataset Downloader & Converter"
    )
    parser.add_argument(
        "--search", action="store_true", default=True,
        help="Search for additional datasets beyond the known list",
    )
    parser.add_argument(
        "--no-download", action="store_true", default=False,
        help="List-only mode, do not download",
    )
    parser.add_argument(
        "--convert-only", action="store_true", default=False,
        help="Skip download, only convert existing CSV to JSONL",
    )
    parser.add_argument(
        "--max-datasets", type=int, default=100,
        help="Maximum number of datasets to download",
    )
    parser.add_argument(
        "--resume", action="store_true", default=True,
        help="Resume from last checkpoint",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 127 -- Kaggle Horse Racing Dataset Downloader")
    log.info("  Known datasets : %d", len(KAGGLE_DATASETS))
    log.info("  Search additional : %s", args.search)
    log.info("  Download : %s", not args.no_download)
    log.info("  Convert-only : %s", args.convert_only)
    log.info("  Output : %s", OUTPUT_DIR)
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    has_cli = check_kaggle_cli()
    output_file = os.path.join(OUTPUT_DIR, "kaggle_datasets.jsonl")

    # --- Convert-only mode ---
    if args.convert_only:
        log.info("--- Convert-only mode: CSV -> JSONL ---")
        total_converted = 0
        for ds_dir_name in os.listdir(DOWNLOAD_DIR):
            ds_dir = os.path.join(DOWNLOAD_DIR, ds_dir_name)
            if not os.path.isdir(ds_dir):
                continue
            dataset_ref = ds_dir_name.replace("_", "/", 1)
            rows = convert_csv_to_jsonl(ds_dir, dataset_ref, JSONL_DIR)
            total_converted += rows

        log.info("=" * 60)
        log.info("DONE (convert-only): %d total rows -> %s", total_converted, JSONL_DIR)
        log.info("=" * 60)
        return

    # --- Build full dataset list ---
    all_datasets = list(KAGGLE_DATASETS)

    if args.search and has_cli:
        log.info("--- Phase 1: Searching for additional datasets ---")
        for keyword in SEARCH_KEYWORDS:
            log.info("  Searching: '%s'", keyword)
            found = search_kaggle_datasets(keyword)
            for ds in found:
                if ds not in all_datasets:
                    all_datasets.append(ds)
                    log.info("    + %s", ds)
            time.sleep(2)

    log.info("  Total datasets to process: %d", len(all_datasets))

    # Checkpoint state
    downloaded = set(checkpoint.get("downloaded", []))
    failed = set(checkpoint.get("failed", []))
    total_records = checkpoint.get("total_records", 0)
    total_converted_rows = checkpoint.get("total_converted_rows", 0)

    ds_count = 0
    ok_count = 0
    fail_count = 0

    # --- Phase 2: Download ---
    log.info("--- Phase 2: Download datasets ---")
    for dataset_ref in all_datasets:
        if ds_count >= args.max_datasets:
            log.info("  Limit of %d reached", args.max_datasets)
            break

        if dataset_ref in downloaded:
            log.info("  Skip (already done): %s", dataset_ref)
            continue

        log.info("  [%d/%d] %s", ds_count + 1, len(all_datasets), dataset_ref)

        if args.no_download:
            record = {
                "source": "kaggle",
                "dataset_ref": dataset_ref,
                "type": "dataset_listing",
                "url": f"https://www.kaggle.com/datasets/{dataset_ref}",
                "scraped_at": datetime.now().isoformat(),
            }
            append_jsonl(output_file, record)
            total_records += 1
        else:
            # Try CLI first, then HTTP fallback
            ds_dir = None
            if has_cli:
                ds_dir = download_via_cli(dataset_ref, DOWNLOAD_DIR)
            if not ds_dir:
                ds_dir = download_via_http(dataset_ref, DOWNLOAD_DIR)

            if ds_dir:
                # Inventory
                inventory = inventory_dataset(ds_dir, dataset_ref)
                append_jsonl(output_file, inventory)
                total_records += 1

                # CSV -> JSONL conversion
                rows = convert_csv_to_jsonl(ds_dir, dataset_ref, JSONL_DIR)
                total_converted_rows += rows

                downloaded.add(dataset_ref)
                ok_count += 1
            else:
                failed.add(dataset_ref)
                fail_count += 1
                append_jsonl(output_file, {
                    "source": "kaggle",
                    "dataset_ref": dataset_ref,
                    "type": "download_failed",
                    "scraped_at": datetime.now().isoformat(),
                })
                total_records += 1

        ds_count += 1

        if ds_count % 5 == 0:
            save_checkpoint(CHECKPOINT_FILE, {
                "downloaded": list(downloaded),
                "failed": list(failed),
                "total_records": total_records,
                "total_converted_rows": total_converted_rows,
            })
            log.info(
                "  Progress: %d processed, %d OK, %d failed, %d rows converted",
                ds_count, ok_count, fail_count, total_converted_rows,
            )

        # Pause between downloads
        time.sleep(random.uniform(2, 5))

    save_checkpoint(CHECKPOINT_FILE, {
        "downloaded": list(downloaded),
        "failed": list(failed),
        "total_records": total_records,
        "total_converted_rows": total_converted_rows,
        "status": "done",
    })

    log.info("=" * 60)
    log.info("DONE: %d datasets processed", ds_count)
    log.info("  OK: %d | Failed: %d", ok_count, fail_count)
    log.info("  Inventory records: %d -> %s", total_records, output_file)
    log.info("  Converted rows: %d -> %s", total_converted_rows, JSONL_DIR)
    log.info("  Downloads: %s", DOWNLOAD_DIR)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
