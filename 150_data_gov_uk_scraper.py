#!/usr/bin/env python3
"""
Script 150 — data.gov.uk Horse Racing Datasets Downloader
Source : data.gov.uk
Collecte : Horse racing datasets (CSV), converted to JSONL
Approach :
  1. Search data.gov.uk CKAN API for "horse racing" datasets
  2. Download available CSV/JSON resources
  3. Convert CSVs to JSONL format
NOTE: This is a downloader, not a web scraper.

Requires:
    pip install requests
"""

import argparse
import csv
import io
import json
import os
import sys
import time
from datetime import datetime

SCRIPT_NAME = "150_data_gov_uk"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
DOWNLOADS_DIR = os.path.join(OUTPUT_DIR, "downloads")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import (
    smart_pause,
    append_jsonl,
    load_checkpoint,
    save_checkpoint,
    create_session,
    fetch_with_retry,
)

log = setup_logging("150_data_gov_uk")

CKAN_API_URL = "https://data.gov.uk/api/action"
SEARCH_QUERIES = [
    "horse racing",
    "horse race results",
    "horseracing",
    "equine racing",
    "thoroughbred",
    "racing results",
    "betting levy",
]
MAX_DATASETS_PER_QUERY = 50
MAX_DOWNLOAD_SIZE_MB = 100  # Skip files larger than this


# ------------------------------------------------------------------
# CKAN API helpers
# ------------------------------------------------------------------

def search_datasets(session, query, rows=MAX_DATASETS_PER_QUERY, start=0):
    """Search CKAN for datasets matching query. Returns list of dataset dicts."""
    url = f"{CKAN_API_URL}/package_search"
    params = {"q": query, "rows": rows, "start": start}
    resp = fetch_with_retry(session, url, params=params, logger=log)
    if resp is None:
        return []
    try:
        data = resp.json()
        if data.get("success"):
            return data.get("result", {}).get("results", [])
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("  JSON decode error searching '%s': %s", query, e)
    return []


def get_dataset_details(session, dataset_id):
    """Get full details for a dataset by ID."""
    url = f"{CKAN_API_URL}/package_show"
    params = {"id": dataset_id}
    resp = fetch_with_retry(session, url, params=params, logger=log)
    if resp is None:
        return None
    try:
        data = resp.json()
        if data.get("success"):
            return data.get("result", {})
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("  JSON decode error for dataset %s: %s", dataset_id, e)
    return None


# ------------------------------------------------------------------
# Download & conversion
# ------------------------------------------------------------------

def download_resource(session, resource_url, filename):
    """Download a resource file. Returns local filepath or None."""
    filepath = os.path.join(DOWNLOADS_DIR, filename)
    if os.path.exists(filepath):
        log.info("    Already downloaded: %s", filename)
        return filepath

    try:
        resp = session.get(resource_url, timeout=120, stream=True)
        if resp.status_code != 200:
            log.warning("    HTTP %d downloading %s", resp.status_code, resource_url[:100])
            return None

        # Check content length
        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > MAX_DOWNLOAD_SIZE_MB * 1024 * 1024:
            log.warning("    File too large (%s bytes), skipping: %s",
                        content_length, filename)
            return None

        with open(filepath, "wb") as f:
            total = 0
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                total += len(chunk)
                if total > MAX_DOWNLOAD_SIZE_MB * 1024 * 1024:
                    log.warning("    Download exceeded %dMB, truncating: %s",
                                MAX_DOWNLOAD_SIZE_MB, filename)
                    break

        log.info("    Downloaded: %s (%d bytes)", filename, total)
        return filepath

    except Exception as e:
        log.warning("    Download error for %s: %s", resource_url[:100], str(e)[:200])
        return None


def csv_to_jsonl(csv_filepath, jsonl_filepath, dataset_meta=None):
    """Convert a CSV file to JSONL format. Returns number of records written."""
    meta = dataset_meta or {}
    count = 0

    # Try different encodings
    for encoding in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
        try:
            with open(csv_filepath, "r", encoding=encoding, errors="replace") as f:
                # Sniff delimiter
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                except csv.Error:
                    dialect = csv.excel

                reader = csv.DictReader(f, dialect=dialect)
                for row in reader:
                    record = {
                        "source": "data_gov_uk",
                        "type": "dataset_row",
                        "dataset_id": meta.get("id", ""),
                        "dataset_title": meta.get("title", ""),
                        "scraped_at": datetime.now().isoformat(),
                    }
                    # Clean and add CSV columns
                    for key, val in row.items():
                        if key is not None:
                            clean_key = str(key).strip().lower().replace(" ", "_").replace(".", "")
                            record[clean_key] = val.strip() if val else ""
                    append_jsonl(jsonl_filepath, record)
                    count += 1
            break  # Success, stop trying encodings
        except UnicodeDecodeError:
            continue
        except Exception as e:
            log.warning("    Error reading CSV %s with %s: %s",
                        csv_filepath, encoding, str(e)[:200])
            break

    return count


def json_to_jsonl(json_filepath, jsonl_filepath, dataset_meta=None):
    """Convert a JSON file to JSONL format. Returns number of records written."""
    meta = dataset_meta or {}
    count = 0
    try:
        with open(json_filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        items = data if isinstance(data, list) else [data]
        for item in items:
            record = {
                "source": "data_gov_uk",
                "type": "dataset_row",
                "dataset_id": meta.get("id", ""),
                "dataset_title": meta.get("title", ""),
                "scraped_at": datetime.now().isoformat(),
            }
            if isinstance(item, dict):
                record.update(item)
            else:
                record["data"] = item
            append_jsonl(jsonl_filepath, record)
            count += 1
    except Exception as e:
        log.warning("    Error reading JSON %s: %s", json_filepath, str(e)[:200])
    return count


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 150 — data.gov.uk Horse Racing Datasets Downloader"
    )
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-datasets", type=int, default=0,
                        help="Max datasets to process (0=unlimited)")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip downloading, only convert existing files")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 150 — data.gov.uk Horse Racing Datasets Downloader")
    log.info("  Search queries: %s", SEARCH_QUERIES)
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    processed_ids = set(checkpoint.get("processed_dataset_ids", []))
    if args.resume:
        log.info("  Already processed: %d datasets", len(processed_ids))

    output_file = os.path.join(OUTPUT_DIR, "data_gov_uk_racing.jsonl")

    session = create_session()
    session.headers.update({
        "Accept": "application/json",
        "Accept-Language": "en-GB,en;q=0.9",
    })

    # Phase 1: Search for datasets
    all_datasets = {}
    for query in SEARCH_QUERIES:
        log.info("  Searching: '%s'", query)
        datasets = search_datasets(session, query)
        for ds in datasets:
            ds_id = ds.get("id") or ds.get("name")
            if ds_id and ds_id not in all_datasets:
                all_datasets[ds_id] = ds
        smart_pause(1.0, 0.5)

    log.info("  Found %d unique datasets", len(all_datasets))

    # Save dataset catalog
    catalog_file = os.path.join(CACHE_DIR, "dataset_catalog.json")
    with open(catalog_file, "w", encoding="utf-8") as f:
        json.dump(list(all_datasets.values()), f, ensure_ascii=False, indent=2)

    # Write catalog entries to JSONL
    for ds_id, ds in all_datasets.items():
        catalog_record = {
            "source": "data_gov_uk",
            "type": "dataset_catalog",
            "dataset_id": ds_id,
            "title": ds.get("title", ""),
            "notes": (ds.get("notes", "") or "")[:500],
            "organization": (ds.get("organization", {}) or {}).get("title", ""),
            "num_resources": ds.get("num_resources", 0),
            "metadata_created": ds.get("metadata_created", ""),
            "metadata_modified": ds.get("metadata_modified", ""),
            "scraped_at": datetime.now().isoformat(),
        }
        append_jsonl(output_file, catalog_record)

    # Phase 2: Download and convert resources
    dataset_count = 0
    total_records = 0

    for ds_id, ds in all_datasets.items():
        if args.max_datasets and dataset_count >= args.max_datasets:
            break
        if ds_id in processed_ids:
            log.info("  Skipping already processed: %s", ds.get("title", ds_id)[:60])
            continue

        log.info("  Processing dataset: %s", ds.get("title", ds_id)[:60])

        # Get full dataset details
        details = get_dataset_details(session, ds_id)
        if not details:
            details = ds
        smart_pause(1.0, 0.5)

        resources = details.get("resources", [])
        dataset_meta = {
            "id": ds_id,
            "title": details.get("title", ""),
        }

        for res in resources:
            res_url = res.get("url", "")
            res_format = (res.get("format", "") or "").lower()
            res_name = res.get("name", "") or res.get("description", "") or "resource"

            if not res_url:
                continue

            # Only process CSV and JSON resources
            if res_format not in ("csv", "json", "geojson", "xls", "xlsx"):
                if not any(ext in res_url.lower() for ext in [".csv", ".json"]):
                    continue

            if args.skip_download:
                continue

            # Sanitize filename
            safe_name = "".join(c if c.isalnum() or c in "-_." else "_"
                                for c in res_name[:50])
            ext = ".csv" if "csv" in res_format else ".json" if "json" in res_format else ""
            if not ext:
                if ".csv" in res_url.lower():
                    ext = ".csv"
                elif ".json" in res_url.lower():
                    ext = ".json"
                else:
                    ext = ".csv"  # default assumption

            filename = f"{ds_id[:30]}_{safe_name}{ext}"
            filepath = download_resource(session, res_url, filename)

            if filepath:
                # Convert to JSONL
                if ext == ".csv" or filepath.endswith(".csv"):
                    count = csv_to_jsonl(filepath, output_file, dataset_meta)
                elif ext == ".json" or filepath.endswith(".json"):
                    count = json_to_jsonl(filepath, output_file, dataset_meta)
                else:
                    count = 0

                total_records += count
                if count > 0:
                    log.info("    Converted %d records from %s", count, filename)

            smart_pause(2.0, 1.0)

        processed_ids.add(ds_id)
        dataset_count += 1

        if dataset_count % 5 == 0:
            save_checkpoint(CHECKPOINT_FILE, {
                "processed_dataset_ids": sorted(processed_ids),
                "total_records": total_records,
                "dataset_count": dataset_count,
            })

    save_checkpoint(CHECKPOINT_FILE, {
        "processed_dataset_ids": sorted(processed_ids),
        "total_records": total_records,
        "dataset_count": dataset_count,
        "status": "done",
    })

    log.info("=" * 60)
    log.info("DONE: %d datasets processed, %d records -> %s",
             dataset_count, total_records, output_file)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
